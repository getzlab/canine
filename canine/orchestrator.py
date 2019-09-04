import typing
import os
import time
import sys
import warnings
import traceback
from subprocess import CalledProcessError
from .adapters import AbstractAdapter, ManualAdapter, FirecloudAdapter
from .backends import AbstractSlurmBackend, LocalSlurmBackend, RemoteSlurmBackend, TransientGCPSlurmBackend, TransientImageSlurmBackend
from .localization import AbstractLocalizer, BatchedLocalizer, LocalLocalizer, RemoteLocalizer, NFSLocalizer
from .utils import check_call
import yaml
import pandas as pd
from agutil import status_bar
version = '0.4.0'

ADAPTERS = {
    'Manual': ManualAdapter,
    'Firecloud': FirecloudAdapter,
    'Terra': FirecloudAdapter
}

BACKENDS = {
    'Local': LocalSlurmBackend,
    'Remote': RemoteSlurmBackend,
    'TransientGCP': TransientGCPSlurmBackend,
    'TransientImage': TransientImageSlurmBackend
}

LOCALIZERS = {
    'Batched': BatchedLocalizer,
    'Local': LocalLocalizer,
    'Remote': RemoteLocalizer,
    'NFS': NFSLocalizer
}

ENTRYPOINT = """#!/bin/bash
export CANINE="{version}"
export CANINE_BACKEND="{{backend}}"
export CANINE_ADAPTER="{{adapter}}"
export CANINE_ROOT="{{CANINE_ROOT}}"
export CANINE_COMMON="{{CANINE_COMMON}}"
export CANINE_OUTPUT="{{CANINE_OUTPUT}}"
export CANINE_JOBS="{{CANINE_JOBS}}"
source $CANINE_JOBS/$SLURM_ARRAY_TASK_ID/setup.sh
{{pipeline_script}}
CANINE_JOB_RC=$?
source $CANINE_JOBS/$SLURM_ARRAY_TASK_ID/teardown.sh
exit $CANINE_JOB_RC
""".format(version=version)

class Orchestrator(object):
    """
    Main class
    Parses a configuration object, initializes, runs, and cleans up a Canine Pipeline
    """

    @staticmethod
    def stringify(obj: typing.Any) -> typing.Any:
        """
        Recurses through the dictionary, converting objects to strings
        """
        if isinstance(obj, list):
            return [
                Orchestrator.stringify(elem)
                for elem in obj
            ]
        elif isinstance(obj, dict):
            return {
                key:Orchestrator.stringify(val)
                for key, val in obj.items()
            }
        return str(obj)

    @staticmethod
    def fill_config(cfg: typing.Union[str, typing.Dict[str, typing.Any]]) -> typing.Dict[str, typing.Any]:
        """
        Loads the given config object (or reads from the given filepath)
        Applies Canine defaults, then returns the final config dictionary
        """
        if isinstance(cfg, str):
            with open(cfg) as r:
                cfg = yaml.load(r, Loader=yaml.loader.SafeLoader)
        DEFAULTS = {
            'name': 'canine',
            'adapter': {
                'type': 'Manual',
            },
            'backend': {
                'type': 'Local'
            },
            'localization': {
                'strategy': 'Batched'
            }
        }
        for key, value in DEFAULTS.items():
            if key not in cfg:
                cfg[key] = value
            elif isinstance(value, dict):
                cfg[key] = {**value, **cfg[key]}
        return cfg


    def __init__(self, config: typing.Union[str, typing.Dict[str, typing.Any]]):
        """
        Initializes the Orchestrator from a given config
        """
        config = Orchestrator.fill_config(config)
        self.name = config['name']
        if 'script' not in config:
            raise KeyError("Config missing required key 'script'")
        self.script = config['script']
        if isinstance(self.script, str):
            if not os.path.isfile(self.script):
                raise FileNotFoundError(self.script)
        elif not isinstance(self.script, list):
            raise TypeError("script must be a path to a bash script or a list of bash commands")
        self.raw_inputs = Orchestrator.stringify(config['inputs']) if 'inputs' in config else {}
        self.resources = Orchestrator.stringify(config['resources']) if 'resources' in config else {}
        adapter = config['adapter']
        if adapter['type'] not in ADAPTERS:
            raise ValueError("Unknown adapter type '{type}'".format(**adapter))
        self._adapter_type=adapter['type']
        self.adapter = ADAPTERS[adapter['type']](**{arg:val for arg,val in adapter.items() if arg != 'type'})
        backend = config['backend']
        if backend['type'] not in BACKENDS:
            raise ValueError("Unknown backend type '{type}'".format(**backend))
        self._backend_type = backend['type']
        self._slurmconf_path = backend['slurm_conf_path'] if 'slurm_conf_path' in backend else None
        self.backend = BACKENDS[backend['type']](**backend)
        self.localizer_args = config['localization'] if 'localization' in config else {}
        if self.localizer_args['strategy'] not in LOCALIZERS:
            raise ValueError("Unknown localization strategy '{}'".format(self.localizer_args))
        self._localizer_type = LOCALIZERS[self.localizer_args['strategy']]
        self.localizer_args = {key:val for key,val in self.localizer_args.items() if key != 'strategy'}
        self.localizer_overrides = {}
        if 'overrides' in self.localizer_args:
            self.localizer_overrides = {**self.localizer_args['overrides']}
            del self.localizer_args['overrides']
        self.raw_outputs = Orchestrator.stringify(config['outputs']) if 'outputs' in config else {}
        if len(self.raw_outputs) == 0:
            warnings.warn("No outputs declared", stacklevel=2)
        if 'stdout' not in self.raw_outputs:
            self.raw_outputs['stdout'] = 'stdout'
        if 'stderr' not in self.raw_outputs:
            self.raw_outputs['stderr'] = 'stderr'

    def run_pipeline(self, output_dir: str = 'canine_output', dry_run: bool = False) -> typing.Tuple[str, dict, dict, pd.DataFrame]:
        """
        Runs the configured pipeline
        Returns a 4-tuple:
        * The batch job id
        * The input job specification
        * The sacct dataframe after all jobs completed
        """
        if isinstance(self.backend, LocalSlurmBackend) and os.path.exists(output_dir):
            raise FileExistsError("Output directory {} already exists".format(output_dir))
        job_spec = self.adapter.parse_inputs(self.raw_inputs)

        if len(job_spec) == 0:
            raise ValueError("You didn't specify any jobs!")

        print("Preparing pipeline of", len(job_spec), "jobs")
        print("Connecting to backend...")
        if isinstance(self.backend, RemoteSlurmBackend):
            self.backend.load_config_args()
        with self.backend:
            print("Initializing pipeline workspace")
            with self._localizer_type(self.backend, **self.localizer_args) as localizer:
                print("Localizing inputs...")
                abs_staging_dir = localizer.localize(
                    job_spec,
                    self.raw_outputs,
                    self.localizer_overrides
                )
                print("Job staged on SLURM controller in:", abs_staging_dir)
                print("Preparing pipeline script")
                env = localizer.environment('compute')
                root_dir = env['CANINE_ROOT']
                entrypoint_path = os.path.join(root_dir, 'entrypoint.sh')
                if isinstance(self.script, str):
                    pipeline_path = os.path.join(root_dir, os.path.basename(self.script))
                else:
                    pipeline_path = self.backend.pack_batch_script(
                        *self.script,
                        script_path=os.path.join(root_dir, 'script.sh')
                    )
                with self.backend.transport() as transport:
                    if isinstance(self.script, str):
                        transport.send(self.script, pipeline_path)
                        transport.chmod(pipeline_path, 0o775)
                    with transport.open(entrypoint_path, 'w') as w:
                        w.write(ENTRYPOINT.format(
                            backend=self._backend_type,
                            adapter=self._adapter_type,
                            pipeline_script=pipeline_path,
                            **env
                        ))
                    transport.chmod(entrypoint_path, 0o775)
                    transport.chmod(pipeline_path, 0o775)
                if dry_run:
                    localizer.clean_on_exit = False
                    return job_spec
                print("Waiting for cluster to finish startup...")
                self.backend.wait_for_cluster_ready()

                # perform hard reset of cluster; some backends do this own their
                # own, in which case we skip.  we also can't do this if path to slurm.conf
                # is unknown.
                if self.backend.hard_reset_on_orch_init and self._slurmconf_path:
                    active_jobs = self.backend.squeue('all')
                    if len(active_jobs):
                        print("There are active jobs. Skipping slurmctld restart")
                    else:
                        try:
                            print("Stopping slurmctld")
                            rc, stdout, stderr = self.backend.invoke(
                                'sudo pkill slurmctld',
                                True
                            )
                            check_call('sudo pkill slurmctld', rc, stdout, stderr)
                            print("Loading configurations", self._slurmconf_path)
                            rc, stdout, stderr = self.backend.invoke(
                                'sudo slurmctld -c -f {}'.format(self._slurmconf_path),
                                True
                            )
                            check_call('sudo slurmctld -c -f {}'.format(self._slurmconf_path), rc, stdout, stderr)
                            print("Restarting slurmctl")
                            rc, stdout, stderr = self.backend.invoke(
                                'sudo slurmctld reconfigure',
                                True
                            )
                            check_call('sudo slurmctld reconfigure', rc, stdout, stderr)
                        except CalledProcessError:
                            traceback.print_exc()
                            print("Slurmctld restart failed")
                print("Submitting batch job")
                batch_id = self.backend.sbatch(
                    entrypoint_path,
                    **{
                        'requeue': True,
                        'job_name': self.name,
                        'array': "0-{}".format(len(job_spec)-1),
                        'output': "{}/%a/workspace/stdout".format(env['CANINE_JOBS']),
                        'error': "{}/%a/workspace/stderr".format(env['CANINE_JOBS']),
                        **self.resources
                    }
                )
                print("Batch id:", batch_id)
                completed_jobs = []
                try:
                    waiting_jobs = {
                        '{}_{}'.format(batch_id, i)
                        for i in range(len(job_spec))
                    }
                    outputs = {}
                    while len(waiting_jobs):
                        time.sleep(30)
                        acct = self.backend.sacct(job=batch_id)
                        for jid in [*waiting_jobs]:
                            if jid in acct.index and acct['State'][jid] not in {'RUNNING', 'PENDING', 'NODE_FAIL'}:
                                job = jid.split('_')[1]
                                print("Job",job, "completed with status", acct['State'][jid], acct['ExitCode'][jid].split(':')[0])
                                completed_jobs.append((job, jid))
                                waiting_jobs.remove(jid)
                except:
                    print("Encountered unhandled exception. Cancelling batch job", file=sys.stderr)
                    self.backend.scancel(batch_id)
                    localizer.clean_on_exit = False
                    raise
                finally:
                    if len(completed_jobs):
                        print("Delocalizing outputs")
                        outputs = localizer.delocalize(self.raw_outputs, output_dir)
            print("Parsing output data")
            self.adapter.parse_outputs(outputs)
            acct = self.backend.sacct(job=batch_id)
            return pd.DataFrame(
                data={
                    job_id: {
                        'slurm_state': acct['State'][batch_id+'_'+job_id],
                        'exit_code': acct['ExitCode'][batch_id+'_'+job_id],
                        **job_spec[job_id],
                        **{
                            key: val[0] if isinstance(val, list) and len(val) == 1 else val
                            for key, val in outputs[job_id].items()
                        }
                    }
                    for job_id in job_spec
                }
            ).T.set_index(pd.Index([*job_spec], name='job_id'))
