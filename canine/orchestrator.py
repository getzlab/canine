import typing
import os
import time
import sys
import warnings
from .adapters import AbstractAdapter, ManualAdapter, FirecloudAdapter
from .backends import AbstractSlurmBackend, LocalSlurmBackend, RemoteSlurmBackend, TransientGCPSlurmBackend
from .localization import Localizer
import yaml
import pandas as pd
version = '0.0.1'

ADAPTERS = {
    'Manual': ManualAdapter,
    'Firecloud': FirecloudAdapter
}

BACKENDS = {
    'Local': LocalSlurmBackend,
    'Remote': RemoteSlurmBackend,
    'TransientGCP': TransientGCPSlurmBackend
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
        if isinstance(self.script, str) and not os.path.isfile(self.script):
            raise FileNotFoundError(self.script)
        elif not isinstance(self.script, list):
            raise TypeError("script must be a path to a bash script or a list of bash commands")
        self.raw_inputs = Orchestrator.stringify(config['inputs']) if 'inputs' in config else {}
        self.resources = Orchestrator.stringify(config['resources']) if 'resources' in config else {}
        adapter = config['adapter']
        if adapter['type'] not in ADAPTERS:
            raise ValueError("Unknown adapter type '{}'".format(adapter))
        self._adapter_type=adapter['type']
        self.adapter: AbstractAdapter = ADAPTERS[adapter['type']](**{arg:val for arg,val in adapter.items() if arg != 'type'})
        backend = config['backend']
        if backend['type'] not in BACKENDS:
            raise ValueError("Unknown backend type '{}'".format(backend))
        self._backend_type=backend['type']
        self.backend: AbstractSlurmBackend = BACKENDS[backend['type']](**{arg:val for arg,val in backend.items() if arg != 'type'})
        self.localizer_args = config['localization'] if 'localization' in config else {}
        self.localizer_overrides = {}
        if 'overrides' in self.localizer_args:
            self.localizer_overrides = {**self.localizer_args['overrides'], **{'stdout': 'stdout', 'stderr': 'stderr'}}
            del self.localizer_args['overrides']
        self.raw_outputs = Orchestrator.stringify(config['outputs']) if 'outputs' in config else {}
        if len(self.raw_outputs) == 0:
            warnings.warn("No outputs declared", stacklevel=2)

    def run_pipeline(self, dry_run: bool = False) -> typing.Tuple[str, dict, dict, pd.DataFrame]:
        """
        Runs the configured pipeline
        Returns a 4-tuple:
        * The batch job id
        * The input job specification
        * The sacct dataframe after all jobs completed
        """
        print("Raw inputs:", self.raw_inputs)
        job_spec = self.adapter.parse_inputs(self.raw_inputs)
        print("Job spec:", job_spec)
        print("Preparing pipeline of", len(job_spec), "jobs")
        print("Connecting to backend...")
        if isinstance(self.backend, RemoteSlurmBackend):
            self.backend.load_config_args()
        with self.backend:
            print("Initializing pipeline workspace")
            with Localizer(self.backend, **self.localizer_args) as localizer:
                print("Localizing inputs...")
                localizer.localize(
                    job_spec,
                    self.localizer_overrides
                )
                print("Preparing pipeline script")
                env = localizer.environment
                root_dir = localizer.mount_path
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
                    with transport.open(entrypoint_path, 'w') as w:
                        w.write(ENTRYPOINT.format(
                            backend=self._backend_type,
                            adapter=self._adapter_type,
                            pipeline_script=pipeline_path,
                            **env
                        ))
                    transport.chmod(entrypoint_path, 0o775)
                    print("Preparing job environments...")
                    for job in job_spec:
                        localizer.localize_job(job, transport=transport)
                if dry_run:
                    localizer.clean_on_exit = False
                    return job_spec
                print("Waiting for cluster to finish startup...")
                self.backend.wait_for_cluster_ready()
                print("Submitting batch job")
                batch_id = self.backend.sbatch(
                    entrypoint_path,
                    'requeue',
                    array="0-{}".format(len(job_spec)-1),
                    output="{}/%a/workspace/stdout".format(env['CANINE_JOBS']),
                    error="{}/%a/workspace/stderr".format(env['CANINE_JOBS']),
                    **self.resources
                )
                print("Batch id:", batch_id)
                try:
                    waiting_jobs = {
                        '{}_{}'.format(batch_id, i)
                        for i in range(len(job_spec))
                    }
                    outputs = {}
                    while len(waiting_jobs):
                        time.sleep(30)
                        acct = self.backend.sacct()
                        completed_jobs = []
                        for jid in [*waiting_jobs]:
                            if jid in acct.index and acct['State'][jid] not in {'RUNNING', 'PENDING', 'NODE_FAIL'}:
                                job = jid.split('_')[1]
                                print("Delocalizing task",job, "with status", acct['State'][jid])
                                # outputs.update(localizer.delocalize(self.raw_outputs, jobId=job))
                                # waiting_jobs.remove(jid)
                                completed_jobs.append((job, jid))
                        if len(completed_jobs):
                            with self.backend.transport() as transport:
                                for job, jid in completed_jobs:
                                    waiting_jobs.remove(jid)
                                    outputs.update(localizer.delocalize(
                                        self.raw_outputs,
                                        jobId=job,
                                        transport=transport
                                    ))

                except:
                    print("Encountered unhandled exception. Cancelling batch job", file=sys.stderr)
                    self.backend.scancel(batch_id)
                    raise
            print("Parsing output data")
            self.adapter.parse_outputs(outputs)
            return batch_id, job_spec, outputs, self.backend.sacct()
