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
version = '0.7.0'

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

        #
        # script
        if 'script' not in config:
            raise KeyError("Config missing required key 'script'")
        self.script = config['script']
        if isinstance(self.script, str):
            if not os.path.isfile(self.script):
                raise FileNotFoundError(self.script)
        elif not isinstance(self.script, list):
            raise TypeError("script must be a path to a bash script or a list of bash commands")

        #
        # inputs/resources
        self.raw_inputs = Orchestrator.stringify(config['inputs']) if 'inputs' in config else {}
        self.resources = Orchestrator.stringify(config['resources']) if 'resources' in config else {}

        #
        # adapter
        adapter = config['adapter']
        if adapter['type'] not in ADAPTERS:
            raise ValueError("Unknown adapter type '{type}'".format(**adapter))
        self._adapter_type=adapter['type']
        self.adapter = ADAPTERS[adapter['type']](**{arg:val for arg,val in adapter.items() if arg != 'type'})
        self.job_spec = self.adapter.parse_inputs(self.raw_inputs)

        #
        # backend
        backend = config['backend']
        if backend['type'] not in BACKENDS:
            raise ValueError("Unknown backend type '{type}'".format(**backend))
        self._backend_type = backend['type']
        self._slurmconf_path = backend['slurm_conf_path'] if 'slurm_conf_path' in backend else None
        self.backend = BACKENDS[self._backend_type](**backend)

        #
        # localizer
        self.localizer_args = config['localization'] if 'localization' in config else {}
        if self.localizer_args['strategy'] not in LOCALIZERS:
            raise ValueError("Unknown localization strategy '{}'".format(self.localizer_args))
        self._localizer_type = LOCALIZERS[self.localizer_args['strategy']]
        self.localizer_overrides = {}
        if 'overrides' in self.localizer_args:
            self.localizer_overrides = {**self.localizer_args['overrides']}

        #
        # outputs
        if "outputs" in config:
            # process optional output postprocessing functions
            self.output_map = {}
            for k, v in config["outputs"].items():
                if type(v) == tuple and callable(v[1]):
                    self.output_map[k] = v[1]
                    config["outputs"][k] = v[0]

            self.raw_outputs = Orchestrator.stringify(config['outputs'])
        else:
            self.raw_outputs = {}

        if len(self.raw_outputs) == 0:
            warnings.warn("No outputs declared", stacklevel=2)
        if 'stdout' not in self.raw_outputs:
            self.raw_outputs['stdout'] = '../stdout'
        if 'stderr' not in self.raw_outputs:
            self.raw_outputs['stderr'] = '../stderr'

    def run_pipeline(self, output_dir: str = 'canine_output', dry_run: bool = False) -> pd.DataFrame:
        """
        Runs the configured pipeline
        Returns a pandas DataFrame containing job inputs, outputs, and runtime information
        """
        if isinstance(self.backend, LocalSlurmBackend) and os.path.exists(output_dir):
            raise FileExistsError("Output directory {} already exists".format(output_dir))

        if len(self.job_spec) == 0:
            raise ValueError("You didn't specify any jobs!")
        elif len(self.job_spec) > 4000000:
            raise ValueError("Cannot exceed 4000000 jobs in one pipeline")

        print("Preparing pipeline of", len(self.job_spec), "jobs")
        print("Connecting to backend...")
        if isinstance(self.backend, RemoteSlurmBackend):
            self.backend.load_config_args()
        start_time = time.monotonic()
        with self.backend:
            print("Initializing pipeline workspace")
            with self._localizer_type(self.backend, **self.localizer_args) as localizer:
                #
                # localize inputs
                self.job_avoid(localizer)
                entrypoint_path = self.localize_inputs_and_script(localizer)

                if dry_run:
                    localizer.clean_on_exit = False
                    return self.job_spec

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

                #
                # submit job
                print("Submitting batch job")
                batch_id = self.submit_batch_job(entrypoint_path, localizer.environment('compute'))
                print("Batch id:", batch_id)

                #
                # wait for jobs to finish
                completed_jobs = []
                cpu_time = {}
                uptime = {}
                prev_acct = None
                try:
                    completed_jobs, cpu_time, uptime, prev_acct = self.wait_for_jobs_to_finish(batch_id)
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

            df = self.make_output_DF(batch_id, outputs, cpu_time, prev_acct, localizer)

        try:
            runtime = time.monotonic() - start_time
            print("Estimated total cluster cost:", self.backend.estimate_cost(
                runtime/3600,
                node_uptime=sum(uptime.values())/120
            )[0])
            job_cost = self.backend.estimate_cost(job_cpu_time=df['cpu_hours'].to_dict())[1]
            df['est_cost'] = [job_cost[job_id] for job_id in df.index] if job_cost is not None else [0] * len(df)
        except:
            traceback.print_exc()
        finally:
            return df

    def localize_inputs_and_script(self, localizer) -> str:
        print("Localizing inputs...")
        abs_staging_dir = localizer.localize(
            self.job_spec,
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

        return entrypoint_path

    def wait_for_jobs_to_finish(self, batch_id):
        completed_jobs = []
        cpu_time = {}
        uptime = {}
        prev_acct = None

        waiting_jobs = {
            '{}_{}'.format(batch_id, i)
            for i in range(len(self.job_spec))
        }

        while len(waiting_jobs):
            time.sleep(30)
            acct = self.backend.sacct(job=batch_id, format="JobId,State,ExitCode,CPUTimeRAW").astype({'CPUTimeRAW': int})
            for jid in [*waiting_jobs]:
                if jid in acct.index:
                    if prev_acct is not None and jid in prev_acct.index and prev_acct['CPUTimeRAW'][jid] > acct['CPUTimeRAW'][jid]:
                        # Job has restarted since last update:
                        if jid in cpu_time:
                            cpu_time[jid] += prev_acct['CPUTimeRAW'][jid]
                        else:
                            cpu_time[jid] = prev_acct['CPUTimeRAW'][jid]
                    if acct['State'][jid] not in {'RUNNING', 'PENDING', 'NODE_FAIL'}:
                        job = jid.split('_')[1]
#                        print("Job",job, "completed with status", acct['State'][jid], acct['ExitCode'][jid].split(':')[0])
                        completed_jobs.append((job, jid))
                        waiting_jobs.remove(jid)
            for node in {node for node in self.backend.squeue(jobs=batch_id)['NODELIST(REASON)'] if not node.startswith('(')}:
                if node in uptime:
                    uptime[node] += 1
                else:
                    uptime[node] = 1
            if prev_acct is None:
                prev_acct = acct
            else:
                prev_acct = pd.concat([
                    acct,
                    prev_acct.loc[[idx for idx in prev_acct.index if idx not in acct.index]]
                ])

        return completed_jobs, cpu_time, uptime, prev_acct

    def make_output_DF(self, batch_id, outputs, cpu_time, prev_acct, localizer = None) -> pd.DataFrame:
        try:
            acct = self.backend.sacct(job=batch_id)

            df = pd.DataFrame.from_dict(
                data={
                    job_id: {
                        ('job', 'slurm_state'): acct['State'][batch_id+'_'+job_id],
                        ('job', 'exit_code'): acct['ExitCode'][batch_id+'_'+job_id],
                        ('job', 'cpu_hours'): (prev_acct['CPUTimeRAW'][batch_id+'_'+job_id] + (
                            cpu_time[batch_id+'_'+job_id] if batch_id+'_'+job_id in cpu_time else 0
                        ))/3600 if prev_acct is not None else -1,
                        **{ ('inputs', key) : val for key, val in self.job_spec[job_id].items() },
                        **{
                            ('outputs', key) : val[0] if isinstance(val, list) and len(val) == 1 else val
                            for key, val in outputs[job_id].items()
                        }
                    }
                    for job_id in self.job_spec
                },
                orient = "index"
            ).rename_axis(index = "_job_id").astype({('job', 'cpu_hours'): int})

            #
            # apply functions to output columns (if any)
            if len(self.output_map) > 0:
                # columns that receive no (i.e., identity) transformation
                identity_map = { x : lambda y : y for x in set(df.columns.get_loc_level("outputs")[1]) - self.output_map.keys() }

                # we get back all columns from the dataframe by aggregating columns
                # that don't receive any transformation with transformed columns
                df["outputs"] = df["outputs"].agg({ **self.output_map, **identity_map })
        except:
            df = pd.DataFrame()

        if isinstance(localizer, AbstractLocalizer):
            fname = "results.k9df.pickle"
            df.to_pickle(fname)
            localizer.localize_file(fname, localizer.reserve_path(localizer.staging_dir, "results.k9df.pickle"))
            os.remove(fname)

        return df

    def submit_batch_job(self, entrypoint_path, compute_env, extra_sbatch_args = {}):
        # this job was avoided
        if len(self.job_spec) == 0:
            return -2
            
        batch_id = self.backend.sbatch(
            entrypoint_path,
            **{
                'requeue': True,
                'job_name': self.name,
                'array': "0-{}".format(len(self.job_spec)-1),
                'output': "{}/%a/stdout".format(compute_env['CANINE_JOBS']),
                'error': "{}/%a/stderr".format(compute_env['CANINE_JOBS']),
                **self.resources,
                **Orchestrator.stringify(extra_sbatch_args)
            }
        )

        return batch_id

    def job_avoid(self, localizer, overwrite = False): #TODO: add params for type of avoidance (force, only if failed, etc.)
        # is there preexisting output?
        df_path = localizer.reserve_path(localizer.staging_dir, "results.k9df.pickle")
        if os.path.exists(df_path.localpath): 
            # load in results and job spec dataframes
            r_df = pd.read_pickle(df_path.localpath)
            js_df = pd.DataFrame.from_dict(self.job_spec, orient = "index").rename_axis(index = "_job_id")

            # check if jobs are compatible: they must have identical inputs and index,
            # and output columns must be matching
            if not (r_df["inputs"].columns.isin(js_df.columns).all() and \
                    js_df.columns.isin(r_df["inputs"].columns).all()):
                raise ValueError("Cannot job avoid; set of input parameters do not match")

            # FIXME: removing stdout/stderr from output keys is a bug fix --
            #        for some reason these aren't getting propagated to the output DF
            output_temp = pd.Series(index = self.raw_outputs.keys() - {'stdout', 'stderr'})
            if not (r_df["outputs"].columns.isin(output_temp.index).all() and \
                    output_temp.index.isin(r_df["outputs"].columns).all()):
                raise ValueError("Cannot job avoid; sets of output parameters do not match")

            # check that values of inputs are the same
            # we have to sort because the order of jobs might differ for the same
            # inputs
            sort_cols = r_df.columns.to_series()["inputs"]
            r_df = r_df.sort_values(sort_cols.tolist())
            js_df = js_df.sort_values(sort_cols.index.tolist())

            if not r_df["inputs"].equals(js_df):
                raise ValueError("Cannot job avoid; values of input parameters do not match!")

            # if all is well, figure out which jobs need to be re-run
            fail_idx = r_df[("job", "slurm_state")] == "FAILED"

            # remove jobs that don't need to be re-run from job_spec
            for k in r_df.index[~fail_idx]:
                self.job_spec.pop(k, None)

            # remove output directories of failed jobs
            for k in self.job_spec:
                os.rmdir(os.path.join(localizer.environment('local')["CANINE_JOBS"], k))
