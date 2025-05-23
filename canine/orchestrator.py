import copy
import typing
import os
import time
import sys
import warnings
import traceback
from subprocess import CalledProcessError
from .adapters import AbstractAdapter, ManualAdapter, FirecloudAdapter
from .backends import AbstractSlurmBackend, LocalSlurmBackend, RemoteSlurmBackend, DummySlurmBackend, TransientGCPSlurmBackend, TransientImageSlurmBackend, DockerTransientImageSlurmBackend, LocalDockerSlurmBackend
from .localization import AbstractLocalizer, BatchedLocalizer, LocalLocalizer, RemoteLocalizer, NFSLocalizer, file_handlers
from .utils import check_call, pandas_read_hdf5_buffered, pandas_write_hdf5_buffered, canine_logging
import yaml
import numpy as np
import pandas as pd
from agutil import status_bar
from operator import itemgetter
from itertools import groupby

version = '0.16.2'

ADAPTERS = {
    'Manual': ManualAdapter,
    'Firecloud': FirecloudAdapter,
    'Terra': FirecloudAdapter
}

BACKENDS = {
    'Local': LocalSlurmBackend,
    'Remote': RemoteSlurmBackend,
    'TransientGCP': TransientGCPSlurmBackend,
    'TransientImage': TransientImageSlurmBackend,
    'DockerTransientImage': DockerTransientImageSlurmBackend,
    'LocalDocker': LocalDockerSlurmBackend,
    'Dummy': DummySlurmBackend
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
export CANINE_RETRY_LIMIT={{retry_limit}}
export CANINE_PREEMPT_LIMIT=5 # hardcoded to 5 for now, since preemptible VMs are ~1/5 the cost of nonpreemptible
export CANINE_ROOT="{{CANINE_ROOT}}"
export CANINE_COMMON="{{CANINE_COMMON}}"
export CANINE_OUTPUT="{{CANINE_OUTPUT}}"
export CANINE_JOBS="{{CANINE_JOBS}}"
echo -n '---- STARTING JOB SETUP ... ' >&2
source $CANINE_JOBS/$SLURM_ARRAY_TASK_ID/setup.sh
echo 'COMPLETE ----' >&2
if [ $((${{{{SLURM_RESTART_COUNT:-0}}}}-$([ -f $CANINE_JOB_ROOT/.localization_failure_count ] && cat $CANINE_JOB_ROOT/.localization_failure_count || echo -n 0))) -ge $CANINE_PREEMPT_LIMIT ]; then
  # localization must have completed successfully and job must not have exited with a failure
  if [[ ( -e $CANINE_JOB_ROOT/.localizer_exit_code && $(cat $CANINE_JOB_ROOT/.job_exit_code) -eq 0) && \
        (( -e $CANINE_JOB_ROOT/.job_exit_code && $(cat $CANINE_JOB_ROOT/.job_exit_code) -eq 0 ) || \
         ! -e $CANINE_JOB_ROOT/.job_exit_code) \
  ]]; then
    echo "Preemption limit exceeded; requeueing on non-preemptible nodes" >&2
    exit 123 # special exit code indicating excessive preemption
  fi
fi
rm -f $CANINE_JOB_ROOT/.*exit_code || :
echo '~~~~ STARTING JOB LOCALIZATION ~~~~' >&2
$CANINE_JOBS/$SLURM_ARRAY_TASK_ID/localization.sh >&2
export LOCALIZER_JOB_RC=$?
if [ $LOCALIZER_JOB_RC -eq 0 ]; then
  echo '~~~~ LOCALIZATION COMPLETE ~~~~' >&2
  echo -n 0 > $CANINE_JOB_ROOT/.localizer_exit_code
  cd $CANINE_JOB_WORKSPACE
  while true; do
    echo '======================' >&2
    echo '==== STARTING JOB ====' >&2
    echo '======================' >&2
    {{pipeline_script}}
    export CANINE_JOB_RC=$?
    if [ $CANINE_JOB_RC == 0 ]; then
      echo '====================================' >&2
      echo '==== JOB COMPLETED SUCCESSFULLY ====' >&2
      echo '====================================' >&2
      break
    else
      echo    "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" >&2
      echo -e "!!!! JOB FAILED! (EXIT CODE                 !!!!\e[29G$CANINE_JOB_RC)" >&2
      echo    "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" >&2
      echo $(($([ -f $CANINE_JOB_ROOT/.job_failure_count ] && cat $CANINE_JOB_ROOT/.job_failure_count || echo -n 0)+1)) > $CANINE_JOB_ROOT/.job_failure_count
      echo '++++ STARTING JOB CLEANUP ++++' >&2
      $CANINE_JOBS/$SLURM_ARRAY_TASK_ID/teardown.sh >&2
      TEARDOWN_RC=$?
      [ $TEARDOWN_RC == 0 ] && echo '++++ CLEANUP COMPLETE ++++' >&2 || echo '!+++ CLEANUP FAILURE +++!' >&2
      [[ ${{{{SLURM_RESTART_COUNT:-0}}}} -ge $CANINE_RETRY_LIMIT ]] && {{{{ echo "ERROR: Retry limit of $CANINE_RETRY_LIMIT retries exceeded" >&2; break; }}}} || :
      if [[ $CANINE_JOB_RC -eq 137 ]]; then
        echo "INFO: job ran out of memory; will not attempt to requeue." >&2
        break
      fi
      echo "INFO: Retrying job (attempt $((${{{{SLURM_RESTART_COUNT:-0}}}}+1))/$CANINE_RETRY_LIMIT)" >&2
      [ -f $CANINE_JOB_ROOT/stdout ] && mv $CANINE_JOB_ROOT/stdout $CANINE_JOB_ROOT/stdout_${{{{SLURM_RESTART_COUNT:-0}}}} || :
      [ -f $CANINE_JOB_ROOT/stderr ] && mv $CANINE_JOB_ROOT/stderr $CANINE_JOB_ROOT/stderr_${{{{SLURM_RESTART_COUNT:-0}}}} || :
      scontrol requeue $SLURM_JOB_ID
    fi
  done
  echo -n $CANINE_JOB_RC > $CANINE_JOB_ROOT/.job_exit_code
# these are special exit codes that localization.sh can explicitly return
elif [ $LOCALIZER_JOB_RC -eq 5 ]; then # localization failed due to recoverable reason (e.g. quota); requeue the job
  echo "WARNING: localization will be retried" >&2
  echo $(($([ -f $CANINE_JOB_ROOT/.localization_failure_count ] && cat $CANINE_JOB_ROOT/.localization_failure_count || echo -n 0)+1)) > $CANINE_JOB_ROOT/.localization_failure_count
  scontrol requeue $SLURM_JOB_ID
elif [ $LOCALIZER_JOB_RC -eq 15 ]; then # localization and job can be skipped (to facilitate avoidance of scratch disk tasks)
  echo '~~~~ LOCALIZATION SKIPPED ~~~~' >&2
  export CANINE_JOB_RC=0
  echo -n "DNR" > $CANINE_JOB_ROOT/.job_exit_code
  echo -n $LOCALIZER_JOB_RC > $CANINE_JOB_ROOT/.localizer_exit_code
else
  echo '!~~~ LOCALIZATION FAILURE! JOB CANNOT RUN! ~~~!' >&2
  echo -n "DNR" > $CANINE_JOB_ROOT/.job_exit_code
  echo -n $LOCALIZER_JOB_RC > $CANINE_JOB_ROOT/.localizer_exit_code
  export CANINE_JOB_RC=$LOCALIZER_JOB_RC
fi
echo '++++ STARTING JOB DELOCALIZATION ++++' >&2
cd $CANINE_JOB_ROOT
$CANINE_JOBS/$SLURM_ARRAY_TASK_ID/teardown.sh >&2
DELOC_RC=$?
[ $DELOC_RC == 0 ] && echo '++++ DELOCALIZATION COMPLETE ++++' >&2 || echo '!+++ DELOCALIZATION FAILURE +++!' >&2
echo -n $DELOC_RC > $CANINE_JOB_ROOT/.teardown_exit_code
exit $CANINE_JOB_RC
""".format(version=version)

def stringify(obj: typing.Any, safe: bool = True) -> typing.Any:
    """
    Recurses through the dictionary, converting objects to strings
    """
    if isinstance(obj, list):
        return [
            stringify(elem)
            for elem in obj
        ]
    elif isinstance(obj, dict):
        return {
            key:stringify(val)
            for key, val in obj.items()
        }
    elif isinstance(obj, pd.core.series.Series) or \
         isinstance(obj, pd.core.indexes.base.Index) or \
         isinstance(obj, np.ndarray):
        return [
            stringify(elem)
            for elem in obj.tolist()
        ]
    elif isinstance(obj, pd.core.frame.DataFrame):
        return stringify(obj.to_dict(orient = "list"))
    # pass through FileType objects as-is
    elif isinstance(obj, file_handlers.FileType):
        return obj

    if safe:
        if "\n" in str(obj):
            raise TypeError('Unsupported type "{}"'.format(type(obj)))

    return str(obj)

class StringifyTypeError(TypeError):
    pass

class Orchestrator(object):
    """
    Main class
    Parses a configuration object, initializes, runs, and cleans up a Canine Pipeline
    """

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
            },
            'outputs': {}
        }
        for key, value in DEFAULTS.items():
            if key not in cfg:
                cfg[key] = value
            elif isinstance(value, dict):
                cfg[key] = {**value, **cfg[key]}
        return cfg

    @staticmethod
    def load_acct_from_disk(job_spec, localizer, batch_id):
        """
        Read in accounting information saved to disk, and convert to format
        equivalent to backend.sacct().
        Used for retrieving accounting information for avoided jobs.
        """

        jobs_dir = localizer.environment("local")["CANINE_JOBS"]
        acct = {}
        placeholder_fields = { "State" : np.nan, "ExitCode": "-", "CPUTimeRAW" : -1, "Submit": np.datetime64('nat'), "n_preempted" : -1, "NodeList" : "-", "Partition" : "-","ReqCPUS" : -1, "NCPUS" : -1, "ReqMem" : "-"}

        with localizer.transport_context() as tr:
            for j, v in job_spec.items():
                sacct_path = os.path.join(jobs_dir, j, ".sacct")
                jid = str(batch_id) + "_" + j
                if tr.exists(sacct_path):
                    with tr.open(sacct_path, "r") as f:
                        acct[jid] = pd.read_csv(
                          f,
                          header = None,
                          sep = "\t",
                          names = [
                            "State", "ExitCode", "CPUTimeRAW", "Submit", "n_preempted","NodeList","Partition","ReqCPUS","NCPUS","ReqMem"
                          ]
                        ).astype({
                          'CPUTimeRAW': int,
                          "Submit" : np.datetime64,
                          "ReqCPUS" : int,
                          "NCPUS" : int,
                        })

                    # sacct info is blank (write error?)
                    if acct[jid].empty:
                        acct[jid] = pd.DataFrame(placeholder_fields, index = [0])

                # sacct never got written
                else:
                    acct[jid] = pd.DataFrame(placeholder_fields, index = [0])

                # if job_spec[j] is None, this indicates a noop (job was avoided)
                # override state to completed, regardless of what got loaded from disk
                if v is None:
                    acct[jid]["State"] = "COMPLETED"

        return pd.concat(acct).droplevel(1).rename_axis("JobID")

    def __init__(self, config: typing.Union[
      str,
      typing.Dict[str, typing.Any],
      pd.core.frame.DataFrame,
      pd.core.series.Series
    ]):
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
        inputs_to_void = { k for k, v in config["inputs"].items() if v is None }
        for k in inputs_to_void:
            canine_logging.warning('Input "{}" was specified as None, ignoring.'.format(k)) 
        config["inputs"] = { k : config["inputs"][k] for k in config["inputs"].keys() - inputs_to_void }
        try:
            self.raw_inputs = stringify(config['inputs']) if 'inputs' in config else {}
        except TypeError:
            raise StringifyTypeError("Unsupported input type! Inputs can only consist of lists, dicts, and Pandas series/dataframes/indices")
        self.resources = stringify(config['resources']) if 'resources' in config else {}

        # retries
        if "retry" in config:
            if type(config["retry"]) != int:
                raise TypeError("Retry count must be an int")
            if config["retry"] < 0:
                raise ValueError("Retry count must be >= 0")
        self.retry_limit = stringify(config['retry']) if 'retry' in config else 0

        # avoidance
        if "avoid" in config:
            if type(config["avoid"]) != bool:
                raise TypeError("job avoid flag must be a bool!")
        self.force_avoid = not config["avoid"] if "avoid" in config else False

        #
        # adapter
        adapter = config['adapter']
        if adapter['type'] not in ADAPTERS:
            raise ValueError("Unknown adapter type '{type}'".format(**adapter))
        self._adapter_type=adapter['type']
        self.adapter = ADAPTERS[adapter['type']](**{arg:val for arg,val in adapter.items() if arg != 'type'})
        self.job_spec = self.adapter.parse_inputs(self.raw_inputs)

        # if no inputs were provided, assume this is a standalone task
        if not self.job_spec:
            canine_logging.warning("No inputs provided; assuming this is a standalone task")
            self.job_spec = { "0" : { "null" : "null" } }
            self.force_avoid = True # standalone tasks cannot job avoid, since they have no inputs

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
        self.output_map = {}
        if "outputs" in config:
            # process optional output postprocessing functions
            for k, v in config["outputs"].items():
                if type(v) == tuple and callable(v[1]):
                    self.output_map[k] = v[1]
                    config["outputs"][k] = v[0]

            self.raw_outputs = stringify(config['outputs'])
        else:
            self.raw_outputs = {}

        if len(self.raw_outputs) == 0:
            warnings.warn("No outputs declared", stacklevel=2)
        if 'stdout' not in self.raw_outputs:
            self.raw_outputs['stdout'] = '$CANINE_JOB_ROOT/stdout'
        if 'stderr' not in self.raw_outputs:
            self.raw_outputs['stderr'] = '$CANINE_JOB_ROOT/stderr'

        # placeholder for dataframe containing previous results that were
        # job avoided
        self.df_avoided = None

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

        canine_logging.print("Preparing pipeline of", len(self.job_spec), "jobs")
        canine_logging.info1("Connecting to backend...")
        if isinstance(self.backend, RemoteSlurmBackend):
            self.backend.load_config_args()
        start_time = time.monotonic()
        with self.backend:
            canine_logging.info1("Initializing pipeline workspace")
            with self._localizer_type(self.backend, **self.localizer_args) as localizer:
                #
                # localize inputs
                n_avoided, original_job_spec = self.job_avoid(localizer = localizer, overwrite = self.force_avoid)
                entrypoint_path = self.localize_inputs_and_script(localizer)

                if dry_run:
                    localizer.clean_on_exit = False
                    return self.job_spec

                canine_logging.info1("Waiting for cluster to finish startup...")
                self.backend.wait_for_cluster_ready()

                # perform hard reset of cluster; some backends do this own their
                # own, in which case we skip.  we also can't do this if path to slurm.conf
                # is unknown.
                if self.backend.hard_reset_on_orch_init and self._slurmconf_path:
                    active_jobs = self.backend.squeue('all')
                    if len(active_jobs):
                        canine_logging.warning("There are active jobs. Skipping slurmctld restart")
                    else:
                        try:
                            canine_logging.info1("Stopping slurmctld")
                            rc, stdout, stderr = self.backend.invoke(
                                'sudo pkill slurmctld',
                                True
                            )
                            check_call('sudo pkill slurmctld', rc, stdout, stderr)
                            canine_logging.print("Loading configurations", self._slurmconf_path)
                            rc, stdout, stderr = self.backend.invoke(
                                'sudo slurmctld -c -f {}'.format(self._slurmconf_path),
                                True
                            )
                            check_call('sudo slurmctld -c -f {}'.format(self._slurmconf_path), rc, stdout, stderr)
                            canine_logging.info1("Restarting slurmctl")
                            rc, stdout, stderr = self.backend.invoke(
                                'sudo slurmctld reconfigure',
                                True
                            )
                            check_call('sudo slurmctld reconfigure', rc, stdout, stderr)
                        except CalledProcessError:
                            traceback.print_exc()
                            canine_logging.error("Slurmctld restart failed")

                #
                # submit job
                canine_logging.info1("Submitting batch job")
                batch_id = self.submit_batch_job(entrypoint_path, localizer.environment('remote'))
                if batch_id != -2:
                    canine_logging.print("Batch id:", batch_id)

                #
                # wait for jobs to finish
                completed_jobs = []
                cpu_time = {}
                uptime = {}
                prev_acct = None
                try:
                    if batch_id != -2: # check if all shards were avoided
                        completed_jobs, uptime, acct = self.wait_for_jobs_to_finish(batch_id)
                except:
                    canine_logging.error("Encountered unhandled exception. Cancelling batch job")
                    self.backend.scancel(batch_id)
                    localizer.clean_on_exit = False
                    raise
                finally:
                    # if some jobs were avoided, read the Slurm accounting info from disk
                    if n_avoided != 0:
                        acct = Orchestrator.load_acct_from_disk(self.job_spec, localizer, batch_id)

                    # Check if fully job-avoided so we still delocalize
                    if batch_id == -2 or len(completed_jobs):
                        canine_logging.info1("Delocalizing outputs")
                        outputs = localizer.delocalize(self.raw_outputs, output_dir)

                canine_logging.info1("Parsing output data")
                self.adapter.parse_outputs(outputs)

                df = self.make_output_DF(batch_id, original_job_spec, outputs, acct, localizer)

        try:
            runtime = time.monotonic() - start_time
            canine_logging.print("Estimated total cluster cost:", self.backend.estimate_cost(
                runtime/3600,
                node_uptime=sum(uptime.values())/120
            )[0])
            job_cost = self.backend.estimate_cost(job_cpu_time=(df[('job', 'cpu_seconds')]/3600).to_dict())[1]
            df['est_cost'] = [job_cost[job_id] for job_id in df.index] if job_cost is not None else [0] * len(df)
        except:
            traceback.print_exc()
        finally:
            return df

    def localize_inputs_and_script(self, localizer) -> str:
        canine_logging.info1("Localizing inputs...")
        abs_staging_dir = localizer.localize(
            self.job_spec,
            self.raw_outputs,
            self.localizer_overrides
        )
        canine_logging.print("Task staged in", abs_staging_dir)
        env = localizer.environment('remote')
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
                    retry_limit=self.retry_limit,
                    **env
                ))
            transport.chmod(entrypoint_path, 0o775)
            transport.chmod(pipeline_path, 0o775)

        return entrypoint_path

    def wait_for_jobs_to_finish(self, batch_id, localizer = None, track_uptime = False):
        def grouper(g):
            g = g.sort_values("Submit")
            final = g.iloc[-1]
            final.at["CPUTimeRAW"] = g["CPUTimeRAW"].sum()
            final.at["Submit"] = g.loc[:, "Submit"].iloc[0]
            final["n_preempted"] = len(g) - 1

            return final

        acct = None
        completed_jobs = []
        uptime = {}

        waiting_jobs = {
            '{}_{}'.format(batch_id, k)
            for k, v in self.job_spec.items() if v is not None
            # exclude noop'd jobs from waiting set
        }

        save_acct = False
        if isinstance(localizer, AbstractLocalizer):
            save_acct = True
            jobs_dir = localizer.environment("local")["CANINE_JOBS"]

        while len(waiting_jobs):
            backoff_factor = 1
            while True:
                time.sleep(30*backoff_factor)
                acct = self.backend.sacct(
                  "D",
                  job = batch_id,
                  format = "JobId%50,State,ExitCode,CPUTimeRAW,PlannedCPURAW,Submit,NodeList%50,Partition%50,ReqCPUS,NCPUS,ReqMem"
                  ).astype({'CPUTimeRAW': int, "PlannedCPURAW" : float, "Submit" : np.datetime64, "ReqCPUS" : int, "NCPUS" : int})
                # sometimes sacct can lag when the cluster is under load and return nothing; retry with exponential backoff
                if len(acct) > 0:
                    break
                # corresponds to a five minute backoff
                elif backoff_factor >= 31:
                    raise Exception("Timeout exceeded waiting to query job accounting information; cluster is likely under extreme load!")
                else:
                    backoff_factor *= 1.1
            acct = acct.loc[~(acct.index.str.endswith("batch") | ~acct.index.str.contains("_"))]
            acct.loc[acct["PlannedCPURAW"].isna(), "PlannedCPURAW"] = 0
            acct.loc[:, "CPUTimeRAW"] += acct.loc[:, "PlannedCPURAW"].astype(int)
            acct = acct.drop(columns = ["PlannedCPURAW"])
            acct = acct.groupby(acct.index).apply(grouper)

            for jid in [*waiting_jobs]:
                if jid in acct.index: 
                    job = jid.split('_')[1]

                    # job has completed
                    if acct['State'][jid] not in {'RUNNING', 'PENDING', 'NODE_FAIL', 'REQUEUED'} or self.job_spec[job] is None:
#                        print("Job",job, "completed with status", acct['State'][jid], acct['ExitCode'][jid].split(':')[0])
                        completed_jobs.append((job, jid))
                        waiting_jobs.remove(jid)

                    # TODO: run this on each worker node
                    # save sacct info for each shard if it's not a noop (None)
                    if save_acct and self.job_spec[job] is not None:
                        with localizer.transport_context() as transport:
                            with transport.open(os.path.join(jobs_dir, job, ".sacct"), 'w') as w:
                                acct.loc[[jid]].to_csv(w, sep = "\t", header = False, index = False)

            # track node uptime
            # this is an expensive operation, so only track if requested
            if track_uptime:
                try:
                    for node in {node for node in self.backend.squeue(jobs=batch_id)['NODELIST(REASON)'] if not node.startswith('(')}:
                        if node in uptime:
                            uptime[node] += 1
                        else:
                            uptime[node] = 1
                # squeue can fail here if the job completed by the time we call it,
                # so we catch any errors.
                # TODO: make something less heavy-handed; this may hide true failures
                except CalledProcessError:
                    pass

        return completed_jobs, uptime, acct

    def make_output_DF(self, batch_id, job_spec, outputs, acct, localizer = None) -> pd.DataFrame:
        df = pd.DataFrame()

        try:
            # we cannot assume that all outputs were properly delocalized, i.e.
            # self.job_spec.keys() == outputs.keys()
            #
            # this could happen if a preemptible job runs out of preemption
            # attempts, so delocalization.py never gets a chance to run

            # sanity check: outputs cannot contain more keys than inputs
            if outputs.keys() - job_spec.keys():
                raise ValueError("{} job outputs discovered, but only {} job(s) specified!".format(
                  len(outputs), len(job_spec)
                ))

            # for jobs that failed to delocalize any outputs, pad the outputs
            # dict with blanks
            missing_outputs = job_spec.keys() - outputs.keys()
            if missing_outputs:
                canine_logging.error("{}/{} job(s) were catastrophically lost (no stdout/stderr available)".format(
                  len(missing_outputs), len(job_spec)
                ))
                outputs = { **outputs, **{ k : {} for k in missing_outputs } }

            batch_id = str(batch_id) # in case it's set to special value -2

            # make the output dataframe
            df = pd.DataFrame.from_dict(
                data={
                    job_id: {
                        ('job', 'slurm_state'): acct['State'][batch_id+'_'+str(array_id)],
                        ('job', 'exit_code'): acct['ExitCode'][batch_id+'_'+str(array_id)],
                        ('job', 'cpu_seconds'): acct['CPUTimeRAW'][batch_id+'_'+str(array_id)],
                        ('job', 'submit_time'): acct['Submit'][batch_id+'_'+str(array_id)],
                        ('job', 'n_preempted'): acct['n_preempted'][batch_id+'_'+str(array_id)],
                        **{ ('inputs', key) : str(val) for key, val in job_spec[job_id].items() },
                        **{
                            ('outputs', key) : val[0] if isinstance(val, list) and len(val) == 1 else val
                            for key, val in outputs[job_id].items()
                        }
                    }
                    for array_id, job_id in enumerate(job_spec)
                },
                orient = "index"
            ).rename_axis(index = "_job_id").astype({('job', 'cpu_seconds'): int, ('job', 'n_preempted'): int})

            #
            # apply functions to output columns (if any)
            if len(self.output_map) > 0:
                # columns that receive no (i.e., identity) transformation
                identity_map = { x : lambda y : y for x in set(df.columns.get_loc_level("outputs")[1]) - self.output_map.keys() }

                # we get back all columns from the dataframe by aggregating columns
                # that don't receive any transformation with transformed columns
                df["outputs"] = df["outputs"].agg({ **self.output_map, **identity_map })
        except:
            canine_logging.error("There were some errors generating output dataframe; see stack trace for details.")
            canine_logging.error(traceback.format_exc())

        # save DF to disk
        if isinstance(localizer, AbstractLocalizer):
            with localizer.transport_context() as transport:
                dest = localizer.reserve_path("results.k9df.hdf5").remotepath
                if not transport.isdir(os.path.dirname(dest)):
                    transport.makedirs(os.path.dirname(dest))
                with transport.open(dest, 'wb') as w:
                    pandas_write_hdf5_buffered(df, buf = w, key = "results")
        return df

    def submit_batch_job(self, entrypoint_path, compute_env, extra_sbatch_args = {}, job_spec = None) -> int:
        if job_spec is None:
            job_spec = self.job_spec

        # all shards in this job were avoided
        if all([x is None for x in job_spec.values()]):
            return -2

        # remove noop'd jobs from array spec
        op_idx = sorted(int(k) for k, v in job_spec.items() if v is not None)
        array_range = []
        for k, g in groupby(enumerate(op_idx), lambda i: i[0]-i[1]):
            group = list(map(itemgetter(1), g))
            if len(group) > 1:
                array_range += [f"{group[0]}-{group[-1]}"]
            else:
                array_range += [str(group[0])]
        array_str = ",".join(array_range)

        # parse out flags vs. params in extra_sbatch_args
        flags = [k for k, v in extra_sbatch_args.items() if v is None]
        params = { k : v for k, v in extra_sbatch_args.items() if v is not None }

        # submit to sbatch
        batch_id = self.backend.sbatch(
            entrypoint_path,
            *stringify(flags),
            **{
                'requeue': True,
                'job_name': self.name,
                'array': array_str,
                'output': "{}/%a/stdout".format(compute_env['CANINE_JOBS']),
                'error': "{}/%a/stderr".format(compute_env['CANINE_JOBS']),
                **self.resources,
                **stringify(params)
            }
        )

        n_jobs = np.array([v is not None for v in job_spec.values()])
        n_submitted = n_jobs.sum()
        n_avoided = (~n_jobs).sum()
        canine_logging.print("{n_submitted} job{plural} submitted{avoid_string}.".format(
          n_submitted = n_submitted,
          plural = "s" if n_submitted > 1 else "",
          avoid_string = f" ({n_avoided} avoided)" if n_avoided > 0 else ""
        ))

        return batch_id

    def job_avoid(self, localizer: AbstractLocalizer, overwrite: bool = False) -> int: #TODO: add params for type of avoidance (force, only if failed, etc.)
        """
        Detects jobs which have previously been run in this staging directory.
        Succeeded jobs are skipped. Failed jobs are reset and rerun
        """
        old_job_spec = copy.deepcopy(self.job_spec)
        n_avoided = 0

        with localizer.transport_context() as transport:
            # remove all output if specified
            if overwrite:
                canine_logging.warning("Job avoidance disabled for this task; overwriting output.")
                if transport.isdir(localizer.staging_dir):
                    transport.rmtree(localizer.staging_dir)
                    transport.makedirs(localizer.staging_dir)
                return n_avoided, old_job_spec

            # check for preexisting jobs' outputs
            # NOTE: this directory will not exist if the task used a scratch disk,
            #       which implicitly disables job avoidance for scratch disks.
            #       Scratch disk tasks will avoid via a different method by having the 
            #       localizer exit early.
            if transport.exists(localizer.staging_dir):
                try:
                    js_df = pd.DataFrame.from_dict(self.job_spec, orient = "index").rename_axis(index = "_job_id") 
                    js_df["failed"] = False
                    js_df["output_ok"] = False
                    jobs_dir = localizer.environment("local")["CANINE_JOBS"]
                    output_dir = localizer.environment("local")["CANINE_OUTPUT"]

                    # if everything succeeded, with matching outputs, we're done
                    # TODO

                    # check for failed shards 
                    for i in js_df.index:
                        # if workspace directory is missing, consider shard failed
                        # this is to disable job avoidance for jobs that write to scratch
                        # directories, which do not generate a workspace directory.
                        if not transport.isdir(os.path.join(jobs_dir, i, "workspace")):
                            js_df.at[i, "failed"] = True

                        # otherwise, make sure all three exit code are OK
                        else:
                            for e in [".job_exit_code", ".localizer_exit_code", ".teardown_exit_code"]:
                                exit_code = os.path.join(jobs_dir, i, e)
                                if transport.isfile(exit_code):
                                    with transport.open(exit_code, "r") as ec:
                                        js_df.at[i, "failed"] = (ec.read() != "0") | js_df.at[i, "failed"]
                                else:
                                    js_df.at[i, "failed"] = True
                                    break

                    # check for matching outputs
                    # name and pattern must both match
                    for i in js_df.loc[~js_df["failed"]].index:
                        o_df = pd.read_csv(
                          os.path.join(output_dir, i, ".canine_job_manifest"),
                          header = None,
                          names = ["shard", "output", "pattern", "path"],
                          sep = "\t"
                        )

                        if o_df.set_index("output").loc[:, "pattern"].to_dict() == self.raw_outputs:
                            js_df.at[i, "output_ok"] = True

                    # shards that both succeeded and have matching outputs can be noop'd
                    # in the job spec
                    js_df["noop"] = ~js_df["failed"] & js_df["output_ok"]

                    # shards that succeeded but don't have matching outputs must have their
                    # delocalizer rerun
                    js_df["re_deloc"] = ~js_df["failed"] & ~js_df["output_ok"]

                    # either way, these jobs will be noop'd; deloc. only jobs will be
                    # submitted separately
                    for i in js_df.index[js_df["noop"] | js_df["re_deloc"]]:
                        self.job_spec[i] = None

                    # shards that failed must have their output directories purged
                    for k in js_df.index[js_df["failed"]]:
                        transport.rmtree(
                            localizer.reserve_path('jobs', k).remotepath
                        )

                    # if we are re-running any jobs, we also have to remove the common
                    # inputs directory, so that the localizer can regenerate it
                    # I don't think we need this anymore, since the localizer checks for noops
            #		if (~js_df["noop"]).any():
            #			transport.rmtree(
            #				localizer.reserve_path('common').remotepath
            #			)

                    n_avoided += (js_df["noop"] | js_df["re_deloc"]).sum()
                except (ValueError, OSError) as e:
                    canine_logging.debug("Cannot recover preexisting task outputs: " + str(e))
                    canine_logging.debug("Overwriting output and aborting job avoidance.")
                    self.job_spec = old_job_spec
                    transport.rmtree(localizer.staging_dir)
                    transport.makedirs(localizer.staging_dir)
                    return 0, old_job_spec

        return n_avoided, old_job_spec
