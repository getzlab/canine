import typing
import os
import time
import sys
import warnings
import traceback
import shlex
import tempfile
from subprocess import CalledProcessError
from .backends import AbstractSlurmBackend, LocalSlurmBackend
from .utils import check_call
from .orchestrator import Orchestrator, BACKENDS, version
import yaml
import pandas as pd
from agutil import status_bar

class Xargs(Orchestrator):
    """
    Simplified Orchestrator
    Built to work on a local slurm cluster.
    No localization is performed. Job-specific commands are dumped to a staging
    directory, and jobs are dispatched. Outputs are not gathered or delocalized,
    and the only cleanup performed removes the temporary staging dir
    """

    def __init__(
        self, command: str, inputs: typing.Dict[str, typing.Any],
        backend: typing.Union[AbstractSlurmBackend, typing.Dict[str, typing.Any]],
        name: typing.Optional[str] = None,
        cwd: typing.Optional[str] = None,
        resources: typing.Optional[typing.Dict[str, typing.Any]] = None
    ):
        if isinstance(backend, AbstractSlurmBackend):
            self.backend = backend
            self._slurmconf_path = None
        else:
            self.backend = BACKENDS[backend['type']](**backend)
            self._slurmconf_path = backend['slurm_conf_path'] if 'slurm_conf_path' in backend else None
        if not isinstance(self.backend, LocalSlurmBackend):
            raise TypeError("Xargs only works on local-based slurm backends")
        self.resources = {} if resources is None else resources
        self.inputs = inputs
        self.cwd = cwd
        self.n_jobs = 0 if len(inputs) == 0 else len(inputs['canine_arg0'])
        self.name = name if name is not None else 'Canine-Xargs'
        self.command = command

    def run_pipeline(self, dry_run: bool = False) -> pd.DataFrame:
        print("Preparing a pipeline of", self.n_jobs, "jobs")
        print("Connecting to backend...")
        start_time = time.monotonic()
        with self.backend:
            print("Initializing pipeline workspace")
            with tempfile.TemporaryDirectory() as tempdir:
                for jid in range(self.n_jobs):
                    # By creating a local tempdir
                    # but using the backend to write the script
                    # we ensure that this is a local-based backend
                    self.backend.pack_batch_script(
                        *(
                            'export {}={}'.format(
                                argname,
                                shlex.quote(argvalues[jid])
                            )
                            for argname, argvalues in self.inputs.items()
                        ),
                        script_path=os.path.join(
                            tempdir,
                            '{}.sh'.format(jid)
                        )
                    )
                print("Job staged on SLURM controller in:", tempdir)
                print("Preparing pipeline script")
                self.backend.pack_batch_script(
                    'export CANINE={}'.format(version),
                    'export CANINE_BACKEND='.format(type(self.backend)),
                    'export CANINE_ADAPTER=Xargs',
                    'export CANINE_ROOT={}'.format(tempdir),
                    'export CANINE_COMMON=""',
                    'export CANINE_OUTPUT=""',
                    'export CANINE_JOBS={}'.format(self.cwd),
                    'source $CANINE_ROOT/$SLURM_ARRAY_TASK_ID.sh',
                    self.command,
                    script_path=os.path.join(
                        tempdir,
                        'entrypoint.sh'
                    )
                )
                if dry_run:
                    return
                print("Waiting for cluster to finish startup...")
                self.backend.wait_for_cluster_ready()
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
                    os.path.join(
                        tempdir,
                        'entrypoint.sh'
                    ),
                    **{
                        'chdir': '~' if self.cwd is None else self.cwd,
                        'requeue': True,
                        'job_name': self.name,
                        'array': "0-{}".format(self.n_jobs-1),
                        'output': "{}/%A.%a.stdout".format('~' if self.cwd is None else self.cwd),
                        'error': "{}/%A.%a.stderr".format('~' if self.cwd is None else self.cwd),
                        **self.resources
                    }
                )
                print("Batch id:", batch_id)
                completed_jobs = []
                cpu_time = {}
                uptime = {}
                prev_acct = None
                try:
                    waiting_jobs = {
                        '{}_{}'.format(batch_id, i)
                        for i in range(self.n_jobs)
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
                                    print("Job",job, "completed with status", acct['State'][jid], acct['ExitCode'][jid].split(':')[0])
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
                except:
                    print("Encountered unhandled exception. Cancelling batch job", file=sys.stderr)
                    self.backend.scancel(batch_id)
                    raise
        runtime = time.monotonic() - start_time
        job_spec = {
            str(i): {
                argname: argvalues[i]
                for argname, argvalues in self.inputs.items()
            }
            for i in range(self.n_jobs)
        }
        df = pd.DataFrame(
            data={
                job_id: {
                    'slurm_state': acct['State'][batch_id+'_'+job_id],
                    'exit_code': acct['ExitCode'][batch_id+'_'+job_id],
                    'cpu_hours': (prev_acct['CPUTimeRAW'][batch_id+'_'+job_id] + (
                        cpu_time[batch_id+'_'+job_id] if batch_id+'_'+job_id in cpu_time else 0
                    ))/3600,
                    **job_spec[job_id],
                }
                for job_id in job_spec
            }
        ).T.set_index(pd.Index([*job_spec], name='job_id')).astype({'cpu_hours': int})
        try:
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
