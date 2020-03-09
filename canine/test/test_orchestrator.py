import unittest
import unittest.mock
import tempfile
import os
import stat
import warnings
import time
import re
import subprocess
from multiprocessing import cpu_count
from contextlib import contextmanager
from canine.backends.dummy import DummySlurmBackend
from canine.orchestrator import Orchestrator, version
from timeout_decorator import timeout as with_timeout
import pandas as pd
import yaml

STAGING_DIR = './travis_tmp' if 'TRAVIS' in os.environ else None
WARNING_CONTEXT = None

def setUpModule():
    global WARNING_CONTEXT
    WARNING_CONTEXT = warnings.catch_warnings()
    WARNING_CONTEXT.__enter__()
    warnings.simplefilter('ignore', ResourceWarning)

def tearDownModule():
    WARNING_CONTEXT.__exit__()

def gpu_test_available():
    try:
        subprocess.check_call(
            'docker run --rm -i --runtime nvidia gcr.io/broad-cga-aarong-gtex/slurmind ls',
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        return True
    except subprocess.CalledProcessError:
        return False

class TestUnit(unittest.TestCase):
    """
    Tests base functions of orchestrator
    """

    @classmethod
    @with_timeout(120)
    def setUpClass(cls):
        cls.orchestrator = Orchestrator({
            'name': 'canine-unittest',
            'backend': {
                'type': 'Dummy',
                'n_workers': 1,
                'staging_dir': STAGING_DIR
            },
            'inputs': {
                'jobIndex': [0, 1, 2, 3, 4],
                'common_file': __file__
            },
            'script': ['echo $(hostname) $jobIndex $common_file', 'touch f1.txt f2.txt f3.txt'],
            'localization': {
                'strategy': 'Batched',
                'staging_dir': '/mnt/nfs/canine'
            },
            'outputs': {
                'output-glob': ('*.txt', lambda x: len(x))
            }
        })
        cls.backend = cls.orchestrator.backend.__enter__()
        cls.localizer = cls.orchestrator._localizer_type(cls.orchestrator.backend, **cls.orchestrator.localizer_args).__enter__()

    @classmethod
    def tearDownClass(cls):
        cls.localizer.__exit__()
        cls.backend.__exit__()

    @with_timeout(20)
    def test_localization(self):
        entrypoint = self.orchestrator.localize_inputs_and_script(self.localizer)
        with self.backend.transport() as transport:
            self.assertTrue(transport.isdir(os.path.dirname(entrypoint)))
            self.assertTrue(transport.isfile(entrypoint))
            with transport.open(entrypoint, 'r') as r:
                self.assertEqual(
                    r.read(),
                    (
                        '#!/bin/bash\n'
                        'export CANINE="{version}"\n'
                        'export CANINE_BACKEND="Dummy"\n'
                        'export CANINE_ADAPTER="Manual"\n'
                        'export CANINE_ROOT="/mnt/nfs/canine"\n'
                        'export CANINE_COMMON="/mnt/nfs/canine/common"\n'
                        'export CANINE_OUTPUT="/mnt/nfs/canine/outputs"\n'
                        'export CANINE_JOBS="/mnt/nfs/canine/jobs"\n'
                        'source $CANINE_JOBS/$SLURM_ARRAY_TASK_ID/setup.sh\n'
                        '/mnt/nfs/canine/script.sh\n'
                        'CANINE_JOB_RC=$?\n'
                        'source $CANINE_JOBS/$SLURM_ARRAY_TASK_ID/teardown.sh\n'
                        'exit $CANINE_JOB_RC\n'.format(version=version)
                    )
                )
            stat_result = transport.stat('/mnt/nfs/canine/entrypoint.sh')
            self.assertTrue(stat_result.st_mode & stat.S_IRWXU)
            self.assertTrue(stat_result.st_mode & stat.S_IRWXG)
            self.assertTrue(stat_result.st_mode & (stat.S_IXOTH | stat.S_IROTH))

            self.assertTrue(transport.isfile('/mnt/nfs/canine/script.sh'))
            with transport.open('/mnt/nfs/canine/script.sh', 'r') as r:
                self.assertEqual(
                    r.read(),
                    (
                        '#!/bin/bash\n'
                        'echo $(hostname) $jobIndex $common_file\n'
                        'touch f1.txt f2.txt f3.txt\n'
                    )
                )
            stat_result = transport.stat('/mnt/nfs/canine/script.sh')
            self.assertTrue(stat_result.st_mode & stat.S_IRWXU)
            self.assertTrue(stat_result.st_mode & stat.S_IRWXG)
            self.assertTrue(stat_result.st_mode & (stat.S_IXOTH | stat.S_IROTH))


    @with_timeout(75)
    def test_wait_for_jobs(self):
        path = self.backend.pack_batch_script('echo start', 'sleep 10', 'echo end')
        batch_id = self.backend.sbatch(
            os.path.join('/root', path),
            array='0-{}'.format(len(self.orchestrator.job_spec) - 1)
        )
        jobs, cpu_time, uptime, acct = self.orchestrator.wait_for_jobs_to_finish(batch_id)
        self.assertListEqual(
            sorted(jobs),
            sorted([(str(i), '{}_{}'.format(batch_id, i)) for i in range(len(self.orchestrator.job_spec))])
        )
        self.assertTrue((acct['CPUTimeRAW'] >= 10).all())

    @with_timeout(20)
    def test_make_output_df(self):
        path = self.backend.pack_batch_script('echo hello, world')
        batch_id = self.backend.sbatch(
            os.path.join('/root', path),
            array='0-{}'.format(len(self.orchestrator.job_spec) - 1)
        )
        time.sleep(5)

        acct = self.backend.sacct(job=batch_id)
        key = '{}_4'.format(batch_id)
        # account for sacct delays in dummy backend
        while key not in acct.index:
            time.sleep(2)
            acct = self.backend.sacct(job=batch_id)

        df = self.orchestrator.make_output_DF(
            batch_id,
            {
                str(jid):{
                    'output-glob': ['f1.txt', 'f2.txt', 'f3.txt']
                }
                for jid in range(len(self.orchestrator.job_spec))
            },
            {},
            pd.DataFrame.from_dict(
                data={
                    '{}_{}'.format(batch_id, str(jid)): {
                        'State': 'COMPLETED',
                        'ExitCode': '0:0',
                        'CPUTimeRAW': 5
                    }
                    for jid in range(len(self.orchestrator.job_spec))
                },
                orient='index'
            ).rename_axis(index = 'JobID')
        )

        for jid in self.orchestrator.job_spec:
            self.assertIn(str(jid), df.index.values)
            self.assertEqual(
                jid,
                df[('inputs', 'jobIndex')][jid]
            )

        self.assertTrue((df[('job', 'slurm_state')] == 'COMPLETED').all())
        self.assertTrue((df[('job', 'exit_code')] == '0:0').all())
        self.assertTrue((df[('job', 'cpu_seconds')] == 5).all())
        self.assertTrue((df[('inputs', 'common_file')] == __file__).all())
        self.assertTrue((df[('outputs', 'output-glob')] == 3).all())

    @with_timeout(20)
    def test_job_submit(self):
        path = self.backend.pack_batch_script('echo start', 'sleep 60', 'echo end')
        batch_id = self.orchestrator.submit_batch_job(
            os.path.join('/root/', path),
            {
                'CANINE_JOBS': ''
            },
            {
                'output': '/root/test-%a-out',
                'error': '/root/test-%a-err'
            }
        )
        time.sleep(15)

        squeue = self.backend.squeue()
        queued_ids = set()

        pattern = re.compile(r'{}_\[(\d+)\-(\d+)\]'.format(batch_id))
        for idx in squeue.index:
            if pattern.match(idx):
                match = pattern.match(idx)
                for i in range(int(match.group(1)), int(match.group(2))+1):
                    queued_ids.add(i)

        for jid in range(len(self.orchestrator.job_spec)):
            self.assertTrue(
                ('{}_{}'.format(batch_id, jid) in squeue.index.values) or
                ('{}_[{}]'.format(batch_id, jid) in squeue.index.values) or
                jid in queued_ids
            )
        self.backend.scancel(batch_id)

    @unittest.skip("Skipping Job Avoidance Test. Waiting for julianhess/develop to merge")
    def test_job_avoidance(self):
        pass


class TestIntegration(unittest.TestCase):
    """
    Runs integration tests using full example pipelines
    """

    @with_timeout(180)
    def test_cmd(self):
        """
        Runs the example pipeline, but again from the commandline
        """
        with tempfile.TemporaryDirectory() as tempdir:
            subprocess.check_call(
                'canine examples/example_pipeline.yaml --backend type:Dummy --backend n_workers:1 {} --localization staging_dir:/mnt/nfs/canine --output-dir {}'.format(
                    '--backend staging_dir:{}'.format(STAGING_DIR) if STAGING_DIR is not None else '',
                    tempdir
                ),
                shell=True
            )

    @with_timeout(180)
    def test_xargs(self):
        """
        Runs a quick test of the xargs orchestrator
        """
        with DummySlurmBackend(n_workers=1, staging_dir=STAGING_DIR) as backend:
            with backend.transport() as transport:
                transport.sendtree(
                    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                    '/opt/canine'
                )
            rc, stdout, stderr = backend.invoke(
                "bash -c 'pip3 install -e /opt/canine && pip3 install pyopenssl; echo foo | canine-xargs -d /mnt/nfs echo @'"
            )
            self.assertFalse(rc)
            time.sleep(5)
            sacct = backend.sacct()
            self.assertIn(
                '2_0',
                sacct.index.values
            )
            self.assertTrue(
                (sacct['ExitCode'] == '0:0').all()
            )
            with backend.transport() as transport:
                self.assertTrue(transport.isfile('/mnt/nfs/2.0.stdout'))
                with transport.open('/mnt/nfs/2.0.stdout', 'r') as r:
                    self.assertEqual(
                        r.read().strip(),
                        'foo'
                    )


    @with_timeout(180)
    def test_example_pipeline(self):
        """
        The example pipeline from the examples directory
        """
        with tempfile.TemporaryDirectory() as tempdir:
            output_dir = os.path.join(tempdir, 'outputs')
            with open(os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                'examples',
                'example_pipeline.yaml'
            )) as r:
                pipeline = yaml.load(r)

            o = Orchestrator({
                **pipeline,
                **{
                    'backend': {
                        'type': 'Dummy',
                        'n_workers': 1,
                        'staging_dir': STAGING_DIR
                    },
                    'localization': {
                        'staging_dir': '/mnt/nfs/canine'
                    }
                }
            })

            df = o.run_pipeline(output_dir)

            self.assertTrue((df[('job', 'exit_code')] == '0:0').all())
            for i, row in df.iterrows():
                self.assertTrue(os.path.isfile(row[('outputs', 'stdout')]))
                self.assertTrue(os.path.isfile(row[('outputs', 'stderr')]))

            self.assertEqual(len(df), len(o.job_spec))

    @unittest.skipIf(not gpu_test_available(), "nvidia runtime is not available")
    @with_timeout(180)
    def test_gpu_pipeline(self):
        """
        The gpu pipeline from the examples directory
        """
        with tempfile.TemporaryDirectory() as tempdir:
            output_dir = os.path.join(tempdir, 'outputs')
            with open(os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                'examples',
                'gpu.yaml'
            )) as r:
                pipeline = yaml.load(r)

            o = Orchestrator({
                **pipeline,
                **{
                    'backend': {
                        'type': 'Dummy',
                        'n_workers': 1,
                        'staging_dir': STAGING_DIR
                    },
                    'localization': {
                        'staging_dir': '/mnt/nfs/canine'
                    }
                }
            })

            df = o.run_pipeline(output_dir)

            self.assertTrue((df[('job', 'exit_code')] == '0:0').all())
            for i, row in df.iterrows():
                self.assertTrue(os.path.isfile(row[('outputs', 'stdout')]))
                self.assertTrue(os.path.isfile(row[('outputs', 'stderr')]))

            self.assertEqual(len(df), len(o.job_spec))

    @with_timeout(300)
    def test_big_pipeline(self):
        """
        Tests a large-scale pipeline
        """
        with tempfile.TemporaryDirectory() as tempdir:
            output_dir = os.path.join(tempdir, 'outputs')
            o = Orchestrator({
                'adapter': {'product': True},
                'inputs': {
                    'i': [*range(25)],
                    'j': [*range(25)],
                },
                'script': ['python3 -c "[print(i,j) for i in range($i) for j in range($j)]"'],
                'backend': {
                    'type': 'Dummy',
                    'n_workers': cpu_count(),
                    'cpus': 1,
                    'staging_dir': STAGING_DIR
                },
                'resources': {
                    'cpus-per-task': 1,
                    'mem-per-cpu': '256M'
                },
                'localization': {
                    'staging_dir': '/mnt/nfs/canine'
                }
            })

            df = o.run_pipeline(output_dir)

            self.assertTrue((df[('job', 'exit_code')] == '0:0').all())
            for i, row in df.iterrows():
                self.assertTrue(os.path.isfile(row[('outputs', 'stdout')]))
                self.assertTrue(os.path.isfile(row[('outputs', 'stderr')]))

            self.assertEqual(len(df), len(o.job_spec))
