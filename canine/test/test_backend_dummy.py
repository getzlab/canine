import unittest
import tempfile
import os
import stat
import warnings
import time
from canine.backends.dummy import DummySlurmBackend, DummyTransport
import docker
import port_for
from timeout_decorator import timeout as with_timeout

WARNING_CONTEXT = None
STAGING_DIR = './travis_tmp' if 'TRAVIS' in os.environ else None

def setUpModule():
    global WARNING_CONTEXT
    WARNING_CONTEXT = warnings.catch_warnings()
    WARNING_CONTEXT.__enter__()
    warnings.simplefilter('ignore', ResourceWarning)

def tearDownModule():
    WARNING_CONTEXT.__exit__()

class TestUnit(unittest.TestCase):
    """
    Runs base functionality tests of the Dummy Slurm Backend
    """

    @classmethod
    def setUpClass(cls):
        cls.dkr = docker.from_env()

    def test_container_invokation(self):
        """
        Tests that commands can be invoked in a container
        """
        container = self.dkr.containers.run('gcr.io/broad-cga-aarong-gtex/slurmind', '/bin/bash', detach=True, tty=True)
        self.assertIsInstance(container, docker.models.containers.Container)

        callback = DummySlurmBackend.exec_run(container, 'ls')
        self.assertTrue(callable(callback))

        result = callback()
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(
            result.output,
            b'bin\nboot\nconf_templates\ncontroller.py\ndev\netc\ngcsdk\nhome'
            b'\nlib\nlib32\nlib64\nlibx32\nmedia\nmnt\nopt\nproc\nroot\nrun'
            b'\nsbin\nsrv\nsys\ntmp\nusr\nvar\nworker.sh\n'
        )

        container.stop(timeout=1)

    def test_transport(self):
        """
        Tests that the file transport can function
        """
        port = port_for.select_random()
        container = self.dkr.containers.run(
            'gcr.io/broad-cga-aarong-gtex/slurmind',
            '/bin/bash',
            detach=True,
            tty=True,
            ports={'22/tcp': port}
        )

        result = container.exec_run('service ssh start')
        self.assertEqual(result.exit_code, 0)

        with tempfile.TemporaryDirectory() as tempdir:
            with DummyTransport(tempdir, container, port) as transport:

                result = transport.listdir('/')
                self.assertIsInstance(result, list)
                self.assertListEqual(
                    sorted(result),
                    sorted([
                        '.dockerenv', 'bin', 'boot', 'conf_templates', 'controller.py',
                         'dev', 'etc', 'gcsdk', 'home', 'lib', 'lib32', 'lib64',
                         'libx32', 'media', 'mnt', 'opt', 'proc', 'root',
                         'run', 'sbin', 'srv', 'sys', 'tmp', 'usr', 'var', 'worker.sh'
                    ])
                )

                self.assertFalse(transport.exists('foo'))
                with transport.open('foo', 'wb') as w:
                    w.write(os.urandom(16))
                self.assertTrue(transport.exists('foo'))
                stat_result = transport.stat('foo')
                self.assertEqual(stat_result.st_size, 16)
                self.assertTrue(stat.S_ISREG(stat_result.st_mode))
                self.assertFalse(transport.isdir('foo'))
                self.assertTrue(transport.isfile('foo'))

                with open(os.path.join(tempdir, 'foo'), 'wb') as w:
                    with transport.open('foo', 'rb') as r:
                        transport.receive(r, w)
                self.assertTrue(os.path.exists(os.path.join(tempdir, 'foo')))
                with open(os.path.join(tempdir, 'foo'), 'rb') as r1:
                    with transport.open('foo', 'rb') as r2:
                        self.assertEqual(r1.read(), r2.read())

                transport.mkdir('bar')
                self.assertTrue(transport.exists('bar'))
                self.assertTrue(transport.isdir('bar'))
                self.assertFalse(transport.isfile('bar'))

                transport.mklink('bar', 'baz')
                self.assertTrue(transport.exists('baz'))
                stat_result = transport.stat('baz', follow_symlinks=False)
                self.assertTrue(stat.S_ISLNK(stat_result.st_mode))

                transport.sendtree(os.path.dirname(__file__), 'test')
                self.assertTrue(transport.isdir('test'))

                transport.chmod('test', 0o777)
                stat_result = transport.stat('test')
                self.assertTrue(stat_result.st_mode & stat.S_IRWXU)
                self.assertTrue(stat_result.st_mode & stat.S_IRWXG)
                self.assertTrue(stat_result.st_mode & stat.S_IRWXO)

                transport.rename('test', 'test2')
                self.assertTrue(transport.isdir('test2'))



class TestIntegration(unittest.TestCase):
    """
    Checks higher-level functions of the dummy backend
    """

    @classmethod
    @with_timeout(120) # Fail the test if startup takes 2 minutes
    def setUpClass(cls):
        cls.backend = DummySlurmBackend(n_workers=5, staging_dir=STAGING_DIR)
        cls.backend.__enter__()

    @classmethod
    def tearDownClass(cls):
        cls.backend.__exit__()

    @with_timeout(60)
    def test_sinfo(self):
        sinfo = self.backend.sinfo()

        self.assertTrue('NODES' in sinfo.columns)

        self.assertEqual(
            self.backend.n_workers,
            sinfo.NODES.sum()
        )

        self.assertTrue('NODELIST' in sinfo.columns)
        nodes = {container.id[:12]:container for container in self.backend.workers}

        for nodelist in sinfo.NODELIST:
            for node in nodelist.split(','):
                self.assertTrue(node in nodes)

        time.sleep(10)
        self.assertTrue((self.backend.sinfo().STATE == 'idle').all())

    @with_timeout(60)
    def test_sacct(self):
        self.assertEqual(
            self.backend.srun('ls')[0],
            0
        )

        time.sleep(5) # sacct sometimes delayed on dummy backend

        sacct = self.backend.sacct()

        self.assertTrue(
            len(sacct) >= 1
        )

        self.assertTrue('JobName' in sacct.columns)
        self.assertTrue('ls' in sacct.JobName.values)

        job = sacct.iloc[-1]
        self.assertEqual(job.Partition, 'slurmind')
        self.assertEqual(job.Account, 'root')
        self.assertEqual(job.AllocCPUS, '1')
        self.assertEqual(job.State, 'COMPLETED')
        self.assertEqual(job.ExitCode, '0:0')

    @with_timeout(90)
    def test_sbatch_squeue_scancel(self):
        path = self.backend.pack_batch_script('echo start', 'sleep 10', 'echo end')
        batch_id = self.backend.sbatch(
            os.path.join('/root', path),
            array='0-9'
        )
        time.sleep(5)

        squeue = self.backend.squeue('array')
        self.assertTrue(len(squeue) == 10)
        for idx in squeue.index.values:
            self.assertTrue(idx.startswith(batch_id+'_'))

        time.sleep(30)

        self.assertTrue(len(self.backend.squeue()) == 0)

        batch_id = self.backend.sbatch(
            os.path.join('/root', path),
            array='0-9'
        )
        time.sleep(5)

        squeue = self.backend.squeue('array')
        self.assertTrue(len(squeue) >= 10)
        for idx in squeue.index.values:
            self.assertTrue(idx.startswith(batch_id+'_'))

        self.backend.scancel(batch_id)
        time.sleep(2)
        self.assertTrue(len(self.backend.squeue()) == 0)

    @with_timeout(60)
    def test_srun(self):
        returncode, stdout, stderr = self.backend.srun('hostname')
        self.assertEqual(returncode, 0)
        hostname = stdout.read().decode().strip().split()[-1]
        self.assertIn(hostname, {worker.id[:12] for worker in self.backend.workers})
