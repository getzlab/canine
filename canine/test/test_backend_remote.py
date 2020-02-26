import unittest
import tempfile
import os
import stat
import warnings
import subprocess
import time
from canine.backends.dummy import DummySlurmBackend
from canine.backends.remote import RemoteSlurmBackend, RemoteTransport, IgnoreKeyPolicy
import paramiko
from timeout_decorator import timeout as with_timeout

WARNING_CONTEXT = None
PROVIDER = None
TEMPDIR = None
KEYPATH = None

@with_timeout(120)
def setUpModule():
    global WARNING_CONTEXT
    global PROVIDER
    global TEMPDIR
    global KEYPATH
    WARNING_CONTEXT = warnings.catch_warnings()
    WARNING_CONTEXT.__enter__()
    warnings.simplefilter('ignore', ResourceWarning)
    PROVIDER = DummySlurmBackend(n_workers=5)
    PROVIDER.__enter__()
    TEMPDIR = tempfile.TemporaryDirectory()
    KEYPATH = os.path.join(TEMPDIR.name, 'id_rsa')
    subprocess.check_call('ssh-keygen -q -b 2048 -t rsa -f {} -N ""'.format(KEYPATH), shell=True)
    subprocess.check_call('docker exec {} mkdir -p -m 600 /root/.shh'.format(PROVIDER.controller.short_id), shell=True)
    subprocess.check_call('docker cp {}.pub {}:/root/tmpkey'.format(KEYPATH, PROVIDER.controller.short_id), shell=True)
    subprocess.check_call('docker exec {} bash -c "cat /root/tmpkey >> /root/.ssh/authorized_keys"'.format(PROVIDER.controller.short_id), shell=True)
    subprocess.check_call('docker exec {} chown root:root /root/.ssh/authorized_keys'.format(PROVIDER.controller.short_id), shell=True)

def tearDownModule():
    TEMPDIR.cleanup()
    PROVIDER.__exit__()
    WARNING_CONTEXT.__exit__()

class TestUnit(unittest.TestCase):
    """
    Runs base functionality tests of the Dummy Slurm Backend
    """

    def test_remote_invokation(self):
        """
        Tests that commands can be invoked over SSH to the remote
        """
        backend = RemoteSlurmBackend(
            'localhost',
            port=PROVIDER.port,
            key_filename=KEYPATH,
            username='root'
        )
        backend.load_config_args()
        backend.client.set_missing_host_key_policy(IgnoreKeyPolicy)
        with backend:
            exitcode, stdout, stderr = backend.invoke('ls /')
            self.assertEqual(exitcode, 0)
            self.assertEqual(
                stdout.read(),
                b'bin\nboot\nconf_templates\ncontroller.py\ndev\netc\ngcsdk\nhome'
                b'\nlib\nlib32\nlib64\nlibx32\nmedia\nmnt\nopt\nproc\nroot\nrun'
                b'\nsbin\nsrv\nsys\ntmp\nusr\nvar\nworker.sh\n'
            )

    def test_transport(self):
        """
        Tests that the file transport can function
        """
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(IgnoreKeyPolicy)
        client.connect(
            'localhost',
            port=PROVIDER.port,
            key_filename=KEYPATH,
            username='root'
        )

        with tempfile.TemporaryDirectory() as tempdir:
            with RemoteTransport(client) as transport:

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
    @with_timeout(30) # Fail the test if startup takes 2 minutes
    def setUpClass(cls):
        cls.backend = RemoteSlurmBackend(
            'localhost',
            port=PROVIDER.port,
            key_filename=KEYPATH,
            username='root'
        )
        cls.backend.load_config_args()
        cls.backend.client.set_missing_host_key_policy(IgnoreKeyPolicy)
        cls.backend.__enter__()

        _invoke = cls.backend._invoke
        def invoke_with_env(command, pty=False):
            return _invoke('SLURM_CONF=/mnt/nfs/clust_conf/slurm/slurm.conf '+command, pty)
        cls.backend._invoke = invoke_with_env
        time.sleep(5)

    @classmethod
    def tearDownClass(cls):
        cls.backend.__exit__()

    @with_timeout(60)
    def test_sinfo(self):
        sinfo = self.backend.sinfo()

        self.assertTrue('NODES' in sinfo.columns)

        self.assertEqual(
            PROVIDER.n_workers,
            sinfo.NODES.sum()
        )

        self.assertTrue('NODELIST' in sinfo.columns)
        nodes = {container.id[:12]:container for container in PROVIDER.workers}

        for nodelist in sinfo.NODELIST:
            for node in nodelist.split(','):
                self.assertTrue(node in nodes)

        time.sleep(5)
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

    @with_timeout(60)
    def test_sbatch_squeue_scancel(self):
        path = self.backend.pack_batch_script('echo start', 'sleep 10', 'echo end')
        batch_id = self.backend.sbatch(
            os.path.join('/root', path),
            array='0-9'
        )
        time.sleep(2)

        squeue = self.backend.squeue()
        self.assertTrue(len(squeue) >= 10)
        for idx in squeue.index.values:
            self.assertTrue(idx.startswith(batch_id+'_'))

        time.sleep(12)

        self.assertTrue(len(self.backend.squeue()) == 0)

        batch_id = self.backend.sbatch(
            os.path.join('/root', path),
            array='0-9'
        )
        time.sleep(2)

        squeue = self.backend.squeue()
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
        self.assertIn(hostname, {worker.id[:12] for worker in PROVIDER.workers})
