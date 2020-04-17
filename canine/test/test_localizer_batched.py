import unittest
import unittest.mock
import tempfile
import os
import stat
import warnings
import time
from contextlib import contextmanager
from canine.backends.dummy import DummySlurmBackend
from canine.localization.base import Localization, PathType
from canine.localization.local import BatchedLocalizer
from timeout_decorator import timeout as with_timeout
import agutil

STAGING_DIR = './travis_tmp' if 'TRAVIS' in os.environ else None
WARNING_CONTEXT = None
BACKEND = None

@with_timeout(120)
def setUpModule():
    global WARNING_CONTEXT
    global BACKEND
    WARNING_CONTEXT = warnings.catch_warnings()
    WARNING_CONTEXT.__enter__()
    warnings.simplefilter('ignore', ResourceWarning)
    BACKEND = DummySlurmBackend(n_workers=1, staging_dir=STAGING_DIR)
    BACKEND.__enter__()

def tearDownModule():
    BACKEND.__exit__()
    WARNING_CONTEXT.__exit__()

def makefile(path, opener=open):
    with opener(path, 'w') as w:
        w.write(path)
    return path

class TestUnit(unittest.TestCase):
    """
    Tests various base features of the BatchedLocalizer
    """

    @classmethod
    @with_timeout(10) # Fail the test if startup takes 10s
    def setUpClass(cls):
        cls.localizer = BatchedLocalizer(BACKEND)
        cls.localizer.__enter__()

    @classmethod
    def tearDownClass(cls):
        cls.localizer.__exit__()

    def test_environment(self):
        local = self.localizer.environment('local')
        remote = self.localizer.environment('remote')

        self.assertIsInstance(local, dict)
        self.assertIsInstance(remote, dict)

        self.assertEqual(len({*remote} ^ {*local}), 0)

        self.assertTrue('CANINE_ROOT' in local)
        self.assertTrue('CANINE_COMMON' in local)
        self.assertTrue('CANINE_JOBS' in local)
        self.assertTrue('CANINE_OUTPUT' in local)

        for key in local:
            self.assertEqual(
                os.path.relpath(local[key], local['CANINE_ROOT']),
                os.path.relpath(remote[key], remote['CANINE_ROOT'])
            )

    def test_send_recv_tree(self):
        with tempfile.TemporaryDirectory() as tempdir:
            with self.localizer.transport_context() as transport:
                self.assertFalse(transport.isdir('/test'))
                self.localizer.sendtree(os.path.dirname(__file__), '/test', transport=transport)
                self.assertTrue(transport.isdir('/test'))

                for (ldirpath, ldirnames, lfilenames), (rdirpath, rdirnames, rfilenames) in zip(os.walk(os.path.dirname(__file__)), transport.walk('/test')):
                    if os.path.basename(ldirpath) == '__pycache__':
                        # Pycache filenames differ by python version
                        continue
                    with self.subTest(dirname=ldirpath):
                        self.assertEqual(os.path.basename(ldirpath), os.path.basename(rdirpath))
                        self.assertListEqual(sorted(ldirnames), sorted(rdirnames))
                        self.assertListEqual(sorted(lfilenames), sorted(rfilenames))

                self.assertFalse(os.path.isdir(os.path.join(tempdir, 'test')))
                self.localizer.receivetree('/test', os.path.join(tempdir, 'test'), transport=transport)
                self.assertTrue(os.path.isdir(os.path.join(tempdir, 'test')))

                for (ldirpath, ldirnames, lfilenames), (rdirpath, rdirnames, rfilenames) in zip(os.walk(os.path.join(tempdir, 'test')), transport.walk('/test')):
                    if os.path.basename(ldirpath) == '__pycache__':
                        # Pycache filenames differ by python version
                        continue
                    with self.subTest(dirname=ldirpath):
                        self.assertEqual(os.path.basename(ldirpath), os.path.basename(rdirpath))
                        self.assertListEqual(sorted(ldirnames), sorted(rdirnames))
                        self.assertListEqual(sorted(lfilenames), sorted(rfilenames))

    def test_reserve_path(self):
        for i in range(10):
            components = [
                ''.join(chunk)
                for chunk in agutil.clump(os.urandom(8).hex(), 2)
            ]
            with self.subTest(path=os.path.join(*components)):
                path = self.localizer.reserve_path(*components)

                self.assertEqual(
                    os.path.relpath(path.localpath, self.localizer.local_dir),
                    os.path.relpath(path.remotepath, self.localizer.staging_dir)
                )

    def test_localize_file(self):
        # NOTE: This test unit will need to be repeated for all localizers
        with self.localizer.transport_context() as transport:
            self.localizer.localize_file(__file__, self.localizer.reserve_path('file.py'), transport)
            self.assertTrue(os.path.isfile(os.path.join(self.localizer.local_dir, 'file.py')))

            self.localizer.localize_file(__file__, self.localizer.reserve_path('dira', 'dirb', 'file.py'), transport)
            self.assertTrue(os.path.isdir(os.path.join(self.localizer.local_dir, 'dira')))
            self.assertTrue(os.path.isdir(os.path.join(self.localizer.local_dir, 'dira', 'dirb')))
            self.assertTrue(os.path.isfile(os.path.join(self.localizer.local_dir, 'dira', 'dirb', 'file.py')))

            self.localizer.localize_file(os.path.dirname(__file__), self.localizer.reserve_path('dirc', 'dird'), transport)
            self.assertTrue(os.path.isdir(os.path.join(self.localizer.local_dir, 'dirc')))
            self.assertFalse(os.path.isdir(os.path.join(self.localizer.local_dir, 'dirc', 'dird')))
            self.assertIn(
                (os.path.dirname(__file__), os.path.join(self.localizer.reserve_path('dirc', 'dird').remotepath, os.path.basename(os.path.dirname(__file__)))),
                self.localizer.queued_batch
            )

class TestIntegration(unittest.TestCase):
    """
    Tests high-level features of the localizer
    """

    @with_timeout(10)
    def test_common_inputs(self):
        with tempfile.TemporaryDirectory() as tempdir:
            test_file = makefile(os.path.join(tempdir, 'testfile'))
            test_file2 = makefile(os.path.join(tempdir, 'testfile2'))
            with BatchedLocalizer(BACKEND) as localizer:
                localizer.localize_file = unittest.mock.MagicMock()
                with localizer.transport_context() as transport:
                    inputs = {
                        jid: {
                            'gs-common': 'gs://foo/bar',
                            'gs-incommon': 'gs://foo/'+os.urandom(8).hex(),
                            'file-common': test_file,
                            'file-common-override': test_file2,
                            'file-incommon': makefile(os.path.join(tempdir, os.urandom(8).hex())),
                            'file-incommon-override': makefile(os.path.join(tempdir, os.urandom(8).hex())),
                            'string-common': 'hey!',
                            'string-incommon': os.urandom(8).hex(),
                        }
                        for jid in range(15)
                    }

                    common_dests = localizer.pick_common_inputs(
                        inputs,
                        {
                            'file-common-override': None,
                            'file-incommon-override': 'common'
                        },
                        transport
                    )
                    self.assertIn('gs://foo/bar', localizer.common_inputs)
                    self.assertIn(test_file, localizer.common_inputs)
                    self.assertIn('gs://foo/bar', common_dests)
                    self.assertIn(test_file, common_dests)
                    for jid in inputs:
                        localizer.localize_file.assert_any_call(
                            inputs[jid]['file-incommon-override'],
                            localizer.reserve_path('common', os.path.basename(os.path.abspath(inputs[jid]['file-incommon-override']))),
                            transport=transport
                        )

    @with_timeout(10)
    def test_prepare_inputs(self):
        with tempfile.TemporaryDirectory() as tempdir:
            test_file = makefile(os.path.join(tempdir, 'testfile'))
            with BatchedLocalizer(BACKEND) as localizer:
                localizer.localize_file = unittest.mock.MagicMock()
                with localizer.transport_context() as transport:
                    inputs = {
                        jid: {
                            'gs-common': 'gs://foo/bar',
                            'gs-incommon': 'gs://foo/'+os.urandom(8).hex(),
                            'gs-stream': 'gs://foo/'+os.urandom(8).hex(),
                            'gs-download': 'gs://foo/'+os.urandom(8).hex(),
                            'file-common': test_file,
                            'file-incommon': makefile(os.path.join(tempdir, os.urandom(8).hex())),
                            'string-common': 'hey!',
                            'string-incommon': os.urandom(8).hex(),
                        }
                        for jid in range(15)
                    }

                    common_dests = {
                        test_file: localizer.reserve_path('common', os.path.basename(os.path.abspath(test_file))),
                        'gs://foo/bar': localizer.reserve_path('common', 'bar'),
                    }

                    for jid, args in inputs.items():
                        localizer.prepare_job_inputs(jid, args, common_dests, {'gs-stream': 'stream', 'gs-download': 'delayed'}, transport=transport)
                        self.assertIn(jid, localizer.inputs)
                        for k, v in args.items():
                            with self.subTest(jid=jid, input=k):
                                self.assertIn(k, localizer.inputs[jid])
                                if k == 'gs-stream':
                                    self.assertEqual(
                                        localizer.inputs[jid][k],
                                        Localization(
                                            'stream',
                                            v
                                        )
                                    )
                                elif k == 'gs-download':
                                    self.assertEqual(
                                        localizer.inputs[jid][k],
                                        Localization(
                                            'download',
                                            v
                                        )
                                    )
                                elif k.startswith('string-'):
                                    self.assertEqual(
                                        localizer.inputs[jid][k],
                                        Localization(
                                            None,
                                            v
                                        )
                                    )
                                elif k.endswith('-common'):
                                    self.assertEqual(
                                        localizer.inputs[jid][k],
                                        Localization(
                                            None,
                                            common_dests[v]
                                        )
                                    )
                                else:
                                    self.assertEqual(
                                        localizer.inputs[jid][k],
                                        Localization(
                                            None,
                                            localizer.reserve_path('jobs', jid, 'inputs', os.path.basename(os.path.abspath(v)))
                                        )
                                    )
                                    localizer.localize_file.assert_any_call(v, localizer.inputs[jid][k].path, transport=transport)
                        localizer.localize_file.reset_mock()

    @with_timeout(10)
    def test_setup_teardown(self):
        with tempfile.TemporaryDirectory() as tempdir:
            test_file = makefile(os.path.join(tempdir, 'testfile'))
            with BatchedLocalizer(BACKEND) as localizer:
                common_gs = localizer.reserve_path('common', 'bar')
                common_file = localizer.reserve_path('common', os.path.basename(os.path.abspath(test_file)))

                output_patterns = {'stdout': '../stdout', 'stderr': '../stderr', 'output-glob': '*.txt', 'output-file': 'file.tar.gz'}

                for jid in range(15):
                    with self.subTest(jid=jid):
                        localizer.inputs[str(jid)] = {
                            'gs-common': Localization(None, common_gs), # already 'localized'
                            'gs-incommon': Localization(None, localizer.reserve_path('jobs', str(jid), 'inputs', os.urandom(8).hex())), # already 'localized'
                            'gs-stream': Localization('stream', 'gs://foo/'+os.urandom(8).hex()), # check for extra tasks in setup_teardown
                            'gs-download': Localization('download', 'gs://foo/'+os.urandom(8).hex()), # check for extra tasks in setup_teardown
                            'file-common': Localization(None, common_file), # already 'localized'
                            'file-incommon': Localization(None, localizer.reserve_path('jobs', str(jid), 'inputs', os.urandom(8).hex())), # already 'localized'
                            'string-common': Localization(None, 'hey!'), # no localization. Setup teardown exports as string
                            'string-incommon': Localization(None, os.urandom(8).hex()), # no localization. Setup teardown exports as string
                        }

                        setup_text, localization_text, teardown_text = localizer.job_setup_teardown(
                            jobId=str(jid),
                            patterns=output_patterns
                        )
                        self.assertRegex(
                            setup_text,
                            r'export CANINE_JOB_VARS=(\w+-\w+:?)+'
                        )
                        self.assertIn(
                            'export CANINE_JOB_INPUTS="{}"'.format(os.path.join(localizer.environment('remote')['CANINE_JOBS'], str(jid), 'inputs')),
                            setup_text
                        )
                        self.assertIn(
                            'export CANINE_JOB_ROOT="{}"'.format(os.path.join(localizer.environment('remote')['CANINE_JOBS'], str(jid), 'workspace')),
                            setup_text
                        )
                        self.assertIn(
                            'export CANINE_JOB_SETUP="{}"'.format(os.path.join(localizer.environment('remote')['CANINE_JOBS'], str(jid), 'setup.sh')),
                            setup_text
                        )
                        self.assertIn(
                            'export CANINE_JOB_TEARDOWN="{}"'.format(os.path.join(localizer.environment('remote')['CANINE_JOBS'], str(jid), 'teardown.sh')),
                            setup_text
                        )
                        for arg, value in localizer.inputs[str(jid)].items():
                            with self.subTest(arg=arg, value=value.path):
                                path = value.path
                                if value.type == 'stream':
                                    src = path
                                    path = localizer.reserve_path('jobs', str(jid), 'inputs', os.path.basename(os.path.abspath(src))).remotepath
                                    self.assertIn(
                                        'if [[ -e {dest} ]]; then rm {dest}; fi\n'
                                        'mkfifo {dest}\n'
                                        'gsutil  cat {src} > {dest} &'.format(
                                            src=src,
                                            dest=path
                                        ),
                                        localization_text
                                    )
                                elif value.type == 'download':
                                    src = path
                                    path = localizer.reserve_path('jobs', str(jid), 'inputs', os.path.basename(os.path.abspath(src))).remotepath
                                    self.assertIn(
                                        'if [[ ! -e {dest}.fin ]]; then gsutil  '
                                        '-o GSUtil:check_hashes=if_fast_else_skip'
                                        ' cp {src} {dest} && touch {dest}.fin'.format(
                                            src=src,
                                            dest=path
                                        ),
                                        localization_text
                                    )
                                if isinstance(path, PathType):
                                    path = path.remotepath
                                self.assertRegex(
                                    setup_text,
                                    r'export {}=[\'"]?{}[\'"]?'.format(arg, path)
                                )
                        for name, pattern in output_patterns.items():
                            with self.subTest(output=name, pattern=pattern):
                                self.assertTrue(
                                    ('-p {} {}'.format(name, pattern) in teardown_text) or
                                    ("-p {} '{}'".format(name, pattern) in teardown_text) or
                                    ('-p {} "{}"'.format(name, pattern) in teardown_text)
                                )

    @with_timeout(30)
    def test_localize_delocalize(self):
        """
        This is the full integration test.
        It checks that the localizer is able to replicate the expected directory
        structure on the remote cluster and that it delocalizes files es expected
        afterwards
        """
        with tempfile.TemporaryDirectory() as tempdir:
            test_file = makefile(os.path.join(tempdir, 'testfile'))
            with BatchedLocalizer(BACKEND) as localizer:
                inputs = {
                    str(jid): {
                        # no gs:// files; We don't want to actually download anything
                        'gs-stream': 'gs://foo/'+os.urandom(8).hex(),
                        'gs-download': 'gs://foo/'+os.urandom(8).hex(),
                        'file-common': test_file,
                        'file-incommon': makefile(os.path.join(tempdir, os.urandom(8).hex())),
                        'string-common': 'hey!',
                        'string-incommon': os.urandom(8).hex(),
                    }
                    for jid in range(15)
                }

                output_patterns = {'stdout': '../stdout', 'stderr': '../stderr', 'output-glob': '*.txt', 'output-file': 'file.tar.gz'}

                staging_dir = localizer.localize(inputs, output_patterns, {'gs-stream': 'stream', 'gs-download': 'delayed'})
                with localizer.transport_context() as transport:
                    self.assertTrue(transport.isdir(staging_dir))
                    self.assertTrue(transport.isfile(os.path.join(staging_dir, 'delocalization.py')))

                    self.assertTrue(transport.isdir(os.path.join(staging_dir, 'common')))
                    self.assertTrue(transport.isfile(os.path.join(staging_dir, 'common', 'testfile')))

                    self.assertTrue(transport.isdir(os.path.join(staging_dir, 'jobs')))
                    contents = transport.listdir(os.path.join(staging_dir, 'jobs'))
                    for jid in range(15):
                        self.assertIn(str(jid), contents)
                        self.assertTrue(transport.isdir(os.path.join(staging_dir, 'jobs', str(jid))))
                        self.assertTrue(transport.isfile(os.path.join(staging_dir, 'jobs', str(jid), 'setup.sh')))
                        self.assertTrue(transport.isfile(os.path.join(staging_dir, 'jobs', str(jid), 'teardown.sh')))

                        self.assertTrue(transport.isdir(os.path.join(staging_dir, 'jobs', str(jid), 'inputs')))
                        self.assertTrue(transport.isfile(os.path.join(staging_dir, 'jobs', str(jid), 'inputs', os.path.basename(inputs[str(jid)]['file-incommon']))))

                    self.assertTrue(transport.isdir(os.path.join(staging_dir, 'outputs')))

                    for jid in range(15):
                        makefile(os.path.join(staging_dir, 'jobs', str(jid), 'stdout'), transport.open)
                        makefile(os.path.join(staging_dir, 'jobs', str(jid), 'stderr'), transport.open)

                        transport.mkdir(os.path.join(staging_dir, 'jobs', str(jid), 'workspace'))

                        makefile(os.path.join(staging_dir, 'jobs', str(jid), 'workspace', 'file1.txt'), transport.open)
                        makefile(os.path.join(staging_dir, 'jobs', str(jid), 'workspace', 'file2.txt'), transport.open)
                        makefile(os.path.join(staging_dir, 'jobs', str(jid), 'workspace', 'file3.txt'), transport.open)

                        makefile(os.path.join(staging_dir, 'jobs', str(jid), 'workspace', 'file.tar.gz'), transport.open)

                        self.assertFalse(localizer.backend.invoke(os.path.join(staging_dir, 'jobs', str(jid), 'teardown.sh'))[0])

                    # man check
                    for jid in range(15):
                        self.assertTrue(transport.isdir(os.path.join(staging_dir, 'outputs', str(jid))))
                        self.assertTrue(transport.isfile(os.path.join(staging_dir, 'outputs', str(jid), 'stdout')))
                        self.assertTrue(transport.isfile(os.path.join(staging_dir, 'outputs', str(jid), 'stderr')))

                        self.assertTrue(transport.isdir(os.path.join(staging_dir, 'outputs', str(jid), 'output-file')))
                        self.assertTrue(transport.isfile(os.path.join(staging_dir, 'outputs', str(jid), 'output-file', 'file.tar.gz')))

                        self.assertTrue(transport.isdir(os.path.join(staging_dir, 'outputs', str(jid), 'output-glob')))
                        self.assertTrue(transport.isfile(os.path.join(staging_dir, 'outputs', str(jid), 'output-glob', 'file1.txt')))
                        self.assertTrue(transport.isfile(os.path.join(staging_dir, 'outputs', str(jid), 'output-glob', 'file2.txt')))
                        self.assertTrue(transport.isfile(os.path.join(staging_dir, 'outputs', str(jid), 'output-glob', 'file3.txt')))

                    outputs = localizer.delocalize(output_patterns, os.path.join(tempdir, 'outputs'))

                    for dirpath, dirnames, filenames in os.walk(os.path.join(tempdir, 'outputs')):
                        rdirpath = os.path.join(
                            staging_dir,
                            os.path.relpath(dirpath, tempdir)
                        )
                        self.assertTrue(transport.isdir(rdirpath))
                        for d in dirnames:
                            self.assertTrue(transport.isdir(os.path.join(rdirpath, d)))
                        for f in filenames:
                            self.assertTrue(transport.isfile(os.path.join(rdirpath, f)))

                    for jid in range(15):
                        jid = str(jid)
                        self.assertIn(jid, outputs)
                        self.assertIsInstance(outputs[jid], dict)

                        self.assertIn('stdout', outputs[jid])
                        self.assertIsInstance(outputs[jid]['stdout'], list)
                        self.assertListEqual(
                            outputs[jid]['stdout'],
                            [os.path.join(tempdir, 'outputs', jid, 'stdout')]
                        )

                        self.assertIn('stderr', outputs[jid])
                        self.assertIsInstance(outputs[jid]['stderr'], list)
                        self.assertListEqual(
                            outputs[jid]['stderr'],
                            [os.path.join(tempdir, 'outputs', jid, 'stderr')]
                        )

                        self.assertIn('output-file', outputs[jid])
                        self.assertIsInstance(outputs[jid]['output-file'], list)
                        self.assertListEqual(
                            outputs[jid]['output-file'],
                            [os.path.join(tempdir, 'outputs', jid, 'output-file', 'file.tar.gz')]
                        )

                        self.assertIn('output-glob', outputs[jid])
                        self.assertIsInstance(outputs[jid]['output-glob'], list)
                        self.assertListEqual(
                            sorted(outputs[jid]['output-glob']),
                            sorted([
                                os.path.join(tempdir, 'outputs', jid, 'output-glob', 'file{}.txt'.format(i))
                                for i in range(1, 4)
                            ])
                        )
