import abc
import os
import sys
import warnings
import typing
import fnmatch
import shlex
import tempfile
import subprocess
import traceback
import shutil
import crayons
from uuid import uuid4
from collections import namedtuple
from contextlib import ExitStack, contextmanager
from ..backends import AbstractSlurmBackend, AbstractTransport, LocalSlurmBackend
from ..utils import get_default_gcp_project, check_call
from hound.client import _getblob_bucket
from agutil import status_bar

Localization = namedtuple("Localization", ['type', 'path'])
# types: stream, download, None
# indicates what kind of action needs to be taken during job startup

PathType = namedtuple(
    'PathType',
    ['localpath', 'controllerpath', 'computepath']
)

class AbstractLocalizer(abc.ABC):
    """
    Base class for localization.
    """
    requester_pays = {}

    def __init__(self, backend: AbstractSlurmBackend, transfer_bucket: typing.Optional[str] = None, common: bool = True, staging_dir: str = None, mount_path: str = None, localize_gs: bool = None, project : typing.Optional[str] = None):
        """
        Initializes the Localizer using the given transport.
        Localizer assumes that the SLURMBackend is connected and functional during
        the localizer's entire life cycle.
        If staging_dir is not provided, a random directory is chosen
        """
        if localize_gs is not None:
            warnings.warn("localize_gs option removed and ignored. Use overrides to explicitly control this behavior")
        self.transfer_bucket = transfer_bucket
        if transfer_bucket is not None and self.transfer_bucket.startswith('gs://'):
            self.transfer_bucket = self.transfer_bucket[5:]
        self.backend = backend
        self.common = common
        self.common_inputs = set()
        self.__sbcast = False
        if staging_dir == 'SBCAST':
            # FIXME: This doesn't actually do anything yet
            # If sbcast is true, then localization needs to use backend.sbcast to move files to the remote system
            # Not sure at all how delocalization would work
            self.__sbcast = True
            staging_dir = None
        self.staging_dir = staging_dir if staging_dir is not None else str(uuid4())
        self._local_dir = tempfile.TemporaryDirectory()
        self.local_dir = self._local_dir.name
        with self.backend.transport() as transport:
            self.mount_path = transport.normpath(mount_path if mount_path is not None else self.staging_dir)
        self.inputs = {} # {jobId: {inputName: (handle type, handle value)}}
        self.clean_on_exit = True
        self.project = project if project is not None else get_default_gcp_project()

    def get_requester_pays(self, path: str) -> bool:
        """
        Returns True if the requested gs:// object or bucket resides in a
        requester pays bucket
        """
        if path.startswith('gs://'):
            path = path[5:]
        bucket = path.split('/')[0]
        if bucket not in self.requester_pays:
            command = 'gsutil ls gs://{}'.format(path)
            try:
                # We check on the remote host because scope differences may cause
                # a requester pays bucket owned by this account to require -u on the controller
                # better safe than sorry
                rc, sout, serr = self.backend.invoke(command)
                self.requester_pays[bucket] = len([line for line in serr.readlines() if b'requester pays bucket but no user project provided' in line]) >= 1
            except subprocess.CalledProcessError:
                pass
        return bucket in self.requester_pays and self.requester_pays[bucket]

    @contextmanager
    def transport_context(self, transport: typing.Optional[AbstractTransport] = None) -> typing.ContextManager[AbstractTransport]:
        """
        Opens a file transport in the context.
        If an existing transport is provided, it is passed through
        If None (default) is provided, a new transport is opened, and closed after the context
        """
        with ExitStack() as stack:
            if transport is None:
                transport = stack.enter_context(self.backend.transport())
            yield transport

    def environment(self, location: str) -> typing.Dict[str, str]:
        """
        Returns environment variables relative to the given location.
        Location must be one of {"local", "controller", "compute"}
        """
        if location not in {"local", "controller", "compute"}:
            raise ValueError('location must be one of {"local", "controller", "compute"}')
        if location == 'local':
            return {
                'CANINE_ROOT': self.local_dir,
                'CANINE_COMMON': os.path.join(self.local_dir, 'common'),
                'CANINE_OUTPUT': os.path.join(self.local_dir, 'outputs'), #outputs/jobid/outputname/...files...
                'CANINE_JOBS': os.path.join(self.local_dir, 'jobs'),
            }
        elif location == "controller":
            return {
                'CANINE_ROOT': self.staging_dir,
                'CANINE_COMMON': os.path.join(self.staging_dir, 'common'),
                'CANINE_OUTPUT': os.path.join(self.staging_dir, 'outputs'), #outputs/jobid/outputname/...files...
                'CANINE_JOBS': os.path.join(self.staging_dir, 'jobs'),
            }
        elif location == "compute":
            return {
                'CANINE_ROOT': self.mount_path,
                'CANINE_COMMON': os.path.join(self.mount_path, 'common'),
                'CANINE_OUTPUT': os.path.join(self.mount_path, 'outputs'),
                'CANINE_JOBS': os.path.join(self.mount_path, 'jobs'),
            }
    def gs_dircp(self, src: str, dest: str, context: str, transport: typing.Optional[AbstractTransport] = None):
        """
        gs_copy for directories
        context must be one of {'local', 'remote'}, which specifies
        where the command should be run
        When uploading to gs://, the destination gs:// director does not have to exist
        When downloading from gs:// the destination directory does not have to exist,
        but its parent does
        """
        assert context in {'local', 'remote'}
        gs_obj = src if src.startswith('gs://') else dest
        if not dest.startswith('gs://'):
            # Download
            if context == 'remote':
                with self.transport_context(transport) as transport:
                    if not transport.exists(dest):
                        transport.makedirs(dest)
            else:
                if not os.path.exists(dest):
                    os.makedirs(dest)
        if not src.startswith('gs://'):
            # Upload
            # Fix empty dirs by touching a file
            # Won't fix nested directory structure containing empty dirs,
            # but it seems inefficient to walk and touch directories
            if context == 'remote':
                self.backend.invoke('touch {}/.canine_dir_marker'.format(src))
            else:
                subprocess.run(['touch', '{}/.canine_dir_marker'.format(src)])
        command = "gsutil -m -o GSUtil:check_hashes=if_fast_else_skip -o GSUtil:parallel_composite_upload_threshold=150M {} cp -r {} {}".format(
            '-u {}'.format(self.project) if self.get_requester_pays(gs_obj) else '',
            src,
            dest
        )
        if context == 'remote':
            # Invoke interactively
            rc, sout, serr = self.backend.invoke(command, True)
            check_call(command, rc, sout, serr)
        else:
            subprocess.check_call(command, shell=True)
        if not dest.startswith('gs://'):
            # Clean up .dir file
            if context == 'remote':
                self.backend.invoke('rm -f {}/*/.canine_dir_marker'.format(dest))
            else:
                subprocess.run(['rm', '-f', '{}/*/.canine_dir_marker'.format(dest)])

    def gs_copy(self, src: str, dest: str, context: str):
        """
        Copy a google storage (gs://) object
        context must be one of {'local', 'remote'}, which specifies
        where the command should be run
        When uploading to gs://, the destination gs:// directory does not have to exist
        When downloading from gs:// the destination parent directory must exist
        """
        assert context in {'local', 'remote'}
        gs_obj = src if src.startswith('gs://') else dest
        try:
            components = gs_obj[5:].split('/')
            bucket = _getblob_bucket(None, components[0], None)
            blobs = {
                blob.name
                for page in bucket.list_blobs(prefix='/'.join(components[1:]), fields='items/name,nextPageToken').pages
                for blob in page
            }
            if len([blob for blob in blobs if blob == '/'.join(components[1:])]) == 0:
                # This is a directory
                print("Copying directory:", gs_obj)
                return self.gs_dircp(src, os.path.dirname(dest), context)
        except:
            # If there is an exception, or the above condition is false
            # Procede as a regular gs_copy
            traceback.print_exc()

        command = "gsutil -o GSUtil:check_hashes=if_fast_else_skip -o GSUtil:parallel_composite_upload_threshold=150M {} cp {} {}".format(
            '-u {}'.format(self.project) if self.get_requester_pays(gs_obj) else '',
            src,
            dest
        )
        if context == 'remote':
            rc, sout, serr = self.backend.invoke(command, True)
            check_call(command, rc, sout, serr)
        else:
            subprocess.check_call(command, shell=True)

    def sendtree(self, src: str, dest: str, transport: typing.Optional[AbstractTransport] = None, exist_okay=False):
        """
        Transfers the given local folder to the given remote destination.
        Source must be a local folder, and destination must not exist
        """
        if isinstance(self.backend, LocalSlurmBackend):
            if exist_okay:
                print("exist_okay not supported by LocalSlurmBackend", file=sys.stderr)
            return shutil.copytree(src, dest)
        with self.transport_context(transport) as transport:
            if not os.path.isdir(src):
                raise ValueError("Not a directory: "+src)
            if transport.exists(dest) and not exist_okay:
                raise ValueError("Destination already exists: "+dest)
            dest = transport.normpath(dest)
            if self.transfer_bucket is not None:
                print("Transferring through bucket", self.transfer_bucket)
                path = os.path.join(str(uuid4()), os.path.basename(dest))
                self.gs_dircp(
                    src,
                    'gs://{}/{}'.format(self.transfer_bucket, path),
                    'local',
                    transport=transport
                )
                if not transport.isdir(os.path.dirname(dest)):
                    transport.makedirs(os.path.dirname(dest))
                try:
                    self.gs_dircp(
                        'gs://{}/{}'.format(self.transfer_bucket, path),
                        os.path.dirname(dest),
                        'remote',
                        transport=transport
                    )
                except:
                    print(
                        crayons.red("ERROR:", bold=True),
                        "Failed to download the data on the remote system."
                        "Your files are still saved in",
                        'gs://{}/{}'.format(self.transfer_bucket, path),
                        file=sys.stderr
                    )
                    raise
                cmd = "gsutil -m {} rm -r gs://{}/{}".format(
                    '-u {}'.format(self.project) if self.get_requester_pays(self.transfer_bucket) else '',
                    self.transfer_bucket,
                    os.path.dirname(path),
                )
                print(cmd)
                subprocess.check_call(
                    cmd,
                    shell=True
                )
            else:
                print("Transferring directly over SFTP")
                transport.sendtree(src, dest)

    def receivetree(self, src: str, dest: str, transport: typing.Optional[AbstractTransport] = None, exist_okay=False):
        """
        Transfers the given remote folder to the given local destination.
        Source must be a remote folder, and dest must not exist
        """
        if isinstance(self.backend, LocalSlurmBackend):
            if exist_okay:
                print("exist_okay not supported by LocalSlurmBackend", file=sys.stderr)
            return shutil.copytree(src, dest)
        with self.transport_context(transport) as transport:
            if not transport.isdir(src):
                raise ValueError("Not a directory: "+src)
            if os.path.exists(dest) and not exist_okay:
                raise ValueError("Destination already exists: "+dest)
            dest = os.path.abspath(dest)
            if self.transfer_bucket is not None:
                print("Transferring through bucket", self.transfer_bucket)
                path = os.path.join(str(uuid4()), os.path.basename(dest))
                self.gs_dircp(
                    src,
                    'gs://{}/{}'.format(self.transfer_bucket, path),
                    'remote',
                    transport=transport
                )
                if not os.path.isdir(os.path.dirname(dest)):
                    os.makedirs(os.path.dirname(dest))
                try:
                    self.gs_dircp(
                        'gs://{}/{}'.format(self.transfer_bucket, path),
                        os.path.dirname(dest),
                        'local',
                        transport=transport
                    )
                except:
                    print(
                        crayons.red("ERROR:", bold=True),
                        "Failed to download the data on the local system."
                        "Your files are still saved in",
                        'gs://{}/{}'.format(self.transfer_bucket, path),
                        file=sys.stderr
                    )
                    raise
                cmd = "gsutil -m {} rm -r gs://{}/{}".format(
                    '-u {}'.format(self.project) if self.get_requester_pays(self.transfer_bucket) else '',
                    self.transfer_bucket,
                    os.path.dirname(path),
                )
                print(cmd)
                subprocess.check_call(
                    cmd,
                    shell=True
                )
            else:
                print("Transferring directly over SFTP")
                transport.receivetree(src, dest)

    def reserve_path(self, *args: typing.Any) -> PathType:
        """
        Takes any number of path components, relative to the CANINE_ROOT directory
        Returns a PathType object which can be used to view the path relative to
        local, compute, or controller contexts
        """
        return PathType(
            os.path.join(self.environment('local')['CANINE_ROOT'], *(str(component) for component in args)),
            os.path.join(self.environment('controller')['CANINE_ROOT'], *(str(component) for component in args)),
            os.path.join(self.environment('compute')['CANINE_ROOT'], *(str(component) for component in args))
        )

    def delocalize(self, patterns: typing.Dict[str, str], output_dir: str = 'canine_output') -> typing.Dict[str, typing.Dict[str, str]]:
        """
        Delocalizes output from all jobs
        """
        self.receivetree(
            self.environment('controller')['CANINE_OUTPUT'],
            output_dir,
            exist_okay=True
        )
        output_files = {}
        for jobId in os.listdir(output_dir):
            start_dir = os.path.join(output_dir, jobId)
            if not os.path.isdir(start_dir):
                continue
            output_files[jobId] = {}
            for dirpath, dirnames, filenames in os.walk(start_dir):
                for name, pattern in patterns.items():
                    for filename in filenames:
                        fullname = os.path.join(dirpath, filename)
                        if fnmatch.fnmatch(fullname, pattern) or fnmatch.fnmatch(os.path.relpath(fullname, start_dir), pattern):
                            if name not in output_files[jobId]:
                                output_files[jobId][name] = [os.path.abspath(fullname)]
                            else:
                                output_files[jobId][name].append(os.path.abspath(fullname))
        return output_files

    def pick_common_inputs(self, inputs: typing.Dict[str, typing.Dict[str, str]], overrides: typing.Dict[str, typing.Optional[str]], transport: typing.Optional[AbstractTransport] = None) -> typing.Dict[str, str]:
        """
        Scans input configuration and overrides to choose inputs which should be treated as common.
        Returns the dictionary of common inputs {input path: common path}
        """
        with self.transport_context(transport) as transport:
            self.common_inputs = set()
            seen = set()
            for jobId, values in inputs.items():
                for arg, path in values.items():
                    if path in seen and (arg not in overrides or overrides[arg] == 'common'):
                        self.common_inputs.add(path)
                    if arg in overrides and overrides[arg] == 'common':
                        self.common_inputs.add(path)
                    seen.add(path)
            common_dests = {}
            for path in self.common_inputs:
                if path.startswith('gs://') or os.path.exists(path):
                    common_dests[path] = self.reserve_path('common', os.path.basename(os.path.abspath(path)))
                    self.localize_file(path, common_dests[path], transport=transport)
                else:
                    print("Could not handle common file", path, file=sys.stderr)
            return {key: value for key, value in common_dests.items()}

    def finalize_staging_dir(self, jobs: typing.Iterable[str], transport: typing.Optional[AbstractTransport] = None) -> str:
        """
        Finalizes the staging directory by building any missing directory trees
        (such as those dropped during transfer for being empty).
        Returns the absolute path of the remote staging directory on the controller node.
        """
        controller_env = self.environment('controller')
        with self.transport_context(transport) as transport:
            if not transport.isdir(controller_env['CANINE_COMMON']):
                transport.mkdir(controller_env['CANINE_COMMON'])
            if not transport.isdir(controller_env['CANINE_JOBS']):
                transport.mkdir(controller_env['CANINE_JOBS'])
            print("Finalizing directory structure. This may take a while...")
            if len(jobs):
                for jobId in status_bar.iter(jobs):
                    if not transport.isdir(os.path.join(controller_env['CANINE_JOBS'], jobId, 'workspace')):
                        transport.makedirs(os.path.join(controller_env['CANINE_JOBS'], jobId, 'workspace'))
                    if not transport.isdir(os.path.join(controller_env['CANINE_JOBS'], jobId, 'inputs')):
                        transport.makedirs(os.path.join(controller_env['CANINE_JOBS'], jobId, 'inputs'))
                if not transport.isdir(controller_env['CANINE_OUTPUT']):
                    transport.mkdir(controller_env['CANINE_OUTPUT'])
            return transport.normpath(self.staging_dir)

    def prepare_job_inputs(self, jobId: str, job_inputs: typing.Dict[str, str], common_dests: typing.Dict[str, str], overrides: typing.Dict[str, typing.Optional[str]], transport: typing.Optional[AbstractTransport] = None):
        """
        Prepares job-specific inputs.
        Fills self.inputs[jobId] with Localization objects for each input
        """
        with self.transport_context(transport) as transport:
            self.inputs[jobId] = {}
            for arg, value in job_inputs.items():
                mode = overrides[arg] if arg in overrides else False
                if mode is not False:
                    if mode == 'stream':
                        if not value.startswith('gs://'):
                            print("Ignoring 'stream' override for", arg, "with value", value, "and localizing now", file=sys.stderr)
                            self.inputs[jobId][arg] = Localization(
                                None,
                                self.reserve_path('jobs', jobId, 'inputs', os.path.basename(os.path.abspath(value)))
                            )
                            self.localize_file(
                                value,
                                self.inputs[jobId][arg].path,
                                transport=transport
                            )
                        else:
                            self.inputs[jobId][arg] = Localization(
                                'stream',
                                value
                            )
                    elif mode == 'localize':
                        self.inputs[jobId][arg] = Localization(
                            None,
                            self.reserve_path('jobs', jobId, 'inputs', os.path.basename(os.path.abspath(value)))
                        )
                        self.localize_file(
                            value,
                            self.inputs[jobId][arg].path,
                            transport=transport
                        )
                    elif mode == 'delayed':
                        if not value.startswith('gs://'):
                            print("Ignoring 'delayed' override for", arg, "with value", value, "and localizing now", file=sys.stderr)
                            self.inputs[jobId][arg] = Localization(
                                None,
                                self.reserve_path('jobs', jobId, 'inputs', os.path.basename(os.path.abspath(value)))
                            )
                            self.localize_file(
                                value,
                                self.inputs[jobId][arg].path,
                                transport=transport
                            )
                        else:
                            self.inputs[jobId][arg] = Localization(
                                'download',
                                value
                            )
                    elif mode is None:
                        # Do not reserve path here
                        # null override treats input as string
                        self.inputs[jobId][arg] = Localization(None, value)
                elif value in common_dests:
                    # common override already handled
                    # No localization needed, already copied
                    self.inputs[jobId][arg] = Localization(None, common_dests[value])
                else:
                    if os.path.exists(value) or value.startswith('gs://'):
                        remote_path = self.reserve_path('jobs', jobId, 'inputs', os.path.basename(os.path.abspath(value)))
                        self.localize_file(value, remote_path, transport=transport)
                        value = remote_path
                    self.inputs[jobId][arg] = Localization(
                        None,
                        # value will either be a PathType, if localized above,
                        # or an unchanged string if not handled
                        value
                    )

    def job_setup_teardown(self, jobId: str, patterns: typing.Dict[str, str]) -> typing.Tuple[str, str]:
        """
        Returns a tuple of (setup script, teardown script) for the given job id.
        Must call after pre-scanning inputs
        """
        job_vars = []
        exports = []
        extra_tasks = [
            'if [[ -d $CANINE_JOB_INPUTS ]]; then cd $CANINE_JOB_INPUTS; fi'
        ]
        compute_env = self.environment('compute')
        for key, val in self.inputs[jobId].items():
            if val.type == 'stream':
                job_vars.append(shlex.quote(key))
                dest = self.reserve_path('jobs', jobId, 'inputs', os.path.basename(os.path.abspath(val.path)))
                extra_tasks += [
                    'if [[ -e {0} ]]; then rm {0}; fi'.format(dest.computepath),
                    'mkfifo {}'.format(dest.computepath),
                    "gsutil {} cat {} > {} &".format(
                        '-u {}'.format(shlex.quote(self.project)) if self.get_requester_pays(val.path) else '',
                        shlex.quote(val.path),
                        dest.computepath
                    )
                ]
                exports.append('export {}="{}"'.format(
                    key,
                    dest.computepath
                ))
            elif val.type == 'download':
                job_vars.append(shlex.quote(key))
                dest = self.reserve_path('jobs', jobId, 'inputs', os.path.basename(os.path.abspath(val.path)))
                extra_tasks += [
                    "if [[ ! -e {2}.fin ]]; then gsutil {0} -o GSUtil:check_hashes=if_fast_else_skip cp {1} {2} && touch {2}.fin; fi".format(
                        '-u {}'.format(shlex.quote(self.project)) if self.get_requester_pays(val.path) else '',
                        shlex.quote(val.path),
                        dest.computepath
                    )
                ]
                exports.append('export {}="{}"'.format(
                    key,
                    dest.computepath
                ))
            elif val.type is None:
                job_vars.append(shlex.quote(key))
                exports.append('export {}="{}"'.format(
                    key,
                    shlex.quote(val.path.computepath if isinstance(val.path, PathType) else val.path)
                ))
            else:
                print("Unknown localization command:", val.type, "skipping", key, val.path, file=sys.stderr)
        setup_script = '\n'.join(
            line.rstrip()
            for line in [
                '#!/bin/bash',
                'export CANINE_JOB_VARS={}'.format(':'.join(job_vars)),
                'export CANINE_JOB_INPUTS="{}"'.format(os.path.join(compute_env['CANINE_JOBS'], jobId, 'inputs')),
                'export CANINE_JOB_ROOT="{}"'.format(os.path.join(compute_env['CANINE_JOBS'], jobId, 'workspace')),
                'export CANINE_JOB_SETUP="{}"'.format(os.path.join(compute_env['CANINE_JOBS'], jobId, 'setup.sh')),
                'export CANINE_JOB_TEARDOWN="{}"'.format(os.path.join(compute_env['CANINE_JOBS'], jobId, 'teardown.sh')),
            ] + exports + extra_tasks
        ) + '\ncd $CANINE_JOB_ROOT\n'
        teardown_script = '\n'.join(
            line.rstrip()
            for line in [
                '#!/bin/bash',
                'if [[ -d {0} ]]; then cd {0}; fi'.format(os.path.join(compute_env['CANINE_JOBS'], jobId, 'workspace')),
                'if which python3 2>/dev/null >/dev/null; then python3 {0} {1} {2} {3}; else python {0} {1} {2} {3}; fi'.format(
                    os.path.join(compute_env['CANINE_ROOT'], 'delocalization.py'),
                    compute_env['CANINE_OUTPUT'],
                    jobId,
                    ' '.join(
                        '-p {} {}'.format(name, shlex.quote(pattern))
                        for name, pattern in patterns.items()
                    )
                ),
            ]
        )
        return setup_script, teardown_script

    @abc.abstractmethod
    def __enter__(self):
        """
        Enter localizer context.
        May take any setup action required
        """
        pass

    def __exit__(self, *args):
        """
        Exit localizer context
        May take any cleanup action required
        """
        if self._local_dir is not None:
            self._local_dir.cleanup()
        if self.clean_on_exit:
            try:
                with self.backend.transport() as transport:
                    transport.rmdir(self.staging_dir)
            except:
                pass

    @abc.abstractmethod
    def localize_file(self, src: str, dest: PathType, transport: typing.Optional[AbstractTransport] = None):
        """
        Localizes the given file.
        """
        pass

    @abc.abstractmethod
    def localize(self, inputs: typing.Dict[str, typing.Dict[str, str]], patterns: typing.Dict[str, str], overrides: typing.Optional[typing.Dict[str, typing.Optional[str]]] = None) -> str:
        """
        3 phase task:
        1) Pre-scan inputs to determine proper localization strategy for all inputs
        2) Begin localizing job inputs. For each job, check the predetermined strategy
        and set up the job's setup and teardown scripts
        3) Finally, finalize the localization. This may include broadcasting the
        staging directory or copying a batch of gsutil files
        Returns the remote staging directory, which is now ready for final startup
        """
        pass
