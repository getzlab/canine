import abc
import os
import sys
import typing
import glob
import shlex
import tempfile
import subprocess
import traceback
import shutil
import warnings
import crayons
import re
from uuid import uuid4
from collections import namedtuple
from contextlib import ExitStack, contextmanager
from ..backends import AbstractSlurmBackend, AbstractTransport, LocalSlurmBackend
from ..utils import get_default_gcp_project, check_call, canine_logging
from hound.client import _getblob_bucket
from agutil import status_bar
import pandas as pd

Localization = namedtuple("Localization", ['type', 'path'])
# types: stream, download, ro_disk, None
# indicates what kind of action needs to be taken during job startup

PathType = namedtuple(
    'PathType',
    ['localpath', 'remotepath']
)

class OverrideValueError(ValueError):
    def __init__(self, override, arg, value):
        super().__init__("'{}' override is invalid for input {} with value {}".format(arg, value))

class AbstractLocalizer(abc.ABC):
    """
    Base class for localization.
    """


    def __init__(
        self, backend: AbstractSlurmBackend, transfer_bucket: typing.Optional[str] = None,
        common: bool = True, staging_dir: str = None,
        project: typing.Optional[str] = None, temporary_disk_type: str = 'standard',
        local_download_dir: typing.Optional[str] = None, **kwargs
    ):
        """
        Initializes the Localizer using the given transport.
        Localizer assumes that the SLURMBackend is connected and functional during
        the localizer's entire life cycle.
        If staging_dir is not provided, a random directory is chosen.
        local_download_dir: Where `local` overrides should be saved. Default: /mnt/canine-local-downloads/(random id).
        temporary_disk_type: "standard" or "ssd". Default "standard".
        NOTE: If temporary_disk_type is explicitly "None", disks will not be created. Files will be downloaded
        to local_download_dir without mounting a disk there. The directory will not be created in that case
        """
        self.transfer_bucket = transfer_bucket
        if transfer_bucket is not None and self.transfer_bucket.startswith('gs://'):
            self.transfer_bucket = self.transfer_bucket[5:]
        self.backend = backend
        self.common = common
        self.common_inputs = set()
        self._local_dir = tempfile.TemporaryDirectory()
        self.local_dir = self._local_dir.name
        # FIXME: This doesn't actually make sense. Unless we assume staging_dir == mount_path, then transport.normpath gives an inaccurate mount_path
        with self.backend.transport() as transport:
            self.staging_dir = transport.normpath(staging_dir if staging_dir is not None else str(uuid4()))
            # if transport.isdir(self.staging_dir) and not force:
            #     raise FileExistsError("{} already exists. Supply force=True to override".format(
            #         self.staging_dir
            #     ))
        self.inputs = {} # {jobId: {inputName: (handle type, handle value)}}
        self.clean_on_exit = True
        self.project = project if project is not None else get_default_gcp_project()
        self.local_download_size = {} # {jobId: size}
        self.disk_key = os.urandom(4).hex()
        self.local_download_dir = local_download_dir if local_download_dir is not None else '/mnt/canine-local-downloads/{}'.format(self.disk_key)
        self.temporary_disk_type = temporary_disk_type
        self.requester_pays = {}

    def get_requester_pays(self, path: str) -> bool:
        """
        Returns True if the requested gs:// object or bucket resides in a
        requester pays bucket
        """
        if path.startswith('gs://'):
            path = path[5:]
        bucket = path.split('/')[0]
        if bucket not in self.requester_pays:
            command = 'gsutil requesterpays get gs://{}'.format(bucket)
            # We check on the remote host because scope differences may cause
            # a requester pays bucket owned by this account to require -u on the controller
            # better safe than sorry
            rc, sout, serr = self.backend.invoke(command)
            text = serr.read()
            if rc == 0 or b'BucketNotFoundException: 404' not in text:
                self.requester_pays[bucket] = (
                    b'requester pays bucket but no user project provided' in text
                    or 'gs://{}: Enabled'.format(bucket).encode() in sout.read()
                )
            else:
                # Try again ls-ing the object itself
                # sometimes permissions can disallow bucket inspection
                # but allow object inspection
                command = 'gsutil ls gs://{}'.format(path)
                rc, sout, serr = self.backend.invoke(command)
                text = serr.read()
                self.requester_pays[bucket] = b'requester pays bucket but no user project provided' in text
            if rc == 1 and b'BucketNotFoundException: 404' in text:
                canine_logging.error(text.decode())
                raise subprocess.CalledProcessError(rc, command)
        return bucket in self.requester_pays and self.requester_pays[bucket]

    def get_object_size(self, path: str) -> int:
        """
        Returns the total number of bytes of the given gsutil object.
        If a directory is given, this will return the total space used by all objects in the directory
        """
        cmd = 'gsutil {} du -s {}'.format(
            '-u {}'.format(self.project) if self.get_requester_pays(path) else '',
            path
        )
        rc, sout, serr = self.backend.invoke(cmd)
        check_call(cmd, rc, sout, serr)
        return int(sout.read().split()[0])

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
        Location must be one of {"local", "remote"}
        """
        if location not in {"local", "remote"}:
            raise ValueError('location must be one of {"local", "remote"}')
        if location == 'local':
            return {
                'CANINE_ROOT': self.local_dir,
                'CANINE_COMMON': os.path.join(self.local_dir, 'common'),
                'CANINE_OUTPUT': os.path.join(self.local_dir, 'outputs'), #outputs/jobid/outputname/...files...
                'CANINE_JOBS': os.path.join(self.local_dir, 'jobs'),
            }
        elif location == "remote":
            return {
                'CANINE_ROOT': self.staging_dir,
                'CANINE_COMMON': os.path.join(self.staging_dir, 'common'),
                'CANINE_OUTPUT': os.path.join(self.staging_dir, 'outputs'), #outputs/jobid/outputname/...files...
                'CANINE_JOBS': os.path.join(self.staging_dir, 'jobs'),
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
                canine_logging.print("Copying directory:", gs_obj)
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
                canine_logging.warning("exist_okay not supported by LocalSlurmBackend")
            return shutil.copytree(src, dest)
        with self.transport_context(transport) as transport:
            if not os.path.isdir(src):
                raise ValueError("Not a directory: "+src)
            if transport.exists(dest) and not exist_okay:
                raise ValueError("Destination already exists: "+dest)
            dest = transport.normpath(dest)
            if self.transfer_bucket is not None:
                canine_logging.print("Transferring through bucket", self.transfer_bucket)
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
                    canine_logging.print(
                        crayons.red("ERROR:", bold=True),
                        "Failed to download the data on the remote system."
                        "Your files are still saved in",
                        'gs://{}/{}'.format(self.transfer_bucket, path),
                        file=sys.stderr, type = "error"
                    )
                    raise
                cmd = "gsutil -m {} rm -r gs://{}/{}".format(
                    '-u {}'.format(self.project) if self.get_requester_pays(self.transfer_bucket) else '',
                    self.transfer_bucket,
                    os.path.dirname(path),
                )
                canine_logging.info(cmd)
                subprocess.check_call(
                    cmd,
                    shell=True
                )
            else:
                canine_logging.info("Transferring directly over SFTP")
                transport.sendtree(src, dest)

    def receivetree(self, src: str, dest: str, transport: typing.Optional[AbstractTransport] = None, exist_okay=False):
        """
        Transfers the given remote folder to the given local destination.
        Source must be a remote folder, and dest must not exist
        """
        if isinstance(self.backend, LocalSlurmBackend):
            if exist_okay:
                canine_logging.warning("exist_okay not supported by LocalSlurmBackend")
            return shutil.copytree(src, dest)
        with self.transport_context(transport) as transport:
            if not transport.isdir(src):
                raise ValueError("Not a directory: "+src)
            if os.path.exists(dest) and not exist_okay:
                raise ValueError("Destination already exists: "+dest)
            dest = os.path.abspath(dest)
            if self.transfer_bucket is not None:
                canine_logging.print("Transferring through bucket", self.transfer_bucket)
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
                    canine_logging.print(
                        crayons.red("ERROR:", bold=True),
                        "Failed to download the data on the local system."
                        "Your files are still saved in",
                        'gs://{}/{}'.format(self.transfer_bucket, path),
                        file=sys.stderr, type="error"
                    )
                    raise
                cmd = "gsutil -m {} rm -r gs://{}/{}".format(
                    '-u {}'.format(self.project) if self.get_requester_pays(self.transfer_bucket) else '',
                    self.transfer_bucket,
                    os.path.dirname(path),
                )
                canine_logging.info(cmd)
                subprocess.check_call(
                    cmd,
                    shell=True
                )
            else:
                canine_logging.info("Transferring directly over SFTP")
                transport.receivetree(src, dest)

    def reserve_path(self, *args: typing.Any) -> PathType:
        """
        Takes any number of path components, relative to the CANINE_ROOT directory
        Returns a PathType object which can be used to view the path relative to
        local, compute, or controller contexts
        """
        return PathType(
            os.path.join(self.environment('local')['CANINE_ROOT'], *(str(component) for component in args)),
            os.path.join(self.environment('remote')['CANINE_ROOT'], *(str(component) for component in args)),
        )

    def build_manifest(self, transport: typing.Optional[AbstractTransport] = None) -> pd.DataFrame:
        """
        Returns the job output manifest from this pipeline.
        Builds the manifest if it does not exist
        """
        with self.transport_context() as transport:
            output_dir = transport.normpath(self.environment('remote')['CANINE_OUTPUT'])
            if not transport.isfile(os.path.join(output_dir, '.canine_pipeline_manifest.tsv')):
                script_path = self.backend.pack_batch_script(
                    'export CANINE_OUTPUTS={}'.format(output_dir),
                    'cat <(echo "jobId\tfield\tpattern\tpath") $CANINE_OUTPUTS/*/.canine_job_manifest > $CANINE_OUTPUTS/.canine_pipeline_manifest.tsv',
                    script_path=os.path.join(output_dir, 'manifest.sh')
                )
                check_call(
                    script_path,
                    *self.backend.invoke(os.path.abspath(script_path))
                )
                transport.remove(script_path)
            with transport.open(os.path.join(output_dir, '.canine_pipeline_manifest.tsv'), 'r') as r:
                return pd.read_csv(r, sep='\t', dtype='str').set_index(['jobId', 'field'])

    def delocalize(self, patterns: typing.Dict[str, str], output_dir: str = 'canine_output') -> typing.Dict[str, typing.Dict[str, str]]:
        """
        Delocalizes output from all jobs
        """
        self.build_manifest()
        self.receivetree(
            self.environment('remote')['CANINE_OUTPUT'],
            output_dir,
            exist_okay=True
        )
        output_files = {}
        for jobId in os.listdir(output_dir):
            start_dir = os.path.join(output_dir, jobId)
            if not os.path.isdir(start_dir):
                continue
            output_files[jobId] = {}
            for outputname in os.listdir(start_dir):
                dirpath = os.path.join(start_dir, outputname)
                if os.path.isdir(dirpath):
                    if outputname not in patterns:
                        warnings.warn("Detected output directory {} which was not declared".format(dirpath))
                    output_files[jobId][outputname] = glob.glob(os.path.join(dirpath, patterns[outputname]))
                elif outputname in {'stdout', 'stderr'} and os.path.isfile(dirpath):
                    output_files[jobId][outputname] = [dirpath]
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
                # noop; this shard was avoided
                if values is None:
                    continue

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
#                else:
#                    print("Could not handle common file", path, file=sys.stderr)
            return {key: value for key, value in common_dests.items()}

    def finalize_staging_dir(self, jobs: typing.Iterable[str], transport: typing.Optional[AbstractTransport] = None) -> str:
        """
        Finalizes the staging directory by building any missing directory trees
        (such as those dropped during transfer for being empty).
        Returns the absolute path of the remote staging directory on the controller node.
        """
        controller_env = self.environment('remote')
        with self.transport_context(transport) as transport:
            if not transport.isdir(controller_env['CANINE_COMMON']):
                transport.mkdir(controller_env['CANINE_COMMON'])
            if not transport.isdir(controller_env['CANINE_JOBS']):
                transport.mkdir(controller_env['CANINE_JOBS'])
            if len(jobs) and not transport.isdir(controller_env['CANINE_OUTPUT']):
                transport.mkdir(controller_env['CANINE_OUTPUT'])
            return self.staging_dir

    def prepare_job_inputs(self, jobId: str, job_inputs: typing.Dict[str, str], common_dests: typing.Dict[str, str], overrides: typing.Dict[str, typing.Optional[str]], transport: typing.Optional[AbstractTransport] = None):
        """
        Prepares job-specific inputs.
        Fills self.inputs[jobId] with Localization objects for each input
        """
        if 'CANINE_JOB_ALIAS' in job_inputs and 'CANINE_JOB_ALIAS' not in overrides:
            overrides['CANINE_JOB_ALIAS'] = None
        with self.transport_context(transport) as transport:
            self.inputs[jobId] = {}
            for arg, value in job_inputs.items():
                mode = overrides[arg] if arg in overrides else False
                if value in common_dests or mode == 'common':
                    # common override already handled
                    # No localization needed, already copied
                    self.inputs[jobId][arg] = Localization(None, common_dests[value])
                elif mode is not False:
                    try:
                        if mode == 'stream':
                            if not value.startswith('gs://'):
                                raise OverrideValueError(mode, arg, value)

                            self.inputs[jobId][arg] = Localization(
                                'stream',
                                value
                            )

                        elif mode in ['localize', 'symlink']:
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
                                raise OverrideValueError(mode, arg, value)

                            self.inputs[jobId][arg] = Localization(
                                'download',
                                value
                            )

                        elif mode == "ro_disk":
                            if not value.startswith('rodisk://'):
                                raise OverrideValueError(mode, arg, value)
                            self.inputs[jobId][arg] = Localization(
                                'ro_disk',
                                value
                            )

                        elif mode is None or mode == 'null':
                            # Do not reserve path here
                            # null override treats input as string
                            self.inputs[jobId][arg] = Localization(None, value)

                        else:
                            raise ValueError("Invalid override option [{}]".format(mode))
                    except OverrideValueError as e:
                        canine_logging.error(e.args[0])
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
                    # if no overrides were given, see if we can immediately
                    # localize this file
                    if os.path.exists(value) or value.startswith('gs://'):
                        remote_path = self.reserve_path('jobs', jobId, 'inputs', os.path.basename(os.path.abspath(value)))
                        self.localize_file(value, remote_path, transport=transport)
                        value = remote_path

                    # autodetect if this is a path on a read-only disk 
                    loc_type = "ro_disk" if isinstance(value, str) and value.startswith('rodisk://') else None

                    self.inputs[jobId][arg] = Localization(
                        # localization type will either be:
                        # * None, indicating no special processing in localization.sh
                        # * ro_disk, indicating that this is a path on a read-only disk
                        loc_type,

                        # value will either be a:
                        # * PathType, if localized above
                        # * an unchanged string if not handled
                        value
                    )

    def job_setup_teardown(self, jobId: str, patterns: typing.Dict[str, str]) -> typing.Tuple[str, str, str]:
        """
        Returns a tuple of (setup script, localization script, teardown script) for the given job id.
        Must call after pre-scanning inputs
        """

        # generate job variable, exports, and localization_tasks arrays
        # - job variables and exports are set when setup.sh is _sourced_
        # - localization tasks are run when localization.sh is _run_
        job_vars = []
        exports = [
            'export CANINE_NODE_NAME=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/name 2> /dev/null)',
            'export CANINE_NODE_ZONE=$(basename $(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/zone 2> /dev/null))'
        ]
        docker_args = ['-v $CANINE_ROOT:$CANINE_ROOT']
        localization_tasks = [
            'if [[ -d $CANINE_JOB_INPUTS ]]; then cd $CANINE_JOB_INPUTS; fi'
        ]

        local_download_size = self.local_download_size.get(jobId, 0)
        disk_name = None
        if local_download_size > 0 and self.temporary_disk_type is not None:
            local_download_size = max(
                10,
                1+int(local_download_size / 1022611260) # bytes -> gib with 5% safety margin
            )
            if local_download_size > 65535:
                raise ValueError("Cannot provision {} GB disk for job {}".format(local_download_size, jobId))
            disk_name = 'canine-{}-{}-{}'.format(self.disk_key, os.urandom(4).hex(), jobId)
            device_name = 'cn{}{}'.format(os.urandom(2).hex(), jobId)
            exports += [
                'export CANINE_LOCAL_DISK_SIZE={}GB'.format(local_download_size),
                'export CANINE_LOCAL_DISK_TYPE={}'.format(self.temporary_disk_type),
                'export CANINE_LOCAL_DISK_DIR={}/{}'.format(self.local_download_dir, disk_name),
            ]
            localization_tasks += [
                'sudo mkdir -p $CANINE_LOCAL_DISK_DIR',
                'if [[ -z "$CANINE_NODE_NAME" ]]; then echo "Unable to provision disk (not running on GCE instance). Attempting download to directory on boot disk" > /dev/stderr; else',
                'echo Provisioning and mounting temporary disk {}'.format(disk_name),
                'gcloud compute disks create {} --size {} --type pd-{} --zone $CANINE_NODE_ZONE'.format(
                    disk_name,
                    local_download_size,
                    self.temporary_disk_type
                ),
                'gcloud compute instances attach-disk $CANINE_NODE_NAME --zone $CANINE_NODE_ZONE --disk {} --device-name {}'.format(
                    disk_name,
                    device_name
                ),
                'gcloud compute instances set-disk-auto-delete $CANINE_NODE_NAME --zone $CANINE_NODE_ZONE --disk {}'.format(disk_name),
                'sudo mkfs.ext4 -m 0 -E lazy_itable_init=0,lazy_journal_init=0,discard /dev/disk/by-id/google-{}'.format(device_name),

                'sudo mount -o discard,defaults /dev/disk/by-id/google-{} $CANINE_LOCAL_DISK_DIR'.format(
                    device_name,
                ),
                'sudo chmod -R a+rwX {}'.format(self.local_download_dir),
                'fi'
            ]
            docker_args.append('-v $CANINE_LOCAL_DISK_DIR:$CANINE_LOCAL_DISK_DIR')

        compute_env = self.environment('remote')
        stream_dir_ready = False
        for key, val in self.inputs[jobId].items():
            if val.type == 'stream':
                job_vars.append(shlex.quote(key))
                if not stream_dir_ready:
                    exports.append('export CANINE_STREAM_DIR=$(mktemp -d /tmp/canine_streams.$SLURM_ARRAY_JOB_ID.$SLURM_ARRAY_TASK_ID.XXXX)')
                    docker_args.append('-v $CANINE_STREAM_DIR:$CANINE_STREAM_DIR')
                    stream_dir_ready = True
                dest = os.path.join('$CANINE_STREAM_DIR', os.path.basename(os.path.abspath(val.path)))
                localization_tasks += [
                    'gsutil ls {} > /dev/null'.format(shlex.quote(val.path)),
                    'if [[ -e {0} ]]; then rm {0}; fi'.format(dest),
                    'mkfifo {}'.format(dest),
                    "gsutil {} cat {} > {} &".format(
                        '-u {}'.format(shlex.quote(self.project)) if self.get_requester_pays(val.path) else '',
                        shlex.quote(val.path),
                        dest
                    )
                ]
                exports.append('export {}="{}"'.format(
                    key,
                    dest
                ))

            elif val.type in {'download', 'local'}:
                job_vars.append(shlex.quote(key))
                if val.type == 'download':
                    dest = self.reserve_path('jobs', jobId, 'inputs', os.path.basename(os.path.abspath(val.path)))
                else:
                    # Local and controller paths not needed on this object
                    dest = PathType(None, os.path.join(self.local_download_dir, disk_name, os.path.basename(val.path)))
                localization_tasks += [
                    "if [[ ! -e {2}.fin ]]; then gsutil {0} -o GSUtil:check_hashes=if_fast_else_skip cp {1} {2} && touch {2}.fin; fi".format(
                        '-u {}'.format(shlex.quote(self.project)) if self.get_requester_pays(val.path) else '',
                        shlex.quote(val.path),
                        dest.remotepath
                    )
                ]
                exports.append('export {}="{}"'.format(
                    key,
                    dest.remotepath
                ))

            elif val.type == 'ro_disk':
                assert val.path.startswith("rodisk://")

                job_vars.append(shlex.quote(key))

                dgrp = re.search(r"rodisk://(.*?)/(.*)", val.path)
                disk = dgrp[1]
                file = dgrp[2]

                dest = self.reserve_path('jobs', jobId, 'inputs', file)

                exports += ["export CANINE_LOCAL_DISK_DIR=/mnt/nfs/ro_disks/{}".format(disk)]
                localization_tasks += [
                  "if [[ ! -d $CANINE_LOCAL_DISK_DIR ]]; then sudo mkdir -p $CANINE_LOCAL_DISK_DIR; fi",

                  # attach the disk if it's not already
                  "if [[ ! -e /dev/disk/by-id/google-{} ]]; then".format(disk),
                  # we can run into a race condition here if other tasks are
                  # attempting to mount the same disk simultaneously, so we
                  # force a 0 exit
                  "gcloud compute instances attach-disk $CANINE_NODE_NAME --zone $CANINE_NODE_ZONE --disk {disk_name} --device-name {disk_name} --mode ro || true".format(disk_name = disk),
                  "fi",

                  # mount the disk if it's not already
                  # as before, we can run into a race condition here, so we again
                  # force a zero exit
                  "if ! mountpoint -q $CANINE_LOCAL_DISK_DIR; then",
                  # within container
                  "sudo mount -o noload,ro,defaults /dev/disk/by-id/google-{disk_name} $CANINE_LOCAL_DISK_DIR || true".format(disk_name = disk),
                  # on host (so that other dockers can access it)
                  "if [[ -f /.dockerenv ]]; then",
                  "sudo nsenter -t 1 -m mount -o noload,ro,defaults /dev/disk/by-id/google-{disk_name} $CANINE_LOCAL_DISK_DIR || true".format(disk_name = disk),
                  "fi",
                  "fi",

                  # because we forced zero exits for the previous commands,
                  # we need to verify that the mount actually exists
                  "mountpoint -q $CANINE_LOCAL_DISK_DIR || { echo 'Read-only disk mount failed!'; exit 1; }",

                  # symlink into the canine directory
                  # note it might already exist if we are retrying this task
                  "if [[ ! -L {path} ]]; then ln -s ${{CANINE_LOCAL_DISK_DIR}}/{file} {path}; fi".format(file = file, path = dest.remotepath),
                ]
                # Removed, this is actually '-v :'
                #docker_args.append('-v $CANINE_LOCAL_DISK_DIR:$CANINE_LOCAL_DISK_DIR')
                exports.append('export {}="{}"'.format(
                    key,
                    dest.remotepath
                ))

            elif val.type is None:
                job_vars.append(shlex.quote(key))
                exports.append('export {}={}'.format(
                    key,
                    shlex.quote(val.path.remotepath if isinstance(val.path, PathType) else val.path)
                ))
            else:
                canine_logging.print("Unknown localization command:", val.type, "skipping", key, val.path,
                    file=sys.stderr, type="warning"
                )

        # generate setup script
        setup_script = '\n'.join(
            line.rstrip()
            for line in [
                '#!/bin/bash',
                'export CANINE_JOB_VARS={}'.format(':'.join(job_vars)),
                'export CANINE_JOB_INPUTS="{}"'.format(os.path.join(compute_env['CANINE_JOBS'], jobId, 'inputs')),
                'export CANINE_JOB_ROOT="{}"'.format(os.path.join(compute_env['CANINE_JOBS'], jobId, 'workspace')),
                'export CANINE_JOB_SETUP="{}"'.format(os.path.join(compute_env['CANINE_JOBS'], jobId, 'setup.sh')),
                'export CANINE_JOB_LOCALIZATION="{}"'.format(os.path.join(compute_env['CANINE_JOBS'], jobId, 'localization.sh')),
                'export CANINE_JOB_TEARDOWN="{}"'.format(os.path.join(compute_env['CANINE_JOBS'], jobId, 'teardown.sh')),
                'mkdir -p $CANINE_JOB_INPUTS',
                'mkdir -p $CANINE_JOB_ROOT',
                'chmod 755 $CANINE_JOB_LOCALIZATION',
            ] + exports
        ) + '\nexport CANINE_DOCKER_ARGS="{docker}"\ncd $CANINE_JOB_ROOT\n'.format(docker=' '.join(set(docker_args)))

        # generate localization script
        localization_script = '\n'.join([
          "#!/bin/bash",
          "set -e"
        ] + localization_tasks) + "\nset +e\n"

        # generate teardown script
        teardown_script = '\n'.join(
            line.rstrip()
            for line in [
                '#!/bin/bash',
                'set -e',
                'if [[ -d {0} ]]; then cd {0}; fi'.format(os.path.join(compute_env['CANINE_JOBS'], jobId, 'workspace')),
                # 'mv ../stderr ../stdout .',
                'if which python3 2>/dev/null >/dev/null; then python3 {0} {1} {2} {3}; else python {0} {1} {2} {3}; fi'.format(
                    os.path.join(compute_env['CANINE_ROOT'], 'delocalization.py'),
                    compute_env['CANINE_OUTPUT'],
                    jobId,
                    ' '.join(
                        '-p {} {}'.format(name, shlex.quote(pattern))
                        for name, pattern in patterns.items()
                    )
                ),
                'if [[ -n "$CANINE_STREAM_DIR" ]]; then rm -rf $CANINE_STREAM_DIR; fi'
            ] + (
                [
                    'sudo umount {}/{}'.format(self.local_download_dir, disk_name),
                    'gcloud compute instances detach-disk $CANINE_NODE_NAME --zone $CANINE_NODE_ZONE --disk {}'.format(disk_name),
                    'gcloud compute disks delete {} --zone $CANINE_NODE_ZONE'.format(disk_name)
                ] if disk_name is not None else []
            )
        )
        return setup_script, localization_script, teardown_script

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
        and set up the job's setup, localization, and teardown scripts
        3) Finally, finalize the localization. This may include broadcasting the
        staging directory or copying a batch of gsutil files
        Returns the remote staging directory, which is now ready for final startup
        """
        pass
