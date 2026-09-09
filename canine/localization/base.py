import abc
import os
import sys
import typing
import glob
import shlex
import tempfile
import random
import subprocess
import threading
import time
import traceback
import shutil
import warnings
import crayons
import re
import json
from uuid import uuid4
from collections import namedtuple
from contextlib import ExitStack, contextmanager
from . import file_handlers
from ..backends import AbstractSlurmBackend, AbstractTransport, LocalSlurmBackend
from ..utils import get_default_gcp_project, get_default_gcp_zone, check_call, canine_logging, localization_bucket_name, get_project_number, _zone_to_region
from hound.client import _getblob_bucket
import pandas as pd
import google.cloud.compute_v1, google.api_core.exceptions

DISK_CLIENT = None
disk_client_creation_lock = threading.Lock()

def gcloud_disk_client():
    global DISK_CLIENT
    with disk_client_creation_lock:
        if DISK_CLIENT is None:
            # this is the expensive operation
            DISK_CLIENT = google.cloud.compute_v1.DisksClient()
    return DISK_CLIENT

PROJECT = get_default_gcp_project()

Localization = namedtuple("Localization", ['type', 'path'])
# types: stream, download, ro_disk, None
# indicates what kind of action needs to be taken during job startup

PathType = namedtuple(
    'PathType',
    ['localpath', 'remotepath']
)

# One input to be placed in a localization bucket by the worker.
#   fh   : the FileType handler for the source
#   dest : full gs:// destination URL
#   kind : how the bytes get there --
#     "server_side" gs:// source; a GCS rewrite, no VM in the data path at all
#     "mount"       remote non-gs:// source; downloaded through the worker's
#                   read-write gcsfuse mount, never staged on local disk
#     "copy"        source is already a file on the shared mount (a previous
#                   task's output); uploaded straight from there, so this adds
#                   no NFS traffic that the file's own existence did not
UploadItem = namedtuple("UploadItem", ["fh", "dest", "kind"])


class OverrideValueError(ValueError):
    def __init__(self, override, arg, value):
        super().__init__("'{}' override is invalid for input {} with value {}".format(override, arg, value))

class AbstractLocalizer(abc.ABC):
    """
    Base class for localization.
    """


    def __init__(
        self, backend: AbstractSlurmBackend, transfer_bucket: typing.Optional[str] = None,
        common: bool = True, staging_dir: str = None,
        project: typing.Optional[str] = None,
        token: typing.Optional[str] = None,
        localize_to_persistent_disk = False, persistent_disk_type: str = "standard",
        use_scratch_disk = False, scratch_disk_size: int = 10, scratch_disk_type: str = "standard", scratch_disk_job_avoid=True, scratch_disk_name = None,
        protect_disk = False,
        files_to_copy_to_outputs = {},
        persistent_disk_dry_run = False,
        cleanup_job_workdir = False,
        allow_requester_pays: bool = False,
        bucket_upload_wait_tries: int = 60,
        localization_expiry_days: int = 1,
        bucketmount_heartbeat_seconds: int = 3600,
        **kwargs
    ):
        """
        Initializes the Localizer using the given transport.
        Localizer assumes that the SLURMBackend is connected and functional during
        the localizer's entire life cycle.
        If staging_dir is not provided, a random directory is chosen.
        localize_to_persistent_disk: if True, will create a new GCP persistent disk and
          localize all files there
        persistent_disk_type: "standard" or "ssd". Default "standard".
        use_scratch_disk: if True, will create a new GCP persistent disk to which
          all job outputs will be written
        scratch_disk_type: "standard" or "ssd". Default "standard".
        scratch_disk_size: size of scratch disk, in gigabytes. Default 10GB
        scratch_disk_name: name of scratch disk. Default is a random string
        scratch_disk_job_avoid: whether to bypass running job script if scratch disk
          already exists. Default True; set to False if you want to retroactively
          modify contents of a scratch disk after it's been created and populated by
          another task.
        protect_disk: add label "protect : yes" to disk; this will prevent it
          from being automatically deleted
        files_to_copy_to_outputs: if using a scratch disk, copy these output keys
          to the NFS from the scratch disk
        persistent_disk_dry_run: don't actually create a persistent disk; just
          return the paths to the files on the disk that would be created
        cleanup_job_workdir: remove files in the job working directory that aren't
          denoted as outputs
        allow_requester_pays: if False (default), downloading from a requester-pays
          GCS bucket raises instead of silently billing self.project
        bucket_upload_wait_tries: how many 60s polls a job waits for a sibling's
          upload to finish before breaking the claim and requeueing (exit 5).
          Default 60, i.e. a one hour ceiling.
        localization_expiry_days: days since last touch after which a localized
          object is deleted by the bucket's lifecycle rule. Default 1. Content is
          re-fetched automatically if something needs it again, and objects still
          in use have their clock refreshed on every localization, so this bounds
          idle storage rather than the lifetime of a running workflow. Raise it if
          you re-run the same inputs over several days and would rather pay for
          storage than re-transfer.
        """
        self.transfer_bucket = transfer_bucket
        if transfer_bucket is not None and self.transfer_bucket.startswith('gs://'):
            self.transfer_bucket = self.transfer_bucket[5:]

        self.backend = backend

        self.common = common

        self._local_dir = tempfile.TemporaryDirectory()
        self.local_dir = self._local_dir.name
        # FIXME: This doesn't actually make sense. Unless we assume staging_dir == mount_path, then transport.normpath gives an inaccurate mount_path
        with self.backend.transport() as transport:
            self.staging_dir = transport.normpath(staging_dir if staging_dir is not None else str(uuid4()))
            # if transport.isdir(self.staging_dir) and not force:
            #     raise FileExistsError("{} already exists. Supply force=True to override".format(
            #         self.staging_dir
            #     ))
        self.inputs = {} # {jobId: {inputName: [(handle type, handle value), ...]}}
        self.input_array_flag = {} # {jobId: {inputName: <bool: is this an array?>}}

        self.clean_on_exit = True
        self.project = project if project is not None else PROJECT
        self.token = token

        # will be removed
        self.local_download_size = {} # {jobId: size}

        self.localize_to_persistent_disk = localize_to_persistent_disk
        self.persistent_disk_type = persistent_disk_type
        # do not use common inputs when localizing to persistent disk
        if self.localize_to_persistent_disk:
            self.common = False
        self.use_scratch_disk = use_scratch_disk
        self.scratch_disk_type = scratch_disk_type
        self.scratch_disk_size = scratch_disk_size
        self.scratch_disk_name = scratch_disk_name
        self.scratch_disk_job_avoid = scratch_disk_job_avoid
        self.protect_disk = protect_disk

        self.files_to_copy_to_outputs = files_to_copy_to_outputs

        self.persistent_disk_dry_run = persistent_disk_dry_run

        self.cleanup_job_workdir = cleanup_job_workdir

        self.allow_requester_pays = allow_requester_pays

        self.bucket_upload_wait_tries = bucket_upload_wait_tries
        self.localization_expiry_days = localization_expiry_days
        # A mount held longer than the expiry window would otherwise have its own
        # lease -- and its data objects -- deleted out from under it, so the
        # heartbeat has to fire well inside that window. Fail here rather than as
        # a mid-run deletion.
        if bucketmount_heartbeat_seconds >= localization_expiry_days * 86400 / 4:
            raise ValueError(
              "bucketmount_heartbeat_seconds ({}) must be well inside the {}-day expiry window; "
              "content held by a long-running job would expire between beats."
              .format(bucketmount_heartbeat_seconds, localization_expiry_days)
            )
        self.bucketmount_heartbeat_seconds = bucketmount_heartbeat_seconds

        # to extract rodisk URLs if we want to re-use disk(s) downstream for
        # other tasks
        # jobId : { input : [RODISK URLs] }
        self.rodisk_paths = {}

        # will be removed
        self.disk_key = os.urandom(4).hex()
        local_download_dir = None
        self.local_download_dir = local_download_dir if local_download_dir is not None else '/mnt/canine-local-downloads/{}'.format(self.disk_key)
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
            command = 'gcloud storage buckets describe gs://{} --format="value(requester_pays)"'.format(bucket)
            # We check on the remote host because scope differences may cause
            # a requester pays bucket owned by this account to require
            # --billing-project on the controller. Better safe than sorry.
            #
            rc, sout, serr = self.backend.invoke(command)
            text = serr.read()
            if rc == 0:
                self.requester_pays[bucket] = (
                    b'requester pays bucket but no user project provided' in text
                    or sout.read().strip() == b'True'
                )
            else:
                # Try again ls-ing the object itself
                # sometimes permissions can disallow bucket inspection
                # (e.g. missing storage.buckets.get, as with many third-party
                # requester-pays buckets) but allow object inspection -- retry
                # regardless of why the describe call failed, not just on 404
                command = 'gcloud storage ls gs://{}'.format(path)
                rc, sout, serr = self.backend.invoke(command)
                text = serr.read()
                self.requester_pays[bucket] = b'requester pays bucket but no user project provided' in text
            if rc != 0 and b'404' in text:
                canine_logging.error(text.decode())
                raise subprocess.CalledProcessError(rc, command)
        return bucket in self.requester_pays and self.requester_pays[bucket]

    def get_object_size(self, path: str) -> int:
        """
        Returns the total number of bytes of the given gs:// object.
        If a directory is given, this will return the total space used by all objects in the directory
        """
        cmd = 'gcloud storage du -s {} {}'.format(
            '--billing-project={}'.format(self.project) if self.get_requester_pays(path) else '',
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
        requester_pays = self.get_requester_pays(gs_obj)
        if requester_pays and not dest.startswith('gs://') and not self.allow_requester_pays:
            raise ValueError(f"{gs_obj} resides in a requester-pays bucket, but access to "
                              "requester-pays buckets is disabled (allow_requester_pays=False)")
        command = "CLOUDSDK_STORAGE_PARALLEL_COMPOSITE_UPLOAD_THRESHOLD=150M gcloud storage cp -r {} {} {}".format(
            '--billing-project={}'.format(self.project) if requester_pays else '',
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

        requester_pays = self.get_requester_pays(gs_obj)
        if requester_pays and not dest.startswith('gs://') and not self.allow_requester_pays:
            raise ValueError(f"{gs_obj} resides in a requester-pays bucket, but access to "
                              "requester-pays buckets is disabled (allow_requester_pays=False)")
        command = "CLOUDSDK_STORAGE_PARALLEL_COMPOSITE_UPLOAD_THRESHOLD=150M gcloud storage cp {} {} {}".format(
            '--billing-project={}'.format(self.project) if requester_pays else '',
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
                cmd = "gcloud storage rm -r {} gs://{}/{}".format(
                    '--billing-project={}'.format(self.project) if self.get_requester_pays(self.transfer_bucket) else '',
                    self.transfer_bucket,
                    os.path.dirname(path),
                )
                canine_logging.info1(cmd)
                subprocess.check_call(
                    cmd,
                    shell=True
                )
            else:
                canine_logging.info1("Transferring directly over SFTP")
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
                cmd = "gcloud storage rm -r {} gs://{}/{}".format(
                    '--billing-project={}'.format(self.project) if self.get_requester_pays(self.transfer_bucket) else '',
                    self.transfer_bucket,
                    os.path.dirname(path),
                )
                canine_logging.info1(cmd)
                subprocess.check_call(
                    cmd,
                    shell=True
                )
            else:
                canine_logging.info1("Transferring directly over SFTP")
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
        # this seems to be crashing pipelines; since we don't use it anywhere,
        # let's disable it for now
        #self.build_manifest()
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

                    # if we're using a scratch disk, outputs should be RODISK
                    # objects for downstream tasks to mount. read symlinks to
                    # RODISK URLs
                    if self.use_scratch_disk and outputname not in self.files_to_copy_to_outputs:
                        output_files[jobId][outputname] = ["rodisk://" + re.match(r".*(canine-scratch.*)", os.readlink(x))[1] for x in output_files[jobId][outputname]]
                elif outputname in {'stdout', 'stderr'} and os.path.isfile(dirpath):
                    output_files[jobId][outputname] = [dirpath]
        return output_files

    def get_destination_path(self, filename, transport, *destdir):
        basename = os.path.basename(os.path.abspath(filename))
        path = self.reserve_path(*destdir, basename)
        n = 2
        with self.transport_context(transport) as transport:
            while transport.exists(path.remotepath):
                if n == 2:
                    basename += "_2"
                else:
                    basename = re.sub(r"_\d+$", "_" + str(n), basename)
                n += 1
                path = self.reserve_path(*destdir, basename)

        return path

    def pick_common_inputs(self, inputs: typing.Dict[str, typing.Dict[str, str]], overrides: typing.Dict[str, typing.Optional[str]], transport: typing.Optional[AbstractTransport] = None) -> typing.Dict[str, str]:
        """
        Scans input configuration and overrides to choose inputs which should be treated as common,
        namely URLs common to all job shards that should only be localized once
        on the controller node, rather than multiple times for each shard on
        worker nodes.
        Returns the dictionary of common inputs {input path: common path}
        """
        with self.transport_context(transport) as transport:
            common_inputs = {}
            seen = set()

            # 1. scan for any duplicate values across inputs
            for jobId, values in inputs.items():
                # noop; this shard was avoided
                if values is None:
                    continue

                for arg, paths in values.items():
                    paths = [paths] if not isinstance(paths, list) else paths
                    if arg not in overrides:
                        for p in paths:
                            # TODO: pass through other file handler arguments here
                            fh = file_handlers.get_file_handler(p, project = self.project, token = self.token, allow_requester_pays = self.allow_requester_pays)

                            # only pick common inputs that are URLs; it does not
                            # save time for any other input types, and only leads
                            # to complications down the road.
                            if fh.localization_mode != "url":
                                continue

                            # if hash has already been precomputed, use it
                            # this will be the case if FileType objects are 
                            # given directly to Canine, and then expanded by
                            # adapter into dense non-ragged job arrays
                            if fh._hash is not None:
                                if fh.hash in seen:
                                    common_inputs[fh.hash] = fh
                                seen.add(fh.hash)

                            # otherwise, use filename: dense job arrays would
                            # require hashing everything
                            else:
                                if fh.path in seen:
                                    common_inputs[fh.path] = fh
                                seen.add(fh.path)

            # 2. if any of these duplicate values can be immediately localized,
            #    do it, and mark them as localized so that subsequent functions
            #    won't touch them
            common_dests = {} # original path -> FileType object
            for file_handler in common_inputs.values():
                path = file_handler.path
                # this path is either a URL that we know how to download, or
                # a local path
                if file_handler.localization_mode in {"url", "local"}:
                    dest_path = self.get_destination_path(
                      path,
                      transport,
                      'common'
                    )

                    try:
                        self.localize_file(file_handler, dest_path, transport=transport)

                        # this is now a regular file, residing at dest_path.remotepath
                        common_dests[path] = file_handlers.HandleRegularFile(dest_path.remotepath)
                        common_dests[path].localized_path = dest_path.remotepath
                        # copy over hash if it's been precomputed
                        if file_handler._hash is not None:
                            common_dests[path]._hash = file_handler._hash
                    except:
                        canine_logging.error("Unknown error localizing common file {}".format(path))
                        raise
            return common_dests

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
        Fills self.inputs[jobId] with file_handler objects for each input
        """

        def handle_input(value, mode):
            nonlocal transport
            
            ## if input is a string, convert it to the appropriate FileType object
            if isinstance(value, str):
                # TODO: pass through other file handler arguments here
                value = file_handlers.get_file_handler(value, project = self.project, token = self.token, allow_requester_pays = self.allow_requester_pays)
            else:
                assert isinstance(value, file_handlers.FileType)

            # if user overrode the handling mode, make sure it's compatible
            # with the file type
            if mode is not False: 
                # user wants to stream a URL, rather than download it
                if mode == 'stream' and value.localization_mode != "stream":
                    # if handler itself does not have localization_mode == "stream",
                    # check if we can stream this, by seeing if any subclasses
                    # implement localization_mode == "stream"
                    stream_subclass = None
                    for subcls in value.__class__.__subclasses__():
                        if subcls.localization_mode == "stream":
                            stream_subclass = subcls
                            break # we assume that only one subclass implements streaming
                    if stream_subclass is None:
                        raise ValueError(f"Input type {value.__class__.__name__} cannot be streamed!")

                    # monkey patch the stream subclass to replace its parent
                    value.__class__ = stream_subclass

                    return value

                # user wants to treat this path as a string literal
                elif mode is None or mode == 'null' or mode == 'string':
                    return file_handlers.StringLiteral(value.path)

            # common file has already been localized and handling mode has not
            # been overridden; we can thus return its file handler object
            # created during pick_common_inputs()
            if value.path in common_dests:
                return common_dests[value.path]

            # otherwise, return whatever file type was inferred
            return value

        if 'CANINE_JOB_ALIAS' in job_inputs and 'CANINE_JOB_ALIAS' not in overrides:
            overrides['CANINE_JOB_ALIAS'] = None
        self.inputs[jobId] = {}
        self.input_array_flag[jobId] = {}
        for arg, value in job_inputs.items():
            mode = overrides[arg] if arg in overrides else False
            self.input_array_flag[jobId][arg] = isinstance(value, list)
            value = [value] if not self.input_array_flag[jobId][arg] else value

            self.inputs[jobId][arg] = [None]*len(value)

            for i, v in enumerate(value):
                self.inputs[jobId][arg][i] = handle_input(v, mode)

    def create_persistent_disk(self,
      file_paths_arrays: typing.Dict[str, typing.List[file_handlers.FileType]] = {},
      disk_name: str = None,
      disk_size: int = 0,
      dry_run = False
    ):
        """
        Generates the commands to (i) create and (ii) destroy a persistent disk.
        If a dict of inputs -> [FileTypes] is provided, disk will be named
        according to the FileType hashes, and sized according to the filesizes.
        """
        is_scratch_disk = len(file_paths_arrays) == 0
        #
        # if we already have files in mind to localize
        if not is_scratch_disk:
            # flatten file_paths dict
            # for non-array inputs, { inputName : path }
            # disambiguate array inputs with numerical suffix, e.g. { inputName + "_0" : path }
            file_paths = []
            for k, v in file_paths_arrays.items(): 
                n_suff = 0
                for x in v:
                    file_paths.append([k, n_suff, x.path, x.hash, x.size, not (isinstance(x, file_handlers.StringLiteral) or isinstance(x, file_handlers.HandleRODISKURL or x.localization_mode == "stream")), isinstance(x, file_handlers.HandleRODISKURL)])
                    n_suff += 1

            ## Create dataframe of files' attributes
            F = pd.DataFrame(file_paths, columns = ["input", "array_idx", "path", "hash", "size", "localize", "rdpassthru"])
            F["file_basename"] = F["path"].apply(os.path.basename)

            ## if there are no files to localize, throw an error
            if len(F) > 0 and (~(F["localize"] | F["rdpassthru"])).all():
                raise ValueError("You requested to localize files to disk, but no localizable inputs were given:\n{}\nInputs must be valid local paths or supported remote URLs.".format(", ".join([f"{input} : \"{path}\"" for _, path, input in F[["path", "input"]].itertuples()])))

            ## if some files cannot be localized, throw a warning
            if (~(F["localize"] | F["rdpassthru"])).any():
                canine_logging.warning("You requested to localize files to disk, but some inputs cannot be localized. Inputs must be valid local paths or supported remote URLs. The following inputs will be skipped:\n{}".format(", ".join([f"{input} : \"{path}\"" for _, path, input in F.loc[~(F["localize"] | F["rdpassthru"]), ["path", "input"]].itertuples()])))

            ## handle special case if we are only passing through rodisk URLs; this is like a dry run
            if F["rdpassthru"].any() and not F["localize"].any():
                return None, [], [], F.loc[F["rdpassthru"], :].groupby("input")["path"].agg(list).to_dict()

            ## Disk name is determined by input names, files' basenames, and hashes
            disk_name = "canine-" + \
              file_handlers.hash_set(set(
                F.loc[F["localize"], "input"] + "_" + \
                F.loc[F["localize"], "file_basename"] + "_" + \
                F.loc[F["localize"], "hash"]
              ))

            canine_logging.info1("Disk name is {}".format(disk_name))

            ## create RODISK URLs for files being localized to disk
            F["disk_path"] = F["path"]
            F.loc[F["localize"], "disk_path"] = "rodisk://" + disk_name + "/" + F.loc[F["localize"], ["input", "file_basename"]].apply(lambda x: "/".join(x), axis = 1)

            ## Calculate disk size
            disk_size = F.loc[F["localize"], "size"].sum()
            #There used to be a bug here: Note that GB is 10^9, while gibibyte is 2^30, so when 
            #a disk is allocated in google cloud, the size is in GB. So, don't divide the size by
            #0.95*2**30 (as was done before) but instead with 0.95*10**9
            disk_size = max(10, 1 + int(disk_size / (0.95*10**9))) # bytes -> gigabytes (not gibibytes) with 5% safety margin

            ## Save RODISK paths for subsequent use by downstream tasks
            rodisk_paths = F.loc[F["localize"], :].groupby("input")["disk_path"].agg(list).to_dict()

            # if we are passing through any rodisk paths, add them to the rodisk_paths dict
            if F["rdpassthru"].any():
                rodisk_paths = {**rodisk_paths, **F.loc[F["rdpassthru"], :].groupby("input")["path"].agg(list).to_dict()}

        #
        # otherwise, we create a blank disk with a given name (if specified),
        # otherwise random
        else:
            disk_name = "canine-scratch-{}".format(disk_name if disk_name is not None else os.urandom(4).hex())
            disk_size = max(10, disk_size)
            rodisk_paths = None

        ## this is a dry run (i.e., don't actually make disk, just return paths)
        if dry_run:
            return None, [], [], rodisk_paths

        ## mount prefix
        mount_prefix = "/mnt/rwdisks"
        disk_mountpoint = mount_prefix + "/" + disk_name

        ## Check if the disk already exists
        # resolved here rather than at import time so that importing canine does
        # not require a discoverable zone; once outside the retry loop so the
        # backoff path doesn't re-query the metadata server on every iteration
        zone = get_default_gcp_zone()
        disk_client = gcloud_disk_client()
        disk_exists = False
        backoff = 60 + random.randint(0, 10)
        while True:
            try:
                disk_attrs = disk_client.get(disk = disk_name, zone = zone, project = PROJECT)
                disk_exists = True
                break
            except google.api_core.exceptions.NotFound:
                break
            except google.api_core.exceptions.Forbidden as e:
                # trigger exponential backoff if quota exceeded
                if "Quota exceeded" in e.message:
                    time.sleep(backoff)
                    backoff *= 2
                    if backoff > 1200:
                        raise RuntimeError("Persistent disk {} cannot be queried due to gcloud quota limit.".format(disk_name))

        # disk exists
        if disk_exists:
            # disk has been finalized
            if "finished" in disk_attrs.labels:
                canine_logging.info1("Found existing disk {}".format(disk_name))

                # no additional localization or teardown commands necessary
                # if this is a localization disk, inputs will be transformed into
                # rodisk *inputs*, whose localization commands to mount will
                # be handled downstream
                # if this is a scratch disk, it will be mounted read-only, and
                # the script will be skipped
                return disk_mountpoint, [], [], (rodisk_paths if not is_scratch_disk else "rodisk://" + disk_name)
            
            # check if disk is currently being built by another instance; notify user
            elif len(disk_attrs.users):
                canine_logging.info1("Persistent disk {} is already being constructed by another instance; waiting to finalize ...".format(disk_name))
                # blocking will happen automatically in localization script

            # disk has not been finalized; finish it
            else:
                canine_logging.info1("Resuming creation of persistent disk {}".format(disk_name))

        # disk does not exist; create it
        else: 
            canine_logging.info1("Creating new persistent disk {}".format(disk_name))

        ## Generate disk creation script
        localization_script = [
            'set -eux',
            'GCP_DISK_NAME={}'.format(disk_name), # TODO: save these as exports that get sourced in setup.sh
            'GCP_DISK_SIZE={}'.format(disk_size),
            'GCP_TSNT_DISKS_DIR={}'.format(mount_prefix),

            'echo "Saving outputs to scratch disk ${GCP_DISK_NAME}" >&2' if is_scratch_disk else 'echo "Localizing inputs to cache disk ${GCP_DISK_NAME} (${GCP_DISK_SIZE}GB)" >&2',

            ## create disk
            'if ! gcloud compute disks describe "${GCP_DISK_NAME}" --zone ${CANINE_NODE_ZONE}; then',
            'gcloud compute disks create "${GCP_DISK_NAME}" --size "${GCP_DISK_SIZE}GB" --type pd-standard --zone "${CANINE_NODE_ZONE}" --labels wolf=canine',
            'fi',

            ## if this is a scratch disk, label it as such
            'gcloud compute disks add-labels "${GCP_DISK_NAME}" --zone "$CANINE_NODE_ZONE" --labels scratch=yes' if is_scratch_disk else '',

            ## attach as read-write, using same device-name as disk-name
            'if [[ ! -e /dev/disk/by-id/google-${GCP_DISK_NAME} ]]; then',
            'gcloud compute instances attach-disk "$CANINE_NODE_NAME" --zone "$CANINE_NODE_ZONE" --disk "$GCP_DISK_NAME" --device-name "$GCP_DISK_NAME" || { [ $? == 5 ] && exit 5 || true; }',
            'fi',

            ## wait for disk to attach, with exponential backoff up to 2 minutes
            'DELAY=1',
            'while [ ! -b /dev/disk/by-id/google-${GCP_DISK_NAME} ]; do',
              ## check if disk is being created by _another_ instance (grep -qv $CANINE_NODE_NAME)
              'if gcloud compute disks describe $GCP_DISK_NAME --zone $CANINE_NODE_ZONE --format "csv(users)[no-heading]" | grep \'^http\' | grep -qv "$CANINE_NODE_NAME"\'$\'; then',
                'echo "ERROR: Disk being created by another instances. Will retry when finished." >&2',
                'exit 5', # cause task to be requeued
              'fi',
              # TODO: what if the task exited on the other
              #       instance without running the teardown script
              #       (e.g. task was cancelled), in which case we'd want to forcibly
              #       detach the disk from the other instance
              #       are there any scenarios in which this would be a bad idea?

              ## if disk is not being created by another instance, it might just be taking a bit to attach. give it a chance to appear in /dev
              '[ $DELAY -gt 128 ] && { echo "ERROR: Exceeded timeout trying to attach disk" >&2; exit 5; } || :',
              'sleep $DELAY; ((DELAY *= 2))',

              # try attaching again if delay has exceeded 8 seconds
              # if disk has attached successfully according to GCP, but disk doesn't appear in /dev,
              # this means that the node is bad
              'if [ $DELAY -gt 8 ]; then',
                'gcloud compute instances attach-disk "$CANINE_NODE_NAME" --zone "$CANINE_NODE_ZONE" --disk "$GCP_DISK_NAME" --device-name "$GCP_DISK_NAME" || :',
                'if gcloud compute disks describe $GCP_DISK_NAME --zone $CANINE_NODE_ZONE --format "csv(users)[no-heading]" | grep \'^http\' | grep -q $CANINE_NODE_NAME\'$\' && [ ! -b /dev/disk/by-id/google-${GCP_DISK_NAME} ]; then',
                  'sudo touch /.fatal_disk_issue_sentinel',
                  'echo "ERROR: Node cannot attach disk; node is likely bad. Tagging for deletion. Will retry localization on another node." >&2',
                  'exit 5',
                'fi',
              'fi',
            'done',

            ## format disk
            'if [[ $(sudo blkid -o value -s TYPE /dev/disk/by-id/google-${GCP_DISK_NAME}) != "ext4" ]]; then',
            'sudo mkfs.ext4 -m 0 -E lazy_itable_init=0,lazy_journal_init=0,discard /dev/disk/by-id/google-${GCP_DISK_NAME}',
            'fi',

            ## mount
            'if [[ ! -d "$GCP_TSNT_DISKS_DIR/$GCP_DISK_NAME" ]]; then',
            'sudo mkdir -p "$GCP_TSNT_DISKS_DIR/$GCP_DISK_NAME"',
            'fi',
            'if ! mountpoint -q "$GCP_TSNT_DISKS_DIR/$GCP_DISK_NAME"; then',
            'sudo timeout -k 30 30 mount -o discard,defaults /dev/disk/by-id/google-"${GCP_DISK_NAME}" "$GCP_TSNT_DISKS_DIR/$GCP_DISK_NAME"',
            'sudo chown $(id -u):$(id -g) "${GCP_TSNT_DISKS_DIR}/${GCP_DISK_NAME}"',
            'sudo chmod 774 "${GCP_TSNT_DISKS_DIR}/${GCP_DISK_NAME}"',
            'fi',

            ## lock the disk
            # will be unlocked during teardown script (or if script crashes). this
            # is a way of other processes surveying if this is a hanging disk.
            'flock -os "$GCP_TSNT_DISKS_DIR/$GCP_DISK_NAME" sleep infinity & echo $! >> ${CANINE_JOB_INPUTS}/.scratchdisk_lock_pids',
        ]

        # scratch disks dynamically resize as they get full.
        # if disk has <30% free space remaining, increase its size by 60%
        if is_scratch_disk:
            localization_script += [
              'cat <<EOF > $CANINE_JOB_ROOT/.diskresizedaemon.sh',
              'DISK_DIR=$GCP_TSNT_DISKS_DIR/$GCP_DISK_NAME',
              'while true; do',
              '  sleep 10',
              '  if ! mountpoint \$DISK_DIR &> /dev/null; then echo "No disk mounted to \$DISK_DIR" >&2; continue; else',
              '    DISK_SIZE_GB=\$(df -B1G "\$DISK_DIR" | awk \'NR == 2 { print int(\$3 + \$4) }\')',
              '    FREE_SPACE_GB=\$(df -B1G "\$DISK_DIR" | awk \'NR == 2 { print int(\$4) }\')',
              '    if [[ \$((100*FREE_SPACE_GB/DISK_SIZE_GB)) -lt 30 ]]; then',
              '      echo "Scratch disk almost full (\${FREE_SPACE_GB}GB free; \${DISK_SIZE_GB}GB total); resizing +60%" >&2',
              '      gcloud_exp_backoff compute disks resize $GCP_DISK_NAME --quiet --zone $CANINE_NODE_ZONE --size \$((DISK_SIZE_GB*160/100))',
              '      sudo resize2fs /dev/disk/by-id/google-${GCP_DISK_NAME}',
              '    fi',
              '  fi',
              'done',
              'EOF',
              'set +e; bash $CANINE_JOB_ROOT/.diskresizedaemon.sh &',
              'echo $! > $CANINE_JOB_ROOT/.diskresizedaemon_pid',
              'set -e',
            ]

        # * disk unmount or deletion script (to append to teardown_script)
        #   -> need to be able to pass option to not delete, if using as a RODISK later
        teardown_script = [
          ## sync any cached data
          'sync',

          ## release all locks obtained by this job
          'if [ -f ${CANINE_JOB_INPUTS}/.scratchdisk_lock_pids ]; then',
          '  while read -r pid; do',
          '    kill $pid',
          '  done < ${CANINE_JOB_INPUTS}/.scratchdisk_lock_pids',
          '  rm -f ${CANINE_JOB_INPUTS}/.scratchdisk_lock_pids',
          'fi',

          ## unmount disk, with exponential backoff up to 2 minutes
          'DELAY=1',
          'while mountpoint {}/{} > /dev/null; do'.format(mount_prefix, disk_name),
          '  (cd /; sudo umount {}/{}) || :'.format(mount_prefix, disk_name),
          '  [ $DELAY -gt 4 ] && {{ echo "Warning: attempting lazy unmount!" >&2; (cd /; sudo umount -l {}/{}); }} || :'.format(mount_prefix, disk_name),
          '  [ $DELAY -gt 128 ] && { echo "Exceeded timeout trying to unmount disk" >&2; exit 1; } || :',
          '  sleep $DELAY; ((DELAY *= 2))',
          'done',

          ## detach disk
          'gcloud compute instances detach-disk $CANINE_NODE_NAME --zone $CANINE_NODE_ZONE --disk {}'.format(disk_name),
          # TODO: add command to optionally delete disk

          ## kill disk resizing daemon, if running
          'kill $(cat .diskresizedaemon_pid) || : &> /dev/null'
        ]

        # scratch disks get labeled "finalized" if the task ran OK.
        if is_scratch_disk:
            teardown_script += [
              'if [[ ! -z $CANINE_JOB_RC && $CANINE_JOB_RC -eq 0 ]]; then gcloud compute disks add-labels "{disk_name}" --zone "$CANINE_NODE_ZONE" --labels finished=yes{protect_string}; fi'.format(
                disk_name = disk_name,
                protect_string = (",protect=yes" if self.protect_disk else "")
              )
            ]

            # we also need to leave the job workspace directory before anything else
            # in the scratch disk teardown script to allow the disk to unmount
            teardown_script = ["cd $CANINE_JOB_ROOT"] + teardown_script

        return disk_mountpoint, localization_script, teardown_script, rodisk_paths

    def backend_zone(self):
        """
        Zone this cluster's nodes live in, used to pick the localization bucket's
        region. gcpTransient records it under "zone"; imageTransient -- and
        therefore DockerTransient -- under "compute_zone", so both keys have to
        be consulted or the configured zone is silently ignored. Auto-detection
        is the last resort and raises if it can't determine one.
        """
        return (
          self.backend.config.get("zone")
          or self.backend.config.get("compute_zone")
          or get_default_gcp_zone()
        )

    def create_bucket_mount(self,
      file_paths_arrays: typing.Dict[str, typing.List[file_handlers.FileType]] = {},
      dry_run = False
    ):
        """
        Replaces create_persistent_disk() for the localize_to_persistent_disk
        case only (use_scratch_disk is unaffected and continues to use
        create_persistent_disk()). Each localization gets its own regional
        bucket, named wolf-<project_number>-<region>-<content hash> -- the direct
        analogue of the old content-addressed RODISK, created on demand, labelled
        with its state, consumed via a read-only gcsfuse mount, and never
        explicitly deleted (objects age out under the lifecycle rule applied at
        creation).

        Emits no script commands of its own; bucket_upload_script() turns the
        returned plan into the worker-side commands. Returns
        (bucket_prefix, skip_upload, bucketmount_paths, upload_plan):
          * bucket_prefix: "gs://<bucket>". None on a dry run.
          * skip_upload: True only on a dry run, where the caller wants the
            bucketmount URLs as string literals and will not mount anything.
            Otherwise False: whether an upload is actually needed is decided
            worker-side, since bucket creation is the mutex and the bucket label
            carries the state (see bucket_upload_script).
          * bucketmount_paths: dict of input name -> [bucketmount:// URLs],
            for the caller to save in self.rodisk_paths (same attribute
            used for disk-backed RODISKs, since wolf.localization.LocalizeToBucket
            reads it generically regardless of backing mechanism).
          * upload_plan: list of UploadItem for bucket_upload_script() to emit.
            url-mode inputs only -- "local" inputs are controller-side files that
            localize_file() copies to the shared mount.

        No byte of a url-mode input transits the shared NFS mount: gs:// sources
        are copied server-side and everything else is written through a
        read-write gcsfuse mount on the worker. Sending them across the
        controller's disk twice -- once on download, once on re-upload -- is the
        regression this replaced.
        """
        # flatten file_paths dict (same scheme as create_persistent_disk).
        # NB: reading x.size here is load-bearing beyond the size itself --
        # HandleGSURL._get_size() goes through blob(), which is also what sets
        # x.is_dir, and the upload destination shaping below depends on it.
        file_paths = []
        for k, v in file_paths_arrays.items():
            n_suff = 0
            for x in v:
                file_paths.append([
                  k, n_suff, x, x.path, x.hash, x.size,
                  not (
                    isinstance(x, file_handlers.StringLiteral)
                    or isinstance(x, (file_handlers.HandleRODISKURL, file_handlers.HandleBucketMountURL))
                    or x.localization_mode == "stream"
                  ),
                  isinstance(x, (file_handlers.HandleRODISKURL, file_handlers.HandleBucketMountURL))
                ])
                n_suff += 1

        F = pd.DataFrame(file_paths, columns = ["input", "array_idx", "fh", "path", "hash", "size", "localize", "rdpassthru"])
        F["file_basename"] = F["path"].apply(os.path.basename)

        if len(F) > 0 and (~(F["localize"] | F["rdpassthru"])).all():
            raise ValueError("You requested to localize files to a bucket, but no localizable inputs were given:\n{}\nInputs must be valid local paths or supported remote URLs.".format(", ".join([f"{input} : \"{path}\"" for _, path, input in F[["path", "input"]].itertuples()])))

        if (~(F["localize"] | F["rdpassthru"])).any():
            canine_logging.warning("You requested to localize files to a bucket, but some inputs cannot be localized. Inputs must be valid local paths or supported remote URLs. The following inputs will be skipped:\n{}".format(", ".join([f"{input} : \"{path}\"" for _, path, input in F.loc[~(F["localize"] | F["rdpassthru"]), ["path", "input"]].itertuples()])))

        # handle special case if we are only passing through bucketmount/RODISK URLs; this is like a dry run
        if F["rdpassthru"].any() and not F["localize"].any():
            return None, True, F.loc[F["rdpassthru"], :].groupby("input")["path"].agg(list).to_dict(), []

        # content is addressed by input names, files' basenames, and hashes --
        # same scheme as create_persistent_disk's disk_name, so identical input
        # sets converge on the same location
        content_hash = file_handlers.hash_set(set(
          F.loc[F["localize"], "input"] + "_" + \
          F.loc[F["localize"], "file_basename"] + "_" + \
          F.loc[F["localize"], "hash"]
        ))

        # one bucket per localization, treated the way a RODISK used to be:
        # created on demand, labelled with its state, mounted read-only by
        # consumers, never explicitly deleted (objects age out via the lifecycle
        # rule applied at creation).
        zone = self.backend_zone()
        bucket = localization_bucket_name(
          get_project_number(self.project), _zone_to_region(zone), content_hash
        )
        canine_logging.info1("Localization bucket is {}".format(bucket))

        # relative path of each object within its bucket. Both the
        # bucketmount:// URL and the upload destination are built from this one
        # column, so they cannot disagree -- the destination used to be built
        # from the possibly-mangled basename in job_setup_teardown while the URL
        # used the unmangled one.
        F["object_path"] = F["path"]
        F.loc[F["localize"], "object_path"] = \
          F.loc[F["localize"], ["input", "file_basename"]].apply(lambda x: "/".join(x), axis = 1)

        F["bucket_path"] = F["path"]
        F.loc[F["localize"], "bucket_path"] = \
          "bucketmount://" + bucket + "/" + F.loc[F["localize"], "object_path"]

        bucketmount_paths = F.loc[F["localize"], :].groupby("input")["bucket_path"].agg(list).to_dict()

        if F["rdpassthru"].any():
            bucketmount_paths = {**bucketmount_paths, **F.loc[F["rdpassthru"], :].groupby("input")["path"].agg(list).to_dict()}

        if dry_run:
            return None, True, bucketmount_paths, []

        # nothing is checked controller-side: the worker decides whether to
        # upload, since bucket creation is the mutex and the bucket label carries
        # the state. Emitting the plan unconditionally is what makes a concurrent
        # sibling wait rather than re-upload.
        upload_plan = [
          UploadItem(
            fh = r.fh,
            dest = "gs://{}/{}".format(bucket, r.object_path),
            kind = (
              "server_side" if isinstance(r.fh, file_handlers.HandleGSURL)
              else "copy" if r.fh.localization_mode == "local"
              else "mount"
            ),
          )
          for r in F.loc[F["localize"], :].itertuples()
        ]
        return "gs://{}".format(bucket), False, bucketmount_paths, upload_plan

    def bucket_upload_script(self, upload_plan, bucket_prefix, region):
        """
        Worker-side commands that place every input in `upload_plan` into the
        localization bucket without any of it transiting the shared NFS mount.

        gs:// sources are copied server-side (a GCS rewrite -- no bytes touch
        any VM at all). Everything else is downloaded straight into a read-write
        gcsfuse mount of the same bucket, so the bytes stream up as they arrive
        instead of being staged on local disk. Both paths keep the shared NFS
        mount entirely out of the data path, which is the point of this method.

        The rw mount relies on gcsfuse streaming writes (--enable-streaming-writes,
        default true since gcsfuse 3.x; the worker image pins 3.11.2). Without
        them gcsfuse buffers the whole object under --temp-dir on the 25GB boot
        disk and ENOSPCs on a large input. Streaming writes are append-only from
        offset 0, so the download must be sequential -- canine's handlers are
        (`aws s3api get-object --range` piped through `cat >>`, `curl -C - -o`),
        but a multipart/parallel writer would silently fall back to staging.

        Concurrency is coordinated entirely through the bucket itself:

          * `buckets create` is the mutex. Bucket names are globally unique, so
            of N racers exactly one creates it and the rest get HTTP 409. No
            compare-and-swap on an object is needed (and `gcloud storage buckets
            update` has no --if-metageneration-match to offer one anyway).
          * The `wolf` bucket label carries the state: "working" while an upload
            is in flight, "success" once every object has landed.
          * Objects are written with --custom-time so the daysSinceCustomTime
            lifecycle rule applied at creation can actually match them; without
            it the rule never fires and the bucket grows forever.
        """
        bucket = bucket_prefix[len("gs://"):] if bucket_prefix.startswith("gs://") else bucket_prefix
        burl = shlex.quote("gs://" + bucket)

        mount_dir = "/mnt/localize/{}".format(bucket)

        uploads = []
        for item in upload_plan:
            if item.kind == "server_side":
                # -n so a requeued shard does not re-copy. rp_string because the
                # *source* may be requester-pays even though our bucket is not.
                # A directory source is copied into its parent: `cp -r gs://a/d
                # gs://B/k/d` would otherwise produce gs://B/k/d/d/...
                dest = os.path.dirname(item.dest) + "/" if getattr(item.fh, "is_dir", False) else item.dest
                uploads.append(
                  '    gcloud storage cp -r -n{rp} --custom-time="$CANINE_BUCKET_CT" {src} {dst}'.format(
                    rp = item.fh.rp_string,
                    src = shlex.quote(item.fh.path),
                    dst = shlex.quote(dest),
                  )
                )
            elif item.kind == "copy":
                # Already a file on the shared mount -- typically an upstream
                # task's output. The worker uploads it from where it already
                # lives, which adds no NFS traffic beyond the read.
                # Deliberately not pre-validating that the worker can read this
                # path. The obvious checks do not work: "under self.staging_dir"
                # is wrong because upstream outputs live in sibling task dirs,
                # and os.path.ismount()/st_dev cannot see the shared mount at all
                # when the controller has it on the same block device as / (the
                # usual layout -- `mountpoint /mnt/nfs` says yes while
                # os.path.ismount says no). A worker that genuinely cannot read
                # the file gets a precise "No such file or directory" from the
                # cp below, which beats a heuristic that rejects valid inputs.
                dest = os.path.dirname(item.dest) + "/" if os.path.isdir(item.fh.path) else item.dest
                uploads.append(
                  '    gcloud storage cp -r -n --custom-time="$CANINE_BUCKET_CT" {src} {dst}'.format(
                    src = shlex.quote(item.fh.path),
                    dst = shlex.quote(dest),
                  )
                )

        # Sources with no server-side copy (s3://, drs://, GDC, http) have to be
        # moved by this VM -- but they still never touch the shared NFS mount.
        # The bucket is mounted read-write and the download is written straight
        # into it, so gcsfuse streams the bytes up as they arrive rather than
        # staging a copy on local disk.
        indirect = [x for x in upload_plan if x.kind == "mount"]
        if indirect:
            uploads += [
              '    sudo mkdir -p {md}'.format(md = shlex.quote(mount_dir)),
              '    sudo chown $(id -u):$(id -g) {md}'.format(md = shlex.quote(mount_dir)),
              # NO `--o ro` here, unlike the consumer mount. Kept on its own
              # mountpoint (/mnt/localize/...) rather than reusing
              # /mnt/bucketmounts: the consumer loop's "mount if not already
              # mounted, no backoff" logic is only safe because those mounts are
              # read-only, and nothing writable should outlive this block.
              '    timeout -k 60 60 gcsfuse --implicit-dirs {b} {md} || {{ echo "ERROR: rw bucket mount failed!" >&2; exit 1; }}'.format(
                b = shlex.quote(bucket), md = shlex.quote(mount_dir)),
            ]
            for item in indirect:
                in_mount = os.path.join(mount_dir, item.dest[len("gs://" + bucket + "/"):])
                uploads += [
                  "    " + line
                  for line in item.fh.localization_command(in_mount).split("\n")
                ]
            uploads += [
              # unmount before anything else runs: finalizes every object, and
              # leaves no writable mount exposed to the task script
              '    fusermount -u {md} || {{ echo "ERROR: could not unmount rw bucket mount" >&2; exit 1; }}'.format(
                md = shlex.quote(mount_dir)),
            ]
            # objects are only visible after the unmount finalizes them, so this
            # is what makes the wolf=success label mean "the content is there"
            uploads += [
              '    gcloud storage ls {obj} > /dev/null || {{ echo "ERROR: {obj} missing after upload" >&2; exit 1; }}'.format(
                obj = shlex.quote(x.dest)) for x in indirect
            ]
            # gcsfuse cannot set customTime on write, and an object without it is
            # invisible to the daysSinceCustomTime lifecycle rule -- it would
            # never expire and the bucket would grow forever. The gs:// path gets
            # this from `cp --custom-time`; this path has to set it afterwards.
            uploads += [
              '    gcloud storage objects update {objs} --custom-time="$CANINE_BUCKET_CT" > /dev/null'.format(
                objs = " ".join(shlex.quote(x.dest) for x in indirect)),
            ]

        all_objects = " ".join(shlex.quote(x.dest) for x in upload_plan)

        return [
          'CANINE_BUCKET_CT=$(date -u +%Y-%m-%dT%H:%M:%SZ)',
          'CANINE_BUCKET_LC=$(mktemp)',
          # the lifecycle rule must be applied at creation: a second call to set
          # it might never happen if this job dies in between
          "cat > \"$CANINE_BUCKET_LC\" <<'CANINE_LIFECYCLE_EOF'",
          '{{"rule":[{{"action":{{"type":"Delete"}},"condition":{{"daysSinceCustomTime":{days}}}}}]}}'.format(
            days = int(self.localization_expiry_days)),
          'CANINE_LIFECYCLE_EOF',
          'CANINE_BUCKET_WAITS=0',
          'while :; do',
          # --soft-delete-duration=0 disables GCS soft delete, which new buckets
          # otherwise get at 7 days. Without it every expired or deleted object
          # is retained and *billed* for a further week, so a 1-day expiry on a
          # multi-TB localization would still be paying for 7 days of soft-deleted
          # copies. This is a cache; there is nothing here worth undeleting.
          '  if gcloud storage buckets create {burl} --location={region} --soft-delete-duration=0 --lifecycle-file="$CANINE_BUCKET_LC" > /dev/null 2>&1; then'.format(burl = burl, region = shlex.quote(region)),
          '    CANINE_BUCKET_UPLOAD=1',
          '  else',
          # Two different 409s land here, and under real contention the transient
          # one dominates: "A conflicting operation is currently in progress ...
          # Please try again" means the create is still in flight, NOT that the
          # bucket is readable yet. So wait for it to resolve before reading the
          # label, rather than racing straight to describe.
          '    CANINE_BUCKET_TRIES=0',
          '    until gcloud storage buckets describe {burl} > /dev/null 2>&1; do'.format(burl = burl),
          '      if [ $CANINE_BUCKET_TRIES -gt 30 ]; then echo "ERROR: localization bucket {b} never became readable" >&2; exit 5; fi'.format(b = bucket),
          '      sleep 2; CANINE_BUCKET_TRIES=$((CANINE_BUCKET_TRIES+1))',
          '    done',
          '    CANINE_BUCKET_STATE=$(gcloud storage buckets describe {burl} --format="value(labels.wolf)" 2>/dev/null)'.format(burl = burl),
          '    if [ "$CANINE_BUCKET_STATE" == "success" ]; then',
          # content is supposed to be here -- but it may have aged out under the
          # lifecycle rule, in which case we take the bucket over and re-upload
          '      if gcloud storage ls {objs} > /dev/null 2>&1; then'.format(objs = all_objects),
          '        echo "INFO: localization bucket {b} already populated" >&2'.format(b = bucket),
          '        CANINE_BUCKET_UPLOAD=0',
          '      else',
          '        echo "INFO: localization bucket {b} expired; repopulating" >&2'.format(b = bucket),
          '        CANINE_BUCKET_UPLOAD=1',
          '      fi',
          '    elif [ "$CANINE_BUCKET_STATE" == "stale" ]; then',
          # a previous uploader timed out or died and released its claim. Take
          # over rather than waiting: nobody else is going to finish this, and
          # -n on the copies keeps a double take-over harmless.
          '      echo "INFO: taking over stale localization bucket {b}" >&2'.format(b = bucket),
          '      CANINE_BUCKET_UPLOAD=1',
          '    else',
          # "working", or absent because the owner created the bucket but has not
          # labelled it yet (buckets create cannot set labels) -- either way,
          # somebody else is uploading, so wait rather than duplicating the work
          '      if [ $CANINE_BUCKET_WAITS -ge {tries} ]; then'.format(tries = self.bucket_upload_wait_tries),
          '        echo "WARNING: timed out waiting for {b}; releasing claim and retrying elsewhere" >&2'.format(b = bucket),
          '        gcloud storage buckets update {burl} --update-labels=wolf=stale > /dev/null 2>&1 || :'.format(burl = burl),
          '        exit 5',
          '      fi',
          '      sleep 60; CANINE_BUCKET_WAITS=$((CANINE_BUCKET_WAITS+1))',
          '      continue',
          '    fi',
          '  fi',
          '  if [ "$CANINE_BUCKET_UPLOAD" == "1" ]; then',
          '    gcloud storage buckets update {burl} --update-labels=wolf=working > /dev/null'.format(burl = burl),
        ] + uploads + [
          '    gcloud storage buckets update {burl} --update-labels=wolf=success > /dev/null'.format(burl = burl),
          '  else',
          # refresh the expiry clock so content still in use is not reclaimed
          '    gcloud storage objects update {objs} --custom-time="$CANINE_BUCKET_CT" > /dev/null 2>&1 || :'.format(objs = all_objects),
          '  fi',
          '  break',
          'done',
          'rm -f "$CANINE_BUCKET_LC"',
        ]

    def bucketmount_lease_register(self):
        """
        Commands registering this job as a live consumer of the bucket it just
        mounted, so DeleteLocalizedFiles will refuse to delete content another
        worker is still reading.

        GCS has no server-side notion of "who has this gcsfuse-mounted" --
        gcsfuse is just a client -- so the bucket itself is the only state every
        VM can see. Each consumer drops a marker under _MOUNTS/ and removes it
        during teardown (see bucketmount_lease_release).

        Written with --custom-time so a lease orphaned by a crashed job is
        reaped by the same lifecycle rule as everything else, rather than
        blocking deletion of that content forever. Failure to write one is a
        warning, never fatal: this is bookkeeping, and losing it costs a
        redundant re-localization at worst.

        MUST be emitted unconditionally, outside the "mount if not already
        mounted" block. A second job landing on a node where the bucket is
        already mounted does no mounting of its own, but is every bit as much a
        consumer -- if it skipped taking a lease, the first job's teardown would
        free the content while the second was still reading it.

        The key is hostname + job + array task so it is unique across VMs and
        across array shards co-scheduled on one VM; teardown removes the exact
        URLs it recorded, never a pattern, so it can only ever free its own.
        """
        return [
          'CANINE_BUCKETMOUNT_LEASE=gs://${CANINE_BUCKETMOUNT}/_MOUNTS/$(hostname)-${SLURM_JOB_ID:-$$}-${SLURM_ARRAY_TASK_ID:-0}',
          'gcloud storage cp --custom-time="$(date -u +%Y-%m-%dT%H:%M:%SZ)" /dev/null "${CANINE_BUCKETMOUNT_LEASE}" > /dev/null 2>&1 || echo "WARNING: could not register mount lease for ${CANINE_BUCKETMOUNT}" >&2',
          'echo "${CANINE_BUCKETMOUNT_LEASE}" >> ${CANINE_JOB_INPUTS}/.bucketmount_leases',
        ]

    def bucketmount_heartbeat_start(self):
        """
        Commands starting a background loop that re-stamps customTime on this
        bucket's contents for as long as the job holds the mount.

        customTime is otherwise written once and never renewed, so a job holding
        a mount longer than localization_expiry_days loses two things at once:

          * its own _MOUNTS/ lease, after which a sibling flow's
            DeleteLocalizedFiles sees no holder and deletes content that is
            actively mounted; and
          * the data objects themselves -- the lifecycle rule carries no prefix
            filter, so GCS deletes the inputs out from under gcsfuse with no
            second flow involved at all. gcsfuse pins the object generation at
            open(), so an in-flight read fails rather than returning wrong
            bytes, but it fails.

        One bucket-wide update covers the lease and the data objects together.
        Failure to refresh is never fatal -- same reasoning as the lease write --
        and if the worker dies the loop dies with it, so content expires on the
        normal schedule rather than being pinned forever.
        """
        return [
          'CANINE_BUCKETMOUNT_HB=${CANINE_JOB_INPUTS}/.bucketmount_heartbeat_${CANINE_BUCKETMOUNT}.sh',
          # Quoted heredoc: the body is written verbatim, so $(date) is evaluated
          # per beat rather than frozen at write time. That also means the bucket
          # cannot come from CANINE_BUCKETMOUNT -- it is a plain shell variable,
          # never exported, so the child would see an empty string and update
          # "gs:///**". Pass it as $1 instead.
          "cat > \"${CANINE_BUCKETMOUNT_HB}\" <<'CANINE_HEARTBEAT_EOF'",
          'while true; do',
          '  sleep {}'.format(int(self.bucketmount_heartbeat_seconds)),
          '  gcloud storage objects update "gs://$1/**" --custom-time="$(date -u +%Y-%m-%dT%H:%M:%SZ)" > /dev/null 2>&1 || :',
          'done',
          'CANINE_HEARTBEAT_EOF',
          'set +e; bash "${CANINE_BUCKETMOUNT_HB}" "${CANINE_BUCKETMOUNT}" & echo $! >> ${CANINE_JOB_INPUTS}/.bucketmount_heartbeat_pids; set -e',
        ]

    def bucketmount_heartbeat_stop(self):
        """
        Teardown counterpart to bucketmount_heartbeat_start(): stops this job's
        refresh loops so the content resumes ageing normally once nobody holds it.
        """
        return [
          'if [ -f ${CANINE_JOB_INPUTS}/.bucketmount_heartbeat_pids ]; then',
          '  while read -r pid; do',
          '    kill $pid 2> /dev/null || :',
          '  done < ${CANINE_JOB_INPUTS}/.bucketmount_heartbeat_pids',
          '  rm -f ${CANINE_JOB_INPUTS}/.bucketmount_heartbeat_pids',
          'fi',
        ]

    def bucketmount_lease_release(self):
        """
        Teardown counterpart to bucketmount_lease_register(): drops this job's
        mount leases so the content becomes deletable once no worker holds it.
        """
        return [
          'if [ -f ${CANINE_JOB_INPUTS}/.bucketmount_leases ]; then',
          '  while read -r CANINE_LEASE; do',
          '    gcloud storage rm "${CANINE_LEASE}" > /dev/null 2>&1 || :',
          '  done < ${CANINE_JOB_INPUTS}/.bucketmount_leases',
          '  rm -f ${CANINE_JOB_INPUTS}/.bucketmount_leases',
          'fi',
        ]

    def bucketmount_reachability_check(self):
        """
        Commands verifying that every input symlinked into the bucket mount
        currently being processed actually resolves.

        A mounted-but-empty bucket is gcsfuse's nastiest failure mode: the mount
        succeeds, `mountpoint -q` passes, and reads come back ENOENT far away
        from the cause. Two ways to land there -- the objects were never written,
        or this node already had the bucket mounted from before they were (the
        loop skips remounting an existing mountpoint) and its metadata cache is
        stale.

        Exits 5 rather than 1: both causes are node-local or transient, so the
        shard is worth retrying elsewhere rather than failing the workflow.
        Expects CANINE_JOB_INPUTS, CANINE_BUCKETMOUNT and CANINE_BUCKETMOUNT_DIR
        to be set -- i.e. this belongs inside the per-bucket mount loop.
        """
        return [
          'for CANINE_BM_LINK in $(find ${CANINE_JOB_INPUTS} -maxdepth 1 -type l 2> /dev/null); do',
          '  if [[ "$(readlink "$CANINE_BM_LINK")" == ${CANINE_BUCKETMOUNT_DIR}/* && ! -e "$CANINE_BM_LINK" ]]; then',
          '    echo "ERROR: $CANINE_BM_LINK does not resolve inside bucket mount ${CANINE_BUCKETMOUNT}; retrying on another node" >&2',
          '    exit 5',
          '  fi',
          'done',
        ]

    def job_setup_teardown(self, jobId: str, patterns: typing.Dict[str, str], transport = None) -> typing.Tuple[str, str, str, typing.Dict[str, typing.List[str]]]:
        """
        Returns a tuple of (setup script, localization script, teardown script) for the given job id.
        Must call after pre-scanning inputs
        """

        # generate job variable, exports, and localization_tasks arrays
        # - job variables and exports are set when setup.sh is _sourced_
        # - localization tasks are run when localization.sh is _run_
        job_vars = set()
        exports = [
            'export CANINE_NODE_NAME=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/name 2> /dev/null)',
            'export CANINE_NODE_ZONE=$(basename $(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/zone 2> /dev/null))'
        ]
        array_exports = {}
        canine_rodisks = []
        canine_bucketmounts = []
        docker_args = ['-v $CANINE_ROOT:$CANINE_ROOT']
        localization_tasks = [
            'if [[ -d $CANINE_JOB_INPUTS ]]; then cd $CANINE_JOB_INPUTS; fi'
        ]

        #
        # create creation script for persistent disk, if specified
        if self.localize_to_persistent_disk or self.use_scratch_disk:
            localization_tasks += ["[ -f .scratchdisk_lock_pids ] && rm .scratchdisk_lock_pids || :"]

        if self.localize_to_persistent_disk:
            # FIXME: we don't have an easy way of parsing which inputs are common
            #        to all shards at this point. if every localizable input is
            #        common to each shard, then we'll upload the same content
            #        multiple times, once per shard. thus, for now we suboptimally
            #        just localize the same file multiple times.

            bucket_prefix, _, bucketmount_paths, upload_plan = self.create_bucket_mount(self.inputs[jobId], dry_run = self.persistent_disk_dry_run)

            # save bucketmount:// URLs for files saved to the bucket for later use
            self.rodisk_paths[jobId] = bucketmount_paths

            # The worker decides for itself whether to upload -- bucket creation
            # is the mutex and the bucket label carries the state -- so the
            # script is emitted unconditionally. That is exactly what makes a
            # concurrent sibling wait instead of re-uploading the same content.
            if not self.persistent_disk_dry_run and len(upload_plan):
                zone = self.backend_zone()
                localization_tasks += self.bucket_upload_script(
                  upload_plan, bucket_prefix, _zone_to_region(zone)
                )

            # every localizable input is now bucket-resident (or will be, by the
            # time the script above finishes), so consume them all through the
            # read-only mount rather than as files to download
            for k, v_array in self.rodisk_paths[jobId].items():
                # If prepare_job_inputs already made a redundant copy of this
                # input, drop it -- the content is going to the bucket instead.
                #
                # The localized_path != path guard is load-bearing:
                # FileType.__init__ seeds localized_path with the *original*
                # path, and localize_file() is what later repoints it at the
                # copy. Under this flow local inputs are turned into bucket
                # mounts before that ever runs, so an unguarded rm here deletes
                # the upstream task's own output -- which is exactly how a
                # previous run destroyed its produce/ outputs and left only the
                # .crc32c sidecars behind.
                for v in self.inputs[jobId][k]:
                    if v.localization_mode == "local" and v.localized_path != v.path:
                        localization_tasks += ["if [ -f {0} -o -d {0} ]; then rm -f {0}; fi".format(v.localized_path)]

                # transform this input into a bucket-mount FileType, to be mounted
                if not self.persistent_disk_dry_run:
                    self.inputs[jobId][k] = [file_handlers.HandleBucketMountURL(v) for v in v_array]

                # if this is a dry run, we are only interested in the bucketmount URL string literals;
                # we will not actually be attempting to mount it. this is mainly
                # for wolf.LocalizeToBucket, which returns bucketmount path inputs
                # as its outputs. this would likely not be useful for most others
                # tasks, since they would have no idea what to do with a bucketmount string
                # literal
                else:
                    self.inputs[jobId][k] = [file_handlers.StringLiteral(v) for v in v_array]

        #
        # create creation/teardown scripts for scratch disk, if specified
        scratch_disk_already_exists = False
        if self.use_scratch_disk:
            scratch_disk_prefix, \
            scratch_disk_creation_script, \
            scratch_disk_teardown_script, \
            finished_scratch_disk_url = self.create_persistent_disk(
              disk_name = self.scratch_disk_name + "-" + jobId, # one scratch disk per shard
              disk_size = self.scratch_disk_size
            )

            localization_tasks += scratch_disk_creation_script

            # if scratch disk already exists and is finalized (as indicated by
            # blank disk creation script), mount it read-only using the same
            # machinery we use to mount any other RODISK.
            if len(scratch_disk_creation_script) == 0:
                scratch_disk_already_exists = True

                canine_rodisks.append(finished_scratch_disk_url[9:]) # strip 'rodisk://' prefix
                exports += ["export CANINE_RODISK_{}={}".format(len(canine_rodisks), finished_scratch_disk_url[9:])] # strip 'rodisk://' prefix
                exports += ["export CANINE_RODISK_DIR_{}={}".format(len(canine_rodisks), scratch_disk_prefix)]

                # we also need to bypass running the script if scratch_disk_job_avoid 
                # is True. this will happen later

        local_download_size = self.local_download_size.get(jobId, 0)
        disk_name = None
        if local_download_size > 0 and self.temporary_disk_type is not None:
            #There used to be a bug here: Note that GB is 10^9, while gibibyte is 2^30, so when 
            #the size is calculated, don't divide the size by
            #0.95*2**30 (as was done before) but instead with 0.95*10**9
            local_download_size = max(
                10,
                1+int(local_download_size / (0.95*10**9)) # bytes -> gigabytes (not gibibytes) with 5% safety margin
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
                'if [[ -z "$CANINE_NODE_NAME" ]]; then echo "Unable to provision disk (not running on GCE instance). Attempting download to directory on boot disk" >&2; else',
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

                'sudo timeout -k 30 30 mount -o discard,defaults /dev/disk/by-id/google-{} $CANINE_LOCAL_DISK_DIR'.format(
                    device_name,
                ),
                'sudo chmod -R a+rwX {}'.format(self.local_download_dir),
                'fi'
            ]
            docker_args.append('-v $CANINE_LOCAL_DISK_DIR:$CANINE_LOCAL_DISK_DIR')

        def export_writer(key, value, is_array):
            if not is_array:
                exports.append("export {}={}".format(key, value))
            else:
                if key not in array_exports:
                    array_exports[key] = []
                array_exports[key].append(value)

        def sensitive_ext_extract(basename):
            """
            Many bioinformatics tools require a index file to exist in the same directory
            as its data file and have the same basename. Basename mangling must be sensitive
            to these namespace constraints.
            """
            # non exhaustive set of sensitive bioinformatic extensions
            search = re.search('.*?(\.bam|\.bam\.bai|\.fa|\.fa\.fai|\.fasta|\.fasta\.fai|\.vcf\.gz|\.vcf\.gz\.tbi|\.vcf\.gz\.csi)$', basename)
            if search is None:
                return None
            else:
                # return sensitive ext
                return search.groups()[0]

        ## now generate exports/localization commands

        # keep running list of basenames, in order to mangle them if duplicate
        # basenames exist. this keeps destination paths unique.
        basenames = {}

        compute_env = self.environment('remote')
        for key, file_handler_array in self.inputs[jobId].items():
            is_array = self.input_array_flag[jobId][key]
            for file_handler in file_handler_array: 
                # mangle basename if necessary
                basename = os.path.basename(os.path.abspath(file_handler.path))
                if basename in basenames:
                    basenames[basename] += 1
                    sensitive_ext = sensitive_ext_extract(basename)
                    if sensitive_ext is None:
                        basename = basename + "_" + str(basenames[basename])
                    else:
                        basename = basename[:-len(sensitive_ext)] + "_" + str(basenames[basename]) + sensitive_ext
                else:
                    basenames[basename] = 1

                #dest_path = self.reserve_path('jobs', jobId, 'inputs', basename)

                # identical to URL, but we don't have the option of localizing to
                # a persistent disk, since localization_command will create
                # some kind of FIFO
                if file_handler.localization_mode == 'stream':
                    job_vars.add(shlex.quote(key))
                    dest = self.reserve_path('jobs', jobId, 'inputs', basename)
                    localization_tasks += [file_handler.localization_command(dest.remotepath)]
                    export_writer(key, dest.remotepath, is_array)

                # this is a URL; create command to download it
                elif file_handler.localization_mode == 'url':
                    job_vars.add(shlex.quote(key))

                    exportpath = self.reserve_path('jobs', jobId, 'inputs', basename)

                    # download to the shared mount. Note this branch is NOT
                    # reached under localize_to_persistent_disk: those inputs are
                    # placed in their localization bucket by bucket_upload_script
                    # and then transformed into bucket_mount FileTypes, so they
                    # take the bucket_mount branch above instead. Routing them
                    # through here is what used to send every byte across the
                    # controller's disk twice.
                    localization_tasks += [file_handler.localization_command(exportpath.remotepath)]
                    file_handler.localized_path = exportpath

                    export_writer(key, exportpath.remotepath, is_array)

                # this is a read-only disk URL; export variables for subsequent mounting
                # and command to symlink file mount path to inputs directory
                elif file_handler.localization_mode == 'ro_disk':
                    assert file_handler.path.startswith("rodisk://")

                    job_vars.add(shlex.quote(key))

                    dgrp = re.search(r"rodisk://(.*?)/(.*)", file_handler.path)
                    disk = dgrp[1]
                    file = dgrp[2]

                    disk_dir = "/mnt/rodisks/{}".format(disk)

                    if disk not in canine_rodisks:
                        canine_rodisks.append(disk)
                        exports += ["export CANINE_RODISK_{}={}".format(len(canine_rodisks), disk)]
                        exports += ["export CANINE_RODISK_DIR_{}={}".format(len(canine_rodisks), disk_dir)]

                    dest = self.reserve_path('jobs', jobId, 'inputs', basename)

                    localization_tasks += [
                      # symlink the future RODISK path into the Canine inputs directory
                      # NOTE: it will be broken upon creation, since the RODISK will
                      #   be mounted subsequently.
                      # NOTE: it might already exist if we are retrying this task
                      # NOTE: the task also might have overwritten the symlink with its own file
                      "if [[ -e {path} && ! -L {path} ]]; then echo 'Warning: task overwrote symlink to {file} on RODISK' >&2; elif [[ ! -L {path} ]]; then ln -s {disk_dir}/{file} {path}; fi".format(disk_dir=disk_dir, file=file, path=dest.remotepath),
                    ]

                    export_writer(key, dest.remotepath, is_array)

                # this is a bucket-mounted (RODISK-replacement) URL; export
                # variables for subsequent gcsfuse mounting and a command to
                # symlink the future mount path into the inputs directory
                elif file_handler.localization_mode == 'bucket_mount':
                    assert file_handler.path.startswith("bucketmount://")

                    job_vars.add(shlex.quote(key))

                    # bucketmount://<bucket>/<object path>. The whole bucket is
                    # mounted and the symlink carries the object path, so one
                    # gcsfuse process serves every input from a given bucket --
                    # previously the second URL segment was consumed as an
                    # --only-dir, which under the one-bucket-per-localization
                    # layout would mount the bucket once per *input key*.
                    dgrp = re.search(r"bucketmount://([^/]+)/(.+)", file_handler.path)
                    bkt = dgrp[1]
                    file = dgrp[2]

                    bucketmount_key = bkt
                    mount_dir = "/mnt/bucketmounts/{}".format(bucketmount_key)

                    if bucketmount_key not in canine_bucketmounts:
                        canine_bucketmounts.append(bucketmount_key)
                        exports += ["export CANINE_BUCKETMOUNT_{}={}".format(len(canine_bucketmounts), bucketmount_key)]
                        exports += ["export CANINE_BUCKETMOUNT_DIR_{}={}".format(len(canine_bucketmounts), mount_dir)]

                    dest = self.reserve_path('jobs', jobId, 'inputs', basename)

                    localization_tasks += [
                      # symlink the future bucket-mount path into the Canine inputs directory
                      # NOTE: it will be broken upon creation, since the bucket will
                      #   be mounted subsequently.
                      # NOTE: it might already exist if we are retrying this task
                      # NOTE: the task also might have overwritten the symlink with its own file
                      "if [[ -e {path} && ! -L {path} ]]; then echo 'Warning: task overwrote symlink to {file} on bucket mount' >&2; elif [[ ! -L {path} ]]; then ln -s {mount_dir}/{file} {path}; fi".format(mount_dir=mount_dir, file=file, path=dest.remotepath),
                    ]

                    export_writer(key, dest.remotepath, is_array)

                # this is a local file; copy or symlink it to the inputs directory.
                # if we are localizing to a persistent disk, create commands
                # to copy it there.
                elif file_handler.localization_mode == "local":
                    job_vars.add(shlex.quote(key))

                    dest = self.reserve_path('jobs', jobId, 'inputs', basename)

                    with self.transport_context(transport) as transport:
                        self.localize_file(
                            file_handler,
                            dest,
                            transport=transport
                        )

                    # update localized_path
                    file_handler.localized_path = dest.remotepath

                    exportpath = dest.remotepath

                    # NB: unreachable under localize_to_persistent_disk -- those
                    # inputs are uploaded by bucket_upload_script (kind="copy")
                    # and transformed into bucket_mount FileTypes before this
                    # loop runs.

                    export_writer(
                      key,
                      exportpath,
                      is_array
                    )

                # this is a string literal
                elif file_handler.localization_mode == "string":
                    job_vars.add(shlex.quote(key))

                    export_writer(
                      key,
                      shlex.quote(file_handler.path), # not actually a path, per se
                      is_array
                    )

                else:
                    canine_logging.print("Unknown localization command:", file_handler.localization_mode, "skipping", key, file_handler.path,
                        file=sys.stderr, type="warning"
                    )

        #
        # add paths to array job files to exports
        for k in array_exports.keys():
            dest = self.reserve_path('jobs', jobId)
            exports += ['export {0}="{1}/{0}_array.txt"'.format(k, dest.remotepath)]

        #
        # Mount RO disks if any
        exports += ["export CANINE_N_RODISKS={}".format(len(canine_rodisks))]
        if len(canine_rodisks):
            localization_tasks += [
              "[ -f .rodisk_lock_pids ] && rm .rodisk_lock_pids || :",
              "for i in `seq ${CANINE_N_RODISKS}`; do",
              "CANINE_RODISK=CANINE_RODISK_${i}",
              "CANINE_RODISK=${!CANINE_RODISK}",
              "CANINE_RODISK_DIR=CANINE_RODISK_DIR_${i}",
              "CANINE_RODISK_DIR=${!CANINE_RODISK_DIR}",

              'echo "INFO: Attaching read-only disk ${CANINE_RODISK} ..." >&2',

              "if [[ ! -d ${CANINE_RODISK_DIR} ]]; then",
              "sudo mkdir -p ${CANINE_RODISK_DIR}",
              "fi",

              # create tempfile to hold diagnostic information
              "DIAG_FILE=$(mktemp)",

              # attach the disk if it's not already
              "if [[ ! -e /dev/disk/by-id/google-${CANINE_RODISK} ]]; then",
              # we can run into a race condition here if other tasks are
              # attempting to mount the same disk simultaneously, so we
              # force a 0 exit, unless gcloud returned exit code 5 (quota exceeded),
              # which we explicitly propagate to cause localization to be retried.
              # mounting the disk can also hang (exit 124), in which case we
              # also cause localization to be retried by returning exit 5
              "timeout -k 30 30 gcloud compute instances attach-disk ${CANINE_NODE_NAME} --zone ${CANINE_NODE_ZONE} --disk ${CANINE_RODISK} --device-name ${CANINE_RODISK} --mode ro &>> $DIAG_FILE || { ec=$?; [[ $ec == 5 || $ec == 124 ]] && exit 5 || true; }",
              "fi",

              # mount the disk if it's not already
              # as before, we can run into a race condition here, so we again
              # force a zero exit
              "if ! mountpoint -q ${CANINE_RODISK_DIR}; then",
              # wait for device to attach
              "tries=0",
              "while [ ! -b /dev/disk/by-id/google-${CANINE_RODISK} ]; do",
                'if [ $tries -gt 12 ]; then',
                  # check if the disk has attached successfully, but doesn't appear in /dev
                  # this means the node is likely bad
                  'if [ ! -b /dev/disk/by-id/google-${CANINE_RODISK} ] && gcloud compute disks describe ${CANINE_RODISK} --zone $CANINE_NODE_ZONE --format "csv(users)[no-heading]" | grep \'^http\' | grep -q $CANINE_NODE_NAME\'$\'; then',
                    'sudo touch /.fatal_disk_issue_sentinel',
                    'echo "ERROR: Node cannot attach disk; node is likely bad. Tagging for deletion. Will retry on another node." >&2',
                    'exit 5', # retry on another node
                  'fi',
                  # otherwise, it didn't attach for some other reason
                  'echo "ERROR: Read-only disk could not be attached!" >&2; [ -s $DIAG_FILE ] && { echo "The following error message may contain insight:" >&2; cat $DIAG_FILE >&2; } || :',
                  'exit 1',
                'fi',
                "sleep 10; ((++tries))",
              "done",

              # mount within Slurm worker container
              "sudo timeout -k 30 30 mount -o noload,ro,defaults /dev/disk/by-id/google-${CANINE_RODISK} ${CANINE_RODISK_DIR} || true",
              "fi",

              # because we forced zero exits for the previous commands,
              # we need to verify that the mount actually exists
              'mountpoint -q ${CANINE_RODISK_DIR} || { echo "ERROR: Read-only disk mount failed!" >&2; [ -s $DIAG_FILE ] && { echo "The following error message may contain insight:" >&2; cat $DIAG_FILE >&2; } || :; exit 1; }',

              # also verify that the filesystem is OK
              "timeout -k 30 30 ls ${CANINE_RODISK_DIR} > /dev/null || { echo 'WARNING: Read-only disk did not properly mount on this node; retrying.' >&2; exit 5; }",

              # lock the disk; will be unlocked during teardown script (or if script crashes)
              # this is to ensure that we don't unmount the disk during teardown
              # if other processes are still using it
              "flock -os ${CANINE_RODISK_DIR} sleep infinity & echo $! >> ${CANINE_JOB_INPUTS}/.rodisk_lock_pids",

              'echo "INFO: Successfully attached read-only disk ${CANINE_RODISK}." >&2',

              "done",
            ]

        #
        # Mount bucket-mounted (RODISK-replacement) inputs, if any
        exports += ["export CANINE_N_BUCKETMOUNTS={}".format(len(canine_bucketmounts))]
        if len(canine_bucketmounts):
            localization_tasks += [
              "[ -f .bucketmount_lock_pids ] && rm .bucketmount_lock_pids || :",
              "for i in `seq ${CANINE_N_BUCKETMOUNTS}`; do",
              "CANINE_BUCKETMOUNT=CANINE_BUCKETMOUNT_${i}",
              "CANINE_BUCKETMOUNT=${!CANINE_BUCKETMOUNT}",
              "CANINE_BUCKETMOUNT_DIR=CANINE_BUCKETMOUNT_DIR_${i}",
              "CANINE_BUCKETMOUNT_DIR=${!CANINE_BUCKETMOUNT_DIR}",

              'echo "INFO: Mounting bucket ${CANINE_BUCKETMOUNT} ..." >&2',

              # The mountpoint must be WRITABLE BY THE USER THAT RUNS gcsfuse.
              # fusermount3 refuses otherwise ("the user doesn't have
              # write-access on the mount point: permission denied"), and
              # gcsfuse below runs unprivileged.
              #
              # This differs from the RODISK path above, which `sudo mount`s and
              # so can leave the mountpoint owned by root. The bucketmount path
              # inherited the `sudo mkdir` from it but not the `sudo` on the
              # mount itself, which made every bucket mount fail.
              #
              # Deliberately NOT fixed by running `sudo gcsfuse`: mounting as
              # the invoking user keeps the mount readable by that user without
              # needing `-o allow_other`, and podman maps the task container's
              # root to this same UID (--uidmap in wolf/task.py), so the task
              # container can read it too.
              # A job that finished on this node moments ago may have unmounted
              # this same path from under us, leaving a stale FUSE endpoint.
              # Every subsequent operation on it -- stat, mkdir, even flock --
              # then fails with ENOTCONN or EACCES rather than ENOENT, so it must
              # be cleared before the path is touched at all. Observed live as
              # "mkdir: cannot stat ...: Permission denied" followed by
              # "flock: ...: Transport endpoint is not connected".
              'if ! stat ${CANINE_BUCKETMOUNT_DIR} > /dev/null 2>&1 && mount | grep -qF " ${CANINE_BUCKETMOUNT_DIR} "; then',
              '  echo "WARNING: clearing stale FUSE mount at ${CANINE_BUCKETMOUNT_DIR}" >&2',
              '  fusermount -uz ${CANINE_BUCKETMOUNT_DIR} 2> /dev/null || sudo umount -l ${CANINE_BUCKETMOUNT_DIR} 2> /dev/null || :',
              'fi',

              "if [[ ! -d ${CANINE_BUCKETMOUNT_DIR} ]]; then",
              "sudo mkdir -p ${CANINE_BUCKETMOUNT_DIR}",
              "fi",
              "sudo chown $(id -u):$(id -g) ${CANINE_BUCKETMOUNT_DIR}",

              # The busy-lock lives OUTSIDE the mount, as a plain file beside it.
              # Locking the mountpoint directory itself (as this used to) puts the
              # lock inside the very thing it protects: once any job unmounts,
              # the fd is invalid and flock cannot even open the path, so the
              # teardown guard silently stops guarding.
              "CANINE_BUCKETMOUNT_LOCK=$(dirname ${CANINE_BUCKETMOUNT_DIR})/.$(basename ${CANINE_BUCKETMOUNT_DIR}).lock",
              "sudo touch ${CANINE_BUCKETMOUNT_LOCK} && sudo chown $(id -u):$(id -g) ${CANINE_BUCKETMOUNT_LOCK}",

              # unlike RODISK, mounting has no cross-node attach race to
              # protect against: gcsfuse supports many concurrent read-only
              # mounts of the same bucket/prefix, so we can just mount if
              # not already mounted, with no backoff/retry dance
              "if ! mountpoint -q ${CANINE_BUCKETMOUNT_DIR}; then",
              # CANINE_BUCKETMOUNT is the bucket name; the whole bucket is
              # mounted and each input's object path lives in its symlink, so
              # one gcsfuse process serves every input from this bucket
              "CANINE_BUCKETMOUNT_BUCKET=${CANINE_BUCKETMOUNT}",
              # gcsfuse resolves credentials via ADC and does NOT read
              # CLOUDSDK_CONFIG, so point it explicitly at the credentials the
              # image stages there. Without this the authenticating identity
              # depends on whatever ADC happens to resolve to (metadata-server
              # SA vs. the copied user credentials), which silently works in
              # one project and fails in another. Mirrors the rclone path in
              # backends/dockerTransient.py.
              'if [ -z "${GOOGLE_APPLICATION_CREDENTIALS:-}" ] && [ -f "${CLOUDSDK_CONFIG}/application_default_credentials.json" ]; then export GOOGLE_APPLICATION_CREDENTIALS=${CLOUDSDK_CONFIG}/application_default_credentials.json; fi',
              "timeout -k 60 60 gcsfuse -o ro --implicit-dirs ${CANINE_BUCKETMOUNT_BUCKET} ${CANINE_BUCKETMOUNT_DIR} || { echo 'ERROR: Bucket mount failed!' >&2; exit 1; }",
              "fi",

              'mountpoint -q ${CANINE_BUCKETMOUNT_DIR} || { echo "ERROR: Bucket mount did not appear!" >&2; exit 1; }',

            ] + self.bucketmount_reachability_check() + [

              # hold the busy-lock; released during teardown (or if the script
              # crashes). unlike RODISK, this isn't to coordinate a cross-node
              # attach race -- it's so a concurrent job sharing this same
              # node/mountpoint doesn't get it unmounted out from under it by
              # another job's teardown
              "flock -os ${CANINE_BUCKETMOUNT_LOCK} sleep infinity & echo $! >> ${CANINE_JOB_INPUTS}/.bucketmount_lock_pids",

            ] + self.bucketmount_lease_register() + self.bucketmount_heartbeat_start() + [

              'echo "INFO: Successfully mounted bucket ${CANINE_BUCKETMOUNT}." >&2',

              "done",
            ]

        ## Symlink common inputs to job inputs
        localization_tasks += [
            'find "$CANINE_COMMON"/ -mindepth 1 -maxdepth 1 -exec sh -c "ln -s {} "$CANINE_JOB_INPUTS"/ || echo \'Could not symlink common input {}\' >&2" \;'
        ]

        # generate setup script
        setup_script = '\n'.join(
            line.rstrip()
            for line in [
                '#!/bin/bash',
                'export CANINE_JOB_VARS={}'.format(':'.join(job_vars)),
                'export CANINE_JOB_INPUTS="{}"'.format(os.path.join(compute_env['CANINE_JOBS'], jobId, 'inputs')),
                'export CANINE_JOB_WORKSPACE="{}"'.format(
                  os.path.join(compute_env['CANINE_JOBS'], jobId, 'workspace') if not self.use_scratch_disk else scratch_disk_prefix
                ),
                'export CANINE_JOB_ROOT="{}"'.format(os.path.join(compute_env['CANINE_JOBS'], jobId)),
                'export CANINE_JOB_SETUP="{}"'.format(os.path.join(compute_env['CANINE_JOBS'], jobId, 'setup.sh')),
                'export CANINE_JOB_LOCALIZATION="{}"'.format(os.path.join(compute_env['CANINE_JOBS'], jobId, 'localization.sh')),
                'export CANINE_JOB_TEARDOWN="{}"'.format(os.path.join(compute_env['CANINE_JOBS'], jobId, 'teardown.sh')),
                'export CANINE_DOCKER_ARGS="{docker} $CANINE_DOCKER_ARGS"'.format(docker=' '.join(set(docker_args))),
                'mkdir -p $CANINE_JOB_INPUTS',
                'mkdir -p $CANINE_JOB_WORKSPACE',
                'chmod 755 $CANINE_JOB_LOCALIZATION'
            ]
            # all exported job variables
            + exports
        ) + "\n"

        # generate localization script
        localization_script = '\n'.join(
          [
            "#!/bin/bash",
            "set -e",
            "shopt -s expand_aliases #DEBUG_OMIT",
            "alias gcloud=gcloud_exp_backoff #DEBUG_OMIT"
          ] + localization_tasks +
          ( # skip running task script if finished scratch disk already exists via special localizer exit code
            ["exit 15 #DEBUG_OMIT"] if self.use_scratch_disk and scratch_disk_already_exists and self.scratch_disk_job_avoid else []
          )
        ) + "\nset +e\n"

        # generate teardown script
        teardown_script = '\n'.join(
            line.rstrip()
            for line in [
                '#!/bin/bash',
                'set -e',
                'shopt -s expand_aliases #DEBUG_OMIT',
                'alias gcloud=gcloud_exp_backoff #DEBUG_OMIT',
                'if [[ -d $CANINE_JOB_WORKSPACE ]]; then cd $CANINE_JOB_WORKSPACE; fi',
                # 'mv ../stderr ../stdout .',
                # do not run delocalization script if we're in debug mode
                'if [[ -z $CANINE_DEBUG_MODE ]]; then if which python3 2>/dev/null >/dev/null; then python3 {script_path} {output_root} {shard} {patterns} {copyflags} {scratchflag} {finishedflag}; else python {script_path} {output_root} {shard} {patterns} {copyflags} {scratchflag} {finishedflag}; fi; fi'.format(
                    script_path = os.path.join(compute_env['CANINE_ROOT'], 'delocalization.py'),
                    output_root = compute_env['CANINE_OUTPUT'],
                    shard = jobId,
                    patterns = ' '.join(
                        '-p {} {}'.format(name, shlex.quote(pattern) if name not in {"stdout", "stderr"} else pattern)
                        for name, pattern in patterns.items()
                    ),
                    copyflags = ' '.join(
                        '-c {}'.format(name)
                        for name in self.files_to_copy_to_outputs 
                    ),
                    scratchflag = "--scratch" if self.use_scratch_disk else "",
                    finishedflag = "--finished_scratch" if self.use_scratch_disk and scratch_disk_already_exists and self.scratch_disk_job_avoid else "",
                ),

                # remove stream dir
                'if [[ -n "$CANINE_STREAM_DIR" ]]; then rm -rf $CANINE_STREAM_DIR; fi',

                # remove all files in workspace directory not captured by outputs
                f'comm -23 <(find $CANINE_JOB_WORKSPACE ! -type d | sort) <(find {compute_env["CANINE_OUTPUT"]}/{jobId} -mindepth 2 -type l -exec readlink -f {{}} \; | sort) | xargs rm -f' if self.cleanup_job_workdir and not self.use_scratch_disk else '',

                # unmount all RODISKs, if they're not in use
                # first, release all locks obtained by this job
                'if [ -f ${CANINE_JOB_INPUTS}/.rodisk_lock_pids ]; then',
                '  while read -r pid; do',
                '    kill $pid',
                '  done < ${CANINE_JOB_INPUTS}/.rodisk_lock_pids',
                '  rm -f ${CANINE_JOB_INPUTS}/.rodisk_lock_pids',
                'fi',
                '(cd /',
                'for i in $(seq ${CANINE_N_RODISKS}); do',
                '  CANINE_RODISK=CANINE_RODISK_${i}',
                '  CANINE_RODISK=${!CANINE_RODISK}',
                '  CANINE_RODISK_DIR=CANINE_RODISK_DIR_${i}',
                '  CANINE_RODISK_DIR=${!CANINE_RODISK_DIR}',
                '  echo "Unmounting read-only disk ${CANINE_RODISK}" >&2',
                '  if flock -n ${CANINE_RODISK_DIR} true && mountpoint -q ${CANINE_RODISK_DIR} && sudo umount ${CANINE_RODISK_DIR}; then',
                '    timeout -k 30 30 gcloud compute instances detach-disk $CANINE_NODE_NAME --zone $CANINE_NODE_ZONE --disk $CANINE_RODISK && echo "Unmounted ${CANINE_RODISK}" >&2 || echo "Error detaching disk ${CANINE_RODISK}" >&2',
                '  else',
                '    echo "Read-only disk ${CANINE_RODISK} is busy and will not be unmounted during teardown. It is likely in use by another job." >&2',
                '  fi',
                'done)',

                # unmount all bucket mounts, if they're not in use
                # first, stop refreshing customTime -- we are done holding this
                # content, so it should resume ageing normally
            ] + self.bucketmount_heartbeat_stop() + [
                # then release all locks obtained by this job
                'if [ -f ${CANINE_JOB_INPUTS}/.bucketmount_lock_pids ]; then',
                '  while read -r pid; do',
                '    kill $pid',
                '  done < ${CANINE_JOB_INPUTS}/.bucketmount_lock_pids',
                '  rm -f ${CANINE_JOB_INPUTS}/.bucketmount_lock_pids',
                'fi',
                '(cd /',
                'for i in $(seq ${CANINE_N_BUCKETMOUNTS}); do',
                '  CANINE_BUCKETMOUNT=CANINE_BUCKETMOUNT_${i}',
                '  CANINE_BUCKETMOUNT=${!CANINE_BUCKETMOUNT}',
                '  CANINE_BUCKETMOUNT_DIR=CANINE_BUCKETMOUNT_DIR_${i}',
                '  CANINE_BUCKETMOUNT_DIR=${!CANINE_BUCKETMOUNT_DIR}',
                '  echo "Unmounting bucket ${CANINE_BUCKETMOUNT}" >&2',
                # lock file lives beside the mountpoint, not inside it, so this
                # stays openable no matter what state the mount is in
                "  CANINE_BUCKETMOUNT_LOCK=$(dirname ${CANINE_BUCKETMOUNT_DIR})/.$(basename ${CANINE_BUCKETMOUNT_DIR}).lock",
                '  if flock -n ${CANINE_BUCKETMOUNT_LOCK} true && mountpoint -q ${CANINE_BUCKETMOUNT_DIR}; then',
                '    fusermount -u ${CANINE_BUCKETMOUNT_DIR} && echo "Unmounted ${CANINE_BUCKETMOUNT}" >&2 || echo "Error unmounting ${CANINE_BUCKETMOUNT}" >&2',
                '  else',
                '    echo "Bucket mount ${CANINE_BUCKETMOUNT} is busy and will not be unmounted during teardown. It is likely in use by another job." >&2',
                '  fi',
                'done)',
            ] + self.bucketmount_lease_release() + [
            ] + ( scratch_disk_teardown_script if self.use_scratch_disk else [] )
        )
        return setup_script, localization_script, teardown_script, array_exports

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
        staging directory or copying a batch of gcloud storage files
        Returns the remote staging directory, which is now ready for final startup
        """
        pass
