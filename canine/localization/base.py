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
from . import file_handlers
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
        super().__init__("'{}' override is invalid for input {} with value {}".format(override, arg, value))

class AbstractLocalizer(abc.ABC):
    """
    Base class for localization.
    """


    def __init__(
        self, backend: AbstractSlurmBackend, transfer_bucket: typing.Optional[str] = None,
        common: bool = True, staging_dir: str = None,
        project: typing.Optional[str] = None,
        localize_to_persistent_disk = False, persistent_disk_type: str = "standard",
        use_scratch_disk = False, scratch_disk_size: int = 10, scratch_disk_type: str = "standard", scratch_disk_name = None,
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
        self.project = project if project is not None else get_default_gcp_project()

        # will be removed
        self.local_download_size = {} # {jobId: size}

        self.localize_to_persistent_disk = localize_to_persistent_disk
        self.persistent_disk_type = persistent_disk_type
        self.use_scratch_disk = use_scratch_disk
        self.scratch_disk_type = scratch_disk_type
        self.scratch_disk_size = scratch_disk_size
        self.scratch_disk_name = scratch_disk_name

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
                cmd = "gsutil -m {} rm -r gs://{}/{}".format(
                    '-u {}'.format(self.project) if self.get_requester_pays(self.transfer_bucket) else '',
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
        Scans input configuration and overrides to choose inputs which should be treated as common.
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
                            fh = file_handlers.get_file_handler(p)

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
                        common_dests[path].localized_path = dest_path
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
                value = file_handlers.get_file_handler(value)
            else:
                assert isinstance(value, file_handlers.FileType)

            ## now that value is known, define closure to localize it immediately
            def localize_now():
                nonlocal transport

                dest_path = self.get_destination_path(
                  value.path,
                  transport, 
                  'jobs', jobId, 'inputs'
                )

                with self.transport_context(transport) as transport:
                    self.localize_file(
                        value,
                        dest_path,
                        transport=transport
                    )

                # update localized_path
                value.localized_path = dest_path

                return value

            # if this is a local file, localize it now
            if value.localization_mode == "local":
                return localize_now()

            # if user overrode the handling mode, make sure it's compatible
            # with the file type
            if mode is not False: 
                # user wants to stream a URL, rather than download it
                if mode == 'stream':
                    # XXX: currently, we only support streaming gs:// URLs,
                    #      so we explicitly check here
                    if not value.startswith('gs://'):
                        raise ValueError("Only gs:// files are currently supported for streaming!")
                    else:
                        value.localization_mode = "stream"

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
      disk_size: int = 0
    ):
        """
        Generates the commands to (i) create and (ii) destroy a persistent disk.
        If a dict of inputs -> [FileTypes] is provided, disk will be named
        according to the FileType hashes, and sized according to the filesizes.
        """
        #
        # if we already have files in mind to localize
        if len(file_paths_arrays):
            # flatten file_paths dict
            # for non-array inputs, { inputName : path }
            # disambiguate array inputs with numerical suffix, e.g. { inputName + "_0" : path }
            file_paths = []
            for k, v in file_paths_arrays.items(): 
                n_suff = 0
                for x in v:
                    file_paths.append([k, n_suff, x.path, x.hash, x.size, not (isinstance(x, file_handlers.StringLiteral) or isinstance(x, file_handlers.HandleRODISKURL or x.localization_mode == "stream"))])
                    n_suff += 1

            ## Create dataframe of files' attributes
            F = pd.DataFrame(file_paths, columns = ["input", "array_idx", "path", "hash", "size", "localize"])
            F["file_basename"] = F["path"].apply(os.path.basename)

            ## Disk name is determined by files' basenames and hashes
            disk_name = "canine-" + \
              file_handlers.hash_set(set(
                F.loc[F["localize"], "file_basename"] + "_" + \
                F.loc[F["localize"], "hash"]
              ))

            canine_logging.info1("Disk name is {}".format(disk_name))

            ## create RODISK URLs for files being localized to disk
            # pass through all other values unaltered
            F["disk_path"] = F["path"]
            F.loc[F["localize"], "disk_path"] = "rodisk://" + disk_name + "/" + F.loc[F["localize"], ["input", "file_basename"]].apply(lambda x: "/".join(x), axis = 1)

            ## Calculate disk size
            disk_size = F.loc[F["localize"], "size"].sum()
            disk_size = max(10, 1 + int(disk_size/1022611260)) # bytes -> gib with 5% safety margin

            ## Save RODISK paths for subsequent use by downstream tasks
            rodisk_paths = F.loc[F["localize"], :].groupby("input")["disk_path"].apply(list).to_dict()

        #
        # otherwise, we create a blank disk with a given name (if specified),
        # otherwise random
        else:
            disk_name = "canine-scratch-{}".format(disk_name if disk_name is not None else os.urandom(4).hex())
            disk_size = max(10, 1 + int(disk_size/1022611260))
            rodisk_paths = None

        ## mount prefix
        mount_prefix = "/mnt/nfs/rodisks"
        disk_mountpoint = mount_prefix + "/" + disk_name

        ## Check if the disk already exists
        out = subprocess.check_output(
            "gcloud compute disks list --filter 'labels.wolf=canine and labels.finished=yes and name=({})'".format(disk_name), shell=True
        )
        out = out.decode().rstrip().split("\n")
        if len(out) > 2:
            raise Exception("Unexpected number of existing disks (should not happen?)")

        # disk exists
        if len(out) == 2: # header + one result
            canine_logging.info1("Found existing disk {}".format(disk_name))

            return disk_mountpoint, [], [], rodisk_paths

        canine_logging.info1("Creating new persistent disk {}".format(disk_name))

        ## Generate disk creation script
        localization_script = [
            'set -eux',
            'GCP_DISK_NAME={}'.format(disk_name),
            'GCP_DISK_SIZE={}'.format(disk_size),
            'GCP_TSNT_DISKS_DIR={}'.format(mount_prefix),

            ## create disk
            'if ! gcloud compute disks describe "${GCP_DISK_NAME}" --zone ${CANINE_NODE_ZONE}; then',
            'gcloud compute disks create "${GCP_DISK_NAME}" --size "${GCP_DISK_SIZE}GB" --type pd-standard --zone "${CANINE_NODE_ZONE}" --labels wolf=canine',
            'fi',

            ## attach as read-write, using same device-name as disk-name
            'if [[ ! -e /dev/disk/by-id/google-${GCP_DISK_NAME} ]]; then',
            'gcloud compute instances attach-disk "$CANINE_NODE_NAME" --zone "$CANINE_NODE_ZONE" --disk "$GCP_DISK_NAME" --device-name "$GCP_DISK_NAME" || true',
            'fi',

            ## wait for disk to attach, with exponential backoff up to 2 minutes
            'DELAY=1',
            'while [[ ! -e /dev/disk/by-id/google-${GCP_DISK_NAME} ]]; do',
            '[ $DELAY -gt 128 ] && { echo "Exceeded timeout trying to attach disk"; exit 1; } || :',
            'sleep $DELAY; ((DELAY *= 2))',
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
            'sudo mount -o discard,defaults /dev/disk/by-id/google-"${GCP_DISK_NAME}" "$GCP_TSNT_DISKS_DIR/$GCP_DISK_NAME"',
            'sudo chmod -R a+rwX "${GCP_TSNT_DISKS_DIR}/${GCP_DISK_NAME}"',
            'fi'
        ]

        # * disk unmount or deletion script (to append to teardown_script)
        #   -> need to be able to pass option to not delete, if using as a RODISK later
        teardown_script = [
          'sudo umount {}/{}'.format(mount_prefix, disk_name),
          'gcloud compute instances detach-disk $CANINE_NODE_NAME --zone $CANINE_NODE_ZONE --disk {}'.format(disk_name),
          # TODO: add command to optionally delete disk
        ]

        return disk_mountpoint, localization_script, teardown_script, rodisk_paths

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
        docker_args = ['-v $CANINE_ROOT:$CANINE_ROOT']
        localization_tasks = [
            'if [[ -d $CANINE_JOB_INPUTS ]]; then cd $CANINE_JOB_INPUTS; fi'
        ]

        #
        # create creation script for persistent disk, if specified
        if self.localize_to_persistent_disk:
            # FIXME: we don't have an easy way of parsing which inputs are common
            #        to all shards at this point. if every localizable input is
            #        common to each shard, then we'll create the same disk
            #        multiple times, once per shard. thus, for now we heavyhandedly
            #        prohibit localizing to persistent disks for multishard jobs.
            if len(self.inputs) > 1:
                raise ValueError("Localization to persistent disk for multishard jobs is currently not supported.")

            disk_prefix, disk_creation_script, disk_teardown_script, rodisk_paths = self.create_persistent_disk(self.inputs[jobId])

            # add commands to create/mount the disk
            localization_tasks += disk_creation_script

            # save rodisk:// URLs for files saved to the disk for later use
            self.rodisk_paths[jobId] = rodisk_paths

        #
        # create creation script for scratch disk, if specified
        # TODO

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

                'sudo mount -o discard,defaults /dev/disk/by-id/google-{} $CANINE_LOCAL_DISK_DIR'.format(
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

        compute_env = self.environment('remote')
        stream_dir_ready = False
        for key, file_handler_array in self.inputs[jobId].items():
            is_array = self.input_array_flag[jobId][key]
            for file_handler in file_handler_array: 
                # create FIFO to stream directly from bucket
                # TODO: generate these commands in file_handlers.py
                if file_handler.localization_mode == 'stream':
                    job_vars.add(shlex.quote(key))
                    if not stream_dir_ready:
                        exports.append('export CANINE_STREAM_DIR=$(mktemp -d /tmp/canine_streams.$SLURM_ARRAY_JOB_ID.$SLURM_ARRAY_TASK_ID.XXXX)')
                        docker_args.append('-v $CANINE_STREAM_DIR:$CANINE_STREAM_DIR')
                        stream_dir_ready = True
                    dest = os.path.join('$CANINE_STREAM_DIR', os.path.basename(os.path.abspath(file_handler.path)))
                    localization_tasks += [
                        'gsutil ls {} > /dev/null'.format(shlex.quote(file_handler.path)),
                        'if [[ -e {0} ]]; then rm {0}; fi'.format(dest),
                        'mkfifo {}'.format(dest),
                        "gsutil {} cat {} > {} &".format(
                            '-u {}'.format(shlex.quote(self.project)) if self.get_requester_pays(file_handler.path) else '',
                            shlex.quote(file_handler.path),
                            dest
                        )
                    ]
                    export_writer(key, dest, is_array)

                # this is a URL; create command to download it
                elif file_handler.localization_mode == 'url':
                    job_vars.add(shlex.quote(key))

                    # localize this file to a persistent disk, if specified
                    if self.localize_to_persistent_disk:
                        # set dest to persistent disk mountpoint
                        # TODO: basenames of URLs have not been mangled to
                        # handle multiple non-unique basenames for an array
                        # input, a la get_destination_path.
                        exportpath = os.path.join(disk_prefix, key, os.path.basename(file_handler.path))
                    else:
                        # set dest to path on NFS
                        dest = self.get_destination_path(
                          os.path.basename(os.path.abspath(file_handler.path)),
                          transport,
                          'jobs', jobId, 'inputs', 
                        )
                        exportpath = dest.remotepath
                    file_handler.localized_path = exportpath

                    localization_tasks += [file_handler.localization_command(exportpath)]

                    export_writer(key, exportpath, is_array)

                # this is a read-only disk URL; export variables for subsequent mounting
                # and command to symlink file mount path to inputs directory
                elif file_handler.localization_mode == 'ro_disk':
                    assert file_handler.path.startswith("rodisk://")

                    job_vars.add(shlex.quote(key))

                    dgrp = re.search(r"rodisk://(.*?)/(.*)", file_handler.path)
                    disk = dgrp[1]
                    file = dgrp[2]
                    base_name = os.path.basename(file)

                    disk_dir = "/mnt/nfs/ro_disks/{}".format(disk)

                    if disk not in canine_rodisks:
                        canine_rodisks.append(disk)
                        exports += ["export CANINE_RODISK_{}={}".format(len(canine_rodisks), disk)]
                        exports += ["export CANINE_RODISK_DIR_{}={}".format(len(canine_rodisks), disk_dir)]

                    dest = self.reserve_path('jobs', jobId, 'inputs', base_name)

                    localization_tasks += [
                      # symlink the future RODISK path into the Canine inputs directory
                      # NOTE: it will be broken upon creation, since the RODISK will
                      #   be mounted subsequently.
                      # NOTE: it might already exist if we are retrying this task
                      "if [[ ! -L {path} ]]; then ln -s {disk_dir}/{file} {path}; fi".format(disk_dir=disk_dir, file=file, path=dest.remotepath),
                    ]

                    export_writer(key, dest.remotepath, is_array)

                # this is a local file. it has already been copied or symlinked 
                # to the inputs directory. if we are using a persistent disk, create
                # commands to copy it over. otherwise, do nothing besides export
                elif file_handler.localization_mode == "local":
                    job_vars.add(shlex.quote(key))

                    localized_path = file_handler.localized_path.remotepath
                    if self.localize_to_persistent_disk:
                        exportpath = os.path.join(disk_prefix, key, os.path.basename(localized_path))
                        localization_tasks += [ "cp -r {} {}".format(localized_path, exportpath) ]
                        file_handler.localized_path = exportpath
                    else:
                        exportpath = localized_path

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
                      file_handler.path, # not actually a path, per se
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
              "for i in `seq ${CANINE_N_RODISKS}`; do",
              "CANINE_RODISK=CANINE_RODISK_${i}",
              "CANINE_RODISK=${!CANINE_RODISK}",
              "CANINE_RODISK_DIR=CANINE_RODISK_DIR_${i}",
              "CANINE_RODISK_DIR=${!CANINE_RODISK_DIR}",

              "if [[ ! -d ${CANINE_RODISK_DIR} ]]; then sudo mkdir -p ${CANINE_RODISK_DIR}; fi",

              # create tempfile to hold diagnostic information
              "DIAG_FILE=$(mktemp)",

              # attach the disk if it's not already
              "if [[ ! -e /dev/disk/by-id/google-${CANINE_RODISK} ]]; then",
              # we can run into a race condition here if other tasks are
              # attempting to mount the same disk simultaneously, so we
              # force a 0 exit
              "gcloud compute instances attach-disk ${CANINE_NODE_NAME} --zone ${CANINE_NODE_ZONE} --disk ${CANINE_RODISK} --device-name ${CANINE_RODISK} --mode ro &>> $DIAG_FILE || true",
              "fi",

              # mount the disk if it's not already
              # as before, we can run into a race condition here, so we again
              # force a zero exit
              "if ! mountpoint -q ${CANINE_RODISK_DIR}; then",
              # wait for device to attach
              "tries=0",
              "while [ ! -b /dev/disk/by-id/google-${CANINE_RODISK} ]; do",
              '[ $tries -gt 12 ] && { echo "Timeout exceeded for disk to attach; perhaps the stderr of \`gcloud compute instances attach disk\` might contain insight:" >&2; cat $DIAG_FILE >&2; exit 1; } || :',
              "sleep 10; ((++tries))",
              "done",

              # mount within container
              "sudo mount -o noload,ro,defaults /dev/disk/by-id/google-${CANINE_RODISK} ${CANINE_RODISK_DIR} &>> $DIAG_FILE || true",
              # mount on host (so that other dockers can access it)
              "if [[ -f /.dockerenv ]]; then",
              "sudo nsenter -t 1 -m mount -o noload,ro,defaults /dev/disk/by-id/google-${CANINE_RODISK} ${CANINE_RODISK_DIR} &>> $DIAG_FILE || true",
              "fi",
              "fi",

              # because we forced zero exits for the previous commands,
              # we need to verify that the mount actually exists
              "mountpoint -q ${CANINE_RODISK_DIR} || { echo 'Read-only disk mount failed!' >&2; cat $DIAG_FILE >&2; exit 1; }",

              "done",
            ]

        ## Symlink common inputs to job inputs
        localization_tasks += [
            'find "$CANINE_COMMON"/ -mindepth 1 -maxdepth 1 -exec ln -sf {} "$CANINE_JOB_INPUTS"/ \;'
        ]

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
            ] + ( disk_teardown_script if self.localize_to_persistent_disk else [] )
            # TODO: add teardown script for scratch disks
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
        staging directory or copying a batch of gsutil files
        Returns the remote staging directory, which is now ready for final startup
        """
        pass
