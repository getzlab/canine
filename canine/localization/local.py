import os
import sys
import warnings
import typing
import fnmatch
import shlex
import tempfile
import subprocess
import shutil
from uuid import uuid4
from collections import namedtuple
from contextlib import ExitStack, contextmanager
from .base import AbstractLocalizer, PathType, Localization
from . import file_handlers
from ..backends import AbstractSlurmBackend, AbstractTransport
from ..utils import get_default_gcp_project, check_call

class BatchedLocalizer(AbstractLocalizer):
    """
    Default localization strategy:
    Constructs the canine staging directory locally.
    Local inputs are symlinked into the staging directory.
    gs:// inputs are queued for later
    After staging, the directory is copied to the slurm controller
    (If a transfer bucket is provided, a more efficient copy is used)
    After all jobs have finished, the output directory is copied back here
    """

    def __init__(
        self, backend: AbstractSlurmBackend, transfer_bucket: typing.Optional[str] = None,
        common: bool = True, staging_dir: str = None,
        project : typing.Optional[str] = None, **kwargs
    ):

        """
        Initializes the Localizer using the given transport.
        Localizer assumes that the SLURMBackend is connected and functional during
        the localizer's entire life cycle.
        If staging_dir is not provided, a random directory is chosen
        """
        super().__init__(backend, transfer_bucket, common, staging_dir, project, **kwargs)
        self.queued_gs = [] # Queued gs:// -> remote staging transfers
        self.queued_batch = [] # Queued local -> remote directory transfers
        self._has_localized = False

    def localize_file(self, src: str, dest: PathType, transport: typing.Optional[AbstractTransport] = None):
        """
        Localizes the given file.
        gs:// files are queued for later transfer
        local files are symlinked to the staging directory
        """
        if self._has_localized:
            warnings.warn(
                "BatchedLocalizer.localize_file called after main localization. File may not be localized"
            )
        if src.startswith('gs://'):
            self.queued_gs.append((
                src,
                dest.remotepath,
                'remote'
            ))
        elif os.path.exists(src):
            src = os.path.abspath(src)
            if not os.path.isdir(os.path.dirname(dest.localpath)):
                os.makedirs(os.path.dirname(dest.localpath))
            if os.path.isfile(src):
                os.symlink(src, dest.localpath)
            else:
                self.queued_batch.append((src, os.path.join(dest.remotepath, os.path.basename(src))))

    def __enter__(self):
        """
        Enter localizer context.
        May take any setup action required
        """
        os.mkdir(self.environment('local')['CANINE_COMMON'])
        os.mkdir(self.environment('local')['CANINE_JOBS'])
        os.mkdir(self.environment('local')['CANINE_OUTPUT'])
        return self

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
        if overrides is None:
            overrides = {}
        overrides = {k:v.lower() if isinstance(v, str) else None for k,v in overrides.items()}
        with self.backend.transport() as transport:
            if self.common:
                common_dests = self.pick_common_inputs(inputs, overrides, transport=transport)
            else:
                common_dests = {}
            for jobId, data in inputs.items():
                # noop; this shard has been avoided
                if data is None:
                    continue

                os.makedirs(os.path.join(
                    self.environment('local')['CANINE_JOBS'],
                    jobId,
                ))
                self.prepare_job_inputs(jobId, data, common_dests, overrides, transport=transport)

                # Now generate and localize job setup, localization, and teardown scripts, and
                # any array job files
                setup_script, localization_script, teardown_script, array_exports = self.job_setup_teardown(jobId, patterns, transport)

                # Setup: 
                script_path = self.reserve_path('jobs', jobId, 'setup.sh')
                with open(script_path.localpath, 'w') as w:
                    w.write(setup_script)
                os.chmod(script_path.localpath, 0o775)

                # Localization: 
                script_path = self.reserve_path('jobs', jobId, 'localization.sh')
                with open(script_path.localpath, 'w') as w:
                    w.write(localization_script)
                os.chmod(script_path.localpath, 0o775)

                # Teardown:
                script_path = self.reserve_path('jobs', jobId, 'teardown.sh')
                with open(script_path.localpath, 'w') as w:
                    w.write(teardown_script)
                os.chmod(script_path.localpath, 0o775)

                # Array exports
                for k, v in array_exports.items():
                    export_path = self.reserve_path('jobs', jobId, k + "_array.txt")
                    with open(export_path.localpath, 'w') as w:
                        w.write("\n".join(v) + "\n")

            # symlink delocalization script
            os.symlink(
                os.path.join(
                    os.path.dirname(__file__),
                    'delocalization.py'
                ),
                os.path.join(self.environment('local')['CANINE_ROOT'], 'delocalization.py')
            )

            # symlink debug script
            os.symlink(
                os.path.join(
                    os.path.dirname(__file__),
                    'debug.sh'
                ),
                os.path.join(self.environment('local')['CANINE_ROOT'], 'debug.sh')
            )

            self.sendtree(
                self.local_dir,
                self.staging_dir,
                transport,exist_okay=True
            )
            staging_dir = self.finalize_staging_dir(inputs.keys(), transport=transport)
            for src, dest, context in self.queued_gs:
                self.gs_copy(src, dest, context)
            for src, dest in self.queued_batch:
                self.sendtree(src, os.path.dirname(dest))
            self._has_localized = True
            return staging_dir

class LocalLocalizer(BatchedLocalizer):
    """
    Similar to BatchedLocalizer:
    Constructs the canine staging directory locally.
    Local inputs are symlinked into the staging directory.
    After staging, the directory is copied to the slurm controller
    (If a transfer bucket is provided, a more efficient copy is used)
    After all jobs have finished, the output directory is copied back here

    EXCEPT:
    Unlike BatchedLocalizer, gs:// files are copied into the local staging directory
    prior to it being copied to the slurm node. This is less efficient (as it
    increases the size of the staging transfer) but utilizes local gsutil credentials
    """
    def localize_file(self, src: file_handlers.FileType, dest: PathType, transport: typing.Optional[AbstractTransport] = None):
        """
        Localizes the given file.
        * remote URLs are copied to the specified destination
        * local files are symlinked to the staging directory
        """
        if not self._has_localized:
            warnings.warn(
                "BatchedLocalizer.localize_file called after main localization. File may not be localized"
            )
        # it's a remote URL; get localization command and execute
        if src.localization_mode == "url":
            cmd = src.localization_command(dest.localpath)
            subprocess.check_call(cmd, shell = True)

        # it's a local file
        elif os.path.exists(src.path):
            src = os.path.abspath(src.path)
            if not os.path.isdir(os.path.dirname(dest.localpath)):
                os.makedirs(os.path.dirname(dest.localpath))
            if os.path.isfile(src):
                os.symlink(src, dest.localpath)
            else:
                self.queued_batch.append((src, os.path.join(dest.remotepath, os.path.basename(src))))
