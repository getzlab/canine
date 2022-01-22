import typing
import warnings
import shutil
import tempfile
import os
import re
import shlex
import glob
import subprocess
from .base import PathType, Localization
from .local import BatchedLocalizer
from ..backends import AbstractSlurmBackend, AbstractTransport
from ..utils import get_default_gcp_project
from agutil import status_bar
import pandas as pd


class NFSLocalizer(BatchedLocalizer):
    """
    Similar to LocalLocalizer:
    Constructs the canine staging directory locally.
    Local inputs are symlinked into the staging directory.

    EXCEPT:
    Unlike LocalLocalizer, there is no final transfer step.
    Files are staged locally and this strategy expects that the given local staging dir
    is NFS mounted to the slurm cluster
    """

    def __init__(
        self, backend: AbstractSlurmBackend, staging_dir = None, **kwargs
    ):
        """
        Initializes the Localizer using the given transport.
        Localizer assumes that the SLURMBackend is connected and functional during
        the localizer's entire life cycle.
        Note: staging_dir refers to the NFS mount path on the local and remote filesystems
        This localization strategy requires that {staging_dir} is a valid NFS mount shared between
        the local system, the slurm controller, and all slurm compute nodes
        """
        if staging_dir is None:
            raise TypeError("staging_dir is a required argument for NFSLocalizer")

        # superclass constructor can mostly be re-used as is, except ...
        super().__init__(backend, staging_dir = staging_dir, **kwargs)

        # ... we don't normalize paths for this localizer. Use staging dir as given
        self._local_dir = None
        self.staging_dir = staging_dir
        self.local_dir = staging_dir
        if not os.path.isdir(self.local_dir):
            os.makedirs(self.local_dir)

    def localize_file(self, src: str, dest: PathType, transport: typing.Optional[AbstractTransport] = None):
        """
        Localizes the given file.
        * gs:// files are copied to the specified destination
        * local files are symlinked to the specified destination
        * results dataframes are copied to the specified directory
        """
        if src.startswith('gs://'):
            self.gs_copy(
                src,
                dest.localpath,
                'local'
            )
        elif os.path.exists(src):
            src = os.path.realpath(os.path.abspath(src))

            if re.search(r"\.k9df\..*$", os.path.basename(src)) is None:
                if not os.path.isdir(os.path.dirname(dest.localpath)):
                    os.makedirs(os.path.dirname(dest.localpath))

                #
                # check if self.mount_path, self.staging_dir, and src all exist on the same NFS share
                # symlink if yes, copy if no
                if self.same_volume(src):
                    os.symlink(src, dest.localpath)
                else:
                    if os.path.isfile(src):
                        shutil.copyfile(src, dest.localpath)
                    else:
                        shutil.copytree(src, dest.localpath)
            else:
                shutil.copyfile(src, dest.localpath)

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

        # automatically override inputs that are absolute paths residing on the same
        # NFS share and are not Canine outputs

        # XXX: this can be potentially slow, since it has to iterate over every
        #      single input. It would make more sense to do this before the adapter
        #      converts raw inputs.
        for input_dict in inputs.values():
            # noop; this shard was avoided
            if input_dict is None:
                continue

            for k, v in input_dict.items():
                if k not in overrides and isinstance(v, str):
                    if re.match(r"^/", v) is not None and self.same_volume(v) and \
                      re.match(r".*/outputs/\d+/.*", v) is None:
                        overrides[k] = None
                        warnings.warn(
                            "One or more inputs has been overriden to None (will be handled as a string)."
                            " To avoid this behavior, explicitly override it with 'localize'"
                        )

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

            # copy delocalization script
            shutil.copyfile(
                os.path.join(
                    os.path.dirname(__file__),
                    'delocalization.py'
                ),
                os.path.join(self.environment('local')['CANINE_ROOT'], 'delocalization.py')
            )

            # copy debug script
            shutil.copyfile(
                os.path.join(
                    os.path.dirname(__file__),
                    'debug.sh'
                ),
                os.path.join(self.environment('local')['CANINE_ROOT'], 'debug.sh')
            )

            return self.finalize_staging_dir(inputs)

    def delocalize(self, patterns: typing.Dict[str, str], output_dir: typing.Optional[str] = None) -> typing.Dict[str, typing.Dict[str, str]]:
        """
        Delocalizes output from all jobs.
        NFSLocalizer does not delocalize files. Data is copied from output dir
        """
        if output_dir is not None:
            warnings.warn("output_dir has no bearing on NFSLocalizer. outputs are available in {}/outputs".format(self.local_dir))
        # this seems to be crashing pipelines; since we don't use it anywhere,
        # let's disable it for now
        #self.build_manifest()
        output_dir = os.path.join(self.local_dir, 'outputs')
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

    def finalize_staging_dir(self, jobs: typing.Iterable[str], transport: typing.Optional[AbstractTransport] = None) -> str:
        """
        Finalizes the staging directory by building any missing directory trees
        (such as those dropped during transfer for being empty).
        Returns the absolute path of the remote staging directory on the controller node.
        """
        controller_env = self.environment('local')
        if not os.path.isdir(controller_env['CANINE_COMMON']):
            os.mkdir(controller_env['CANINE_COMMON'])
        if not os.path.isdir(controller_env['CANINE_JOBS']):
            os.mkdir(controller_env['CANINE_JOBS'])
        if len(jobs) and not os.path.isdir(controller_env['CANINE_OUTPUT']):
            os.mkdir(controller_env['CANINE_OUTPUT'])
        return self.staging_dir

    def same_volume(self, *args):
        """
        Check if args are stored on the same NFS mount as the output directory.
        """
        vols = subprocess.check_output(
          "df -P {} | awk 'NR > 1 {{ print $6 }}'".format(
            " ".join([shlex.quote(x) for x in [self.staging_dir, *args]])
          ),
          shell = True
        )
        return len(set(vols.decode("utf-8").rstrip().split("\n"))) == 1
