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
        self, backend: AbstractSlurmBackend, transfer_bucket: typing.Optional[str] = None,
        common: bool = True, staging_dir: str = None, mount_path: str = None,
        project: typing.Optional[str] = None, **kwargs
    ):
        """
        Initializes the Localizer using the given transport.
        Localizer assumes that the SLURMBackend is connected and functional during
        the localizer's entire life cycle.
        Note: staging_dir refers to the directory on the LOCAL filesystem
        Note: mount_path refers to the mounted directory on the REMOTE filesystem (both the controller and worker nodes)
        """
        if staging_dir is None:
            raise TypeError("staging_dir is a required argument for NFSLocalizer")
        if transfer_bucket is not None:
            warnings.warn("transfer_bucket has no bearing on NFSLocalizer. It is kept purely for adherence to the API")
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
        self._local_dir = tempfile.TemporaryDirectory()
        self.local_dir = os.path.realpath(os.path.abspath(staging_dir))
        if not os.path.isdir(self.local_dir):
            os.makedirs(self.local_dir)
        with self.backend.transport() as transport:
            self.mount_path = transport.normpath(mount_path if mount_path is not None else staging_dir)
        self.staging_dir = self.mount_path
        self.inputs = {} # {jobId: {inputName: (handle type, handle value)}}
        self.clean_on_exit = True
        self.project = project if project is not None else get_default_gcp_project()

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
                # check if self.mount_path, self.local_dir, and src all exist on the same NFS share
                # symlink if yes, copy if no
                vols = subprocess.check_output(
                  "df {} {} {} | awk 'NR > 1 {{ print $1 }}'".format(
                    self.mount_path,
                    self.local_dir,
                    src
                  ),
                  shell = True
                )
                if len(set(vols.decode("utf-8").rstrip().split("\n"))) == 1:
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
        and set up the job's setup and teardown scripts
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
                os.makedirs(os.path.join(
                    self.environment('local')['CANINE_JOBS'],
                    jobId,
                ))
                self.prepare_job_inputs(jobId, data, common_dests, overrides, transport=transport)
                # Now localize job setup and teardown scripts
                setup_script, teardown_script = self.job_setup_teardown(jobId, patterns)
                # Setup:
                script_path = self.reserve_path('jobs', jobId, 'setup.sh')
                with open(script_path.localpath, 'w') as w:
                    w.write(setup_script)
                os.chmod(script_path.localpath, 0o775)
                # Teardown:
                script_path = self.reserve_path('jobs', jobId, 'teardown.sh')
                with open(script_path.localpath, 'w') as w:
                    w.write(teardown_script)
                os.chmod(script_path.localpath, 0o775)
            shutil.copyfile(
                os.path.join(
                    os.path.dirname(__file__),
                    'delocalization.py'
                ),
                os.path.join(self.environment('local')['CANINE_ROOT'], 'delocalization.py')
            )
            return self.finalize_staging_dir(inputs)

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
                exports.append('export {}={}'.format(
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
                'mkdir -p $CANINE_JOB_INPUTS',
                'mkdir -p $CANINE_JOB_ROOT',
            ] + exports + extra_tasks
        ) + '\ncd $CANINE_JOB_ROOT\n'
        teardown_script = '\n'.join(
            line.rstrip()
            for line in [
                '#!/bin/bash',
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
            ]
        )
        return setup_script, teardown_script

    def delocalize(self, patterns: typing.Dict[str, str], output_dir: typing.Optional[str] = None) -> typing.Dict[str, typing.Dict[str, str]]:
        """
        Delocalizes output from all jobs.
        NFSLocalizer does not delocalize files. Data is copied from output dir
        """
        if output_dir is not None:
            warnings.warn("output_dir has no bearing on NFSLocalizer. outputs are available in {}/outputs".format(self.local_dir))
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
