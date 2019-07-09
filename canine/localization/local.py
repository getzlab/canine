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
from ..backends import AbstractSlurmBackend, AbstractTransport
from ..utils import get_default_gcp_project, check_call
from agutil import status_bar

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

    def __init__(self, backend: AbstractSlurmBackend, strategy: str = "batched", transfer_bucket: typing.Optional[str] = None, common: bool = True, staging_dir: str = None, mount_path: str = None, localize_gs: bool = None):
        """
        Initializes the Localizer using the given transport.
        Localizer assumes that the SLURMBackend is connected and functional during
        the localizer's entire life cycle.
        If staging_dir is not provided, a random directory is chosen
        """
        super().__init__(backend, strategy, transfer_bucket, common, staging_dir, mount_path, localize_gs)
        self.queued_gs = [] # Queued gs:// -> remote staging transfers
        self.queued_batch = [] # Queued local -> remote directory transfers

    def localize_file(self, src: str, dest: PathType):
        """
        Localizes the given file.
        gs:// files are queued for later transfer
        local files are symlinked to the staging directory
        """
        if src.startswith('gs://'):
            self.queued_gs.append((
                src,
                dest.controllerpath,
                'remote'
            ))
        elif os.path.exists(src):
            src = os.path.abspath(src)
            if not os.path.isdir(os.path.dirname(dest.localpath)):
                os.makedirs(os.path.dirname(dest.localpath))
            if os.path.isfile(src):
                os.symlink(src, dest.localpath)
            else:
                self.queued_batch.append((src, os.path.join(dest.controllerpath, os.path.basename(src))))

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
                    self.localize_file(path, common_dests[path])
                else:
                    print("Could not handle common file", path, file=sys.stderr)
            for jobId, data in inputs.items():
                os.makedirs(os.path.join(
                    self.environment('local')['CANINE_JOBS'],
                    jobId,
                ))
                # workspace_path = os.path.join(self.environment('controller')['CANINE_JOBS'], str(jobId), 'workspace')
                # self.stage_dir(workspace_path)
                self.inputs[jobId] = {}
                for arg, value in data.items():
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
                                    self.inputs[jobId][arg].path
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
                                self.inputs[jobId][arg].path
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
                                    self.inputs[jobId][arg].path
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
                            self.localize_file(value, remote_path)
                            value = remote_path
                        self.inputs[jobId][arg] = Localization(
                            None,
                            # value will either be a PathType, if localized above,
                            # or an unchanged string if not handled
                            value
                        )
                # Now localize job setup and teardown scripts
                setup_text = ''
                job_vars = []
                exports = []
                extra_tasks = [
                    'if [[ -d $CANINE_JOB_INPUTS ]]; then cd $CANINE_JOB_INPUTS; fi'
                ]
                for key, val in self.inputs[jobId].items():
                    if val.type == 'stream':
                        job_vars.append(shlex.quote(key))
                        dest = self.reserve_path('jobs', jobId, 'inputs', os.path.basename(os.path.abspath(val.path)))
                        extra_tasks += [
                            'if [[ -e {0} ]]; then rm {0}; fi'.format(dest.computepath),
                            'mkfifo {}'.format(dest.computepath),
                            "gsutil {} cat {} > {} &".format(
                                '-u {}'.format(shlex.quote(get_default_gcp_project())) if self.get_requester_pays(val.path) else '',
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
                                '-u {}'.format(shlex.quote(get_default_gcp_project())) if self.get_requester_pays(val.path) else '',
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
                script_path = self.reserve_path('jobs', jobId, 'setup.sh')
                script = '\n'.join(
                    line.rstrip()
                    for line in [
                        '#!/bin/bash',
                        'export CANINE_JOB_VARS={}'.format(':'.join(job_vars)),
                        'export CANINE_JOB_INPUTS="{}"'.format(os.path.join(self.environment('compute')['CANINE_JOBS'], jobId, 'inputs')),
                        'export CANINE_JOB_ROOT="{}"'.format(os.path.join(self.environment('compute')['CANINE_JOBS'], jobId, 'workspace')),
                        'export CANINE_JOB_SETUP="{}"'.format(os.path.join(self.environment('compute')['CANINE_JOBS'], jobId, 'setup.sh')),
                        'export CANINE_JOB_TEARDOWN="{}"'.format(os.path.join(self.environment('compute')['CANINE_JOBS'], jobId, 'teardown.sh')),
                    ] + exports + extra_tasks
                ) + '\ncd $CANINE_JOB_ROOT\n' + setup_text
                with open(script_path.localpath, 'w') as w:
                    w.write(script)
                os.chmod(script_path.localpath, 0o775)
                script_path = self.reserve_path('jobs', jobId, 'teardown.sh')
                script = '\n'.join(
                    line.rstrip()
                    for line in [
                        '#!/bin/bash',
                        'if [[ -d {0} ]]; then cd {0}; fi'.format(os.path.join(self.environment('compute')['CANINE_JOBS'], jobId, 'workspace')),
                        'if which python3 2>/dev/null >/dev/null; then python3 {0} {1} {2} {3}; else python {0} {1} {2} {3}; fi'.format(
                            os.path.join(self.environment('compute')['CANINE_ROOT'], 'delocalization.py'),
                            self.environment('compute')['CANINE_OUTPUT'],
                            jobId,
                            ' '.join(
                                '-p {} {}'.format(name, shlex.quote(pattern))
                                for name, pattern in patterns.items()
                            )
                        ),
                    ]
                )
                with open(script_path.localpath, 'w') as w:
                    w.write(script)
                os.chmod(script_path.localpath, 0o775)
            os.symlink(
                os.path.join(
                    os.path.dirname(__file__),
                    'delocalization.py'
                ),
                os.path.join(self.environment('local')['CANINE_ROOT'], 'delocalization.py')
            )
            self.sendtree(
                self.local_dir,
                self.staging_dir,
                transport
            )
            if not transport.isdir(self.environment('controller')['CANINE_COMMON']):
                transport.mkdir(self.environment('controller')['CANINE_COMMON'])
            if not transport.isdir(self.environment('controller')['CANINE_JOBS']):
                transport.mkdir(self.environment('controller')['CANINE_JOBS'])
            print("Finalizing directory structure. This may take a while...")
            for jobId in status_bar.iter(inputs):
                if not transport.isdir(os.path.join(self.environment('controller')['CANINE_JOBS'], jobId, 'workspace')):
                    transport.makedirs(os.path.join(self.environment('controller')['CANINE_JOBS'], jobId, 'workspace'))
                if not transport.isdir(os.path.join(self.environment('controller')['CANINE_JOBS'], jobId, 'inputs')):
                    transport.makedirs(os.path.join(self.environment('controller')['CANINE_JOBS'], jobId, 'inputs'))
            if not transport.isdir(self.environment('controller')['CANINE_OUTPUT']):
                transport.mkdir(self.environment('controller')['CANINE_OUTPUT'])
            for src, dest, context in self.queued_gs:
                self.gs_copy(src, dest, context)
            for src, dest in self.queued_batch:
                self.sendtree(src, os.path.dirname(dest))
            return transport.normpath(self.staging_dir)

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
    def localize_file(self, src: str, dest: PathType):
        """
        Localizes the given file.
        gs:// files are queued for later transfer
        local files are symlinked to the staging directory
        """
        if src.startswith('gs://'):
            self.gs_copy(
                src,
                dest.localpath,
                'local'
            )
        elif os.path.exists(src):
            src = os.path.abspath(src)
            if not os.path.isdir(os.path.dirname(dest.localpath)):
                os.makedirs(os.path.dirname(dest.localpath))
            if os.path.isfile(src):
                os.symlink(src, dest.localpath)
            else:
                self.queued_batch.append((src, os.path.join(dest.controllerpath, os.path.basename(src))))
