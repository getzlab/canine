import os
import subprocess
import typing
import shlex
from contextlib import ExitStack
from .base import AbstractLocalizer, PathType, Localization
from ..backends import AbstractSlurmBackend, AbstractTransport
from ..utils import get_default_gcp_project, check_call
from agutil import status_bar

class RemoteLocalizer(AbstractLocalizer):
    """
    Remote Only strategy:
    Staging dir is created remotely and files are copied in
    """
    def __enter__(self):
        """
        Enter localizer context.
        May take any setup action required
        """
        with self.backend.transport() as transport:
            if not transport.isdir(self.environment('controller')['CANINE_ROOT']):
                transport.makedirs(self.environment('controller')['CANINE_ROOT'])
            if not transport.isdir(self.environment('controller')['CANINE_COMMON']):
                transport.makedirs(self.environment('controller')['CANINE_COMMON'])
            if not transport.isdir(self.environment('controller')['CANINE_JOBS']):
                transport.makedirs(self.environment('controller')['CANINE_JOBS'])
            if not transport.isdir(self.environment('controller')['CANINE_OUTPUT']):
                transport.makedirs(self.environment('controller')['CANINE_OUTPUT'])
        return self

    def localize_file(self, src: str, dest: PathType, transport: typing.Optional[AbstractTransport] = None):
        """
        Localizes the given file.
        All files are immediately transferred
        """
        if src.startswith('gs://'):
            self.gs_copy(
                src,
                dest.controllerpath,
                'remote'
            )
        elif os.path.exists(src):
            with ExitStack() as stack:
                if transport is None:
                    transport = stack.enter_context(self.backend.transport())
            if not transport.isdir(os.path.dirname(dest.controllerpath)):
                transport.makedirs(os.path.dirname(dest.controllerpath))
            if os.path.isfile(src):
                transport.send(src, dest.controllerpath)
            else:
                subprocess.check_call(['touch', '{}/.dir'.format(src)])
                self.sendtree(
                    src,
                    dest.controllerpath,
                    transport
                )

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
                    self.localize_file(path, common_dests[path], transport=transport)
                else:
                    print("Could not handle common file", path, file=sys.stderr)
            for jobId, data in inputs.items():
                transport.makedirs(
                    os.path.join(
                        self.environment('controller')['CANINE_JOBS'],
                        jobId
                    )
                )
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
                        dest = self.reserve_path('jobs', jobId, 'inputs', os.path.basename(val.path))
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
                        dest = self.reserve_path('jobs', jobId, 'inputs', os.path.basename(val.path))
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
                with transport.open(script_path.controllerpath, 'w') as w:
                    w.write(script)
                transport.chmod(script_path.controllerpath, 0o775)
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
                with transport.open(script_path.controllerpath, 'w') as w:
                    w.write(script)
                transport.chmod(script_path.controllerpath, 0o775)
            transport.send(
                os.path.join(
                    os.path.dirname(__file__),
                    'delocalization.py'
                ),
                os.path.join(self.environment('controller')['CANINE_ROOT'], 'delocalization.py')
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
            return transport.normpath(self.staging_dir)
