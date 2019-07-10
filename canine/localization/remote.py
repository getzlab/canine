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
            with self.transport_context(transport) as transport:
                if not transport.isdir(os.path.dirname(dest.controllerpath)):
                    transport.makedirs(os.path.dirname(dest.controllerpath))
                if os.path.isfile(src):
                    transport.send(src, dest.controllerpath)
                else:
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
                common_dests = self.pick_common_inputs(inputs, overrides, transport=transport)
            else:
                common_dests = {}
            for jobId, data in inputs.items():
                transport.makedirs(
                    os.path.join(
                        self.environment('controller')['CANINE_JOBS'],
                        jobId
                    )
                )
                self.prepare_job_inputs(jobId, data, common_dests, overrides, transport=transport)
                # Now localize job setup and teardown scripts
                setup_script, teardown_script = self.job_setup_teardown(jobId, patterns)
                # Setup:
                script_path = self.reserve_path('jobs', jobId, 'setup.sh')
                with transport.open(script_path.controllerpath, 'w') as w:
                    w.write(setup_script)
                transport.chmod(script_path.controllerpath, 0o775)
                # Teardown:
                script_path = self.reserve_path('jobs', jobId, 'teardown.sh')
                with transport.open(script_path.controllerpath, 'w') as w:
                    w.write(teardown_script)
                transport.chmod(script_path.controllerpath, 0o775)
            transport.send(
                os.path.join(
                    os.path.dirname(__file__),
                    'delocalization.py'
                ),
                os.path.join(self.environment('controller')['CANINE_ROOT'], 'delocalization.py')
            )
            return self.finalize_staging_dir(inputs.keys(), transport=transport)
