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
            if not transport.isdir(self.environment('remote')['CANINE_ROOT']):
                transport.makedirs(self.environment('remote')['CANINE_ROOT'])
            if not transport.isdir(self.environment('remote')['CANINE_COMMON']):
                transport.makedirs(self.environment('remote')['CANINE_COMMON'])
            if not transport.isdir(self.environment('remote')['CANINE_JOBS']):
                transport.makedirs(self.environment('remote')['CANINE_JOBS'])
            if not transport.isdir(self.environment('remote')['CANINE_OUTPUT']):
                transport.makedirs(self.environment('remote')['CANINE_OUTPUT'])
        return self

    def localize_file(self, src: str, dest: PathType, transport: typing.Optional[AbstractTransport] = None):
        """
        Localizes the given file.
        All files are immediately transferred
        """
        if src.startswith('gs://'):
            self.gs_copy(
                src,
                dest.remotepath,
                'remote'
            )
        elif os.path.exists(src):
            with self.transport_context(transport) as transport:
                if not transport.isdir(os.path.dirname(dest.remotepath)):
                    transport.makedirs(os.path.dirname(dest.remotepath))
                if os.path.isfile(src):
                    transport.send(src, dest.remotepath)
                else:
                    self.sendtree(
                        src,
                        dest.remotepath,
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
                # noop; this shard has been avoided
                if data is None:
                    continue

                transport.makedirs(
                    os.path.join(
                        self.environment('remote')['CANINE_JOBS'],
                        jobId
                    )
                )
                self.prepare_job_inputs(jobId, data, common_dests, overrides, transport=transport)

                # Now localize job setup, localization, and teardown scripts
                setup_script, localization_script, teardown_script = self.job_setup_teardown(jobId, patterns)

                # Setup:
                script_path = self.reserve_path('jobs', jobId, 'setup.sh')
                with transport.open(script_path.remotepath, 'w') as w:
                    w.write(setup_script)
                transport.chmod(script_path.remotepath, 0o775)

                # Localization:
                script_path = self.reserve_path('jobs', jobId, 'localization.sh')
                with transport.open(script_path.remotepath, 'w') as w:
                    w.write(localization_script)
                transport.chmod(script_path.remotepath, 0o775)

                # Teardown:
                script_path = self.reserve_path('jobs', jobId, 'teardown.sh')
                with transport.open(script_path.remotepath, 'w') as w:
                    w.write(teardown_script)
                transport.chmod(script_path.remotepath, 0o775)

            # send delocalization script
            transport.send(
                os.path.join(
                    os.path.dirname(__file__),
                    'delocalization.py'
                ),
                os.path.join(self.environment('remote')['CANINE_ROOT'], 'delocalization.py')
            )

            # send debug script
            transport.send(
                os.path.join(
                    os.path.dirname(__file__),
                    'debug.sh'
                ),
                os.path.join(self.environment('remote')['CANINE_ROOT'], 'debug.sh')
            )

            return self.finalize_staging_dir(inputs.keys(), transport=transport)
