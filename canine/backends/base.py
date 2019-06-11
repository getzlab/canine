import abc
import typing
import re
import shutil
from contextlib import ExitStack
from subprocess import CalledProcessError
from ..utils import ArgumentHelper
import pandas as pd

batch_job_pattern = re.compile(r'Submitted batch job (\d+)')

class AbstractTransport(abc.ABC):
    """
    Base class for file transport
    """
    @abc.abstractmethod
    def __enter__(self):
        """
        Allows the Transport to function as a context manager
        No action is required, so long as the context meets these requirements
        * Any required initialization takes place during __enter__
        * All transport methods work within the context
        * If initialization is required, transport methods fail outside context
        """
        pass

    @abc.abstractmethod
    def __exit__(self, *args):
        """
        Allows the Transport to function as a context manager
        No action is required, so long as the context meets these requirements
        * Any required teardown takes place during __exit__
        * If teardown is required, transport methods fail outside context
        """
        pass

    @abc.abstractmethod
    def open(self, filename: str, mode: str = 'r', bufsize: int = -1) -> typing.IO:
        """
        Returns a File-Like object open on the slurm cluster
        """
        pass

    def send(self, localfile: typing.Union[str, typing.IO], remotefile: typing.Union[str, typing.IO]):
        """
        Sends the requested file to the slurm cluster
        Both localfile and remotefile can either be a filepath or a file object
        If remotefile is a file object, it must have been opened by this transport
        or the copy will not behave as expected
        """
        with ExitStack() as stack:
            if isinstance(localfile, str):
                localfile = stack.enter_context(open(localfile, 'rb'))
            if isinstance(remotefile, str):
                remotefile = stack.enter_context(self.open(remotefile, 'wb' if 'b' in localfile.mode else 'w'))
            shutil.copyfileobj(localfile, remotefile)

    def receive(self, remotefile: typing.Union[str, typing.IO], localfile: typing.Union[str, typing.IO]):
        """
        Copies the requested file from the slurm cluster
        Both localfile and remotefile can either be a filepath or a file object
        If remotefile is a file object, it must have been opened by this transport
        or the copy will not behave as expected
        """
        with ExitStack() as stack:
            if isinstance(localfile, str):
                localfile = stack.enter_context(open(localfile, 'rb'))
            if isinstance(remotefile, str):
                remotefile = stack.enter_context(self.open(remotefile, 'wb' if 'b' in localfile.mode else 'w'))
            shutil.copyfileobj(remotefile, localfile)

    @abc.abstractmethod
    def listdir(self, path: str) -> typing.List[str]:
        """
        Lists the contents of the requested path
        """
        pass

    @abc.abstractmethod
    def mkdir(self, path: str):
        """
        Creates the requested directory
        """
        pass

    @abc.abstractmethod
    def stat(self, path: str) -> typing.Any:
        """
        Returns stat information
        """
        pass

    def exists(self, path: str) -> bool:
        """
        Returns True if the given path exists
        """
        try:
            self.stat(path)
            return True
        except FileNotFoundError:
            return False


class AbstractSlurmBackend(abc.ABC):
    """
    Base class for a SLURM backend
    """

    @abc.abstractmethod
    def invoke(self, command: str) -> typing.Tuple[int, typing.BinaryIO, typing.BinaryIO]:
        """
        Invoke an arbitrary command in the slurm console
        Returns a tuple containing (exit status, byte stream of standard out from the command, byte stream of stderr from the command)
        """
        pass

    def squeue(self, *slurmopts: str, **slurmparams: typing.Any) -> pd.DataFrame:
        """
        Shows the current status of the job queue
        slurmopts and kwargs are passed into an ArgumentHelper and unpacked
        as command line arguments
        """
        command = 'squeue'+ArgumentHelper(*slurmopts, **slurmparams).commandline
        status, stdout, stderr = self.invoke(command)
        if status != 0:
            raise CalledProcessError(status, command)
        return pd.read_fwf(
            stdout,
            index_col=0
        )


    def sacct(self, *slurmopts: str, **slurmparams: typing.Any) -> pd.DataFrame:
        """
        Shows the current job accounting information
        slurmopts and slurmparams are passed into an ArgumentHelper and unpacked
        as command line arguments
        """
        command = 'sacct'+ArgumentHelper(*slurmopts, **slurmparams).commandline
        status, stdout, stderr = self.invoke(command)
        if status != 0:
            raise CalledProcessError(status, command)
        return pd.read_fwf(
            stdout,
            index_col=0
        )

    def sinfo(self, *slurmopts: str, **slurmparams: typing.Any) -> pd.DataFrame:
        """
        Shows the current cluster information
        slurmopts and slurmparams are passed into an ArgumentHelper and unpacked
        as command line arguments
        """
        command = 'sinfo'+ArgumentHelper(*slurmopts, **slurmparams).commandline
        status, stdout, stderr = self.invoke(command)
        if status != 0:
            raise CalledProcessError(status, command)
        return pd.read_fwf(
            stdout,
            index_col=0
        )

    @abc.abstractmethod
    def srun(self, command: str, *slurmopts: str, **slurmparams: typing.Any) -> typing.Tuple[int, typing.BinaryIO, typing.BinaryIO]:
        """
        Runs the given command interactively
        The first argument MUST contain the entire command and options to run
        Additional arguments and keyword arguments provided are passed as slurm arguments to srun
        Returns a tuple containing (exit status, stdout buffer, stderr buffer)
        """
        pass

    def sbatch(self, command: str, *slurmopts: str, **slurmparams: typing.Any) -> str:
        """
        Runs the given command in the background
        The first argument MUST contain the entire command and options to run
        Additional arguments and keyword arguments provided are passed as slurm arguments to sbatch
        Returns the jobID of the batch request
        """
        command = 'sbatch {} -- {}'.format(
            ArgumentHelper(*slurmopts, **slurmparams).commandline,
            command
        )
        status, stdout, stderr = self.invoke(command)
        if status != 0:
            raise CalledProcessError(status, command)
        result = batch_job_pattern.search(stdout.read().decode())
        if result:
            return result.group(1)
        raise ValueError("Command output did not match pattern")

    def scancel(self, jobID: str, *slurmopts: str, **slurmparams: typing.Any):
        """
        Cancels the given jobID
        The first argument MUST be the jobID
        Additional arguments and keyword arguments are passed as slurm arguments
        to scancel
        """
        command = 'scancel {} {}'.format(
            ArgumentHelper(*slurmopts, **slurmparams).commandline,
            jobID
        )
        status, stdout, stderr = self.invoke(command)
        if status != 0:
            raise CalledProcessError(status, command)


    @abc.abstractmethod
    def __enter__(self):
        """
        Allows backends to serve as context managers
        The context is not explicitly required to do anything, but must meet the
        following:
        * Any required initialization happens during __enter__
        * Slurm commands will be connected and functional inside the context
        * If the backend requires an initialization step, slurm commands will not
        function before entering the context
        """
        pass

    @abc.abstractmethod
    def __exit__(self, *args):
        """
        Allows backends to serve as context managers
        The context is not explicitly required to do anything, but must meet the
        following:
        * Any required teardown happens during __exit__
        * If the backend requires explicit teardown, slurm commands will not function
        after exiting the context
        """
        pass

    @abc.abstractmethod
    def transport(self) -> AbstractTransport:
        """
        Return a Transport object suitable for moving files between the
        SLURM cluster and the local filesystem
        """
        pass
