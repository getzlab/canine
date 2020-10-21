import abc
import typing
import re
import shutil
import os
import time
import stat
import sys
from contextlib import ExitStack
from uuid import uuid4 as uuid
from ..utils import ArgumentHelper, check_call, canine_logging
import pandas as pd

SLURM_PARTITION_RECON = b'slurm_load_partitions: Unable to contact slurm controller (connect failure)'
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
            self.chmod(remotefile.name, os.stat(localfile.name).st_mode)

    def receive(self, remotefile: typing.Union[str, typing.IO], localfile: typing.Union[str, typing.IO]):
        """
        Copies the requested file from the slurm cluster
        Both localfile and remotefile can either be a filepath or a file object
        If remotefile is a file object, it must have been opened by this transport
        or the copy will not behave as expected
        """
        with ExitStack() as stack:
            if isinstance(remotefile, str):
                remotefile = stack.enter_context(self.open(remotefile, 'rb'))
            if isinstance(localfile, str):
                localfile = stack.enter_context(open(localfile, 'wb' if 'b' in remotefile.mode else 'w'))
            shutil.copyfileobj(remotefile, localfile)
            os.chmod(localfile.name, self.stat(remotefile.name).st_mode)

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
    def stat(self, path: str, follow_symlinks: bool = True) -> typing.Any:
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

    @abc.abstractmethod
    def chmod(self, path: str, mode: int):
        """
        Change file permissions
        """
        pass

    @abc.abstractmethod
    def normpath(self, path: str) -> str:
        """
        Returns a normalized path relative to the transport
        """
        pass

    @abc.abstractmethod
    def remove(self, path: str):
        """
        Removes the file at the given path
        """
        pass

    @abc.abstractmethod
    def rmdir(self, path: str):
        """
        Removes the directory at the given path
        """
        pass

    @abc.abstractmethod
    def rename(self, src: str, dest: str):
        """
        Move the file or folder 'src' to 'dest'
        Will overwrite dest if it exists
        """
        pass

    @abc.abstractmethod
    def mklink(self, src: str, dest: str):
        """
        Creates a symlink from dest->src
        """
        pass

    def isdir(self, path: str) -> bool:
        """
        Returns True if the requested path is a directory
        """
        try:
            return stat.S_ISDIR(self.stat(path).st_mode)
        except FileNotFoundError:
            return False

    def isfile(self, path: str) -> bool:
        """
        Returns True if the requested path is a regular file
        """
        try:
            return stat.S_ISREG(self.stat(path).st_mode)
        except FileNotFoundError:
            return False

    def islink(self, path: str) -> bool:
        """
        Returns True if the requested path is a symlink
        """
        try:
            return stat.S_ISLNK(self.stat(path).st_mode)
        except FileNotFoundError:
            return False

    @abc.abstractmethod
    def glob(self, path: str) -> typing.List[str]:
        """
        Returns an array matching the glob pattern, or an empty list if no match.
        """
        pass

    def makedirs(self, path: str, exist_okay: bool = False):
        """
        Recursively build the requested directory structure
        """
        if path == '' or self.isdir(path):
            return
        dirname = os.path.dirname(path)
        if not (dirname == '' or self.exists(dirname)):
            self.makedirs(dirname)
        if not self.exists(path):
            self.mkdir(path)
        elif not exist_okay:
            raise FileExistsError(path)

    def walk(self, path: str) -> typing.Generator[typing.Tuple[str, typing.List[str], typing.List[str]], None, None]:
        """
        Walk through a directory tree
        Each iteration yields a 3-tuple:
        (dirpath, dirnames, filenames) ->
        * dirpath: The current filepath relative to the starting path
        * dirnames: The base names of all subdirectories in the current directory
        * filenames: The base names of all files in the current directory
        """
        dirnames = []
        filenames = []
        for name in self.listdir(path):
            if self.isdir(os.path.join(path, name)):
                dirnames.append(name)
            else:
                filenames.append(name)
        yield (path, dirnames, filenames)
        for dirname in dirnames:
            yield from self.walk(os.path.join(path, dirname))

    def rmtree(self, path: str, max_retries: int = 5, timeout: int = 5):
        """
        Recursively remove the directory tree rooted at the given path.
        Automatically retries failures after a brief timeout
        """
        pathstat = self.stat(path)
        if not stat.S_ISDIR(pathstat.st_mode):
            raise NotADirectoryError(path)
        for attempt in range(max_retries):
            try:
                return self._rmtree(path, pathstat)
            except (OSError, FileNotFoundError, IOError, NotADirectoryError):
                # Helps to preserve the exception traceback by conditionally re-raising here
                if attempt >= (max_retries - 1):
                    raise
            time.sleep(timeout)
        # Should not be possible to reach here
        raise RuntimeError("AbstractTransport.rmtree exceeded retries without exception")

    def _rmtree(self, path: str, pathstat: os.stat_result):
        """
        (Internal)
        Recursively remove the directory tree rooted at the given path.
        Automatically retries failures after a brief timeout
        """
        if not stat.S_ISDIR(pathstat.st_mode):
            raise NotADirectoryError(path)
        for fname in self.listdir(path):
            fname = os.path.join(path, fname)
            try:
                fstat = self.stat(fname)
            except FileNotFoundError:
                # Handling for broken symlinks is bad
                self.remove(fname)
            else:
                if stat.S_ISDIR(fstat.st_mode):
                    self._rmtree(
                        fname,
                        fstat
                    )
                else:
                    self.remove(fname)
        self.rmdir(path)

    def sendtree(self, src: str, dest: str):
        """
        Copy the full local file tree src to the remote path dest
        """
        if not self.exists(dest):
            self.makedirs(dest)
        for path, dirnames, filenames in os.walk(src):
            rpath = os.path.join(
                dest,
                os.path.relpath(path, src)
            )
            if not self.exists(rpath):
                self.mkdir(rpath)
            for f in filenames:
                self.send(os.path.join(path, f), os.path.join(rpath, f))


    def receivetree(self, src: str, dest: str):
        """
        Copy the full remote file tree src to the local path dest
        """
        if not os.path.exists(dest):
            os.makedirs(dest)
        for path, dirnames, filenames in self.walk(src):
            lpath = os.path.join(
                dest,
                os.path.relpath(path, src)
            )
            if not os.path.exists(lpath):
                os.mkdir(lpath)
            for f in filenames:
                self.receive(os.path.join(path, f), os.path.join(lpath, f))

class AbstractSlurmBackend(abc.ABC):
    """
    Base class for a SLURM backend
    """

    def __init__(self, hard_reset_on_orch_init: bool = True, **kwargs):
        """
        If an implementing class defines a constructor, it must take **kwargs.
        """

        #:param bool hard_reset_on_orch_init: Whether this backend needs the orchestrator to hard reset it after initialization (True), or the backend resets itself (False)
        self.hard_reset_on_orch_init = hard_reset_on_orch_init

    @abc.abstractmethod
    def invoke(self, command: str, interactive: bool = False, **kwargs) -> typing.Tuple[int, typing.BinaryIO, typing.BinaryIO]:
        """
        Invoke an arbitrary command in the slurm console
        Returns a tuple containing (exit status, byte stream of standard out from the command, byte stream of stderr from the command).
        If interactive is True, stdin, stdout, and stderr should all be connected live to the user's terminal
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
        check_call(command, status, stdout, stderr)
        df = pd.read_fwf(
            stdout,
            index_col=0
        )
        df.index = df.index.map(str)
        return df


    def sacct(self, *slurmopts: str, **slurmparams: typing.Any) -> pd.DataFrame:
        """
        Shows the current job accounting information
        slurmopts and slurmparams are passed into an ArgumentHelper and unpacked
        as command line arguments
        """
        command = 'sacct'+ArgumentHelper(*slurmopts, **slurmparams).commandline
        status, stdout, stderr = self.invoke(command)
        check_call(command, status, stdout, stderr)
        df = pd.read_fwf(
            stdout,
            index_col=0
        ).iloc[1:]
        df.index = df.index.map(str)
        return df

    def sinfo(self, *slurmopts: str, **slurmparams: typing.Any) -> pd.DataFrame:
        """
        Shows the current cluster information
        slurmopts and slurmparams are passed into an ArgumentHelper and unpacked
        as command line arguments
        """
        command = 'sinfo'+ArgumentHelper(*slurmopts, **slurmparams).commandline
        status, stdout, stderr = self.invoke(command)
        check_call(command, status, stdout, stderr)
        df = pd.read_fwf(
            stdout,
            index_col=0
        )
        df.index = df.index.map(str)
        return df

    def srun(self, command: str, *slurmopts: str, **slurmparams: typing.Any) -> typing.Tuple[int, typing.BinaryIO, typing.BinaryIO]:
        """
        Runs the given command interactively
        The first argument MUST contain the entire command and options to run
        Additional arguments and keyword arguments provided are passed as slurm arguments to srun
        Returns a tuple containing (exit status, stdout buffer, stderr buffer)
        """
        command = 'srun {} -- {}'.format(
            ArgumentHelper(*slurmopts, **slurmparams).commandline,
            command
        )
        status, stdout, stderr = self.invoke(command, True)
        check_call(command, status) # Don't pass stdout and stderr here, they're already printed live to terminal
        return status, stdout, stderr

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
        check_call(command, status, stdout, stderr)
        out = stdout.read().decode()
        err = stderr.read().decode()
        result = batch_job_pattern.search(out)
        if result:
            return result.group(1)
        raise ValueError("Command\n  {command}\noutput did not match sbatch return pattern.\nstdout: {stdout}\nstderr: {stderr}\nexit code: {exitcode}".format(command = command, stdout = out, stderr = err, exitcode = status))

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
        check_call(command, status, stdout, stderr)


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

    def pack_batch_script(self, *commands: str, script_path: typing.Optional[str] = None):
        """
        writes the list of commands to a file and runs it
        Script_path may be the path to where the script should be written or None
        """
        if script_path is None:
            script_path = str(uuid())+'.sh'
        with self.transport() as transport:
            transport.makedirs(os.path.dirname(script_path))
            with transport.open(script_path, 'w') as w:
                w.write('#!/bin/bash\n')
                for command in commands:
                    w.write(command.rstrip()+'\n')
            transport.chmod(script_path, 0o775)
        return script_path

    def wait_for_cluster_ready(self, elastic: bool = True, timeout = 0):
        """
        Blocks until the main partition is marked as up.
        An optional timeout command will only block until this many seconds
        (default: block forever)
        """

        status, stdout, stderr = self.invoke("sinfo")
        n_iter = 0
        while status != 0 and SLURM_PARTITION_RECON in stderr.read():
            canine_logging.warning("Slurm controller not ready. Retrying in 10s...")
            time.sleep(10)
            status, stdout, stderr = self.invoke("sinfo")
            n_iter += 1

            if timeout > 0 and n_iter*10 >= timeout:
                raise TimeoutError("Timed out waiting for Slurm cluster to come online after {} seconds.".format(timeout))

        df = self.sinfo()
        default = df.index[df.index.str.contains(r"\*$")]

        # wait for partition to appear
        while len(default) == 0:
            time.sleep(10)
            df = self.sinfo()
            default = df.index[df.index.str.contains(r"\*$")]
        while (df.loc[default, "AVAIL"] != 'up').all():
            time.sleep(10)
            df = self.sinfo()

        # if Canine is responsible for managing worker nodes, then we have to check
        # whether any workers have started.
        if not elastic:
            # wait for any node in the partition to be ready
            while ~df.loc[default, "STATE"].str.contains(r"(?:mixed|idle~?|completing|allocated\+?)$").any():
                time.sleep(10)
                df = self.sinfo()

    def estimate_cost(self, clock_uptime: typing.Optional[float] = None, node_uptime: typing.Optional[float] = None, job_cpu_time: typing.Optional[typing.Dict[str, float]] = None) -> typing.Tuple[float, typing.Optional[typing.Dict[str, float]]]:
        """
        Returns a cost estimate for the cluster, based on any cost information available
        to the backend. May provide total node uptime (for cluster cost estimate)
        and/or cpu_time for each job to get job specific cost estimates.
        Clock uptime may be provided and is useful if the cluster has an inherrant
        overhead for uptime (ie: controller nodes).
        Note: Job cost estimates may not sum up to the total cluster cost if the
        cluster was not at full utilization.
        All units are in hours
        """
        return 0, None
