import typing
import os
import io
import sys
import subprocess
from .base import AbstractSlurmBackend, AbstractTransport
from ..utils import ArgumentHelper
from agutil import StdOutAdapter
import pandas as pd

class LocalTransport(AbstractTransport):
    """
    Base class for file transport
    """
    def __enter__(self):
        """
        Allows the Transport to function as a context manager
        No action is taken
        """
        return self

    def __exit__(self, *args):
        """
        Allows the Transport to function as a context manager
        No action is taken
        """
        pass

    def open(self, filename: str, mode: str = 'r', bufsize: int = -1) -> typing.IO:
        """
        Returns a File-Like object open on the slurm cluster
        """
        return open(filename, mode, buffering=bufsize)

    def listdir(self, path: str) -> typing.List[str]:
        """
        Lists the contents of the requested path
        """
        return os.listdir(path)

    def mkdir(self, path: str):
        """
        Creates the requested directory
        """
        return os.mkdir(path)

    def stat(self, path: str) -> typing.Any:
        """
        Returns stat information
        """
        return os.stat(path)

class LocalSlurmBackend(AbstractSlurmBackend):
    """
    SLURM backend for interacting with a local slurm node
    """

    def invoke(self, command: str) -> typing.Tuple[int, typing.BinaryIO, typing.BinaryIO]:
        """
        Invoke an arbitrary command in the slurm console
        Returns a tuple containing (exit status, byte stream of standard out from the command, byte stream of stderr from the command)
        """
        stdout = StdOutAdapter(True)
        stderr = StdOutAdapter(True)
        stdinFD = os.dup(sys.stdin.fileno())
        proc = subprocess.Popen(
            command,
            shell=True,
            stdout=stdout.writeFD,
            stderr=stderr.writeFD,
            stdin=stdinFD,
            universal_newlines=False,
            executable='/bin/bash'
        )
        proc.wait()
        stdout.kill()
        stderr.kill()
        os.close(stdinFD)
        return (
            proc.returncode,
            io.BytesIO(stdout.buffer),
            io.BytesIO(stderr.buffer)
        )

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
        return self.invoke(command)

    def __enter__(self):
        """
        Allows the Local backend to serve as a context manager
        No action is taken
        """
        return self

    def __exit__(self, *args):
        """
        Allows the Local backend to serve as a context manager
        No action is taken
        """
        pass

    def transport(self) -> LocalTransport:
        """
        Return a Transport object suitable for moving files between the
        SLURM cluster and the local filesystem
        """
        return LocalTransport()
