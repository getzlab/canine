import typing
import os
import io
import sys
import subprocess
from .base import AbstractSlurmBackend, AbstractTransport
from ..utils import ArgumentHelper, check_call
from agutil import StdOutAdapter
import pandas as pd

class LocalTransport(AbstractTransport):
    """
    Dummy file transport for working with the local filesystem
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

    def chmod(self, path: str, mode: int):
        """
        Change file permissions
        """
        os.chmod(path, mode)

    def normpath(self, path: str) -> str:
        """
        Returns a normalized path relative to the transport
        """
        return os.path.abspath(os.path.normpath(path))

    def remove(self, path: str):
        """
        Removes the file at the given path
        """
        os.remove(path)

    def rmdir(self, path: str):
        """
        Removes the directory at the given path
        """
        os.rmdir(path)

    def mklink(self, src: str, dest: str):
        """
        Creates a symlink from dest->src
        """
        os.symlink(src, dest)

    def rename(self, src: str, dest: str):
        """
        Move the file or folder 'src' to 'dest'
        Will overwrite dest if it exists
        """
        os.rename(src, dest)

    def walk(self, path: str) -> typing.Generator[typing.Tuple[str, typing.List[str], typing.List[str]], None, None]:
        """
        Walk through a directory tree
        Each iteration yields a 3-tuple:
        (dirpath, dirnames, filenames) ->
        * dirpath: The current filepath relative to the starting path
        * dirnames: The base names of all subdirectories in the current directory
        * filenames: The base names of all files in the current directory
        """
        yield from os.walk(path)

class LocalSlurmBackend(AbstractSlurmBackend):
    """
    SLURM backend for interacting with a local slurm node
    """

    def invoke(self, command: str, interactive: bool = False) -> typing.Tuple[int, typing.BinaryIO, typing.BinaryIO]:
        """
        Invoke an arbitrary command in the slurm console
        Returns a tuple containing (exit status, byte stream of standard out from the command, byte stream of stderr from the command).
        If interactive is True, stdin, stdout, and stderr should all be connected live to the user's terminal.
        """
        stdout = StdOutAdapter(interactive)
        stderr = StdOutAdapter(interactive)
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
