import typing
import os
import sys
import select
import io
import warnings
import shlex
from subprocess import CalledProcessError
import google.auth
import paramiko

class ArgumentHelper(dict):
    """
    Helper class for setting arguments to slurm commands
    Used only to handle keyword arguments to console commands
    Should not be responsible for positionals
    """

    def __init__(self, *flags: str, **params: typing.Any):
        """
        Creates a new ArgumentHelper
        Flags can be passed as positional arguments
        Parameters can be passed as keyword arguments
        """
        object.__setattr__(self, 'defaults', {})
        object.__setattr__(self, 'flags', [item for item in flags])
        object.__setattr__(self, 'params', {k:v for k,v in params.items()})
        for key, val in [*self.params.items()]:
            if val is True:
                self.flags.append(key)
                del self.params[key]

    def __repr__(self) -> str:
        return '<ArgumentHelper{}>'.format(
            self.commandline
        )

    def __getattr__(self, name):
        return self[name]

    def __setattr__(self, name, value):
        self[name] = value

    def __delattr__(self, name):
        del self[name]

    def __getitem__(self, name):
        if name in self.params:
            return self.params[name]
        if name in self.flags:
            return True
        if name in self.defaults:
            return self.defaults[name]

    def __setitem__(self, name, value):
        if value is True:
            self.flags.append(name)
        elif value is False:
            object.__setattr__(self, 'flags', [flag for flag in self.flags if flag != name])
        else:
            self.params[name] = value

    def __delitem__(self, name):
        if name in self.params:
            del self.params[name]
        elif name in self.flags:
            object.__setattr__(self, 'flags', [flag for flag in self.flags if flag != name])
        else:
            raise KeyError("No such argument {}".format(name))

    @staticmethod
    def translate(flag) -> str:
        """Converts acceptable python strings to command line args"""
        return flag.replace('_', '-')

    @property
    def commandline(self) -> str:
        """Expands the arguments to command line form"""
        return '{short_prespace}{short_flags}{long_flags}{params}'.format(
            short_prespace=' -' if len([f for f in self.flags if len(f) == 1]) else '',
            short_flags=''.join(flag for flag in self.flags if len(flag)==1),
            long_flags=''.join(' --{}'.format(self.translate(flag)) for flag in self.flags if len(flag) > 1),
            params=''.join(
                ' --{}={}'.format(self.translate(key), shlex.quote(value))
                for key, value in self.params.items()
            )
        )

    def setdefaults(self, **kwargs: typing.Any):
        self.defaults.update(kwargs)

def make_interactive(channel: paramiko.Channel) -> typing.Tuple[int, typing.BinaryIO, typing.BinaryIO]:
    """
    Manages an interactive command
    Takes in a paramiko.Channel shared by stdin, stdout, stderr of a currently running command
    The current interpreter stdin is duplicated and written to the command's stdin
    The command's stdout and stderr are written to the interpreter's stdout and stderr
    and also buffered in a ByteStream for later reading
    Returns (exit status, Stdout buffer, Stderr bufer)
    """
    infd = os.dup(sys.stdin.fileno())
    try:
        poll = select.poll()
        poll.register(infd)
        poll.register(channel, select.POLLIN+select.POLLPRI+select.POLLERR+select.POLLHUP)
        stdout = io.BytesIO()
        stderr = io.BytesIO()
        while not channel.exit_status_ready():
            for fd, event in poll.poll(0.5):
                if fd == infd and event == select.POLLIN or event == select.POLLPRI:
                    # Text available on python stdin
                    channel.send(os.read(infd, 4096))
            if channel.recv_ready():
                content = channel.recv(4096)
                sys.stdout.write(content.decode())
                stdout.write(content)
            if channel.recv_stderr_ready():
                content = channel.recv_stderr(4096)
                sys.stderr.write(content.decode())
                stderr.write(content)
        if channel.recv_ready():
            content = channel.recv(4096)
            sys.stdout.write(content.decode())
            stdout.write(content)
        if channel.recv_stderr_ready():
            content = channel.recv_stderr(4096)
            sys.stderr.write(content.decode())
            stderr.write(content)
        stdout.seek(0,0)
        stderr.seek(0,0)
        return channel.recv_exit_status(), stdout, stderr
    finally:
        os.close(infd)

__DEFAULT_GCP_PROJECT__ = None

def get_default_gcp_project():
    """
    Returns the currently configured default project
    """
    global __DEFAULT_GCP_PROJECT__
    if __DEFAULT_GCP_PROJECT__ is None:
        __DEFAULT_GCP_PROJECT__ = google.auth.default()[1]
    return __DEFAULT_GCP_PROJECT__

def check_call(cmd:str, rc: int, stdout: typing.Optional[typing.BinaryIO] = None, stderr: typing.Optional[typing.BinaryIO] = None):
    """
    Checks that the rc is 0
    If not, flush stdout and stderr streams and raise a CalledProcessError
    """
    if rc != 0:
        if stdout is not None:
            sys.stdout.write(stdout.read().decode())
            sys.stdout.flush()
        if stderr is not None:
            sys.stderr.write(stderr.read().decode())
            sys.stderr.flush()
        raise CalledProcessError(rc, cmd)
