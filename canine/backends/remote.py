import typing
import os
import re
import io
import sys
import subprocess
import warnings
import binascii
import traceback
import shlex
import atexit
from .base import AbstractSlurmBackend, AbstractTransport
from ..utils import ArgumentHelper, make_interactive, check_call, isatty, canine_logging
from agutil import StdOutAdapter
import pandas as pd
import paramiko

import logging
logging.getLogger('paramiko').setLevel(logging.WARNING)

SSH_AGENT_PATTERN = re.compile(r'SSH_AUTH_SOCK=(.+); export SSH_AUTH_SOCK')
SSH_AGENT_PID = re.compile(r'SSH_AGENT_PID=(\d+); export SSH_AGENT_PID')

class IgnoreKeyPolicy(paramiko.client.AutoAddPolicy):
    """
    Slight modification of paramiko.client.AutoAddPolicy
    Doesn't save the new key to the file, but accepts it anyways
    """

    def missing_host_key(self, client, hostname, key):
        client._host_keys.add(hostname, key.get_name(), key)

class RemoteTransport(AbstractTransport):
    """
    Transport for working with remote files over ssh
    """
    def __init__(self, client: paramiko.SSHClient):
        """
        Initializes the transport from a given SSH client
        """
        self.client = client
        self.session = None

    def __enter__(self):
        """
        Starts a connection to the remote server
        """
        if self.client._transport is None:
            raise paramiko.SSHException("Client is not connected")
        self.session = self.client.open_sftp()
        return self

    def __exit__(self, *args):
        """
        Closes the connection to the remote server
        """
        self.session.close()
        self.session = None

    def open(self, filename: str, mode: str = 'r', bufsize: int = -1) -> typing.IO:
        """
        Returns a File-Like object open on the slurm cluster
        """
        if self.session is None:
            raise paramiko.SSHException("Transport is not connected")
        handle = self.session.open(filename, mode, bufsize)
        handle.mode = mode
        handle.name = filename
        if 'w' in mode:
            handle.set_pipelined(True)
        elif mode == 'r':
            actual_read = handle.read
            def read_decode(size=None, encoding=sys.getdefaultencoding()):
                return actual_read(size=size).decode(encoding=encoding)
            handle.read = read_decode
        return handle

    def listdir(self, path: str) -> typing.List[str]:
        """
        Lists the contents of the requested path
        """
        if self.session is None:
            raise paramiko.SSHException("Transport is not connected")
        return self.session.listdir(path)

    def mkdir(self, path: str):
        """
        Creates the requested directory
        """
        if self.session is None:
            raise paramiko.SSHException("Transport is not connected")
        return self.session.mkdir(path)

    def stat(self, path: str, follow_symlinks: bool = True) -> typing.Any:
        """
        Returns stat information
        """
        if self.session is None:
            raise paramiko.SSHException("Transport is not connected")
        if follow_symlinks:
            return self.session.stat(path)
        else:
            try:
                dirname = os.path.dirname(path)
                if dirname == '':
                    dirname = '.'
                return {
                    attr.filename: attr
                    for attr in self.session.listdir_attr(dirname)
                }[os.path.basename(path)]
            except KeyError as e:
                raise FileNotFoundError(path) from e

    def glob(self, path: str) -> typing.List[str]:
        raise NotImplementedError("globbing not yet implemented for remote transport!")

    def chmod(self, path: str, mode: int):
        """
        Change file permissions
        """
        if self.session is None:
            raise paramiko.SSHException("Transport is not connected")
        self.session.chmod(path, mode)

    def normpath(self, path: str) -> str:
        """
        Returns a normalized path relative to the transport
        """
        if self.session is None:
            raise paramiko.SSHException("Transport is not connected")
        try:
            return self.session.normalize(path)
        except FileNotFoundError:
            if path.startswith('/'):
                return path # Absolute
            # Relative
            return os.path.join(
                self.session.normalize('.'),
                path
            )

    def remove(self, path: str):
        """
        Removes the file at the given path
        """
        if self.session is None:
            raise paramiko.SSHException("Transport is not connected")
        self.session.remove(path)

    def rmdir(self, path: str):
        """
        Removes the directory at the given path
        """
        if self.session is None:
            raise paramiko.SSHException("Transport is not connected")
        self.session.rmdir(path)

    def mklink(self, src: str, dest: str):
        """
        Creates a symlink from dest->src
        """
        if self.session is None:
            raise paramiko.SSHException("Transport is not connected")
        self.session.symlink(src, dest)

    def rename(self, src: str, dest: str):
        """
        Move the file or folder 'src' to 'dest'
        Will overwrite dest if it exists
        """
        if self.session is None:
            raise paramiko.SSHException("Transport is not connected")
        try:
            self.session.posix_rename(src, dest)
        except IOError:
            self.session.rename(src, dest)

class RemoteSlurmBackend(AbstractSlurmBackend):
    """
    SLURM backend for interacting with a remote slurm node
    """

    @staticmethod
    def ssh_agent(restart=True) -> typing.Optional[str]:
        """
        Ensures proper environment variables are set for the ssh agent
        Returns the current value of SSH_AUTH_SOCK or None, if the agent could
        not be located/started
        """
        if restart or 'SSH_AUTH_SOCK' not in os.environ:
            proc = subprocess.run(
                'ssh-agent',
                shell=True,
                stdout=subprocess.PIPE
            )
            match = SSH_AGENT_PATTERN.search(proc.stdout.decode())
            if not match:
                return None
            os.environ['SSH_AUTH_SOCK'] = match.group(1)
            match = SSH_AGENT_PID.search(proc.stdout.decode())
            if match:
                atexit.register(lambda :os.kill(int(match.group(1)), 15))
        return os.environ['SSH_AUTH_SOCK']

    @staticmethod
    def add_key_to_agent(filepath: str):
        """
        Adds the requested filepath to the ssh agent
        This is useful for keys in formats not supported by paramiko/cryptography
        Mojave users: You will probably need to call this function on keys generated by mojave
        """
        if RemoteSlurmBackend.ssh_agent() is None:
            raise RuntimeError("Could not boot ssh agent")
        subprocess.check_call(
            'ssh-add {}'.format(filepath),
            shell=True,
            executable='/bin/bash'
        )

    def __init__(self, hostname: str, hard_reset_on_orch_init: bool = True, **kwargs: typing.Any):
        """
        Initializes the backend.
        No connection is established until the context is entered.
        provided arguments and keyword arguments are passed to paramiko.SSHClient.Connect
        """
        super().__init__(hard_reset_on_orch_init=hard_reset_on_orch_init)
        self.hostname = hostname
        self.__hostname = hostname
        self.__sshkwargs = {
            **{
                'timeout': 30,
                'banner_timeout': 30,
                'auth_timeout': 60
            },
            **{
                key:val for key,val in kwargs.items()
                if key != 'type'
            }
        }
        self._force_rekey = True
        self.client = paramiko.SSHClient()
        self.client.load_system_host_keys()
        self.__transports = []

    def load_config_args(self):
        """
        Parses the ssh config file and sets up this client's ssh arguments
        as specified by the config file.
        Paramiko is too stupid to read ssh config so we have to write this function
        """
        config_path = os.path.expanduser('~/.ssh/config')
        config = paramiko.SSHConfig()
        if os.path.isfile(config_path):
            with open(config_path) as r:
                config.parse(r)
        config = config.lookup(self.hostname)
        if 'hostname' in config:
            self.hostname = config['hostname']
        if 'port' in config:
            self.__sshkwargs['port'] = config['port']
        if 'user' in config:
            self.__sshkwargs['username'] = config['user']
        if 'identityfile' in config:
            self.__sshkwargs['key_filename'] = os.path.expanduser(config['identityfile'][0])
        if 'userknownhostsfile' in config:
            self.client.load_host_keys(config['userknownhostsfile'])
        if 'hostkeyalias' in config:
            host_keys = self.client.get_host_keys()
            if config['hostkeyalias'] in host_keys:
                host_keys[self.hostname] = host_keys[config['hostkeyalias']]
            else:
                warnings.warn("Requested HostKeyAlias not found. Switching to auto-add policy", stacklevel=2)
                self.client.set_missing_host_key_policy(IgnoreKeyPolicy)
        if 'key_filename' in self.__sshkwargs and ('allow_agent' not in self.__sshkwargs or self.__sshkwargs['allow_agent']):
            try:
                RemoteSlurmBackend.add_key_to_agent(self.__sshkwargs['key_filename'])
            except:
                warnings.warn("Unable to add specified key file to ssh agent. Mojave users may be unable to authenticate")

    def _invoke(self, command: str, pty: typing.Optional[bool] = False) -> typing.Tuple[paramiko.ChannelFile, paramiko.ChannelFile, paramiko.ChannelFile]:
        """
        Raw handle to exec_command
        """
        if self.client._transport is None:
            raise paramiko.SSHException("Client is not connected")
        try:
            return self.client.exec_command(command, get_pty=pty)
        except paramiko.ssh_exception.SSHException as e:
            if e.args == ('Key-exchange timed out waiting for key negotiation',):
                canine_logging.print("Rekey timeout. Restarting client. Open transports may be interrupted")
                reinit = []
                for t in self.__transports:
                    if t.session is not None:
                        t.__exit__()
                        reinit.append(t)
                self.__exit__()
                self.__enter__()
                for t in reinit:
                    t.__enter__()
                return self.client.exec_command(command, get_pty=pty)
            else:
                raise

    def early_rekey(self):
        if self._force_rekey:
            packetizer = self.client.get_transport().packetizer
            bytes_count = packetizer._Packetizer__received_bytes / packetizer.REKEY_BYTES
            packets_count = packetizer._Packetizer__received_packets / packetizer.REKEY_PACKETS
            if packets_count >= 0.8 or bytes_count >= 0.8:
                reinit = []
                for t in self.__transports:
                    if t.session is not None:
                        t.__exit__()
                        reinit.append(t)
                self.__exit__()
                self.__enter__()
                for t in reinit:
                    t.__enter__()

    def invoke(self, command: str, interactive: bool = False, **kwargs) -> typing.Tuple[int, typing.BinaryIO, typing.BinaryIO]:
        """
        Invoke an arbitrary command in the slurm console
        Returns a tuple containing (exit status, byte stream of standard out from the command, byte stream of stderr from the command).
        If interactive is True, stdin, stdout, and stderr should all be connected live to the user's terminal.
        NOTE: For interactive commands, we recommend you prefix your command with 'stty -echo &&' to disable echoing your input on stdout.
        EX: backend.invoke('stty -echo && python', True) would invoke an interactive python session without your input also appearing in stdout
        """
        self.early_rekey()
        interactive = interactive and isatty(sys.stdout, sys.stdin)
        raw_stdin, raw_stdout, raw_stderr = self._invoke(command, pty=interactive)
        try:
            if interactive:
                return make_interactive(raw_stdout.channel)
            stdout = io.BytesIO(raw_stdout.read())
            stderr = io.BytesIO(raw_stderr.read())
            self.early_rekey()
            return raw_stdout.channel.recv_exit_status(), stdout, stderr
        except KeyboardInterrupt:
            canine_logging.warning("Warning: Command will continue running on remote server as Paramiko has no way to interrupt commands")
            raise

    def interactive_login(self) -> int:
        """
        Connects to the client interactively.
        Stdin/out/err will be connected directly, and
        Python will not have access to the streams
        Returns ssh's exit status
        """
        return subprocess.call(
            'ssh {}'.format(self.__hostname),
            shell=True,
            executable='/bin/bash'
        )

    def sbcast(self, localpath: str, remotepath: str, *slurmopts: str, **slurmparams: typing.Any):
        """
        Broadcasts the localpath (on the local filesystem)
        to all compute nodes at remotepath
        Additional arguments and keyword arguments provided are passed as slurm arguments to srun
        """
        command = 'sbcast {0} -- {1} {1}'.format(
            ArgumentHelper(*slurmopts, **slurmparams).commandline,
            remotepath
        )
        with self.transport() as transport:
            transport.send(localpath, remotepath)
            status, stdout, stderr = self.invoke(command)
            check_call(command, status, stdout, stderr)

    def disable_paramiko_rekey(self):
        """
        Disables the re-key feature of SSH2.
        Paramiko's implementation deadlocks during rekey (see paramiko #822).
        """
        warnings.warn(
            "User disabled paramiko rekey. An attacker may be able to read data in the SSH channel",
            stacklevel=2
        )
        __NEED_REKEY__ = self.client.get_transport().packetizer.need_rekey
        def need_rekey(*args, **kwargs):
            if __NEED_REKEY__(*args, **kwargs):
                warnings.warn(
                    "Supressing rekey request from paramiko to avoid deadlock. Current SSH channel should be considered insecure",
                    stacklevel=2
                )
                packetizer = self.client.get_transport().packetizer
                packetizer._Packetizer__need_rekey = False
                packetizer._Packetizer__received_bytes = 0
                packetizer._Packetizer__received_packets = 0
                packetizer._Packetizer__received_bytes_overflow = 0
                packetizer._Packetizer__received_packets_overflow = 0
            return False

        self.client.get_transport().packetizer.need_rekey = need_rekey
        self._force_rekey = False

    def __enter__(self):
        """
        Establishes a connection to the remote server
        """
        self.client.connect(self.hostname, **self.__sshkwargs)
        return self

    def __exit__(self, *args):
        """
        Closes the connection to the remote server
        """
        self.client.close()

    def transport(self) -> RemoteTransport:
        """
        Return a Transport object suitable for moving files between the
        SLURM cluster and the local filesystem
        """
        t = RemoteTransport(self.client)
        self.__transports.append(t)
        return t
