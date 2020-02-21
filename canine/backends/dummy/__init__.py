import typing
import os
import io
import sys
import subprocess
import tempfile
import time
from ..base import AbstractSlurmBackend
from ..local import LocalSlurmBackend
from ..remote import IgnoreKeyPolicy, RemoteTransport, RemoteSlurmBackend
from ...utils import ArgumentHelper, check_call
import docker
import paramiko
import port_for
from agutil.parallel import parallelize2

class DummyTransport(RemoteTransport):
    """
    Handles filesystem interaction with the NFS container for the Dummy Backend.
    Files existing within the mount_path are reached via the bind path on the local file system.
    Files outside the mount_path are reached via SFTP to the controller
    """

    def __init__(self, mount_path: str , container: docker.models.containers.Container, port: int):
        """
        In the dummy backend, the mount_path is mounted within all containers at /mnt/nfs.
        This transport simulates interacting with files in the containers by interacting
        with the local copies at mount_path. The container is used for some actions, like symlinking objects
        """
        self.mount_path = mount_path
        self.ssh_key_path = os.path.join(mount_path, '.ssh', 'id_rsa')
        self.container = container
        self.port = port
        super().__init__(None)

    def __enter__(self):
        """
        Allows the Transport to function as a context manager
        Opens a paramiko SFTP connection to the host container
        """
        if not os.path.exists(self.ssh_key_path):
            os.makedirs(os.path.dirname(self.ssh_key_path), exist_okay=True)
            subprocess.check_call('ssh-keygen -q -b 2048 -t rsa -f {} -N ""'.format(self.ssh_key_path), shell=True)
            subprocess.check_call('docker cp {}.pub {}:/root/.ssh/authorized_keys'.format(
                self.ssh_key_path,
                self.container.short_id
            ))
            RemoteSlurmBackend.add_key_to_agent(self.ssh_key_path)
        self.client = paramiko.SSHClient()
        self.client.load_system_host_keys()
        self.client.connect(
            'localhost',
            port=self.port,
            key_filename=self.ssh_key_path
        )

        # Disable re-keying. This is a local-only ssh connection
        __NEED_REKEY__ = self.client.get_transport().packetizer.need_rekey
        def need_rekey(*args, **kwargs):
            if __NEED_REKEY__(*args, **kwargs):
                packetizer = self.client.get_transport().packetizer
                packetizer._Packetizer__need_rekey = False
                packetizer._Packetizer__received_bytes = 0
                packetizer._Packetizer__received_packets = 0
                packetizer._Packetizer__received_bytes_overflow = 0
                packetizer._Packetizer__received_packets_overflow = 0
            return False
        self.client.get_transport().packetizer.need_rekey = need_rekey
        return super().__enter__()

    def __exit__(self, *args):
        """
        Allows the Transport to function as a context manager
        Closes the underlying SFTP connection
        """
        super().__exit__()
        self.client = None

    def get_mount_filename(self, filename: str) -> typing.Optional[str]:
        """
        Gets the externally visible filepath given a filename that is valid within the container.
        Returns None if the filename cannot be accessed via the bind mount (indicates that the transport must use SFTP)
        """
        if filename.startswith('/mnt/nfs/'):
            return filename.replace('/mnt/nfs/', self.mount_path, 1)

    def open(self, filename: str, mode: str = 'r', bufsize: int = -1) -> typing.IO:
        """
        Returns a File-Like object open on the slurm cluster
        """
        path = self.get_mount_filename(filename)
        if path is not None:
            return open(path, mode=mode, buffering=bufsize)
        return super().open(filename, mode, bufsize)

    def listdir(self, path: str) -> typing.List[str]:
        """
        Lists the contents of the requested path
        """
        canon_path = self.get_mount_filename(path)
        if canon_path is not None:
            return os.listdir(canon_path)
        return super().listdir(path)

    def mkdir(self, path: str):
        """
        Creates the requested directory
        """
        canon_path = self.get_mount_filename(path)
        if canon_path is not None:
            return os.mkdir(canon_path)
        return super().mkdir(path)

    def stat(self, path: str) -> typing.Any:
        """
        Returns stat information
        """
        canon_path = self.get_mount_filename(path)
        if canon_path is not None:
            return os.stat(canon_path)
        return super().stat(path)

    def chmod(self, path: str, mode: int):
        """
        Change file permissions
        """
        canon_path = self.get_mount_filename(path)
        if canon_path is not None:
            return os.chmod(canon_path, mode)
        return super().chmod(path, mode)

    def remove(self, path: str):
        """
        Removes the file at the given path
        """
        canon_path = self.get_mount_filename(path)
        if canon_path is not None:
            return os.remove(canon_path)
        return super().remove(path)

    def rmdir(self, path: str):
        """
        Removes the directory at the given path
        """
        canon_path = self.get_mount_filename(path)
        if canon_path is not None:
            return os.rmdir(canon_path)
        return super().rmdir(path)

    def rename(self, src: str, dest: str):
        """
        Move the file or folder 'src' to 'dest'
        Will overwrite dest if it exists
        """
        canon_src = self.get_mount_filename(src)
        canon_dest = self.get_mount_filename(dest)
        if canon_src is not None and canon_dest is not None:
            return os.rename(canon_src, canon_dest)
        return super().rename(src, dest)

class DummySlurmBackend(AbstractSlurmBackend):
    """
    Operates a SLURM cluster locally, using docker containers.
    Only docker is required (the cluster will use local compute resources).
    Useful for unittesting or for running Canine on a single, powerful compute node.
    """

    @parallelize2()
    @staticmethod
    def exec_run(container: docker.models.containers.Container, command: str, **kwargs) -> typing.Callable[[], docker.models.containers.ExecResult]:
        """
        Invoke the given command within the given container.
        Returns a callback object
        """
        return container.exec_run(command, **kwargs)

    def __init__(self, n_workers: int, network: str = None, cpus: typing.Optional[int] = None, memory: typing.Optional[int] = None, compute_script: str = "", controller_script: str = "", image: str = "gcr.io/broad-cga-aarong-gtex/slurmind", **kwargs):
        """
        Saves configuration.
        No containers are started until the backend is __enter__'ed
        """
        super().__init__(**kwargs)
        self.n_workers = n_workers
        self.network = Networks
        self.cpus = cpus
        self.mem = memory
        self.compute_script = compute_script
        self.controller_script = controller_script
        self.image = image
        self.bind_path = None
        self.dkr = None
        self.controller = None
        self.workers = []
        self.port = None
        self.startup_callbacks = []

    def __enter__(self):
        """
        Activates the cluster.
        Pulls the image and starts the controller container.
        Controller starts all necessary workers and fills slurm config
        """
        self.port = port_for.select_random()
        self.bind_path = tempfile.TemporaryDirectory()
        self.dkr = docker.from_env()
        self.dkr.images.pull(self.image)
        self.controller = self.dkr.containers.run(
            self.image,
            '/controller.py {network} {mount} {workers} {cpus} {mem}'.format(
                network=self.network,
                mount=self.bind_path.name,
                workers=self.n_workers,
                cpus='--cpus {}'.format(self.cpus) if self.cpus is not None else '',
                mem='--memory {}'.format(self.mem) if self.mem is not None else ''
            ),
            auto_remove=True,
            detach=True,
            # --interactive?
            tty=True,
            network=self.network,
            volumes={
                self.bind_path.name: {
                    'bind': '/mnt/nfs', 'mode': 'rw'
                }
            },
            ports={'22/tcp': self.port}
        )
        print("Slurm controller started in", self.controller.short_id)
        print("Waiting for containers to start...")
        proc = subprocess.Popen(
            'docker logs -f {}'.format(self.controller.short_id),
            shell=True
        ) # let the user follow the startup logs
        self.controller.reload()
        with self.transport() as transport:
            while self.controller.status in {'running', 'created'} and not transport.exists("/mnt/nfs/controller.ready"):
                time.sleep(5)
                self.controller.reload()
        proc.terminate()
        self.workers = self.dkr.containers.list(
            since=self.controller.short_id,
            filters={'network': self.network}
        )
        if len(self.workers) != self.n_workers:
            raise RuntimeError("Number of worker containers ({}) does not match expected count ({})".format(len(self.workers), self.n_workers))
        self.controller.exec_run()
        self.startup_callbacks = []
        if len(self.controller_script.strip()):
            self.startup_callbacks.append(DummySlurmBackend.exec_run(self.controller, 'bash -c \'{}\''.format(self.controller_script), stderr=True, demux=True))
        if len(self.compute_script.strip()):
            self.startup_callbacks += [
                DummySlurmBackend.exec_run(worker, 'bash -c \'{}\''.format(self.compute_script), stderr=True, demux=True)
                for worker in self.workers
            ]

    def wait_for_cluster_ready(self, elastic: bool = False):
        """
        Blocks until the main partition is marked as up
        """
        # Ensure that all user-provided startup scripts are called
        for callback in self.startup_callbacks():
            callback()
        return super().wait_for_cluster_ready(elastic)


    def invoke(self, command: str, interactive: bool = False) -> typing.Tuple[int, typing.BinaryIO, typing.BinaryIO]:
        """
        Invoke an arbitrary command in the slurm console
        Returns a tuple containing (exit status, byte stream of standard out from the command, byte stream of stderr from the command).
        If interactive is True, stdin, stdout, and stderr should all be connected live to the user's terminal
        """
        if interactive:
            # Interactive commands are kind of shit using the docker API, so we outsource them
            return LocalSlurmBackend.invoke(
                'docker exec -it {}'.format(command),
                interactive=True
            )
        result = DummySlurmBackend.exec_run(
            self.controller,
            command,
            demux=True
        )() # wait for callback here
        return (
            result.exit_code,
            io.BytesIO(result.output[0] if result.output[0] is not None else b''),
            io.BytesIO(result.output[1] if result.output[1] is not None else b'')
        )

    def __exit__(self, *args):
        """
        Kills all running containers
        """
        for worker in self.workers:
            worker.stop()
        self.controller.stop()
        self.bind_path.cleanup()
        self.bind_path = None
        self.port = None

    def transport(self) -> DummyTransport:
        """
        Return a Transport object suitable for moving files between the
        SLURM cluster and the local filesystem
        """
        return DummyTransport(self.bind_path.name, self.controller, self.port)
