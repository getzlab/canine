import typing
import os
import io
import sys
import subprocess
import tempfile
import time
import shutil
from ..base import AbstractSlurmBackend
from ..local import LocalSlurmBackend
from ..remote import IgnoreKeyPolicy, RemoteTransport, RemoteSlurmBackend
from ...utils import ArgumentHelper, check_call
import docker
import paramiko
import port_for
from agutil.parallel import parallelize, parallelize2

class DummyTransport(RemoteTransport):
    """
    Handles filesystem interaction with the NFS container for the Dummy Backend.
    Uses SFTP to interact with the docker filesystem
    """

    def __init__(self, mount_path: str, container: docker.models.containers.Container, port: int):
        """
        In the dummy backend, the mount_path is mounted within all containers at /mnt/nfs.
        This transport simulates interacting with files in the containers by
        connecting over SFTP to the controller
        """
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
            os.makedirs(os.path.dirname(self.ssh_key_path), exist_ok=True)
            subprocess.check_call('ssh-keygen -q -b 2048 -t rsa -f {} -N ""'.format(self.ssh_key_path), shell=True)
            subprocess.check_call('docker exec {} mkdir -p -m 600 /root/.ssh/'.format(
                self.container.short_id
            ), shell=True)
            subprocess.check_call('docker cp {}.pub {}:/root/tmpkey'.format(
                self.ssh_key_path,
                self.container.short_id
            ), shell=True)
            subprocess.check_call('docker exec {} bash -c "cat /root/tmpkey >> /root/.ssh/authorized_keys"'.format(
                self.container.short_id
            ), shell=True)
            subprocess.check_call('docker exec {} chown root:root /root/.ssh/authorized_keys'.format(
                self.container.short_id
            ), shell=True)
            RemoteSlurmBackend.add_key_to_agent(self.ssh_key_path)
        self.client = paramiko.SSHClient()
        self.client.load_system_host_keys()
        self.client.set_missing_host_key_policy(IgnoreKeyPolicy)
        if os.path.exists('/.dockerenv'):
            # We are inside a container
            self.container.reload()
            if 'NetworkSettings' in self.container.attrs and 'Networks' in self.container.attrs['NetworkSettings'] and len(self.container.attrs['NetworkSettings']['Networks']) == 1:
                self.client.connect(
                    [net['IPAddress'] for net in self.container.attrs['NetworkSettings']['Networks'].values()][0],
                    key_filename=self.ssh_key_path,
                    username='root',
                    timeout=10
                )
            else:
                raise RuntimeError(
                    "Canine is running inside a docker but was unable to determine network settings of peer docker"
                )
        else:
            self.client.connect(
                'localhost',
                port=self.port,
                key_filename=self.ssh_key_path,
                username='root'
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

class ManualBind(object):
    """
    Mimics the tempfile.TemporaryDirectory API for a predefined directory
    """
    def __init__(self, path: str):
        """
        Creates the given dirpath if it does not exist already
        """
        self.name = os.path.abspath(path)
        self._cleanup = False
        if not os.path.exists(self.name):
            os.makedirs(self.name)
            self._cleanup = True

    def __enter__(self):
        """
        Returns the name
        """
        return self.name

    def cleanup(self):
        """
        Removes the directory, if it was created at start.
        If the directory already existed, it is left intact
        """
        if self._cleanup and os.path.exists(self.name):
            shutil.rmtree(self.name)
            self._cleanup = False

    def __exit__(self, *args):
        """
        Calls .cleanup()
        """
        self.cleanup()

class DummySlurmBackend(AbstractSlurmBackend):
    """
    Operates a SLURM cluster locally, using docker containers.
    Only docker is required (the cluster will use local compute resources).
    Useful for unittesting or for running Canine on a single, powerful compute node.
    """

    @staticmethod
    @parallelize2()
    def exec_run(container: docker.models.containers.Container, command: str, **kwargs) -> typing.Callable[[], docker.models.containers.ExecResult]:
        """
        Invoke the given command within the given container.
        Returns a callback object
        """
        return container.exec_run(command, **kwargs)

    @staticmethod
    @parallelize()
    def stop_containers(containers: typing.List[docker.models.containers.Container]):
        """
        Stops all given containers in parallel
        """
        containers.stop()
        return containers

    def __init__(
        self, n_workers: int, network: str = 'canine_dummy_slurm',
        cpus: typing.Optional[int] = None, memory: typing.Optional[int] = None,
        compute_script: str = "", controller_script: str = "",
        image: str = "gcr.io/broad-cga-aarong-gtex/slurmind", staging_dir: typing.Optional[str] = None, **kwargs
    ):
        """
        Saves configuration.
        No containers are started until the backend is __enter__'ed
        Memory is in GiB.
        If staging dir is provided, th
        """
        if int(n_workers) < 1:
            raise ValueError("n_workers must be at least 1 worker")
        super().__init__(**kwargs)
        self.n_workers = int(n_workers)
        self.network = network
        if '-' in self.network:
            raise ValueError("Network name cannot contain '-'")
        self.cpus = cpus
        self.mem = memory
        self.compute_script = compute_script
        self.controller_script = controller_script
        self.image = image
        self.staging_dir = staging_dir
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
        if self.staging_dir is None:
            self.bind_path = tempfile.TemporaryDirectory(dir=os.path.expanduser('~'))
        else:
            self.bind_path = ManualBind(self.staging_dir)
        self.dkr = docker.from_env()
        try:
            # Check that the chosen network exists
            self.dkr.networks.get(self.network)
        except docker.errors.NotFound:
            # Network does not exist; create it
            print("Creating bridge network", self.network)
            self.dkr.networks.create(
                self.network,
                driver='bridge'
            )
        try:
            self.dkr.images.get(self.image)
        except docker.errors.NotFound:
            print("Pulling image", self.image)
            self.dkr.images.pull(self.image)
        if not os.path.isdir(os.path.expanduser('~/.config/gcloud')):
            os.makedirs(os.path.expanduser('~/.config/gcloud'))
        try:
            self.controller = self.dkr.containers.run(
                self.image,
                '/controller.py {network} {workers} {cpus} {mem}'.format(
                    network=self.network,
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
                        'bind': '/mnt/nfs', 'mode': 'rw',
                    },
                    '/var/run/docker.sock': {
                        'bind': '/var/run/docker.sock', 'mode': 'rw'
                    },
                    os.path.expanduser('~/.config/gcloud'): {
                        'bind': '/root/.config/gcloud', 'mode': 'rw'
                    },
                },
                ports={'22/tcp': self.port}
            )
        except docker.errors.APIError as e:
            if sys.platform == 'darwin':
                raise RuntimeError("Check your docker preferences and ensure that {} is bindable".format(self.bind_path.name)) from e
            raise
        print("Slurm controller started in", self.controller.short_id)
        print("Waiting for containers to start...")
        proc = subprocess.Popen(
            'docker logs -f {}'.format(self.controller.short_id),
            shell=True
        ) # let the user follow the startup logs
        time.sleep(5)
        self.controller.reload()
        with self.transport() as transport:
            while self.controller.status in {'running', 'created'} and not transport.exists("/mnt/nfs/controller.ready"):
                time.sleep(5)
                self.controller.reload()
        proc.terminate()
        self.workers = [
            container for container in self.dkr.containers.list(
                filters={
                    'network': self.network,
                    'since': self.controller.id
                }
            )
            # 'since' kwarg is inclusive, so the controller shows up in this list
            if container.id != self.controller.id
        ]
        if len(self.workers) != self.n_workers:
            raise RuntimeError("Number of worker containers ({}) does not match expected count ({})".format(len(self.workers), self.n_workers))
        self.startup_callbacks = []
        if len(self.controller_script.strip()):
            self.startup_callbacks.append(DummySlurmBackend.exec_run(self.controller, 'bash -c \'{}\''.format(self.controller_script), stderr=True, demux=True))
        if len(self.compute_script.strip()):
            self.startup_callbacks += [
                DummySlurmBackend.exec_run(worker, 'bash -c \'{}\''.format(self.compute_script), stderr=True, demux=True)
                for worker in self.workers
            ]
        return self

    def wait_for_cluster_ready(self, elastic: bool = False):
        """
        Blocks until the main partition is marked as up
        """
        # Ensure that all user-provided startup scripts are called
        for callback in self.startup_callbacks:
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
                self,
                'docker exec -it {} {}'.format(self.controller.short_id, command),
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
        print("Cleaning up Slurm Cluster", self.controller.short_id)
        for container in DummySlurmBackend.stop_containers(self.workers + [self.controller]):
            print("Stopped", container.short_id)
        self.bind_path.cleanup()
        self.bind_path = None
        self.port = None

    def transport(self) -> DummyTransport:
        """
        Return a Transport object suitable for moving files between the
        SLURM cluster and the local filesystem
        """
        return DummyTransport(self.bind_path.name, self.controller, self.port)
