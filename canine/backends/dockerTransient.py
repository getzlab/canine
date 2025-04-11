# vim: set expandtab:

import typing
import subprocess
import os
import pwd
import sys
import docker
import re
import socket
import psutil
import io
import pickle
import math
import threading
import time
import shutil
import uuid

from .imageTransient import TransientImageSlurmBackend, list_instances, get_gce_client
from ..utils import get_default_gcp_zone, get_default_gcp_project, gcp_hourly_cost, isatty, canine_logging

from requests.exceptions import ConnectionError as RConnectionError, ReadTimeout
from urllib3.exceptions import ProtocolError

import pandas as pd

import threading

gce_lock = threading.Lock()

class DockerTransientImageSlurmBackend(TransientImageSlurmBackend): # {{{
    def __init__(
        self, cluster_name, *,
        action_on_stop = "delete",
        image_family = "slurm-gcp-docker-v2",
        image_project = "broad-getzlab-workflows",
        image = None,
        storage_namespace = "workspace", storage_bucket = None, storage_disk = None, storage_disk_size = "100",
        clust_frac = 1.0, user = None, shutdown_on_exit = False, **kwargs
    ):
        image_family = "n4-h-n1-ssd"
        image_project = "broad-tcga-wgs-thca-pran-6"
        if user is None:
            if "USER" in os.environ:
                user = os.environ["USER"]
            else:
                raise ValueError("$USER not set in environment. Must explicitly pass user argument")

        if storage_bucket is not None and storage_disk is not None:
            canine_logging.warning("You specified both a persistent disk and cloud bucket to store workflow outputs; will only store to bucket!")

        if "image" not in kwargs:
            kwargs["image"] = image

        super().__init__(**{**kwargs, **{ "slurm_conf_path" : "" }})

        self.config = {
          "cluster_name" : cluster_name,
          "worker_prefix" : socket.gethostname(),
          "action_on_stop" : action_on_stop,
          "image_family" : image_family,
          "image_project" : image_project,
          "clust_frac" : 1.0,
          "user" : user,
          "storage_namespace" : storage_namespace,
          "storage_bucket" : storage_bucket,
          "storage_disk" : storage_disk,
          "storage_disk_size" : storage_disk_size,
          "storage_uuid" : str(uuid.uuid4().hex[0:4]),
          **{ k : v for k, v in self.config.items() if k not in { "worker_prefix", "user", "action_on_stop" } }
        }
        self.config["image"] = self.get_latest_image(
          image_family = self.config["image_family"],
          project = self.config["image_project"],
        )["name"] if image is None else image

        # placeholder for Docker API
        self.dkr = None

        # placeholder for Docker container object
        self.container = None

        # flag to indicate whether the Docker was already running
        self.preexisting_container = False

        # flag to indicate whether we shutdown the container once the backend is exited
        self.shutdown_on_exit = shutdown_on_exit

        # placeholder for node list (loaded from lookup table)
        self.nodes = pd.DataFrame()

    def init_slurm(self):
        # query /etc/passwd for UID/GID information if we are running as a different user
        # FIXME: how should this work for OS Login/LDAP/etc.?
        uid = None; gid = None
        if self.config["user"] != os.environ["USER"]:
            uinfo = pwd.getpwnam(self.config["user"])
            uid = uinfo.pw_uid; gid = uinfo.pw_gid
        else:
            uid = os.getuid(); gid = os.getgid()

        # initialize Docker API
        self.dkr = docker.from_env()

        #
        # check if image exists
        try:
            image = self.dkr.images.get(f'gcr.io/{self.config["image_project"]}/slurm_gcp_docker_ssd:latest')
        except docker.errors.ImageNotFound:
            raise Exception("You have not yet built or pulled the Slurm Docker image!")
        except RConnectionError as e:
            if isinstance(e.args[0], ProtocolError):
                if isinstance(e.args[0].args[1], PermissionError):
                    raise PermissionError("You do not have permission to run Docker!")
                elif isinstance(e.args[0].args[1], ConnectionRefusedError):
                    raise ConnectionRefusedError("The Docker daemon does not appear to be running on this machine. Please start it.")
            raise Exception("Unknown problem connecting to the Docker daemon")
        except Exception as e:
            raise Exception("Problem starting Slurm Docker: {}: {}".format(
              type(e).__name__, e 
            ))

        #
        # create NFS/rclone bucket mountpoints
        self.create_filesystem(uid, gid)

        # copy credential files to NFS
        self.copy_cloud_credentials()

        #
        # ensure that Docker can start (no Slurm processes outside of Docker already running)
        try:
            ready_for_docker()
        except:
            canine_logging.error("Docker host is not ready to start container!")
            raise

        #
        # create the Slurm container if it's not already present
        canine_logging.info1("Starting Slurm controller ...")
        if self.config["cluster_name"] not in [x.name for x in self.dkr.containers.list()]:
            self.dkr.containers.run(
              image = image.tags[0], detach = True, network_mode = "host",
              mounts = [
                docker.types.Mount(
                  target = "/mnt", source = "/mnt", type = "bind", propagation = "rshared"
                ),
                docker.types.Mount(
                  target = "/dev", source = "/dev", type = "bind", propagation = "rshared"
                )
              ],
              name = self.config["cluster_name"], command = "/bin/bash",
              stdin_open = True, remove = True, privileged = True,
              environment = { "HOST_USER" : self.config["user"], "HOST_UID" : uid, "HOST_GID" : gid }
            )
            self.container = self._get_container(self.config["cluster_name"])

        # otherwise, try and start it if it's stopped
        else:
            self.preexisting_container = True
            self.container = self._get_container(self.config["cluster_name"])
            if self.container().status == "exited":
                self.container().start()

        # TODO: should we restart slurmctld in the container here?

        #
        # wait until the container is fully started, or error out if it failed
        # to start
        self.wait_for_container_to_be_ready(timeout = 60)

        #
        # initialize storage
        self.init_storage()

        #
        # save the configuration to disk so that Slurm knows how to configure
        # the nodes it creates
        subprocess.check_call("""
          [ ! -d /mnt/nfs/clust_conf/canine ] && mkdir -p /mnt/nfs/clust_conf/canine ||
            echo -n
          """, shell = True, executable = '/bin/bash')
        with open("/mnt/nfs/clust_conf/canine/backend_conf.pickle", "wb") as f:
            pickle.dump(self.config, f)

    def init_nodes(self):
        self.wait_for_cluster_ready(elastic = True, timeout=60)

        # list all the nodes that Slurm is aware of; this may be useful subsequently?
        #allnodes = pd.read_pickle("/mnt/nfs/clust_conf/slurm/host_LuT.pickle")

    def stop(self): 
        # remove any bucket mount commands created by this instance 
        if os.path.exists(f"/mnt/nfs/.rclone_mounts_{self.config['storage_uuid']}.sh"):
            os.remove(f"/mnt/nfs/.rclone_mounts_{self.config['storage_uuid']}.sh")

        # if the Docker was not spun up by this context manager, do not tear
        # anything down -- we don't want to clobber an already running cluster
        if self.shutdown_on_exit and not self.preexisting_container:
            # delete node configuration file
            try:
                subprocess.check_call(
                  "rm -f /mnt/nfs/clust_conf/canine/backend_conf.pickle",
                  shell = True,
                  timeout = 10
                )
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
                canine_logging.error("Couldn't delete node configuration file:")
                canine_logging.error(e)

            #
            # shutdown nodes that are still running (except NFS)
            allnodes = self.nodes

            # if we're aborting before the NFS has even been started, there are no
            # nodes to shutdown.
            if not allnodes.empty:
                # sometimes the Google API will spectacularly fail; in that case, we
                # just try to shutdown everything in the node list, regardless of whether
                # it exists.
                try:
                    extant_nodes = self.list_instances_all_zones()
                    self.nodes = allnodes.loc[allnodes.index.isin(extant_nodes["name"]) &
                                   (allnodes["machine_type"] != "nfs")]
                except:
                    self.nodes = allnodes.loc[allnodes["machine_type"] != "nfs"]

                # superclass method will stop/delete/leave these running, depending on how
                # self.config["action_on_stop"] is set
                super().stop()

            #
            # stop the Docker

            # this needs to happen after super().stop() is invoked, since that
            # calls scancel, which in turn requires a running Slurm controller Docker
            if self.container is not None:
                self.container().stop()

    def _get_container(self, container_name):
        def closure():
            backoff_factor = 1
            while True:
                try:
                    container = self.dkr.containers.get(container_name)
                    break
                except ReadTimeout:
                    canine_logging.warning(f"Request to controller Docker timed out; retrying in {int(10*backoff_factor)} seconds ...")
                    time.sleep(10*backoff_factor)
                    backoff_factor *= 1.1
            return container

        return closure

    def reenter(self):
        # docker object cannot be reused between processes due to socket connections
        self.dkr = docker.from_env()

    def create_filesystem(self, uid, gid):
        ## create main NFS mountpoint
        if not os.path.exists("/mnt/nfs"):
            try:
                subprocess.check_call("sudo mkdir /mnt/nfs", shell=True)
                subprocess.check_call(f"sudo chown {uid}:{gid} /mnt/nfs", shell=True)
            except:
                # TODO: be more specific about exception catching
                canine_logging.error("Could not create NFS mountpoint; see stack trace for details")
                raise

        ## create root mountpoint for rclone bucket filesystems (will be bind mounted
        #  into main mountpoint; for some reason, these cannot be nested in /mnt/nfs)
        if not os.path.exists("/mnt/rclone"):
            try:
                subprocess.check_call("sudo mkdir /mnt/rclone", shell=True)
                subprocess.check_call(f"sudo chown {uid}:{gid} /mnt/rclone", shell=True)
            except:
                # TODO: be more specific about exception catching
                canine_logging.error("Could not create rclone; see stack trace for details")
                raise

    def init_storage(self):
        ## make /mnt/nfs NFS its own virtual filesystem
        # this is so that Canine's system for detecting whether files to be
        # localized won't symlink things that reside outside /mnt/nfs but on the same
        # actual filesystem as /mnt/nfs
        subprocess.check_call("""[ $(df -P /mnt/nfs/ | awk 'NR > 1 { print $6 }') == '/mnt/nfs' ] || \
          sudo mount --bind /mnt/nfs /mnt/nfs""", shell=True, executable="/bin/bash")

        ## mount bucket via rclone (unstable!)
        if self.config["storage_bucket"] is not None:
            canine_logging.info1(f"Saving workflow results to bucket {self.config['storage_bucket']} mounted at /mnt/nfs/{self.config['storage_namespace']} ...")

            # TODO: check if bucket exists; create it if not

            # generate cache disk if it doesn't exist
            rc, stdout, stderr = self.invoke(
              "gcloud_make_rwdisk {disk_name} {disk_size} {mount_prefix} false {node_name} {node_zone}".format(
                disk_name = f'cache-disk-{self.config["worker_prefix"]}',
                disk_size = 100,
                mount_prefix = "/tmp/rclone_cache",
                node_name = self.config["worker_prefix"],
                node_zone = get_default_gcp_zone()
              )
            )
            if rc != 0:
                canine_logging.error(f"Could not generate bucket mountpoint; see error log for details:")
                canine_logging.error(stderr.read().decode())
                raise RuntimeError()

            # invoke rclone inside docker to create bucket-backed FUSE filesystem
            # this is complicated, because NFS cannot export a nested FUSE filesystem.
            # we thus mount it outside of the NFS to /mnt/rclone/<bucket> (which will be exported to workers),
            # and bind mount /mnt/rclone/<bucket> to /mnt/nfs/<bucket> (in order to access it on the controller)
            # workers will NFS mount /mnt/rclone/<bucket> to /mnt/nfs/<bucket> for consistent paths.
            rc, stdout, stderr = self.invoke(
              """bash -c \
                'set -e; export GOOGLE_APPLICATION_CREDENTIALS=$CLOUDSDK_CONFIG/application_default_credentials.json; \
                 [ ! -d {mountpoint} ] && mkdir {mountpoint}; \
                 [ ! -d {bind_mountpoint} ] && mkdir {bind_mountpoint}; \
                 df -t fuse.rclone {mountpoint} || \
                  rclone mount gcs:{bucket_name} {mountpoint} --daemon --links \
                   --uid $HOST_UID --gid $HOST_GID --file-perms 0755 --allow-other \
                   --vfs-cache-mode full --cache-dir /tmp/rclone_cache/ --vfs-cache-max-size 95Gi \
                   --vfs-write-back 10m --vfs-cache-poll-interval 10m --vfs-fast-fingerprint \
                   --vfs-cache-max-age 48h --dir-cache-time 2h --poll-interval 10m --no-modtime \
                   --config /sgcpd/conf/rclone.conf; \
                 df -t fuse.rclone {bind_mountpoint} || \
                  mount --bind {mountpoint} {bind_mountpoint}'""".format(
                bucket_name = self.config["storage_bucket"][5:] if self.config["storage_bucket"].startswith("gs://") else self.config["storage_bucket"],
                mountpoint = f'/mnt/rclone/{self.config["storage_namespace"]}',
                bind_mountpoint = f'/mnt/nfs/{self.config["storage_namespace"]}'
              ),
              user = "root"
            )
            if rc != 0:
                canine_logging.error(f"Could not mount bucket; see error log for details:")
                canine_logging.error(stderr.read().decode())
                raise RuntimeError()

            # export mount
            subprocess.check_call(f"sudo exportfs -o fsid=1,rw,async,no_subtree_check,insecure,no_root_squash *.internal:/mnt/rclone/{self.config['storage_namespace']}", shell=True)

            # save rclone mountpoint list (worker nodes will subsequently read this in to know what directories to mount after starting up)
            # this file will be erased when backend is torn down
            # TODO: use flock to remove file if backend crashes; when starting backend, check for unlocked files and remove them
            # will need try/except on worker nodes to avoid them freezing up if they inadvertently attempt to mount non-exported buckets
            with open(f"/mnt/nfs/.rclone_mounts_{self.config['storage_uuid']}.sh", "w") as f:
                f.write("if ! mountpoint -q /mnt/nfs/{mount_dir}; then sudo timeout -k 30 30 mount -o defaults,hard,intr ${{CONTROLLER_NAME}}:/mnt/rclone/{mount_dir} /mnt/nfs/{mount_dir} || echo 'Could not mount rclone mountpoint /mnt/rclone/{mount_dir}'; fi".format(mount_dir = self.config['storage_namespace']))

        ## create disk
        # note that bucket takes priority if both are specified
        elif self.config["storage_disk"] is not None:
            # GCP disks must match regex '^[a-z]([a-z0-9-]*[a-z0-9])?'; raise error if not
            if re.match('^[a-z]([a-z0-9-]*[a-z0-9])?$', self.config["storage_disk"]) is None:
                raise ValueError(f"\"{self.config['storage_disk']}\" is an invalid storage namespace. Storage namespaces can only contain lowercase letters, numbers, and dashes, must start with a letter, and must end with a letter/number.")

            canine_logging.info1(f"Saving workflow results to persistent disk {self.config['storage_disk']} ({self.config['storage_disk_size']}GB) mounted at /mnt/nfs/{self.config['storage_namespace']} ...")
            # use procedure to create RW disk from localization, inside docker
            rc, stdout, stderr = self.invoke(
              "gcloud_make_rwdisk {disk_name} {disk_size} {mount_prefix} false {node_name} {node_zone}".format(
                disk_name = self.config["storage_disk"],
                disk_size = f"{self.config['storage_disk_size']}",
                mount_prefix = f"/mnt/nfs/{self.config['storage_namespace']}",
                node_name = self.config["worker_prefix"],
                node_zone = get_default_gcp_zone()
              )
            )
            if rc != 0:
                canine_logging.error("Error attaching workflow results disk; see error log for details:")
                canine_logging.error(stderr.read().decode())
                raise RuntimeError()

        ## Check disk usage and warn user if it is small
        free_space_gb = int(shutil.disk_usage(f"/mnt/nfs/{self.config['storage_namespace']}").free/(1024**3))
        if free_space_gb < 300:
            canine_logging.warning(
                f"Workflow results disk low on space ({free_space_gb} GB remaining)"
            )

        # TODO: add warnings if overall disk is small (bad disk IO) or node core
        #       count is low (bad network IO)

        ## export NFS
        subprocess.check_call("sudo exportfs -o fsid=0,rw,async,no_subtree_check,insecure,no_root_squash,crossmnt *.internal:/mnt/nfs", shell=True)

    def copy_cloud_credentials(self):
        ## gcloud
        # TODO: check that we are properly authenticated
        # TODO: check $CLOUDSDK_CONFIG environment variable
        gcloud_conf_dir = subprocess.check_output("echo -n ~/.config/gcloud", shell = True).decode()
        if os.path.isdir(gcloud_conf_dir):
            if not os.path.isdir("/mnt/nfs/credentials/gcloud"):
                os.makedirs("/mnt/nfs/credentials/gcloud")
            subprocess.run(f'cp -rf $(find {gcloud_conf_dir} -mindepth 1 -maxdepth 1 ! -name "logs") /mnt/nfs/credentials/gcloud', shell = True)

    def get_latest_image(self, image_family = None, project = None):
        image_family = self.config["image_family"] if image_family is None else image_family
        project = self.config["project"] if project is None else project
        with gce_lock:
            ans = get_gce_client().images().getFromFamily(family = image_family, project = project).execute()
        return ans

    def invoke(self, command, interactive = False, bypass_docker = False, user = None):
        """
        Set bypass_docker to True to execute the command directly on the host,
        rather than in the controller container. Useful for debugging.
        """
        if not isatty(sys.stdout, sys.stdin):
            interactive = False

        user = self.config["user"] if user is None else user

        # re-purpose LocalSlurmBackend's invoke
        local_invoke = super(TransientImageSlurmBackend, self).invoke
        if self.container is not None and self.container().status == "running":
            if not bypass_docker:
                cmd = "docker exec --user {user} {ti_flag} {container} {command}".format(
                  user = user,
                  ti_flag = "-ti" if interactive else "",
                  container = self.config["cluster_name"],
                  command = command
                )
                # if command fails for a recoverable reason, retry up to 7 times
                # with exponential backoff (max ~2 minute wait)
                timeout = 8
                tries = 0
                while tries < 7:
                    ret, stdout, stderr = local_invoke(cmd, interactive)
                    stderr_str = stderr.read().decode().rstrip()
                    stderr.seek(0)

                    # no stderr; assume command ran successfully
                    if stderr_str == "":
                        break

                    # stderr corresponds to a known Docker failure mode than can
                    # be recovered from
                    if any([reason in stderr_str for reason in [
                      "Error response from daemon: No such exec instance",
                      "OCI runtime exec failed: exec failed",
                      "Unable to contact slurm controller (connect failure)",
                      "Socket timed out on send/recv operation"
                    ]]):
                        canine_logging.warning(
                          'Command {cmd} failed with known recoverable error reason "{err}"; retrying in {timeout} seconds up to {tries} more times'.format(
                            cmd = command,
                            err = stderr_str,
                            timeout = timeout,
                            tries = 7 - tries
                          )
                        )
                        time.sleep(timeout)
                        timeout *= 2
                        tries += 1

                    # warn the user that the command had stuff written to stderr,
                    # since this may indicate something is wrong
                    else:
                        canine_logging.debug(
                          'Command {cmd} returned stderr "{err}"'.format(
                            cmd = command,
                            err = stderr_str
                          )
                        )
                        break
            else:
                cmd = command
                ret, stdout, stderr = local_invoke(cmd, interactive)
            return (ret, stdout, stderr)
        else:
            return (1, io.BytesIO(), io.BytesIO(b"Container is not running!"))

    def wait_for_container_to_be_ready(self, timeout = 3000):
        canine_logging.info1("Waiting up to {} seconds for Slurm controller to start ...".format(timeout))
        (rc, _, _) = self.invoke(
          "timeout {} bash -c 'while [ ! -f /.started ]; do sleep 1; done'".format(timeout),
          interactive = True,
          user = "root"
        )
        if rc == 124:
            raise TimeoutError("Slurm controller did not start within {} seconds!".format(timeout))
        canine_logging.info1("Started Slurm controller.")

# }}}

class LocalDockerSlurmBackend(DockerTransientImageSlurmBackend):
    def __enter__(self):
        self.dkr = docker.from_env()
        self.container = self._get_container(self.config["cluster_name"])
        return self
    def __exit__(self, *args):
        pass

# Python version of checks in docker_run.sh
def ready_for_docker():
    #
    # check if Slurm/mysql/Munge are already running
    already_running = [["slurmctld", "A Slurm controller"],
                       ["slurmdbd", "The Slurm database daemon"],
                       ["mysqld", "mysql"],
                       ["munged", "Munge"]]

    all_procs = { x.name() : x.pid for x in psutil.process_iter() }

    for proc, desc in already_running:
        # is the process is running at all?
        if proc in all_procs:
            # iterate through parents to see if it was launched in a Docker (containerd)
            for parent_proc in psutil.Process(all_procs[proc]).parents():
                if parent_proc.name().startswith("containerd"):
                    break
                if parent_proc.pid == 1:
                    raise Exception("{desc} is already running on this machine (outside of a Docker container). Please run `[sudo] killall {proc}' and try again.".format(desc = desc, proc = proc))
