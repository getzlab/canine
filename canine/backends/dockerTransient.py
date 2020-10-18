# vim: set expandtab:

import typing
import subprocess
import os
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

from .imageTransient import TransientImageSlurmBackend, list_instances, gce
from ..utils import get_default_gcp_project, gcp_hourly_cost, isatty

from requests.exceptions import ConnectionError as RConnectionError
from urllib3.exceptions import ProtocolError

import pandas as pd

from threading import Lock

gce_lock = Lock()

class DockerTransientImageSlurmBackend(TransientImageSlurmBackend): # {{{
    def __init__(
        self, cluster_name, *,
        nfs_startup_script = "/usr/local/share/slurm_gcp_docker/src/provision_storage_container_host.sh",
        startup_script = "/usr/local/share/slurm_gcp_docker/src/provision_worker_container_host.sh",
        shutdown_script = "/usr/local/share/slurm_gcp_docker/src/shutdown_worker_container_host.sh",
        nfs_shutdown_script = "/usr/local/share/slurm_gcp_docker/src/shutdown_worker_container_host.sh",
        nfs_disk_size = 2000, nfs_disk_type = "pd-standard", nfs_action_on_stop = "stop", nfs_image = "",
        nfs_image_project = "", action_on_stop = "delete", image_family = None, image = None,
        clust_frac = 0.01, user = os.environ["USER"], **kwargs
    ):
        if user is None:
            # IE: USER was not set
            raise ValueError("USER not set in environment. Must explicitly pass user argument")

        if "image" not in kwargs:
            kwargs["image"] = image

        # superclass constructor does something special with startup|shutdown_script so
        # we need to pass it in
        kwargs["startup_script"] = "{script} {worker_prefix}".format(
          script = startup_script,
          worker_prefix = socket.gethostname()
        )
        kwargs["shutdown_script"] = shutdown_script
        super().__init__(**{**kwargs, **{ "slurm_conf_path" : "" }})

        # handle NFS startup/shutdown scripts
        nfs_compute_script = {
          "startup-script" : "{script} {nfsds:d} {nfsdt} {nfsimg} {nfsproj}".format(
            script = nfs_startup_script,
            nfsds = nfs_disk_size,
            nfsdt = nfs_disk_type,
            nfsimg = nfs_image,
            nfsproj = nfs_image_project
          ),
          "shutdown-script" : nfs_shutdown_script
        }

        self.config = {
          "cluster_name" : cluster_name,
          "worker_prefix" : socket.gethostname(),
          "nfs_compute_script" :
            "--metadata " + \
            ",".join([ "{}=\"{}\"".format(k, v) for k, v in nfs_compute_script.items() if v is not None ]),
          "action_on_stop" : action_on_stop,
          "nfs_action_on_stop" : nfs_action_on_stop if nfs_action_on_stop is not None
            else self.config["action_on_stop"],
          "image_family" : image_family if image_family is not None else "slurm-gcp-docker-" + user,
          "clust_frac" : max(min(clust_frac, 1.0), 1e-6),
          "user" : user,
          **{ k : v for k, v in self.config.items() if k not in { "worker_prefix", "user", "action_on_stop" } }
        }
        self.config["image"] = self.get_latest_image(self.config["image_family"])["name"] if image is None else image

        # placeholder for Docker API
        self.dkr = None

        # placeholder for Docker container object
        self.container = None

        # flag to indicate whether the Docker was already running
        self.preexisting_container = False

        # placeholder for node list (loaded from lookup table)
        self.nodes = pd.DataFrame()

        self.NFS_server_ready = False
        self.NFS_ready = False
        self.NFS_monitor_thread = None
        self.NFS_monitor_lock = None

    def init_slurm(self):
        self.dkr = docker.from_env()

        #
        # check if image exists
        try:
            image = self.dkr.images.get('broadinstitute/slurm_gcp_docker:latest')
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
        # start the NFS
        self.start_NFS()

        #
        # mount the NFS
        self.mount_NFS()

        #
        # ensure that Docker can start (no Slurm processes outside of Docker already running)
        try:
            ready_for_docker()
        except:
            print("Docker host is not ready to start container!")
            raise

        #
        # create the Slurm container if it's not already present
        print("Starting Slurm controller ...")
        if self.config["cluster_name"] not in [x.name for x in self.dkr.containers.list()]:
            # FIXME: gcloud is cloud-provider specific. how can we make this more generic?
            gcloud_conf_dir = subprocess.check_output("echo -n ~/.config/gcloud", shell = True).decode()
            self.dkr.containers.run(
              image = image.tags[0], detach = True, network_mode = "host",
              volumes = {
                "/mnt/nfs" : { "bind" : "/mnt/nfs", "mode" : "rw" },
                gcloud_conf_dir : { "bind" : "/etc/gcloud", "mode" : "rw" }
               },
              name = self.config["cluster_name"], command = "/bin/bash",
              user = self.config["user"], stdin_open = True, remove = True
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
        # save the configuration to disk so that Slurm knows how to configure
        # the nodes it creates
        subprocess.check_call("""
          [ ! -d /mnt/nfs/clust_conf/canine ] && mkdir -p /mnt/nfs/clust_conf/canine ||
            echo -n
          """, shell = True, executable = '/bin/bash')
        with open("/mnt/nfs/clust_conf/canine/backend_conf.pickle", "wb") as f:
            pickle.dump(self.config, f)

    def init_nodes(self):
        if not self.NFS_ready:
            raise Exception("NFS must be mounted before starting nodes!")

        self.wait_for_cluster_ready(elastic = True)

        # list all the nodes that Slurm is aware of

        # although this backend does not manage starting/stopping nodes
        # -- this is handled by Slurm's elastic scaling, it will stop any
        # nodes still running when __exit__() is called.
        # TODO: deal with nodes that already exist
        allnodes = pd.read_pickle("/mnt/nfs/clust_conf/slurm/host_LuT.pickle")

        # we only care about nodes that Slurm will actually dispatch jobs to;
        # the rest will be set to "drain" (i.e., blacklisted) below.
        self.nodes = allnodes.groupby("machine_type").apply(
          lambda x : x.iloc[0:math.ceil(len(x)*self.config["clust_frac"])]
        ).droplevel(0)

        # set nodes that will never be used to drain
        for _, g in allnodes.loc[~allnodes.index.isin(self.nodes.index)].groupby("machine_type"):
            node_expr = re.sub(
                          r"(.*worker)(\d+)\1(\d+)", r"\1[\2-\3]",
                          "".join(g.iloc[[0, -1]].index.tolist())
                        )
            (ret, _, _) = self.invoke(
                                      """sudo -E scontrol update nodename={}
                                         state=drain reason=unused""".format(node_expr)
                                    )
            if ret != 0:
                raise RuntimeError("Could not drain nodes!")

        # add NFS server to node list
        self.nodes = pd.concat([
          self.nodes,
          pd.DataFrame({ "machine_type" : "nfs" }, index = [self.config["worker_prefix"] + "-nfs"])
        ])

    def stop(self):
        # delete node configuration file
        try:
            subprocess.check_call(
              "rm -f /mnt/nfs/clust_conf/canine/backend_conf.pickle",
              shell = True,
              timeout = 10
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            print("Couldn't delete node configuration file:", file = sys.stderr)
            print(e)

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

        # if the Docker was not spun up by this context manager, do not shut it
        # down -- it was started from the external wolF server.
        if self.container is not None and not self.preexisting_container:
            self.container().stop()

        #
        # unmount the NFS

        # this needs to be the last step, since Docker will hang if NFS is pulled
        # out from under it
        if self.config["nfs_action_on_stop"] != "run":
            try:
                subprocess.check_call("sudo umount -f /mnt/nfs", shell = True)
            except subprocess.CalledProcessError:
                print("Could not unmount NFS (do you have open files on it?)\nPlease run `lsof | grep /mnt/nfs`, close any open files, and run `sudo umount -f /mnt/nfs` before attempting to run another pipeline.")

        # superclass method will stop/delete/leave the NFS running, depending on
        # how self.config["nfs_action_on_stop"] is set.

        # kill thread that auto-restarts NFS
        if self.NFS_monitor_lock is not None:
            self.NFS_monitor_lock.set()

        if not allnodes.empty:
            self.nodes = allnodes.loc[allnodes["machine_type"] == "nfs"]
            super().stop(action_on_stop = self.config["nfs_action_on_stop"], kill_straggling_jobs = False)

    def _get_container(self, container_name):
        def closure():
            return self.dkr.containers.get(container_name)

        return closure

    def start_NFS(self):
        nfs_nodename = self.config["worker_prefix"] + "-nfs"
        instances = self.list_instances_all_zones()

        # NFS doesn't exist; create it
        # TODO: use API for this
        nfs_inst = instances.loc[instances["name"] == nfs_nodename].squeeze()
        if nfs_inst.empty:
            print("Creating NFS server " + nfs_nodename)
            subprocess.check_call(
                """gcloud compute instances create {nfs_nodename} \
                   --image {image} --machine-type n1-standard-4 --zone {compute_zone} \
                   {nfs_compute_script} \
                   --tags caninetransientimage
                """.format(nfs_nodename = nfs_nodename, **self.config),
                shell = True
            )

        # otherwise, check that NFS is a valid node, and if so, start if necessary
        else:
            print("Found preexisting NFS server " + nfs_nodename)

            # make sure NFS was created by Canine
            if "caninetransientimage" not in nfs_inst["tags"]:
                raise RuntimeError("Preexisting NFS server was not created by Canine.")

            # make sure boot disk image matches image in config.
            nfs_inst_details = self._pzw(gce.instances().get)(instance = nfs_nodename).execute()
            nfs_boot_disk = [x for x in nfs_inst_details["disks"] if x["boot"]][0]
            nfs_disk = self._pzw(gce.disks().get)(
                         disk = re.sub(r".*/(.*)$", r"\1", nfs_boot_disk["source"])
                       ).execute()
            nfs_image = re.sub(r".*/(.*)$", r"\1", nfs_disk["sourceImage"])
            if nfs_image != self.config["image"]:
                raise RuntimeError("Preexisting NFS server's image {ni} does not match image {ci} defined in configuration.".format(ni = nfs_image, ci = self.config["image"]))

            # if we passed these checks, start the NFS if necessary
            # TODO: use the API for this
            if nfs_inst["status"] == "TERMINATED":
                print("Starting preexisting NFS server ... ", end = "", flush = True)
                subprocess.check_call(
                    """gcloud compute instances start {ni} --zone {z} \
                    """.format(ni = nfs_nodename, z = nfs_inst["zone"]),
                    shell = True
                )
                print("done", flush = True)

        # start NFS monitoring thread
        self.NFS_monitor_lock = threading.Event()
        self.NFS_monitor_thread = threading.Thread(
          target = self.autorestart_preempted_node,
          args = (nfs_nodename,)
        )
        self.NFS_monitor_thread.start()

        self.NFS_server_ready = True

    def mount_NFS(self):
        if not self.NFS_server_ready:
            raise Exception("Need to start NFS server before attempting to mount!")

        # TODO: don't hardcode this script path; make it a parameter instead
        nfs_prov_script = os.path.join(
                            os.path.dirname(__file__),
                            'slurm-docker/src/nfs_provision_worker.sh'
                          )
        nfs_nodename = self.config["worker_prefix"] + "-nfs"

        subprocess.check_call("{nps} {nnn}".format(
          nps = nfs_prov_script, nnn = nfs_nodename
        ), shell = True)

        self.NFS_ready = True

    def get_latest_image(self, image_family = None):
        image_family = self.config["image_family"] if image_family is None else image_family
        with gce_lock: # I had issues without the lock
            ans = gce.images().getFromFamily(family = image_family, project = self.config["project"]).execute()
        return ans

    def invoke(self, command, interactive = False):
        if not isatty(sys.stdout, sys.stdin):
            interactive = False

        # re-purpose LocalSlurmBackend's invoke
        local_invoke = super(TransientImageSlurmBackend, self).invoke
        if self.container is not None and self.container().status == "running":
            cmd = "docker exec {ti_flag} {container} {command}".format(
              ti_flag = "-ti" if interactive else "",
              container = self.config["cluster_name"],
              command = command
            )
            return local_invoke(cmd, interactive)
        else:
            return (1, io.BytesIO(), io.BytesIO(b"Container is not running!"))

    def autorestart_preempted_node(self, nodename):
        while not self.NFS_monitor_lock.is_set():
            try:
                inst_details = self._pzw(gce.instances().get)(instance = nodename).execute()
                if inst_details["status"] != "RUNNING":
                    self._pzw(gce.instances().start)(instance = nodename).execute()
            except:
                print("Error querying NFS server status; retrying in 60s ...", file = sys.stderr)

            time.sleep(60)

    def wait_for_container_to_be_ready(self, timeout = 3000):
        print("Waiting up to {} seconds for Slurm controller to start ...".format(timeout))
        (rc, _, _) = self.invoke(
          "timeout {} bash -c 'while [ ! -f /.started ]; do sleep 1; done'".format(timeout),
          interactive = True
        )
        if rc == 124:
            raise TimeoutError("Slurm controller did not start within {} seconds!".format(timeout))

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
                if parent_proc.name() == "containerd":
                    break
                if parent_proc.pid == 1:
                    raise Exception("{desc} is already running on this machine (outside of a Docker container). Please run `[sudo] killall {proc}' and try again.".format(desc = desc, proc = proc))

    #
    # check if mountpoint exists
    try:
        subprocess.check_call(
          "mountpoint -q /mnt/nfs".format(proc),
          shell = True,
          executable = '/bin/bash',
          timeout = 10
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        # TODO: add the repo URL
        raise Exception("NFS did not successfully mount. Please report this bug as a GitHub issue.")
