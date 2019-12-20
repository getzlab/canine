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

from .imageTransient import TransientImageSlurmBackend, list_instances, gce
from ..utils import get_default_gcp_project, gcp_hourly_cost

import pandas as pd

class DockerTransientImageSlurmBackend(TransientImageSlurmBackend): # {{{
    def __init__(
        self, nfs_compute_script = "/usr/local/share/cga_pipeline/src/provision_storage.sh",
        nfs_disk_size = 100, nfs_disk_type = "pd-standard", image_family = "pydpiper",
        image = None, cluster_name = None, **kwargs
    ):
        if cluster_name is None:
            raise ValueError("You must specify a name for this Slurm cluster!")

        if "image" not in kwargs:
            kwargs["image"] = image

        # superclass constructor does something special with compute_script so
        # we need to pass it in
        kwargs["compute_script"] = "/usr/local/share/cga_pipeline/src/provision_worker.sh {worker_prefix}".format(worker_prefix = socket.gethostname())
        super().__init__(**kwargs)

        self.config = {
          "cluster_name" : cluster_name,
          "worker_prefix" : socket.gethostname(),
          "nfs_compute_script" :
            "--metadata startup-script=\"{script} {nfsds:d} {nfsdt}\"".format(
              script = nfs_compute_script,
              nfsds = nfs_disk_size,
              nfsdt = nfs_disk_type
            ),
          "image_family" : image_family,
          "image" : self.get_latest_image(image_family)["name"] if image is None else image,
          **{ k : v for k, v in self.config.items() if k not in { "worker_prefix", "image" } }
        }

        # placeholder for Docker API
        self.dkr = None

        # placeholder for Docker container object
        self.container = None

        self.NFS_server_ready = False
        self.NFS_ready = False

    def init_slurm(self):
        self.dkr = docker.from_env()

        #
        # check if image exists
        try:
            image = self.dkr.images.get('broadinstitute/pydpiper:latest')
        except docker.errors.ImageNotFound:
            raise Exception("You have not yet built or pulled the Slurm Docker image!")

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
        if self.config["cluster_name"] not in [x.name for x in self.dkr.containers.list()]:
        #if image not in [x.image for x in self.dkr.containers.list()]:
            self.dkr.containers.run(
              image = image.tags[0], detach = True, network_mode = "host",
              volumes = { "/mnt/nfs" : { "bind" : "/mnt/nfs", "mode" : "rw" } },
              name = self.config["cluster_name"], command = "/bin/bash",
              stdin_open = True, remove = True
            )
            self.container = self._get_container(self.config["cluster_name"])

        # otherwise, try and start it if it's stopped
        else:
            self.container = self._get_container(self.config["cluster_name"])
            if self.container().status == "exited":
                self.container().start()

        # TODO: should we restart slurmctld in the container here?

        #
        # save the configuration to disk so that Slurm knows how to configure
        # the nodes it creates
        subprocess.check_call("""
          [ ! -d /mnt/nfs/clust_conf/canine ] && mkdir -p /mnt/nfs/clust_conf/canine ||
            echo -n
          """, shell = True, executable = '/bin/bash')
        with open("/mnt/nfs/clust_conf/canine/backend_conf.pickle", "wb") as f:
            pickle.dump(self.config, f)
        # TODO: erase this when the backend exits

    def init_nodes(self):
        if not self.NFS_ready:
            raise Exception("NFS must be mounted before starting nodes!")

        parts = []
        with open("/mnt/nfs/clust_conf/slurm/slurm.conf", "r") as f:
            for line in f:
                if re.match(r"^PartitionName", line) is None:
                    continue
                parts.append({ y[0] : y[1] for y in [x.split("=") for x in line.rstrip().split(" ")] })
        parts = pd.DataFrame(parts)

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
            subprocess.check_call(
                """gcloud compute instances create {nfs_nodename} \
                   --image {image} --machine-type n1-standard-4 --zone {compute_zone} \
                   {nfs_compute_script} {preemptible} \
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

        self.NFS_server_ready = True

    def mount_NFS(self):
        if not self.NFS_server_ready:
            raise Exception("Need to start NFS server before attempting to mount!")

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
        return gce.images().getFromFamily(family = image_family, project = self.config["project"]).execute()

    def invoke(self, command, interactive = False):
        return_code, (stdout, stderr) = self.container().exec_run(
          command, demux = True, tty = interactive, stdin = interactive
        )
        return (return_code, io.BytesIO(stdout), io.BytesIO(stderr))

    def stop(self, action_on_stop = None):
        if action_on_stop is None:
            action_on_stop = self.config["action_on_stop"]

        #
        # stop running worker nodes
        super().stop(action_on_stop)

        #
        # stop NFS node and Docker
        #if action_on_stop == "delete":

# }}}                

# Python version of checks in docker_run.sh
def ready_for_docker():
    #
    # check if Slurm/Munge are already running
    already_running = [["slurmctld", "A Slurm controller"],
                       ["slurmdbd", "The Slurm database daemon"],
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
          executable = '/bin/bash'
        )
    except subprocess.CalledProcessError:
        # TODO: add the repo URL
        raise Exception("NFS did not successfully mount. Please report this bug as a GitHub issue.")
