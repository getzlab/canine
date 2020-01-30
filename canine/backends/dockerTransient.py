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

from .imageTransient import TransientImageSlurmBackend, list_instances, gce
from ..utils import get_default_gcp_project, gcp_hourly_cost

import pandas as pd

class DockerTransientImageSlurmBackend(TransientImageSlurmBackend): # {{{
    def __init__(
        self, nfs_compute_script = "/usr/local/share/cga_pipeline/src/provision_storage_container_host.sh",
        compute_script = "/usr/local/share/cga_pipeline/src/provision_worker_container_host.sh",
        nfs_disk_size = 2000, nfs_disk_type = "pd-standard", nfs_action_on_stop = "stop", nfs_image = "",
        action_on_stop = "delete", image_family = "pydpiper", image = None,
        cluster_name = None, clust_frac = 0.01, user = os.environ["USER"], **kwargs
    ):
        if cluster_name is None:
            raise ValueError("You must specify a name for this Slurm cluster!")

        if "image" not in kwargs:
            kwargs["image"] = image

        # superclass constructor does something special with compute_script so
        # we need to pass it in
        kwargs["compute_script"] = "{script} {worker_prefix}".format(
          script = compute_script,
          worker_prefix = socket.gethostname()
        )
        super().__init__(**{**kwargs, **{ "slurm_conf_path" : "" }})

        self.config = {
          "cluster_name" : cluster_name,
          "worker_prefix" : socket.gethostname(),
          "nfs_compute_script" :
            "--metadata startup-script=\"{script} {nfsds:d} {nfsdt} {nfsimg}\"".format(
              script = nfs_compute_script,
              nfsds = nfs_disk_size,
              nfsdt = nfs_disk_type,
              nfsimg = nfs_image
            ),
          "action_on_stop" : action_on_stop,
          "nfs_action_on_stop" : nfs_action_on_stop if nfs_action_on_stop is not None
            else self.config["action_on_stop"],
          "image_family" : image_family,
          "image" : self.get_latest_image(image_family)["name"] if image is None else image,
          "clust_frac" : max(min(clust_frac, 1.0), 1e-6),
          "user" : user,
          **{ k : v for k, v in self.config.items() if k not in { "worker_prefix", "image", "user", "action_on_stop" } }
        }

        # placeholder for Docker API
        self.dkr = None

        # placeholder for Docker container object
        self.container = None

        # placeholder for node list (loaded from lookup table)
        self.nodes = None

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
              user = self.config["user"], stdin_open = True, remove = True
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

    def init_nodes(self):
        if not self.NFS_ready:
            raise Exception("NFS must be mounted before starting nodes!")

        self.wait_for_cluster_ready()

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
        # stop the Docker
        if self.container is not None:
            self.container().stop()

        # delete node configuration file
        subprocess.check_call("rm -f /mnt/nfs/clust_conf/canine/backend_conf.pickle", shell = True)

        # get list of nodes that still exist
        allnodes = self.nodes
        extant_nodes = self.list_instances_all_zones()
        self.nodes = allnodes.loc[allnodes.index.isin(extant_nodes["name"]) &
                       (allnodes["machine_type"] != "nfs")]

        # superclass method will stop/delete/leave these running, depending on how
        # self.config["action_on_stop"] is set
        super().stop()

        # we handle the NFS separately
        self.nodes = allnodes.loc[allnodes["machine_type"] == "nfs"]
        super().stop(action_on_stop = self.config["nfs_action_on_stop"])

        if self.config["nfs_action_on_stop"] != "run":
            try:
                subprocess.check_call("sudo umount -f /mnt/nfs", shell = True)
            except subprocess.CalledProcessError:
                print("Could not unmount NFS (do you have open files on it?)\nPlease run `lsof | grep /mnt/nfs`, close any open files, and run `sudo umount -f /mnt/nfs` before attempting to run another pipeline.")

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
        return gce.images().getFromFamily(family = image_family, project = self.config["project"]).execute()

    def invoke(self, command, interactive = False):
        return_code, (stdout, stderr) = self.container().exec_run(
          command, demux = True, tty = interactive, stdin = interactive
        )
        return (return_code, io.BytesIO(stdout), io.BytesIO(stderr))

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
