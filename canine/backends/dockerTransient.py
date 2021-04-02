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
import shutil

from .imageTransient import TransientImageSlurmBackend, list_instances, gce
from ..utils import get_default_gcp_project, gcp_hourly_cost, isatty, canine_logging

from requests.exceptions import ConnectionError as RConnectionError
from urllib3.exceptions import ProtocolError

import pandas as pd

import multiprocessing

gce_lock = multiprocessing.Lock()

class DockerTransientImageSlurmBackend(TransientImageSlurmBackend): # {{{
    def __init__(
        self, cluster_name, *,
        startup_script = "/usr/local/share/slurm_gcp_docker/src/provision_worker_container_host.sh",
        shutdown_script = "/usr/local/share/slurm_gcp_docker/src/shutdown_worker_container_host.sh",
        action_on_stop = "delete", image_family = None, image = None,
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

        self.config = {
          "cluster_name" : cluster_name,
          "worker_prefix" : socket.gethostname(),
          "action_on_stop" : action_on_stop,
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
        # export the NFS mountpoint
        self.export_NFS()

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
        self.wait_for_cluster_ready(elastic = True, timeout=60)

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
                                      """sudo -E scontrol update nodename={} state=drain reason=unused""".format(node_expr)
                                    )
            if ret != 0:
                raise RuntimeError("Could not drain nodes!")

    def stop(self): 
        # if the Docker was not spun up by this context manager, do not tear
        # anything down -- we don't want to clobber an already running cluster
        if not self.preexisting_container:
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
            return self.dkr.containers.get(container_name)

        return closure

    def export_NFS(self):
        ## Check if /mnt/nfs is created
        if not os.path.exists("/mnt/nfs"):
            try:
                subprocess.check_call("sudo mkdir /mnt/nfs", shell=True)
                subprocess.check_call("sudo chmod 777 /mnt/nfs", shell=True)
            except:
                # TODO: be more specific about exception catching
                canine_logging.error("Could not create NFS mountpoint; see stack trace for details")
                raise
        
        ## Check disk usage and warn user if it is small
        free_space_gb = int(shutil.disk_usage("/mnt/nfs").free/(1024**3))
        if free_space_gb < 300:
            canine_logging.warning(
                "Available disk storage at /mnt/nfs is small ({} GB remaining)".format(free_space_gb)
            )

        # TODO: add warnings if overall disk is small (bad disk IO) or node core
        #       count is low (bad network IO)
        
        ## Expose NFS (we won't unexport in __exit__)
        subprocess.check_call("sudo exportfs -o rw,async,no_subtree_check,insecure,no_root_squash *.internal:/mnt/nfs", shell=True)

    def get_latest_image(self, image_family = None):
        image_family = self.config["image_family"] if image_family is None else image_family
        with gce_lock: # multiprocessing.Lock
            ans = gce.images().getFromFamily(family = image_family, project = self.config["project"]).execute()
        return ans

    def invoke(self, command, interactive = False, bypass_docker = False):
        """
        Set bypass_docker to True to execute the command directly on the host,
        rather than in the controller container. Useful for debugging.
        """
        if not isatty(sys.stdout, sys.stdin):
            interactive = False

        # re-purpose LocalSlurmBackend's invoke
        local_invoke = super(TransientImageSlurmBackend, self).invoke
        if self.container is not None and self.container().status == "running":
            if not bypass_docker:
                cmd = "docker exec {ti_flag} {container} {command}".format(
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
                    if any([stderr_str.startswith(reason) for reason in [
                      "Error response from daemon: No such exec instance",
                      "OCI runtime exec failed: exec failed"
                    ]]):
                        canine_logging.warning(
                          'Command {cmd} failed with known recoverable error reason "{err}"; retrying in {timeout} seconds up to {tries} times'.format(
                            cmd = command,
                            err = stderr_str,
                            timeout = timeout,
                            tries = 5 - tries
                          )
                        )
                        time.sleep(timeout)
                        timeout *= 2
                        tries += 1

                    # warn the user that the command had stuff written to stderr,
                    # since this may indicate something is wrong
                    else:
                        # TODO: when we implement verbosity, this should be "verbose"
                        canine_logging.info1(
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
