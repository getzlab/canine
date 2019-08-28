# vim: set expandtab:

import typing
from functools import lru_cache
import subprocess
import tempfile
from io import BytesIO
import os
from .remote import RemoteSlurmBackend
from ..utils import get_default_gcp_project, ArgumentHelper, check_call
import pandas as pd


@lru_cache(2)
def get_machine_types(zone: str) -> pd.DataFrame:
    """
    Returns a dataframe of defined machine types in the given zone
    """
    cmd = "gcloud compute machine-types list --zones {}".format(zone)
    proc = subprocess.run(
        cmd,
        shell=True,
        stdout=subprocess.PIPE
    )
    stdout = BytesIO(proc.stdout)
    check_call(cmd, proc.returncode, stdout, None)
    df = pd.read_fwf(
        stdout,
        index_col=0
    )
    df['MEMORY'] = [int(x*1024) for x in df['MEMORY_GB']]
    return df[['CPUS', 'MEMORY']]


def parse_machine_type(mtype: str, zone: str) -> typing.Tuple[int, int]:
    """
    Parses a google machine type name and returns a tuple of cpu count, memory (mb)
    """
    mtypes = get_machine_types(zone)
    if mtype in mtypes.index:
        return (mtypes['CPUS'][mtype], mtypes['MEMORY'][mtype])
    if mtype.endswith('-ext'):
        mtype = mtype[:-4] # Drop -ext suffix
    custom, cores, mem = mtype.split('-')
    return (int(cores), int(mem))


class TransientImageSlurmBackend(LocalSlurmBackend):
    """
    Backend for starting a Slurm cluster using a preconfigured GCP image.
    The image must meet the following requirements:
    * Slurm is installed and configured (compute node configuration not necessary)
    * Worker node names must match names specified in a partition defined in slurm.conf
    * Worker node types must be consistent with node definitions in slurm.conf
    * If GPUs are added, drivers must already be installed
    * An external NFS server must be present, /etc/fstab must be set up on the
      image to mount the server, and /etc/exports must allow mounting of the nodes
    """

    def __init__(
        self, *, image: str, worker_prefix: str = 'slurm-canine', tot_node_count: int = 50,
        init_node_count: typing.Optional[int] = None, compute_zone: str = 'us-central1-a',
        worker_type: str = 'n1-highcpu-2', preemptible: bool = True,
        gpu_type: typing.Optional[str] = None, gpu_count: int = 0,
        compute_script_file: typing.Optional[str] = None,
        compute_script: typing.Optional[str] = None,
        controller_script_file: typing.Optional[str] = None,
        controller_script: typing.Optional[str] = None,
        secondary_disk_size: int = 0, project: typing.Optional[str] = None, 
        user: typing.Optional[str] = None, slurm_conf_path: typing.Optional[str] = None
    ):
        #
        # validate inputs that will not be caught later on (e.g. by gcloud invocations)
        if tot_node_count < 1:
            raise ValueError("tot_node_count cannot be less than 1.")

        if init_node_count is not None:
            if init_node_count < 0:
                raise ValueError("init_node_count cannot be negative.")

            if init_node_count > tot_node_count:
                raise ValueError("init_node_count cannot exceed tot_node_count.")

        if compute_script_file is not None and compute_script is not None:
            raise ValueError("Cannot simultaneously specifiy compute_script_file and compute_script.")

        if controller_script_file is not None and controller_script is not None:
            raise ValueError("Cannot simultaneously specifiy controller_script_file and controller_script.")

        if slurm_conf_path is None:
            raise ValueError("Currently, path to slurm.conf must be explicitly specified.")

        # make config dict
        self.config = {
            "image" : image,
            "worker_prefix" : worker_prefix,
            "tot_node_count" : tot_node_count,
            "init_node_count" : init_node_count if init_node_count else tot_node_count,
            "compute_zone" : compute_zone,
            "worker_type" : worker_type,
            "preemptible" : "--preemptible" if preemptible else "",
            "gpu_type" : gpu_type,
            "gpu_count" : gpu_count,
            "compute_script_file" :
                "--metadata-from-file startup-script={}".format(compute_script_file)
                if compute_script_file else "",
            "compute_script" :
                "--metadata startup-script={}".format(compute_script)
                if compute_script else "",
            "controller_script_file" : 
                "--metadata-from-file startup-script={}".format(controller_script_file)
                if controller_script_file else "",
            "controller_script" :
                "--metadata startup-script={}".format(controller_script)
                if controller_script else "",
            "secondary_disk_size" : secondary_disk_size,
            "project" : project if project else "",
            "user" : user if user else os.getenv('USER'),
            "slurm_conf_path" : slurm_conf_path
        }

        # since GCP cannot start nodes powered off, after starting all nodes,
        # we will power down nodes init_node_count + 1 ... tot_node_count
        self.config["init_node_count"] += 1

    def __enter__(self):
        try:
            #
            # check if Slurm is already running locally; start (with hard reset) if not
            subprocess.check_call(
                """sudo -u {user} bash -c 'pgrep slurmctld || slurmctld -c -f {slurm_conf_path} &&
                   slurmctld reconfigure; pgrep munged || munged -f'
                """.format(**self.config)
                shell = True
            )

            # ensure both started successfully
            subprocess.check_call("pgrep slurmctld && pgrep munged", shell = True)

            #
            # create worker nodes

            # check which nodes already exist

            # TODO: support the other config flags
            # TODO: check if any worker nodes already exist; skip them
            subprocess.check_call(
                """gcloud compute instances create $(eval echo {worker_prefix}\{1..{tot_node_count}\})
                   --image {image} --machine-type {worker_type} --zone {compute_zone}
                   {compute_script} {compute_script_file} {preemptible}
                """.format(**self.config),
                shell = True,
                exceutable = '/bin/bash'
            )

            #
            # shut down nodes exceeding init_node_count
            subprocess.check_call(
                """gcloud compute instances stop $(eval echo {worker_prefix}\{{init_node_count}..{tot_node_count}\})
                   --zone {compute_zone} --quiet 
                """.format(**self.config),
                shell = True,
                exceutable = '/bin/bash'
            )

            return self
        except:
            self.stop()
            raise

    def __exit__(self, *args):
        self.stop()

    def stop(self):
        try:
            #
            # shut down compute nodes

            # XXX: hard vs. soft shutdown -- totally delete the nodes or not?

            # XXX: if some of the nodes are already shut down, does this return a nonzero exit?
            #      if so, we don't want to raise any exceptions.
            subprocess.check_call(
                """gcloud compute instances stop $(eval echo {worker_prefix}\{1..{tot_node_count}\})
                   --zone {compute_zone} --quiet 
                """.format(**self.config),
                shell = True,
                exceutable = '/bin/bash'
            )
        except:
            raise
