# vim: set expandtab:

import typing
from functools import lru_cache
import subprocess
import tempfile
from io import BytesIO
import os
import sys

#
# switch these lines appropriately when testing in an interactive session

from .local import LocalSlurmBackend
#from canine.backends.local import LocalSlurmBackend

from ..utils import get_default_gcp_project, ArgumentHelper, check_call
#from canine.utils import get_default_gcp_project, ArgumentHelper, check_call

from IPython.core.debugger import set_trace

import googleapiclient.discovery as gd
import pandas as pd

gce = gd.build('compute', 'v1');

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


def list_instances(zone: str, project: str) -> pd.DataFrame:
    inst_dict = gce.instances().list(project = project, zone = zone).execute()

    fnames = ['name', 'machineType', 'status', 'zone', 'selfLink', 'tags'];

    if "items" in inst_dict:
        inst_DF = pd.DataFrame([[x[y] for y in fnames] for x in inst_dict['items']], columns = fnames)
        inst_DF["tags"] = inst_DF["tags"].apply(lambda x : x["items"] if "items" in x else [])
    else:
        return pd.DataFrame()

    # API returns selfLinks; parse these into something human-readable
    return inst_DF.apply(lambda x : x.str.replace(r'.*/', '')
                         if x.name in ["machineType", "zone"] else x)

class TransientImageSlurmBackend(LocalSlurmBackend): # {{{
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
        user: typing.Optional[str] = None, slurm_conf_path: typing.Optional[str] = None,
        delete_on_stop: bool = False,
        **kwargs
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
            "project" : project if project else get_default_gcp_project(),
            "user" : user if user else os.getenv('USER'),
            "slurm_conf_path" : slurm_conf_path,
            "delete_on_stop" : delete_on_stop
        }

        # this backend resets itself on startup; no need for the orchestrator
        # to do this.
        self.hard_reset_on_orch_init = False

        # list of nodes under the purview of Canine
        self.nodes = pd.DataFrame()

        # export slurm_conf_path to environment
        os.environ['SLURM_CONF'] = self.config["slurm_conf_path"]

    def __enter__(self):
        try:
            #
            # check if Slurm is already running locally; start (with hard reset) if not

            subprocess.check_call(
                """sudo -E -u {user} bash -c 'pgrep slurmctld || slurmctld -c -f {slurm_conf_path} &&
                   slurmctld reconfigure; pgrep slurmdbd || slurmdbd; pgrep munged || munged -f'
                """.format(**self.config),
                shell = True
            )

            # ensure all started successfully
            subprocess.check_call("pgrep slurmctld && pgrep slurmdbd && pgrep munged", shell = True)

            #
            # create/start worker nodes

            nodenames = pd.DataFrame(index = [
                          self.config["worker_prefix"] + str(x) for x in
                          range(1, self.config["tot_node_count"] + 1)
                        ])

            # check which worker nodes exist outside of Canine
            instances = self.list_instances_all_zones()
            k9_inst_idx = instances["tags"].apply(lambda x : "caninetransientimage" in x)

            nodenames["is_ex_node"] = nodenames.index.isin(instances.loc[~k9_inst_idx, "name"])

            # check which worker nodes were previous created by Canine
            nodenames["is_k9_node"] = nodenames.index.isin(instances.loc[k9_inst_idx, "name"])

            instances = instances.merge(nodenames, left_on = "name", right_index = True,
                          how = "right")

            # handle nodes that will not be created
            if (nodenames["is_ex_node"] | nodenames["is_k9_node"]).any():
                # WARN about worker nodes outside of Canine
                ex_nodes = nodenames.index[nodenames["is_ex_node"]]

                if not ex_nodes.empty:
                    print("WARNING: the following nodes already exist outside of Canine:\n - ",
                      file = sys.stderr, end = "")
                    print("\n - ".join(ex_nodes), file = sys.stderr)
                    print("Canine will not touch these nodes.")

                # ERROR if any instance types (Canine or external) are incongruous
                # with given definition
                typemismatch_idx = (instances.loc[:, "machineType"] != self.config["worker_type"])

                if typemismatch_idx.any():
                    print("ERROR: nodes that already exist do not match specified machine type ({worker_type}):".format(**self.config), file = sys.stderr)
                    print(instances.drop(columns = "selfLink").loc[typemismatch_idx] \
                      .to_string(index = False), file = sys.stderr)
                    print("This will result in Slurm bitmap corruption.", file = sys.stderr)

                    raise RuntimeError('Preexisting cluster nodes do not match specified machine type for new nodes')

                # WARN if any nodes (Canine or external) are already defined but
                # present in other zones
                zonemismatch_idx = (instances.loc[:, "zone"] != self.config["compute_zone"])

                if zonemismatch_idx.any():
                    print("WARNING: nodes that already exist do not match specified compute zone ({compute_zone}):".format(**self.config), file = sys.stderr)
                    print(instances.drop(columns = "selfLink").loc[zonemismatch_idx] \
                      .to_string(index = False), file = sys.stderr)
                    print("This may result in degraded performance or egress charges.",
                      file = sys.stderr)

            self.nodes = nodenames.loc[~nodenames["is_ex_node"]]

            # create the nodes

            # TODO: support the other config flags
            # TODO: use API to launch these

            if ~self.nodes["is_k9_node"].any():
                subprocess.check_call(
                    """gcloud compute instances create {workers} \
                       --image {image} --machine-type {worker_type} --zone {compute_zone} \
                       {compute_script} {compute_script_file} {preemptible} \
                       --tags caninetransientimage
                    """.format(**self.config, workers = " ".join(self.nodes.index[~self.nodes["is_k9_node"]].values)),
                    shell = True
                )

            # start nodes previously created by Canine

            instances_to_start = instances.loc[instances["is_k9_node"] &
              (instances["status"] == "TERMINATED"), "name"]

            # TODO: use API to start these
            if instances_to_start.shape[0] > 0:
                subprocess.check_call(
                    """gcloud compute instances start {workers} --zone {compute_zone} \
                    """.format(**self.config, workers = " ".join(instances_to_start.values)),
                    shell = True
                )

            #
            # shut down nodes exceeding init_node_count

            for node in self.nodes.index[self.config["init_node_count"]:]:
                try:
                    self._pzw(gce.instances().stop)(instance = node).execute()
                except Exception as e:
                    print("WARNING: couldn't shutdown instance {}".format(node), file = sys.stderr)
                    print(e) 

            return self
        except Exception as e:
            print("ERROR: Could not initialize cluster; attempting to tear down.", file = sys.stderr)

            self.stop()
            raise e

    def __exit__(self, *args):
        self.stop()

    def stop(self, delete_on_stop = None):
        if delete_on_stop is None:
            delete_on_stop = self.config["delete_on_stop"] 

        #
        # stop or delete compute nodes

        for node in self.nodes.index:
            try:
                if delete_on_stop:
                    self._pzw(gce.instances().delete)(instance = node).execute()
                else:
                    self._pzw(gce.instances().stop)(instance = node).execute()
            except Exception as e:
                print("WARNING: couldn't shutdown instance {}".format(node), file = sys.stderr)
                print(e) 

    def list_instances_all_zones(self):
        zone_dict = gce.zones().list(project = self.config["project"]).execute()

        return pd.concat([
          list_instances(zone = x["name"], project = self.config["project"])
          for x in zone_dict["items"]
        ], axis = 0).reset_index(drop = True)

    # a handy wrapper to automatically add this instance's project and zone to
    # GCP API calls
    # TODO: for bonus points, can we automatically apply this to all relevant
    #       methods in a GCE class instance?
    def _pzw(self, f):
        def x(*args, **kwargs):
            return f(project = self.config["project"], zone = self.config["compute_zone"], *args, **kwargs)

        return x


# }}}
