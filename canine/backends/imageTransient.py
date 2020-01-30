# vim: set expandtab:

import typing
import subprocess
import os
import sys

from .local import LocalSlurmBackend
from ..utils import get_default_gcp_project, gcp_hourly_cost

import googleapiclient.discovery as gd
import pandas as pd

gce = gd.build('compute', 'v1')

def list_instances(zone: str, project: str) -> pd.DataFrame:
    """
    List all instances in a given zone and project
    """
    inst_dict = gce.instances().list(project = project, zone = zone).execute()

    fnames = ['name', 'machineType', 'status', 'zone', 'selfLink', 'tags']

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
    Backend for starting a Slurm cluster using a preconfigured GCE image.
    The image must meet the following requirements:
    * The current node is a valid Slurm controller, i.e.:
        * `slurmctld`, `munged`, and `slurmdbd` run properly (note that these do not need
          to already be running when invoking Canine with this backend; they will
          be started automatically as needed.)
        * Accounting is enabled (i.e., `sacct` can list completed jobs)
    * The default Slurm partition is compatible with any nodes that get spun up:
        * Worker node names must match names specified in a partition defined in `slurm.conf`
        * Worker node types must be consistent with node definitions in `slurm.conf`
    * The image provided has a valid Slurm installation, compatible with that of the
      controller node (e.g., same version, same plugins, etc.)
    * If GPUs are added, drivers must already be installed
    """

    def __init__(
        self, *, image: str, worker_prefix: str = 'slurm-canine', tot_node_count: int = 50,
        init_node_count: typing.Optional[int] = None, compute_zone: str = 'us-central1-a',
        worker_type: str = 'n1-highcpu-2', preemptible: bool = True,
        gpu_type: typing.Optional[str] = None, gpu_count: int = 0,
        compute_script_file: typing.Optional[str] = None,
        compute_script: typing.Optional[str] = None,
        project: typing.Optional[str] = None,
        user: typing.Optional[str] = None, slurm_conf_path: typing.Optional[str] = None,
        action_on_stop: str = "stop",
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
                "--metadata startup-script=\"{}\"".format(compute_script)
                if compute_script else "",
            "project" : project if project else get_default_gcp_project(),
            "user" : user if user else "root",
            "slurm_conf_path" : slurm_conf_path,
            "action_on_stop" : action_on_stop
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
            # start Slurm controller (and associated programs)
            self.init_slurm()

            # start nodes
            self.init_nodes()

            return self
        except Exception as e:
            print("ERROR: Could not initialize cluster; attempting to tear down.", file = sys.stderr)

            self.stop()
            raise e

    def __exit__(self, *args):
        self.stop()

    def init_slurm(self):
        #
        # check if Slurm is already running locally; start (with hard reset) if not

        print("Checking for running Slurm controller ... ", end = "", flush = True)

        subprocess.check_call(
            """sudo -E -u {user} bash -c 'pgrep slurmctld || slurmctld -c -f {slurm_conf_path} &&
               slurmctld reconfigure; pgrep slurmdbd || slurmdbd; pgrep munged || munged -f'
            """.format(**self.config),
            shell = True,
            stdout = subprocess.DEVNULL
        )

        # ensure all started successfully
        subprocess.check_call("pgrep slurmctld && pgrep slurmdbd && pgrep munged", shell = True,
                              stdout = subprocess.DEVNULL)

        print("done", flush = True)

    def init_nodes(self):
        #
        # create/start worker nodes
        print("Checking for preexisting cluster nodes ... ", end = "", flush = True)

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

        print("done", flush = True)

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
            typemismatch_idx = ~instances.loc[:, "machineType"].isna() & \
                               (instances.loc[:, "machineType"] != self.config["worker_type"])

            if typemismatch_idx.any():
                print("ERROR: nodes that already exist do not match specified machine type ({worker_type}):".format(**self.config), file = sys.stderr)
                print(instances.drop(columns = "selfLink").loc[typemismatch_idx] \
                  .to_string(index = False), file = sys.stderr)
                print("This will result in Slurm bitmap corruption.", file = sys.stderr)

                raise RuntimeError('Preexisting cluster nodes do not match specified machine type for new nodes')

            # WARN if any nodes (Canine or external) are already defined but
            # present in other zones
            zonemismatch_idx = ~instances.loc[:, "zone"].isna() & \
                               (instances.loc[:, "zone"] != self.config["compute_zone"])

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

        if (~self.nodes["is_k9_node"]).any():
            nodes_to_create = self.nodes.index[~self.nodes["is_k9_node"]].values

            print("Creating {0:d} worker nodes ... ".format(nodes_to_create.shape[0]),
                  end = "", flush = True)
            subprocess.check_call(
                """gcloud compute instances create {workers} \
                   --image {image} --machine-type {worker_type} --zone {compute_zone} \
                   {compute_script} {compute_script_file} {preemptible} \
                   --tags caninetransientimage
                """.format(**self.config, workers = " ".join(nodes_to_create)),
                shell = True
            )
            print("done", flush = True)

        # start nodes previously created by Canine

        instances_to_start = instances.loc[instances["is_k9_node"] &
          (instances["status"] == "TERMINATED"), "name"]

        # TODO: use API to start these
        if instances_to_start.shape[0] > 0:
            print("Starting {0:d} preexisting worker nodes ... ".format(instances_to_start.shape[0]),
                  end = "", flush = True)
            subprocess.check_call(
                """gcloud compute instances start {workers} --zone {compute_zone} \
                """.format(**self.config, workers = " ".join(instances_to_start.values)),
                shell = True
            )
            print("done", flush = True)

        #
        # shut down nodes exceeding init_node_count

        for node in self.nodes.index[self.config["init_node_count"]:]:
            try:
                self._pzw(gce.instances().stop)(instance = node).execute()
            except Exception as e:
                print("WARNING: couldn't shutdown instance {}".format(node), file = sys.stderr)
                print(e)

    def stop(self, action_on_stop = None):
        """
        Delete or stop (default) compute instances
        """
        if action_on_stop is None:
            action_on_stop = self.config["action_on_stop"]

        #
        # stop, delete, or leave running compute nodes

        for node in self.nodes.index:
            try:
                if action_on_stop == "delete":
                    self._pzw(gce.instances().delete)(instance = node).execute()
                elif action_on_stop == "run":
                    # leave it running
                    pass
                else:
                    # default behavior is to shut down
                    self._pzw(gce.instances().stop)(instance = node).execute()
            except Exception as e:
                print("WARNING: couldn't shutdown instance {}".format(node), file = sys.stderr)
                print(e)

    def list_instances_all_zones(self):
        """
        List all instances across all zones in `self.config["project"]`
        """
        zone_dict = gce.zones().list(project = self.config["project"]).execute()

        return pd.concat([
          list_instances(zone = x["name"], project = self.config["project"])
          for x in zone_dict["items"]
        ], axis = 0).reset_index(drop = True)

    def wait_for_cluster_ready(self):
        """
        Blocks until the main partition is marked as up
        """
        super().wait_for_cluster_ready(elastic = False)

    # a handy wrapper to automatically add this instance's project and zone to
    # GCP API calls
    # TODO: for bonus points, can we automatically apply this to all relevant
    #       methods in a GCE class instance?
    def _pzw(self, f):
        def x(*args, **kwargs):
            return f(project = self.config["project"], zone = self.config["compute_zone"], *args, **kwargs)

        return x

    def estimate_cost(self, clock_uptime: typing.Optional[float] = None, node_uptime: typing.Optional[float] = None, job_cpu_time: typing.Optional[typing.Dict[str, float]] = None) -> typing.Tuple[float, typing.Optional[typing.Dict[str, float]]]:
        """
        Returns a cost estimate for the cluster, based on any cost information available
        to the backend. May provide total node uptime (for cluster cost estimate)
        and/or cpu_time for each job to get job specific cost estimates.
        Clock uptime may be provided and is useful if the cluster has an inherrant
        overhead for uptime (ie: controller nodes).
        Note: Job cost estimates may not sum up to the total cluster cost if the
        cluster was not at full utilization.
        """
        cluster_cost = 0
        worker_cpu_cost = 0
        job_cost = None
        if node_uptime is not None:
            worker_info = {
                'mtype': self.config['worker_type'],
                'preemptible': bool(self.config['preemptible'])
            }
            if 'gpu_type' in self.config and 'gpu_count' in self.config and self.config['gpu_count'] > 0:
                worker_info['gpu_type'] = self.config['gpu_type']
                worker_info['gpu_count'] = self.config['gpu_count']
            worker_hourly_cost = gcp_hourly_cost(**worker_info)
            cluster_cost = node_uptime * worker_hourly_cost
            mtype_prefix = self.config['worker_type'][:3]
            if mtype_prefix in {'f1-', 'g1-'}:
                ncpus = 1
            elif mtype_prefix == 'cus': # n1-custom-X
                ncpus = int(self.config['worker_type'].split('-')[1])
            else:
                ncpus = int(self.config['worker_type'].split('-')[2])
            # Approximates the cost burden / CPU hour of the VM
            worker_cpu_cost = worker_hourly_cost / ncpus
        if job_cpu_time is not None:
            job_cost = {
                job_id: worker_cpu_cost * cpu_time
                for job_id, cpu_time in job_cpu_time.items()
            }
        return cluster_cost, job_cost



# }}}
