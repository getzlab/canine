import getpass
import tempfile
import typing
import time
import subprocess
import shutil
import os
import hashlib
import io
import datetime
import sys
from .remote import RemoteSlurmBackend
from ..utils import get_default_gcp_project, ArgumentHelper, check_call, gcp_hourly_cost

import googleapiclient.discovery as gd
import googleapiclient.errors
import pandas as pd
import yaml
import crayons

try:
    gce = gd.build('compute', 'v1')
except gd._auth.google.auth.exceptions.GoogleAuthError:
    print(
        "Unable to load gcloud credentials. Transient Backends may not be available",
        file=sys.stderr
    )
    gce = None

GPU_TYPES = {
    'nvidia-tesla-k80',
    'nvidia-tesla-p100',
    'nvidia-tesla-v100',
    'nvidia-tesla-p4',
    'nvidia-tesla-t4'
}

TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%S.%f%z'

# Overhaul plan
# Follow setup script to build new base GCP image
# Update script to do base configuration and support mixed node topography

class TransientGCPSlurmBackend(RemoteSlurmBackend):
    """
    Backend for transient slurm clusters which need to be deployed and configured
    on GCP before they're ready for use.
    """

    @staticmethod
    def get_controller_image():
        controller_images = gce.images().list(project='broad-cga-aarong-gtex', filter='family = canine-tgcp-controller').execute()
        if 'items' not in controller_images or len(controller_images['items']) < 1:
            raise ValueError("No public controller images found")
        return sorted(
            controller_images['items'],
            key=lambda img:datetime.datetime.strptime(img['creationTimestamp'], TIMESTAMP_FORMAT),
            reverse=True
        )[0]['selfLink']

    @staticmethod
    def get_compute_service_account():
        proc = subprocess.run(
            'gcloud iam service-accounts list',
            shell=True,
            stdout=subprocess.PIPE
        )
        df = pd.read_fwf(
            io.BytesIO(proc.stdout),
            index_col=0
        )
        return df['EMAIL']['Compute Engine default service account']

    @staticmethod
    def personalize_worker_image(project: typing.Optional[str] = None):
        if project is None:
            project = get_default_gcp_project()
        print(crayons.green("Checking current base images...", bold=True))
        worker_images = gce.images().list(project='broad-cga-aarong-gtex', filter='family = canine-tgcp-worker').execute()
        if 'items' not in worker_images or len(worker_images['items']) < 1:
            raise ValueError("No public worker images found")
        worker_image = sorted(
            worker_images['items'],
            key=lambda img:datetime.datetime.strptime(img['creationTimestamp'], TIMESTAMP_FORMAT),
            reverse=True
        )[0]
        key = hashlib.md5((worker_image['name']+worker_image['labelFingerprint']).encode()).hexdigest()[:6]
        personalized_image_name = 'canine-tgcp-{}'.format(key)
        print(crayons.green("Checking for personalized version of latest base image...", bold=True))
        try:
            gce.images().get(project=project, image=personalized_image_name).execute()
            return personalized_image_name
        except googleapiclient.errors.HttpError as e:
            if e.args[0]['status'] != '404':
                raise
            print("No personalized image found, or the template image has been updated")
            print("Please wait while the new worker image is personalized")
            print("This may take a few minutes...")
            gce.instances().insert(
                project=project,
                zone='us-central1-a',
                body={
                    'description': 'Temporary instance to personalize the Canine TGCP worker image',
                    'name': 'canine-tgcp-worker-personalizer',
                    'machineType': 'zones/us-central1-a/machineTypes/f1-micro',
                    'disks': [
                        {
                            'boot': True,
                            'initializeParams': {
                                'diskSizeGb': '30',
                                'diskType': 'zones/us-central1-a/diskTypes/pd-standard',
                                'sourceImage': worker_image['selfLink']
                            },
                            'autoDelete': True
                        }
                    ],
                    "canIpForward": False,
                    "networkInterfaces": [
                        {
                            'subnetwork': 'regions/us-central1/subnetworks/default',
                            'accessConfigs': [
                                {
                                    'name': 'External NAT',
                                    'type': 'ONE_TO_ONE_NAT',
                                    'networkTier': 'STANDARD'
                                }
                            ]
                        }
                    ]
                }
            ).execute()
            print(crayons.green("Starting personalizer instance...", bold=True))
            time.sleep(45)
            try:
                print(crayons.green("Running personalization script...", bold=True))
                subprocess.check_call(
                    'gcloud compute ssh canine-tgcp-worker-personalizer --zone us-central1-a -- bash /opt/canine/personalize_worker.sh',
                    shell=True
                )
                time.sleep(10)
                # Running from CLI is easier, because it will block until stopped
                print(crayons.green("Stopping instance...", bold=True))
                subprocess.check_call(
                    'gcloud compute instances stop canine-tgcp-worker-personalizer --zone us-central1-a',
                    shell=True
                )
                print(crayons.green("Generating image...", bold=True))
                # gce.images().insert(
                #     project=project,
                #     body={
                #         'name': personalized_image_name,
                #         'description': 'Personalized version of the Canine TransientGCP worker image',
                #         'sourceDisk': 'zones/us-central1-a/disks/canine-tgcp-worker-personalizer',
                #         'family': 'canine-tgcp-worker-personalized'
                #     }
                # ).execute()
                # Again, CLI is better here because blocking
                subprocess.check_call(
                    'gcloud compute images create {name} --project {project}'
                    ' --description "{description}" --source-disk-zone {zone}'
                    ' --source-disk {instance} --family {family}'.format(
                        name=personalized_image_name,
                        description='Personalized version of the Canine TransientGCP worker image',
                        instance='canine-tgcp-worker-personalizer',
                        family='canine-tgcp-worker-personalized',
                        project=project,
                        zone='us-central1-a'
                    ),
                    shell=True
                )
                time.sleep(10)
            finally:
                print(crayons.green("Deleting instance...", bold=True))
                gce.instances().delete(
                    project=project,
                    zone='us-central1-a',
                    instance='canine-tgcp-worker-personalizer'
                ).execute()


    def __init__(
        self, name: str = 'slurm-canine', *, nodes_per_tier: int = 10, compute_zone: str = 'us-central1-a',
        controller_type: str = 'n1-standard-4', preemptible: bool = True, external_ip: bool = False,
        controller_disk_size: int = 200, gpu_type: typing.Optional[str] = None, gpu_count: int = 0,
        compute_script: str = "", controller_script: str = "", secondary_disk_size: int = 0, project: typing.Optional[str]  = None,
        **kwargs : typing.Any
    ):
        self.project = project if project is not None else get_default_gcp_project()
        super().__init__('{}-controller.{}.{}'.format(
            name,
            compute_zone,
            self.project,
            **kwargs
        ))
        self.zone = compute_zone
        self.controller_type = controller_type
        self.controller_disk_size = controller_disk_size

        self.config = {
            'n_workers': nodes_per_tier,
            'cluster_name': name,
            'gpus': '0:0',
            'sec': str(secondary_disk_size) if secondary_disk_size > 0 else '-',
            'ip': '+' if external_ip else '-',
            'preempt': '+' if preemptible else '-',
            'worker_start': compute_script,
            'controller_start': controller_script,
        }

        if gpu_type is not None and gpu_count > 0:
            if gpu_type not in GPU_TYPES:
                raise ValueError("gpu_type must be one of {}".format(GPU_TYPES))
            self.config['gpus'] = '{}:{}'.format(gpu_type, gpu_count)


        subprocess.check_call(
            'touch ~/.ssh/google_compute_known_hosts',
            shell=True,
            executable='/bin/bash'
        )

    def __enter__(self):
        """
        Create NFS server
        Set up the NFS server and SLURM cluster
        Use default VPC
        """
        TransientGCPSlurmBackend.personalize_worker_image(self.project)
        disks = [
            {
                'boot': True,
                'initializeParams': {
                    'diskSizeGb': str(self.controller_disk_size),
                    'diskType': 'zones/{}/diskTypes/pd-standard'.format(
                        self.zone
                    ),
                    'sourceImage': TransientGCPSlurmBackend.get_controller_image()
                },
                'autoDelete': True
            }
        ]
        if self.config['sec'] != '-':
            print(crayons.red("Secondary disks currently not supported", bold=True))
        try:
            gce.instances().insert(
                project=self.project,
                zone=self.zone,
                body={
                    'description': 'Canine TransientGCPSlurmBackend controller instance',
                    'name': '{}-controller'.format(self.config['cluster_name']),
                    'machineType': 'zones/{}/machineTypes/{}'.format(
                        self.zone,
                        self.controller_type
                    ),
                    'disks': disks,
                    "canIpForward": False,
                    "networkInterfaces": [
                        {
                            'subnetwork': 'regions/{}/subnetworks/default'.format(
                                self.zone[:-2]
                            ),
                            'accessConfigs': [
                                {
                                    'name': 'External NAT',
                                    'type': 'ONE_TO_ONE_NAT',
                                    'networkTier': 'STANDARD'
                                }
                            ]
                        }
                    ],
                    'labels': {
                        'k9cluster': self.config['cluster_name'],
                        'canine': 'tgcp-controller'
                    },
                    'metadata': {
                        'items': [
                            {
                                'key': 'canine_conf_{}'.format(k),
                                'value': v
                            }
                            for k,v in self.config.items()
                        ] + [
                            {
                                'key': 'startup-script',
                                'value': "#!/bin/bash\npython3 /apps/slurm/scripts/controller_start.py > /apps/slurm/controller_start.stdout 2> /apps/slurm/controller_start.stderr"
                            },
                            {
                                'key': 'canine_conf_user',
                                'value': getpass.getuser()
                            }
                        ]
                    },
                    'serviceAccounts': [
                        {
                            'email': TransientGCPSlurmBackend.get_compute_service_account(),
                            'scopes': [
                                'https://www.googleapis.com/auth/compute',
                                'https://www.googleapis.com/auth/cloud-platform'
                            ]
                        }
                    ]
                }
            ).execute()
            print(crayons.green("Starting controller instance...", bold=True))
            time.sleep(45)
            subprocess.check_call(
                'gcloud compute config-ssh --project {}'.format(self.project),
                shell=True
            )
            subprocess.check_call(
                'gcloud compute os-login ssh-keys add --key-file '
                '~/.ssh/google_compute_engine.pub --project {} >/dev/null'.format(
                    self.project
                ),
                shell=True
            )
            self.load_config_args()
            print(crayons.green("Connecting to controller instance...", bold=True))
            time.sleep(45) # Key propagation time
            super().__enter__()
            print("Waiting for slurm to initialize")
            rc, sout, serr = self.invoke("which sinfo")
            while rc != 0:
                time.sleep(10)
                rc, sout, serr = self.invoke("which sinfo")
            # time.sleep(60)
            print("Slurm controller is ready. Please call .wait_for_cluster_ready() to wait until the slurm compute nodes are ready to accept work")
            return self
        except:
            self.stop()
            raise

    def stop(self):
        """
        Kills the slurm cluster
        """
        subprocess.check_call(
            'gcloud compute config-ssh --remove',
            shell=True
        )
        instances = gce.instances().list(
            project=self.project,
            zone=self.zone,
            filter='labels.k9cluster = "{}"'.format(self.config['cluster_name'])
        ).execute()['items']
        for instance in instances:
            print(crayons.red("Deleting instance:"), instance['name'])
            gce.instances().delete(
                project=self.project,
                zone=self.zone,
                instance=instance['name']
            ).execute()

    def __exit__(self, *args):
        """
        kill NFS server
        delete the deployment
        """
        super().__exit__()
        self.stop()

    def wait_for_cluster_ready(self):
        """
        Blocks until the main partition is marked as up
        """
        super().wait_for_cluster_ready(elastic = False)

    def estimate_cost(self, clock_uptime: typing.Optional[float] = None, node_uptime: typing.Optional[float] = None, job_cpu_time: typing.Optional[typing.Dict[str, float]] = None) -> typing.Tuple[float, typing.Optional[typing.Dict[str, float]]]:
        """
        Returns a cost estimate for the cluster, based on any cost information available
        to the backend. May provide total node uptime (for cluster cost estimate)
        and/or cpu_time for each job to get job specific cost estimates.
        Clock uptime may be provided and is useful if the cluster has an inherrant
        overhead for uptime (ie: controller nodes).
        Note: Job cost estimates may not sum up to the total cluster cost if the
        cluster was not at full utilization.
        All units are in hours
        """
        cluster_cost = 0
        worker_cpu_cost = 0
        job_cost = None
        if node_uptime is not None:
            worker_info = {
                'mtype': self.config['compute_machine_type'],
                'preemptible': self.config['preemptible_bursting']
            }
            if self.config['compute_disk_type'] == 'pd-ssd':
                worker_info['ssd_size'] = self.config['compute_disk_size_gb']
            else:
                worker_info['hdd_size'] = self.config['compute_disk_size_gb']
            if 'gpu_type' in self.config and 'gpu_count' in self.config and self.config['gpu_count'] > 0:
                worker_info['gpu_type'] = self.config['gpu_type']
                worker_info['gpu_count'] = self.config['gpu_count']
            worker_hourly_cost = gcp_hourly_cost(**worker_info)
            cluster_cost += node_uptime * worker_hourly_cost
            mtype_prefix = self.config['compute_machine_type'][:3]
            if mtype_prefix in {'f1-', 'g1-'}:
                ncpus = 1
            elif mtype_prefix == 'cus': # n1-custom-X
                ncpus = int(self.config['compute_machine_type'].split('-')[1])
            else:
                ncpus = int(self.config['compute_machine_type'].split('-')[2])
            # Approximates the cost burden / CPU hour of the VM
            worker_cpu_cost = worker_hourly_cost / ncpus
        if clock_uptime is not None:
            controller_info = {
                'mtype': self.config['controller_machine_type'],
                'preemptible': False,
            }
            if self.config['controller_disk_type'] == 'pd-ssd':
                controller_info['ssd_size'] = self.config['controller_disk_size_gb']
            else:
                controller_info['hdd_size'] = self.config['controller_disk_size_gb']
            if 'controller_secondary_disk_size_gb' in self.config:
                if 'hdd_size' in controller_info:
                    controller_info['hdd_size'] += self.config['controller_secondary_disk_size_gb']
                elif self.config['controller_secondary_disk_size_gb'] > 0:
                    controller_info['hdd_size'] = self.config['controller_secondary_disk_size_gb']
            cluster_cost += clock_uptime * gcp_hourly_cost(**controller_info)
        if job_cpu_time is not None:
            job_cost = {
                job_id: worker_cpu_cost * cpu_time
                for job_id, cpu_time in job_cpu_time.items()
            }
        return cluster_cost, job_cost
