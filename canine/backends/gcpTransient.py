import getpass
import tempfile
import typing
import time
import subprocess
import shutil
import os
import sys
from .remote import RemoteSlurmBackend
from ..utils import get_default_gcp_project, ArgumentHelper, check_call
# import paramiko
import yaml
import pandas as pd

SLURM_PARTITION_RECON = b'slurm_load_partitions: Unable to contact slurm controller (connect failure)'
PARAMIKO_PEM_KEY = os.path.expanduser('~/.ssh/canine_pem_key')

# FIXME: allow custom startup and controller scripty bois

class TransientGCPSlurmBackend(RemoteSlurmBackend):
    """
    Backend for transient slurm clusters which need to be deployed and configured
    on GCP before they're ready for use
    """

    def __init__(self, name='slurm-canine', *, max_node_count=10, compute_zone='us-central1-a', controller_type='n1-standard-16', login_type='n1-standard-1', worker_type='n1-highcpu-2', login_count=0, compute_disk_size=20, controller_disk_size=200, gpu_type=None, gpu_count=0, compute_script="", controller_script=""):
        super().__init__('{}-controller.{}.{}'.format(
            name,
            compute_zone,
            get_default_gcp_project()
        ))
        self.config = {
          "cluster_name": name,
          "static_node_count": 0,
          "max_node_count": int(max_node_count),
          "zone": compute_zone,
          "region": compute_zone[:-2],
          "cidr": "10.10.0.0/16",
          "controller_machine_type": controller_type,
          "compute_machine_type": worker_type,
          "compute_disk_type": "pd-standard",
          "compute_disk_size_gb": int(compute_disk_size),
          "controller_disk_type": "pd-ssd",
          "controller_disk_size_gb": int(controller_disk_size),
          "controller_secondary_disk": False,
          "login_machine_type": login_type,
          "login_node_count": int(login_count),
          "login_disk_size_gb": 10,
          "preemptible_bursting": True,
          "private_google_access": False,
          "vpc_net": "default",
          "vpc_subnet": "default",
          "default_users": getpass.getuser()
        }

        if gpu_type is not None and gpu_count > 0:
            self.config['gpu_type'] = gpu_type
            self.config['gpu_count'] = gpu_count

        self.startup_script = """
        sudo yum install -y yum-utils device-mapper-persistent-data lvm2 libcgroup libcgroup-tools htop gcc python-devel python-setuptools redhat-rpm-config
        sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
        sudo yum install -y docker-ce
        sudo groupadd docker
        sudo usermod -aG docker {0}
        sudo systemctl enable docker.service
        sudo systemctl start docker.service
        sudo chown root:docker /var/run/docker.sock
        sudo bash -c "echo {0}$'\\t'ALL='(ALL:ALL)'$'\\t'ALL >> /etc/sudoers"
        sudo sed -e 's/GRUB_CMDLINE_LINUX="\\?\\([^"]*\\)"\\?/GRUB_CMDLINE_LINUX="\\1 cgroup_enable=memory swapaccount=1"/' < /etc/default/grub > grub.tmp
        sudo mv grub.tmp /etc/default/grub
        sudo grub2-mkconfig -o /etc/grub2.cfg
        yes | sudo pip uninstall crcmod
        sudo pip install --no-cache-dir -U crcmod
        {1}
        """.format(
            getpass.getuser(),
            compute_script
        )

        self.controller_script = """
        sudo dd if=/dev/zero of=/swapfile count=4096 bs=1MiB
        sudo chmod 700 /swapfile
        sudo mkswap /swapfile
        sudo swapon /swapfile
        sudo yum install -y gcc python-devel python-setuptools redhat-rpm-config htop
        yes | sudo pip uninstall crcmod
        sudo pip install --no-cache-dir -U crcmod
        {0}
        """.format(controller_script)

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
        # Idea: Allow the final deployment step to run in a Popen
        # cluster.run then polls the object to make sure the deployment has finished
        # allows copytree to begin while deployment still in progress

        # User may also provide an NFS server IP

        # 1) create NFS: (n1-highcpu-16 [16CPU, 14GB, 4GB SW], SSD w/ user GB)
        # 2) SCP setup script to NFS
        # 3) SSH into NFS, run script
        try:
            with tempfile.TemporaryDirectory() as tempdir:
                shutil.copytree(
                    os.path.join(
                        os.path.dirname(__file__),
                        'slurm-gcp'
                    ),
                    tempdir+'/slurm'
                )
                with open(os.path.join(tempdir, 'slurm', 'scripts', 'custom-compute-install'), 'w') as w:
                    w.write(self.startup_script)
                with open(os.path.join(tempdir, 'slurm', 'scripts', 'custom-controller-install'), 'w') as w:
                    w.write(self.controller_script)
                with open(os.path.join(tempdir, 'slurm', 'slurm-cluster.yaml'), 'w') as w:
                    yaml.dump(
                        {
                            "imports": [
                              {
                                "path": 'slurm.jinja'
                              }
                            ],
                            "resources": [
                              {
                                "name": "slurm-cluster",
                                "type": "slurm.jinja",
                                "properties": self.config
                              }
                            ]
                        },
                        w
                    )
                subprocess.check_call(
                    'gcloud deployment-manager deployments create {} --config {}'.format(
                        self.config['cluster_name'],
                        os.path.join(tempdir, 'slurm', 'slurm-cluster.yaml')
                    ),
                    shell=True,
                    executable='/bin/bash'
                )
            subprocess.check_call(
                'gcloud compute config-ssh',
                shell=True
            )
            subprocess.check_call(
                'gcloud compute os-login ssh-keys add --key-file ~/.ssh/google_compute_engine.pub >/dev/null',
                shell=True
            )
            self.load_config_args()
            time.sleep(30) # Key propagation time
            super().__enter__()
            print("Waiting for slurm to initialize")
            rc, sout, serr = self.invoke("which sinfo")
            while rc != 0:
                time.sleep(10)
                rc, sout, serr = self.invoke("which sinfo")
            time.sleep(60)
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
        subprocess.check_call(
            'echo y | gcloud deployment-manager deployments delete {}'.format(
                self.config['cluster_name']
            ),
            shell=True,
            executable='/bin/bash'
        )
        subprocess.run(
            "echo y | gcloud compute images delete $(gcloud compute images list --filter name:{}| awk 'NR>1 {{print $1}}')".format(
                self.config['cluster_name']
            ),
            shell=True,
            executable='/bin/bash'
        )
        subprocess.run(
            "echo y | gcloud compute instances delete --zone {} $(gcloud compute instances list --filter name:{} | awk 'NR>1 {{print $1}}')".format(
                self.config['zone'],
                self.config['cluster_name']
            ),
            shell=True,
            executable='/bin/bash'
        )

    def __exit__(self, *args):
        """
        kill NFS server
        delete the deployment
        """
        super().__exit__()
        self.stop()

    def sinfo(self, *slurmopts: str, **slurmparams: typing.Any) -> pd.DataFrame:
        """
        Shows the current cluster information
        slurmopts and slurmparams are passed into an ArgumentHelper and unpacked
        as command line arguments
        """
        command = 'sinfo'+ArgumentHelper(*slurmopts, **slurmparams).commandline
        status, stdout, stderr = self.invoke(command)
        if status != 0 and SLURM_PARTITION_RECON in stderr.read():
            print("Transient controller timed out while checking partitions. Allowing one additional retry", file=sys.stderr)
            time.sleep(120)
            status, stdout, stderr = self.invoke(command)
        stderr.seek(0,0)
        check_call(command, status, stdout, stderr)
        df = pd.read_fwf(
            stdout,
            index_col=0
        )
        df.index = df.index.map(str)
        return df


# ====
# Old code to get around openssh issues. May be needed in future if ssh-agent trick doesn't work
# @staticmethod
# def get_paramiko_acceptable_key():
#     """
#     Because, openssh keys are not accepted by paramiko,
#     but for some reason, it's the default format on OSx.
#     Returns the filename of a paramiko-accepted 2048-bit rsa key
#     You'll need to copy it over yourself or add it to os-login
#     """
#     if not os.path.isdir(os.path.dirname(PARAMIKO_PEM_KEY)):
#         os.path.makedirs(os.path.dirname(PARAMIKO_PEM_KEY))
#     if not (os.path.isfile(PARAMIKO_PEM_KEY) and os.path.isfile(PARAMIKO_PEM_KEY+'.pub')):
#         print("Generating new ssh keypair for canine/paramiko")
#         try:
#             subprocess.check_call(
#                 "ssh-keygen -t rsa -b 2048 -f {} -N '' -m PEM".format(PARAMIKO_PEM_KEY),
#                 shell=True
#             )
#         except CalledProcessError:
#             # probably fine. On non-openssh implementations "-m PEM" is not
#             # a valid argument. Try anyways
#             subprocess.check_call(
#                 "ssh-keygen -t rsa -b 2048 -f {} -N ''".format(PARAMIKO_PEM_KEY),
#                 shell=True
#             )
#     # Double-check that the key is acceptable
#     paramiko.RSAKey.from_private_key_file(PARAMIKO_PEM_KEY)
#     return PARAMIKO_PEM_KEY
# subprocess.check_call(
#     'gcloud compute --project {} ssh --zone {} {}-controller -- bash -c \'"if [[ ! -d .ssh ]]; then mkdir .ssh; fi"\''.format(
#         get_default_gcp_project(),
#         self.config['zone'],
#         self.config['cluster_name']
#     ),
#     shell=True,
#     executable='/bin/bash'
# )
# subprocess.check_call(
#     'gcloud compute --project {} ssh --zone {} {}-controller -- bash -c \'"cat - >> .ssh/authorized_keys"\' < {}.pub'.format(
#         get_default_gcp_project(),
#         self.config['zone'],
#         self.config['cluster_name'],
#         self.get_paramiko_acceptable_key()
#     ),
#     shell=True,
#     executable='/bin/bash'
# )
# subprocess.check_call(
#     'gcloud compute --project {} ssh --zone {} {}-controller -- bash -c \'"chmod 600 .ssh/authorized_keys"\''.format(
#         get_default_gcp_project(),
#         self.config['zone'],
#         self.config['cluster_name']
#     ),
#     shell=True,
#     executable='/bin/bash'
# )
# self._RemoteSlurmBackend__sshkwargs['key_filename'] = self.get_paramiko_acceptable_key()