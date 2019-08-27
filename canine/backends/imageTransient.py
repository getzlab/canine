import typing
from functools import lru_cache
import subprocess
import tempfile
from io import BytesIO
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


class TransientImageSlurmBackend(RemoteSlurmBackend):
    """
    Backend for starting a slurm cluster using a preconfigured GCP image.
    The image must meet the following requirements:
    * Slurm is installed and configured (Compute node configuration not necessary)
    * If gpus are added, drivers must already be installed
    * An external NFS server must be present and /etc/fstab must be set up on the image to mount the server
    """

    def __init__(
        self, image: str, name: str = 'slurm-canine', *, max_node_count: int = 10, compute_zone: str = 'us-central1-a',
        controller_type: str = 'n1-standard-16', worker_type: str = 'n1-highcpu-2', compute_disk_size: int = 20,
        controller_disk_size: int = 200, gpu_type: typing.Optional[str] = None, gpu_count: int = 0,
        compute_script: str = "", controller_script: str = "", secondary_disk_size: int = 0, project: typing.Optional[str]  = None,
    ):
        # GresTypes=gpu
        # NodeName=X CPUs=X RealMemory=X State=CLOUD Gres=gpu:X
