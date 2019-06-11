
from .remote import RemoteSlurmBackend

class TransientGCPSlurmBackend(RemoteSlurmBackend):
    """
    Backend for transient slurm clusters which need to be deployed and configured
    on GCP before they're ready for use
    """
    pass
