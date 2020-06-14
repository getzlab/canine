"""
canine.backends
=================================
Contains SLURM backends for canine
"""

from .base import AbstractTransport, AbstractSlurmBackend
from .local import LocalTransport, LocalSlurmBackend
from .remote import RemoteTransport, RemoteSlurmBackend
from .dummy import DummyTransport, DummySlurmBackend
from .gcpTransient import TransientGCPSlurmBackend
from .imageTransient import TransientImageSlurmBackend
from .dockerTransient import DockerTransientImageSlurmBackend, LocalDockerSlurmBackend

__all__ = [
    'LocalSlurmBackend',
    'LocalTransport',
    'RemoteSlurmBackend',
    'RemoteTransport',
    'DummySlurmBackend',
    'DummyTransport',
    'TransientGCPSlurmBackend',
    'TransientImageSlurmBackend',
    'DockerTransientImageSlurmBackend',
    'LocalDockerSlurmBackend'
]
