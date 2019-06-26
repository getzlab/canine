import typing
import abc
from collections import namedtuple
from ..backends import AbstractSlurmBackend

FSAction = namedtuple(
    'FSAction',
    ['type', 'path', 'data']
)

class AbstractTransaction(abc.ABC):
    """
    Represents the base class for all Localization transactions.
    Localizer should delegate all actions through a transaction object.
    """

    def __init__(self, backend: AbstractSlurmBackend):
        """
        Constructs a new Transaction object

        Args:
            backend: The SLURM backend to use. Needed to issue transports and to
                invoke commands
        """
        self.backend = backend
        self._transport = None
        self.actions = []

    def __enter__(self):
        """
        Begins transaction. Actions are buffered through transport
