from importlib.metadata import version
from .orchestrator import Orchestrator
from .localization import BatchedLocalizer, LocalLocalizer
from . import cost

__version__ = version("canine")
