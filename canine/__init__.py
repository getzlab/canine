from importlib.metadata import version
from .orchestrator import Orchestrator
from .localization import BatchedLocalizer, LocalLocalizer

__version__ = version("canine")
