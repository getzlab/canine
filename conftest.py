"""
Project-level pytest conftest for canine.

Stubs out packages that are either:
  - non-installable locally (slurm_gcp_docker runs `docker pull` at install time)
  - require live infrastructure credentials to install (dalmatian/firecloud needs Terra)

This allows pure unit tests to run without the full canine installation.
The integration/backend tests still require the full environment.
"""
import sys
from unittest.mock import MagicMock

_STUB_MODULES = [
    # slurm_gcp_docker runs `docker pull` in its setup.py — can't install locally
    "slurm_gcp_docker",
    # dalmatian / firecloud-dalmatian requires Terra account setup to install
    "dalmatian",
    "firecloud_dalmatian",
    "firecloud",
    "FISS",
]

for _mod in _STUB_MODULES:
    if _mod not in sys.modules:
        sys.modules[_mod] = MagicMock()
