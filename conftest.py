"""
Project-level pytest conftest for canine.

Stubs out packages that are either:
  - non-installable locally (slurm_gcp_docker runs `docker pull` at install time)
  - require live infrastructure credentials to install (dalmatian/firecloud needs Terra)

This allows pure unit tests to run without the full canine installation.
The integration/backend tests still require the full environment.
"""
import sys
import types
from unittest.mock import MagicMock

_STUB_MODULES = [
    # dalmatian / firecloud-dalmatian requires Terra account setup to install
    "dalmatian",
    "firecloud_dalmatian",
    "firecloud",
    "FISS",
]

for _mod in _STUB_MODULES:
    if _mod not in sys.modules:
        sys.modules[_mod] = MagicMock()

# slurm_gcp_docker runs `docker pull` in its setup.py, so it can't just be pip
# installed locally -- but it also needs to be a real (if empty) package, not a
# bare MagicMock: canine.backends.dockerTransient does `from slurm_gcp_docker.
# test_controller_environment import check_all`, and Python's import machinery
# can't resolve a submodule import through a MagicMock stand-in -- it requires
# an actual package with __path__.
if "slurm_gcp_docker" not in sys.modules:
    _slurm_gcp_docker = types.ModuleType("slurm_gcp_docker")
    _slurm_gcp_docker.__path__ = []
    sys.modules["slurm_gcp_docker"] = _slurm_gcp_docker

    _test_controller_environment = types.ModuleType("slurm_gcp_docker.test_controller_environment")
    _test_controller_environment.check_all = MagicMock()
    sys.modules["slurm_gcp_docker.test_controller_environment"] = _test_controller_environment
