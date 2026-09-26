"""
Project-level pytest conftest for canine.

Stubs out packages that are either:
  - non-installable locally (slurm_gcp_docker runs `docker pull` at install time)
  - require live infrastructure credentials to install (dalmatian/firecloud needs Terra)

This allows pure unit tests to run without the full canine installation.
The integration/backend tests still require the full environment.
"""
import os
import sys
import types
from unittest.mock import MagicMock

# Emitted localization commands run the parallel downloader as `python3`, whichever one
# is first on PATH -- on a worker, the image's, which has canine's dependencies (among
# them google_crc32c, which the downloader imports). Put the interpreter running the tests
# first, so the commands tests execute get the same: a python3 with this venv's packages,
# not whatever the invoking shell happens to have.
os.environ["PATH"] = os.path.dirname(sys.executable) + os.pathsep + os.environ.get("PATH", "")

_STUB_MODULES = [
    # NOT slurm_gcp_docker -- see the real-module stub below. Both branches hit the
    # same ModuleNotFoundError and fixed it differently: this branch registered the
    # parent and the submodule as two separate MagicMocks, fuse-localize built a real
    # package with a __path__. Either works alone, but listing it here ALSO defeats the
    # other one, because `if "slurm_gcp_docker" not in sys.modules` is then false and
    # the real stub never installs.
    # dalmatian / firecloud-dalmatian requires Terra account setup to install
    "dalmatian",
    "firecloud_dalmatian",
    "firecloud",
    "FISS",
]

for _mod in _STUB_MODULES:
    if _mod not in sys.modules:
        sys.modules[_mod] = MagicMock()

# slurm_gcp_docker needs to be a real (if empty) package, not a bare MagicMock:
# canine.backends.dockerTransient does `from slurm_gcp_docker.test_controller_environment
# import check_all`, and Python's import machinery can't resolve a submodule import
# through a MagicMock stand-in -- it requires an actual package with __path__.
# (slurm_gcp_docker also runs `docker pull` in its own setup.py, so it can't just be
# pip-installed locally either.)
if "slurm_gcp_docker" not in sys.modules:
    _slurm_gcp_docker = types.ModuleType("slurm_gcp_docker")
    _slurm_gcp_docker.__path__ = []
    sys.modules["slurm_gcp_docker"] = _slurm_gcp_docker

    _test_controller_environment = types.ModuleType("slurm_gcp_docker.test_controller_environment")
    _test_controller_environment.check_all = MagicMock()
    sys.modules["slurm_gcp_docker.test_controller_environment"] = _test_controller_environment
