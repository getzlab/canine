"""
Each disk canine attaches gets a 16 MiB read-ahead before it's mounted. At the
kernel's default of 128 KiB, a sequential read of a 100 GB file on a pd-standard
disk ran at 68 MB/s; at 16 MiB, 453 MB/s (2026-11-01).
"""
import inspect
import re
import shutil
import subprocess
from unittest.mock import MagicMock, patch

import google.api_core.exceptions
import pytest

from canine.localization import base
from canine.localization.nfs import NFSLocalizer


BASH = shutil.which("bash")


def test_the_read_ahead_is_16_mib():
    assert base.DISK_READ_AHEAD_SECTORS * 512 == 16 * 1024 * 1024


def test_the_line_sets_the_devices_read_ahead():
    assert base.read_ahead_line("/dev/sdb") == "sudo blockdev --setra 32768 /dev/sdb || true"


@pytest.mark.skipif(BASH is None, reason = "bash needed")
def test_failing_to_set_it_doesnt_fail_the_job(tmp_path):
    for tool in ("sudo", "blockdev"):
        (tmp_path / tool).write_text("#!/bin/bash\n" + ('exec "$@"\n' if tool == "sudo" else "exit 1\n"))
        (tmp_path / tool).chmod(0o755)
    script = "\n".join(["set -e", 'export PATH="{}:$PATH"'.format(tmp_path),
                        base.read_ahead_line("/dev/sdb"), "echo continued"])
    proc = subprocess.run([BASH], input = script, capture_output = True, text = True)
    assert proc.returncode == 0 and proc.stdout.strip() == "continued"


def test_a_scratch_disk_gets_it_before_it_is_mounted():
    """Scratch and cache disks share these lines."""
    localizer = object.__new__(NFSLocalizer)
    localizer.protect_disk = False
    disk_client = MagicMock()
    disk_client.get.side_effect = google.api_core.exceptions.NotFound("no disk")
    with patch.object(base, "get_default_gcp_zone", return_value = "us-central1-a"), \
         patch.object(base, "gcloud_disk_client", return_value = disk_client):
        _, script, _, _ = localizer.create_persistent_disk(disk_name = "abc")

    read_ahead = script.index(base.read_ahead_line('/dev/disk/by-id/google-"${GCP_DISK_NAME}"'))
    mount = next(i for i, line in enumerate(script) if line.startswith("sudo timeout -k 30 30 mount"))
    assert read_ahead < mount
    # outside the mount's if, so a disk already mounted by an earlier attempt gets it too
    assert script[mount - 1].startswith("if ! mountpoint") and read_ahead < mount - 1


def test_every_disk_mount_is_preceded_by_it():
    """Every place base.py mounts an attached disk sets its read-ahead first."""
    lines = inspect.getsource(base).splitlines()
    mounts = [i for i, line in enumerate(lines) if re.search(r"mount -o [^ ]+ /dev/disk/by-id/", line)]
    assert len(mounts) == 3  # scratch and cache disks, temporary download disks, read-only disks
    for i in mounts:
        before = "\n".join(lines[max(0, i - 12):i])
        assert "read_ahead_line(" in before, lines[i]
