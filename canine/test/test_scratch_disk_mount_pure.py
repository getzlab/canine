"""
A scratch disk resumed after a preemption is mounted with its ext4 journal still
to replay. On a hard-clipping shard's 133 GB disk (2026-11-06) that took longer
than the 30 s the mount was allowed: the timeout gave up (exit 124) while the
kernel went on to mount the disk, and the job failed. The mount now has
SCRATCH_DISK_MOUNT_TIMEOUT_S, and one that finishes after the timeout counts.
"""
import shutil
import subprocess
from unittest.mock import MagicMock, patch

import google.api_core.exceptions
import pytest

from canine.localization import base
from canine.localization.nfs import NFSLocalizer

BASH = shutil.which("bash")


def scratch_disk_script():
    localizer = object.__new__(NFSLocalizer)
    localizer.protect_disk = False
    disk_client = MagicMock()
    disk_client.get.side_effect = google.api_core.exceptions.NotFound("no disk")
    with patch.object(base, "get_default_gcp_zone", return_value = "us-central1-a"), \
         patch.object(base, "gcloud_disk_client", return_value = disk_client):
        _, script, _, _ = localizer.create_persistent_disk(disk_name = "abc")
    return script


def mount_line():
    line, = [l for l in scratch_disk_script() if " mount -o discard,defaults " in l]
    return line


def test_the_mount_has_time_to_replay_the_journal():
    assert base.SCRATCH_DISK_MOUNT_TIMEOUT_S == 900
    assert mount_line().startswith("sudo timeout -k 30 900 mount -o discard,defaults /dev/disk/by-id/google-\"${GCP_DISK_NAME}\" ")


@pytest.mark.skipif(BASH is None, reason = "bash needed")
@pytest.mark.parametrize("mounted, ok", [(True, True), (False, False)])
def test_a_mount_that_finishes_after_the_timeout_counts(tmp_path, mounted, ok):
    """The timeout exits 124; whether the disk ended up mounted decides."""
    tools = { "sudo" : 'exec "$@"', "timeout" : "exit 124", "mountpoint" : "exit {}".format(0 if mounted else 1) }
    for tool, body in tools.items():
        (tmp_path / tool).write_text("#!/bin/bash\n" + body + "\n")
        (tmp_path / tool).chmod(0o755)
    script = "\n".join(["set -e", 'export PATH="{}:$PATH"'.format(tmp_path),
                        "GCP_DISK_NAME=abc GCP_TSNT_DISKS_DIR=/mnt/disks", mount_line(), "echo continued"])
    proc = subprocess.run([BASH], input = script, capture_output = True, text = True)
    assert (proc.returncode == 0 and proc.stdout.strip() == "continued") == ok
