"""
A scratch disk records the job that wrote it, in a file at its root
(SCRATCH_DISK_PROVENANCE), so whatever later reads the disk can tell where
its files came from.
"""
import subprocess
from unittest.mock import MagicMock, patch

import google.api_core.exceptions

from canine.localization import base
from canine.localization.base import SCRATCH_DISK_PROVENANCE
from canine.localization.nfs import NFSLocalizer


def test_the_lines_record_the_job_root(tmp_path):
    disk = tmp_path / "canine-scratch-abc"
    disk.mkdir()
    lines = base.AbstractLocalizer._scratch_disk_provenance_lines('"$DISK"')
    job_root = "/mnt/nfs/ns/run1/MarkDups__2026-10-29--10-00-00_abc_def/jobs/3"
    subprocess.run(["bash", "-euc", "\n".join(lines)], check = True,
                   env = { "PATH" : "/usr/bin:/bin", "DISK" : str(disk), "CANINE_JOB_ROOT" : job_root })
    assert (disk / SCRATCH_DISK_PROVENANCE).read_text() == job_root + "\n"


def test_a_scratch_disk_records_it_once_mounted():
    localizer = object.__new__(NFSLocalizer)
    localizer.protect_disk = False
    disk_client = MagicMock()
    disk_client.get.side_effect = google.api_core.exceptions.NotFound("no disk")
    with patch.object(base, "get_default_gcp_zone", return_value = "us-central1-a"), \
         patch.object(base, "gcloud_disk_client", return_value = disk_client):
        _, script, _, _ = localizer.create_persistent_disk(disk_name = "abc")

    record = base.AbstractLocalizer._scratch_disk_provenance_lines('"$GCP_TSNT_DISKS_DIR/$GCP_DISK_NAME"')
    assert all(line in script for line in record)
    mount = next(i for i, line in enumerate(script) if line.startswith("sudo timeout -k 30 30 mount"))
    assert script.index(record[0]) > mount
