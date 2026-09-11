"""
Pure unit tests for the two orthogonal workspace options.

  workdir_mode  -- where computation runs: "shared" (NFS staging dir), "local" (a
                   worker-local disk), or "bucket" (the results bucket, gcsfuse-
                   mounted read-write).
  save_intermediates -- whether the WHOLE workspace is copied to the bucket at
                   teardown, not just declared outputs.

These exist because the local-disk design loses debuggability: intermediates die
with the worker, so a task that failed or produced something suspicious leaves
nothing to inspect. The shared-NFS layout used to preserve them.

The invariant most worth protecting here is that TMPDIR stays on local disk in
*every* mode. Spill files from bcftools/GNU sort/GATK/Picard are written, seeked
and deleted in place -- the access pattern gcsfuse handles worst -- so routing
them through FUSE would be the single largest risk in bucket mode.
"""
from unittest.mock import MagicMock

import pytest

from canine.localization.base import WORKDIR_MOUNT_ROOT, BUCKETMOUNT_ROOT
from canine.localization.local import BatchedLocalizer

COMPUTE_ENV = {"CANINE_JOBS": "/mnt/nfs/ns/run/task/jobs"}
BUCKET = "wolf-123456789012-us-central1-ns"


def make(**kwargs):
    return BatchedLocalizer(MagicMock(), **kwargs)


class TestModeValidation:

    def test_default_is_shared(self):
        assert make().workdir_mode == "shared"

    def test_unknown_mode_raises(self):
        with pytest.raises(ValueError, match="workdir_mode must be one of"):
            make(workdir_mode="bucketmount", results_bucket=BUCKET)

    @pytest.mark.parametrize("mode", ["local", "bucket"])
    def test_non_shared_mode_requires_a_bucket(self, mode):
        with pytest.raises(ValueError, match="requires results_bucket"):
            make(workdir_mode=mode)

    @pytest.mark.parametrize("mode", ["local", "bucket"])
    def test_scratch_disk_is_mutually_exclusive(self, mode):
        with pytest.raises(ValueError, match="mutually exclusive"):
            make(workdir_mode=mode, results_bucket=BUCKET,
                 use_scratch_disk=True, scratch_disk_name="s")


class TestBucketModeWorkspace:

    def test_workspace_is_inside_the_rw_mount(self, tmp_path):
        loc = make(workdir_mode="bucket", results_bucket=BUCKET, staging_dir=str(tmp_path))
        path = loc.job_workspace_path(COMPUTE_ENV, "0")
        assert path.startswith("{}/{}/".format(WORKDIR_MOUNT_ROOT, BUCKET))
        assert path.endswith("/0/workspace")

    def test_rw_mount_is_not_under_the_readonly_root(self, tmp_path):
        """
        The read-only consumer mounts use a 'mount if not already mounted, no
        backoff' loop that is only safe because nothing writable shares the tree.
        """
        loc = make(workdir_mode="bucket", results_bucket=BUCKET, staging_dir=str(tmp_path))
        assert not loc.workdir_mount_dir().startswith(BUCKETMOUNT_ROOT)

    def test_shards_do_not_collide(self, tmp_path):
        loc = make(workdir_mode="bucket", results_bucket=BUCKET, staging_dir=str(tmp_path))
        assert loc.job_workspace_path(COMPUTE_ENV, "0") != \
            loc.job_workspace_path(COMPUTE_ENV, "1")

    def test_mount_script_is_read_write(self, tmp_path):
        loc = make(workdir_mode="bucket", results_bucket=BUCKET, staging_dir=str(tmp_path))
        script = "\n".join(loc.workdir_mount_script())
        assert "gcsfuse" in script
        assert "-o ro" not in script

    def test_mount_script_chowns_the_mountpoint(self, tmp_path):
        """fusermount3 refuses a mountpoint the invoking user cannot write."""
        script = "\n".join(
          make(workdir_mode="bucket", results_bucket=BUCKET,
               staging_dir=str(tmp_path)).workdir_mount_script()
        )
        assert "chown" in script
        assert script.index("chown") < script.index("gcsfuse")

    def test_mount_script_pins_adc(self, tmp_path):
        """gcsfuse resolves credentials via ADC and ignores CLOUDSDK_CONFIG."""
        script = "\n".join(
          make(workdir_mode="bucket", results_bucket=BUCKET,
               staging_dir=str(tmp_path)).workdir_mount_script()
        )
        assert "GOOGLE_APPLICATION_CREDENTIALS" in script

    def test_unmount_script_leaves_the_directory_first(self, tmp_path):
        """Unmounting while cwd is inside the mount fails with EBUSY."""
        script = "\n".join(
          make(workdir_mode="bucket", results_bucket=BUCKET,
               staging_dir=str(tmp_path)).workdir_unmount_script()
        )
        assert "cd /" in script
        assert script.index("cd /") < script.index("fusermount -u")

    @pytest.mark.parametrize("mode", ["shared", "local"])
    def test_no_mount_scripts_outside_bucket_mode(self, mode, tmp_path):
        kwargs = {"results_bucket": BUCKET} if mode != "shared" else {}
        loc = make(workdir_mode=mode, staging_dir=str(tmp_path), **kwargs)
        assert loc.workdir_mount_script() == []
        assert loc.workdir_unmount_script() == []


class TestTmpdirStaysLocal:
    """The invariant: never route spill files through FUSE."""

    @pytest.mark.parametrize("mode", ["local", "bucket"])
    def test_tmpdir_is_on_local_disk(self, mode, tmp_path):
        loc = make(workdir_mode=mode, results_bucket=BUCKET, staging_dir=str(tmp_path))
        line = next(l for l in loc.tmpdir_exports("0") if l.startswith("export TMPDIR="))
        assert "/mnt/local_workdir/0/tmp" in line

    def test_bucket_mode_tmpdir_is_not_in_the_mount(self, tmp_path):
        loc = make(workdir_mode="bucket", results_bucket=BUCKET, staging_dir=str(tmp_path))
        line = next(l for l in loc.tmpdir_exports("0") if l.startswith("export TMPDIR="))
        assert WORKDIR_MOUNT_ROOT not in line
        # ... even though the workspace itself is
        assert WORKDIR_MOUNT_ROOT in loc.job_workspace_path(COMPUTE_ENV, "0")

    def test_shared_mode_leaves_tmpdir_alone(self):
        assert make().tmpdir_exports("0") == []


class TestSaveIntermediates:

    def test_defaults_off(self):
        assert make().save_intermediates is False

    def test_allowed_with_local(self, tmp_path):
        loc = make(workdir_mode="local", results_bucket=BUCKET,
                   save_intermediates=True, staging_dir=str(tmp_path))
        assert loc.save_intermediates is True

    @pytest.mark.parametrize("mode", ["shared", "bucket"])
    def test_rejected_outside_local(self, mode, tmp_path):
        """
        Silently ignoring it would leave someone believing their intermediates
        were preserved when they were not.
        """
        kwargs = {"results_bucket": BUCKET} if mode != "shared" else {}
        with pytest.raises(ValueError, match="save_intermediates only applies"):
            make(workdir_mode=mode, save_intermediates=True,
                 staging_dir=str(tmp_path), **kwargs)
