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

    def test_mount_is_serialised_by_a_lock(self, tmp_path):
        """
        The mountpoint is per BUCKET, so every shard on a node shares it. Without
        a lock two shards starting together both see "not mounted", both run
        gcsfuse, and the loser dies with "mountPoint is not empty".
        """
        script = "\n".join(
          make(workdir_mode="bucket", results_bucket=BUCKET,
               staging_dir=str(tmp_path)).workdir_mount_script()
        )
        assert "flock" in script
        assert script.index("flock") < script.index("gcsfuse")

    def test_mount_clears_a_stale_fuse_endpoint(self, tmp_path):
        """
        A shard that finished on this node moments ago may have unmounted this
        same path. Operations on the stale endpoint fail with ENOTCONN/EACCES,
        not ENOENT, so mkdir and gcsfuse both fail unless it is cleared first.
        This mountpoint is torn down at every job teardown, so it is more exposed
        to this than the read-only consumer mounts.
        """
        script = "\n".join(
          make(workdir_mode="bucket", results_bucket=BUCKET,
               staging_dir=str(tmp_path)).workdir_mount_script()
        )
        assert "fusermount -uz" in script or "umount -l" in script
        assert script.index("fusermount -uz") < script.index("gcsfuse")

    def test_mount_failure_requeues_rather_than_kills_the_shard(self, tmp_path):
        """
        exit 5 is canine's "requeue this shard elsewhere" signal. exit 1 would
        just fail it. Mount failures (stale endpoint, races, quota) are transient.
        """
        lines = make(workdir_mode="bucket", results_bucket=BUCKET,
                     staging_dir=str(tmp_path)).workdir_mount_script()
        gcsfuse = next(l for l in lines if "timeout" in l and "gcsfuse" in l)
        assert "exit 5" in gcsfuse and "exit 1" not in gcsfuse

    def test_mount_holds_a_busy_lock_for_the_job(self, tmp_path):
        """Another shard's teardown must not unmount this job's workspace."""
        script = "\n".join(
          make(workdir_mode="bucket", results_bucket=BUCKET,
               staging_dir=str(tmp_path)).workdir_mount_script()
        )
        assert "flock -os" in script
        assert ".workdirmount_lock_pids" in script

    def test_unmount_releases_the_lock_then_checks_for_other_holders(self, tmp_path):
        script = "\n".join(
          make(workdir_mode="bucket", results_bucket=BUCKET,
               staging_dir=str(tmp_path)).workdir_unmount_script()
        )
        assert ".workdirmount_lock_pids" in script
        assert "flock -n" in script
        # release our share before testing whether anyone else holds one
        assert script.index(".workdirmount_lock_pids") < script.index("flock -n")

    def test_unmount_waits_for_the_lock_holder_to_actually_die(self, tmp_path):
        """
        Regression, seen live: kill() only *sends* the signal, so testing flock -n
        immediately afterwards makes a job see its own still-held lock and skip its
        own unmount -- every single time, even with no other shard on the node.
        The symptom was 'leaving workdir mount in place (still in use by another
        shard)' on a single-shard task.
        """
        script = "\n".join(
          make(workdir_mode="bucket", results_bucket=BUCKET,
               staging_dir=str(tmp_path)).workdir_unmount_script()
        )
        assert "kill -0" in script
        assert script.index("kill -0") < script.index("flock -n")

    def test_unmount_failure_is_not_fatal(self, tmp_path):
        """
        Objects are already flushed by close(); a busy mountpoint must not fail an
        otherwise-successful shard, which is what `exit 1` here used to do.
        """
        script = "\n".join(
          make(workdir_mode="bucket", results_bucket=BUCKET,
               staging_dir=str(tmp_path)).workdir_unmount_script()
        )
        assert "WARNING" in script
        assert "exit 1" not in script

    def test_mount_creates_the_workspace(self, tmp_path):
        """
        setup.sh cannot mkdir it -- the mount does not exist yet at that point --
        so the mount script owns creating it.
        """
        script = "\n".join(
          make(workdir_mode="bucket", results_bucket=BUCKET,
               staging_dir=str(tmp_path)).workdir_mount_script()
        )
        assert "mkdir -p $CANINE_JOB_WORKSPACE" in script

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
