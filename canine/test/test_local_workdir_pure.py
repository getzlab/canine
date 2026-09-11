"""
Pure unit tests for the worker-local job workdir.

Jobs run in a local directory instead of the shared mount, and their outputs are
pushed to a results bucket at teardown. Two reasons, and both matter:

  * The controller stops being an I/O bottleneck -- previously every output byte
    was written to an NFS-exported disk on the controller.
  * Tasks get a real POSIX filesystem. A read-write gcsfuse workspace cannot
    serve as a general job workdir: bedGraphToBigWig fseek()s backwards to patch
    its header, and pyflow (so Strelka2 and Manta) appends in place to its state
    files on every task-state transition, which gcsfuse turns into a full object
    re-download plus re-upload.

The mutual-exclusion and required-bucket checks are not defensive noise: each
maps to a specific silent-wrong-behaviour failure, noted on the test.
"""
from unittest.mock import MagicMock

import pytest

from canine.localization.local import BatchedLocalizer

COMPUTE_ENV = {"CANINE_JOBS": "/mnt/nfs/ns/run/task/jobs"}


def make_localizer(**kwargs):
    return BatchedLocalizer(MagicMock(), **kwargs)


class TestWorkspaceLocation:

    def test_defaults_to_the_shared_mount(self):
        """Unchanged behaviour for anyone who has not opted in."""
        loc = make_localizer()
        assert loc.job_workspace_path(COMPUTE_ENV, "0") == \
            "/mnt/nfs/ns/run/task/jobs/0/workspace"

    def test_local_workdir_is_off_the_shared_mount(self):
        loc = make_localizer(workdir_mode="local", results_bucket="gs://b")
        path = loc.job_workspace_path(COMPUTE_ENV, "0")
        assert path == "/mnt/local_workdir/0/workspace"
        assert not path.startswith(COMPUTE_ENV["CANINE_JOBS"])

    def test_local_workdir_root_is_configurable(self):
        loc = make_localizer(
          workdir_mode="local", results_bucket="gs://b", local_workdir_root="/scratch"
        )
        assert loc.job_workspace_path(COMPUTE_ENV, "7") == "/scratch/7/workspace"

    def test_each_shard_gets_its_own_directory(self):
        """Shards run concurrently on one worker; they must not share a workdir."""
        loc = make_localizer(workdir_mode="local", results_bucket="gs://b")
        assert loc.job_workspace_path(COMPUTE_ENV, "0") != \
            loc.job_workspace_path(COMPUTE_ENV, "1")

    def test_scratch_disk_still_wins_over_the_shared_mount(self):
        loc = make_localizer(use_scratch_disk=True, scratch_disk_name="s")
        assert loc.job_workspace_path(COMPUTE_ENV, "0", "/mnt/canine-scratch-x") == \
            "/mnt/canine-scratch-x"


class TestConstructorGuards:

    def test_local_workdir_without_results_bucket_raises(self):
        """
        Outputs would otherwise be copied back onto the shared mount by
        delocalization.py's same_volume() branch -- silently reintroducing the
        exact controller round-trip this mode removes.
        """
        with pytest.raises(ValueError, match="results_bucket"):
            make_localizer(workdir_mode="local")

    def test_local_workdir_with_scratch_disk_raises(self):
        """Both redirect CANINE_JOB_WORKSPACE; one would silently win."""
        with pytest.raises(ValueError, match="mutually exclusive"):
            make_localizer(
              workdir_mode="local", results_bucket="gs://b",
              use_scratch_disk=True, scratch_disk_name="s",
            )

    def test_results_bucket_alone_is_harmless(self):
        """Setting the bucket without opting in must not change anything."""
        loc = make_localizer(results_bucket="gs://b")
        assert loc.job_workspace_path(COMPUTE_ENV, "0") == \
            "/mnt/nfs/ns/run/task/jobs/0/workspace"


class TestTempDir:
    """
    Java tools (GATK4, Picard, MuTect1) honour --tmp-dir / TMP_DIR /
    -Djava.io.tmpdir inconsistently, so all three channels are set. Tools whose
    temp location defaults to sit beside their *output* rather than $TMPDIR --
    `samtools sort -T`, `vcf2maf --tmp-dir` -- still have to be passed
    explicitly by the task; nothing here can fix those.
    """

    def test_no_tmpdir_override_by_default(self):
        """
        On the shared mount the long-standing behaviour is to leave TMPDIR alone
        (tools spill to the container's own /tmp, already local disk). Emitting
        nothing keeps that exactly as it was.
        """
        assert make_localizer().tmpdir_exports("0") == []

    def test_tmpdir_points_at_local_disk(self):
        loc = make_localizer(workdir_mode="local", results_bucket="gs://b")
        lines = loc.tmpdir_exports("0")
        assert 'export TMPDIR="/mnt/local_workdir/0/tmp"' in lines
        assert "mkdir -p $TMPDIR" in lines

    def test_tmpdir_is_not_on_the_shared_mount(self):
        loc = make_localizer(workdir_mode="local", results_bucket="gs://b")
        tmpdir_line = next(l for l in loc.tmpdir_exports("0") if l.startswith("export TMPDIR="))
        assert COMPUTE_ENV["CANINE_JOBS"] not in tmpdir_line

    def test_all_three_java_channels_are_set(self):
        """
        GATK does not read TMPDIR at all, and Picard's --TMP_DIR is ignored in
        some code paths and silently falls back to /tmp, so setting only one of
        these leaves a tool spilling somewhere unintended.
        """
        lines = make_localizer(
          workdir_mode="local", results_bucket="gs://b"
        ).tmpdir_exports("0")
        joined = "\n".join(lines)
        assert "export TMP=" in joined
        assert "export TEMP=" in joined
        assert "-Djava.io.tmpdir=$TMPDIR" in joined

    def test_java_options_preserves_any_existing_value(self):
        """Clobbering _JAVA_OPTIONS would drop heap settings the image sets."""
        lines = make_localizer(
          workdir_mode="local", results_bucket="gs://b"
        ).tmpdir_exports("0")
        java = next(l for l in lines if "_JAVA_OPTIONS" in l)
        assert "${_JAVA_OPTIONS:-}" in java

    def test_tmpdir_is_per_shard(self):
        loc = make_localizer(workdir_mode="local", results_bucket="gs://b")
        assert loc.tmpdir_exports("0") != loc.tmpdir_exports("1")
