"""
Pure unit tests for AbstractLocalizer.bucketmount_reachability_check().

These execute the generated bash against a real temporary directory with real
symlinks, because the thing being tested *is* shell behaviour -- glob matching
in [[ ]], readlink, and `-e` following a symlink into a directory that may or
may not contain the target. Asserting on the string would test nothing.
"""
import os
import shutil
import subprocess
from unittest.mock import MagicMock

import pytest

from canine.localization.local import BatchedLocalizer


BASH = shutil.which("bash")
pytestmark = pytest.mark.skipif(BASH is None, reason="bash not available")


def run_check(tmp_path, links, mount_name="wolf-1-us-central1-abc"):
    """
    Lay out an inputs dir and a fake mount, then run the generated check.

    links: {symlink name: target path relative to the mount dir, or None to
            point somewhere outside the mount entirely}
    Files are created under the mount only when `present` says so.
    """
    inputs = tmp_path / "inputs"
    mount = tmp_path / "mnt" / mount_name
    inputs.mkdir(parents=True)
    mount.mkdir(parents=True)

    for name, (rel, present) in links.items():
        if rel is None:
            target = tmp_path / "elsewhere" / name
            target.parent.mkdir(exist_ok=True)
            target.write_text("x")
        else:
            target = mount / rel
            if present:
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text("x")
        os.symlink(str(target), str(inputs / name))

    loc = BatchedLocalizer(MagicMock())
    script = "\n".join([
        "set -e",
        'CANINE_JOB_INPUTS="{}"'.format(inputs),
        'CANINE_BUCKETMOUNT="{}"'.format(mount_name),
        'CANINE_BUCKETMOUNT_DIR="{}"'.format(mount),
    ] + loc.bucketmount_reachability_check())
    return subprocess.run([BASH], input=script.encode(), capture_output=True)


class TestReachabilityCheck:

    def test_passes_when_every_link_resolves(self, tmp_path):
        proc = run_check(tmp_path, {
            "reads.bam": ("filename/reads.bam", True),
            "index.bai": ("filename/index.bai", True),
        })
        assert proc.returncode == 0, proc.stderr.decode()

    def test_exits_5_on_a_broken_link_into_the_mount(self, tmp_path):
        """The mounted-but-empty case: link points into the mount, target absent."""
        proc = run_check(tmp_path, {"reads.bam": ("filename/reads.bam", False)})
        assert proc.returncode == 5
        assert "does not resolve inside bucket mount" in proc.stderr.decode()

    def test_names_the_offending_link(self, tmp_path):
        proc = run_check(tmp_path, {"reads.bam": ("filename/reads.bam", False)})
        assert "reads.bam" in proc.stderr.decode()

    def test_one_broken_among_several_still_fails(self, tmp_path):
        proc = run_check(tmp_path, {
            "a.bam": ("filename/a.bam", True),
            "b.bam": ("filename/b.bam", False),
            "c.bam": ("filename/c.bam", True),
        })
        assert proc.returncode == 5

    def test_ignores_broken_links_outside_this_mount(self, tmp_path):
        """
        Only links into the bucket currently being mounted are this loop's
        business -- another iteration handles the others, and a task's own
        dangling symlink is not our problem.
        """
        inputs = tmp_path / "inputs"
        mount = tmp_path / "mnt" / "wolf-1-us-central1-abc"
        inputs.mkdir(parents=True)
        mount.mkdir(parents=True)
        os.symlink(str(tmp_path / "nowhere" / "x.bam"), str(inputs / "unrelated.bam"))

        loc = BatchedLocalizer(MagicMock())
        script = "\n".join([
            "set -e",
            'CANINE_JOB_INPUTS="{}"'.format(inputs),
            'CANINE_BUCKETMOUNT="wolf-1-us-central1-abc"',
            'CANINE_BUCKETMOUNT_DIR="{}"'.format(mount),
        ] + loc.bucketmount_reachability_check())
        proc = subprocess.run([BASH], input=script.encode(), capture_output=True)
        assert proc.returncode == 0, proc.stderr.decode()

    def test_no_links_at_all_is_fine(self, tmp_path):
        proc = run_check(tmp_path, {})
        assert proc.returncode == 0, proc.stderr.decode()

    def test_survives_a_missing_inputs_dir(self, tmp_path):
        """find's failure is swallowed; the loop must not abort under set -e."""
        loc = BatchedLocalizer(MagicMock())
        script = "\n".join([
            "set -e",
            'CANINE_JOB_INPUTS="{}/does-not-exist"'.format(tmp_path),
            'CANINE_BUCKETMOUNT="b"',
            'CANINE_BUCKETMOUNT_DIR="{}/mnt"'.format(tmp_path),
        ] + loc.bucketmount_reachability_check())
        proc = subprocess.run([BASH], input=script.encode(), capture_output=True)
        assert proc.returncode == 0, proc.stderr.decode()

    def test_directory_input_resolves(self, tmp_path):
        """An is_dir input symlinks to a directory inside the mount."""
        inputs = tmp_path / "inputs"
        mount = tmp_path / "mnt" / "wolf-1-us-central1-abc"
        (mount / "refdir" / "sub").mkdir(parents=True)
        inputs.mkdir(parents=True)
        os.symlink(str(mount / "refdir"), str(inputs / "refdir"))

        loc = BatchedLocalizer(MagicMock())
        script = "\n".join([
            "set -e",
            'CANINE_JOB_INPUTS="{}"'.format(inputs),
            'CANINE_BUCKETMOUNT="wolf-1-us-central1-abc"',
            'CANINE_BUCKETMOUNT_DIR="{}"'.format(mount),
        ] + loc.bucketmount_reachability_check())
        proc = subprocess.run([BASH], input=script.encode(), capture_output=True)
        assert proc.returncode == 0, proc.stderr.decode()


class TestMountLeases:
    """
    Consumers register a lease object in the bucket while mounted, so
    DeleteLocalizedFiles can refuse to delete content another worker is reading.
    GCS cannot report who has a bucket gcsfuse-mounted, so the bucket itself is
    the only registry every VM can see.
    """

    def _register(self):
        return "\n".join(BatchedLocalizer(MagicMock()).bucketmount_lease_register())

    def _release(self):
        return "\n".join(BatchedLocalizer(MagicMock()).bucketmount_lease_release())

    def test_lease_is_written_into_the_bucket(self):
        """The bucket is the only registry every VM can see."""
        assert "_MOUNTS/" in self._register()
        assert "storage cp" in self._register()

    def test_lease_is_unique_across_vms_and_array_shards(self):
        """
        Two jobs sharing a lease means one's teardown frees content the other is
        reading. hostname separates VMs; job id + array task separate shards
        co-scheduled on one VM.
        """
        reg = self._register()
        assert "$(hostname)" in reg
        assert "SLURM_JOB_ID" in reg
        assert "SLURM_ARRAY_TASK_ID" in reg

    def test_release_removes_exact_urls_never_a_pattern(self):
        """
        Teardown must free only its own leases. Deleting by prefix or wildcard
        would free every other VM's lease on the same shared bucket.
        """
        rel = self._release()
        assert "_MOUNTS/**" not in rel and "_MOUNTS/*" not in rel
        assert '"${CANINE_LEASE}"' in rel, "must remove the recorded URL verbatim"

    def test_lease_is_taken_even_when_reusing_an_existing_mount(self):
        """
        A job landing on a node where the bucket is already mounted does no
        mounting itself, but is still a consumer. If registration sat inside the
        "mount if not already mounted" block it would take no lease, and the
        first job's teardown would free content it is still reading.
        """
        import inspect
        from canine.localization import base
        src = inspect.getsource(base.AbstractLocalizer.job_setup_teardown)
        conditional_mount = src.index("if ! mountpoint -q ${CANINE_BUCKETMOUNT_DIR}")
        # the unconditional post-mount assertion marks the end of that block
        after_block = src.index("Bucket mount did not appear")
        registration = src.index("bucketmount_lease_register()")
        assert conditional_mount < after_block < registration

    def test_lease_carries_custom_time(self):
        """Without it, a lease orphaned by a crashed job blocks deletion forever."""
        assert "--custom-time" in self._register()

    def test_lease_write_failure_is_non_fatal(self):
        """Bookkeeping must not kill a workflow; losing it costs a re-localization."""
        line = [l for l in self._register().splitlines() if "storage cp" in l][0]
        assert "||" in line and "WARNING" in line

    def test_release_removes_every_lease_this_job_took(self):
        rel = self._release()
        assert ".bucketmount_leases" in rel
        assert "storage rm" in rel
        assert "rm -f" in rel, "the tracking file itself must be cleaned up"

    def test_release_tolerates_an_already_gone_lease(self):
        line = [l for l in self._release().splitlines() if "storage rm" in l][0]
        assert "||" in line

    def test_generated_shell_is_valid(self):
        for script in (self._register(), self._release()):
            proc = subprocess.run([BASH, "-n"], input=("set -e\n" + script).encode(),
                                  capture_output=True)
            assert proc.returncode == 0, proc.stderr.decode()


class TestMountHeartbeat:
    """
    customTime is otherwise stamped once at mount and never renewed, so a job
    holding a mount longer than localization_expiry_days loses its own lease AND
    has its data objects deleted by the lifecycle rule while gcsfuse still has
    them mounted. The heartbeat re-stamps both for as long as the mount is held.
    """

    def _start(self, seconds=3600):
        loc = BatchedLocalizer(MagicMock(), bucketmount_heartbeat_seconds=seconds)
        return "\n".join(loc.bucketmount_heartbeat_start())

    def _stop(self):
        return "\n".join(BatchedLocalizer(MagicMock()).bucketmount_heartbeat_stop())

    def test_refresh_covers_the_whole_bucket(self):
        """One update must cover the _MOUNTS/ lease and the data objects alike."""
        s = self._start()
        assert "objects update" in s
        assert '/**' in s, "must be bucket-wide, not just the lease"
        assert "--custom-time=" in s

    def test_bucket_is_passed_as_an_argument_not_inherited(self):
        """
        Regression: the loop runs in a child bash. CANINE_BUCKETMOUNT is a plain
        shell variable, never exported, so referencing it inside the quoted
        heredoc made the child update "gs:///**".
        """
        s = self._start()
        assert 'gs://$1/**' in s
        assert '"${CANINE_BUCKETMOUNT}"' in s.splitlines()[-1], "bucket must be passed to the script"

    def test_timestamp_is_evaluated_per_beat(self):
        """
        A quoted heredoc keeps $(date) literal so each beat stamps 'now'. An
        unquoted one would freeze the write-time value and renew nothing.
        """
        s = self._start()
        assert "<<'CANINE_HEARTBEAT_EOF'" in s
        assert '$(date -u' in s

    def test_interval_is_configurable(self):
        assert "sleep 60" in self._start(seconds=60)
        assert "sleep 3600" in self._start()

    def test_refresh_failure_is_non_fatal(self):
        line = [l for l in self._start().splitlines() if "objects update" in l][0]
        assert "|| :" in line

    def test_pid_is_recorded_for_teardown(self):
        assert ".bucketmount_heartbeat_pids" in self._start()

    def test_stop_kills_recorded_pids(self):
        s = self._stop()
        assert "kill $pid" in s
        assert ".bucketmount_heartbeat_pids" in s
        assert "rm -f" in s

    def test_interval_must_sit_inside_the_expiry_window(self):
        """A beat slower than expiry renews nothing; fail at construction."""
        with pytest.raises(ValueError) as e:
            BatchedLocalizer(MagicMock(), localization_expiry_days=1,
                             bucketmount_heartbeat_seconds=86400)
        assert "expiry window" in str(e.value)

    def test_default_interval_is_valid_for_the_default_expiry(self):
        BatchedLocalizer(MagicMock())  # must not raise

    def test_generated_shell_is_valid(self):
        for script in (self._start(), self._stop()):
            proc = subprocess.run([BASH, "-n"], input=("set -e\n" + script).encode(),
                                  capture_output=True)
            assert proc.returncode == 0, proc.stderr.decode()
