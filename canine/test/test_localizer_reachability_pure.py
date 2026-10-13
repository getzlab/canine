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


class TestMountLogWiring:
    """
    gcsfuse logs beside the mountpoint; bucketmount_check.py (tested in
    test_bucketmount_check_pure.py) reads what it logged while the job ran.
    """

    def _src(self):
        import inspect
        from canine.localization import base
        return inspect.getsource(base.AbstractLocalizer.job_setup_teardown)

    def test_mark_is_taken_even_when_reusing_an_existing_mount(self):
        """Same reason as the lease: a job reusing a mount reads through it too."""
        src = self._src()
        conditional_mount = src.index("if ! mountpoint -q ${CANINE_BUCKETMOUNT_DIR}")
        after_block = src.index("Bucket mount did not appear")
        mark = src.index("bucketmount_log_mark()")
        assert conditional_mount < after_block < mark
        assert src.index("bucketmount_log_report(") > mark

    def test_gcsfuse_writes_the_log(self):
        line = [l for l in self._src().splitlines() if "gcsfuse -o ro" in l][0]
        assert "--log-file ${CANINE_BUCKETMOUNT_LOG}" in line
        assert "--log-severity warning" in line
        # slurm moves the daemon out of the job's cgroup by matching this
        assert line.index("${CANINE_BUCKETMOUNT_DIR} 200>&-") > line.index("--log-format")

    def test_teardown_only_reports(self):
        """A failed job never reached the entrypoint's check; teardown must not change its outcome."""
        report = "\n".join(BatchedLocalizer.bucketmount_log_report("/root"))
        assert "/root/bucketmount_check.py ${CANINE_JOB_INPUTS} --report-only || :" in report

    def test_generated_shell_is_valid(self):
        loc = BatchedLocalizer(MagicMock())
        for script in (loc.bucketmount_log_mark(), loc.bucketmount_log_report("/root")):
            proc = subprocess.run([BASH, "-n"], input=("set -e\n" + "\n".join(script)).encode(),
                                  capture_output=True)
            assert proc.returncode == 0, proc.stderr.decode()


class TestTheMountCanWriteItsLog:
    """
    Daemonized, gcsfuse creates <log-file>.stderr beside its log. The log sat in the
    mountpoints' directory, which is root's, so every mount failed with "open
    ....gcsfuse.log.stderr: permission denied" -- on the first real runs, not on a test VM
    whose log was in the home directory.

    Runs canine's own commands, from the log path through the gcsfuse call, with the
    mountpoints' directory unwritable except through sudo, and a gcsfuse that writes
    beside its log the way the real one does.
    """

    @staticmethod
    def _mount_commands():
        import ast
        import inspect
        from canine.localization import base
        import warnings
        src = inspect.getsource(base.AbstractLocalizer.job_setup_teardown)
        with warnings.catch_warnings():
            # base.py's shell strings carry escapes like "\$" that Python warns about
            warnings.simplefilter("ignore", SyntaxWarning)
            tree = ast.parse("if 1:\n" + src)
        strings = [n.value for n in ast.walk(tree)
                   if isinstance(n, ast.Constant) and isinstance(n.value, str)]
        start = next(i for i, s in enumerate(strings) if s.startswith("CANINE_BUCKETMOUNT_LOGDIR="))
        end = next(i for i, s in enumerate(strings) if "gcsfuse -o ro" in s)
        return [s for s in strings[start:end + 1]
                if s.startswith(("CANINE_BUCKETMOUNT_LOG", "sudo mkdir -p ${CANINE_BUCKETMOUNT_LOGDIR}",
                                 "touch ${CANINE_BUCKETMOUNT_LOG}", "timeout -k 60 60 gcsfuse"))]

    def test_gcsfuse_can_create_its_stderr_file_beside_the_log(self, tmp_path):
        import os
        import stat
        mounts = tmp_path / "bucketmounts"
        (mounts / "wolf-1-us-east1-abc").mkdir(parents=True)
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        # sudo: may write in the mountpoints' directory, as root may
        (bin_dir / "sudo").write_text(
            '#!/bin/bash\nchmod u+w {m}; "$@"; rc=$?; chmod u-w {m}; exit $rc\n'.format(m=mounts))
        (bin_dir / "timeout").write_text('#!/bin/bash\nshift 3; exec "$@"\n')   # -k 60 60
        (bin_dir / "gcsfuse").write_text(
            '#!/bin/bash\nwhile [ $# -gt 0 ]; do [ "$1" = --log-file ] && log=$2; shift; done\n'
            ': > "$log.stderr" || { echo "open $log.stderr: permission denied" >&2; exit 1; }\n')
        for name in ("sudo", "timeout", "gcsfuse"):
            os.chmod(str(bin_dir / name), 0o755)
        os.chmod(str(mounts), stat.S_IRUSR | stat.S_IXUSR)       # root's, as far as we are concerned
        try:
            script = "\n".join(["set -e",
                                'export PATH="{}:$PATH"'.format(bin_dir),
                                'CANINE_BUCKETMOUNT_DIR="{}/wolf-1-us-east1-abc"'.format(mounts),
                                "CANINE_BUCKETMOUNT_BUCKET=wolf-1-us-east1-abc"]
                               + self._mount_commands() + ['echo "LOG=$CANINE_BUCKETMOUNT_LOG"'])
            proc = subprocess.run([BASH], input=script, capture_output=True, text=True)
        finally:
            os.chmod(str(mounts), 0o755)
        assert proc.returncode == 0, proc.stderr
        log = proc.stdout.strip().split("LOG=", 1)[1]
        assert os.path.exists(log + ".stderr")
        assert os.path.dirname(log) != str(mounts), "the log must not sit in root's directory"
