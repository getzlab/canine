"""
bucketmount_check.py: fail a job when gcsfuse failed a read of one of its inputs.

A tool can exit 0 after a read through a bucket mount failed: getdRangerSupportingReads
did after pysam's "IOError: truncated file", and its half-finished output failed a later
task on every retry. The log lines here are gcsfuse 3.11.2's, from failing reads on a
real mount (getzlab-wolf-develop, 2026-10-11): deleting or replacing an object mid-read
fails the read with ESTALE and logs these ERRORs, while blocking the network for up to
six minutes only logs retry WARNINGs, and the read completes intact.

Each test lays out a job: inputs symlinked into a mount directory, a gcsfuse log beside
it, the record written by the localizer's real mark command, then runs the script as the
entrypoint does.
"""
import ast
import os
import shutil
import subprocess
import sys
from unittest.mock import MagicMock

import pytest

from canine.localization.base import AbstractLocalizer
from canine.localization.local import BatchedLocalizer

BASH = shutil.which("bash")
pytestmark = pytest.mark.skipif(BASH is None, reason="bash not available")

SCRIPT = AbstractLocalizer.staged_script_source("bucketmount_check.py")
BUCKET = "wolf-1-us-east1-abc"


def read_error(obj):
    return ('time="11/10/2026 02:28:12.940027" severity=ERROR message="ReadFile: stale file '
            'handle, fh.readManager.ReadAt: startRead: The file \\"{}\\" was modified or '
            'deleted by another process, possibly due to concurrent modification: NewReader: '
            'NewReaderWithReadHandle: gcs.NotFoundError: storage: object doesn\'t exist" '
            'mount-id=x\n').format(obj)

OP_ERROR = ('time="11/10/2026 02:28:12.941019" severity=ERROR message="fuse: Op 0x000070a0 '
            '*fuseops.ReadFileOp] -> Error: \\"stale file handle\\"" mount-id=x\n')
RETRY = ('time="11/10/2026 02:33:34.012466" severity=WARNING message="Retrying  for \\"\\": '
         'InvocationID: 5d67ce0d, Attempt: 2, due to error: context deadline exceeded" '
         'mount-id=x\n')


class Job:
    def __init__(self, tmp_path, inputs, before=""):
        """inputs: {link name: object path in the bucket}; before: log content at the mark."""
        self.inputs = tmp_path / "inputs"
        self.mount = tmp_path / "bucketmounts" / BUCKET
        self.log = tmp_path / "bucketmounts" / ".{}.gcsfuse.log".format(BUCKET)
        self.inputs.mkdir()
        self.mount.mkdir(parents=True)
        for name, obj in inputs.items():
            os.symlink(str(self.mount / obj), str(self.inputs / name))
        if before is not None:
            self.log.write_text(before)
        proc = subprocess.run([BASH], capture_output=True, input="\n".join([
            "set -e",
            'CANINE_JOB_INPUTS="{}"'.format(self.inputs),
            'CANINE_BUCKETMOUNT="{}"'.format(BUCKET),
            'CANINE_BUCKETMOUNT_DIR="{}"'.format(self.mount),
            'CANINE_BUCKETMOUNT_LOG="{}"'.format(self.log),
        ] + BatchedLocalizer(MagicMock()).bucketmount_log_mark()).encode())
        assert proc.returncode == 0, proc.stderr.decode()

    def logs(self, text):
        with open(self.log, "a") as fh:
            fh.write(text)

    def check(self, *args):
        return subprocess.run([sys.executable, SCRIPT, str(self.inputs)] + list(args),
                              capture_output=True, text=True)


class TestFailing:

    def test_a_failed_read_of_an_input_fails_the_job(self, tmp_path):
        job = Job(tmp_path, {"n.bam": "n_bam/normal.bam"})
        job.logs(read_error("n_bam/normal.bam") + OP_ERROR)
        proc = job.check()
        assert proc.returncode == 3
        assert "gs://{}/n_bam/normal.bam".format(BUCKET) in proc.stderr

    def test_a_file_inside_a_directory_input_counts(self, tmp_path):
        job = Job(tmp_path, {"refs": "refs"})
        job.logs(read_error("refs/sub/genome.fa"))
        assert job.check().returncode == 3

    def test_each_failed_input_is_named_once(self, tmp_path):
        job = Job(tmp_path, {"n.bam": "n_bam/normal.bam"})
        job.logs(read_error("n_bam/normal.bam") * 3)
        assert job.check().stderr.count("ERROR: gcsfuse failed a read") == 1


class TestNotFailing:

    def test_another_jobs_object_is_only_reported(self, tmp_path):
        """The mount is shared by every job on the node."""
        job = Job(tmp_path, {"n.bam": "n_bam/normal.bam"})
        job.logs(read_error("t_bam/someone_else.bam"))
        proc = job.check()
        assert proc.returncode == 0
        assert "someone_else.bam" in proc.stderr

    def test_a_name_that_only_starts_with_an_inputs_name_does_not_count(self, tmp_path):
        job = Job(tmp_path, {"n.bam": "n_bam/normal.bam"})
        job.logs(read_error("n_bam/normal.bam.bai"))
        assert job.check().returncode == 0

    def test_retry_warnings_from_a_network_outage_are_only_reported(self, tmp_path):
        """gcsfuse rode out a six-minute outage, and the read completed intact."""
        job = Job(tmp_path, {"n.bam": "n_bam/normal.bam"})
        job.logs(RETRY * 7)
        proc = job.check()
        assert proc.returncode == 0
        assert "context deadline exceeded" in proc.stderr

    def test_a_warning_naming_an_input_is_only_reported(self, tmp_path):
        """gcsfuse's retry line has a slot for the object, empty in the outage seen."""
        job = Job(tmp_path, {"n.bam": "n_bam/normal.bam"})
        job.logs(RETRY.replace('for \\"\\"', 'for \\"n_bam/normal.bam\\"'))
        proc = job.check()
        assert proc.returncode == 0
        assert "n_bam/normal.bam" in proc.stderr

    def test_an_error_logged_before_the_job_started_is_not_its_own(self, tmp_path):
        job = Job(tmp_path, {"n.bam": "n_bam/normal.bam"}, before=read_error("n_bam/normal.bam"))
        proc = job.check()
        assert proc.returncode == 0 and proc.stderr == ""

    def test_report_only_never_fails(self, tmp_path):
        job = Job(tmp_path, {"n.bam": "n_bam/normal.bam"})
        job.logs(read_error("n_bam/normal.bam"))
        proc = job.check("--report-only")
        assert proc.returncode == 0
        assert "gcsfuse failed a read" in proc.stderr

    def test_quiet_when_nothing_was_logged(self, tmp_path):
        job = Job(tmp_path, {"n.bam": "n_bam/normal.bam"})
        proc = job.check()
        assert proc.returncode == 0 and proc.stderr == ""

    def test_a_mount_without_a_log_is_skipped(self, tmp_path):
        """A mount made by a canine that predates the logging has no log file."""
        job = Job(tmp_path, {"n.bam": "n_bam/normal.bam"}, before=None)
        proc = job.check()
        assert proc.returncode == 0 and proc.stderr == ""

    def test_no_record_at_all(self, tmp_path):
        (tmp_path / "inputs").mkdir()
        proc = subprocess.run([sys.executable, SCRIPT, str(tmp_path / "inputs")],
                              capture_output=True, text=True)
        assert proc.returncode == 0 and proc.stderr == ""


class TestReporting:

    def test_a_rotated_log_is_read_from_its_start(self, tmp_path):
        job = Job(tmp_path, {"n.bam": "n_bam/normal.bam"}, before="x" * 5000 + "\n")
        job.log.write_text(read_error("n_bam/normal.bam"))
        assert job.check().returncode == 3

    def test_the_record_is_consumed(self, tmp_path):
        """The entrypoint's check and teardown's report must not both print it."""
        job = Job(tmp_path, {"n.bam": "n_bam/normal.bam"})
        job.logs(RETRY)
        assert job.check().stderr
        assert not (job.inputs / ".bucketmount_logs").exists()
        assert job.check().stderr == ""

    def test_output_is_bounded_but_every_line_is_checked(self, tmp_path):
        job = Job(tmp_path, {"n.bam": "n_bam/normal.bam"})
        job.logs(RETRY * 1000 + read_error("n_bam/normal.bam"))
        proc = job.check()
        assert proc.returncode == 3
        assert len(proc.stderr.splitlines()) < 210

    def test_a_record_without_the_mount_dir_is_reported_but_never_fails(self, tmp_path):
        """Written by the first version of the mark, which recorded no mount directory."""
        job = Job(tmp_path, {"n.bam": "n_bam/normal.bam"})
        record = job.inputs / ".bucketmount_logs"
        record.write_text(" ".join(record.read_text().split()[:3]) + "\n")
        job.logs(read_error("n_bam/normal.bam"))
        proc = job.check()
        assert proc.returncode == 0 and "stale file handle" in proc.stderr


class TestTheScript:

    def test_imports_only_the_standard_library(self):
        """It runs as `python3 <path>` on the node, outside canine's environment."""
        tree = ast.parse(open(SCRIPT).read())
        modules = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import)
                   for a in n.names}
        modules |= {n.module.split(".")[0] for n in ast.walk(tree)
                    if isinstance(n, ast.ImportFrom)}
        assert modules <= set(sys.stdlib_module_names), modules

    def test_is_run_as_python3_with_no_shebang(self):
        with open(SCRIPT) as fh:
            assert not fh.readline().startswith("#!")


class TestTheEntrypoint:

    def _entrypoint(self):
        from canine.orchestrator import ENTRYPOINT
        return ENTRYPOINT

    def test_checks_only_after_the_tool_succeeds_and_before_success_is_declared(self):
        e = self._entrypoint()
        rc = e.index("export CANINE_JOB_RC=$?")
        check = e.index("bucketmount_check.py $CANINE_JOB_INPUTS")
        success = e.index("JOB COMPLETED SUCCESSFULLY")
        assert rc < check < success
        assert "[ $CANINE_JOB_RC == 0 ] && [ -f $CANINE_JOB_INPUTS/.bucketmount_logs ]" in e

    def _run(self, tmp_path, check_rc):
        """Run the entrypoint's check lines with a stand-in python3 exiting check_rc."""
        e = self._entrypoint()
        start = e.index("    # a tool can exit 0 after gcsfuse")
        end = e.index("    if [ $CANINE_JOB_RC == 0 ]; then", start)
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        (bin_dir / "python3").write_text("#!/bin/bash\nexit {}\n".format(check_rc))
        os.chmod(str(bin_dir / "python3"), 0o755)
        (tmp_path / ".bucketmount_logs").write_text("x\n")
        script = "\n".join([
            'export PATH="{}:$PATH"'.format(bin_dir),
            'CANINE_JOB_INPUTS="{}"'.format(tmp_path), "CANINE_ROOT=/nowhere",
            "export CANINE_JOB_RC=0", e[start:end], "echo $CANINE_JOB_RC"])
        return subprocess.run([BASH], input=script, capture_output=True, text=True).stdout.strip()

    def test_a_failed_read_becomes_a_failed_job(self, tmp_path):
        assert self._run(tmp_path, 3) == "74"

    def test_a_clean_check_leaves_the_job_alone(self, tmp_path):
        assert self._run(tmp_path, 0) == "0"

    @pytest.mark.parametrize("check_rc", [1, 2])
    def test_the_check_failing_for_any_other_reason_does_not_fail_the_job(self, tmp_path, check_rc):
        """A staged copy missing (2) or the script crashing (1) is not a failed read."""
        assert self._run(tmp_path, check_rc) == "0"
