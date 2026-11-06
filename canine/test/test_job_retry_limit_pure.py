"""
The job entrypoint's own retry of a failed job counts failures, not restarts.

A failed job is requeued at once, up to CANINE_RETRY_LIMIT, while the rest of its
array keeps running. The limit used to be checked against SLURM_RESTART_COUNT, which
counts preemptions too, so a shard preempted three times had no retries left: an
ApplyBQSR shard (2026-11-02) failed after a preemption and two requeues with "Retry
limit of 3 retries exceeded" after a single retry. Preemptions have their own limit,
CANINE_PREEMPT_LIMIT, which in turn no longer counts job failures. These run the
rendered entrypoint in bash, with the job's scripts and scontrol stubbed.
"""
import os
import shutil
import subprocess

import pytest

from canine.orchestrator import ENTRYPOINT

BASH = shutil.which("bash")
RETRY_LIMIT = 3


def run_entrypoint(tmp_path, job_rc, restarts, failures = 0, localization_failures = 0, last_attempt_ok = False,
                   localizer_rc = 0, slurm_ids = None):
    """
    Run one attempt of a job whose script exits job_rc, as its restarts-th restart,
    with failures and localization_failures recorded by earlier attempts, and the
    last attempt's localization and job recorded as succeeded if last_attempt_ok;
    its localization exits localizer_rc. slurm_ids are the job's Slurm IDs, by
    default task 0 of array 7 (job 8). A requeue ends the attempt, as Slurm's
    does. Returns (requeued, stderr, the failure count after).
    """
    slurm_ids = slurm_ids or { "SLURM_ARRAY_JOB_ID" : "7", "SLURM_ARRAY_TASK_ID" : "0", "SLURM_JOB_ID" : "8" }
    jobs = tmp_path / "jobs"
    root = jobs / slurm_ids["SLURM_ARRAY_TASK_ID"]
    root.mkdir(parents = True)
    (tmp_path / "ws").mkdir()
    (tmp_path / "bin").mkdir()
    if failures:
        (root / ".job_failure_count").write_text(str(failures))
    if localization_failures:
        (root / ".localization_failure_count").write_text(str(localization_failures))
    if last_attempt_ok:
        (root / ".localizer_exit_code").write_text("0")
        (root / ".job_exit_code").write_text("0")
    (root / "setup.sh").write_text("\n".join([
        "export CANINE_JOB_ROOT={}".format(root),
        "export CANINE_JOB_WORKSPACE={}".format(tmp_path / "ws"),
        "export CANINE_JOB_INPUTS={}".format(tmp_path / "ws"), ""]))
    scripts = {
        root / "localization.sh": "exit {}".format(localizer_rc),
        root / "teardown.sh": "exit 0",
        tmp_path / "script.sh": "exit {}".format(job_rc),
        # a requeue stops the running attempt
        tmp_path / "bin" / "scontrol": 'echo "$@" >> {}; kill -KILL $PPID'.format(tmp_path / "requeues"),
    }
    for path, body in scripts.items():
        path.write_text("#!/bin/bash\n" + body + "\n")
        os.chmod(str(path), 0o755)
    entrypoint = ENTRYPOINT.format(backend = "test", adapter = "test", retry_limit = RETRY_LIMIT,
                                   pipeline_script = tmp_path / "script.sh", CANINE_ROOT = tmp_path,
                                   CANINE_COMMON = tmp_path, CANINE_OUTPUT = tmp_path, CANINE_JOBS = jobs)
    env = { k : v for k, v in os.environ.items() if not k.startswith("SLURM_") }
    env.update(PATH = "{}:{}".format(tmp_path / "bin", os.environ["PATH"]), SLURM_RESTART_COUNT = str(restarts),
               **slurm_ids)
    proc = subprocess.run([BASH, "-c", entrypoint], env = env, capture_output = True, text = True)
    count = root / ".job_failure_count"
    return ((tmp_path / "requeues").exists(), proc.stderr,
            int(count.read_text()) if count.exists() else 0)


class TestFailedJobRetries:

    def test_a_first_failure_is_retried(self, tmp_path):
        requeued, err, failures = run_entrypoint(tmp_path, 3, restarts = 0)
        assert requeued and failures == 1
        assert "Retrying job (attempt 1/3)" in err

    def test_preemptions_dont_use_up_the_retries(self, tmp_path):
        """Seen live: preempted, then requeued twice, then a failure; it's still retried."""
        requeued, err, failures = run_entrypoint(tmp_path, 3, restarts = 3)
        assert requeued and failures == 1
        assert "Retry limit" not in err

    def test_the_last_retry_is_allowed(self, tmp_path):
        requeued, err, _ = run_entrypoint(tmp_path, 3, restarts = 2, failures = 2)
        assert requeued
        assert "Retrying job (attempt 3/3)" in err

    def test_failures_alone_still_stop_at_the_limit(self, tmp_path):
        requeued, err, failures = run_entrypoint(tmp_path, 3, restarts = 3, failures = 3)
        assert not requeued and failures == 4
        assert "Retry limit of 3 retries exceeded" in err

    def test_running_out_of_memory_is_not_retried(self, tmp_path):
        """wolF retries it with more memory instead."""
        requeued, err, _ = run_entrypoint(tmp_path, 137, restarts = 0)
        assert not requeued
        assert "ran out of memory" in err

    def test_success_is_not_retried(self, tmp_path):
        requeued, err, failures = run_entrypoint(tmp_path, 0, restarts = 2)
        assert not requeued and failures == 0
        assert "JOB COMPLETED SUCCESSFULLY" in err


class TestARequeueIsOfThisJobOnly:
    """
    The requeue names the job array's task, <array>_<task>. Slurm gives an
    array's last task the array's own ID as its SLURM_JOB_ID, so requeuing that
    ID requeued every task: a hard-clipping shard (2026-11-06), the last of 26,
    failed three times, and each retry restarted all 26.
    """

    LAST_TASK = { "SLURM_ARRAY_JOB_ID" : "149", "SLURM_ARRAY_TASK_ID" : "25", "SLURM_JOB_ID" : "149" }

    @staticmethod
    def requeued(tmp_path):
        return (tmp_path / "requeues").read_text().split()

    def test_a_failed_arrays_last_task_requeues_only_itself(self, tmp_path):
        run_entrypoint(tmp_path, 3, restarts = 0, slurm_ids = self.LAST_TASK)
        assert self.requeued(tmp_path) == ["requeue", "149_25"]

    def test_so_does_a_localization_to_retry(self, tmp_path):
        run_entrypoint(tmp_path, 0, restarts = 0, localizer_rc = 5, slurm_ids = self.LAST_TASK)
        assert self.requeued(tmp_path) == ["requeue", "149_25"]

    def test_another_task_requeues_itself(self, tmp_path):
        run_entrypoint(tmp_path, 3, restarts = 0)
        assert self.requeued(tmp_path) == ["requeue", "7_0"]

    def test_a_job_outside_an_array_requeues_its_own_id(self, tmp_path):
        run_entrypoint(tmp_path, 3, restarts = 0, slurm_ids = { "SLURM_ARRAY_TASK_ID" : "0", "SLURM_JOB_ID" : "42" })
        assert self.requeued(tmp_path) == ["requeue", "42"]


class TestThePreemptionLimit:
    """A job restarted CANINE_PREEMPT_LIMIT (5) times by preemption moves to nonpreemptible nodes (exit 123)."""

    def exits_for_preemption(self, tmp_path, **kw):
        # the move needs the last attempt's localization and job to have succeeded
        return "Preemption limit exceeded" in run_entrypoint(tmp_path, 0, last_attempt_ok = True, **kw)[1]

    def test_five_preemptions_move_the_job(self, tmp_path):
        assert self.exits_for_preemption(tmp_path, restarts = 5)

    def test_job_failures_arent_counted_as_preemptions(self, tmp_path):
        assert not self.exits_for_preemption(tmp_path, restarts = 5, failures = 2)

    def test_localization_failures_still_arent_either(self, tmp_path):
        assert not self.exits_for_preemption(tmp_path, restarts = 5, localization_failures = 1)
