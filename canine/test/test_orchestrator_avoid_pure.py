"""
Orchestrator.job_avoid on a scratch-disk task, and make_output_DF's message for a job
that hit the preemption limit. No cluster: a task directory laid out on local disk the
way a finished job leaves it on NFS.
"""
import contextlib
import os
import shutil
from collections import namedtuple

import pandas as pd
import pytest

from canine.orchestrator import Orchestrator

PathType = namedtuple("PathType", ["localpath", "remotepath"])
OUTPUTS = {
    "mat_file": "*.dRanger_results.mat",
    "forBP_file": "*.dRanger_results.forBP.txt",
    "stdout": "$CANINE_JOB_ROOT/stdout",
    "stderr": "$CANINE_JOB_ROOT/stderr",
}


class Transport:
    def exists(self, path):
        return os.path.exists(path)

    def isdir(self, path):
        return os.path.isdir(path)

    def isfile(self, path):
        return os.path.isfile(path)

    def open(self, path, mode):
        return open(path, mode)

    def rmtree(self, path):
        shutil.rmtree(path, ignore_errors=True)

    def makedirs(self, path):
        os.makedirs(path, exist_ok=True)


class Localizer:
    def __init__(self, root, use_scratch_disk, files_to_copy_to_outputs=()):
        self.staging_dir = str(root)
        self.use_scratch_disk = use_scratch_disk
        self.files_to_copy_to_outputs = set(files_to_copy_to_outputs)

    def environment(self, _):
        return {"CANINE_JOBS": os.path.join(self.staging_dir, "jobs"),
                "CANINE_OUTPUT": os.path.join(self.staging_dir, "outputs")}

    def reserve_path(self, *parts):
        path = os.path.join(self.staging_dir, *parts)
        return PathType(path, path)

    @contextlib.contextmanager
    def transport_context(self):
        yield Transport()


def finished_job(root, job="0", workspace=False, exit_codes=(0, 0, 0), copied=("mat_file", "forBP_file")):
    """A finished job as it is left on NFS. A scratch-disk job has no workspace there."""
    jobs = root / "jobs" / job
    jobs.mkdir(parents=True)
    if workspace:
        (jobs / "workspace").mkdir()
    for name, code in zip((".job_exit_code", ".localizer_exit_code", ".teardown_exit_code"), exit_codes):
        (jobs / name).write_text(str(code))
    (jobs / "stdout").write_text("")
    (jobs / "stderr").write_text("")
    out = root / "outputs" / job
    out.mkdir(parents=True)
    rows = []
    for name, suffix in (("mat_file", "dRanger_results.mat"), ("forBP_file", "dRanger_results.forBP.txt")):
        rel = "{}/{}/S.{}".format(job, name, suffix)
        rows.append("{}\t{}\t{}\t{}".format(job, name, OUTPUTS[name], rel))
        if name in copied:
            (root / "outputs" / rel).parent.mkdir(parents=True, exist_ok=True)
            (root / "outputs" / rel).write_text("x")
    # The delocalizer records stdout/stderr's pattern expanded, not as declared.
    for name in ("stdout", "stderr"):
        rows.append("{}\t{}\t{}\t{}/{}".format(job, name, jobs / name, job, name))
    (out / ".canine_job_manifest").write_text("\n".join(rows) + "\n")


def avoid(root, outputs=OUTPUTS, **localizer):
    orch = object.__new__(Orchestrator)
    orch.job_spec = {"0": {"individual": "S"}}
    orch.raw_outputs = dict(outputs)
    n, _ = orch.job_avoid(Localizer(root, **localizer))
    return n, orch.job_spec


class TestAScratchDiskJobWithEveryOutputOnNfs:
    """
    Seen on tonly-dih, 2026-10-08: dRangerRun finished, the lab's cost-monitoring app
    deleted its scratch disk about ten hours later, and a rerun the next day redid it
    from scratch, though both its outputs were already copied to NFS.
    """

    def test_is_avoided_without_its_disk(self, tmp_path):
        finished_job(tmp_path)
        n, spec = avoid(tmp_path, use_scratch_disk=True,
                        files_to_copy_to_outputs={"mat_file", "forBP_file"})
        assert n == 1 and spec["0"] is None

    def test_is_rerun_when_a_copy_is_missing(self, tmp_path):
        finished_job(tmp_path, copied=("mat_file",))
        n, spec = avoid(tmp_path, use_scratch_disk=True,
                        files_to_copy_to_outputs={"mat_file", "forBP_file"})
        assert n == 0 and spec["0"] is not None
        assert not (tmp_path / "jobs" / "0").exists(), "a failed job's directory is purged"

    def test_is_rerun_when_its_declared_outputs_changed(self, tmp_path):
        finished_job(tmp_path)
        n, spec = avoid(tmp_path, outputs={**OUTPUTS, "mat_file": "*.mat"}, use_scratch_disk=True,
                        files_to_copy_to_outputs={"mat_file", "forBP_file"})
        assert n == 0 and spec["0"] is not None

    def test_is_rerun_when_it_failed(self, tmp_path):
        finished_job(tmp_path, exit_codes=(1, 0, 0))
        n, spec = avoid(tmp_path, use_scratch_disk=True,
                        files_to_copy_to_outputs={"mat_file", "forBP_file"})
        assert n == 0 and spec["0"] is not None


class TestUnchangedCases:

    def test_a_scratch_disk_job_with_an_output_left_on_its_disk_is_not_avoided(self, tmp_path):
        '''Its NFS outputs are not a complete record, so it still needs the disk.'''
        finished_job(tmp_path, copied=("mat_file",))
        n, _ = avoid(tmp_path, use_scratch_disk=True, files_to_copy_to_outputs={"mat_file"})
        assert n == 0

    def test_a_job_without_a_scratch_disk_still_needs_its_workspace(self, tmp_path):
        finished_job(tmp_path)
        assert avoid(tmp_path, use_scratch_disk=False)[0] == 0

    def test_an_ordinary_finished_job_is_avoided(self, tmp_path):
        finished_job(tmp_path, workspace=True)
        n, spec = avoid(tmp_path, use_scratch_disk=False)
        assert n == 1 and spec["0"] is None


class TestOverwrite:
    """With avoidance off, previous jobs are cleared; the warning is the caller's choice."""

    @staticmethod
    def _overwrite(tmp_path, monkeypatch, **kwargs):
        finished_job(tmp_path, workspace=True)
        logged = {"warning": [], "info1": []}
        for level in logged:
            monkeypatch.setattr("canine.orchestrator.canine_logging." + level, logged[level].append)
        orch = object.__new__(Orchestrator)
        orch.job_spec = {"0": {"individual": "S"}}
        orch.raw_outputs = dict(OUTPUTS)
        n, _ = orch.job_avoid(Localizer(tmp_path, use_scratch_disk=False), overwrite=True, **kwargs)
        assert n == 0 and not (tmp_path / "jobs").exists()
        return logged

    def test_warns_by_default(self, tmp_path, monkeypatch):
        logged = self._overwrite(tmp_path, monkeypatch)
        assert any("rerunning them" in m for m in logged["warning"])

    def test_only_logs_when_the_caller_avoids_its_own_way(self, tmp_path, monkeypatch):
        logged = self._overwrite(tmp_path, monkeypatch, warn_overwrite=False)
        assert logged["warning"] == []
        assert any("Clearing staging directory" in m for m in logged["info1"])


class TestAJobStoppedAtThePreemptionLimit:
    """
    The entrypoint stops a job preempted CANINE_PREEMPT_LIMIT times with exit 123,
    before delocalization, so it has no outputs. It used to be reported as
    "catastrophically lost (no stdout/stderr available)".
    """

    @staticmethod
    def _make_df(monkeypatch, exit_code):
        logged = []
        monkeypatch.setattr("canine.orchestrator.canine_logging.error", logged.append)
        orch = object.__new__(Orchestrator)
        orch.output_map = {}
        acct = pd.DataFrame({
            "State": ["FAILED"], "ExitCode": [exit_code], "CPUTimeRAW": [1],
            "Submit": [pd.Timestamp("2026-10-08")], "n_preempted": [5],
        }, index=["485_0"])
        orch.make_output_DF(485, {"0": {"x": "1"}}, {}, acct)
        return logged

    def test_says_so(self, monkeypatch):
        logged = self._make_df(monkeypatch, "123:0")
        assert any("preemption limit (exit 123)" in m and ": 0" in m for m in logged)
        assert not any("catastrophically lost" in m for m in logged)

    def test_any_other_lost_job_is_still_reported_as_lost(self, monkeypatch):
        logged = self._make_df(monkeypatch, "0:9")
        assert any("1/1 job(s) were catastrophically lost" in m for m in logged)
        assert not any("preemption limit" in m for m in logged)
