"""
Pure unit tests for Orchestrator.load_acct_from_disk and the grouper() logic inside
wait_for_jobs_to_finish -- no SLURM cluster, no Docker required. Covers the on-disk
.sacct format introduced for cost estimation (Start, End, Elapsed, AllocTRES,
Account, and the per-attempt "attempts" breakdown), plus backward compatibility
with .sacct files written by older canine versions.
"""
import contextlib
import json
import os
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from canine.orchestrator import Orchestrator


class FakeTransport:
    def exists(self, path):
        return os.path.exists(path)

    def open(self, path, mode):
        return open(path, mode)


class FakeLocalizer:
    def __init__(self, jobs_dir):
        self.jobs_dir = jobs_dir

    def environment(self, _):
        return {"CANINE_JOBS": self.jobs_dir}

    @contextlib.contextmanager
    def transport_context(self):
        yield FakeTransport()


def write_sacct_row(jobs_dir, job_id, fields):
    job_dir = os.path.join(jobs_dir, job_id)
    os.makedirs(job_dir, exist_ok=True)
    with open(os.path.join(job_dir, ".sacct"), "w") as f:
        f.write("\t".join(str(x) for x in fields) + "\n")


class TestLoadAcctFromDisk:
    def test_current_format_round_trips_attempts(self, tmp_path):
        attempts = [
          {"NodeList": "worker1", "Start": "2026-01-01T00:00:00", "End": "2026-01-01T00:10:00", "AllocTRES": "cpu=2,mem=4G", "CPUTimeRAW": 500},
        ]
        write_sacct_row(str(tmp_path), "a", [
          "COMPLETED", "0:0", 500, "2026-01-01 00:00:00", "worker1", "main", 2, 2, "4G",
          "2026-01-01T00:00:00", "2026-01-01T00:10:00", "00:10:00", "cpu=2,mem=4G", "abcd", 0,
          json.dumps(attempts),
        ])
        job_spec = {"a": {}}
        result = Orchestrator.load_acct_from_disk(job_spec, FakeLocalizer(str(tmp_path)), 999)

        row = result.loc["999_a"]
        assert row["NodeList"] == "worker1"
        assert row["CPUTimeRAW"] == 500
        assert row["AllocTRES"] == "cpu=2,mem=4G"
        assert row["Account"] == "abcd"
        assert row["attempts"] == attempts

    def test_old_10_column_format_gets_placeholders_for_new_fields(self, tmp_path):
        write_sacct_row(str(tmp_path), "b", [
          "COMPLETED", "0:0", 300, "2026-01-01 00:00:00", "worker2", "main", 1, 1, "2G", 0,
        ])
        job_spec = {"b": {}}
        result = Orchestrator.load_acct_from_disk(job_spec, FakeLocalizer(str(tmp_path)), 999)

        row = result.loc["999_b"]
        assert row["NodeList"] == "worker2"
        assert row["CPUTimeRAW"] == 300
        assert pd.isna(row["Start"])
        assert pd.isna(row["End"])
        assert row["AllocTRES"] == "-"
        assert row["Account"] == "-"
        assert row["attempts"] == []

    def test_oldest_5_column_format_gets_all_placeholders(self, tmp_path):
        write_sacct_row(str(tmp_path), "c", ["COMPLETED", "0:0", 100, "2026-01-01 00:00:00", 0])
        job_spec = {"c": {}}
        result = Orchestrator.load_acct_from_disk(job_spec, FakeLocalizer(str(tmp_path)), 999)

        row = result.loc["999_c"]
        assert row["NodeList"] == "-"
        assert row["ReqCPUS"] == -1
        assert row["CPUTimeRAW"] == 100
        assert row["AllocTRES"] == "-"
        assert row["attempts"] == []

    def test_missing_sacct_file_gives_placeholder_row(self, tmp_path):
        # job dir doesn't even exist -- e.g. avoided job that never ran
        job_spec = {"d": None}
        result = Orchestrator.load_acct_from_disk(job_spec, FakeLocalizer(str(tmp_path)), 999)

        row = result.loc["999_d"]
        assert row["State"] == "COMPLETED"  # overridden because job_spec[j] is None
        assert row["NodeList"] == "-"
        assert row["attempts"] == []

    def test_unrecognized_column_count_falls_back_to_placeholder(self, tmp_path):
        # 7 columns matches none of the known tiers -- must not silently misalign
        # data into the wrong fields (this is exactly the bug this rewrite fixes:
        # pd.read_csv does not raise when `names=` is longer than the data present,
        # it silently pads/misaligns instead).
        write_sacct_row(str(tmp_path), "e", ["COMPLETED", "0:0", 100, "2026-01-01 00:00:00", "w1", "main", 0])
        job_spec = {"e": {}}
        result = Orchestrator.load_acct_from_disk(job_spec, FakeLocalizer(str(tmp_path)), 999)

        row = result.loc["999_e"]
        assert row["NodeList"] == "-"
        assert row["CPUTimeRAW"] == -1
        assert row["attempts"] == []

    def test_multiple_jobs_mixed_formats(self, tmp_path):
        attempts = [{"NodeList": "worker1", "Start": "s", "End": "e", "AllocTRES": "-", "CPUTimeRAW": 10}]
        write_sacct_row(str(tmp_path), "new", [
          "COMPLETED", "0:0", 10, "2026-01-01 00:00:00", "worker1", "main", 1, 1, "1G",
          "s", "e", "el", "-", "acct1", 0, json.dumps(attempts),
        ])
        write_sacct_row(str(tmp_path), "old", [
          "COMPLETED", "0:0", 20, "2026-01-01 00:00:00", "worker2", "main", 1, 1, "1G", 0,
        ])
        job_spec = {"new": {}, "old": {}, "avoided": None}
        result = Orchestrator.load_acct_from_disk(job_spec, FakeLocalizer(str(tmp_path)), 1)

        assert set(result.index) == {"1_new", "1_old", "1_avoided"}
        assert result.loc["1_new", "attempts"] == attempts
        assert result.loc["1_old", "attempts"] == []
        assert result.loc["1_avoided", "attempts"] == []


def make_raw_sacct_df(rows):
    """rows: list of dicts, one per raw sacct row (pre-grouping), indexed by JobID."""
    df = pd.DataFrame(rows)
    df.index = [r["_index"] for r in rows]
    return df.drop(columns=["_index"])


class TestWaitForJobsToFinishGrouper:
    """
    Exercises the grouper() closure inside wait_for_jobs_to_finish -- specifically,
    that a preempted/requeued job's per-attempt NodeList/Start/End/AllocTRES/
    CPUTimeRAW are retained (not collapsed into a single last-attempt-only row),
    while the existing collapsed summary fields (State, NodeList, summed
    CPUTimeRAW, n_preempted) keep their pre-existing meaning.
    """

    def test_preempted_job_retains_per_attempt_breakdown(self):
        # job "1" was preempted once: first attempt ran on worker1 and got
        # preempted, second (later-submitted) attempt ran to completion on worker2.
        raw = make_raw_sacct_df([
          { "_index": "999_1", "State": "PREEMPTED", "ExitCode": "0:0", "CPUTimeRAW": 100, "PlannedCPURAW": 0,
            "Submit": "2026-01-01 00:00:00", "NodeList": "worker1", "Partition": "main", "ReqCPUS": 2, "NCPUS": 2, "ReqMem": "4G",
            "Start": "2026-01-01T00:00:05", "End": "2026-01-01T00:02:00", "Elapsed": "00:01:55", "AllocTRES": "cpu=2,mem=4G,node=1", "Account": "abcd" },
          { "_index": "999_1", "State": "COMPLETED", "ExitCode": "0:0", "CPUTimeRAW": 200, "PlannedCPURAW": 0,
            "Submit": "2026-01-01 00:05:00", "NodeList": "worker2", "Partition": "main", "ReqCPUS": 2, "NCPUS": 2, "ReqMem": "4G",
            "Start": "2026-01-01T00:05:05", "End": "2026-01-01T00:08:25", "Elapsed": "00:03:20", "AllocTRES": "cpu=2,mem=4G,node=1", "Account": "abcd" },
        ])

        orch = object.__new__(Orchestrator)
        orch.backend = MagicMock()
        orch.backend.sacct.return_value = raw
        orch.job_spec = {"1": {"some": "spec"}}

        with patch("canine.orchestrator.time.sleep"):
            completed_jobs, uptime, acct = orch.wait_for_jobs_to_finish(999, localizer=None)

        assert completed_jobs == [("1", "999_1")]
        row = acct.loc["999_1"]

        # collapsed summary fields keep their existing, pre-existing meaning
        assert row["State"] == "COMPLETED"      # last attempt's state
        assert row["NodeList"] == "worker2"      # last attempt's node
        assert row["CPUTimeRAW"] == 300          # summed across both attempts
        assert row["n_preempted"] == 1

        # new per-attempt breakdown retains what the collapsed fields lose
        assert len(row["attempts"]) == 2
        assert row["attempts"][0]["NodeList"] == "worker1"
        assert row["attempts"][0]["CPUTimeRAW"] == 100
        assert row["attempts"][0]["NCPUS"] == 2
        assert row["attempts"][0]["ReqMem"] == "4G"
        assert row["attempts"][1]["NodeList"] == "worker2"
        assert row["attempts"][1]["CPUTimeRAW"] == 200

    def test_single_attempt_job_has_one_element_attempts_list(self):
        raw = make_raw_sacct_df([
          { "_index": "999_2", "State": "COMPLETED", "ExitCode": "0:0", "CPUTimeRAW": 50, "PlannedCPURAW": 0,
            "Submit": "2026-01-01 00:00:00", "NodeList": "worker3", "Partition": "main", "ReqCPUS": 1, "NCPUS": 1, "ReqMem": "1G",
            "Start": "2026-01-01T00:00:05", "End": "2026-01-01T00:01:00", "Elapsed": "00:00:55", "AllocTRES": "cpu=1,mem=1G,node=1", "Account": "abcd" },
        ])

        orch = object.__new__(Orchestrator)
        orch.backend = MagicMock()
        orch.backend.sacct.return_value = raw
        orch.job_spec = {"2": {"some": "spec"}}

        with patch("canine.orchestrator.time.sleep"):
            _, _, acct = orch.wait_for_jobs_to_finish(999, localizer=None)

        row = acct.loc["999_2"]
        assert row["n_preempted"] == 0
        assert row["attempts"] == [{
          "NodeList": "worker3", "Start": "2026-01-01T00:00:05", "End": "2026-01-01T00:01:00",
          "AllocTRES": "cpu=1,mem=1G,node=1", "CPUTimeRAW": 50, "NCPUS": 1, "ReqMem": "1G",
        }]

    def test_constant_submit_across_attempts_still_orders_by_start(self):
        # Real SLURM behavior for preemption/requeue: Submit is the ORIGINAL
        # job submission time and does not change across requeues of the same
        # JobID -- confirmed live (a real 48-attempt job's summary Submit
        # exactly matched its *first* attempt's own Start). Sorting on an
        # all-equal Submit column doesn't reliably preserve/produce
        # chronological order (pandas' sort isn't stable for tied keys), so
        # the old code could pick an arbitrary row -- not necessarily the
        # truly last one -- as the "final" summary row. This deliberately
        # puts the chronologically-LAST attempt FIRST in the raw (pre-
        # grouping) row order, so a naive "whatever's physically last"
        # assumption can't accidentally save a broken implementation.
        raw = make_raw_sacct_df([
          { "_index": "999_3", "State": "COMPLETED", "ExitCode": "0:0", "CPUTimeRAW": 50, "PlannedCPURAW": 0,
            "Submit": "2026-01-01 00:00:00", "NodeList": "worker-final", "Partition": "main", "ReqCPUS": 1, "NCPUS": 1, "ReqMem": "1G",
            "Start": "2026-01-01T00:10:00", "End": "2026-01-01T00:10:30", "Elapsed": "00:00:30", "AllocTRES": "cpu=1,mem=1G,node=1", "Account": "abcd" },
          { "_index": "999_3", "State": "PREEMPTED", "ExitCode": "0:0", "CPUTimeRAW": 20, "PlannedCPURAW": 0,
            "Submit": "2026-01-01 00:00:00", "NodeList": "worker-early", "Partition": "main", "ReqCPUS": 1, "NCPUS": 1, "ReqMem": "1G",
            "Start": "2026-01-01T00:00:05", "End": "2026-01-01T00:00:15", "Elapsed": "00:00:10", "AllocTRES": "cpu=1,mem=1G,node=1", "Account": "abcd" },
        ])

        orch = object.__new__(Orchestrator)
        orch.backend = MagicMock()
        orch.backend.sacct.return_value = raw
        orch.job_spec = {"3": {"some": "spec"}}

        with patch("canine.orchestrator.time.sleep"):
            _, _, acct = orch.wait_for_jobs_to_finish(999, localizer=None)

        row = acct.loc["999_3"]
        assert row["State"] == "COMPLETED"
        assert row["NodeList"] == "worker-final"  # chronologically last, not whichever row happened to be last in raw order
        assert row["n_preempted"] == 1

class TestAttemptSortKey:
    """
    _attempt_sort_key(), used to order a job's preemption/requeue attempts by
    Start rather than Submit. Tested directly (not by driving the whole
    wait_for_jobs_to_finish polling loop) since a still-PENDING/RUNNING
    attempt is explicitly a non-terminal state (see the "job has completed"
    check further down in wait_for_jobs_to_finish) -- routing one through the
    full polling loop, which only returns once every job reaches a terminal
    state, would just hang.
    """

    def test_unknown_and_dash_placeholders_sort_before_real_timestamps(self):
        from canine.orchestrator import _attempt_sort_key
        col = pd.Series(["2026-01-01T00:00:05", "Unknown", "-", "2026-01-01T00:00:01"])
        key = _attempt_sort_key(col)
        assert list(key) == ["2026-01-01T00:00:05", "", "", "2026-01-01T00:00:01"]
        sorted_index = list(key.sort_values().index)
        assert set(sorted_index[:2]) == {1, 2}  # both placeholders sort first, in either relative order
        assert sorted_index[2:] == [3, 0]  # then real timestamps, in chronological order


class TestQuerySacctForNodes:
    """
    Orchestrator.query_sacct_for_nodes -- the cluster-wide, node/time-scoped query
    used to build a cost-estimation snapshot of node occupancy. Unlike
    wait_for_jobs_to_finish(), this must see jobs from any account/tenant sharing
    the node, not just wolF/canine's own array-job shards.
    """

    def test_empty_node_list_returns_without_querying(self):
        backend = MagicMock()
        result = Orchestrator.query_sacct_for_nodes(backend, [], "2026-01-01", "2026-01-02")
        assert result.empty
        backend.sacct.assert_not_called()

    def test_passes_expected_sacct_flags(self):
        backend = MagicMock()
        backend.sacct.return_value = pd.DataFrame()
        Orchestrator.query_sacct_for_nodes(
          backend, ["worker1", "worker2"],
          pd.Timestamp("2026-01-01 00:00:00"), pd.Timestamp("2026-01-01 01:00:00"),
        )
        args, kwargs = backend.sacct.call_args
        assert args == ("D",)
        assert kwargs["allusers"] is True
        assert kwargs["nodelist"] == "worker1,worker2"
        assert kwargs["starttime"] == "2026-01-01T00:00:00"
        assert kwargs["endtime"] == "2026-01-01T01:00:00"

    def test_batch_step_rows_dropped_but_non_underscore_jobids_kept(self):
        # unlike wait_for_jobs_to_finish, a job from a different tool/account that
        # doesn't follow wolF/canine's "<batch_id>_<shard>" JobID convention must
        # still be visible here -- only the ".batch" substep rows are noise.
        raw = make_raw_sacct_df([
          { "_index": "555_3", "State": "COMPLETED", "ExitCode": "0:0", "CPUTimeRAW": 100, "Submit": "2026-01-01 00:00:00",
            "NodeList": "worker1", "Partition": "main", "ReqCPUS": 2, "NCPUS": 2, "ReqMem": "4G",
            "Start": "2026-01-01T00:00:05", "End": "2026-01-01T00:02:00", "Elapsed": "00:01:55", "AllocTRES": "cpu=2,mem=4G", "Account": "abcd" },
          { "_index": "555_3.batch", "State": "COMPLETED", "ExitCode": "0:0", "CPUTimeRAW": 100, "Submit": "2026-01-01 00:00:00",
            "NodeList": "worker1", "Partition": "main", "ReqCPUS": 2, "NCPUS": 2, "ReqMem": "4G",
            "Start": "2026-01-01T00:00:05", "End": "2026-01-01T00:02:00", "Elapsed": "00:01:55", "AllocTRES": "cpu=2,mem=4G", "Account": "abcd" },
          { "_index": "777", "State": "RUNNING", "ExitCode": "0:0", "CPUTimeRAW": 50, "Submit": "2026-01-01 00:01:00",
            "NodeList": "worker1", "Partition": "other-team", "ReqCPUS": 4, "NCPUS": 4, "ReqMem": "8G",
            "Start": "2026-01-01T00:01:05", "End": "Unknown", "Elapsed": "00:00:30", "AllocTRES": "cpu=4,mem=8G", "Account": "otherteam" },
        ])
        backend = MagicMock()
        backend.sacct.return_value = raw

        result = Orchestrator.query_sacct_for_nodes(backend, ["worker1"], "2026-01-01", "2026-01-02")

        assert set(result.index) == {"555_3", "777"}
        assert result.loc["777", "Account"] == "otherteam"
        assert result.loc["777", "End"] == "Unknown"  # left unparsed, per Start/End convention elsewhere

    def test_empty_sacct_result_does_not_crash(self):
        backend = MagicMock()
        backend.sacct.return_value = pd.DataFrame()
        result = Orchestrator.query_sacct_for_nodes(backend, ["worker1"], "2026-01-01", "2026-01-02")
        assert result.empty
