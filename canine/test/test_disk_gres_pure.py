"""
Each job requests, as the Slurm GRES "pdisk", the persistent disks it attaches:
its own scratch disk, and other jobs' scratch disks read-only. GCE allows 128 per
VM, and Slurm otherwise doesn't know how many a node already has: a MarkDuplicates
attaching 120 BWA scratch disks failed when other jobs' disks took its node to 140
(2026-11-02). Only a cluster that defines pdisk gets the request.
"""
import io
from unittest.mock import MagicMock, patch

import pytest

from canine.localization.file_handlers import HandleRODISKURL
from canine.localization.local import BatchedLocalizer
from canine.orchestrator import DISK_GRES, Orchestrator, _with_gres


def attached(read_only, use_scratch_disk = True, finished = False):
    """How many disks job_setup_teardown counts for a job reading read_only scratch disks."""
    loc = BatchedLocalizer(MagicMock(), use_scratch_disk = use_scratch_disk, scratch_disk_name = "canine-scratch-abc")
    loc.clean_on_exit = False
    loc.inputs = {"0": {"bam": [HandleRODISKURL("rodisk://canine-scratch-up-{}/x.bam".format(i)) for i in range(read_only)]}}
    loc.input_array_flag = {"0": {"bam": True}}
    disk = (("/mnt/rwdisks/canine-scratch-abc-0", [], [], "rodisk://canine-scratch-abc-0") if finished
            else ("/mnt/rwdisks/canine-scratch-abc-0", ["echo create"], ["echo teardown"], None))
    with patch.object(loc, "create_persistent_disk", return_value = disk):
        loc.job_setup_teardown("0", {"stdout": "../stdout", "stderr": "../stderr"}, MagicMock())
    return loc.attached_disks["0"]


class TestCounting:

    def test_its_own_scratch_disk_and_the_ones_it_reads(self):
        assert attached(read_only = 120) == 121

    def test_a_job_without_a_scratch_disk_counts_only_what_it_reads(self):
        assert attached(read_only = 3, use_scratch_disk = False) == 3

    def test_a_finished_scratch_disk_is_counted_once(self):
        # mounted read-only, among the disks it reads
        assert attached(read_only = 2, finished = True) == 3

    def test_the_same_disk_read_twice_is_one_disk(self):
        loc = BatchedLocalizer(MagicMock(), use_scratch_disk = False)
        loc.clean_on_exit = False
        loc.inputs = {"0": {"bam": [HandleRODISKURL("rodisk://canine-scratch-up-0/x.bam")],
                            "bai": [HandleRODISKURL("rodisk://canine-scratch-up-0/x.bai")]}}
        loc.input_array_flag = {"0": {"bam": False, "bai": False}}
        loc.job_setup_teardown("0", {"stdout": "../stdout", "stderr": "../stderr"}, MagicMock())
        assert loc.attached_disks["0"] == 1


def orchestrator(gres_types, resources = None, job_spec = None):
    orch = object.__new__(Orchestrator)
    orch.resources = resources or {}
    orch.job_spec = job_spec or {"0": {}, "1": {}, "2": {}}
    config = "SelectType              = select/cons_tres\nGresTypes               = {}\n".format(gres_types) if gres_types is not None else "SelectType = select/cons_tres\n"
    orch.backend = MagicMock()
    orch.backend.invoke.return_value = (0, io.BytesIO(config.encode()), io.BytesIO(b""))
    return orch


def localizer(counts):
    return MagicMock(attached_disks = counts)


class TestRequest:

    def test_the_array_requests_the_most_any_job_attaches(self):
        orch = orchestrator("pdisk")
        assert orch.disk_gres_args(localizer({"0": 1, "1": 121, "2": 5})) == {"gres": "pdisk:121"}

    def test_jobs_that_were_avoided_dont_count(self):
        orch = orchestrator("pdisk", job_spec = {"0": {}, "1": None})
        assert orch.disk_gres_args(localizer({"0": 2, "1": 121})) == {"gres": "pdisk:2"}

    def test_nothing_is_requested_without_disks(self):
        orch = orchestrator("pdisk")
        assert orch.disk_gres_args(localizer({"0": 0, "1": 0, "2": 0})) == {}
        assert orch.disk_gres_args(MagicMock(spec = [])) == {}   # a localizer that doesn't count

    @pytest.mark.parametrize("gres_types", [None, "(null)", "gpu"])
    def test_a_cluster_without_pdisk_gets_no_request(self, gres_types):
        orch = orchestrator(gres_types)
        assert orch.disk_gres_args(localizer({"0": 121})) == {}

    def test_pdisk_among_other_gres_types(self):
        assert orchestrator("gpu,pdisk").disk_gres_args(localizer({"0": 3})) == {"gres": "pdisk:3"}

    def test_a_failing_query_means_no_request(self):
        orch = orchestrator("pdisk")
        orch.backend.invoke.side_effect = RuntimeError("no controller")
        assert orch.disk_gres_args(localizer({"0": 3})) == {}

    def test_the_cluster_is_asked_once(self):
        orch = orchestrator("pdisk")
        orch.disk_gres_args(localizer({"0": 3}))
        orch.disk_gres_args(localizer({"0": 4}))
        assert orch.backend.invoke.call_count == 1

    def test_a_tasks_own_gres_is_kept(self):
        orch = orchestrator("gpu,pdisk", resources = {"gres": "gpu:1"})
        assert orch.disk_gres_args(localizer({"0": 2})) == {"gres": "gpu:1,pdisk:2"}

    def test_with_gres(self):
        assert _with_gres(None, DISK_GRES, 4) == "pdisk:4"
        assert _with_gres("gpu:2", DISK_GRES, 4) == "gpu:2,pdisk:4"


def test_the_request_reaches_sbatch():
    orch = orchestrator("pdisk", job_spec = {"0": {}, "1": {}})
    orch.name = "t"
    orch.backend.sbatch.return_value = 7
    orch.submit_batch_job("/e.sh", {"CANINE_JOBS": "/jobs"}, extra_sbatch_args = orch.disk_gres_args(localizer({"0": 121, "1": 1})))
    assert orch.backend.sbatch.call_args.kwargs["gres"] == "pdisk:121"
