"""
A job's read-only disks are attached together, then each is mounted; in
teardown each is unmounted, then they're detached together. One call at a time,
attaching and detaching took about 6.5 s per disk, so MarkDuplicates over 120
BWA shard disks spent most of its half hour on gcloud calls (2026-10-30).

These run the generated bash against a stand-in gcloud that records each call,
takes a second per call, and exits with a code chosen per disk.
"""
import shutil
import subprocess
import time
from unittest.mock import MagicMock

import pytest

from canine.localization.local import BatchedLocalizer


BASH = shutil.which("bash")
pytestmark = pytest.mark.skipif(BASH is None or shutil.which("xargs") is None,
                                reason="bash and xargs needed")


def loc(**kwargs):
    return BatchedLocalizer(MagicMock(), **kwargs)


def calls_at_once(lines, cpus = None):
    """CANINE_RODISK_CALLS as the generated lines set it, for a job with cpus CPUs."""
    script = "\n".join((["SLURM_CPUS_PER_TASK={}".format(cpus)] if cpus is not None else [])
                       + lines[:3] + ["echo $CANINE_RODISK_CALLS"])
    return int(subprocess.run([BASH], input = script, capture_output = True, text = True, check = True).stdout)


def fake_gcloud(tmp_path, exit_codes = None):
    """exit_codes: {disk: exit code}; a disk not listed succeeds."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    cases = "".join('  *"--disk {}"*) rc={};;\n'.format(d, rc) for d, rc in (exit_codes or {}).items())
    (bin_dir / "gcloud").write_text(
        "#!/bin/bash\n"
        'echo "$*" >> "{calls}"\n'
        'echo "gcloud output for $*"\n'
        "sleep 1\n"
        "rc=0\n"
        'case "$*" in\n{cases}esac\n'
        "exit $rc\n".format(calls = tmp_path / "calls", cases = cases))
    (bin_dir / "gcloud").chmod(0o755)
    # `timeout -k 30 30 <command>`, where there's no GNU timeout (e.g. macOS)
    if shutil.which("timeout") is None:
        (bin_dir / "timeout").write_text('#!/bin/bash\nshift 3; exec "$@"\n')
        (bin_dir / "timeout").chmod(0o755)
    return bin_dir


def run(tmp_path, lines, disks, exit_codes = None, setup = ()):
    bin_dir = fake_gcloud(tmp_path, exit_codes)
    script = "\n".join(
        ["set -e", 'export PATH="{}:$PATH"'.format(bin_dir),
         "CANINE_NODE_NAME=node-1", "CANINE_NODE_ZONE=us-central1-a",
         "CANINE_N_RODISKS={}".format(len(disks))]
        + ["CANINE_RODISK_{}={}".format(i + 1, d) for i, d in enumerate(disks)]
        + list(setup) + lines + ['echo "DIAG=$CANINE_RODISK_DIAG_DIR"'])
    start = time.monotonic()
    proc = subprocess.run([BASH], input = script, capture_output = True, text = True)
    calls = (tmp_path / "calls").read_text().splitlines() if (tmp_path / "calls").exists() else []
    return proc, calls, time.monotonic() - start


class TestAttach:

    DISKS = ["canine-scratch-a-{}".format(i) for i in range(8)]

    def test_every_disk_is_attached_read_only_to_this_node(self, tmp_path):
        proc, calls, _ = run(tmp_path, loc().rodisk_attach(), self.DISKS)
        assert proc.returncode == 0, proc.stderr
        assert sorted(c.split("--disk ")[1].split()[0] for c in calls) == sorted(self.DISKS)
        assert all(c.startswith("compute instances attach-disk node-1 --zone us-central1-a") for c in calls)
        assert all("--mode ro" in c and "--device-name" in c for c in calls)

    def test_the_disks_are_attached_together(self, tmp_path):
        """With 8 CPUs, eight one-second attaches take about a second, not eight."""
        proc, _, elapsed = run(tmp_path, loc().rodisk_attach(), self.DISKS, setup = ["SLURM_CPUS_PER_TASK=8"])
        assert proc.returncode == 0, proc.stderr
        assert elapsed < 4

    def test_each_disks_output_is_kept_for_the_mount_loop(self, tmp_path):
        proc, _, _ = run(tmp_path, loc().rodisk_attach(), self.DISKS[:2])
        diag = proc.stdout.split("DIAG=", 1)[1].strip()
        import os
        assert sorted(os.listdir(diag)) == sorted(self.DISKS[:2])
        assert "--disk {}".format(self.DISKS[0]) in open(os.path.join(diag, self.DISKS[0])).read()

    def test_another_jobs_attach_isnt_an_error_here(self, tmp_path):
        """
        Another job may be attaching the same disk; the mount loop fails if its
        device never appears.
        """
        proc, calls, _ = run(tmp_path, loc().rodisk_attach(), self.DISKS[:3],
                             exit_codes = {self.DISKS[1]: 1})
        assert proc.returncode == 0, proc.stderr
        assert len(calls) == 3

    @pytest.mark.parametrize("rc", [5, 124])
    def test_quota_or_a_hung_attach_requeues(self, tmp_path, rc):
        """gcloud's quota exit (5), or timeout's (124), retries the job, as before."""
        proc, calls, _ = run(tmp_path, loc().rodisk_attach(), self.DISKS[:3],
                             exit_codes = {self.DISKS[2]: rc})
        assert proc.returncode == 5
        assert "quota limit or timed out" in proc.stderr

    def test_no_disks_no_calls(self, tmp_path):
        proc, calls, _ = run(tmp_path, loc().rodisk_attach(), [])
        assert proc.returncode == 0, proc.stderr
        assert calls == []


class TestDetach:

    DISKS = ["canine-scratch-b-{}".format(i) for i in range(8)]

    def _run(self, tmp_path, to_detach, exit_codes = None):
        setup = ["SLURM_CPUS_PER_TASK=8", "CANINE_RODISKS_TO_DETACH=({})".format(" ".join(to_detach))]
        return run(tmp_path, loc().rodisk_detach(), [], exit_codes, setup)

    def test_every_unmounted_disk_is_detached_together(self, tmp_path):
        proc, calls, elapsed = self._run(tmp_path, self.DISKS)
        assert proc.returncode == 0, proc.stderr
        assert sorted(c.split("--disk ")[1] for c in calls) == sorted(self.DISKS)
        assert all(c.startswith("compute instances detach-disk node-1 --zone us-central1-a") for c in calls)
        assert elapsed < 4
        assert all("Unmounted {}".format(d) in proc.stderr for d in self.DISKS)

    def test_a_failed_detach_is_reported_not_fatal(self, tmp_path):
        proc, _, _ = self._run(tmp_path, self.DISKS[:2], {self.DISKS[1]: 1})
        assert proc.returncode == 0, proc.stderr
        assert "Error detaching disk {} (exit 1): gcloud output for".format(self.DISKS[1]) in proc.stderr
        assert "Unmounted {}".format(self.DISKS[0]) in proc.stderr

    def test_nothing_to_detach_no_calls(self, tmp_path):
        proc, calls, _ = self._run(tmp_path, [])
        assert proc.returncode == 0, proc.stderr
        assert calls == []


class TestHowManyCallsAtOnce:
    """
    One gcloud call per CPU the job has, at least 4, up to rodisk_max_parallel_calls
    (default 16): each call is a gcloud process, so a small node shouldn't run many.
    """

    @pytest.mark.parametrize("cpus, expected", [(None, 4), (1, 4), (2, 4), (8, 8), (14, 14), (32, 16)])
    def test_one_per_cpu_between_4_and_the_cap(self, cpus, expected):
        for lines in (loc().rodisk_attach(), loc().rodisk_detach()):
            assert calls_at_once(lines, cpus) == expected

    def test_the_cap_is_a_localizer_setting(self):
        assert calls_at_once(loc(rodisk_max_parallel_calls = 6).rodisk_attach(), 32) == 6
        assert calls_at_once(loc(rodisk_max_parallel_calls = 2).rodisk_detach(), 8) == 2

    def test_a_cap_below_1_is_refused(self):
        with pytest.raises(ValueError):
            loc(rodisk_max_parallel_calls = 0)

    def test_a_small_job_makes_4_calls_at_once(self, tmp_path):
        """Eight one-second attaches, 4 at a time, take about two seconds."""
        proc, calls, elapsed = run(tmp_path, loc().rodisk_attach(), TestAttach.DISKS, setup = ["SLURM_CPUS_PER_TASK=1"])
        assert proc.returncode == 0, proc.stderr
        assert len(calls) == 8 and 1.9 < elapsed < 4

    def test_a_call_may_wait_its_turn(self):
        """GCE works through a VM's calls in turn, so one call can take far longer than alone."""
        for lines in (loc().rodisk_attach(), loc().rodisk_detach()):
            assert "timeout -k 30 120 gcloud" in "\n".join(lines)


def test_the_generated_scripts_wire_them_in():
    """Localization attaches, then mounts each disk; teardown unmounts each, then detaches."""
    import inspect
    from canine.localization import base
    src = inspect.getsource(base.AbstractLocalizer.job_setup_teardown)
    attach = src.index("self.rodisk_attach()")
    mount_loop = src.index('"DIAG_FILE=${CANINE_RODISK_DIAG_DIR}/${CANINE_RODISK}"')
    assert attach < mount_loop
    assert "attach-disk" not in src[attach:src.index("Mount bucket-mounted")], "the mount loop no longer attaches"
    unmount = src.index("CANINE_RODISKS_TO_DETACH+=(${CANINE_RODISK})")
    assert unmount < src.index("self.rodisk_detach()")
    assert "detach-disk" not in src[unmount - 2000:src.index("self.rodisk_detach()")]


def test_a_jobs_scripts_with_a_read_only_disk_are_valid_bash():
    """A finished scratch disk is mounted read-only, through the same attach and mount steps."""
    from unittest.mock import patch
    backend = MagicMock()   # with real paths, so the scripts are real bash
    backend.transport.return_value.__enter__.return_value.normpath.side_effect = lambda p: "/mnt/nfs/ws/" + str(p)
    loc = BatchedLocalizer(backend, use_scratch_disk = True, scratch_disk_name = "canine-scratch-abc")
    loc.clean_on_exit = False
    loc.inputs = {"0": {}}
    loc.input_array_flag = {"0": {}}
    disk = ("/mnt/rwdisks/canine-scratch-abc-0", [], [], "rodisk://canine-scratch-abc-0")
    with patch.object(loc, "create_persistent_disk", return_value = disk):
        setup, localization, teardown, _ = loc.job_setup_teardown(
          "0", {"stdout": "../stdout", "stderr": "../stderr"}, MagicMock())
    assert "attach-disk" in localization and "detach-disk" in teardown
    for script in (localization, teardown):
        proc = subprocess.run([BASH, "-n"], input = script, capture_output = True, text = True)
        assert proc.returncode == 0, proc.stderr
