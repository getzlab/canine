"""
dockerTransient.ready_for_docker, which checks that no Slurm/mysql/Munge daemon runs
outside Docker before the controller container starts. No cluster needed.
"""
import psutil

import canine.backends.dockerTransient as dt


class _Proc:
    def __init__(self, pid, name, gone=False):
        self.pid, self._name, self._gone = pid, name, gone
        self.info = {"name": name}

    def name(self):
        if self._gone:
            raise psutil.NoSuchProcess(self.pid)
        return self._name


def _process_iter_with_one_exiting(attrs=None):
    """
    psutil's behavior when a process exits during the scan: listed, but asking for
    its name raises; process_iter(attrs) fetches the names itself and skips it.
    """
    procs = [_Proc(1, "systemd"), _Proc(5332, "sh", gone=True), _Proc(7, "bash")]
    return (p for p in procs if attrs is None or not p._gone)


class TestReadyForDocker:

    def test_a_process_exiting_mid_scan_does_not_crash_it(self, monkeypatch):
        """
        Seen on tonly4, 2026-10-10: run_sv_tonly.py died at startup with
        psutil.NoSuchProcess (pid 5332) from ready_for_docker's process scan.
        """
        monkeypatch.setattr(dt.psutil, "process_iter", _process_iter_with_one_exiting)
        dt.ready_for_docker()

    def test_runs_against_the_real_process_table(self):
        # no Slurm daemons run on a test machine, so it returns without raising
        dt.ready_for_docker()
