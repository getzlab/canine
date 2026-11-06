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


class TestInitStorage:
    """
    Where workflow results go: a bucket, a persistent disk, or, with neither, a
    directory on /mnt/nfs itself.
    """

    @staticmethod
    def _init_storage(monkeypatch, **config):
        """Runs init_storage, recording directories made and disk-usage checks, in order."""
        events = []
        monkeypatch.setattr(dt.subprocess, "check_call", lambda *a, **k: 0)
        monkeypatch.setattr(dt.os, "makedirs", lambda path, exist_ok=False: events.append(("makedirs", path)))
        monkeypatch.setattr(dt.shutil, "disk_usage", lambda path: events.append(("disk_usage", path)) or
                            type("usage", (), {"free": 500 * 1024**3})())
        backend = object.__new__(dt.DockerTransientImageSlurmBackend)
        backend.config = {"storage_bucket": None, "storage_disk": None, "storage_namespace": "ns",
                          "worker_prefix": "wolf", "compute_zone": "us-central1-a",
                          "nfs_disk_type": "pd-balanced", "storage_disk_size": 512, **config}
        backend.invoke = lambda *a, **k: (0, None, None)
        backend.init_storage()
        return events

    def test_without_a_bucket_or_disk_the_directory_is_made_before_it_is_checked(self, monkeypatch):
        """
        Seen on a new controller, 2026-10-30: nothing had made /mnt/nfs/workspace,
        so checking its free space raised FileNotFoundError and the cluster
        didn't start.
        """
        assert self._init_storage(monkeypatch) == [("makedirs", "/mnt/nfs/ns"), ("disk_usage", "/mnt/nfs/ns")]

    def test_a_disk_is_mounted_there_instead(self, monkeypatch):
        assert self._init_storage(monkeypatch, storage_disk = "ns") == [("disk_usage", "/mnt/nfs/ns")]
