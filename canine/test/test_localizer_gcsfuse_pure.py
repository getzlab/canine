"""
Pure unit tests for the two localizer behaviours that a FUSE mount changes --
no SLURM cluster, no Docker required.

Covers NFS-FUSE-IMPLEMENTATION-PLAN.md 8.2 and 8.3:

  * same_volume() decides symlink-vs-copy, and is implemented twice -- once on
    the controller (NFSLocalizer) and once on the worker (delocalization.py).
    They must ask df the same question or they will disagree, producing either
    a dangling symlink or a needless full copy.

  * chmod is a silent no-op on gcsfuse (NFS-FUSE.md, P1). It returns 0 and
    leaves the mode at the mount-wide --file-mode, so the generated scripts can
    end up non-executable with nothing raising. _make_executable() is the one
    place that notices.
"""
import os
import stat
import warnings
from unittest.mock import MagicMock, patch

import pytest

from canine.localization.nfs import NFSLocalizer
from canine.localization import delocalization


class TestSameVolumeImplementationsAgree:
    """
    The worker-side copy previously used `df` (no -P) and field $1. Both were
    wrong: without -P, df pads to the terminal and wraps device names longer
    than the column, shifting fields onto a continuation line; and $1 is the
    device, which cannot distinguish a bind mount from what it shadows -- which
    is exactly what dockerTransient.init_storage relies on to make /mnt/nfs a
    volume boundary.
    """

    def _captured_cmd(self, fn, *args):
        with patch.object(delocalization.subprocess, "check_output", return_value=b"/mnt/nfs\n") as co:
            fn(*args)
        return co.call_args[0][0]

    def test_worker_side_uses_posix_output(self):
        cmd = self._captured_cmd(delocalization.same_volume, "/mnt/nfs/a", "/mnt/nfs/b")
        assert "df -P " in cmd

    def test_worker_side_reads_the_mount_point_field(self):
        cmd = self._captured_cmd(delocalization.same_volume, "/mnt/nfs/a", "/mnt/nfs/b")
        assert "$6" in cmd
        assert "$1" not in cmd

    def test_worker_side_quotes_its_arguments(self):
        cmd = self._captured_cmd(delocalization.same_volume, "/mnt/nfs/has a space", "/mnt/nfs/b")
        assert "'/mnt/nfs/has a space'" in cmd

    def test_both_implementations_build_the_same_command(self):
        """
        The actual invariant. Compare the two generated commands directly
        rather than asserting on each separately, so they cannot drift apart
        without this failing.
        """
        paths = ["/mnt/nfs/staging", "/mnt/nfs/input.bam"]

        worker_cmd = self._captured_cmd(delocalization.same_volume, *paths)

        loc = NFSLocalizer.__new__(NFSLocalizer)
        loc.staging_dir = paths[0]
        with patch("canine.localization.nfs.subprocess.check_output", return_value=b"/mnt/nfs\n") as co:
            loc.same_volume(*paths[1:])
        controller_cmd = co.call_args[0][0]

        assert worker_cmd == controller_cmd

    def test_agrees_on_a_real_path(self):
        """
        Sanity check against the real filesystem: a path is on the same volume
        as itself, both sides.
        """
        loc = NFSLocalizer.__new__(NFSLocalizer)
        loc.staging_dir = "/tmp"
        assert loc.same_volume("/tmp") is True
        assert delocalization.same_volume("/tmp", "/tmp") is True


class TestMakeExecutable:

    def _localizer(self, tmp_path):
        loc = NFSLocalizer.__new__(NFSLocalizer)
        loc.staging_dir = str(tmp_path)
        loc.local_dir = str(tmp_path)
        return loc

    def test_sets_the_execute_bit(self, tmp_path):
        script = tmp_path / "setup.sh"
        script.write_text("#!/bin/bash\n")
        os.chmod(script, 0o644)

        self._localizer(tmp_path)._make_executable(str(script))
        assert os.stat(script).st_mode & stat.S_IXUSR

    def test_silent_when_the_mode_takes(self, tmp_path):
        script = tmp_path / "setup.sh"
        script.write_text("#!/bin/bash\n")

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self._localizer(tmp_path)._make_executable(str(script))
        assert caught == []

    def test_warns_when_chmod_silently_does_nothing(self, tmp_path):
        """
        The gcsfuse case: chmod returns 0 but the mode is whatever --file-mode
        says. Simulated by making chmod a no-op and leaving the file at 0644 --
        without this check the scripts are quietly non-executable and the
        failure only shows up much later, at job submission, on every shard.
        """
        script = tmp_path / "setup.sh"
        script.write_text("#!/bin/bash\n")
        os.chmod(script, 0o644)

        with patch("canine.localization.nfs.os.chmod"):  # returns None, changes nothing
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                self._localizer(tmp_path)._make_executable(str(script))

        assert len(caught) == 1
        assert "--file-mode=0755" in str(caught[0].message)
