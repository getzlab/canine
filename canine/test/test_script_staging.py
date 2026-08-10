"""
Tests for staging the scripts the compute node runs into CANINE_ROOT.

They ride the shared staging directory rather than the worker image: baking them in
would need a fleet-wide rebuild and would version-skew against the installed canine.

The localizer-level assertions live in test_localizer_{nfs,local,remote,batched}.py, but
those need a live cluster. These cover the part that can be checked locally, including
the executable bit -- which is what actually broke.
"""

import os
import shutil
import stat

import pytest

from canine.localization.base import STAGED_SCRIPTS, AbstractLocalizer

EXECUTABLE = {"debug.sh", "parallel_download.py"}


class TestStagedScriptSet:

    def test_parallel_download_is_staged(self):
        assert "parallel_download.py" in STAGED_SCRIPTS

    def test_the_previously_staged_scripts_are_still_there(self):
        assert "delocalization.py" in STAGED_SCRIPTS
        assert "debug.sh" in STAGED_SCRIPTS

    @pytest.mark.parametrize("script", STAGED_SCRIPTS)
    def test_source_exists(self, script):
        assert os.path.isfile(AbstractLocalizer.staged_script_source(script))

    @pytest.mark.parametrize("script", sorted(EXECUTABLE))
    def test_directly_invoked_scripts_are_executable_at_source(self, script):
        mode = os.stat(AbstractLocalizer.staged_script_source(script)).st_mode
        assert mode & stat.S_IXUSR, "{} must be committed 0755".format(script)

    def test_delocalization_is_not_executable_and_needs_no_shebang(self):
        """
        Documents the asymmetry: delocalization.py is 0644 with no shebang and is only
        ever run as `python3 <path>`, so its mode is deliberately not fixed up.
        """
        source = AbstractLocalizer.staged_script_source("delocalization.py")
        assert not os.stat(source).st_mode & stat.S_IXUSR
        with open(source) as fh:
            assert not fh.readline().startswith("#!")


class TestCopyStagedScripts:

    def test_all_scripts_land(self, tmp_path):
        AbstractLocalizer.copy_staged_scripts(str(tmp_path))
        for script in STAGED_SCRIPTS:
            assert (tmp_path / script).is_file(), script

    @pytest.mark.parametrize("script", sorted(EXECUTABLE))
    def test_executable_bit_survives_the_copy(self, tmp_path, script):
        """
        The regression this function exists for. The staged copy is invoked directly, so
        losing the bit silently changed how it had to be run.
        """
        AbstractLocalizer.copy_staged_scripts(str(tmp_path))
        assert os.stat(str(tmp_path / script)).st_mode & stat.S_IXUSR

    def test_contents_are_identical(self, tmp_path):
        AbstractLocalizer.copy_staged_scripts(str(tmp_path))
        for script in STAGED_SCRIPTS:
            source = AbstractLocalizer.staged_script_source(script)
            assert (tmp_path / script).read_bytes() == open(source, "rb").read()

    def test_a_bare_copyfile_would_have_dropped_the_bit(self, tmp_path):
        """
        Pins down why the mode is restored explicitly -- otherwise a future edit could
        quietly reintroduce the original bug.
        """
        source = AbstractLocalizer.staged_script_source("parallel_download.py")
        destination = str(tmp_path / "via_copyfile.py")
        shutil.copyfile(source, destination)
        assert not os.stat(destination).st_mode & stat.S_IXUSR

    def test_shutil_copy_would_not_tolerate_a_chmod_refusal(self, tmp_path, monkeypatch):
        """
        And pins down why shutil.copy is NOT used: it calls copymode internally, so a
        filesystem that refuses chmod makes the copy itself raise -- outside any
        try/except we could wrap around it -- and localization fails instead of
        degrading. This is the trap the obvious implementation falls into.
        """
        def refuse(path, mode, **kwargs):
            raise OSError(1, "Operation not permitted")

        monkeypatch.setattr(os, "chmod", refuse)
        source = AbstractLocalizer.staged_script_source("parallel_download.py")
        with pytest.raises(OSError):
            shutil.copy(source, str(tmp_path / "via_copy.py"))

    def test_a_chmod_failure_is_tolerated(self, tmp_path, monkeypatch):
        """
        gcsfuse cannot represent an exec bit at all and NFS root_squash may refuse the
        call. Neither may fail localization: the emitted command falls back to
        `python3 <path>`.
        """
        def refuse(path, mode):
            raise OSError(1, "Operation not permitted")

        monkeypatch.setattr(os, "chmod", refuse)
        AbstractLocalizer.copy_staged_scripts(str(tmp_path))   # must not raise
        for script in STAGED_SCRIPTS:
            assert (tmp_path / script).is_file(), script

    def test_overwrites_an_existing_staged_copy(self, tmp_path):
        """Staging runs again on a requeue, so it has to be idempotent."""
        stale = tmp_path / "parallel_download.py"
        stale.write_text("stale contents")
        AbstractLocalizer.copy_staged_scripts(str(tmp_path))
        source = AbstractLocalizer.staged_script_source("parallel_download.py")
        assert stale.read_bytes() == open(source, "rb").read()


class TestStagedCopyIsRunnable:

    def test_the_staged_parallel_download_runs(self, tmp_path):
        """
        End of the chain: the copy that lands in CANINE_ROOT must actually be invocable
        directly, which is what the exec bit is for.
        """
        import subprocess

        AbstractLocalizer.copy_staged_scripts(str(tmp_path))
        staged = str(tmp_path / "parallel_download.py")
        proc = subprocess.run([staged, "--help"], capture_output=True, text=True,
                              timeout=60)
        assert proc.returncode == 0, proc.stderr
        assert "--connections" in proc.stdout

    def test_the_staged_copy_keeps_the_eof_sentinel(self, tmp_path):
        """
        The resolver checks for this line so a truncated or partially-visible staged copy
        is skipped rather than executed. A copy that dropped it would be rejected on the
        node.
        """
        AbstractLocalizer.copy_staged_scripts(str(tmp_path))
        with open(str(tmp_path / "parallel_download.py")) as fh:
            assert fh.read().rstrip().endswith("# k9pdl-eof")
