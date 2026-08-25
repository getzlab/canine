"""
Pure unit tests for Orchestrator.job_avoid()'s purge decision -- no SLURM
cluster, no Docker required.

job_avoid() inspects a previous run's shards and deletes the job directory of
every one it considers failed. "Failed" was set both by reading a nonzero exit
code and by simply failing to find the workspace or the exit code files -- and
outputs/ is a symlink farm pointing back into the directory being deleted, so a
shard that actually succeeded but whose exit codes could not be read had its
results destroyed and left dangling symlinks behind.

That is a latent bug on any backend. gcsfuse makes it likely rather than rare:
NegativeTtlSecs means a *miss* is cached too, so a stale lookup returns "not
found" for a file that exists (NFS-FUSE-IMPLEMENTATION-PLAN.md 8.4, item 2).

These tests pin the distinction: an unreadable verdict still re-runs the shard,
but only a verdict we actually read and found nonzero may delete anything.
"""
import io
import pandas as pd
from unittest.mock import MagicMock, patch

import pytest

from canine.orchestrator import Orchestrator


EXIT_CODE_FILES = [".job_exit_code", ".localizer_exit_code", ".teardown_exit_code"]


class FakeTransport:
    """
    Filesystem stub. `present` maps a path suffix to its contents; anything not
    listed is reported absent, which is exactly the condition under test.
    """
    def __init__(self, dirs, files):
        self.dirs = set(dirs)
        self.files = dict(files)
        self.removed = []

    def isdir(self, path):
        return path in self.dirs

    def isfile(self, path):
        return path in self.files

    def exists(self, path):
        return path in self.dirs or path in self.files

    def open(self, path, mode="r"):
        return io.StringIO(self.files[path])

    def rmtree(self, path):
        self.removed.append(path)

    def makedirs(self, path):
        self.dirs.add(path)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def run_job_avoid(shards, dirs, files):
    """
    Drive job_avoid() over `shards` against a stubbed filesystem.
    Returns (transport, orchestrator) so callers can inspect what was removed.
    """
    orch = Orchestrator.__new__(Orchestrator)
    orch.job_spec = {s: {"input": "value"} for s in shards}
    orch.raw_outputs = {}

    # job_avoid only inspects anything if the staging dir already exists
    transport = FakeTransport(list(dirs) + ["/mnt/nfs/staging"], files)

    localizer = MagicMock()
    localizer.staging_dir = "/mnt/nfs/staging"
    localizer.transport_context.return_value = transport
    localizer.environment.side_effect = lambda loc: {
      "CANINE_JOBS": "/mnt/nfs/staging/jobs",
      "CANINE_OUTPUT": "/mnt/nfs/staging/outputs",
    }
    localizer.reserve_path.side_effect = \
      lambda *a: MagicMock(remotepath="/mnt/nfs/staging/" + "/".join(str(x) for x in a))

    # shards that are not failed go on to have their output manifest compared
    # against raw_outputs; return an empty one so that comparison succeeds
    # against the empty raw_outputs above, rather than touching a real file
    empty_manifest = pd.DataFrame(columns=["shard", "output", "pattern", "path"])
    with patch("canine.orchestrator.pd.read_csv", return_value=empty_manifest):
        orch.job_avoid(localizer)
    return transport


def job_paths(shard, workspace=True, exit_codes=None):
    """
    Build the (dirs, files) a shard in a given state would leave behind.
    exit_codes: dict of filename -> contents, for the ones that exist.
    """
    root = "/mnt/nfs/staging/jobs/{}".format(shard)
    dirs = [root] + (["{}/workspace".format(root)] if workspace else [])
    files = {"{}/{}".format(root, k): v for k, v in (exit_codes or {}).items()}
    return dirs, files


class TestPurgeRequiresPositiveFailure:

    def test_confirmed_failure_is_purged(self):
        """A verdict we read, that said failure. The one case that may delete."""
        dirs, files = job_paths("0", exit_codes={
          ".job_exit_code": "1", ".localizer_exit_code": "0", ".teardown_exit_code": "0",
        })
        transport = run_job_avoid(["0"], dirs, files)
        assert transport.removed == ["/mnt/nfs/staging/jobs/0"]

    def test_missing_exit_code_is_not_purged(self):
        """
        The regression. A shard whose workspace is present but whose exit code
        is not readable might have succeeded -- a stale negative lookup is
        indistinguishable from a shard that died before writing it.
        """
        dirs, files = job_paths("0", exit_codes={})
        transport = run_job_avoid(["0"], dirs, files)
        assert transport.removed == []

    def test_partially_readable_exit_codes_are_not_purged(self):
        """
        Two of three readable and zero, the third missing. Still no verdict.
        """
        dirs, files = job_paths("0", exit_codes={
          ".localizer_exit_code": "0", ".teardown_exit_code": "0",
        })
        transport = run_job_avoid(["0"], dirs, files)
        assert transport.removed == []

    def test_missing_workspace_is_not_purged(self):
        dirs, files = job_paths("0", workspace=False, exit_codes={})
        transport = run_job_avoid(["0"], dirs, files)
        assert transport.removed == []

    def test_shard_that_never_ran_is_not_purged(self):
        """No directory at all -- nothing to delete, and nothing anomalous."""
        transport = run_job_avoid(["0"], [], {})
        assert transport.removed == []

    def test_successful_shard_is_not_purged(self):
        dirs, files = job_paths("0", exit_codes={
          ".job_exit_code": "0", ".localizer_exit_code": "0", ".teardown_exit_code": "0",
        })
        transport = run_job_avoid(["0"], dirs, files)
        assert transport.removed == []

    def test_only_the_confirmed_shard_is_purged_among_many(self):
        """
        The mixed case that matters in practice: one genuinely failed shard
        alongside one whose verdict is unreadable. Exactly one may be deleted.
        """
        dirs, files = [], {}
        d, f = job_paths("0", exit_codes={
          ".job_exit_code": "1", ".localizer_exit_code": "0", ".teardown_exit_code": "0",
        })
        dirs += d; files.update(f)
        d, f = job_paths("1", exit_codes={})          # unreadable verdict
        dirs += d; files.update(f)
        d, f = job_paths("2", exit_codes={            # succeeded
          ".job_exit_code": "0", ".localizer_exit_code": "0", ".teardown_exit_code": "0",
        })
        dirs += d; files.update(f)

        transport = run_job_avoid(["0", "1", "2"], dirs, files)
        assert transport.removed == ["/mnt/nfs/staging/jobs/0"]


class TestIndeterminateShardsAreReported:

    def test_unreadable_verdict_warns(self):
        import canine.orchestrator as orch_mod
        dirs, files = job_paths("0", exit_codes={})
        with patch.object(orch_mod.canine_logging, "warning") as warn:
            run_job_avoid(["0"], dirs, files)
            assert warn.called
            assert "Could not determine the outcome" in warn.call_args[0][0]

    def test_shard_that_never_ran_does_not_warn(self):
        """
        Adding shards to a rerun is routine and must stay quiet -- otherwise the
        warning is noise and gets ignored when it matters.
        """
        import canine.orchestrator as orch_mod
        with patch.object(orch_mod.canine_logging, "warning") as warn:
            run_job_avoid(["0"], [], {})
            warn.assert_not_called()
