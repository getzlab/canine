"""
Pure unit tests for delocalization.py's results-bucket branch (the worker half).

delocalization.py runs standalone on the worker -- it is copied into the staging
dir rather than imported from the canine package -- so it is exercised here by
importing the module directly and stubbing out the gcloud calls.

The property that matters most: object paths must include the shard id. Deriving
them relative to the job directory instead of the outputs root makes every shard
write to the same objects, which does not fail, it silently corrupts results.
"""
import os
from unittest.mock import patch

import pytest

from canine.localization import delocalization


def run_main(tmp_path, patterns, monkeypatch, **kwargs):
    """Set up a workspace + outputs dir, cd into the workspace, run main()."""
    workspace = tmp_path / "workspace"
    workspace.mkdir(exist_ok=True)
    outputs = tmp_path / "outputs"
    outputs.mkdir(exist_ok=True)
    monkeypatch.chdir(workspace)
    monkeypatch.setenv("CANINE_JOB_RC", "1")  # skip the crc32c pass

    calls = []

    def fake_upload(job):
        calls.append(job)
        return (job[1], 0, "")

    with patch.object(delocalization, "upload_one", fake_upload):
        delocalization.main(
          str(outputs), kwargs.pop("jobId", "0"), patterns, set(), False, False,
          **kwargs
        )
    return outputs, calls


class TestResultsBucketBranch:

    def test_uploads_and_writes_a_pointer_symlink(self, tmp_path, monkeypatch):
        (tmp_path / "workspace").mkdir(exist_ok=True)
        (tmp_path / "workspace" / "out.bam").write_text("data")
        outputs, calls = run_main(
          tmp_path, [("bam", "out.bam")], monkeypatch,
          results_bucket="bkt", results_prefix="task__abc", custom_time="2026-09-09T00:00:00Z",
        )

        dest = outputs / "0" / "bam" / "out.bam"
        assert os.path.islink(dest)
        assert os.readlink(dest) == \
            "{}/bkt/task__abc/0/bam/out.bam".format(delocalization.BUCKETMOUNT_ROOT)

        assert len(calls) == 1
        src, dst, ct = calls[0]
        assert dst == "gs://bkt/task__abc/0/bam/out.bam"
        assert ct == "2026-09-09T00:00:00Z"
        assert src.endswith("/workspace/out.bam")

    def test_object_path_includes_the_shard_id(self, tmp_path, monkeypatch):
        """
        Regression: deriving the object path relative to the job dir rather than
        the outputs root drops the shard id, so every shard of an array job
        overwrites the same object.
        """
        dsts = []
        for shard in ("0", "1"):
            d = tmp_path / shard
            d.mkdir()
            (d / "workspace").mkdir()
            (d / "workspace" / "out.bam").write_text(shard)
            _, calls = run_main(
              d, [("bam", "out.bam")], monkeypatch,
              jobId=shard, results_bucket="bkt", results_prefix="p",
            )
            dsts.append(calls[0][1])

        assert dsts == ["gs://bkt/p/0/bam/out.bam", "gs://bkt/p/1/bam/out.bam"]
        assert dsts[0] != dsts[1]

    def test_no_prefix_still_produces_a_valid_url(self, tmp_path, monkeypatch):
        """HandleBucketMountURL needs at least <bucket>/<seg>/<rest>."""
        (tmp_path / "workspace").mkdir(exist_ok=True)
        (tmp_path / "workspace" / "out.bam").write_text("x")
        _, calls = run_main(
          tmp_path, [("bam", "out.bam")], monkeypatch, results_bucket="bkt",
        )
        assert calls[0][1] == "gs://bkt/0/bam/out.bam"

    def test_nothing_is_copied_onto_the_shared_mount(self, tmp_path, monkeypatch):
        """
        The whole point: the outputs tree must contain links, not data. A real
        file here means the same_volume() copy branch won and every output byte
        crossed the controller.
        """
        (tmp_path / "workspace").mkdir(exist_ok=True)
        (tmp_path / "workspace" / "out.bam").write_text("payload")
        outputs, _ = run_main(
          tmp_path, [("bam", "out.bam")], monkeypatch, results_bucket="bkt", results_prefix="p",
        )
        dest = outputs / "0" / "bam" / "out.bam"
        assert os.path.islink(dest)
        assert not os.path.isfile(os.path.realpath(dest))

    def test_copy_list_still_copies(self, tmp_path, monkeypatch):
        """files_to_copy_to_outputs must keep landing real bytes on the mount."""
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        (workspace / "out.bam").write_text("payload")
        outputs = tmp_path / "outputs"
        outputs.mkdir()
        monkeypatch.chdir(workspace)
        monkeypatch.setenv("CANINE_JOB_RC", "1")

        calls = []
        with patch.object(delocalization, "upload_one", lambda j: (calls.append(j), (j[1], 0, ""))[1]):
            delocalization.main(
              str(outputs), "0", [("bam", "out.bam")], {"bam"}, False, False,
              results_bucket="bkt", results_prefix="p",
            )

        dest = outputs / "0" / "bam" / "out.bam"
        assert os.path.isfile(dest) and not os.path.islink(dest)
        assert dest.read_text() == "payload"
        assert calls == []

    def test_upload_failure_aborts_the_shard(self, tmp_path, monkeypatch):
        """
        A pointer symlink with no object behind it would hand downstream tasks a
        link resolving to nothing. Must fail the shard instead.
        """
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        (workspace / "out.bam").write_text("x")
        outputs = tmp_path / "outputs"
        outputs.mkdir()
        monkeypatch.chdir(workspace)
        monkeypatch.setenv("CANINE_JOB_RC", "1")

        with patch.object(delocalization, "upload_one", lambda j: (j[1], 1, "boom")):
            with pytest.raises(SystemExit) as e:
                delocalization.main(
                  str(outputs), "0", [("bam", "out.bam")], set(), False, False,
                  results_bucket="bkt", results_prefix="p",
                )
        assert e.value.code == 1


class TestStdoutStderrStayLocal:
    """
    Regression: uploading stdout/stderr to the bucket made them broken symlinks on
    the controller, so NFSLocalizer.delocalize()'s os.path.isfile() check dropped
    them and wolF's WolfTaskResults blew up with KeyError: 'stderr'. They are also
    the first thing anyone reads when debugging, so they belong on the shared mount.
    """

    @pytest.mark.parametrize("name", ["stdout", "stderr"])
    def test_not_uploaded(self, tmp_path, monkeypatch, name):
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        (workspace / name).write_text("log output")
        outputs, calls = run_main(
          tmp_path, [(name, name)], monkeypatch,
          results_bucket="bkt", results_prefix="p",
        )
        assert calls == []

    @pytest.mark.parametrize("name", ["stdout", "stderr"])
    def test_remains_readable_on_the_controller(self, tmp_path, monkeypatch, name):
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        (workspace / name).write_text("log output")
        outputs, _ = run_main(
          tmp_path, [(name, name)], monkeypatch,
          results_bucket="bkt", results_prefix="p",
        )
        dest = outputs / "0" / name
        # os.path.isfile() is what nfs.py uses to pick these up; it must resolve
        assert os.path.isfile(dest)

    def test_regular_outputs_still_go_to_the_bucket(self, tmp_path, monkeypatch):
        """The exclusion must be limited to stdout/stderr."""
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        (workspace / "stdout").write_text("log")
        (workspace / "out.bam").write_text("payload")
        _, calls = run_main(
          tmp_path, [("stdout", "stdout"), ("bam", "out.bam")], monkeypatch,
          results_bucket="bkt", results_prefix="p",
        )
        assert [c[1] for c in calls] == ["gs://bkt/p/0/bam/out.bam"]


class TestUnchangedBehaviour:

    def test_without_results_bucket_outputs_are_symlinked_locally(self, tmp_path, monkeypatch):
        """The default path must be untouched by any of this."""
        (tmp_path / "workspace").mkdir(exist_ok=True)
        (tmp_path / "workspace" / "out.bam").write_text("x")
        outputs, calls = run_main(tmp_path, [("bam", "out.bam")], monkeypatch)

        dest = outputs / "0" / "bam" / "out.bam"
        assert os.path.islink(dest)
        assert delocalization.BUCKETMOUNT_ROOT not in os.readlink(dest)
        assert calls == []
