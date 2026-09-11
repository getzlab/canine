"""
Pure unit tests for job avoidance against bucket-backed outputs.

Two distinct problems are covered here, and the first is easy to miss:

  * Under local_workdir the job workspace lives on the worker's own disk, so
    <staging_dir>/jobs/<shard>/workspace never exists. job_avoid() treats a
    missing workspace as a failed shard (that check exists to disable avoidance
    for scratch disks), so without an exemption every shard looks failed and
    avoidance silently never fires -- every re-run redoes all the work.

  * Once avoidance *can* fire, a matching manifest is no longer sufficient
    evidence. The manifest lives on the shared mount; the data lives in the
    results bucket under a daysSinceCustomTime lifecycle rule. They expire
    independently, so a shard can have an intact manifest and no objects.
    Skipping it would hand downstream tasks bucketmount:// URLs resolving to
    nothing.
"""
import os
from unittest.mock import MagicMock, patch

import pytest

from canine.localization.base import BUCKETMOUNT_ROOT
from canine.localization.nfs import NFSLocalizer

BUCKET = "wolf-123456789012-us-central1-ns"


def make_localizer(tmp_path, prefix="task__abc"):
    staging = tmp_path / prefix
    staging.mkdir(exist_ok=True)
    return NFSLocalizer(
      MagicMock(), staging_dir=str(staging),
      workdir_mode="local", results_bucket=BUCKET,
    )


def make_shard_outputs(output_dir, jobId, objpaths, bucket=BUCKET):
    """Lay out a shard's outputs dir the way delocalization.py does."""
    shard = output_dir / str(jobId)
    for objpath in objpaths:
        dest = shard / os.path.basename(objpath)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.symlink_to("{}/{}/{}".format(BUCKETMOUNT_ROOT, bucket, objpath))
    # real files delocalization also drops here; must be ignored
    shard.mkdir(parents=True, exist_ok=True)
    (shard / ".canine_job_manifest").write_text("0\tbam\tout.bam\t0/bam/out.bam\n")
    return shard


class TestBucketOutputsPresent:

    def _run(self, tmp_path, objpaths, present):
        loc = make_localizer(tmp_path)
        out = tmp_path / "outputs"
        out.mkdir()
        make_shard_outputs(out, "0", objpaths)
        with patch.object(loc, "_existing_result_objects", return_value=set(present)):
            return loc.bucket_outputs_present(str(out), "0")

    def test_true_when_every_object_exists(self, tmp_path):
        objs = ["task__abc/0/bam/out.bam", "task__abc/0/bai/out.bai"]
        assert self._run(tmp_path, objs, objs) is True

    def test_false_when_an_object_has_expired(self, tmp_path):
        objs = ["task__abc/0/bam/out.bam", "task__abc/0/bai/out.bai"]
        # the bai aged out from under an intact manifest
        assert self._run(tmp_path, objs, objs[:1]) is False

    def test_false_when_all_objects_are_gone(self, tmp_path):
        objs = ["task__abc/0/bam/out.bam"]
        assert self._run(tmp_path, objs, []) is False

    def test_false_when_shard_dir_is_absent(self, tmp_path):
        loc = make_localizer(tmp_path)
        out = tmp_path / "outputs"
        out.mkdir()
        assert loc.bucket_outputs_present(str(out), "0") is False

    def test_false_when_there_are_no_bucket_links(self, tmp_path):
        """
        A shard whose outputs dir holds only real files vouches for nothing --
        do not report it complete just because nothing failed to match.
        """
        loc = make_localizer(tmp_path)
        out = tmp_path / "outputs"
        shard = out / "0"
        shard.mkdir(parents=True)
        (shard / ".canine_job_manifest").write_text("x")
        with patch.object(loc, "_existing_result_objects", return_value=set()):
            assert loc.bucket_outputs_present(str(out), "0") is False

    def test_links_to_a_different_bucket_are_rejected(self, tmp_path):
        loc = make_localizer(tmp_path)
        out = tmp_path / "outputs"
        out.mkdir()
        make_shard_outputs(out, "0", ["task__abc/0/bam/out.bam"], bucket="some-other-bucket")
        with patch.object(loc, "_existing_result_objects", return_value={"task__abc/0/bam/out.bam"}):
            assert loc.bucket_outputs_present(str(out), "0") is False

    def test_real_sidecar_files_are_ignored(self, tmp_path):
        """.crc32c sidecars and the manifest stay on the shared mount."""
        loc = make_localizer(tmp_path)
        out = tmp_path / "outputs"
        out.mkdir()
        shard = make_shard_outputs(out, "0", ["task__abc/0/bam/out.bam"])
        (shard / ".out.bam.crc32c").write_text("DEADBEEF")
        with patch.object(loc, "_existing_result_objects", return_value={"task__abc/0/bam/out.bam"}):
            assert loc.bucket_outputs_present(str(out), "0") is True

    def test_listing_failure_is_treated_as_absent(self, tmp_path):
        """
        Re-running a shard is wasteful; wrongly skipping one is incorrect. A
        bucket we cannot list must not be read as 'everything is fine'.
        """
        loc = make_localizer(tmp_path)
        out = tmp_path / "outputs"
        out.mkdir()
        make_shard_outputs(out, "0", ["task__abc/0/bam/out.bam"])
        with patch.object(loc, "_existing_result_objects", side_effect=RuntimeError("403")):
            assert loc.bucket_outputs_present(str(out), "0") is False


class TestDirectoryTypedOutputs:
    """
    A directory output is a single symlink standing for a whole prefix -- SVelfie's
    only output is "model_results/". No object is ever named exactly that path, only
    objects beneath it, so exact membership reported every such shard as missing and
    re-ran it. For SVelfie that is 4.5 hours of analysis repeated per shard.
    """

    def _run(self, tmp_path, link_objpath, present):
        loc = make_localizer(tmp_path)
        out = tmp_path / "outputs"
        out.mkdir()
        make_shard_outputs(out, "0", [link_objpath])
        with patch.object(loc, "_existing_result_objects", return_value=set(present)):
            return loc.bucket_outputs_present(str(out), "0")

    def test_directory_output_is_present_when_objects_live_beneath_it(self, tmp_path):
        assert self._run(
          tmp_path,
          "task__abc/0/model_results/model_results",
          ["task__abc/0/model_results/model_results/ucs/QQ_plots/qq.png",
           "task__abc/0/model_results/model_results/ucs/results.tsv"],
        ) is True

    def test_directory_output_is_absent_when_the_prefix_is_empty(self, tmp_path):
        assert self._run(
          tmp_path, "task__abc/0/model_results/model_results", []
        ) is False

    def test_a_merely_similar_prefix_does_not_count(self, tmp_path):
        """`model_results_old/...` must not vouch for `model_results/`."""
        assert self._run(
          tmp_path,
          "task__abc/0/model_results/model_results",
          ["task__abc/0/model_results/model_results_old/x.tsv"],
        ) is False

    def test_exact_file_match_still_works(self, tmp_path):
        assert self._run(
          tmp_path, "task__abc/0/bam/out.bam", ["task__abc/0/bam/out.bam"]
        ) is True


class TestListingIsCached:

    def test_bucket_is_listed_once_per_task(self, tmp_path):
        """
        An array job can have thousands of shards; avoidance must not cost a
        listing per shard.
        """
        loc = make_localizer(tmp_path)
        client = MagicMock()
        client.list_blobs.return_value = []
        with patch("canine.localization.base.gcloud_storage_client", return_value=client):
            loc._existing_result_objects()
            loc._existing_result_objects()
            loc._existing_result_objects()
        assert client.list_blobs.call_count == 1

    def test_listing_is_scoped_to_this_task_prefix(self, tmp_path):
        """Other tasks share the bucket; listing it whole would be wasteful."""
        loc = make_localizer(tmp_path, prefix="task__abc")
        client = MagicMock()
        client.list_blobs.return_value = []
        with patch("canine.localization.base.gcloud_storage_client", return_value=client):
            loc._existing_result_objects()
        assert client.list_blobs.call_args.kwargs["prefix"] == "task__abc/"
