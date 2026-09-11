"""
Pure unit tests for the bucket-backed output round trip under local_workdir.

The producer half runs on the worker (delocalization.py): it uploads each output
to the results bucket and leaves a symlink pointing at the future gcsfuse mount,
deliberately broken until a downstream task mounts that bucket. The consumer half
runs on the controller (NFSLocalizer.delocalize): it reads those links back out as
bucketmount:// URLs, which route downstream tasks through the existing
bucket_mount branch -- symlink into a read-only mount, never a copy.

The two halves are coupled through the exact text of the symlink, and they live in
different processes on different machines, so the round trip is what these tests
pin down.
"""
import os
from unittest.mock import MagicMock

import pytest

from canine.localization.base import BUCKETMOUNT_ROOT
from canine.localization.local import BatchedLocalizer
from canine.localization.nfs import NFSLocalizer


def make_nfs_localizer(tmp_path, **kwargs):
    kwargs.setdefault("results_bucket", "wolf-123456789012-us-central1-ns")
    kwargs.setdefault("workdir_mode", "local")
    return NFSLocalizer(MagicMock(), staging_dir=str(tmp_path), **kwargs)


class TestBucketmountRoundTrip:
    """delocalization.py writes the link; NFSLocalizer reads it back."""

    def test_link_becomes_bucketmount_url(self, tmp_path):
        loc = make_nfs_localizer(tmp_path)
        link = tmp_path / "out.bam"
        link.symlink_to(
          "{}/wolf-123456789012-us-central1-ns/task__abc/0/bam/out.bam".format(BUCKETMOUNT_ROOT)
        )
        assert loc.bucketmount_url_from_link(str(link)) == \
            "bucketmount://wolf-123456789012-us-central1-ns/task__abc/0/bam/out.bam"

    def test_works_on_a_broken_link(self, tmp_path):
        """
        The link is broken on the controller by design -- the bucket is only
        mounted on the worker that consumes it. Reading it must not resolve it.
        """
        loc = make_nfs_localizer(tmp_path)
        link = tmp_path / "out.bam"
        link.symlink_to("{}/some-bucket/p/0/bam/out.bam".format(BUCKETMOUNT_ROOT))
        assert not os.path.exists(link)   # broken
        assert os.path.lexists(link)      # but present
        assert loc.bucketmount_url_from_link(str(link)).startswith("bucketmount://some-bucket/")

    def test_nested_object_paths_survive(self, tmp_path):
        loc = make_nfs_localizer(tmp_path)
        link = tmp_path / "deep"
        link.symlink_to("{}/b/pfx/0/name/a/b/c.txt".format(BUCKETMOUNT_ROOT))
        assert loc.bucketmount_url_from_link(str(link)) == "bucketmount://b/pfx/0/name/a/b/c.txt"

    def test_url_matches_the_consumer_regex(self, tmp_path):
        """
        HandleBucketMountURL requires bucketmount://<bucket>/<seg>/<rest> with a
        non-empty third group. A URL that does not satisfy it is rejected at
        hash time, well after this point, so check it here.
        """
        from canine.localization.file_handlers import HandleBucketMountURL
        loc = make_nfs_localizer(tmp_path)
        link = tmp_path / "out.bam"
        link.symlink_to("{}/bkt/task__abc/0/bam/out.bam".format(BUCKETMOUNT_ROOT))
        url = loc.bucketmount_url_from_link(str(link))
        assert HandleBucketMountURL(url)._get_hash() == url

    def test_a_link_outside_the_mount_root_raises(self, tmp_path):
        """
        A plain intra-workspace symlink reaching this code means the producer and
        consumer disagree about the mode. Fail loudly rather than emit a
        bucketmount:// URL naming a bucket that does not exist.
        """
        loc = make_nfs_localizer(tmp_path)
        link = tmp_path / "out.bam"
        link.symlink_to("../../jobs/0/workspace/out.bam")
        with pytest.raises(ValueError, match="does not point into"):
            loc.bucketmount_url_from_link(str(link))

    def test_scratch_disk_link_is_not_mistaken_for_a_bucket(self, tmp_path):
        loc = make_nfs_localizer(tmp_path)
        link = tmp_path / "out.bam"
        link.symlink_to("/mnt/canine-scratch-abc/out.bam")
        with pytest.raises(ValueError, match="does not point into"):
            loc.bucketmount_url_from_link(str(link))


class TestResultsPrefix:

    def test_prefix_is_the_task_directory_name(self, tmp_path):
        d = tmp_path / "mutect1__2026-09-09_deadbeef"
        d.mkdir()
        loc = make_nfs_localizer(d)
        assert loc.results_prefix() == "mutect1__2026-09-09_deadbeef"

    def test_trailing_slash_does_not_blank_the_prefix(self, tmp_path):
        """os.path.basename('a/b/') is '' -- normpath first."""
        d = tmp_path / "task__abc"
        d.mkdir()
        loc = make_nfs_localizer(d)
        loc.staging_dir = str(d) + "/"
        assert loc.results_prefix() == "task__abc"

    def test_identical_task_reuses_one_prefix(self, tmp_path):
        """
        The staging dir name encodes the script/docker/input hashes, so a rerun of
        an unchanged task converges on the same prefix and `cp -n` no-ops.
        """
        d = tmp_path / "task__abc"
        d.mkdir()
        assert make_nfs_localizer(d).results_prefix() == make_nfs_localizer(d).results_prefix()


class TestTeardownWiring:
    """
    The flags base.py hands delocalization.py. Asserted on the localizer rather
    than a rendered script because job_setup_teardown() needs a live backend.
    """

    def test_results_bucket_is_required_for_local_workdir(self):
        with pytest.raises(ValueError, match="results_bucket"):
            BatchedLocalizer(MagicMock(), workdir_mode="local")

    def test_results_bucket_recorded(self, tmp_path):
        loc = make_nfs_localizer(tmp_path, results_bucket="my-bucket")
        assert loc.results_bucket == "my-bucket"

    def test_off_by_default(self, tmp_path):
        loc = NFSLocalizer(MagicMock(), staging_dir=str(tmp_path))
        assert loc.workdir_mode == "shared"
        assert loc.results_bucket is None
