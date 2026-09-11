"""
Pure unit tests for directory-typed outputs under local_workdir.

Regression coverage for a silent failure. A task may declare a directory as its
output -- SVelfie's only output is `{"model_results": "model_results/"}`. Under
local_workdir that output is recorded as a symlink into a bucket that is not
mounted on the controller, so it is broken by design. `glob.glob("x/")` returns []
for a broken symlink, because the trailing slash requires a real directory.

The result was an empty output list and *no error*: the task appeared to succeed
having produced nothing, and downstream tasks got no inputs.
"""
import glob
import os
from unittest.mock import MagicMock

import pytest

from canine.localization.base import BUCKETMOUNT_ROOT
from canine.localization.nfs import NFSLocalizer

BUCKET = "wolf-123456789012-us-central1-ns"


def make_localizer(tmp_path, workdir_mode="local"):
    staging = tmp_path / "task__abc"
    staging.mkdir(exist_ok=True)
    kwargs = (
      {"workdir_mode": workdir_mode, "results_bucket": BUCKET}
      if workdir_mode != "shared" else {}
    )
    return NFSLocalizer(MagicMock(), staging_dir=str(staging), **kwargs)


class TestGlobPremise:
    """The stdlib behaviour this whole fix exists for."""

    def test_trailing_slash_does_not_match_a_broken_symlink(self, tmp_path):
        d = tmp_path / "out"
        d.mkdir()
        (d / "model_results").symlink_to("{}/{}/p/0/model_results".format(BUCKETMOUNT_ROOT, BUCKET))
        assert glob.glob(str(d / "model_results") + "/") == []
        assert glob.glob(str(d / "model_results")) != []


class TestGlobOutputs:

    def _outdir(self, tmp_path, link_target=None):
        d = tmp_path / "outputs" / "0" / "model_results"
        d.mkdir(parents=True)
        (d / "model_results").symlink_to(
          link_target or "{}/{}/task__abc/0/model_results/model_results".format(
            BUCKETMOUNT_ROOT, BUCKET
          )
        )
        return d

    def test_directory_pattern_matches_the_broken_link(self, tmp_path):
        loc = make_localizer(tmp_path)
        d = self._outdir(tmp_path)
        assert loc.glob_outputs(str(d), "model_results/") == [str(d / "model_results")]

    def test_file_pattern_still_works(self, tmp_path):
        loc = make_localizer(tmp_path)
        d = tmp_path / "outputs" / "0" / "bam"
        d.mkdir(parents=True)
        (d / "out.bam").symlink_to("{}/{}/p/0/bam/out.bam".format(BUCKETMOUNT_ROOT, BUCKET))
        assert loc.glob_outputs(str(d), "out.bam") == [str(d / "out.bam")]

    def test_wildcard_pattern_still_works(self, tmp_path):
        loc = make_localizer(tmp_path)
        d = tmp_path / "outputs" / "0" / "vcf"
        d.mkdir(parents=True)
        (d / "a.vcf").symlink_to("{}/{}/p/0/vcf/a.vcf".format(BUCKETMOUNT_ROOT, BUCKET))
        assert loc.glob_outputs(str(d), "*.vcf") == [str(d / "a.vcf")]

    def test_genuinely_absent_output_still_yields_nothing(self, tmp_path):
        """The retry must not invent matches for a task that produced nothing."""
        loc = make_localizer(tmp_path)
        d = tmp_path / "outputs" / "0" / "model_results"
        d.mkdir(parents=True)
        assert loc.glob_outputs(str(d), "model_results/") == []

    def test_default_path_is_untouched(self, tmp_path):
        """
        Without local_workdir the retry must not fire -- a real directory output
        on the shared mount matches the trailing-slash pattern normally, and a
        broken link there would be a genuine fault we should not paper over.
        """
        loc = make_localizer(tmp_path, workdir_mode="shared")
        d = tmp_path / "outputs" / "0" / "model_results"
        d.mkdir(parents=True)
        (d / "model_results").symlink_to("/nonexistent/target")
        assert loc.glob_outputs(str(d), "model_results/") == []

    def test_real_directory_output_matches_without_the_retry(self, tmp_path):
        """A real directory (the pre-change shape) still matches directly."""
        loc = make_localizer(tmp_path, workdir_mode="shared")
        d = tmp_path / "outputs" / "0" / "model_results"
        (d / "model_results").mkdir(parents=True)
        assert loc.glob_outputs(str(d), "model_results/") == [str(d / "model_results") + "/"]


class TestEndToEndForADirectoryOutput:
    """glob_outputs feeding bucketmount_url_from_link, the way delocalize() does."""

    def test_directory_output_becomes_a_bucketmount_url(self, tmp_path):
        loc = make_localizer(tmp_path)
        d = tmp_path / "outputs" / "0" / "model_results"
        d.mkdir(parents=True)
        (d / "model_results").symlink_to(
          "{}/{}/task__abc/0/model_results/model_results".format(BUCKETMOUNT_ROOT, BUCKET)
        )
        matched = loc.glob_outputs(str(d), "model_results/")
        assert [loc.bucketmount_url_from_link(p) for p in matched] == [
          "bucketmount://{}/task__abc/0/model_results/model_results".format(BUCKET)
        ]
