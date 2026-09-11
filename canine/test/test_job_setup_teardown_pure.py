"""
Executes job_setup_teardown() end to end and asserts on the scripts it renders.

Every other pure test of this method inspects its *source* with inspect.getsource()
rather than running it, which is why an UnboundLocalError in it reached a live
cluster: `scratch_disk_prefix` is only assigned inside `if self.use_scratch_disk:`,
but was passed unconditionally to job_workspace_path(). The original expression had
short-circuited (`... if not self.use_scratch_disk else scratch_disk_prefix`), so
refactoring it into a call silently made the reference eager.

Rendering the scripts is cheap; the method needs only a jobId with inputs and a
transport, both mockable. Anything that actually runs it catches that whole class
of bug, so the assertions below are deliberately about *what the scripts say*
rather than about implementation details.
"""
from unittest.mock import MagicMock

import pytest

from canine.localization.nfs import NFSLocalizer

BUCKET = "wolf-123456789012-us-central1-ns"
PATTERNS = {"out": "out.txt"}


def render(tmp_path, **kwargs):
    """Build a localizer, give shard 0 an empty input set, render its scripts."""
    staging = tmp_path / "task__abc"
    staging.mkdir(exist_ok=True)
    loc = NFSLocalizer(MagicMock(), staging_dir=str(staging), **kwargs)
    loc.inputs = {"0": {}}
    setup, localization, teardown, arrays = loc.job_setup_teardown(
      "0", PATTERNS, transport=MagicMock()
    )
    return loc, setup, localization, teardown


class TestRendersWithoutScratchDisk:
    """The default configuration -- and the one the UnboundLocalError hit."""

    def test_default_renders(self, tmp_path):
        _, setup, _, teardown = render(tmp_path)
        assert setup.startswith("#!/bin/bash")
        assert teardown.startswith("#!/bin/bash")

    def test_workspace_is_on_the_shared_mount(self, tmp_path):
        loc, setup, _, _ = render(tmp_path)
        assert 'export CANINE_JOB_WORKSPACE="{}/jobs/0/workspace"'.format(loc.staging_dir) in setup

    def test_no_tmpdir_override(self, tmp_path):
        _, setup, _, _ = render(tmp_path)
        assert "export TMPDIR=" not in setup

    def test_no_results_flags_in_teardown(self, tmp_path):
        _, _, _, teardown = render(tmp_path)
        assert "--results_bucket" not in teardown


class TestRendersWithLocalWorkdir:

    def test_local_workdir_renders(self, tmp_path):
        _, setup, _, teardown = render(
          tmp_path, workdir_mode="local", results_bucket=BUCKET
        )
        assert setup.startswith("#!/bin/bash")
        assert teardown.startswith("#!/bin/bash")

    def test_workspace_is_off_the_shared_mount(self, tmp_path):
        loc, setup, _, _ = render(tmp_path, workdir_mode="local", results_bucket=BUCKET)
        assert 'export CANINE_JOB_WORKSPACE="/mnt/local_workdir/0/workspace"' in setup
        assert 'CANINE_JOB_WORKSPACE="{}'.format(loc.staging_dir) not in setup

    def test_tmpdir_points_at_local_disk(self, tmp_path):
        _, setup, _, _ = render(tmp_path, workdir_mode="local", results_bucket=BUCKET)
        assert 'export TMPDIR="/mnt/local_workdir/0/tmp"' in setup
        assert "-Djava.io.tmpdir=$TMPDIR" in setup

    def test_teardown_passes_results_flags(self, tmp_path):
        _, _, _, teardown = render(tmp_path, workdir_mode="local", results_bucket=BUCKET)
        assert "--results_bucket {}".format(BUCKET) in teardown
        assert "--results_prefix task__abc" in teardown

    def test_teardown_stamps_custom_time_itself(self, tmp_path):
        """
        localization.sh sets CANINE_BUCKET_CT, but that is a different process.
        Without a local stamp the uploads get an unset custom time and the results
        bucket's daysSinceCustomTime rule treats them as already expired.
        """
        _, _, _, teardown = render(tmp_path, workdir_mode="local", results_bucket=BUCKET)
        assert "CANINE_BUCKET_CT=$(date -u" in teardown
        assert '--custom_time "$CANINE_BUCKET_CT"' in teardown

    def test_custom_time_is_stamped_before_it_is_used(self, tmp_path):
        _, _, _, teardown = render(tmp_path, workdir_mode="local", results_bucket=BUCKET)
        assert teardown.index("CANINE_BUCKET_CT=$(date -u") < teardown.index("--custom_time")
