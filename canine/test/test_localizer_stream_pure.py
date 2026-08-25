"""
Pure unit tests for streaming-input (FIFO) placement -- no SLURM cluster, no
Docker required (plain MagicMock backend, same approach as
test_localizer_requester_pays_pure.py, since test_localizer_batched.py's
module-level DummySlurmBackend needs a Docker image that is not reachable
everywhere).

Covers the two things NFS-FUSE-IMPLEMENTATION-PLAN.md phase 4 moves onto
node-local disk, both under the per-job root from
AbstractLocalizer.node_local_job_dir():

  B4 (streams/) -- gcsfuse implements no mknod, so `mkfifo` on the shared mount
  fails with "Operation not supported". localization.sh runs under `set -e`,
  which turns that into a hard failure of every `localization: stream` task.

  B7b (download/) -- gcloud's resumable-download tracker directory and its -L
  manifest. Not something gcsfuse refuses; just bookkeeping no job reads back,
  paid for in HTTP round trips if it stays on the shared mount.

Neither moves job data. A FIFO transports no bytes through the filesystem at
all, and B7b is metadata about a transfer whose payload still lands under
CANINE_ROOT -- an invariant asserted below, since it is what separates B7b from
B7a, which cannot move.
"""
import os
from unittest.mock import MagicMock, patch

import pytest

from canine.localization.local import BatchedLocalizer
from canine.localization import file_handlers


NODE_LOCAL_ROOT = "/tmp/canine"
STREAM_ROOT = NODE_LOCAL_ROOT
STAGING_DIR = "/mnt/nfs/canine-staging-0123abcd"


def make_localizer(**kwargs):
    loc = BatchedLocalizer(MagicMock(), **kwargs)
    # AbstractLocalizer runs staging_dir through transport.normpath(), which on
    # a MagicMock backend yields a MagicMock rather than a path. Pin a real one
    # so CANINE_ROOT is a string and the "not under the shared mount"
    # assertions below actually compare paths.
    loc.staging_dir = STAGING_DIR
    return loc


def make_stream_handler(path="gs://bucket/reads.bam"):
    # get_requester_pays() shells out; the bucket policy is irrelevant here
    with patch.object(file_handlers.HandleGSURLStream, "get_requester_pays", return_value=False):
        return file_handlers.HandleGSURLStream(path)


def make_literal_handler(value="just-a-string"):
    return file_handlers.StringLiteral(value)


def make_download_handler(path="gs://bucket/reads.bam"):
    """localization_mode == "url" -- an ordinary download, not a stream."""
    with patch.object(file_handlers.HandleGSURL, "get_requester_pays", return_value=False):
        return file_handlers.HandleGSURL(path)


def setup_job(loc, jobId, inputs):
    """
    inputs: {input_name: [handler, ...]}
    """
    loc.inputs = {jobId: inputs}
    loc.input_array_flag = {jobId: {k: False for k in inputs}}


def scripts_for(loc, jobId="0"):
    setup, localization, teardown = loc.job_setup_teardown(jobId, {})[:3]
    return setup, localization, teardown


def expected_stream_dir(loc, jobId="0"):
    return os.path.join(
      NODE_LOCAL_ROOT,
      os.path.basename(loc.environment("remote")["CANINE_ROOT"].rstrip("/")),
      jobId,
      "streams",
    )


class TestStreamFIFOPlacement:

    def test_fifo_is_created_on_node_local_disk(self):
        loc = make_localizer()
        setup_job(loc, "0", {"reads": [make_stream_handler()]})
        _, localization, _ = scripts_for(loc)

        fifo = os.path.join(expected_stream_dir(loc), "reads.bam")
        assert "mkfifo {}".format(fifo) in localization

    def test_fifo_is_not_created_under_the_shared_mount(self):
        """
        The actual regression: any mkfifo under CANINE_ROOT is what breaks on
        gcsfuse. Assert on the operation, not just on the new path, so this
        still fails if a second mkfifo site is added later.
        """
        loc = make_localizer()
        setup_job(loc, "0", {"reads": [make_stream_handler()]})
        _, localization, _ = scripts_for(loc)

        canine_root = loc.environment("remote")["CANINE_ROOT"]
        mkfifo_targets = [
          line.split("mkfifo ", 1)[1].strip()
          for line in localization.splitlines() if line.strip().startswith("mkfifo ")
        ]
        assert mkfifo_targets, "expected at least one mkfifo"
        for target in mkfifo_targets:
            assert not target.startswith(canine_root)
            assert target.startswith(STREAM_ROOT)

    def test_exported_input_variable_points_at_the_fifo(self):
        """
        The job reads its stream through the exported variable, so the export
        has to follow the FIFO to its new location.
        """
        loc = make_localizer()
        setup_job(loc, "0", {"reads": [make_stream_handler()]})
        setup, _, _ = scripts_for(loc)

        fifo = os.path.join(expected_stream_dir(loc), "reads.bam")
        assert "export reads={}".format(fifo) in setup

    def test_stream_dir_is_exported_and_created(self):
        loc = make_localizer()
        setup_job(loc, "0", {"reads": [make_stream_handler()]})
        setup, _, _ = scripts_for(loc)

        assert 'export CANINE_STREAM_DIR="{}"'.format(expected_stream_dir(loc)) in setup
        assert "mkdir -p $CANINE_STREAM_DIR" in setup

    def test_stream_dir_is_bind_mounted_into_the_task_container(self):
        """
        The FIFO is written from the worker container and read from the task
        container nested inside it. CANINE_ROOT's bind mount used to cover it;
        now it needs its own.
        """
        loc = make_localizer()
        setup_job(loc, "0", {"reads": [make_stream_handler()]})
        setup, _, _ = scripts_for(loc)

        docker_line = next(l for l in setup.splitlines() if l.startswith("export CANINE_DOCKER_ARGS="))
        assert "-v $CANINE_STREAM_DIR:$CANINE_STREAM_DIR" in docker_line

    def test_stream_dir_export_precedes_docker_args(self):
        """
        CANINE_DOCKER_ARGS is assigned inside double quotes, so
        $CANINE_STREAM_DIR in the bind mount is expanded at export time. If the
        export came later the mount would silently become "-v :".
        """
        loc = make_localizer()
        setup_job(loc, "0", {"reads": [make_stream_handler()]})
        setup, _, _ = scripts_for(loc)

        lines = setup.splitlines()
        stream_idx = next(i for i, l in enumerate(lines) if l.startswith("export CANINE_STREAM_DIR="))
        docker_idx = next(i for i, l in enumerate(lines) if l.startswith("export CANINE_DOCKER_ARGS="))
        assert stream_idx < docker_idx

    def test_teardown_removes_the_stream_dir(self):
        loc = make_localizer()
        setup_job(loc, "0", {"reads": [make_stream_handler()]})
        _, _, teardown = scripts_for(loc)

        assert 'if [[ -n "$CANINE_STREAM_DIR" ]]; then rm -rf $CANINE_STREAM_DIR; fi' in teardown

    def test_array_shards_do_not_collide(self):
        """
        Two shards of one array job can land on the same node and stream inputs
        with the same basename, so jobId has to be part of the path.
        """
        loc = make_localizer()

        setup_job(loc, "0", {"reads": [make_stream_handler()]})
        setup_0, _, _ = scripts_for(loc, "0")
        setup_job(loc, "1", {"reads": [make_stream_handler()]})
        setup_1, _, _ = scripts_for(loc, "1")

        dir_0 = expected_stream_dir(loc, "0")
        dir_1 = expected_stream_dir(loc, "1")
        assert dir_0 != dir_1
        assert 'export CANINE_STREAM_DIR="{}"'.format(dir_0) in setup_0
        assert 'export CANINE_STREAM_DIR="{}"'.format(dir_1) in setup_1


class TestNonStreamingJobsUnaffected:
    """
    The stream directory is per-job opt-in: a job that streams nothing must not
    get an extra bind mount or an empty directory in every task container.
    """

    def test_no_stream_dir_export_without_stream_inputs(self):
        loc = make_localizer()
        setup_job(loc, "0", {"label": [make_literal_handler()]})
        setup, _, _ = scripts_for(loc)

        assert "CANINE_STREAM_DIR" not in setup

    def test_no_bind_mount_without_stream_inputs(self):
        loc = make_localizer()
        setup_job(loc, "0", {"label": [make_literal_handler()]})
        setup, _, _ = scripts_for(loc)

        docker_line = next(l for l in setup.splitlines() if l.startswith("export CANINE_DOCKER_ARGS="))
        assert "CANINE_STREAM_DIR" not in docker_line
        assert "-v $CANINE_ROOT:$CANINE_ROOT" in docker_line

    def test_no_download_tracker_export_without_download_inputs(self):
        loc = make_localizer()
        setup_job(loc, "0", {"label": [make_literal_handler()]})
        setup, _, _ = scripts_for(loc)

        assert "CANINE_DOWNLOAD_TRACKER_DIR" not in setup

    def test_teardown_cleanup_is_inert_without_stream_inputs(self):
        """
        The teardown line is unconditional in the generated script, so it must
        no-op when the variable was never exported -- that is what the -n guard
        is for.
        """
        loc = make_localizer()
        setup_job(loc, "0", {"label": [make_literal_handler()]})
        setup, _, teardown = scripts_for(loc)

        assert 'if [[ -n "$CANINE_STREAM_DIR" ]]' in teardown
        assert "CANINE_STREAM_DIR" not in setup


def expected_download_dir(loc, jobId="0"):
    return os.path.join(
      NODE_LOCAL_ROOT,
      os.path.basename(loc.environment("remote")["CANINE_ROOT"].rstrip("/")),
      jobId,
      "download",
    )


class TestDownloadBookkeepingIsNodeLocal:
    """
    Blocker B7b. gcloud's resumable-download tracker directory and its -L
    manifest are written beside every download. Nothing reads them back, but on
    gcsfuse the tracker is a burst of small create/read/delete operations and
    the manifest is a read-modify-write of a whole object -- each one an HTTP
    round trip against shared storage, for bookkeeping.

    Only these move. The download itself has to land where the job expects it,
    which is the invariant the last test here pins.
    """

    def test_tracker_dir_is_node_local(self):
        loc = make_localizer()
        setup_job(loc, "0", {"reads": [make_download_handler()]})
        _, localization, _ = scripts_for(loc)

        assert 'CLOUDSDK_STORAGE_TRACKER_DIR="{}/.gcloud_tracker_dir"'.format(
          expected_download_dir(loc)) in localization

    def test_manifest_is_node_local(self):
        loc = make_localizer()
        setup_job(loc, "0", {"reads": [make_download_handler()]})
        _, localization, _ = scripts_for(loc)

        assert '-L "{}/.gcloud_manifest"'.format(expected_download_dir(loc)) in localization

    def test_the_download_itself_still_lands_on_the_shared_mount(self):
        """
        The non-regression that matters. B7's other half -- the payload being
        resumed in place -- cannot move, because the job reads it from
        CANINE_ROOT. If this ever fails, the bookkeeping change has dragged the
        data with it.
        """
        loc = make_localizer()
        setup_job(loc, "0", {"reads": [make_download_handler()]})
        setup, localization, _ = scripts_for(loc)

        canine_root = loc.environment("remote")["CANINE_ROOT"]
        expected_dest = os.path.join(canine_root, "jobs", "0", "inputs")
        assert expected_dest in localization
        assert "export reads={}/reads.bam".format(expected_dest) in setup

    def test_tracker_dir_is_exported_and_created(self):
        loc = make_localizer()
        setup_job(loc, "0", {"reads": [make_download_handler()]})
        setup, _, _ = scripts_for(loc)

        assert 'export CANINE_DOWNLOAD_TRACKER_DIR="{}"'.format(expected_download_dir(loc)) in setup
        assert "mkdir -p $CANINE_DOWNLOAD_TRACKER_DIR" in setup

    def test_not_bind_mounted_into_the_task_container(self):
        """
        Unlike the stream dir, only localization.sh touches this -- the task
        container never sees it, so mounting it in would be noise.
        """
        loc = make_localizer()
        setup_job(loc, "0", {"reads": [make_download_handler()]})
        setup, _, _ = scripts_for(loc)

        docker_line = next(l for l in setup.splitlines() if l.startswith("export CANINE_DOCKER_ARGS="))
        assert "CANINE_DOWNLOAD_TRACKER_DIR" not in docker_line

    def test_teardown_removes_it(self):
        loc = make_localizer()
        setup_job(loc, "0", {"reads": [make_download_handler()]})
        _, _, teardown = scripts_for(loc)

        assert 'if [[ -n "$CANINE_DOWNLOAD_TRACKER_DIR" ]]; then rm -rf $CANINE_DOWNLOAD_TRACKER_DIR; fi' in teardown

    def test_handler_falls_back_to_dest_dir_without_a_localizer(self):
        """
        localization_command() is called directly in places that have no
        localizer to supply a node-local directory; those must keep the old
        behavior rather than emitting a path under /tmp that nobody created.
        """
        handler = make_download_handler()
        assert handler.download_tracker_dir is None
        cmd = handler.localization_command("/mnt/nfs/staging/jobs/0/inputs/reads.bam")

        assert 'CLOUDSDK_STORAGE_TRACKER_DIR="/mnt/nfs/staging/jobs/0/inputs/.gcloud_tracker_dir"' in cmd
        assert NODE_LOCAL_ROOT not in cmd
