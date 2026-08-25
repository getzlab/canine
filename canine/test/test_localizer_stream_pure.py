"""
Pure unit tests for streaming-input (FIFO) placement -- no SLURM cluster, no
Docker required (plain MagicMock backend, same approach as
test_localizer_requester_pays_pure.py, since test_localizer_batched.py's
module-level DummySlurmBackend needs a Docker image that is not reachable
everywhere).

Covers NFS-FUSE-IMPLEMENTATION-PLAN.md phase 4 / blocker B4: gcsfuse implements
no mknod, so `mkfifo` on the shared mount fails with "Operation not supported".
localization.sh runs under `set -e`, which turns that into a hard failure of
every `localization: stream` task. FIFOs therefore have to be created on
node-local disk, under CANINE_STREAM_DIR.

A FIFO transports no data through the filesystem -- bytes move through a kernel
pipe buffer -- and both ends live on the same node, so this is a placement
change only, with no bearing on where job data lands.
"""
import os
from unittest.mock import MagicMock, patch

import pytest

from canine.localization.local import BatchedLocalizer
from canine.localization import file_handlers


STREAM_ROOT = "/tmp/canine-streams"
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
      STREAM_ROOT,
      os.path.basename(loc.environment("remote")["CANINE_ROOT"].rstrip("/")),
      jobId,
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
