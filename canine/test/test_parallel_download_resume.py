"""
Preemption-resumability tests for parallel_download.py. This is the critical suite.

SIGKILL is the primary instrument, never SIGTERM: the design may not assume a
catchable signal, because a GCE preemption can remove the VM between any two
instructions. There is deliberately no signal handler in the downloader yet, so these
tests already run against the "handler never fires" case that has to work.

What SIGKILL does and does not simulate: killing a *process* does not discard the page
cache, so bytes it wrote still reach disk. A real preemption loses them. That gap is
covered separately by punching holes in the written region
(TestSimulatedPageCacheLoss), which is the closest portable analogue.

Two filesystem paths exist and which one runs depends on the host: ext4 (the
localization PD, and Linux CI) passes the SEEK_HOLE probe and uses frontier recovery;
APFS fails it and uses the checkpoint fallback. Both must produce byte-identical
output, so these tests assert on outcomes rather than on the mechanism, and the
mechanism-specific arithmetic is unit-tested in test_parallel_download.py.
"""

import hashlib
import json
import os
import signal
import time

import pytest

from canine.localization import parallel_download as pdl
from pdl_server import Server, run_downloader, spawn_downloader

MIB = 1024 * 1024

# big enough to span several chunks and survive a few kills, small enough to be quick
PAYLOAD_SIZE = 24 * MIB + 4321


@pytest.fixture(scope="module")
def payload():
    return os.urandom(PAYLOAD_SIZE)


@pytest.fixture(scope="module")
def payload_md5(payload):
    return hashlib.md5(payload).hexdigest()


def md5_of(path):
    return hashlib.md5(open(path, "rb").read()).hexdigest()


def kill_after_bytes(server, proc, threshold, timeout=60):
    """
    SIGKILL the downloader once the server has pushed `threshold` bytes, so the kill
    reliably lands mid-transfer rather than before it starts or after it finishes.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if server.state.snapshot()["sent"] >= threshold:
            break
        if proc.poll() is not None:
            return False
        time.sleep(0.01)
    if proc.poll() is None:
        proc.send_signal(signal.SIGKILL)
        proc.wait(timeout=30)
        return True
    return False


def drain(proc):
    try:
        proc.stdout.close()
        proc.stderr.close()
    except (OSError, ValueError):
        pass


def run_to_completion(server, dest, size, *extra, max_attempts=25):
    """Re-run the downloader until it exits 0, returning the number of attempts."""
    for attempt in range(1, max_attempts + 1):
        proc = run_downloader(server.url(), dest, size, *extra)
        if proc.returncode == 0:
            return attempt, proc
        assert proc.returncode == pdl.EXIT_REQUEUE, (
            "attempt {} exited {} (expected 0 or 5):\n{}".format(
                attempt, proc.returncode, proc.stderr)
        )
    raise AssertionError("did not complete within {} attempts".format(max_attempts))


# ---------------------------------------------------------------------------
# the headline tests
# ---------------------------------------------------------------------------

class TestKillStorm:

    @pytest.mark.parametrize("seed", [1, 2, 3, 4, 5])
    def test_repeated_sigkill_still_yields_a_correct_file(
        self, tmp_path, payload, payload_md5, seed
    ):
        """
        Kill at a pseudo-random point, re-run, repeat -- the final file must be
        byte-identical every time. Runs with no signal handler present at all, which is
        the point: correctness may not depend on catching anything.
        """
        import random
        rng = random.Random(seed)
        dest = str(tmp_path / "obj.bin")
        ARGS = ("--min-chunk", MIB, "--connections", 4, "--check-md5", payload_md5)

        with Server(payload) as server:
            server.state.throttle_bytes = 256 * 1024
            server.state.throttle_delay = 0.004

            for _ in range(3):
                threshold = server.state.snapshot()["sent"] + rng.randint(
                    MIB // 2, 3 * MIB)
                proc = spawn_downloader(server.url(), dest, len(payload), *ARGS)
                kill_after_bytes(server, proc, threshold)
                drain(proc)

            server.state.throttle_delay = 0
            _, proc = run_to_completion(server, dest, len(payload), *ARGS)

        assert md5_of(dest) == payload_md5
        _, marker = pdl.sidecar_paths(dest)
        assert os.path.exists(marker)

    def test_kills_at_many_offsets(self, tmp_path, payload, payload_md5):
        """Sweep the kill point across the transfer rather than sampling one spot."""
        dest = str(tmp_path / "obj.bin")
        ARGS = ("--min-chunk", MIB, "--connections", 4, "--check-md5", payload_md5)
        with Server(payload) as server:
            server.state.throttle_bytes = 128 * 1024
            server.state.throttle_delay = 0.003

            for fraction in (0.1, 0.3, 0.5, 0.7, 0.9):
                threshold = server.state.snapshot()["sent"] + int(PAYLOAD_SIZE * fraction)
                proc = spawn_downloader(server.url(), dest, len(payload), *ARGS)
                kill_after_bytes(server, proc, threshold)
                drain(proc)

            server.state.throttle_delay = 0
            run_to_completion(server, dest, len(payload), *ARGS)

        assert md5_of(dest) == payload_md5


class TestNoCommittedWorkDiscarded:
    """
    The invariant that rules out the rejected periodic-checkpoint design: a re-run may
    refetch only the filesystem's uncommitted tail, not a whole checkpoint interval per
    stream.
    """

    def test_total_transferred_stays_close_to_the_object_size(
        self, tmp_path, payload, payload_md5
    ):
        dest = str(tmp_path / "obj.bin")
        connections = 4

        with Server(payload) as server:
            server.state.throttle_bytes = 256 * 1024
            server.state.throttle_delay = 0.004

            args = ("--min-chunk", MIB, "--connections", connections,
                    "--check-md5", payload_md5)
            proc = spawn_downloader(server.url(), dest, len(payload), *args)
            killed = kill_after_bytes(server, proc, PAYLOAD_SIZE // 2)
            drain(proc)
            assert killed, "kill did not land mid-transfer; test is not exercising resume"

            server.state.throttle_delay = 0
            run_to_completion(server, dest, len(payload), *args)
            total_sent = server.state.snapshot()["sent"]

        assert md5_of(dest) == payload_md5

        waste = total_sent - PAYLOAD_SIZE
        # One in-flight chunk per connection can lose its uncommitted tail. The bound
        # is generous enough to cover both the frontier path (a block per chunk) and
        # the checkpoint fallback (its interval per chunk), while still being far below
        # "every chunk restarted from zero".
        budget = connections * pdl.FALLBACK_CHECKPOINT_INTERVAL + 2 * MIB
        assert waste <= budget, (
            "refetched {} bytes beyond the object ({} MiB); budget {} MiB. "
            "Progress is being discarded rather than resumed.".format(
                waste, waste // MIB, budget // MIB)
        )

    def test_each_attempt_transfers_less_than_a_full_object(
        self, tmp_path, payload, payload_md5
    ):
        """A resumed attempt must not start over from zero."""
        dest = str(tmp_path / "obj.bin")
        with Server(payload) as server:
            server.state.throttle_bytes = 256 * 1024
            server.state.throttle_delay = 0.004
            # every attempt must pass identical flags: the expected hash participates
            # in plan_id, so varying it between attempts would legitimately invalidate
            # the manifest and mask whether resume works. canine emits the same command
            # each time, so this mirrors real usage.
            args = ("--min-chunk", MIB, "--connections", 4, "--check-md5", payload_md5)
            proc = spawn_downloader(server.url(), dest, len(payload), *args)
            assert kill_after_bytes(server, proc, int(PAYLOAD_SIZE * 0.6))
            drain(proc)

            before = server.state.snapshot()["sent"]
            server.state.throttle_delay = 0
            run_to_completion(server, dest, len(payload), *args)
            resumed_bytes = server.state.snapshot()["sent"] - before

        assert md5_of(dest) == payload_md5
        assert resumed_bytes < PAYLOAD_SIZE, (
            "the resumed attempt transferred {} of {} bytes -- it restarted".format(
                resumed_bytes, PAYLOAD_SIZE)
        )


# ---------------------------------------------------------------------------
# manifest integrity across crash windows
# ---------------------------------------------------------------------------

class TestManifestIntegrity:

    def test_manifest_is_never_torn(self, tmp_path, payload, payload_md5):
        """
        Killed repeatedly, the manifest on disk must always be either the old version
        or the new one -- never a partial write. That is what the tmp+rename commit
        buys, since rename is atomic.
        """
        dest = str(tmp_path / "obj.bin")
        manifest_path, _ = pdl.sidecar_paths(dest)

        with Server(payload) as server:
            server.state.throttle_bytes = 64 * 1024
            server.state.throttle_delay = 0.002

            for fraction in (0.2, 0.4, 0.6, 0.8):
                threshold = server.state.snapshot()["sent"] + int(PAYLOAD_SIZE * fraction)
                proc = spawn_downloader(server.url(), dest, len(payload),
                                        "--min-chunk", MIB, "--connections", 4)
                kill_after_bytes(server, proc, threshold)
                drain(proc)

                if os.path.exists(manifest_path):
                    with open(manifest_path) as fh:
                        state = json.load(fh)   # raises if torn
                    assert state["schema_version"] == pdl.SCHEMA_VERSION
                    assert "plan_id" in state and "chunks" in state

            server.state.throttle_delay = 0
            run_to_completion(server, dest, len(payload), "--min-chunk", MIB,
                              "--connections", 4, "--check-md5", payload_md5)

        assert md5_of(dest) == payload_md5

    @pytest.mark.parametrize("corruption", [
        b"", b"{", b"not json", b'{"schema_version": 42}', b"null",
    ])
    def test_corrupt_manifest_restarts_cleanly(
        self, tmp_path, payload, payload_md5, corruption
    ):
        """
        A corrupt manifest must degrade to a correct full re-download, never to a
        corrupt output.
        """
        dest = str(tmp_path / "obj.bin")
        manifest_path, _ = pdl.sidecar_paths(dest)

        with Server(payload) as server:
            server.state.throttle_bytes = 128 * 1024
            server.state.throttle_delay = 0.003
            proc = spawn_downloader(server.url(), dest, len(payload),
                                    "--min-chunk", MIB, "--connections", 4)
            kill_after_bytes(server, proc, PAYLOAD_SIZE // 3)
            drain(proc)

            with open(manifest_path, "wb") as fh:
                fh.write(corruption)

            server.state.throttle_delay = 0
            proc = run_downloader(server.url(), dest, len(payload), "--min-chunk", MIB,
                                  "--connections", 4, "--check-md5", payload_md5)

        assert proc.returncode == 0, proc.stderr
        assert md5_of(dest) == payload_md5

    def test_absent_manifest_restarts_cleanly(self, tmp_path, payload, payload_md5):
        """Models a wiped ephemeral local disk, where resume is impossible."""
        dest = str(tmp_path / "obj.bin")
        manifest_path, _ = pdl.sidecar_paths(dest)

        with Server(payload) as server:
            server.state.throttle_bytes = 128 * 1024
            server.state.throttle_delay = 0.003
            proc = spawn_downloader(server.url(), dest, len(payload),
                                    "--min-chunk", MIB, "--connections", 4)
            kill_after_bytes(server, proc, PAYLOAD_SIZE // 3)
            drain(proc)
            os.unlink(manifest_path)

            server.state.throttle_delay = 0
            proc = run_downloader(server.url(), dest, len(payload), "--min-chunk", MIB,
                                  "--connections", 4, "--check-md5", payload_md5)

        assert proc.returncode == 0, proc.stderr
        assert md5_of(dest) == payload_md5


class TestPlanIdMismatch:

    def test_changed_object_discards_the_partial_file(self, tmp_path, payload, payload_md5):
        """
        If the object changed between attempts, resuming would splice two different
        objects together. The plan_id check has to force a restart.
        """
        dest = str(tmp_path / "obj.bin")

        with Server(payload) as server:
            server.state.throttle_bytes = 128 * 1024
            server.state.throttle_delay = 0.003
            proc = spawn_downloader(server.url(), dest, len(payload),
                                   "--min-chunk", MIB, "--connections", 4)
            kill_after_bytes(server, proc, PAYLOAD_SIZE // 3)
            drain(proc)

            # serve a different object of a different size at the same URL
            replacement = os.urandom(PAYLOAD_SIZE + 9999)
            server.state.payload = replacement
            server.state.throttle_delay = 0

            proc = run_downloader(
                server.url(), dest, len(replacement), "--min-chunk", MIB,
                "--connections", 4, "--check-md5", hashlib.md5(replacement).hexdigest(),
            )

        assert proc.returncode == 0, proc.stderr
        assert md5_of(dest) == hashlib.md5(replacement).hexdigest()

    def test_changed_hash_invalidates_the_plan(self, tmp_path):
        """Same size, different content hash: plan_id must still differ."""
        a = pdl.compute_plan_id("https://h/o", 1000, "hash-a", MIB)
        b = pdl.compute_plan_id("https://h/o", 1000, "hash-b", MIB)
        assert a != b


class TestConnectionCountChange:

    def test_resume_with_a_different_connection_count_keeps_progress(
        self, tmp_path, payload, payload_md5
    ):
        """
        A requeued task can land on a differently-loaded node. Because the chunk layout
        is derived from size and min_chunk only, changing --connections must not
        invalidate the partial file.
        """
        dest = str(tmp_path / "obj.bin")

        with Server(payload) as server:
            server.state.throttle_bytes = 256 * 1024
            server.state.throttle_delay = 0.004
            proc = spawn_downloader(server.url(), dest, len(payload), "--min-chunk",
                                    MIB, "--check-md5", payload_md5, "--connections", 8)
            assert kill_after_bytes(server, proc, PAYLOAD_SIZE // 2)
            drain(proc)

            before = server.state.snapshot()["sent"]
            server.state.throttle_delay = 0
            # only --connections differs; everything feeding plan_id is held constant
            proc = run_downloader(server.url(), dest, len(payload), "--min-chunk", MIB,
                                  "--check-md5", payload_md5, "--connections", 2)
            resumed = server.state.snapshot()["sent"] - before

        assert proc.returncode == 0, proc.stderr
        assert md5_of(dest) == payload_md5
        assert resumed < PAYLOAD_SIZE, (
            "changing --connections discarded progress: refetched {} of {}".format(
                resumed, PAYLOAD_SIZE)
        )


# ---------------------------------------------------------------------------
# the preallocation hazard (§4.4)
# ---------------------------------------------------------------------------

class TestFullSizeIncompleteFile:

    def test_sparse_full_size_file_is_not_accepted_as_complete(
        self, tmp_path, payload, payload_md5
    ):
        """
        ftruncate gives an incomplete file its full apparent size immediately, so
        anything inferring completion from stat would accept a file of zeros. Only the
        done marker may assert completion.
        """
        dest = str(tmp_path / "obj.bin")
        with open(dest, "wb") as fh:
            fh.truncate(PAYLOAD_SIZE)

        with Server(payload) as server:
            before = server.state.snapshot()["sent"]
            proc = run_downloader(server.url(), dest, len(payload), "--min-chunk", MIB,
                                  "--connections", 4, "--check-md5", payload_md5)
            transferred = server.state.snapshot()["sent"] - before

        assert proc.returncode == 0, proc.stderr
        assert transferred > 0, "a full-size file of zeros was mistaken for complete"
        assert md5_of(dest) == payload_md5

    def test_marker_for_a_different_size_does_not_short_circuit(
        self, tmp_path, payload, payload_md5
    ):
        dest = str(tmp_path / "obj.bin")
        _, marker = pdl.sidecar_paths(dest)
        pdl.write_done_marker(marker, PAYLOAD_SIZE + 1, "stale", None)

        with Server(payload) as server:
            proc = run_downloader(server.url(), dest, len(payload), "--min-chunk", MIB,
                                  "--connections", 4, "--check-md5", payload_md5)

        assert proc.returncode == 0, proc.stderr
        assert md5_of(dest) == payload_md5


# ---------------------------------------------------------------------------
# simulated page-cache loss (§8.4f)
# ---------------------------------------------------------------------------

def punch_hole(path, offset, length):
    """
    Deallocate a range, so the region reads as zeros AND stops being an allocated
    extent -- which is what makes SEEK_HOLE report a shorter frontier.

    This is the only faithful way to simulate a real preemption's page-cache loss:
    SIGKILL does not discard dirty pages, so bytes the killed process wrote still reach
    disk, whereas a vanished VM loses them.

    Linux/ext4 (the localization PD, and CI) has FALLOC_FL_PUNCH_HOLE. macOS exposes
    fcntl F_PUNCHHOLE, but it returns EINVAL on APFS here, so these tests skip locally
    and run where the production filesystem actually is.
    """
    if not (hasattr(os, "fallocate") and hasattr(os, "FALLOC_FL_PUNCH_HOLE")):
        return False
    try:
        fd = os.open(path, os.O_RDWR)
    except OSError:
        return False
    try:
        os.fallocate(
            fd, os.FALLOC_FL_PUNCH_HOLE | os.FALLOC_FL_KEEP_SIZE, offset, length
        )
        return True
    except OSError:
        return False
    finally:
        os.close(fd)


def rebuild_completed_manifest(dest, url, payload_len, connections, expected_hash):
    """
    Reconstruct the state that exists between "every chunk finished" and "marker
    written": manifest present with all chunks recorded complete.

    Used to reach that crash window deterministically, since timing a kill into it is
    not reliable.
    """
    manifest_path, _ = pdl.sidecar_paths(dest)
    chunks = pdl.plan_chunks(payload_len, connections, MIB)
    chunk_size = chunks[0][1] - chunks[0][0]
    plan_id = pdl.compute_plan_id(url, payload_len, expected_hash, chunk_size)
    manifest = pdl.Manifest.create(
        manifest_path, plan_id, payload_len, chunk_size, chunks, os.stat(dest),
        pdl.probe_seek_hole(os.path.dirname(dest)),
        pdl.checkpoint_interval_for(chunk_size),
    )
    for index in range(len(chunks)):
        manifest.record_chunk_done(index, None)
    return manifest


class TestSimulatedPageCacheLoss:

    def test_lost_tail_is_refetched_and_the_hash_still_matches(
        self, tmp_path, payload, payload_md5
    ):
        """
        The scenario SIGKILL alone cannot produce: bytes the process wrote are gone
        because the VM vanished before they were committed. The resumed download must
        notice the shorter frontier and refetch exactly that tail.
        """
        dest = str(tmp_path / "obj.bin")

        with Server(payload) as server:
            server.state.throttle_bytes = 256 * 1024
            server.state.throttle_delay = 0.004
            proc = spawn_downloader(server.url(), dest, len(payload),
                                    "--min-chunk", MIB, "--connections", 4)
            assert kill_after_bytes(server, proc, PAYLOAD_SIZE // 2)
            drain(proc)

            # blow away a window in the middle of the written region
            if not punch_hole(dest, 4 * MIB, 2 * MIB):
                pytest.skip("no punch-hole support on this platform/filesystem")

            server.state.throttle_delay = 0
            run_to_completion(server, dest, len(payload), "--min-chunk", MIB,
                              "--connections", 4, "--check-md5", payload_md5)

        assert md5_of(dest) == payload_md5

    def test_corruption_inside_a_completed_chunk_is_caught_by_verification(
        self, tmp_path, payload, payload_md5
    ):
        """
        The portable counterpart, and the honest statement of the guarantee.

        A chunk the manifest records complete is trusted and not re-derived, so if its
        bytes are lost anyway the resume machinery cannot notice -- by construction, on
        either filesystem path. End-to-end verification is the backstop, which is
        exactly why check_hash may never be skipped or downgraded.

        Corrupting in place (rather than punching a hole) needs no platform support, and
        tests the more dangerous direction: recorded progress overstating durable data.
        """
        dest = str(tmp_path / "obj.bin")
        connections = 4

        with Server(payload) as server:
            proc = run_downloader(server.url(), dest, len(payload), "--min-chunk", MIB,
                                  "--connections", connections,
                                  "--check-md5", payload_md5)
            assert proc.returncode == 0, proc.stderr

            # rewind to the pre-marker state, then destroy bytes the manifest calls done
            _, marker_path = pdl.sidecar_paths(dest)
            os.unlink(marker_path)
            rebuild_completed_manifest(dest, server.url(), len(payload), connections,
                                       payload_md5)
            with open(dest, "r+b") as fh:
                fh.seek(2 * MIB)
                fh.write(b"\0" * MIB)
            assert md5_of(dest) != payload_md5

            before = server.state.snapshot()["sent"]
            proc = run_downloader(server.url(), dest, len(payload), "--min-chunk", MIB,
                                  "--connections", connections,
                                  "--check-md5", payload_md5)
            transferred = server.state.snapshot()["sent"] - before

        # nothing was refetched -- the manifest said the chunks were done ...
        assert transferred <= 1, "expected no refetch, got {} bytes".format(transferred)
        # ... so verification is what has to catch it, and it must fail hard
        assert proc.returncode == pdl.EXIT_FAIL, (
            "corruption inside a completed chunk was not caught (exit {}):\n{}".format(
                proc.returncode, proc.stderr)
        )
        assert not os.path.exists(dest), "a file that failed verification must be deleted"
        assert not os.path.exists(marker_path)

    def test_corruption_is_repaired_by_a_subsequent_run(self, tmp_path, payload, payload_md5):
        """
        After verification deletes the corrupt file, the next attempt must produce a
        correct one from scratch rather than inheriting any stale state.
        """
        dest = str(tmp_path / "obj.bin")
        with Server(payload) as server:
            run_downloader(server.url(), dest, len(payload), "--min-chunk", MIB,
                           "--connections", 4, "--check-md5", payload_md5)
            _, marker_path = pdl.sidecar_paths(dest)
            os.unlink(marker_path)
            rebuild_completed_manifest(dest, server.url(), len(payload), 4, payload_md5)
            with open(dest, "r+b") as fh:
                fh.seek(2 * MIB)
                fh.write(b"\0" * MIB)

            run_downloader(server.url(), dest, len(payload), "--min-chunk", MIB,
                           "--connections", 4, "--check-md5", payload_md5)
            proc = run_downloader(server.url(), dest, len(payload), "--min-chunk", MIB,
                                  "--connections", 4, "--check-md5", payload_md5)

        assert proc.returncode == 0, proc.stderr
        assert md5_of(dest) == payload_md5


# ---------------------------------------------------------------------------
# concurrent writers (§4.5)
# ---------------------------------------------------------------------------

class TestConcurrentWriters:

    def test_two_downloaders_on_one_destination(self, tmp_path, payload, payload_md5):
        """
        A requeued task can start while the preempted VM is still being torn down. The
        flock is advisory at best -- it does nothing across VMs on a nolock NFS mount
        and can fail outright on a FUSE mount -- so correctness must not depend on it.
        Two writers derive the same plan and write byte-identical data to the same
        offsets, so overlapping writes are benign.
        """
        dest = str(tmp_path / "obj.bin")
        with Server(payload) as server:
            first = spawn_downloader(server.url(), dest, len(payload),
                                     "--min-chunk", MIB, "--connections", 4)
            second = spawn_downloader(server.url(), dest, len(payload),
                                      "--min-chunk", MIB, "--connections", 4)
            codes = [first.wait(timeout=180), second.wait(timeout=180)]
            drain(first)
            drain(second)

            assert any(code == 0 for code in codes), \
                "neither concurrent writer succeeded: {}".format(codes)

            proc = run_downloader(server.url(), dest, len(payload), "--min-chunk", MIB,
                                 "--connections", 4, "--check-md5", payload_md5)
        assert proc.returncode == 0, proc.stderr
        assert md5_of(dest) == payload_md5

    def test_flock_failure_is_tolerated(self, tmp_path):
        """
        On gcsfuse the flock call itself raises ENOTSUP. That must read as "no lock
        available", never as a download failure.
        """
        import errno as _errno

        class Boom:
            LOCK_EX = 2
            LOCK_NB = 4

            @staticmethod
            def flock(fd, flags):
                raise OSError(_errno.ENOTSUP, "Operation not supported")

        import sys
        saved = sys.modules.get("fcntl")
        sys.modules["fcntl"] = Boom
        try:
            path = tmp_path / "f"
            path.write_bytes(b"x")
            fd = os.open(str(path), os.O_RDWR)
            try:
                assert pdl.try_lock(fd) is False
            finally:
                os.close(fd)
        finally:
            if saved is not None:
                sys.modules["fcntl"] = saved
            else:
                del sys.modules["fcntl"]


# ---------------------------------------------------------------------------
# verification is resumable, and never skipped (§11)
# ---------------------------------------------------------------------------

class TestVerificationResumes:

    def test_kill_between_last_chunk_and_marker_does_not_refetch(
        self, tmp_path, payload, payload_md5
    ):
        """
        The real crash window between "every chunk finished" and "marker written": at
        that instant the manifest still records every chunk complete, so the re-run
        must verify and finalize without transferring anything.

        The state is reconstructed through the real Manifest API rather than by simply
        deleting the marker, because the marker is written *before* the manifest is
        removed -- so "neither sidecar present, data intact" is not a state a crash can
        actually produce, and testing it would assert against a fiction.
        """
        dest = str(tmp_path / "obj.bin")
        manifest_path, marker_path = pdl.sidecar_paths(dest)

        with Server(payload) as server:
            proc = run_downloader(server.url(), dest, len(payload), "--min-chunk", MIB,
                                  "--connections", 4, "--check-md5", payload_md5)
            assert proc.returncode == 0, proc.stderr

            # rewind to the pre-marker state: manifest present, all chunks done
            os.unlink(marker_path)
            chunks = pdl.plan_chunks(len(payload), 4, MIB)
            chunk_size = chunks[0][1] - chunks[0][0]
            plan_id = pdl.compute_plan_id(server.url(), len(payload), payload_md5,
                                          chunk_size)
            manifest = pdl.Manifest.create(
                manifest_path, plan_id, len(payload), chunk_size, chunks,
                os.stat(dest), pdl.probe_seek_hole(str(tmp_path)),
                pdl.checkpoint_interval_for(chunk_size),
            )
            for index in range(len(chunks)):
                manifest.record_chunk_done(index, None)

            before = server.state.snapshot()["sent"]
            proc = run_downloader(server.url(), dest, len(payload), "--min-chunk", MIB,
                                  "--connections", 4, "--check-md5", payload_md5)
            transferred = server.state.snapshot()["sent"] - before

        assert proc.returncode == 0, proc.stderr
        assert md5_of(dest) == payload_md5
        # the mandatory Range: bytes=0-0 probe always runs, hence the 1-byte allowance
        assert transferred <= 1, (
            "refetched {} bytes although every chunk was recorded complete".format(
                transferred)
        )
        assert os.path.exists(marker_path)

    def test_marker_is_only_written_after_verification_passes(self, tmp_path, payload):
        dest = str(tmp_path / "obj.bin")
        _, marker = pdl.sidecar_paths(dest)
        with Server(payload) as server:
            proc = run_downloader(server.url(), dest, len(payload), "--min-chunk", MIB,
                                  "--connections", 4, "--check-md5", "0" * 32)
        assert proc.returncode == pdl.EXIT_FAIL
        assert not os.path.exists(marker)
        assert not os.path.exists(dest)

    def test_marker_survives_a_successful_download(self, tmp_path, payload, payload_md5):
        """
        The marker is deliberately not cleaned up: it has to outlive the download to
        cover a preemption between "download finished" and "disk labelled
        finished=yes", since the label is the real completion signal.
        """
        dest = str(tmp_path / "obj.bin")
        with Server(payload) as server:
            proc = run_downloader(server.url(), dest, len(payload), "--min-chunk", MIB,
                                  "--check-md5", payload_md5)
        assert proc.returncode == 0, proc.stderr
        manifest, marker = pdl.sidecar_paths(dest)
        assert os.path.exists(marker), "marker must persist after success"
        assert not os.path.exists(manifest), "manifest is transient and should be gone"


# ---------------------------------------------------------------------------
# exit-code contract (§4.2)
# ---------------------------------------------------------------------------

class TestExitCodeContract:

    def test_no_forward_progress_fails_rather_than_requeueing(self, tmp_path):
        """
        Exit 5 requeues are excluded from the preemption limit, so returning 5 when no
        progress is possible would loop forever. A genuinely stuck download must
        surface as a failure instead.
        """
        dest = str(tmp_path / "obj.bin")
        payload = os.urandom(4 * MIB)
        with Server(payload) as server:
            server.state.fail_next = 10_000   # every request 500s, forever
            proc = run_downloader(server.url(), dest, len(payload), "--min-chunk", MIB,
                                  "--connections", 2, "--retries", 1, timeout=300)
        assert proc.returncode in (pdl.EXIT_FAIL, pdl.EXIT_REQUEUE), proc.stderr
        if proc.returncode == pdl.EXIT_REQUEUE:
            pytest.fail(
                "returned 5 with no forward progress; this requeues without limit:\n"
                + proc.stderr
            )

    def test_permanent_http_error_is_do_not_retry(self, tmp_path):
        """404 is a real error; anything other than 5/15 marks the job do-not-retry."""
        dest = str(tmp_path / "obj.bin")

        class NotFound(Server):
            pass

        with Server(b"x" * (4 * MIB)) as server:
            url = server.url()
            server.close()   # nothing listening now
            proc = run_downloader(url, dest, 4 * MIB, "--min-chunk", MIB,
                                  "--retries", 1, "--timeout", 2)
        assert proc.returncode != 0
        assert proc.returncode in (pdl.EXIT_FAIL, pdl.EXIT_REQUEUE)
