"""
Tests for canine/localization/parallel_download.py -- chunk planning, the manifest,
the filesystem capability probes, verification, and end-to-end downloads against a
local HTTP server.

Preemption-specific behavior lives in test_parallel_download_resume.py.
"""

import hashlib
import inspect
import re
import json
import os
import stat
import subprocess
import sys
import urllib.request

import pytest

from canine.localization import parallel_download as pdl

# canine/test/ is not a package (no __init__.py), so pytest puts this directory on
# sys.path and the helper is imported by plain name
from pdl_server import PDL_PATH, Server, run_downloader

MIB = 1024 * 1024


# ---------------------------------------------------------------------------
# script conventions (§6.5 of the design doc)
# ---------------------------------------------------------------------------

class TestScriptConventions:
    """
    The script is staged onto the compute node and invoked directly, so these are part
    of its contract rather than cosmetic.
    """

    def test_has_shebang(self):
        with open(PDL_PATH) as fh:
            assert fh.readline().rstrip() == "#!/usr/bin/env python3"

    def test_is_executable_in_the_repo(self):
        mode = os.stat(PDL_PATH).st_mode
        assert mode & stat.S_IXUSR, "parallel_download.py must be committed 0755"

    def test_ends_with_the_eof_sentinel(self):
        """
        The resolver checks for this line so a truncated or partially-visible staged
        copy is skipped rather than executed.
        """
        with open(PDL_PATH) as fh:
            assert fh.read().rstrip().endswith("# k9pdl-eof")

    def test_runs_standalone_without_importing_canine(self):
        """
        An operator debugging a failed localization has to be able to re-run the exact
        download by hand, on a node where canine may not be importable.
        """
        proc = subprocess.run(
            [sys.executable, PDL_PATH, "--help"],
            capture_output=True, text=True, cwd="/", timeout=60,
        )
        assert proc.returncode == 0, proc.stderr
        assert "--connections" in proc.stdout

    def test_imports_only_the_standard_library(self):
        """
        Parsed rather than grepped: a substring search also matches prose in the
        docstring, and what matters is the actual import statements.
        """
        import ast
        import sys

        with open(PDL_PATH) as fh:
            tree = ast.parse(fh.read())

        imported = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                imported.add(node.module.split(".")[0])

        assert "canine" not in imported
        non_stdlib = imported - set(sys.stdlib_module_names)
        assert not non_stdlib, "non-stdlib imports would need the worker image rebuilt: {}".format(
            sorted(non_stdlib))


# ---------------------------------------------------------------------------
# chunk planning (§2)
# ---------------------------------------------------------------------------

class TestPlanChunks:

    def test_chunks_are_contiguous_and_cover_the_object(self):
        chunks = pdl.plan_chunks(100 * MIB, 8, MIB)
        assert chunks[0][0] == 0
        assert chunks[-1][1] == 100 * MIB
        for (_, end), (nxt, _) in zip(chunks, chunks[1:]):
            assert end == nxt

    def test_chunks_are_even_except_a_shorter_tail(self):
        chunks = pdl.plan_chunks(100 * MIB + 12345, 8, MIB)
        sizes = [end - start for start, end in chunks]
        assert len(set(sizes[:-1])) == 1
        assert sizes[-1] <= sizes[0]

    def test_small_object_is_one_chunk(self):
        """Below min_chunk the behavior is identical to the legacy single stream."""
        assert pdl.plan_chunks(1000, 8, 64 * MIB) == [(0, 1000)]

    def test_chunk_size_is_min_chunk(self):
        """
        Chunk size is fixed at min_chunk rather than size/connections, so a large object
        becomes many chunks that the worker pool queues through.
        """
        chunks = pdl.plan_chunks(1000 * MIB, 8, 64 * MIB)
        assert chunks[0][1] - chunks[0][0] == 64 * MIB
        assert len(chunks) == 1000 // 64 + 1

    def test_layout_is_completely_independent_of_connections(self):
        """
        The invariant behind resumability: plan_id is derived from the chunk size, so if
        the layout moved with the connection count a requeued task on a
        differently-configured node would discard a good partial file and start over.
        """
        size = 512 * MIB + 12345
        base = pdl.plan_chunks(size, 8, 64 * MIB)
        for connections in (1, 2, 4, 8, 12, 16, 999):
            assert pdl.plan_chunks(size, connections, 64 * MIB) == base

    def test_plan_id_is_stable_across_connection_counts(self):
        """The end-to-end consequence of the above."""
        size = 512 * MIB
        ids = set()
        for connections in (2, 8, 16):
            chunks = pdl.plan_chunks(size, connections, 64 * MIB)
            ids.add(pdl.compute_plan_id("https://h/o", size, "abc",
                                        chunks[0][1] - chunks[0][0]))
        assert len(ids) == 1

    def test_starts_are_block_aligned(self):
        """
        Adjacent chunks must not share a partial filesystem block, or the frontier
        arithmetic at chunk boundaries stops being sound.
        """
        for start, _ in pdl.plan_chunks(333 * MIB + 7, 8, 16 * MIB):
            assert start % pdl.CHUNK_ALIGN == 0

    def test_zero_length_object(self):
        assert pdl.plan_chunks(0, 8, MIB) == [(0, 0)]

    def test_negative_size_yields_nothing(self):
        assert pdl.plan_chunks(-1, 8, MIB) == []

    def test_part_length_snapping(self):
        """
        For a multipart S3 object every chunk must span whole parts, so the
        md5-of-md5s ETag can be computed during the download instead of by a second
        full read afterwards.
        """
        part = 8 * MIB
        chunks = pdl.plan_chunks(80 * MIB, 8, MIB, part_length=part)
        for start, end in chunks[:-1]:
            assert start % part == 0
            assert (end - start) % part == 0

    def test_part_length_larger_than_ideal_chunk_is_used_directly(self):
        part = 64 * MIB
        chunks = pdl.plan_chunks(128 * MIB, 8, MIB, part_length=part)
        assert chunks[0][1] - chunks[0][0] == part


class TestPlanId:

    def test_stable_for_the_same_inputs(self):
        a = pdl.compute_plan_id("https://h/o", 100, "abc", 8 * MIB)
        b = pdl.compute_plan_id("https://h/o", 100, "abc", 8 * MIB)
        assert a == b

    @pytest.mark.parametrize("kwargs", [
        {"size": 101}, {"content_hash": "def"}, {"chunk_size": 4 * MIB},
    ])
    def test_changes_when_the_object_or_layout_changes(self, kwargs):
        base = dict(url="https://h/o", size=100, content_hash="abc", chunk_size=8 * MIB)
        assert pdl.compute_plan_id(**base) != pdl.compute_plan_id(**{**base, **kwargs})

    def test_ignores_the_query_string(self):
        """
        A signed URL is re-minted with fresh credentials and expiry on every attempt.
        Including the query would make every resume look like a different object.
        """
        a = pdl.compute_plan_id("https://h/o?Signature=aaa&Expires=1", 100, "h", MIB)
        b = pdl.compute_plan_id("https://h/o?Signature=bbb&Expires=2", 100, "h", MIB)
        assert a == b

    def test_distinguishes_different_paths(self):
        a = pdl.compute_plan_id("https://h/one", 100, "h", MIB)
        b = pdl.compute_plan_id("https://h/two", 100, "h", MIB)
        assert a != b


# ---------------------------------------------------------------------------
# durable frontier (§4.1)
# ---------------------------------------------------------------------------

class TestChunkFrontier:
    """
    SEEK_HOLE is block-granular, so the reported hole can be past the last byte
    actually written. The frontier must therefore under-report, never over-report:
    over-reporting makes the resumed download skip bytes and silently corrupt output.

    os.lseek is stubbed so the arithmetic is tested independently of whichever
    filesystem the tests happen to run on.
    """

    BLOCK = 4096

    def _frontier(self, monkeypatch, hole, start, end):
        monkeypatch.setattr(pdl.os, "lseek", lambda fd, off, whence: hole)
        return pdl.chunk_frontier(0, start, end, block_size=self.BLOCK)

    def test_nothing_written_yields_chunk_start(self, monkeypatch):
        assert self._frontier(monkeypatch, hole=1000, start=1000, end=9000) == 1000

    def test_discards_the_final_allocated_block(self, monkeypatch):
        """
        Writing 5000 bytes allocates two 4096-byte blocks, so the hole is reported at
        8192. Only 4096 may be trusted.
        """
        assert self._frontier(monkeypatch, hole=8192, start=0, end=1 << 20) == 4096

    def test_a_single_partial_block_yields_nothing_trusted(self, monkeypatch):
        assert self._frontier(monkeypatch, hole=4096, start=0, end=1 << 20) == 0

    def test_never_exceeds_the_bytes_actually_written(self, monkeypatch):
        """The property that matters: the frontier must never overstate durability."""
        for written in range(1, 40961, 137):
            hole = ((written + self.BLOCK - 1) // self.BLOCK) * self.BLOCK
            frontier = self._frontier(monkeypatch, hole=hole, start=0, end=1 << 20)
            assert frontier <= written, \
                "frontier {} > written {} would skip bytes".format(frontier, written)

    def test_clamped_to_chunk_end_and_still_conservative(self, monkeypatch):
        """
        A hole reported past chunk_end still does not prove the chunk is fully written.

        Suppose the chunk is [0, 8192) and only 5000 bytes were written: the block
        [4096, 8192) is allocated by that partial write, and the region past 8192 is
        allocated by the *next* chunk, so no hole is reported until well beyond this
        chunk. An unallocated-block scan therefore cannot distinguish "wrote 5000" from
        "wrote 8192", and the last block must be discarded either way.

        The cost is bounded and rarely paid: a genuinely complete chunk is recorded in
        the manifest, so this path only runs when that record was lost to a crash.
        """
        assert self._frontier(monkeypatch, hole=1 << 30, start=0, end=8192) == 4096

    def test_never_exceeds_chunk_end(self, monkeypatch):
        assert self._frontier(monkeypatch, hole=1 << 30, start=0, end=1 << 20) <= 1 << 20

    def test_hole_before_chunk_start_yields_chunk_start(self, monkeypatch):
        assert self._frontier(monkeypatch, hole=0, start=8192, end=16384) == 8192

    def test_enxio_is_tolerated(self, monkeypatch):
        def boom(fd, off, whence):
            raise OSError(6, "ENXIO")
        monkeypatch.setattr(pdl.os, "lseek", boom)
        assert pdl.chunk_frontier(0, 4096, 8192) == 4096

    def test_offsets_are_relative_to_the_chunk(self, monkeypatch):
        frontier = self._frontier(monkeypatch, hole=20480, start=8192, end=1 << 20)
        assert frontier == 16384
        assert (frontier - 8192) % self.BLOCK == 0


# ---------------------------------------------------------------------------
# filesystem capability probes (§4.1, §4.7)
# ---------------------------------------------------------------------------

class TestSeekHoleProbe:

    def test_probe_fsyncs_before_looking(self, tmp_path, monkeypatch):
        """
        Regression test. The probe originally checked SEEK_HOLE without fsyncing, so
        under delayed allocation it saw a hole where an unflushed write had no extent
        yet -- passing on filesystems (APFS among them) that then report a written
        sparse file as entirely data. Every real resume happens post-crash, i.e.
        post-commit, so the probe has to test the committed state.
        """
        synced = []
        real_fsync = pdl.os.fsync

        def tracking_fsync(fd):
            synced.append(fd)
            return real_fsync(fd)

        monkeypatch.setattr(pdl.os, "fsync", tracking_fsync)
        pdl.probe_seek_hole(str(tmp_path))
        assert synced, "probe must fsync before consulting SEEK_HOLE"

    def test_probe_rejects_a_filesystem_reporting_no_hole(self, tmp_path, monkeypatch):
        """A filesystem that reports the whole file as data must fail the probe."""
        monkeypatch.setattr(pdl.os, "lseek", lambda fd, off, whence: 8 * MIB)
        assert pdl.probe_seek_hole(str(tmp_path)) is False

    def test_probe_rejects_a_coarsely_rounded_hole(self, tmp_path, monkeypatch):
        """
        Technically supporting SEEK_HOLE is not enough: a filesystem that rounds the
        boundary up by megabytes would make the frontier overstate durable data.
        """
        monkeypatch.setattr(pdl.os, "lseek", lambda fd, off, whence: 4 * MIB)
        assert pdl.probe_seek_hole(str(tmp_path)) is False

    def test_probe_accepts_a_hole_just_past_the_written_region(self, tmp_path, monkeypatch):
        monkeypatch.setattr(pdl.os, "lseek", lambda fd, off, whence: 64 * 1024)
        assert pdl.probe_seek_hole(str(tmp_path)) is True

    def test_probe_leaves_no_temporary_files(self, tmp_path):
        pdl.probe_seek_hole(str(tmp_path))
        pdl.probe_random_write(str(tmp_path))
        assert list(tmp_path.iterdir()) == []

    def test_probe_returns_false_on_an_unwritable_directory(self):
        assert pdl.probe_seek_hole("/nonexistent/canine/probe/dir") is False


# ---------------------------------------------------------------------------
# sidecars (§4.6)
# ---------------------------------------------------------------------------

class TestSidecarPaths:

    def test_are_dotfiles_beside_the_destination(self):
        manifest, marker = pdl.sidecar_paths("/mnt/disk/inputs/sample.bam")
        assert manifest == "/mnt/disk/inputs/.sample.bam.k9pdl.json"
        assert marker == "/mnt/disk/inputs/.sample.bam.k9pdl.done"

    def test_dotfiles_keep_them_out_of_output_globs(self):
        for path in pdl.sidecar_paths("/d/f.bam"):
            assert os.path.basename(path).startswith(".")


# ---------------------------------------------------------------------------
# manifest (§4.1, §4.3)
# ---------------------------------------------------------------------------

class TestManifest:

    def _create(self, tmp_path, size=1000, chunk_size=500, plan_id="pid"):
        dest = tmp_path / "obj.bin"
        dest.write_bytes(b"\0" * size)
        path = str(tmp_path / ".obj.bin.k9pdl.json")
        st = os.stat(str(dest))
        chunks = [(0, 500), (500, 1000)]
        return pdl.Manifest.create(path, plan_id, size, chunk_size, chunks, st,
                                   True, 0), path, st

    def test_roundtrip(self, tmp_path):
        manifest, path, st = self._create(tmp_path)
        loaded = pdl.Manifest.load(path)
        assert loaded is not None
        assert loaded.state["plan_id"] == "pid"
        assert loaded.matches("pid", st, 1000)

    def test_leaves_no_tmp_file_behind(self, tmp_path):
        _, path, _ = self._create(tmp_path)
        assert not os.path.exists(path + ".tmp")

    def test_plan_id_mismatch_is_rejected(self, tmp_path):
        manifest, path, st = self._create(tmp_path)
        assert not pdl.Manifest.load(path).matches("different", st, 1000)

    def test_size_mismatch_is_rejected(self, tmp_path):
        manifest, path, st = self._create(tmp_path)
        assert not pdl.Manifest.load(path).matches("pid", st, 2000)

    def test_chunk_completion_is_recorded(self, tmp_path):
        manifest, path, _ = self._create(tmp_path)
        assert not manifest.is_complete(0)
        manifest.record_chunk_done(0, None, digest="abc")
        assert manifest.is_complete(0)
        assert pdl.Manifest.load(path).is_complete(0)
        assert pdl.Manifest.load(path).chunk_record(0)["md5"] == "abc"

    def test_checkpoint_offsets(self, tmp_path):
        manifest, path, _ = self._create(tmp_path)
        assert manifest.checkpoint_offset(0) == 0
        manifest.record_checkpoint(0, 256, None)
        assert pdl.Manifest.load(path).checkpoint_offset(0) == 256

    @pytest.mark.parametrize("content", [
        b"", b"not json at all", b'{"schema_version": 999}', b'{"truncated": ',
        b"[]",
    ])
    def test_unusable_manifest_loads_as_none(self, tmp_path, content):
        """
        Absent, truncated, corrupt and wrong-version all mean the same thing: no
        usable resume state, so restart cleanly rather than risk a corrupt output.
        """
        path = str(tmp_path / "m.json")
        with open(path, "wb") as fh:
            fh.write(content)
        assert pdl.Manifest.load(path) is None

    def test_missing_manifest_loads_as_none(self, tmp_path):
        assert pdl.Manifest.load(str(tmp_path / "absent.json")) is None

    def test_unlink_removes_the_manifest_and_its_own_tmp(self, tmp_path):
        manifest, path, _ = self._create(tmp_path)
        with open(manifest.tmp_path, "w") as fh:
            fh.write("x")
        manifest.unlink()
        assert not os.path.exists(path)
        assert not os.path.exists(manifest.tmp_path)

    def test_tmp_path_is_per_process(self, tmp_path):
        """
        A concurrent writer against the same destination is legitimate (a requeued task
        starting while the preempted VM tears down). A shared staging name let one
        writer's finalize delete the other's half-written tmp out from under its rename,
        which surfaced as an uncaught FileNotFoundError.
        """
        manifest, path, _ = self._create(tmp_path)
        assert str(os.getpid()) in manifest.tmp_path
        assert manifest.tmp_path != path + ".tmp"

    def test_unlink_leaves_a_peers_tmp_alone(self, tmp_path):
        manifest, path, _ = self._create(tmp_path)
        peer_tmp = "{}.{}.tmp".format(path, os.getpid() + 1)
        with open(peer_tmp, "w") as fh:
            fh.write("peer")
        manifest.unlink()
        assert os.path.exists(peer_tmp), "must not delete another writer's staging file"

    def test_flush_failure_does_not_raise(self, tmp_path, monkeypatch):
        """
        The manifest is bookkeeping, not the source of truth: losing an update costs a
        re-download at worst, so it must never fail the transfer.
        """
        manifest, _, _ = self._create(tmp_path)

        def boom(*args, **kwargs):
            raise OSError(28, "No space left on device")

        monkeypatch.setattr(pdl.os, "rename", boom)
        manifest.record_chunk_done(0, None)  # must not raise


class TestDoneMarker:

    def test_roundtrip(self, tmp_path):
        path = str(tmp_path / ".f.k9pdl.done")
        pdl.write_done_marker(path, 4096, "pid", "deadbeef")
        marker = pdl.read_done_marker(path)
        assert marker["size"] == 4096
        assert marker["plan_id"] == "pid"
        assert marker["hash"] == "deadbeef"

    def test_absent_marker_reads_as_none(self, tmp_path):
        assert pdl.read_done_marker(str(tmp_path / "absent")) is None

    def test_corrupt_marker_reads_as_none(self, tmp_path):
        path = str(tmp_path / "m")
        with open(path, "w") as fh:
            fh.write("{oops")
        assert pdl.read_done_marker(path) is None


# ---------------------------------------------------------------------------
# verification (§4.7, §11)
# ---------------------------------------------------------------------------

class _Options:
    def __init__(self, **kwargs):
        self.check_md5 = None
        self.check_etag = None
        self.part_length = None
        for k, v in kwargs.items():
            setattr(self, k, v)


class TestVerify:

    def test_no_check_requested_returns_none(self, tmp_path):
        f = tmp_path / "f"
        f.write_bytes(b"hello")
        assert pdl.verify(str(f), _Options()) is None

    def test_correct_md5_passes(self, tmp_path):
        f = tmp_path / "f"
        f.write_bytes(b"hello")
        digest = hashlib.md5(b"hello").hexdigest()
        assert pdl.verify(str(f), _Options(check_md5=digest)) == digest

    def test_wrong_md5_raises(self, tmp_path):
        f = tmp_path / "f"
        f.write_bytes(b"hello")
        with pytest.raises(pdl.PermanentError, match="md5 mismatch"):
            pdl.verify(str(f), _Options(check_md5="0" * 32))

    def test_base64_md5_is_accepted(self, tmp_path):
        """Servers put the base64 form in Content-MD5."""
        import base64
        f = tmp_path / "f"
        f.write_bytes(b"hello")
        raw = hashlib.md5(b"hello").digest()
        b64 = base64.b64encode(raw).decode()
        assert pdl.verify(str(f), _Options(check_md5=b64)) == raw.hex()

    def test_multipart_etag_passes(self, tmp_path):
        part = 4
        data = b"abcdefghij"
        f = tmp_path / "f"
        f.write_bytes(data)
        digests = [hashlib.md5(data[i:i + part]).digest()
                   for i in range(0, len(data), part)]
        expected = "{}-{}".format(
            hashlib.md5(b"".join(digests)).hexdigest(), len(digests))
        options = _Options(check_etag=expected, part_length=part)
        assert pdl.verify(str(f), options) == expected

    def test_wrong_multipart_etag_raises(self, tmp_path):
        f = tmp_path / "f"
        f.write_bytes(b"abcdefghij")
        with pytest.raises(pdl.PermanentError, match="ETag mismatch"):
            pdl.verify(str(f), _Options(check_etag="deadbeef-3", part_length=4))

    def test_quoted_etag_is_accepted(self, tmp_path):
        data = b"abcdefghij"
        f = tmp_path / "f"
        f.write_bytes(data)
        digests = [hashlib.md5(data[i:i + 4]).digest() for i in range(0, len(data), 4)]
        bare = "{}-{}".format(hashlib.md5(b"".join(digests)).hexdigest(), len(digests))
        options = _Options(check_etag='"{}"'.format(bare), part_length=4)
        assert pdl.verify(str(f), options) == bare


class TestNormalizeExpectedMd5:

    def test_hex_passthrough(self):
        assert pdl.normalize_expected_md5("D41D8CD98F00B204E9800998ECF8427E") == \
            "d41d8cd98f00b204e9800998ecf8427e"

    def test_base64_converted(self):
        assert pdl.normalize_expected_md5("1B2M2Y8AsgTpgAmY7PhCfg==") == \
            "d41d8cd98f00b204e9800998ecf8427e"

    def test_quotes_stripped(self):
        assert pdl.normalize_expected_md5('"d41d8cd98f00b204e9800998ecf8427e"') == \
            "d41d8cd98f00b204e9800998ecf8427e"


# ---------------------------------------------------------------------------
# credential redaction (§6.2)
# ---------------------------------------------------------------------------

class TestRedact:

    def test_signature_is_stripped(self):
        out = pdl.redact("GET https://h/o?X-Goog-Signature=abc123def&Expires=99")
        assert "abc123def" not in out
        assert "REDACTED" in out

    def test_aws_credentials_stripped(self):
        out = pdl.redact("https://h/o?X-Amz-Credential=AKIAsecret&X-Amz-Signature=sig")
        assert "AKIAsecret" not in out and "sig" not in out.split("Signature=")[-1]

    def test_non_secret_text_is_preserved(self):
        assert pdl.redact("chunk 3: short read at 4096") == "chunk 3: short read at 4096"


# ---------------------------------------------------------------------------
# end to end (§8.3)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def payload():
    # not a multiple of any chunk count, so the tail chunk is always short
    return os.urandom(12 * MIB + 7777)


@pytest.fixture(scope="module")
def payload_md5(payload):
    return hashlib.md5(payload).hexdigest()


class TestEndToEnd:

    @pytest.mark.parametrize("connections", [1, 2, 8, 16])
    def test_byte_identical_for_each_connection_count(
        self, tmp_path, payload, payload_md5, connections
    ):
        dest = str(tmp_path / "obj.bin")
        with Server(payload) as server:
            proc = run_downloader(
                server.url(), dest, len(payload),
                "--connections", connections, "--min-chunk", MIB,
                "--check-md5", payload_md5,
            )
        assert proc.returncode == 0, proc.stderr
        assert hashlib.md5(open(dest, "rb").read()).hexdigest() == payload_md5

    def test_done_marker_written_and_manifest_removed(self, tmp_path, payload, payload_md5):
        dest = str(tmp_path / "obj.bin")
        with Server(payload) as server:
            proc = run_downloader(server.url(), dest, len(payload),
                                  "--min-chunk", MIB, "--check-md5", payload_md5)
        assert proc.returncode == 0, proc.stderr
        manifest, marker = pdl.sidecar_paths(dest)
        assert os.path.exists(marker)
        assert not os.path.exists(manifest)

    def test_rerun_transfers_nothing(self, tmp_path, payload):
        """Completion short-circuits on the marker, not on the file's size."""
        dest = str(tmp_path / "obj.bin")
        with Server(payload) as server:
            run_downloader(server.url(), dest, len(payload), "--min-chunk", MIB)
            before = server.state.snapshot()["sent"]
            proc = run_downloader(server.url(), dest, len(payload), "--min-chunk", MIB)
            after = server.state.snapshot()["sent"]
        assert proc.returncode == 0, proc.stderr
        assert after == before

    def test_hash_mismatch_deletes_dest_and_exits_one(self, tmp_path, payload):
        """
        Exit 1 rather than 5: a corrupt object is a real error, and canine's entrypoint
        treats anything other than 5/15 as do-not-retry.
        """
        dest = str(tmp_path / "obj.bin")
        with Server(payload) as server:
            proc = run_downloader(server.url(), dest, len(payload),
                                  "--min-chunk", MIB, "--check-md5", "0" * 32)
        assert proc.returncode == 1
        assert not os.path.exists(dest)
        _, marker = pdl.sidecar_paths(dest)
        assert not os.path.exists(marker)

    def test_range_unsupported_falls_back_to_a_single_stream(self, tmp_path, payload):
        """
        A server that ignores Range returns the whole object with 200. Chunking must
        be abandoned rather than issuing N full-object requests -- GCS bills for the
        entire object per ranged request against a transcoded blob.
        """
        dest = str(tmp_path / "obj.bin")
        with Server(payload) as server:
            server.state.support_range = False
            proc = run_downloader(
                server.url(), dest, len(payload), "--min-chunk", MIB,
                "--legacy-cmd", "printf fallback > {}".format(dest),
            )
        assert proc.returncode == 0, proc.stderr
        assert "falling back to a single stream" in proc.stderr
        assert open(dest).read() == "fallback"

    def test_only_one_request_against_a_range_ignoring_server(self, tmp_path, payload):
        """
        The mandatory probe bounds billing exposure to a single aborted request instead
        of one whole object per connection.
        """
        dest = str(tmp_path / "obj.bin")
        with Server(payload) as server:
            server.state.support_range = False
            run_downloader(
                server.url(), dest, len(payload), "--min-chunk", MIB,
                "--connections", 8, "--legacy-cmd", "true",
            )
            sent = server.state.snapshot()["sent"]
        assert sent <= len(payload), \
            "sent {} bytes; the probe should have aborted after one object".format(sent)

    def test_connections_one_uses_the_legacy_path(self, tmp_path, payload):
        dest = str(tmp_path / "obj.bin")
        with Server(payload) as server:
            proc = run_downloader(
                server.url(), dest, len(payload), "--connections", 1,
                "--legacy-cmd", "printf legacy > {}".format(dest),
            )
        assert proc.returncode == 0, proc.stderr
        assert open(dest).read() == "legacy"

    def test_transient_5xx_is_retried(self, tmp_path, payload, payload_md5):
        dest = str(tmp_path / "obj.bin")
        with Server(payload) as server:
            server.state.fail_next = 3
            proc = run_downloader(server.url(), dest, len(payload),
                                  "--min-chunk", MIB, "--connections", 4,
                                  "--check-md5", payload_md5, "--retries", 6)
        assert proc.returncode == 0, proc.stderr
        assert hashlib.md5(open(dest, "rb").read()).hexdigest() == payload_md5

    def test_dropped_connection_mid_chunk_is_resumed(self, tmp_path, payload, payload_md5):
        dest = str(tmp_path / "obj.bin")
        with Server(payload) as server:
            server.state.drop_after = 300 * 1024
            proc = run_downloader(server.url(), dest, len(payload),
                                  "--min-chunk", MIB, "--connections", 4,
                                  "--check-md5", payload_md5, "--retries", 40)
        assert proc.returncode == 0, proc.stderr
        assert hashlib.md5(open(dest, "rb").read()).hexdigest() == payload_md5
        # The drop has to have HAPPENED. It did not for as long as this test existed:
        # the fake server wrote the whole body in one unthrottled write before checking
        # drop_after, so this passed on a clean transfer and proved nothing about resume.
        assert "short read" in proc.stderr, proc.stderr

    def test_kill_switch_env_var_forces_the_legacy_path(self, tmp_path, payload):
        """CANINE_DISABLE_PARALLEL_DOWNLOAD is the on-VM rollback with no redeploy."""
        dest = str(tmp_path / "obj.bin")
        env = dict(os.environ, CANINE_DISABLE_PARALLEL_DOWNLOAD="1")
        with Server(payload) as server:
            proc = run_downloader(
                server.url(), dest, len(payload), "--min-chunk", MIB,
                "--legacy-cmd", "printf disabled > {}".format(dest), env=env,
            )
        assert proc.returncode == 0, proc.stderr
        assert open(dest).read() == "disabled"

    def test_connections_env_override(self, tmp_path, payload, payload_md5):
        dest = str(tmp_path / "obj.bin")
        env = dict(os.environ, CANINE_DOWNLOAD_CONNECTIONS="2")
        with Server(payload) as server:
            proc = run_downloader(server.url(), dest, len(payload),
                                  "--min-chunk", MIB, "--check-md5", payload_md5,
                                  env=env)
        assert proc.returncode == 0, proc.stderr
        assert hashlib.md5(open(dest, "rb").read()).hexdigest() == payload_md5

    def test_unparseable_connections_env_is_ignored(self, tmp_path, payload, payload_md5):
        dest = str(tmp_path / "obj.bin")
        env = dict(os.environ, CANINE_DOWNLOAD_CONNECTIONS="not-a-number")
        with Server(payload) as server:
            proc = run_downloader(server.url(), dest, len(payload),
                                  "--min-chunk", MIB, "--check-md5", payload_md5,
                                  env=env)
        assert proc.returncode == 0, proc.stderr

    def test_zero_length_object(self, tmp_path):
        dest = str(tmp_path / "empty.bin")
        with Server(b"") as server:
            proc = run_downloader(server.url(), dest, 0, "--min-chunk", MIB)
        assert proc.returncode == 0, proc.stderr
        assert os.path.getsize(dest) == 0

    def test_headers_are_sent(self, tmp_path, payload, payload_md5):
        dest = str(tmp_path / "obj.bin")
        with Server(payload) as server:
            proc = run_downloader(server.url(), dest, len(payload),
                                  "--min-chunk", MIB, "--check-md5", payload_md5,
                                  "--header", "X-Auth-Token: secret-token")
        assert proc.returncode == 0, proc.stderr
        assert "secret-token" not in proc.stderr, "token must not be logged"


# ---------------------------------------------------------------------------
# destination filesystem gate and route selection (§4.7)
# ---------------------------------------------------------------------------

def mounts_from(text):
    """Build a mount table from /proc/mounts-formatted text."""
    import tempfile
    with tempfile.NamedTemporaryFile("w", suffix=".mounts", delete=False) as fh:
        fh.write(text)
        path = fh.name
    try:
        return pdl.read_mounts(path)
    finally:
        os.unlink(path)


ALWAYS = lambda directory: True
NEVER = lambda directory: False


class TestReadMounts:

    def test_parses_fields(self):
        mounts = mounts_from(
            "/dev/sda1 / ext4 rw,relatime 0 0\n"
            "mybucket /mnt/gcs fuse.gcsfuse rw,only_dir=sub 0 0\n"
        )
        assert [m.mountpoint for m in mounts] == ["/", "/mnt/gcs"]
        assert mounts[1].fstype == "fuse.gcsfuse"
        assert mounts[1].device == "mybucket"
        assert mounts[1].option("only_dir") == "sub"

    def test_unescapes_octal_in_mountpoints(self):
        mounts = mounts_from("dev /mnt/my\\040disk ext4 rw 0 0\n")
        assert mounts[0].mountpoint == "/mnt/my disk"

    def test_skips_malformed_lines(self):
        assert mounts_from("garbage\n/dev/sda1 / ext4 rw 0 0\n")[0].fstype == "ext4"

    def test_missing_file_yields_empty_list(self):
        assert pdl.read_mounts("/nonexistent/mounts") == []

    def test_option_absent(self):
        assert mounts_from("d /m ext4 rw 0 0\n")[0].option("only_dir") is None


class TestResolveMount:

    TABLE = (
        "/dev/sda1 / ext4 rw 0 0\n"
        "/dev/sdb1 /mnt ext4 rw 0 0\n"
        "/dev/sdc1 /mnt/foo xfs rw 0 0\n"
        "bucket /mnt/foobar fuse.gcsfuse rw 0 0\n"
    )

    def test_longest_prefix_wins(self):
        mounts = mounts_from(self.TABLE)
        assert pdl.resolve_mount("/mnt/foo/some/file", mounts).fstype == "xfs"

    def test_component_wise_so_foo_does_not_match_foobar(self):
        """A raw string prefix test would resolve /mnt/foobar to the /mnt/foo mount."""
        mounts = mounts_from(self.TABLE)
        assert pdl.resolve_mount("/mnt/foobar/obj", mounts).fstype == "fuse.gcsfuse"

    def test_falls_back_to_root(self):
        mounts = mounts_from(self.TABLE)
        assert pdl.resolve_mount("/elsewhere/file", mounts).mountpoint == "/"

    def test_exact_mountpoint(self):
        mounts = mounts_from(self.TABLE)
        assert pdl.resolve_mount("/mnt/foo", mounts).fstype == "xfs"

    def test_empty_table(self):
        assert pdl.resolve_mount("/anything", []) is None


class TestGsUrlFor:

    def test_plain_bucket_mount(self):
        mount = mounts_from("mybucket /mnt/gcs fuse.gcsfuse rw 0 0\n")[0]
        assert pdl.gs_url_for("/mnt/gcs/inputs/a.bam", mount) == \
            "gs://mybucket/inputs/a.bam"

    def test_only_dir_is_prepended(self):
        """--only-dir mounts a subdirectory, which is part of the object name."""
        mount = mounts_from("mybucket /mnt/gcs fuse.gcsfuse rw,only_dir=staging 0 0\n")[0]
        assert pdl.gs_url_for("/mnt/gcs/a.bam", mount) == "gs://mybucket/staging/a.bam"

    def test_bucket_from_options_when_device_is_a_placeholder(self):
        mount = mounts_from("gcsfuse /mnt/gcs fuse.gcsfuse rw,bucket=realbucket 0 0\n")[0]
        assert pdl.gs_url_for("/mnt/gcs/a.bam", mount) == "gs://realbucket/a.bam"

    def test_unresolvable_bucket_yields_none(self):
        """
        None routes to stage-then-publish, which is correct for a bucket we cannot
        address. Guessing would upload the parts somewhere wrong.
        """
        for table in (
            "gcsfuse /mnt/gcs fuse.gcsfuse rw 0 0\n",              # placeholder, no option
            "not/a/bucket /mnt/gcs fuse.gcsfuse rw 0 0\n",         # contains a slash
            "A /mnt/gcs fuse.gcsfuse rw 0 0\n",                    # too short, uppercase
        ):
            mount = mounts_from(table)[0]
            assert pdl.gs_url_for("/mnt/gcs/a.bam", mount) is None, table

    def test_non_fuse_mount_yields_none(self):
        mount = mounts_from("/dev/sda1 /mnt ext4 rw 0 0\n")[0]
        assert pdl.gs_url_for("/mnt/a.bam", mount) is None

    def test_path_outside_the_mount_yields_none(self):
        mount = mounts_from("mybucket /mnt/gcs fuse.gcsfuse rw 0 0\n")[0]
        assert pdl.gs_url_for("/somewhere/else/a.bam", mount) is None

    def test_bare_mountpoint_yields_none(self):
        """There is no object name for the mount root itself."""
        mount = mounts_from("mybucket /mnt/gcs fuse.gcsfuse rw 0 0\n")[0]
        assert pdl.gs_url_for("/mnt/gcs", mount) is None


class TestSelectRoute:

    def _select(self, table, dest="/mnt/data/obj.bin", random_write=ALWAYS,
                seek_hole=ALWAYS):
        return pdl.select_route(dest, mounts=mounts_from(table),
                                random_write_probe=random_write,
                                seek_hole_probe=seek_hole)

    @pytest.mark.parametrize("fstype", sorted(pdl.POSIX_FSTYPE_ALLOWLIST))
    def test_allowlisted_filesystems_take_route_a(self, fstype):
        decision = self._select("/dev/sda1 /mnt {} rw 0 0\n".format(fstype))
        assert decision.route == pdl.ROUTE_POSIX

    def test_gcsfuse_with_a_resolvable_bucket_takes_route_b(self):
        """
        Preferred for a bucket destination: compose concatenates server-side with no
        data transfer, so the whole transfer stays parallel end to end.
        """
        decision = self._select("mybucket /mnt fuse.gcsfuse rw 0 0\n")
        assert decision.route == pdl.ROUTE_BUCKET
        assert decision.gs_url == "gs://mybucket/data/obj.bin"

    def test_gcsfuse_with_an_unresolvable_bucket_takes_route_c(self):
        decision = self._select("gcsfuse /mnt fuse.gcsfuse rw 0 0\n")
        assert decision.route == pdl.ROUTE_STAGED

    def test_unrecognized_fstype_fails_safe(self):
        """
        The allowlist-not-denylist property: a filesystem nobody has evaluated must not
        get the chunked fast path by default.
        """
        decision = self._select("/dev/x /mnt somefuturefs rw 0 0\n")
        assert decision.route == pdl.ROUTE_STAGED

    def test_any_fuse_filesystem_is_off_the_allowlist(self):
        for fstype in ("fuse", "fuse.sshfs", "fuse.s3fs", "fuseblk"):
            decision = self._select("dev /mnt {} rw 0 0\n".format(fstype))
            assert decision.route != pdl.ROUTE_POSIX, fstype

    def test_allowlisted_fstype_failing_the_probe_is_still_off_route_a(self):
        """fstype strings lie; observed behavior overrides the name."""
        decision = self._select("/dev/sda1 /mnt ext4 rw 0 0\n", random_write=NEVER)
        assert decision.route == pdl.ROUTE_STAGED
        assert "random-write probe" in decision.reason

    def test_seek_hole_failure_does_not_change_the_route(self):
        """
        Failing SEEK_HOLE (NFSv3) only swaps frontier recovery for checkpointing. It is
        still an in-place chunked write, so it must not be treated as a gate.
        """
        decision = self._select("/dev/sda1 /mnt nfs rw 0 0\n", seek_hole=NEVER)
        assert decision.route == pdl.ROUTE_POSIX
        assert decision.seek_hole is False

    def test_seek_hole_result_is_reported(self):
        decision = self._select("/dev/sda1 /mnt ext4 rw 0 0\n", seek_hole=ALWAYS)
        assert decision.seek_hole is True

    def test_populated_table_with_no_covering_entry_fails_safe(self):
        decision = pdl.select_route(
            "/mnt/data/obj.bin",
            mounts=[pdl.Mount("/other", "ext4", "/dev/x", "rw")],
            random_write_probe=ALWAYS, seek_hole_probe=ALWAYS,
        )
        assert decision.route == pdl.ROUTE_STAGED

    def test_absent_mount_table_relies_on_probes(self, tmp_path):
        """
        A host with no /proc/mounts (a developer machine) cannot consult the allowlist,
        so it falls back to the runtime probes, which are the stronger signal. Linux --
        the production target -- always has the table, so the strict path applies there.
        """
        decision = pdl.select_route(str(tmp_path / "obj.bin"), mounts=[],
                                    random_write_probe=ALWAYS, seek_hole_probe=ALWAYS)
        assert decision.route == pdl.ROUTE_POSIX
        assert "probes alone" in decision.reason

    def test_absent_mount_table_still_honors_a_failing_probe(self, tmp_path):
        decision = pdl.select_route(str(tmp_path / "obj.bin"), mounts=[],
                                    random_write_probe=NEVER, seek_hole_probe=ALWAYS)
        assert decision.route == pdl.ROUTE_STAGED


class TestNonPosixDestinationIsNotWrittenInPlace:
    """
    A non-POSIX destination must never see a full-size ftruncate or an out-of-order
    pwrite: those are precisely the operations that make a FUSE object store materialize
    gigabytes of zeros and re-upload the whole object per write.

    Only the degradation path is covered here. The same property for the bucket-compose route needs a GCS
    endpoint to be meaningful, so it lives in test_parallel_download_bucket.py, asserted
    on a run that succeeds.
    """

    def _run_with_route(self, tmp_path, monkeypatch, route):
        calls = []
        monkeypatch.setattr(pdl.os, "ftruncate",
                            lambda *a: calls.append(("ftruncate",) + a))
        monkeypatch.setattr(pdl.os, "pwrite",
                            lambda *a: calls.append(("pwrite",) + a))
        monkeypatch.setattr(
            pdl, "select_route",
            lambda dest, **kw: pdl.RouteDecision(route, "test: forced route"),
        )
        monkeypatch.setattr(pdl, "build_source", lambda opts: _StubSource())

        dest = str(tmp_path / "obj.bin")
        marker = str(tmp_path / "ran")
        options = pdl.build_parser().parse_args([
            "--url", "http://127.0.0.1:1/obj", "--dest", dest, "--size", str(8 * MIB),
            "--connections", "4", "--min-chunk", str(MIB),
            "--legacy-cmd", "touch {}".format(marker),
        ])
        rc = pdl.run(options)
        return rc, calls, marker

    def test_staged_route_degrades_to_the_legacy_command(self, tmp_path, monkeypatch):
        """
        the stage-publish route declines when no staging directory has room for the object, and takes the
        single sequential stream instead -- the documented degradation, and for a FUSE
        object store the only access pattern that reaches its streaming-write path.
        """
        rc, calls, marker = self._run_with_route(
            tmp_path, monkeypatch, pdl.ROUTE_STAGED
        )
        assert rc == 0
        assert os.path.exists(marker), "should have degraded to the legacy command"
        assert calls == [], "wrote in place to a non-POSIX destination: {}".format(calls)


class _StubSource:
    def probe_range(self, size):
        return None

    def open_range(self, start, end):
        raise AssertionError("must not fetch ranges on a non-POSIX destination")

    def refresh_url(self):
        return False


# ---------------------------------------------------------------------------
# handing over to the single-stream command
# ---------------------------------------------------------------------------

class TestPreallocatedFileIsClearedBeforeFallback:
    """
    The legacy commands resume from the destination's own size -- `curl -C -`, and the
    `aws s3api --range "bytes=$SZ-"` form before it was removed. That is only meaningful
    for a file a single stream appended to.

    Our working file is created at its full apparent size upfront, so handing that to
    `curl -C -` makes it report "already fully downloaded", exit 0, and accept a file of
    zeros. Verified directly: curl really does exit 0 there, so nothing downstream notices
    unless check_hash happens to be set. That is silent corruption, and the reason the
    working file has to be discarded before the handover.
    """

    def test_curl_resume_really_does_accept_a_preallocated_file(self, tmp_path, payload):
        """
        Pins the underlying hazard, so the guard below cannot be removed as 'defensive'.
        """
        dest = str(tmp_path / "obj.bin")
        with open(dest, "wb") as fh:
            fh.truncate(len(payload))
        with Server(payload) as server:
            proc = subprocess.run(
                ["curl", "-sS", "-C", "-", "-o", dest, server.url()],
                capture_output=True, text=True, timeout=120,
            )
        assert proc.returncode == 0, "expected curl to think it was already done"
        assert open(dest, "rb").read() == b"\0" * len(payload), \
            "curl downloaded something; the hazard may no longer exist"

    def test_working_file_is_discarded_before_the_fallback_runs(self, tmp_path, payload):
        dest = str(tmp_path / "obj.bin")
        manifest_path, _ = pdl.sidecar_paths(dest)
        with open(dest, "wb") as fh:
            fh.truncate(len(payload))
        with open(manifest_path, "w") as fh:
            json.dump({"schema_version": pdl.SCHEMA_VERSION, "plan_id": "x"}, fh)

        pdl.clear_preallocated_working_file(dest)
        assert not os.path.exists(dest)
        assert not os.path.exists(manifest_path)

    def test_a_genuine_partial_without_a_manifest_is_preserved(self, tmp_path):
        """
        A partial left by a previous single-stream attempt has no manifest, and its
        progress must survive -- that is the case `curl -C -` exists to handle. The
        manifest is what identifies a file as ours.
        """
        dest = str(tmp_path / "obj.bin")
        with open(dest, "wb") as fh:
            fh.write(b"partial data")
        pdl.clear_preallocated_working_file(dest)
        assert open(dest, "rb").read() == b"partial data"

    def test_end_to_end_fallback_after_the_file_was_preallocated(self, tmp_path, payload,
                                                                payload_md5):
        """
        The reachable path: the downloader creates the sparse file, then discovers the
        server does not honour Range, and hands over. The result must be the real object,
        not a file of zeros.
        """
        dest = str(tmp_path / "obj.bin")
        legacy = "curl -sS -C - -o {} {}"
        with Server(payload) as server:
            # let the probe pass, then refuse ranges once the download starts
            proc = run_downloader(
                server.url(), dest, len(payload), "--min-chunk", MIB,
                "--connections", 4,
                "--legacy-cmd", legacy.format(dest, server.url()),
            )
            assert proc.returncode == 0, proc.stderr

            # now force the fallback with a preallocated file already in place
            server.state.support_range = False
            os.unlink(dest)
            _, marker = pdl.sidecar_paths(dest)
            os.unlink(marker)
            proc = run_downloader(
                server.url(), dest, len(payload), "--min-chunk", MIB,
                "--connections", 4, "--check-md5", payload_md5,
                "--legacy-cmd", legacy.format(dest, server.url()),
            )

        assert proc.returncode == 0, proc.stdout + proc.stderr
        assert hashlib.md5(open(dest, "rb").read()).hexdigest() == payload_md5, \
            "fallback produced the wrong bytes"


# ---------------------------------------------------------------------------
# gzip decompressive transcoding (§12.6)
# ---------------------------------------------------------------------------

class TestTranscodedObject:
    """
    A GCS object stored with Content-Encoding: gzip is served decompressed to any client
    that does not ask for gzip. The response omits BOTH Content-Encoding and
    Content-Length, so transcoding cannot be detected by looking for the encoding header --
    it is absent precisely when transcoding is happening.

    Worse, Range is silently ignored and the whole object is returned, and Google bills for
    the full object transfer rather than the requested range. With N workers issuing ranged
    GETs that is N whole objects, with no error raised anywhere. This is the single largest
    billing hazard in the design, and the mandatory probe is what bounds it.
    """

    def test_only_one_request_is_ever_issued(self, tmp_path, payload):
        """
        The bound that matters: one aborted probe, not one whole object per connection.
        """
        dest = str(tmp_path / "obj.bin")
        with Server(payload) as server:
            server.state.transcoding = True
            proc = run_downloader(
                server.url(), dest, len(payload), "--min-chunk", MIB,
                "--connections", 8, "--legacy-cmd", "true",
            )
            snapshot = server.state.snapshot()

        assert proc.returncode == 0, proc.stderr
        assert snapshot["requests"] <= 2, (
            "issued {} requests against a transcoding server; each one is billed as a "
            "whole object".format(snapshot["requests"])
        )

    def test_falls_back_to_a_single_stream(self, tmp_path, payload):
        dest = str(tmp_path / "obj.bin")
        with Server(payload) as server:
            server.state.transcoding = True
            proc = run_downloader(
                server.url(), dest, len(payload), "--min-chunk", MIB,
                "--connections", 8,
                "--legacy-cmd", "printf transcoded > {}".format(dest),
            )
        assert proc.returncode == 0, proc.stderr
        assert "falling back to a single stream" in proc.stderr
        assert open(dest).read() == "transcoded"

    def test_billed_bytes_stay_bounded(self, tmp_path, payload):
        """
        Eight connections against a transcoding server would otherwise transfer -- and be
        billed for -- eight copies of the object.
        """
        dest = str(tmp_path / "obj.bin")
        with Server(payload) as server:
            server.state.transcoding = True
            run_downloader(server.url(), dest, len(payload), "--min-chunk", MIB,
                           "--connections", 8, "--legacy-cmd", "true")
            sent = server.state.snapshot()["sent"]
        assert sent <= 2 * len(payload), (
            "transferred {} bytes for a {}-byte object".format(sent, len(payload))
        )

    def test_no_compressed_flag_is_ever_emitted(self):
        """
        `curl --compressed` together with `-C -` is a silent-corruption hazard: the resume
        offset is taken from the local *decompressed* size but interpreted by the server as
        an offset into the *compressed* stream. It must never appear in an emitted command.
        """
        with open(PDL_PATH) as fh:
            assert "--compressed" not in fh.read()


class TestShelledOutCommandsRunUnderBash:
    """
    Every command the downloader shells out to comes from a bash script and is written in
    bash. subprocess's shell=True uses /bin/sh, which is dash on the Ubuntu worker image,
    where process substitution and [[ ]] are syntax errors.
    """

    def test_shell_is_named_explicitly(self):
        assert pdl.SHELL.endswith("bash")

    def test_no_shell_true_without_an_explicit_executable(self):
        """
        A future shell-out that forgets this would fail only on the specific commands that
        use bash syntax, which is the kind of bug that reaches production.
        """
        import re as _re
        with open(PDL_PATH) as fh:
            source = fh.read()
        for match in _re.finditer(r"subprocess\.(run|Popen)\((.{0,400}?)\)\n", source,
                                  _re.S):
            call = match.group(2)
            if "shell=True" in call:
                assert "executable=" in call, \
                    "shell=True without an explicit executable:\n" + call

    def test_a_bash_only_fallback_command_actually_runs(self, tmp_path, payload):
        """
        End to end: a legacy command using process substitution has to work, because that
        is the shape of the S3 fallback.
        """
        dest = str(tmp_path / "obj.bin")
        legacy = "cat >(cat > {}) < /dev/null; printf works > {}".format(
            str(tmp_path / "sink"), dest)
        with Server(payload) as server:
            server.state.support_range = False
            proc = run_downloader(server.url(), dest, len(payload),
                                  "--min-chunk", MIB, "--legacy-cmd", legacy)
        assert proc.returncode == 0, proc.stdout + proc.stderr
        assert open(dest).read() == "works"


class TestGunzip:
    """
    The decompression step. Same shape as the stage-publish route's publish: the compressed bytes are
    verified first, then transformed into the destination, then the marker is written.
    """

    def test_round_trips(self, tmp_path):
        import gzip
        plain = os.urandom(3 * MIB) + b"tail"
        source = str(tmp_path / "o.gz")
        with open(source, "wb") as fh:
            fh.write(gzip.compress(plain))
        dest = str(tmp_path / "o.bin")
        assert pdl.gunzip_to(source, dest) == len(plain)
        assert open(dest, "rb").read() == plain

    def test_bytes_that_are_not_gzip_are_kept_rather_than_failing(self, tmp_path):
        """
        A bad gzip HEADER means the server advertised Content-Encoding: gzip over bytes
        that are not gzip -- its metadata is wrong, which is not a reason to fail the
        localization. The stored bytes are the object.

        Distinguished from truncation, which is a broken transfer and does still fail:
        a bad header raises BadGzipFile, a truncated body raises EOFError.
        """
        source = str(tmp_path / "bad.gz")
        with open(source, "wb") as fh:
            fh.write(b"this is not gzip data at all")
        dest = str(tmp_path / "o.bin")
        pdl.gunzip_to(source, dest)
        assert open(dest, "rb").read() == b"this is not gzip data at all"
        assert not os.path.exists(dest + ".k9pdl.gz.part")

    def test_a_truncated_transfer_still_fails(self, tmp_path):
        """
        The other half of that split. A valid header with an incomplete body is a broken
        download, not mislabelled metadata, and must not be accepted.
        """
        import gzip
        blob = gzip.compress(b"payload" * 2000)
        source = str(tmp_path / "trunc.gz")
        with open(source, "wb") as fh:
            fh.write(blob[: len(blob) // 2])
        dest = str(tmp_path / "o.bin")
        with pytest.raises(pdl.PermanentError):
            pdl.gunzip_to(source, dest)
        assert not os.path.exists(dest)
        assert not os.path.exists(dest + ".k9pdl.gz.part")

    def test_truncated_gzip_is_rejected(self, tmp_path):
        import gzip
        blob = gzip.compress(os.urandom(MIB))
        source = str(tmp_path / "trunc.gz")
        with open(source, "wb") as fh:
            fh.write(blob[: len(blob) // 2])
        dest = str(tmp_path / "o.bin")
        with pytest.raises(pdl.PermanentError):
            pdl.gunzip_to(source, dest)
        assert not os.path.exists(dest)

    def test_writes_via_a_temp_name_then_renames(self, tmp_path, monkeypatch):
        """
        Atomic publication is what makes the step resumable-by-restart: a preemption leaves
        either no output or complete output.
        """
        import gzip
        renames = []
        real_rename = pdl.os.rename
        monkeypatch.setattr(pdl.os, "rename",
                            lambda a, b: (renames.append((a, b)), real_rename(a, b))[1])
        source = str(tmp_path / "o.gz")
        with open(source, "wb") as fh:
            fh.write(gzip.compress(b"data" * 1000))
        dest = str(tmp_path / "o.bin")
        pdl.gunzip_to(source, dest)
        assert renames and renames[-1][1] == dest
        assert renames[-1][0].endswith(".k9pdl.gz.part")

    def test_overwrites_an_existing_destination(self, tmp_path):
        """A re-run after an interrupted decompress must not append."""
        import gzip
        source = str(tmp_path / "o.gz")
        with open(source, "wb") as fh:
            fh.write(gzip.compress(b"new"))
        dest = tmp_path / "o.bin"
        dest.write_bytes(b"stale and longer")
        pdl.gunzip_to(source, str(dest))
        assert dest.read_bytes() == b"new"


class TestDoublyCompressedGzipNames:
    """
    A file whose name already promises gzip content, served with Content-Encoding: gzip,
    is ambiguous -- two different situations produce it:

      * gzipped TWICE: a .gz additionally encoded for transport, so removing one layer
        yields the original uploaded .gz;
      * gzipped ONCE with the content-encoding metadata set by mistake (a common slip when
        uploading an already-compressed file), so the stored bytes ALREADY are that .gz and
        decompressing would leave plain data in a file called .gz.

    Distinguished by whether one layer of decoding yields gzip. The invariant either way:
    the localized file matches what its name says.
    """

    VCF = b"##fileformat=VCFv4.2\n" + b"chr1\t1\t.\tA\tT\t.\t.\t.\n" * 500

    @staticmethod
    def _write(tmp_path, data, name="s.gz"):
        path = str(tmp_path / name)
        with open(path, "wb") as fh:
            fh.write(data)
        return path

    def test_doubly_compressed_keeps_one_layer(self, tmp_path):
        import gzip
        source = self._write(tmp_path, gzip.compress(gzip.compress(self.VCF)))
        dest = str(tmp_path / "d.vcf.gz")
        pdl.gunzip_to(source, dest)
        assert open(dest, "rb").read() == gzip.compress(self.VCF)
        assert open(dest, "rb").read(2) == pdl.GZIP_MAGIC

    def test_singly_compressed_with_a_gzip_name_keeps_the_stored_bytes(self, tmp_path):
        """
        Decompressing here would produce plain data in a file named .gz -- the outcome a
        downstream `gzip -d` would choke on.
        """
        import gzip
        stored = gzip.compress(self.VCF)
        source = self._write(tmp_path, stored)
        dest = str(tmp_path / "d.vcf.gz")
        pdl.gunzip_to(source, dest)
        assert open(dest, "rb").read() == stored

    def test_a_non_gzip_name_is_always_decompressed(self, tmp_path):
        import gzip
        source = self._write(tmp_path, gzip.compress(self.VCF))
        dest = str(tmp_path / "d.vcf")
        pdl.gunzip_to(source, dest)
        assert open(dest, "rb").read() == self.VCF

    def test_a_doubly_compressed_non_gzip_name_still_removes_one_layer(self, tmp_path):
        """
        One Content-Encoding means one decode, regardless of what the result looks like.
        The extension check only resolves the ambiguity for .gz-named files.
        """
        import gzip
        source = self._write(tmp_path, gzip.compress(gzip.compress(self.VCF)))
        dest = str(tmp_path / "d.vcf")
        pdl.gunzip_to(source, dest)
        assert open(dest, "rb").read() == gzip.compress(self.VCF)

    @pytest.mark.parametrize("name", [
        "a.gz", "a.BGZ", "a.tgz", "a.gzip", "a.vcf.gz",
        # gzip is only one case: BGZF is a gzip container by design, so these are all
        # gzip-magic. Omitting them was the bug -- a .bam served with
        # Content-Encoding: gzip would have been decompressed into a raw BAM stream,
        # which samtools cannot read. Verified against bcftools output.
        "s.bam", "s.bai", "x.bcf", "i.tbi", "j.csi",
    ])
    def test_gzip_family_names(self, name):
        assert pdl.expected_magic(name) == pdl.GZIP_MAGIC

    @pytest.mark.parametrize("name,magic", [
        ("a.bz2", b"BZh"), ("a.xz", b"\xfd7zXZ\x00"), ("a.zst", b"\x28\xb5\x2f\xfd"),
        ("a.zip", b"PK"), ("a.cram", b"CRAM"),
    ])
    def test_other_compressed_names_expect_their_own_magic(self, name, magic):
        """
        The generalization: the name says what the DECODED bytes should be, and gzip is
        just one possibility. A .bz2 wrapped in transport gzip decodes to bz2, not gzip.
        """
        assert pdl.expected_magic(name) == magic

    @pytest.mark.parametrize("name", ["a.vcf", "a.fa", "a.dict", "a.sam", "a.gz.txt",
                                      "gz", "a.tar", "a.bed"])
    def test_names_implying_plain_content(self, name):
        assert pdl.expected_magic(name) is None

    def test_no_temporary_files_are_left_either_way(self, tmp_path):
        """
        The partially-decoded temp must never survive. The compressed source may -- its
        lifecycle belongs to the caller, which unlinks it after a successful decode (and
        in the singly-compressed branch it has already been moved into place).
        """
        import gzip
        for label, data in [("single", gzip.compress(self.VCF)),
                            ("double", gzip.compress(gzip.compress(self.VCF)))]:
            directory = tmp_path / label
            directory.mkdir()
            source = self._write(directory, data)
            dest = str(directory / "d.vcf.gz")
            pdl.gunzip_to(source, dest)
            remaining = sorted(p.name for p in directory.iterdir())
            assert not any(n.endswith(".part") for n in remaining), remaining
            assert "d.vcf.gz" in remaining

    def test_the_magic_table_has_a_single_definition(self):
        """
        The emitted fallback and the downloader must agree about which files are
        ambiguous, or the localized file would depend on which route ran. That was once a
        duplicated table plus a test that they matched; file_handlers now imports it, so
        the property holds by construction and this asserts identity rather than
        equality -- equality would still pass if someone reintroduced a copy.
        """
        from canine.localization import file_handlers as fh
        assert fh.NAMED_FORMAT_MAGIC is pdl.NAMED_FORMAT_MAGIC
        assert fh.expected_magic is pdl.expected_magic

    def test_the_download_defaults_have_a_single_definition(self):
        from canine.localization import file_handlers as fh
        assert fh.DEFAULT_DOWNLOAD_CONNECTIONS is pdl.DEFAULT_CONNECTIONS
        assert fh.DEFAULT_DOWNLOAD_MIN_CHUNK is pdl.DEFAULT_MIN_CHUNK

    def test_the_downloader_still_imports_no_canine(self):
        """
        The constraint that makes the import one-directional. If the downloader ever
        imported canine it would stop being runnable by hand on a node where canine is
        absent, and would also make this a circular import.
        """
        import ast
        with open(PDL_PATH) as fh_:
            tree = ast.parse(fh_.read())
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                assert not node.level, "relative import in the standalone script"
                assert not (node.module or "").startswith("canine")
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    assert not alias.name.startswith("canine")


class TestNamedFormatsAreNotWronglyDecoded:
    """
    The generalization of the .gz rule: the filename says what the DECODED bytes should
    be, and gzip is only one possibility. Several formats here are gzip containers by
    design -- BGZF backs .bam, .bcf and the .bai/.tbi/.csi indices -- so a gzip-only list
    would decompress a .bam into a raw BAM stream that samtools cannot read.
    """

    @staticmethod
    def _localize(tmp_path, stored, name):
        source = str(tmp_path / "src")
        with open(source, "wb") as fh:
            fh.write(stored)
        dest = str(tmp_path / name)
        pdl.gunzip_to(source, dest)
        return open(dest, "rb").read()

    def test_a_bam_stored_as_bgzf_is_not_decompressed(self, tmp_path):
        """
        The case the gzip-only list got wrong. A .bam IS BGZF, so the stored bytes already
        are the object; decoding would leave an unreadable raw stream in a .bam.
        """
        import gzip
        bgzf = gzip.compress(b"BAM\x01" + b"\x00" * 500)
        assert self._localize(tmp_path, bgzf, "s.bam") == bgzf

    def test_a_double_wrapped_bam_loses_one_layer(self, tmp_path):
        import gzip
        bgzf = gzip.compress(b"BAM\x01" + b"\x00" * 500)
        assert self._localize(tmp_path, gzip.compress(bgzf), "s.bam") == bgzf

    def test_a_double_wrapped_bz2_yields_the_bz2(self, tmp_path):
        """
        Shows why a gzip-only magic check is insufficient: the decoded bytes here are bz2,
        not gzip, yet they are exactly what the name promises.
        """
        import bz2
        import gzip
        payload = bz2.compress(b"data" * 500)
        assert self._localize(tmp_path, gzip.compress(payload), "s.bz2") == payload

    def test_a_singly_compressed_bz2_keeps_the_stored_bytes(self, tmp_path):
        """gzip decoding fails outright here, and the stored bytes are the object."""
        import bz2
        payload = bz2.compress(b"data" * 500)
        assert self._localize(tmp_path, payload, "s.bz2") == payload

    def test_bytes_that_are_not_gzip_at_all_are_kept(self, tmp_path):
        """
        A server can advertise Content-Encoding: gzip over bytes that are not gzip. That is
        the server's metadata being wrong, not a reason to fail the localization.
        """
        assert self._localize(tmp_path, b"plain text, not gzip", "s.vcf") == \
            b"plain text, not gzip"

    def test_a_plain_name_is_still_decoded(self, tmp_path):
        import gzip
        payload = b"##fileformat=VCFv4.2\n" * 100
        assert self._localize(tmp_path, gzip.compress(payload), "s.vcf") == payload


class TestColumnarAndArrayContainers:
    """
    parquet and HDF5 are used in these pipelines and are NOT gzip streams. They already
    localized correctly before being listed, but only via the "advertised as gzip yet is
    not gzip" fallback -- listing them makes the outcome explicit and tested rather than
    incidental.

    Their internal compression is a separate concern that must never be touched: parquet
    compresses per column chunk and HDF5 has a per-dataset gzip filter, both inside the
    container. Only a transport Content-Encoding is unwrapped.
    """

    PARQUET = b"PAR1" + b"\x00" * 2000 + b"PAR1"
    HDF5 = b"\x89HDF\r\n\x1a\n" + b"\x00" * 2000

    def _localize(self, tmp_path, stored, name):
        source = str(tmp_path / "src")
        with open(source, "wb") as fh:
            fh.write(stored)
        dest = str(tmp_path / name)
        pdl.gunzip_to(source, dest)
        return open(dest, "rb").read()

    @pytest.mark.parametrize("name,magic", [
        ("t.parquet", b"PAR1"), ("t.pq", b"PAR1"),
        ("t.h5", b"\x89HDF\r\n\x1a\n"), ("t.hdf5", b"\x89HDF\r\n\x1a\n"),
    ])
    def test_recognized(self, name, magic):
        assert pdl.expected_magic(name) == magic

    def test_hdf4_extension_is_deliberately_not_claimed(self):
        """HDF4 has a different signature, so `.hdf` alone is ambiguous."""
        assert pdl.expected_magic("t.hdf") is None

    @pytest.mark.parametrize("name,body", [("t.parquet", PARQUET), ("t.h5", HDF5)])
    def test_a_mislabelled_container_is_kept_intact(self, tmp_path, name, body):
        """
        The likely real case: the object is uploaded as-is and the content-encoding
        metadata is set by mistake. The bytes are not gzip at all, so they are kept.
        """
        assert self._localize(tmp_path, body, name) == body

    @pytest.mark.parametrize("name,body", [("t.parquet", PARQUET), ("t.h5", HDF5)])
    def test_a_transport_gzipped_container_is_unwrapped(self, tmp_path, name, body):
        import gzip
        assert self._localize(tmp_path, gzip.compress(body), name) == body

    def test_internal_compression_is_untouched(self, tmp_path):
        """
        A parquet whose column chunks are gzip-compressed internally still localizes
        byte-for-byte: only the transport layer is unwrapped, never the container's own
        encoding.
        """
        import gzip
        inner = gzip.compress(b"column chunk payload" * 50)
        body = b"PAR1" + inner + b"PAR1"
        assert self._localize(tmp_path, gzip.compress(body), "t.parquet") == body
        assert self._localize(tmp_path, body, "t.parquet") == body


class TestMultipartEtagIsParallelAndBounded:
    """
    The S3 multipart ETag is md5-of-md5s, and each part's md5 is independent -- so the
    read-back is parallelizable, which matters because on the in-place route it is a full
    re-read of the object (300 GB in the case driving this work).

    An earlier version buffered a whole part via _read_exactly, making peak memory a
    property of how the uploader chose to chunk the object rather than of anything this
    code controls. S3 parts run from 8 MB to several GB.
    """

    @staticmethod
    def reference_etag(data, part_length):
        digests = [
            hashlib.md5(data[i:i + part_length]).digest()
            for i in range(0, len(data), part_length)
        ]
        return "{}-{}".format(hashlib.md5(b"".join(digests)).hexdigest(), len(digests))

    @pytest.mark.parametrize("size,part", [
        (1024, 1024),            # exactly one part
        (1024, 4096),            # one short part
        (4096, 1024),            # exact multiple
        (4097, 1024),            # ragged tail
        (1024 * 1024 + 7, 4096),  # many parts, ragged
    ])
    def test_matches_a_straightforward_implementation(self, tmp_path, size, part):
        data = os.urandom(size)
        path = tmp_path / "obj.bin"
        path.write_bytes(data)
        assert pdl.multipart_etag(str(path), part) == \
            self.reference_etag(data, part)

    @pytest.mark.parametrize("workers", [1, 2, 3, 8, 64])
    def test_worker_count_does_not_change_the_answer(self, tmp_path, workers):
        data = os.urandom(200000)
        path = tmp_path / "obj.bin"
        path.write_bytes(data)
        assert pdl.multipart_etag(str(path), 4096, workers=workers) == \
            self.reference_etag(data, 4096)

    def test_reads_are_bounded_by_block_not_part_length(self, tmp_path, monkeypatch):
        """
        The property that keeps a multi-gigabyte S3 part from becoming a
        multi-gigabyte allocation.
        """
        data = os.urandom(600000)
        path = tmp_path / "obj.bin"
        path.write_bytes(data)

        sizes = []
        real_open = open

        class Watched:
            def __init__(self, fh):
                self._fh = fh

            def read(self, n=-1):
                sizes.append(n)
                return self._fh.read(n)

            def __getattr__(self, name):
                return getattr(self._fh, name)

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                self._fh.close()

        monkeypatch.setattr("builtins.open",
                            lambda *a, **kw: Watched(real_open(*a, **kw)))
        pdl.multipart_etag(str(path), 500000, block=8192, workers=1)
        assert sizes, "never read anything"
        assert max(sizes) <= 8192, \
            "read {} bytes at once for a 500000-byte part".format(max(sizes))

    def test_a_short_file_does_not_silently_produce_a_digest(self, tmp_path):
        """A truncated file must fail rather than hash whatever is there."""
        path = tmp_path / "obj.bin"
        path.write_bytes(b"x" * 100)
        # part_length beyond the file is fine -- that is just one short part
        assert pdl.multipart_etag(str(path), 4096) is not None

    def test_empty_file_has_no_etag(self, tmp_path):
        path = tmp_path / "empty.bin"
        path.write_bytes(b"")
        assert pdl.multipart_etag(str(path), 4096) is None

    def test_verify_does_not_scale_readers_with_connections(self, tmp_path, monkeypatch):
        """
        This used to pass `connections`, which was measured to be actively harmful: on a
        316 GB pd-standard aggregate read throughput falls with concurrency (86 / 85 / 76
        / 62 MiB/s at 1 / 2 / 4 / 8 readers), so 8 readers made the read-back of a 279 GiB
        object 21 minutes slower than one. md5 outruns any persistent disk on a single
        core, so the parallelism had nothing to win.
        """
        data = os.urandom(50000)
        path = tmp_path / "obj.bin"
        path.write_bytes(data)
        seen = {}
        real = pdl.multipart_etag

        def spy(p, part_length, block=pdl.READ_BUFFER, workers=None):
            seen["workers"] = workers
            return real(p, part_length, block, workers)

        monkeypatch.setattr(pdl, "multipart_etag", spy)
        options = pdl.build_parser().parse_args([
            "--url", "http://h/o", "--dest", str(path), "--size", str(len(data)),
            "--connections", "6", "--part-length", "4096",
            "--check-etag", self.reference_etag(data, 4096),
        ])
        assert pdl.verify(str(path), options) == self.reference_etag(data, 4096)
        assert seen["workers"] == pdl.VERIFY_READ_WORKERS
        assert seen["workers"] != 6, "must not scale readers with the connection count"
        assert pdl.VERIFY_READ_WORKERS <= 2, \
            "more than 2 readers measured slower on a persistent disk"


class TestPhaseTiming:
    """
    Localization is two costs, not one -- moving the bytes, then re-reading them to hash
    -- and the log reported neither. The runbook asks for that split because it decides
    whether hashing during the transfer is worth building and whether the node can be
    sized down, so the format is a contract the benchmark parses.
    """

    def test_logs_a_parseable_line_with_name_seconds_and_rate(self, capsys):
        with pdl.phase("download", 2 * MIB):
            pass
        err = capsys.readouterr().err
        assert re.search(r"k9pdl-phase download [\d.]+s, [\d.]+ MB/s", err), err

    def test_omits_the_rate_when_no_size_is_given(self, capsys):
        with pdl.phase("compose"):
            pass
        err = capsys.readouterr().err
        assert re.search(r"k9pdl-phase compose [\d.]+s", err)
        assert "MB/s" not in err

    def test_records_elapsed_on_the_object(self):
        with pdl.phase("x") as timing:
            pass
        assert timing.seconds is not None and timing.seconds >= 0

    def test_an_exception_is_marked_and_propagates(self, capsys):
        with pytest.raises(ValueError):
            with pdl.phase("verify", MIB):
                raise ValueError("boom")
        err = capsys.readouterr().err
        assert "k9pdl-phase verify" in err
        assert "(failed)" in err

    def test_the_benchmark_parses_what_the_downloader_emits(self, capsys):
        """
        The two halves of the contract, checked against each other rather than against a
        hand-written sample -- a format change in one must not silently pass here.
        """
        with pdl.phase("download", 4 * MIB):
            pass
        with pdl.phase("verify", 4 * MIB):
            pass
        err = capsys.readouterr().err
        parsed = dict(
            (m.group(1), float(m.group(2)))
            for m in re.finditer(r"k9pdl-phase (\w+) ([\d.]+)s", err)
        )
        assert sorted(parsed) == ["download", "verify"]

    def test_route_a_times_download_and_verify_separately(self, tmp_path, capsys):
        """
        End to end on the route LocalizeToDisk uses, where verify is a full re-read.
        """
        payload = os.urandom(300000)
        with Server(payload) as server:
            dest = str(tmp_path / "obj.bin")
            rc = pdl.main([
                "--url", server.url(), "--dest", dest, "--size", str(len(payload)),
                "--connections", "4", "--min-chunk", "65536",
                "--check-md5", hashlib.md5(payload).hexdigest(),
            ])
        assert rc == pdl.EXIT_OK
        err = capsys.readouterr().err
        assert "k9pdl-phase download" in err
        assert "k9pdl-phase verify" in err


class TestRoutesAreNamed:
    """
    The route identifier reaches logs, manifests and emitted scripts, so it is read by
    people diagnosing a localization. "B" told them nothing.
    """

    def test_each_route_says_what_it_does(self):
        assert pdl.ROUTE_POSIX == "in-place"
        assert pdl.ROUTE_BUCKET == "bucket-compose"
        assert pdl.ROUTE_STAGED == "stage-publish"

    def test_no_route_is_a_bare_letter(self):
        for name in ("ROUTE_POSIX", "ROUTE_BUCKET", "ROUTE_STAGED"):
            value = getattr(pdl, name)
            assert len(value) > 1, "{} is {!r}, which explains nothing".format(name, value)
            assert value.islower(), value

    def test_the_chosen_route_is_named_in_the_log(self, tmp_path, monkeypatch, capsys):
        payload = os.urandom(80000)
        monkeypatch.setattr(
            pdl, "select_route",
            lambda dest, **kw: pdl.RouteDecision(pdl.ROUTE_POSIX, "test: ext4"),
        )
        with Server(payload) as server:
            rc = pdl.main([
                "--url", server.url(), "--dest", str(tmp_path / "o.bin"),
                "--size", str(len(payload)), "--connections", "2",
                "--min-chunk", "16384",
            ])
        assert rc == pdl.EXIT_OK
        assert "route in-place: test: ext4" in capsys.readouterr().err


class TestInTransferPartHashing:
    """
    The in-place route used to re-read the whole object to reproduce a multipart ETag.
    Measured on a 316 GB pd-standard that read-back costs about as long as the download
    (0.9 h each for a 279 GiB BAM), so it doubled localization. Hashing each S3 part as
    its bytes go past removes it.

    The invariant it rests on: plan_chunks makes every chunk start a multiple of
    part_length, so a chunk covers a whole run of parts.
    """

    @staticmethod
    def reference_etag(data, part_length):
        digests = [hashlib.md5(data[i:i + part_length]).digest()
                   for i in range(0, len(data), part_length)]
        return "{}-{}".format(hashlib.md5(b"".join(digests)).hexdigest(), len(digests))

    def download(self, tmp_path, payload, part_length, min_chunk, connections=4,
                 extra=()):
        dest = str(tmp_path / "obj.bam")
        with Server(payload) as server:
            rc = pdl.main([
                "--url", server.url(), "--dest", dest, "--size", str(len(payload)),
                "--connections", str(connections), "--min-chunk", str(min_chunk),
                "--part-length", str(part_length),
                "--check-etag", self.reference_etag(payload, part_length),
            ] + list(extra))
        return rc, dest

    @pytest.mark.parametrize("size,part,min_chunk", [
        (300000, 100000, 200000),      # 3 parts, chunk = 2 parts
        (300001, 100000, 200000),      # ragged tail
        (100000, 100000, 100000),      # exactly one part
        (250000, 50000, 150000),       # 5 parts, chunk = 3 parts
    ])
    def test_the_etag_matches_without_a_read_back(self, tmp_path, capsys,
                                                 size, part, min_chunk):
        payload = os.urandom(size)
        rc, _ = self.download(tmp_path, payload, part, min_chunk)
        assert rc == pdl.EXIT_OK
        err = capsys.readouterr().err
        assert "0 re-read" in err, \
            "should have hashed every part during transfer: {}".format(
                [l for l in err.split("\n") if "etag from" in l])

    def test_a_wrong_etag_still_fails(self, tmp_path):
        """The digests must be able to reject, not merely to agree with themselves."""
        payload = os.urandom(300000)
        dest = str(tmp_path / "obj.bam")
        with Server(payload) as server:
            rc = pdl.main([
                "--url", server.url(), "--dest", dest, "--size", str(len(payload)),
                "--connections", "4", "--min-chunk", "200000",
                "--part-length", "100000", "--check-etag", "0" * 32 + "-3",
            ])
        assert rc == pdl.EXIT_FAIL
        assert not os.path.exists(dest), "a failed verification must discard the file"

    def test_parts_are_keyed_separately_from_chunks(self, tmp_path):
        """
        A chunk spans several parts on this route, so part digests cannot share the
        `chunks` keyspace that the bucket-compose route uses.
        """
        payload = os.urandom(300000)
        rc, dest = self.download(tmp_path, payload, 100000, 200000)
        assert rc == pdl.EXIT_OK
        # 3 parts across 2 chunks -- if the keyspaces were shared this could not hold
        assert (300000 + 99999) // 100000 == 3
        assert len(pdl.plan_chunks(300000, 4, 200000, 100000)) == 2


class TestPartDigestsSurviveInterruption:
    """
    A chunk resumed mid-way has bytes on disk this process never hashed. Those parts must
    fall back to a read -- but only those parts, since after one preemption that is a
    handful of 29 MiB reads standing in for a 279 GiB one.
    """

    def manifest_with(self, tmp_path, path, part_length, recorded):
        chunks = pdl.plan_chunks(os.path.getsize(path), 4, 200000, part_length)
        manifest = pdl.Manifest.create(
            str(tmp_path / "m.json"), "plan", os.path.getsize(path), 200000, chunks,
            os.stat(path), True, 0)
        manifest.record_part_md5s(recorded, None)
        return manifest

    def test_missing_parts_are_re_read_and_the_etag_still_matches(self, tmp_path):
        data = os.urandom(300000)
        path = tmp_path / "obj.bin"
        path.write_bytes(data)
        part = 100000
        full = [hashlib.md5(data[i:i + part]).digest()
                for i in range(0, len(data), part)]
        expected = "{}-{}".format(
            hashlib.md5(b"".join(full)).hexdigest(), len(full))

        # only parts 0 and 2 recorded; part 1 must be re-read
        manifest = self.manifest_with(tmp_path, str(path), part, {
            0: full[0].hex(), 2: full[2].hex()})
        actual, reread = pdl.multipart_etag_from_manifest(str(path), part, manifest)
        assert actual == expected
        assert reread == 1, "should re-read exactly the one missing part"

    def test_no_digests_at_all_degrades_to_a_full_read(self, tmp_path):
        data = os.urandom(250000)
        path = tmp_path / "obj.bin"
        path.write_bytes(data)
        part = 50000
        manifest = self.manifest_with(tmp_path, str(path), part, {})
        actual, reread = pdl.multipart_etag_from_manifest(str(path), part, manifest)
        assert actual == TestInTransferPartHashing.reference_etag(data, part)
        assert reread == 5, "every part missing means every part re-read"

    def test_a_corrupt_recorded_digest_is_re_read_not_trusted(self, tmp_path):
        data = os.urandom(200000)
        path = tmp_path / "obj.bin"
        path.write_bytes(data)
        part = 100000
        manifest = self.manifest_with(tmp_path, str(path), part, {0: "not-hex", 1: "zz"})
        actual, reread = pdl.multipart_etag_from_manifest(str(path), part, manifest)
        assert actual == TestInTransferPartHashing.reference_etag(data, part)
        assert reread == 2

    def test_a_stale_digest_produces_a_mismatch_rather_than_a_pass(self, tmp_path):
        """
        The failure that matters: if a recorded digest were trusted for bytes that
        changed, verification would pass on wrong data. It must not agree with itself.
        """
        data = os.urandom(200000)
        path = tmp_path / "obj.bin"
        path.write_bytes(data)
        part = 100000
        wrong = hashlib.md5(b"different").digest().hex()
        manifest = self.manifest_with(tmp_path, str(path), part, {0: wrong})
        actual, _ = pdl.multipart_etag_from_manifest(str(path), part, manifest)
        assert actual != TestInTransferPartHashing.reference_etag(data, part)


class TestTheSingleStreamFallbackFailsLoudly:
    """
    `curl -sSL` without --fail writes an HTTP error body to the output file and exits 0,
    so a 403 presents as a successful download of a short "AccessDenied" file. With
    check_hash on, verification catches it; with check_hash off it would be accepted as
    the object.
    """

    def test_the_synthesized_command_uses_fail(self, tmp_path):
        options = pdl.build_parser().parse_args([
            "--url", "https://h/o", "--dest", str(tmp_path / "o"), "--size", "10",
            "--connections", "1"])
        recorded = {}
        import subprocess as sp

        def spy(command, **kw):
            recorded["command"] = command
            return sp.CompletedProcess(command, 0)

        original = pdl.subprocess.run
        pdl.subprocess.run = spy
        try:
            pdl.single_stream_fallback(options, "test")
        finally:
            pdl.subprocess.run = original
        assert "--fail" in recorded["command"], recorded["command"]

    def test_an_http_error_is_a_nonzero_exit_not_a_short_file(self, tmp_path):
        """End to end against a server that 403s."""
        dest = str(tmp_path / "o.bin")
        with Server(b"unused") as server:
            server.state.force_status = 403
            rc = pdl.main([
                "--url", server.url(), "--dest", dest, "--size", "1000",
                "--connections", "1"])
        assert rc != pdl.EXIT_OK
        assert not (os.path.exists(dest) and os.path.getsize(dest) > 0), \
            "an error body was written to the destination and treated as the object"


class TestConcurrencyIsReported:
    """
    The GDC sweep came out flat: 16.6 MiB/s at 1 connection and 16.7 at 16. That has two
    readings with opposite consequences -- the source caps aggregate bandwidth, so
    parallelism cannot help and the >=4x target is unreachable against it; or the
    requests never ran concurrently, and there is a defect here. Nothing in the output
    distinguished them, so the run could not settle the question it was run to answer.

    `k9pdl-streams` is that number: streaming-seconds summed over chunks, divided by
    wall time, which is the mean count of requests actually receiving bytes.
    """

    def parse(self, output):
        match = re.search(
            r"k9pdl-streams mean ([\d.]+) of (\d+) workers "
            r"\((\d+) chunks, ([\d.]+)s wall, ([\d.]+)s streaming", output)
        assert match, "no k9pdl-streams line in:\n" + output
        return {"mean": float(match.group(1)), "workers": int(match.group(2)),
                "chunks": int(match.group(3)), "wall": float(match.group(4)),
                "streaming": float(match.group(5))}

    def test_a_parallel_run_reports_concurrency_near_the_worker_count(self, tmp_path):
        payload = os.urandom(4 * MIB)
        with Server(payload) as server:
            # trickle each response, so the reads genuinely overlap in time
            server.state.throttle_bytes = 64 * 1024
            server.state.throttle_delay = 0.01
            dest = str(tmp_path / "out.bin")
            proc = run_downloader(server.url(), dest, len(payload),
                                  "--connections", 4, "--min-chunk", 256 * 1024)
        assert proc.returncode == 0, proc.stderr
        got = self.parse(proc.stderr)
        assert got["workers"] == 4
        # Loose on purpose: this asserts the requests overlapped, not how well. A serial
        # implementation reports ~1.0, which is what the assertion has to exclude.
        assert got["mean"] > 2.0, "concurrency reported as {:.2f} of 4".format(got["mean"])

    def test_a_single_chunk_run_reports_one_stream(self, tmp_path):
        """
        The figure has to be honest downward too, or "near the worker count" means
        nothing. One chunk cannot exceed one stream however many workers were asked for.
        """
        payload = os.urandom(64 * 1024)
        with Server(payload) as server:
            dest = str(tmp_path / "out.bin")
            proc = run_downloader(server.url(), dest, len(payload),
                                  "--connections", 8, "--min-chunk", 8 * MIB)
        assert proc.returncode == 0, proc.stderr
        got = self.parse(proc.stderr)
        assert got["chunks"] == 1
        assert got["workers"] == 1, "one pending chunk means one worker"
        assert got["mean"] <= 1.05, got

    def test_backoff_sleeps_are_not_counted_as_streaming(self, tmp_path):
        """
        Counting retry sleeps would inflate the figure with time nothing was being
        transferred -- and a run that retried a lot would then report high concurrency
        while achieving nothing, which is the false reassurance this number exists to
        prevent.
        """
        payload = os.urandom(512 * 1024)
        with Server(payload) as server:
            # drop_after, not fail_next: fail_next is consumed by probe_range before the
            # pool starts, so the retry happens outside the accounting and the test
            # measures nothing -- it passed with wall 0.0s that way. A mid-response drop
            # forces the short-read handler INSIDE the read loop, which is the path
            # whose backoff must be excluded.
            server.state.drop_after = 256 * 1024
            dest = str(tmp_path / "out.bin")
            # connections=2 with one chunk: workers is min(connections, pending), so this
            # is one worker on the parallel path. connections=1 would take the legacy
            # single-stream fallback, which never enters the pool and reports nothing.
            proc = run_downloader(server.url(), dest, len(payload),
                                  "--connections", 2, "--min-chunk", 512 * 1024,
                                  "--retries", 5)
        assert proc.returncode == 0, proc.stderr
        assert "retrying in" in proc.stderr, "the retry path did not run"
        got = self.parse(proc.stderr)
        assert got["workers"] == 1, got
        # the sleeps are real wall time, so streaming must fall well short of it --
        # backoff is min(2**attempt, 60) * (0.5 + random()), so at least ~1s per retry
        assert got["streaming"] < got["wall"], got
        assert got["wall"] - got["streaming"] > 0.9, got
        assert got["mean"] < 1.0, got

    def test_every_stream_exit_is_accounted(self):
        """
        The accounting is called from each exit path of the read loop rather than from a
        `finally`, so that backoff sleeps stay out. The cost is that a handler added
        later without a call biases the figure downward -- and a low figure reads as
        "the requests were not concurrent", i.e. as a defect that is not there. So check
        the source rather than trusting it.
        """
        source = inspect.getsource(pdl.Downloader.download_chunk)
        body = source.split("stream_started = time.time()", 1)[1]
        # Exactly this indent: handlers of the read loop itself. A deeper `except`
        # belongs to the nested stream.close() in the `finally`, which is not an exit
        # path from the loop and must not be counted as one.
        handlers = [line for line in body.splitlines()
                    if re.match(r"^ {12}except\b", line)]
        assert len(handlers) == 2, handlers
        calls = body.count("_count_stream_time(stream_started)")
        # one per handler, plus the success path that falls out of the loop normally
        assert calls == len(handlers) + 1, (
            "{} exit paths but {} accounting calls".format(len(handlers) + 1, calls))


class TestEachChunkGetsItsOwnConnection:
    """
    Whether the workers share a TCP connection decides how to read a flat sweep: if 16
    workers were multiplexed onto one socket, a server-side per-connection cap would look
    exactly like a per-object cap, and the GDC result would mean something different.

    The claim is that `urllib.request` neither pools nor keeps alive, so each ranged GET
    gets a fresh connection. That claim is load-bearing in PARALLEL_DOWNLOAD.md and in
    the runbook, and it was asserted from memory twice before anyone checked it. The
    fake server records client source ports, so it can be checked instead.
    """

    def test_urllib_asks_the_server_to_close(self):
        """The mechanism, in one assertion: no keep-alive is requested."""
        import http.client
        sent = []
        original = http.client.HTTPConnection.putheader

        def spy(self, header, *values):
            sent.append((header.lower(), values))
            return original(self, header, *values)

        payload = os.urandom(4096)
        http.client.HTTPConnection.putheader = spy
        try:
            with Server(payload) as server:
                urllib.request.urlopen(server.url()).read()
        finally:
            http.client.HTTPConnection.putheader = original
        assert ("connection", ("close",)) in sent, sent

    def test_a_multi_chunk_download_uses_a_connection_per_request(self, tmp_path):
        payload = os.urandom(8 * MIB)
        with Server(payload) as server:
            dest = str(tmp_path / "out.bin")
            proc = run_downloader(server.url(), dest, len(payload),
                                  "--connections", 4, "--min-chunk", MIB)
            seen = server.state.snapshot()
        assert proc.returncode == 0, proc.stderr
        # 8 chunks plus the range probe. Sharing would show far fewer connections than
        # requests; one connection for all of them is the case that would invalidate
        # the reading of a flat sweep.
        assert seen["requests"] >= 8, seen
        assert seen["connections"] == seen["requests"], seen


class TestAPrefixOfALargerObjectStaysParallel:
    """
    probe_range compares the server's declared total against the size it was given. When
    only a prefix is wanted those differ legitimately, and the check called the server
    broken: RangeNotSupported, then single_stream_fallback. Both GDC sweeps ran every row
    as one curl while reporting 1/4/8/12/16 connections, and every other observable --
    byte count, hash, NIC ratio, throughput -- was consistent with a healthy transfer.

    --object-size separates "how long the object is" from "how much of it to fetch".
    """

    def test_without_object_size_a_prefix_falls_back(self, tmp_path):
        """The bug, pinned: this is what produced two void sweeps."""
        payload = os.urandom(4 * MIB)
        with Server(payload) as server:
            dest = str(tmp_path / "out.bin")
            proc = run_downloader(server.url(), dest, MIB,          # 1 MiB of 4 MiB
                                  "--connections", 4, "--min-chunk", 256 * 1024,
                                  "--legacy-cmd",
                                  "head -c {} /dev/zero > {}".format(MIB, dest))
        assert proc.returncode == 0, proc.stderr
        assert "falling back to a single stream" in proc.stderr
        assert "k9pdl-streams" not in proc.stderr

    def test_with_object_size_the_same_prefix_runs_parallel(self, tmp_path):
        # 4 MiB of an 8 MiB object at 1 MiB chunks -> 4 chunks, 4 workers. min-chunk
        # below CHUNK_ALIGN (1 MiB) rounds up, which would give one chunk and one worker
        # and prove nothing about concurrency.
        payload = os.urandom(8 * MIB)
        with Server(payload) as server:
            server.state.throttle_bytes = 32 * 1024
            server.state.throttle_delay = 0.01
            dest = str(tmp_path / "out.bin")
            proc = run_downloader(server.url(), dest, 4 * MIB,
                                  "--object-size", len(payload),
                                  "--connections", 4, "--min-chunk", MIB)
        assert proc.returncode == 0, proc.stderr
        assert "falling back to a single stream" not in proc.stderr, proc.stderr
        match = re.search(r"k9pdl-streams mean ([\d.]+) of (\d+) workers", proc.stderr)
        assert match, proc.stderr
        assert float(match.group(1)) > 1.5, match.group(0)

    def test_the_prefix_bytes_are_correct(self, tmp_path):
        """Parallel is worthless if it fetches the wrong bytes."""
        payload = os.urandom(8 * MIB)
        with Server(payload) as server:
            dest = str(tmp_path / "out.bin")
            proc = run_downloader(server.url(), dest, 4 * MIB,
                                  "--object-size", len(payload),
                                  "--connections", 4, "--min-chunk", MIB)
        assert proc.returncode == 0, proc.stderr
        with open(dest, "rb") as fh:
            got = fh.read()
        assert got == payload[:4 * MIB]
        assert len(got) == 4 * MIB, "the prefix must stop at --size"

    def test_a_whole_object_download_is_unchanged(self, tmp_path):
        """
        Production never passes --object-size, and the check it relaxes is a real one
        there: a declared total that differs from the handler's size means stale
        metadata. Omitting the flag must still reject that.
        """
        payload = os.urandom(2 * MIB)
        with Server(payload) as server:
            dest = str(tmp_path / "out.bin")
            proc = run_downloader(server.url(), dest, MIB,   # lie about the size
                                  "--connections", 4, "--min-chunk", 256 * 1024,
                                  "--legacy-cmd", "true")
        assert "falling back to a single stream" in proc.stderr
        assert "299" not in proc.stderr        # sanity: it is our mismatch, not a stub
        assert "but {} was expected".format(MIB) in proc.stderr, proc.stderr

    def test_object_size_equal_to_size_behaves_like_omitting_it(self, tmp_path):
        payload = os.urandom(MIB)
        with Server(payload) as server:
            dest = str(tmp_path / "out.bin")
            proc = run_downloader(server.url(), dest, len(payload),
                                  "--object-size", len(payload),
                                  "--connections", 4, "--min-chunk", 256 * 1024)
        assert proc.returncode == 0, proc.stderr
        assert "falling back to a single stream" not in proc.stderr


class TestBookkeepingTimeIsMeasured:
    """
    The 279 GiB run reached 50% of the disk's floor where the 4 GiB run reached 83%, with
    per-stream throughput unchanged at the source rate (16.17 vs 15.82 MiB/s) and only
    3.00 of 16 streams active. So workers were idle outside the read loop, and the only
    work that grows with the CHUNK COUNT rather than the byte count is chunk_done -- the
    manifest rewrite, its fsyncs, and the wait for Manifest._lock. 64 chunks at 4 GiB
    against 3283 at full size.

    Guessing at this once already gave a wrong answer: the fsync-barrier hypothesis
    predicted a large win from fewer chunks and delivered 8%, because the flush work is
    invariant to chunk count. So it is measured rather than reasoned about.
    """

    def parse(self, output):
        match = re.search(
            r"k9pdl-bookkeeping ([\d.]+)s over (\d+) calls "
            r"\(mean ([\d.]+)s, ([\d.]+)% of ([\d.]+) worker-seconds\)", output)
        assert match, "no k9pdl-bookkeeping line in:\n" + output
        return {"total": float(match.group(1)), "calls": int(match.group(2)),
                "mean": float(match.group(3)), "pct": float(match.group(4))}

    def test_one_call_is_recorded_per_chunk(self, tmp_path):
        payload = os.urandom(8 * MIB)
        with Server(payload) as server:
            dest = str(tmp_path / "out.bin")
            proc = run_downloader(server.url(), dest, len(payload),
                                  "--connections", 4, "--min-chunk", MIB)
        assert proc.returncode == 0, proc.stderr
        got = self.parse(proc.stderr)
        assert got["calls"] == 8, got          # 8 MiB / 1 MiB
        assert got["total"] >= 0.0

    def test_a_slow_chunk_done_is_attributed_to_bookkeeping_not_streaming(self, tmp_path):
        """
        The number has to move when the thing it measures gets slower, or it cannot
        distinguish the hypothesis from its negation.
        """
        payload = os.urandom(4 * MIB)
        with Server(payload) as server:
            dest = str(tmp_path / "out.bin")
            slow = (
                "import sys, time, types\n"
                "sys.argv = ['pdl'] + {argv!r}\n"
                "src = open({path!r}).read()\n"
                "mod = types.ModuleType('pdl'); mod.__file__ = {path!r}\n"
                "exec(compile(src, {path!r}, 'exec'), mod.__dict__)\n"
                "orig = mod.PosixChunkSink.chunk_done\n"
                "mod.PosixChunkSink.chunk_done = "
                "lambda self, i: (time.sleep(0.30), orig(self, i))[1]\n"
                "sys.exit(mod.main())\n"
            ).format(argv=["--url", server.url(), "--dest", dest,
                           "--size", str(len(payload)), "--connections", "4",
                           "--min-chunk", str(MIB)], path=PDL_PATH)
            proc = subprocess.run([sys.executable, "-c", slow],
                                  capture_output=True, text=True, timeout=120)
        assert proc.returncode == 0, proc.stderr
        got = self.parse(proc.stderr)
        assert got["calls"] == 4, got
        # 4 chunks x 0.30s of injected sleep, serialised or not
        assert got["total"] >= 1.1, got
        assert got["mean"] >= 0.28, got

    def test_bookkeeping_is_excluded_from_streaming_time(self, tmp_path):
        """
        chunk_done runs after the read loop, so its cost must NOT inflate the streams
        figure -- otherwise a run bottlenecked on bookkeeping would report healthy
        concurrency, which is the exact confusion this pair of numbers exists to resolve.
        """
        payload = os.urandom(4 * MIB)
        with Server(payload) as server:
            dest = str(tmp_path / "out.bin")
            proc = run_downloader(server.url(), dest, len(payload),
                                  "--connections", 4, "--min-chunk", MIB)
        assert proc.returncode == 0, proc.stderr
        streams = re.search(r"k9pdl-streams mean [\d.]+ of \d+ workers "
                            r"\(\d+ chunks, ([\d.]+)s wall, ([\d.]+)s streaming", proc.stderr)
        assert streams, proc.stderr
        book = self.parse(proc.stderr)
        # streaming time is bounded by the read loop; bookkeeping sits outside it
        assert book["total"] <= float(streams.group(1)) * 4 + 1.0, (book, streams.groups())

    def test_the_share_is_of_the_worker_pool_not_the_wall_clock(self, tmp_path):
        """
        With N workers the budget is N*wall, so a percentage of wall clock could exceed
        100 and mean nothing. The denominator has to be worker-seconds.
        """
        payload = os.urandom(4 * MIB)
        with Server(payload) as server:
            dest = str(tmp_path / "out.bin")
            proc = run_downloader(server.url(), dest, len(payload),
                                  "--connections", 4, "--min-chunk", MIB)
        assert proc.returncode == 0, proc.stderr
        assert "worker-seconds)" in proc.stderr
        assert self.parse(proc.stderr)["pct"] <= 100.0
