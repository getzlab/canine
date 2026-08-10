"""
Tests for canine/localization/parallel_download.py -- chunk planning, the manifest,
the filesystem capability probes, verification, and end-to-end downloads against a
local HTTP server.

Preemption-specific behavior lives in test_parallel_download_resume.py.
"""

import hashlib
import json
import os
import stat
import subprocess
import sys

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
    The safety property that matters while routes B and C are unimplemented: a non-POSIX
    destination must never see a full-size ftruncate or an out-of-order pwrite. Those
    are precisely the operations that make a FUSE object store materialize gigabytes of
    zeros and re-upload the whole object per write.
    """

    def _run_with_route(self, tmp_path, monkeypatch, route, gs_url=None):
        calls = []
        monkeypatch.setattr(pdl.os, "ftruncate",
                            lambda *a: calls.append(("ftruncate",) + a))
        monkeypatch.setattr(pdl.os, "pwrite",
                            lambda *a: calls.append(("pwrite",) + a))
        monkeypatch.setattr(
            pdl, "select_route",
            lambda dest, **kw: pdl.RouteDecision(
                route, "test: forced route", gs_url=gs_url),
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

    def test_no_ftruncate_or_pwrite_on_the_bucket_route(self, tmp_path, monkeypatch):
        """
        Route B relays bytes straight from the source into resumable upload sessions, so
        it must never touch a local file. Here it fails (nothing is listening), which is
        fine -- what matters is that it failed without writing in place.
        """
        rc, calls, _ = self._run_with_route(
            tmp_path, monkeypatch, pdl.ROUTE_BUCKET, gs_url="gs://b/o"
        )
        assert rc != 0
        assert calls == [], "wrote in place to a bucket destination: {}".format(calls)

    def test_staged_route_degrades_to_the_legacy_command(self, tmp_path, monkeypatch):
        """
        Route C is not implemented, so it takes the single sequential stream -- the
        documented degradation, and for a FUSE object store the only access pattern
        that reaches its streaming-write path.
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
