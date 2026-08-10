#!/usr/bin/env python3
"""
Parallel chunked downloader for canine localization.

Downloads one object with N simultaneous ranged GETs of even-sized chunks, written
in place into a sparse destination file, and reassembled byte-identically without a
merge pass.

Why in place rather than part-files-plus-merge: the localization disk is sized as the
sum of input sizes with only a 5% margin, so a design whose peak usage is ~2x the
object simply would not fit.

RESUMABILITY IS THE HARD REQUIREMENT. Workers are preemptible, so this process may
be killed at any byte, at any instant, and re-run later on a different VM. Nothing
about correctness may depend on catching a signal, running a cleanup path, or
flushing at shutdown. The design instead derives durable progress from the
destination file itself, exactly as `curl -C -` does:

  * the file is created sparse with ftruncate (NOT posix_fallocate, whose unwritten
    extents are indistinguishable from written data);
  * each chunk is written strictly sequentially within its own byte range, so every
    chunk has a single contiguous frontier;
  * on resume that frontier is recovered with lseek(SEEK_HOLE), which reports
    allocated extents, which under ext4 data=ordered implies journal-committed data.

Consequently there is no progress counter to go stale, no periodic checkpointing, and
no fsync in the hot path -- and a re-run refetches only the filesystem's uncommitted
tail rather than a whole checkpoint interval.

Two details that are easy to get wrong and are handled explicitly:

  * SEEK_HOLE is block-granular. Writing a partial block allocates the whole block, so
    the reported hole can be *past* the last byte actually written. Trusting it would
    skip bytes and silently corrupt the output, so the final allocated block is always
    discarded (see `chunk_frontier`).
  * On a filesystem without SEEK_HOLE support the kernel reports the entire file as
    data, which would make every chunk look complete. That is why support is probed at
    startup rather than assumed, with a checkpointing fallback for filesystems that
    lack it.

Completion is asserted ONLY by the `.k9pdl.done` marker. Because the file is created
at its full apparent size upfront, `stat` size means nothing about completeness.

Runnable standalone for debugging:
    parallel_download.py --url URL --dest PATH --size N [...]
and as a module:
    python3 -m canine.localization.parallel_download [...]

Stdlib only, so it works inside the worker image with no added dependencies. It must
not import canine.
"""

import argparse
import base64
import binascii
import errno
import hashlib
import json
import os
import random
import re
import shlex
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

SCHEMA_VERSION = 1

# Tuning defaults for an n1-standard-8 worker, which localization owns exclusively
# (LocalizeToDisk pins cpus-per-task=8 and passes --exclusive). A single TCP stream to
# S3/GDC realistically gets 50-200 MB/s against a ~2 GB/s egress cap, which is the
# whole reason for fanning out.
DEFAULT_CONNECTIONS = 8
MAX_CONNECTIONS = 16
DEFAULT_MIN_CHUNK = 64 * 1024 * 1024

# Chunk boundaries are aligned to this so that every chunk start is also block-aligned
# on any plausible filesystem. Without it, adjacent chunks share a partial block and
# the frontier arithmetic at chunk boundaries stops being sound.
CHUNK_ALIGN = 1024 * 1024

READ_BUFFER = 8 * 1024 * 1024

# How much is read before being written out. This is deliberately much smaller than a
# chunk and is a durability parameter, not a throughput one: HTTPResponse.read(n)
# blocks until it has all n bytes, so a large value means nothing reaches the disk
# until that much has arrived -- and a kill in the meantime discards all of it, which
# defeats both the frontier scheme and the checkpoint fallback. 1 MiB keeps the write
# granularity fine enough that a kill loses almost nothing.
READ_BLOCK = 1024 * 1024

# Used only on filesystems that fail the SEEK_HOLE probe. Deliberately small: it
# bounds lost work to 8 MiB per stream instead of the frontier scheme's uncommitted
# tail, at the cost of an fsync per interval.
FALLBACK_CHECKPOINT_INTERVAL = 8 * 1024 * 1024

# ...but the interval is useless if it exceeds the chunk size, because then no
# checkpoint is ever recorded before the chunk ends and a kill loses the whole chunk.
# Scaling by the chunk size guarantees several checkpoints per chunk whatever the
# layout; the cap keeps the fsync rate sane for the large chunks real transfers use.
MIN_CHECKPOINT_INTERVAL = 1024 * 1024
CHECKPOINTS_PER_CHUNK = 4


def checkpoint_interval_for(chunk_size):
    return max(
        MIN_CHECKPOINT_INTERVAL,
        min(FALLBACK_CHECKPOINT_INTERVAL, chunk_size // CHECKPOINTS_PER_CHUNK),
    )

# Assume the largest block size we might plausibly meet, rather than trying to query
# it: over-estimating only costs a few KiB of refetch per chunk, while
# under-estimating would corrupt the output.
ASSUMED_BLOCK_SIZE = 64 * 1024

DEFAULT_RETRIES = 5
DEFAULT_TIMEOUT = 60

# ENOSPC is retryable, not fatal: sparse ftruncate does not reserve blocks, so an
# undersized localization disk fails on a pwrite deep into the transfer -- and the
# disk-resize daemon makes that condition temporary.
ENOSPC_MAX_WAIT = 600

PROGRESS_INTERVAL = 5

# Exit codes are interpreted by canine's entrypoint: 5 means requeue-and-resume, 15
# means skip the job, and ANY other nonzero value is treated as do-not-retry. So a
# transient failure must never escape as an arbitrary nonzero code.
EXIT_OK = 0
EXIT_FAIL = 1
EXIT_REQUEUE = 5


# --------------------------------------------------------------------------------
# logging
# --------------------------------------------------------------------------------

_log_lock = threading.Lock()

_REDACT_QUERY_KEYS = re.compile(
    r"(?i)\b(signature|sig|x-goog-signature|x-amz-signature|x-amz-credential|"
    r"x-amz-security-token|token|access_token|awsaccesskeyid|goog-access-id)="
    r"[^&\s]*"
)


def redact(text):
    """
    Strip credentials from anything that might reach stderr. Signed URLs carry
    bearer-equivalent material in their query string, and localization logs are not
    treated as secret.
    """
    text = str(text)
    text = _REDACT_QUERY_KEYS.sub(lambda m: m.group(0).split("=")[0] + "=REDACTED", text)
    return text


def log(message):
    with _log_lock:
        sys.stderr.write("[k9pdl] {}\n".format(redact(message)))
        sys.stderr.flush()


# --------------------------------------------------------------------------------
# chunk planning
# --------------------------------------------------------------------------------

def align_up(value, alignment):
    return ((value + alignment - 1) // alignment) * alignment


def plan_chunks(size, connections, min_chunk, part_length=None):
    """
    Split `size` into even-sized chunks (with a possibly-shorter tail).

    `connections` is accepted but deliberately IGNORED for the layout, which depends
    only on `size`, `min_chunk` and `part_length`. This is a hard requirement, not a
    simplification: `plan_id` is derived from the chunk size, so if the layout moved
    with the connection count then a requeued task landing on a differently-configured
    node would compute a different `plan_id`, discard a perfectly good partial file,
    and re-download the object from scratch. `connections` governs only how many chunks
    are in flight at once (see `Downloader.run`).

    This diverges from the original design sketch, which had
    `n_chunks = clamp(ceil(size / min_chunk), 1, connections)` -- that formula makes the
    layout connection-dependent and contradicts the resumability requirement it appears
    alongside. Fixing the chunk size at `min_chunk` instead yields more, smaller chunks
    for a large object (a 50 GB object becomes 800 x 64 MiB rather than 8 x 6.4 GB),
    which is also better in its own right: finer resume granularity, and the worker pool
    load-balances over a queue instead of each thread owning one huge range. The extra
    per-request overhead is why `min_chunk` is 64 MiB to begin with.

    When `part_length` is given (an S3 multipart object whose ETag is an md5-of-md5s),
    boundaries snap to whole parts so the ETag can be computed incrementally during the
    download instead of by a second full read afterwards.
    """
    if size < 0:
        return []
    if size == 0:
        return [(0, 0)]

    min_chunk = max(1, int(min_chunk))

    if part_length:
        # every chunk must span whole parts, so snap up to a multiple of part_length
        chunk_size = max(part_length, align_up(min_chunk, part_length))
    else:
        chunk_size = align_up(min_chunk, CHUNK_ALIGN)

    chunk_size = max(1, min(chunk_size, size))

    chunks = []
    start = 0
    while start < size:
        end = min(start + chunk_size, size)
        chunks.append((start, end))
        start = end
    return chunks


def compute_plan_id(url, size, content_hash, chunk_size):
    """
    Identify a chunk layout for a specific object. A mismatch on resume means the URL,
    the object, or the layout changed, so any partial file must be discarded rather
    than reused.

    Only the URL *path* participates, not its query string: a signed URL is re-minted
    on every attempt with fresh credentials and expiry, so including the query would
    make every resume look like a different object.
    """
    parsed = urllib.parse.urlsplit(url)
    identity = "{}://{}{}".format(parsed.scheme, parsed.netloc, parsed.path)
    payload = "|".join([
        str(SCHEMA_VERSION),
        identity,
        str(size),
        str(content_hash or ""),
        str(chunk_size),
    ])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------------
# sidecar paths
# --------------------------------------------------------------------------------

def sidecar_paths(dest):
    """
    Sidecars live next to the working file, on the same filesystem, so they are
    attached, detached, preempted and resumed atomically with the data they describe.
    Dotfiles, so they stay out of glob- and find-based output patterns. There is direct
    precedent: HandleGSURL already drops .gcloud_tracker_dir/.gcloud_manifest beside
    its destination for the same purpose.
    """
    directory = os.path.dirname(os.path.abspath(dest))
    base = os.path.basename(dest)
    return (
        os.path.join(directory, ".{}.k9pdl.json".format(base)),
        os.path.join(directory, ".{}.k9pdl.done".format(base)),
    )


# --------------------------------------------------------------------------------
# filesystem capability probes
# --------------------------------------------------------------------------------

def probe_seek_hole(directory):
    """
    Verify that SEEK_HOLE reports holes accurately enough to resume from on this
    filesystem.

    This is not optional, and it is deliberately strict. Where SEEK_HOLE is
    unsupported the kernel reports the entire file as data, so every chunk would look
    complete on resume and the output would be silently truncated garbage.

    Two things this has to get right:

      * It must fsync before looking. Under delayed allocation an unflushed write has
        no extent yet, so the probe would see a hole, pass, and then be wrong about
        every real resume -- which happens only after a crash, i.e. always post-commit.
        APFS is a live example: it passes a no-fsync probe and then reports a
        1 MiB-written 8 MiB file as entirely data.
      * It must check that the hole begins *near* the written region, not merely that
        some hole exists somewhere. A filesystem that rounds the boundary up by
        megabytes is unusable here even though it technically supports SEEK_HOLE,
        because the frontier would overstate durable data and the resumed download
        would skip bytes.

    ext4 on the localization PD passes; NFSv4.2 passes; NFSv3 and APFS do not.
    """
    path = os.path.join(directory, ".k9pdl.holeprobe.{}".format(os.getpid()))
    size = 8 * 1024 * 1024
    written = 64 * 1024
    fd = None
    try:
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o600)
        os.ftruncate(fd, size)
        os.pwrite(fd, b"x" * written, 0)
        os.fsync(fd)
        hole = os.lseek(fd, 0, os.SEEK_HOLE)
        # the hole must start after what we wrote but within a block or so of it
        return written <= hole <= written + ASSUMED_BLOCK_SIZE
    except (OSError, ValueError):
        return False
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass
        try:
            os.unlink(path)
        except OSError:
            pass


def probe_random_write(directory):
    """
    Confirm the destination tolerates out-of-order writes into a sparse file.

    fstype strings lie, and a FUSE object-store mount will accept these calls while
    turning each one into a read-modify-write and a full-object re-upload. Checking
    that a hole really is unallocated is the cheap way to notice.
    """
    path = os.path.join(directory, ".k9pdl.rwprobe.{}".format(os.getpid()))
    fd = None
    try:
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o600)
        os.ftruncate(fd, 4 * 1024 * 1024)
        payload = b"canine-probe"
        offset = 2 * 1024 * 1024
        os.pwrite(fd, payload, offset)
        if os.pread(fd, len(payload), offset) != payload:
            return False
        if os.pread(fd, 16, 0) != b"\0" * 16:
            return False
        allocated = os.fstat(fd).st_blocks * 512
        return allocated < 4 * 1024 * 1024
    except (OSError, ValueError, AttributeError):
        return False
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass
        try:
            os.unlink(path)
        except OSError:
            pass


# --------------------------------------------------------------------------------
# durable frontier
# --------------------------------------------------------------------------------

def chunk_frontier(fd, chunk_start, chunk_end, block_size=ASSUMED_BLOCK_SIZE):
    """
    How many bytes of [chunk_start, chunk_end) are known to be durably written.

    SEEK_HOLE returns the start of the next hole at or after chunk_start, which is
    reported at block granularity: writing one byte allocates a whole block, so the
    hole can begin *after* the last byte actually written. Over-reporting here would
    cause the resumed download to skip bytes, so the last allocated block is always
    discarded. Worst case that refetches one extra block per chunk.
    """
    try:
        hole = os.lseek(fd, chunk_start, os.SEEK_HOLE)
    except OSError:
        # no hole at or after this offset is reported as ENXIO on some systems
        return chunk_start

    if hole <= chunk_start:
        return chunk_start

    # A fully written chunk is contiguous with whatever follows it, so the hole can
    # land past chunk_end; the chunk cannot be more complete than its own end.
    hole = min(hole, chunk_end)

    safe = hole - block_size
    if safe <= chunk_start:
        return chunk_start
    # align down relative to the chunk, whose start is CHUNK_ALIGN-aligned
    safe -= (safe - chunk_start) % block_size
    return max(chunk_start, min(safe, chunk_end))


# --------------------------------------------------------------------------------
# manifest
# --------------------------------------------------------------------------------

class Manifest:
    """
    Records the immutable chunk *plan*, plus one small record per chunk completion.

    Deliberately not a progress counter. Progress is derived from the file's own
    extents (see module docstring), so the manifest never has to be updated in the hot
    path and can never disagree with the data. Chunk-completion records are rare
    (8-16 per file) and carry the per-part digests needed to verify a multipart ETag
    without re-reading the file.

    Writes are ordered fsync(data) -> update -> tmp+fsync -> atomic rename ->
    fsync(dir), so a crash leaves either the old manifest or the new one and never a
    torn one. A crash before a record is written costs a re-hash, never a re-download.
    """

    def __init__(self, path, state):
        self.path = path
        self.state = state
        self._lock = threading.Lock()

    @classmethod
    def load(cls, path):
        try:
            with open(path, "r") as fh:
                state = json.load(fh)
        except (IOError, OSError, ValueError):
            # absent, truncated, or corrupt all mean the same thing: no usable resume
            # state, so start over rather than risk a corrupt output
            return None
        if not isinstance(state, dict) or state.get("schema_version") != SCHEMA_VERSION:
            return None
        return cls(path, state)

    @classmethod
    def create(cls, path, plan_id, size, chunk_size, chunks, dest_stat, seek_hole,
               checkpoint_interval):
        state = {
            "schema_version": SCHEMA_VERSION,
            "plan_id": plan_id,
            "size": size,
            "chunk_size": chunk_size,
            "n_chunks": len(chunks),
            "dest": {
                "dev": dest_stat.st_dev,
                "ino": dest_stat.st_ino,
                "size": dest_stat.st_size,
            },
            "seek_hole": seek_hole,
            "checkpoint_interval": checkpoint_interval,
            "chunks": {},
            "writer": {
                "hostname": _hostname(),
                "pid": os.getpid(),
                "boot_id": _boot_id(),
            },
        }
        manifest = cls(path, state)
        manifest.flush(data_fd=None)
        return manifest

    def matches(self, plan_id, dest_stat, size):
        """
        A stale manifest left on reused storage, a changed object, or a changed layout
        must all force a clean restart.
        """
        if self.state.get("plan_id") != plan_id:
            return False
        if self.state.get("size") != size:
            return False
        recorded = self.state.get("dest") or {}
        if recorded.get("ino") not in (None, dest_stat.st_ino):
            return False
        if recorded.get("size") not in (None, dest_stat.st_size):
            return False
        return True

    def chunk_record(self, index):
        return self.state.setdefault("chunks", {}).get(str(index)) or {}

    def is_complete(self, index):
        return bool(self.chunk_record(index).get("done"))

    def checkpoint_offset(self, index):
        return int(self.chunk_record(index).get("offset") or 0)

    def record_chunk_done(self, index, data_fd, digest=None):
        with self._lock:
            record = {"done": True}
            if digest is not None:
                record["md5"] = digest
            self.state.setdefault("chunks", {})[str(index)] = record
            self.flush(data_fd)

    def record_checkpoint(self, index, offset, data_fd):
        """Only used on filesystems that failed the SEEK_HOLE probe."""
        with self._lock:
            record = self.state.setdefault("chunks", {}).setdefault(str(index), {})
            record["offset"] = offset
            self.flush(data_fd)

    @property
    def tmp_path(self):
        """
        Per-process staging name. A second writer can legitimately be running against
        the same destination -- a requeued task starting while the preempted VM is
        still being torn down -- and a shared `.tmp` name means one writer's finalize
        deletes the other's half-written staging file out from under its rename.
        """
        return "{}.{}.tmp".format(self.path, os.getpid())

    def flush(self, data_fd):
        # The manifest is bookkeeping, not the source of truth for correctness, so a
        # failure to record it must never fail the download. On the frontier path
        # progress is re-derived from the file's own extents; on the checkpoint
        # fallback a lost record costs a re-download of that chunk, never corruption.
        try:
            self._flush(data_fd)
        except OSError as e:
            log("could not update the manifest ({}); progress may be re-fetched".format(e))

    def _flush(self, data_fd):
        # data before metadata: a manifest claiming progress that the filesystem has
        # not committed would be worse than no manifest at all
        if data_fd is not None:
            try:
                os.fsync(data_fd)
            except OSError as e:
                if e.errno != errno.EINVAL:
                    raise

        tmp = self.tmp_path
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            os.write(fd, json.dumps(self.state).encode("utf-8"))
            os.fsync(fd)
        finally:
            os.close(fd)

        os.rename(tmp, self.path)

        directory = os.path.dirname(os.path.abspath(self.path))
        try:
            dir_fd = os.open(directory, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(dir_fd)
        except OSError:
            pass
        finally:
            os.close(dir_fd)

    def unlink(self):
        # only this process's staging file, never a peer's
        for path in (self.tmp_path, self.path):
            try:
                os.unlink(path)
            except OSError:
                pass


def _hostname():
    try:
        import socket
        return socket.gethostname()
    except Exception:
        return "unknown"


def _boot_id():
    try:
        with open("/proc/sys/kernel/random/boot_id") as fh:
            return fh.read().strip()
    except (IOError, OSError):
        return ""


def try_lock(fd):
    """
    Advisory only, and correctness never depends on it.

    Workers mount the NFS share with `nolock`, so flock coordinates nothing between
    VMs there; on a FUSE object store the call may fail outright with ENOTSUP/ENOSYS.
    Two writers deriving the same plan write byte-identical data to the same offsets,
    so overlapping writes are benign anyway -- the lock only avoids redundant work.
    """
    try:
        import fcntl
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except (ImportError, OSError, IOError):
        return False


# --------------------------------------------------------------------------------
# completion marker
# --------------------------------------------------------------------------------

def write_done_marker(path, size, plan_id, digest):
    payload = {
        "schema_version": SCHEMA_VERSION,
        "size": size,
        "plan_id": plan_id,
        "hash": digest,
    }
    fd = os.open(path + ".tmp", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        os.write(fd, json.dumps(payload).encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)
    os.rename(path + ".tmp", path)


def read_done_marker(path):
    try:
        with open(path) as fh:
            return json.load(fh)
    except (IOError, OSError, ValueError):
        return None


# --------------------------------------------------------------------------------
# byte sources
# --------------------------------------------------------------------------------

class RangeNotSupported(Exception):
    """The server ignored Range, so chunking this object is not possible."""


class PermanentError(Exception):
    """404, 403-after-refresh, permission denied: retrying cannot help."""


class TransientError(Exception):
    """Reset, 5xx, 429, short read: worth another attempt."""


class HttpSource:
    def __init__(self, url, headers=None, timeout=DEFAULT_TIMEOUT, url_refresh_cmd=None):
        self.url = url
        self.headers = dict(headers or {})
        self.timeout = timeout
        self.url_refresh_cmd = url_refresh_cmd
        self._lock = threading.Lock()

    def refresh_url(self):
        """
        Signed URLs can expire mid-transfer. Re-minting one lets the download resume
        in place rather than starting over.
        """
        if not self.url_refresh_cmd:
            return False
        with self._lock:
            try:
                out = subprocess.run(
                    self.url_refresh_cmd, shell=True, capture_output=True, timeout=120
                )
            except subprocess.SubprocessError as e:
                log("URL refresh command failed: {}".format(e))
                return False
            if out.returncode != 0:
                log("URL refresh command exited {}".format(out.returncode))
                return False
            fresh = out.stdout.decode("utf-8", "replace").strip()
            if not fresh.startswith(("http://", "https://")):
                log("URL refresh command produced no usable URL")
                return False
            self.url = fresh
            log("refreshed signed URL")
            return True

    def _request(self, extra_headers):
        headers = dict(self.headers)
        headers.update(extra_headers)
        return urllib.request.Request(self.url, headers=headers)

    def probe_range(self, size):
        """
        Confirm the server really honors Range before any parallel work starts.

        A host-side size is not evidence of range support. A gzip-transcoded GCS object
        silently *ignores* Range and returns the whole object -- and Google bills for
        the entire object per request, so eight workers would each pull the whole
        thing. One aborted probe request bounds that exposure, and it also catches any
        other server that ignores Range.
        """
        request = self._request({"Range": "bytes=0-0"})
        try:
            response = urllib.request.urlopen(request, timeout=self.timeout)
        except urllib.error.HTTPError as e:
            if e.code in (401, 403, 404):
                raise PermanentError("probe failed with HTTP {}".format(e.code))
            raise TransientError("probe failed with HTTP {}".format(e.code))
        except (urllib.error.URLError, OSError) as e:
            raise TransientError("probe failed: {}".format(e))

        try:
            if response.status != 206:
                raise RangeNotSupported(
                    "server returned HTTP {} to a ranged request".format(response.status)
                )
            content_range = response.headers.get("Content-Range", "")
            if not re.match(r"^bytes 0-0/(\d+|\*)$", content_range.strip()):
                raise RangeNotSupported(
                    "unexpected Content-Range {!r}".format(content_range)
                )
            declared = content_range.strip().rsplit("/", 1)[-1]
            if declared != "*" and size is not None and int(declared) != size:
                raise RangeNotSupported(
                    "server reports size {} but {} was expected".format(declared, size)
                )
        finally:
            # abort rather than drain: on a server that ignored Range this is the
            # difference between one wasted request and one wasted object transfer
            response.close()

    def open_range(self, start, end):
        """Open [start, end) inclusive-exclusive. Returns a readable stream."""
        request = self._request({"Range": "bytes={}-{}".format(start, end - 1)})
        try:
            response = urllib.request.urlopen(request, timeout=self.timeout)
        except urllib.error.HTTPError as e:
            if e.code == 403 and self.refresh_url():
                raise TransientError("signed URL expired; refreshed")
            if e.code in (401, 403, 404):
                raise PermanentError("HTTP {}".format(e.code))
            if e.code == 416:
                raise PermanentError("HTTP 416: range not satisfiable")
            raise TransientError("HTTP {}".format(e.code))
        except (urllib.error.URLError, OSError) as e:
            raise TransientError(str(e))

        expected = end - start
        if response.status != 206:
            response.close()
            raise RangeNotSupported(
                "server returned HTTP {} to a ranged request".format(response.status)
            )

        content_range = response.headers.get("Content-Range", "").strip()
        match = re.match(r"^bytes (\d+)-(\d+)/", content_range)
        if not match or int(match.group(1)) != start or int(match.group(2)) != end - 1:
            response.close()
            raise TransientError(
                "Content-Range {!r} does not match requested {}-{}".format(
                    content_range, start, end - 1
                )
            )

        length = response.headers.get("Content-Length")
        if length is not None and int(length) != expected:
            response.close()
            raise TransientError(
                "Content-Length {} does not match requested {}".format(length, expected)
            )
        return response


class S3ApiSource:
    """
    Fallback for when `aws s3 presign` cannot be used (session-token-only creds, an
    exotic endpoint). Costs one `aws` process per chunk attempt, which is why the
    presigned-URL path through HttpSource is preferred.
    """

    def __init__(self, bucket, key, extra_args="", env=None, timeout=DEFAULT_TIMEOUT):
        self.bucket = bucket
        self.key = key
        self.extra_args = extra_args
        self.env = env
        self.timeout = timeout

    def refresh_url(self):
        return False

    def probe_range(self, size):
        return None

    def open_range(self, start, end):
        command = (
            "aws s3api {extra} get-object --bucket {bucket} --key {key} "
            "--range {range} /dev/stdout"
        ).format(
            extra=self.extra_args,
            bucket=shlex.quote(self.bucket),
            key=shlex.quote(self.key),
            range=shlex.quote("bytes={}-{}".format(start, end - 1)),
        )
        try:
            process = subprocess.Popen(
                command, shell=True, stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL, env=self.env,
            )
        except OSError as e:
            raise TransientError(str(e))
        return _ProcessStream(process)


class _ProcessStream:
    def __init__(self, process):
        self.process = process

    def read(self, n):
        return self.process.stdout.read(n)

    def close(self):
        try:
            self.process.stdout.close()
        except (IOError, OSError):
            pass
        try:
            self.process.wait(timeout=30)
        except subprocess.SubprocessError:
            self.process.kill()


# --------------------------------------------------------------------------------
# download
# --------------------------------------------------------------------------------

class Progress:
    def __init__(self, total):
        self.total = total
        self.done = 0
        self.transferred = 0
        self._lock = threading.Lock()
        self._last = 0.0

    def add(self, n, resumed=False):
        with self._lock:
            self.done += n
            if not resumed:
                self.transferred += n
            now = time.monotonic()
            if now - self._last < PROGRESS_INTERVAL:
                return
            self._last = now
            pct = (100.0 * self.done / self.total) if self.total else 100.0
            log("{:.1f}% ({}/{} bytes, {} transferred)".format(
                pct, self.done, self.total, self.transferred))


class Downloader:
    def __init__(self, source, fd, manifest, chunks, options, progress):
        self.source = source
        self.fd = fd
        self.manifest = manifest
        self.chunks = chunks
        self.options = options
        self.progress = progress
        self.made_progress = False
        self._progress_lock = threading.Lock()
        self.use_checkpoints = not manifest.state.get("seek_hole", True)
        self.checkpoint_interval = (
            manifest.state.get("checkpoint_interval") or FALLBACK_CHECKPOINT_INTERVAL
        )

    def resume_offset(self, index):
        """
        Where this chunk should continue from. Derived from the file's own extents
        where SEEK_HOLE works, and only from a stored counter where it does not.
        """
        start, end = self.chunks[index]
        if self.manifest.is_complete(index):
            return end
        if self.use_checkpoints:
            recorded = self.manifest.checkpoint_offset(index)
            return min(max(start, recorded), end)
        frontier = chunk_frontier(self.fd, start, end)
        # a frontier must be monotonic and within the chunk; anything else means the
        # derivation cannot be trusted, so rewind to the chunk start
        if not start <= frontier <= end:
            log("chunk {}: implausible frontier {}, restarting chunk".format(index, frontier))
            return start
        return frontier

    def download_chunk(self, index):
        start, end = self.chunks[index]
        offset = self.resume_offset(index)

        if offset >= end:
            if not self.manifest.is_complete(index):
                self.manifest.record_chunk_done(index, self.fd)
            self.progress.add(end - start, resumed=True)
            return

        if offset > start:
            self.progress.add(offset - start, resumed=True)

        attempts = 0
        last_checkpoint = offset
        while offset < end:
            try:
                stream = self.source.open_range(offset, end)
            except TransientError as e:
                attempts += 1
                if attempts > self.options.retries:
                    raise
                self._backoff(attempts, "chunk {}: {}".format(index, e))
                continue

            try:
                while offset < end:
                    want = min(READ_BLOCK, end - offset)
                    buf = stream.read(want)
                    if not buf:
                        raise TransientError(
                            "short read at {} ({} bytes short)".format(offset, end - offset)
                        )
                    self._pwrite_all(buf, offset)
                    offset += len(buf)
                    self.progress.add(len(buf))
                    with self._progress_lock:
                        self.made_progress = True

                    if (self.use_checkpoints
                            and offset - last_checkpoint >= self.checkpoint_interval):
                        self.manifest.record_checkpoint(index, offset, self.fd)
                        last_checkpoint = offset
                attempts = 0
            except TransientError as e:
                attempts += 1
                if attempts > self.options.retries:
                    raise
                self._backoff(attempts, "chunk {}: {}".format(index, e))
            except (IOError, OSError) as e:
                if e.errno == errno.ENOSPC:
                    self._await_space(index)
                    continue
                attempts += 1
                if attempts > self.options.retries:
                    raise TransientError(str(e))
                self._backoff(attempts, "chunk {}: {}".format(index, e))
            finally:
                try:
                    stream.close()
                except (IOError, OSError):
                    pass

        self.manifest.record_chunk_done(index, self.fd)

    def _pwrite_all(self, buf, offset):
        view = memoryview(buf)
        while view:
            written = os.pwrite(self.fd, view, offset)
            if written <= 0:
                raise TransientError("pwrite returned {}".format(written))
            view = view[written:]
            offset += written

    def _await_space(self, index):
        """
        The localization disk grows on demand, so ENOSPC is a temporary condition
        rather than a failure. The frontier scheme means a paused chunk resumes exactly
        where it stopped and loses nothing.
        """
        waited = 0
        delay = 5
        while waited < ENOSPC_MAX_WAIT:
            log("chunk {}: ENOSPC, waiting {}s for the disk to grow".format(index, delay))
            time.sleep(delay)
            waited += delay
            delay = min(delay * 2, 60)
            try:
                probe = os.statvfs(os.path.dirname(os.path.abspath(self.options.dest)))
                if probe.f_bavail * probe.f_frsize > READ_BUFFER:
                    return
            except OSError:
                return
        raise TransientError("still out of space after {}s".format(ENOSPC_MAX_WAIT))

    def _backoff(self, attempt, message):
        delay = min(2 ** attempt, 60) * (0.5 + random.random())
        log("{} -- retrying in {:.1f}s (attempt {}/{})".format(
            message, delay, attempt, self.options.retries))
        time.sleep(delay)

    def run(self):
        pending = [i for i in range(len(self.chunks)) if not self.manifest.is_complete(i)]
        if not pending:
            return
        workers = max(1, min(self.options.connections, len(pending), MAX_CONNECTIONS))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(self.download_chunk, i) for i in pending]
            errors = []
            for future in futures:
                try:
                    future.result()
                except Exception as e:  # re-raised below, once every worker has settled
                    errors.append(e)
        if errors:
            for error in errors:
                if isinstance(error, PermanentError):
                    raise error
            raise errors[0]


# --------------------------------------------------------------------------------
# verification
# --------------------------------------------------------------------------------

def file_md5(path, block=READ_BUFFER):
    digest = hashlib.md5()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(block), b""):
            digest.update(chunk)
    return digest.hexdigest()


def multipart_etag(path, part_length, block=READ_BUFFER):
    """
    S3's multipart ETag is md5-of-md5s with a "-N" part-count suffix. Computed here by
    reading the finished file; when chunk boundaries were snapped to part boundaries
    the per-part digests are already in the manifest, so this pass is avoidable.
    """
    digests = []
    with open(path, "rb") as fh:
        while True:
            part = _read_exactly(fh, part_length, block)
            if not part:
                break
            digests.append(hashlib.md5(part).digest())
    if not digests:
        return None
    return "{}-{}".format(hashlib.md5(b"".join(digests)).hexdigest(), len(digests))


def _read_exactly(fh, count, block):
    buf = bytearray()
    while len(buf) < count:
        piece = fh.read(min(block, count - len(buf)))
        if not piece:
            break
        buf.extend(piece)
    return bytes(buf)


def normalize_expected_md5(value):
    """Accept either hex or the base64 form servers put in Content-MD5."""
    value = value.strip().strip('"')
    if re.fullmatch(r"[0-9a-fA-F]{32}", value):
        return value.lower()
    try:
        raw = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError):
        return value.lower()
    if len(raw) != 16:
        return value.lower()
    return binascii.hexlify(raw).decode()


def verify(path, options):
    """
    Verification is an absolute guarantee, not a best-effort optimization: when a hash
    was supplied the file is checked before the done marker is written, and being
    unable to check is a hard failure rather than a silent pass.

    Returns the digest that was verified, or None when no check was requested.
    """
    if options.check_etag and options.part_length:
        actual = multipart_etag(path, options.part_length)
        expected = options.check_etag.strip().strip('"')
        if actual != expected:
            raise PermanentError(
                "ETag mismatch: expected {}, got {}".format(expected, actual)
            )
        return actual

    if options.check_md5:
        actual = file_md5(path)
        expected = normalize_expected_md5(options.check_md5)
        if actual != expected:
            raise PermanentError(
                "md5 mismatch: expected {}, got {}".format(expected, actual)
            )
        return actual

    return None


# --------------------------------------------------------------------------------
# single-stream fallback
# --------------------------------------------------------------------------------

def single_stream_fallback(options, reason):
    """
    Everything the chunked path declines to handle falls back to `curl -C -`, which is
    exactly today's behavior and today's resume semantics.
    """
    log("falling back to a single stream: {}".format(reason))
    if options.legacy_cmd:
        command = options.legacy_cmd
    else:
        command = "curl -C - -sSL{headers} -o {dest} {url}".format(
            headers="".join(
                " --header {}".format(shlex.quote(h)) for h in (options.header or [])
            ),
            dest=shlex.quote(options.dest),
            url=shlex.quote(options.url),
        )
    result = subprocess.run(command, shell=True)
    return result.returncode


# --------------------------------------------------------------------------------
# orchestration
# --------------------------------------------------------------------------------

def open_destination(dest, size):
    """
    Create the working file sparse at its final size.

    ftruncate, deliberately not posix_fallocate: fallocate's unwritten extents are
    indistinguishable from written data, which would destroy the frontier scheme. The
    cost is that an incomplete file already has its full apparent size, which is why
    completion is asserted only by the done marker.
    """
    directory = os.path.dirname(os.path.abspath(dest))
    if directory and not os.path.isdir(directory):
        os.makedirs(directory, exist_ok=True)
    fd = os.open(dest, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        if os.fstat(fd).st_size != size:
            os.ftruncate(fd, size)
    except OSError:
        os.close(fd)
        raise
    return fd


def run(options):
    dest = options.dest
    manifest_path, marker_path = sidecar_paths(dest)

    if not options.no_resume:
        marker = read_done_marker(marker_path)
        if marker and marker.get("size") == options.size:
            log("already complete per {}".format(os.path.basename(marker_path)))
            return EXIT_OK

    size = options.size
    if size is None or size < 0:
        return single_stream_fallback(options, "size unknown")

    if urllib.parse.urlsplit(options.url or "").scheme == "ftp":
        return single_stream_fallback(options, "ftp is not rangeable")

    if options.connections <= 1:
        return single_stream_fallback(options, "connections <= 1")

    source = build_source(options)

    # The probe is retried rather than allowed to fail the attempt: a single 5xx or
    # reset here would otherwise cost a whole job requeue before any bytes moved.
    attempt = 0
    while True:
        try:
            source.probe_range(size)
            break
        except RangeNotSupported as e:
            return single_stream_fallback(options, str(e))
        except PermanentError as e:
            log("permanent failure probing the object: {}".format(e))
            return EXIT_FAIL
        except TransientError as e:
            attempt += 1
            if attempt > options.retries:
                # No bytes have moved, so this is the "no forward progress" case: exit
                # 1 rather than 5. Exit-5 requeues are excluded from the preemption
                # limit, so requeueing here would loop forever against a server that
                # is never going to answer.
                log("could not probe range support after {} attempts: {}".format(
                    attempt, e))
                return EXIT_FAIL
            delay = min(2 ** attempt, 30) * (0.5 + random.random())
            log("probe failed ({}); retrying in {:.1f}s".format(e, delay))
            time.sleep(delay)

    directory = os.path.dirname(os.path.abspath(dest))
    if directory and not os.path.isdir(directory):
        os.makedirs(directory, exist_ok=True)

    seek_hole = probe_seek_hole(directory)
    if not seek_hole:
        log("SEEK_HOLE unsupported here; using {} MiB checkpoints instead".format(
            FALLBACK_CHECKPOINT_INTERVAL // (1024 * 1024)))
    if not probe_random_write(directory):
        return single_stream_fallback(
            options, "destination does not support sparse random writes"
        )

    chunks = plan_chunks(size, options.connections, options.min_chunk, options.part_length)
    chunk_size = (chunks[0][1] - chunks[0][0]) if chunks else size
    plan_id = compute_plan_id(
        options.url or "", size, options.check_etag or options.check_md5, chunk_size
    )

    fd = open_destination(dest, size)
    manifest = None
    try:
        dest_stat = os.fstat(fd)

        manifest = None if options.no_resume else Manifest.load(manifest_path)
        if manifest is not None and not manifest.matches(plan_id, dest_stat, size):
            log("manifest describes a different object or layout; restarting")
            manifest.unlink()
            manifest = None
            os.ftruncate(fd, 0)
            os.ftruncate(fd, size)
            dest_stat = os.fstat(fd)

        if manifest is None:
            manifest = Manifest.create(
                manifest_path, plan_id, size, chunk_size, chunks, dest_stat,
                seek_hole, 0 if seek_hole else checkpoint_interval_for(chunk_size),
            )
        else:
            log("resuming: {}/{} chunks already complete".format(
                sum(1 for i in range(len(chunks)) if manifest.is_complete(i)),
                len(chunks)))

        try_lock(fd)

        downloader = Downloader(source, fd, manifest, chunks, options, Progress(size))
        try:
            downloader.run()
        except PermanentError as e:
            log("permanent failure: {}".format(e))
            return EXIT_FAIL
        except RangeNotSupported as e:
            os.close(fd)
            fd = None
            return single_stream_fallback(options, str(e))
        except TransientError as e:
            # forward progress decides requeue-vs-fail: returning 5 unconditionally
            # would let a download that can never progress requeue forever, and exit-5
            # requeues are deliberately excluded from the preemption limit
            if downloader.made_progress:
                log("transient failure after progress; requesting requeue: {}".format(e))
                return EXIT_REQUEUE
            log("no forward progress this attempt: {}".format(e))
            return EXIT_FAIL

        os.fsync(fd)
        actual_size = os.fstat(fd).st_size
        if actual_size != size:
            log("size mismatch after download: {} != {}".format(actual_size, size))
            return EXIT_FAIL
    finally:
        if fd is not None:
            os.close(fd)

    try:
        digest = verify(dest, options)
    except PermanentError as e:
        log("verification failed: {}".format(e))
        discard(dest, manifest)
        return EXIT_FAIL

    write_done_marker(marker_path, size, plan_id, digest)
    if manifest is not None:
        manifest.unlink()
    log("complete: {} bytes{}".format(size, " (verified)" if digest else ""))
    return EXIT_OK


def discard(dest, manifest):
    try:
        os.unlink(dest)
    except OSError:
        pass
    if manifest is not None:
        manifest.unlink()


def build_source(options):
    if options.s3_bucket and options.s3_key:
        return S3ApiSource(
            options.s3_bucket, options.s3_key, options.s3_extra_args or "",
            timeout=options.timeout,
        )
    headers = {}
    for item in options.header or []:
        name, _, value = item.partition(":")
        if value:
            headers[name.strip()] = value.strip()
    return HttpSource(options.url, headers, options.timeout, options.url_refresh_cmd)


# --------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------

def build_parser():
    parser = argparse.ArgumentParser(
        prog="parallel_download.py",
        description="Download one object with N parallel ranged GETs, resumably.",
    )
    parser.add_argument("--url", help="object URL")
    parser.add_argument("--dest", required=True, help="destination path")
    parser.add_argument("--size", type=int, help="expected size in bytes")
    parser.add_argument("--connections", type=int, default=DEFAULT_CONNECTIONS,
                        help="simultaneous ranged GETs (0/1 = single stream)")
    parser.add_argument("--min-chunk", type=int, default=DEFAULT_MIN_CHUNK,
                        dest="min_chunk", help="smallest chunk worth its own request")
    parser.add_argument("--header", action="append", default=[],
                        help="extra request header, 'Name: value' (repeatable)")
    parser.add_argument("--s3-bucket", dest="s3_bucket")
    parser.add_argument("--s3-key", dest="s3_key")
    parser.add_argument("--s3-extra-args", dest="s3_extra_args", default="")
    parser.add_argument("--check-md5", dest="check_md5",
                        help="expected whole-file md5 (hex or base64)")
    parser.add_argument("--check-etag", dest="check_etag",
                        help="expected S3 multipart ETag")
    parser.add_argument("--part-length", dest="part_length", type=int,
                        help="S3 multipart part length, for ETag verification")
    parser.add_argument("--url-refresh-cmd", dest="url_refresh_cmd",
                        help="shell command printing a fresh signed URL")
    parser.add_argument("--legacy-cmd", dest="legacy_cmd",
                        help="command to run for the single-stream fallback")
    parser.add_argument("--no-resume", action="store_true", dest="no_resume",
                        help="discard any existing partial file")
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    return parser


def main(argv=None):
    parser = build_parser()
    options = parser.parse_args(argv)

    if os.environ.get("CANINE_DISABLE_PARALLEL_DOWNLOAD"):
        return single_stream_fallback(options, "CANINE_DISABLE_PARALLEL_DOWNLOAD is set")

    override = os.environ.get("CANINE_DOWNLOAD_CONNECTIONS")
    if override:
        try:
            options.connections = int(override)
        except ValueError:
            log("ignoring unparseable CANINE_DOWNLOAD_CONNECTIONS={!r}".format(override))

    if not options.url and not (options.s3_bucket and options.s3_key):
        parser.error("one of --url or --s3-bucket/--s3-key is required")

    try:
        return run(options)
    except KeyboardInterrupt:
        log("interrupted")
        return EXIT_REQUEUE
    except PermanentError as e:
        log("permanent failure: {}".format(e))
        return EXIT_FAIL
    except Exception:
        # Nothing may escape as a bare traceback: canine's entrypoint reads the exit
        # code, and an unhandled exception exiting 1 is indistinguishable from a
        # deliberate do-not-retry. Log the traceback for diagnosis and return 1
        # explicitly -- a code defect should surface loudly rather than requeue forever.
        import traceback
        log("unexpected failure:\n" + traceback.format_exc())
        return EXIT_FAIL


if __name__ == "__main__":
    sys.exit(main())

# k9pdl-eof
