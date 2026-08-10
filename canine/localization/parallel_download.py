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
import tempfile
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
# destination filesystem gate and route selection
# --------------------------------------------------------------------------------

MOUNTS_PATH = "/proc/mounts"

# An ALLOWLIST, not a denylist: an unrecognized future filesystem must fail safe to a
# conservative route rather than silently take the chunked fast path. Every entry here
# is one where sparse files, random writes and rename-atomicity behave as POSIX says.
POSIX_FSTYPE_ALLOWLIST = frozenset({
    "ext2", "ext3", "ext4", "xfs", "btrfs", "tmpfs", "nfs", "nfs4", "zfs",
})

# GCS bucket naming: 3-63 chars of lowercase alphanumerics, dashes, underscores, dots.
_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]$")

ROUTE_POSIX = "A"       # write chunks straight into dest (the dominant PD/NFS path)
ROUTE_BUCKET = "B"      # upload parts, compose server-side (bucket-backed dest)
ROUTE_STAGED = "C"      # stage on a real block device, then publish


class Mount:
    __slots__ = ("mountpoint", "fstype", "device", "options")

    def __init__(self, mountpoint, fstype, device, options):
        self.mountpoint = mountpoint
        self.fstype = fstype
        self.device = device
        self.options = options

    def option(self, name):
        for item in self.options.split(","):
            key, _, value = item.partition("=")
            if key == name:
                return value
        return None

    def __repr__(self):
        return "Mount({!r}, {!r}, {!r})".format(self.mountpoint, self.fstype, self.device)


class RouteDecision:
    __slots__ = ("route", "reason", "mount", "gs_url", "seek_hole")

    def __init__(self, route, reason, mount=None, gs_url=None, seek_hole=False):
        self.route = route
        self.reason = reason
        self.mount = mount
        self.gs_url = gs_url
        self.seek_hole = seek_hole

    def __repr__(self):
        return "RouteDecision({}, {!r})".format(self.route, self.reason)


def read_mounts(path=None):
    """
    Parse the mount table. Returns [] when it is unavailable (a non-Linux host), which
    callers must distinguish from "parsed but nothing matched".
    """
    try:
        with open(path or MOUNTS_PATH) as fh:
            raw = fh.read()
    except (IOError, OSError):
        return []

    mounts = []
    for line in raw.splitlines():
        fields = line.split()
        if len(fields) < 4:
            continue
        device, mountpoint, fstype, options = fields[0], fields[1], fields[2], fields[3]
        # mount(8) escapes spaces and friends as octal in both device and mountpoint
        mountpoint = mountpoint.replace("\\040", " ").replace("\\011", "\t")
        mounts.append(Mount(mountpoint, fstype, device, options))
    return mounts


def resolve_mount(path, mounts):
    """
    Find the mount that `path` lives on, by longest-prefix match after realpath.

    Matching is component-wise, so /mnt/foo does not match a /mnt/foobar mount.

    The path need not exist: realpath resolves symlinks in whatever components do exist
    and leaves the rest lexically, which is what we want since the destination file is
    about to be created. (Walking up to the nearest existing ancestor instead would
    collapse a path under an unmounted tree all the way to /, and silently match the
    root filesystem's fstype.)
    """
    if not mounts:
        return None

    target = os.path.realpath(os.path.abspath(path))

    best = None
    for mount in mounts:
        mountpoint = mount.mountpoint.rstrip("/") or "/"
        if target == mountpoint or mountpoint == "/" or target.startswith(
            mountpoint + "/"
        ):
            if best is None or len(mountpoint) > len(best.mountpoint.rstrip("/") or "/"):
                best = mount
    return best


def gs_url_for(path, mount):
    """
    Work out the gs:// URL a path on a gcsfuse mount corresponds to, or None when it
    cannot be determined unambiguously.

    Returning None is not a failure -- it routes to stage-then-publish instead, which
    is correct for any bucket we cannot address directly. Guessing would be worse: the
    parts would be uploaded to the wrong place.
    """
    if mount is None or not mount.fstype.startswith("fuse"):
        return None

    bucket = mount.device
    if bucket in ("gcsfuse", "fuse", ""):
        # some versions report a placeholder device and name the bucket in the options
        bucket = mount.option("bucket") or ""
    if "/" in bucket or not _BUCKET_RE.match(bucket):
        return None

    prefix = (mount.option("only_dir") or mount.option("only-dir") or "").strip("/")

    mountpoint = mount.mountpoint.rstrip("/") or "/"
    target = os.path.realpath(os.path.abspath(path))
    if mountpoint == "/":
        relative = target.lstrip("/")
    elif target == mountpoint:
        relative = ""
    elif target.startswith(mountpoint + "/"):
        relative = target[len(mountpoint) + 1:]
    else:
        return None

    object_name = "/".join(part for part in (prefix, relative) if part)
    if not object_name:
        return None
    return "gs://{}/{}".format(bucket, object_name)


def select_route(dest, mounts=None, random_write_probe=None, seek_hole_probe=None):
    """
    Decide how to write `dest`, gating on what its filesystem can actually do.

    The rule is "gate, don't adapt": rather than trying to make the chunked writer
    behave on a FUSE object store -- where there are no sparse files, random writes
    degrade to full-object re-uploads, nothing is durable before close(), and rename is
    not atomic -- detect it up front and take a different route.

    Two independent checks, because either alone is insufficient:
      * the fstype allowlist, which catches known-bad filesystems by name;
      * a random-write probe, because fstype strings lie and a FUSE mount will happily
        accept the calls while quietly turning each into a read-modify-write.

    SEEK_HOLE is deliberately NOT a gate. Failing it (NFSv3) only means the frontier
    must be replaced by checkpointing, which is still an in-place chunked write.
    """
    random_write_probe = random_write_probe or probe_random_write
    seek_hole_probe = seek_hole_probe or probe_seek_hole

    directory = os.path.dirname(os.path.abspath(dest)) or "/"
    mounts = read_mounts() if mounts is None else mounts
    mount = resolve_mount(dest, mounts)

    if mount is None:
        # No mount table at all (a non-Linux host, e.g. a developer machine). The
        # allowlist cannot be consulted, so fall back to the runtime probes, which are
        # the stronger signal anyway. On the production target /proc/mounts always
        # exists, so the strict path below is what actually runs there.
        if mounts:
            return RouteDecision(
                ROUTE_STAGED,
                "no mount table entry covers {}; failing safe".format(directory),
            )
        if not random_write_probe(directory):
            return RouteDecision(
                ROUTE_STAGED,
                "destination does not support sparse random writes",
            )
        return RouteDecision(
            ROUTE_POSIX, "mount table unavailable; accepted on probes alone",
            seek_hole=seek_hole_probe(directory),
        )

    if mount.fstype in POSIX_FSTYPE_ALLOWLIST:
        if not random_write_probe(directory):
            # fstype says POSIX but behavior says otherwise; behavior wins
            return RouteDecision(
                ROUTE_STAGED,
                "{} on {} failed the random-write probe".format(
                    mount.fstype, mount.mountpoint),
                mount=mount,
            )
        return RouteDecision(
            ROUTE_POSIX, "{} on {}".format(mount.fstype, mount.mountpoint),
            mount=mount, seek_hole=seek_hole_probe(directory),
        )

    # Off the allowlist. A bucket we can address directly is strictly better than
    # staging, because GCS compose concatenates server-side with no data transfer.
    gs_url = gs_url_for(dest, mount)
    if gs_url:
        return RouteDecision(
            ROUTE_BUCKET,
            "{} on {} resolves to {}".format(mount.fstype, mount.mountpoint, gs_url),
            mount=mount, gs_url=gs_url,
        )

    return RouteDecision(
        ROUTE_STAGED,
        "{} on {} is not an allowlisted POSIX filesystem and no bucket could be "
        "resolved".format(mount.fstype, mount.mountpoint),
        mount=mount,
    )


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

    def record_session(self, index, session_uri):
        """
        Persist a resumable upload session URI (Route B).

        This MUST happen before any bytes are sent to that session, because the URI is
        the only handle to the partially-uploaded data: losing it means the part has to
        start over, and losing it after sending bytes means paying for bytes nothing can
        ever reach.
        """
        with self._lock:
            record = self.state.setdefault("chunks", {}).setdefault(str(index), {})
            record["session"] = session_uri
            self.flush(None)

    def session_uri(self, index):
        return self.chunk_record(index).get("session")

    def record_part_digest(self, index, md5_hex):
        with self._lock:
            record = self.state.setdefault("chunks", {}).setdefault(str(index), {})
            record["md5"] = md5_hex
            self.flush(None)

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
# GCS JSON API client (Route B)
# --------------------------------------------------------------------------------

GCS_API_ROOT = "https://storage.googleapis.com/storage/v1"
GCS_UPLOAD_ROOT = "https://storage.googleapis.com/upload/storage/v1"
METADATA_TOKEN_URL = (
    "http://metadata.google.internal/computeMetadata/v1/"
    "instance/service-accounts/default/token"
)

# GCS persists resumable-upload bytes at this granularity, and every non-final PUT in a
# session must be a multiple of it. This is what bounds discarded work on Route B.
GCS_UPLOAD_GRANULARITY = 256 * 1024

# A single compose call accepts at most this many sources; more are tree-composed.
GCS_COMPOSE_MAX_SOURCES = 32


class GcsClient:
    """
    Minimal GCS JSON API client over urllib, so Route B needs nothing beyond the
    standard library.

    Writes deliberately bypass the gcsfuse mount and go straight to the API. That
    sidesteps staged-write re-uploads, the close()-only durability rule, the metadata
    cache and non-atomic rename in one move.
    """

    def __init__(self, timeout=DEFAULT_TIMEOUT):
        self.timeout = timeout
        self._token = None
        self._token_expiry = 0.0
        self._lock = threading.Lock()

    # -- auth ---------------------------------------------------------------

    def token(self):
        with self._lock:
            if self._token and time.monotonic() < self._token_expiry:
                return self._token
            token, lifetime = self._fetch_token()
            self._token = token
            # renew early; a token expiring mid-upload would fail a whole part
            self._token_expiry = time.monotonic() + max(60, lifetime - 300)
            return self._token

    def _fetch_token(self):
        """
        Prefer the GCE metadata server (no subprocess, and workers are GCE VMs), then
        fall back to gcloud, which canine already relies on elsewhere.
        """
        request = urllib.request.Request(
            METADATA_TOKEN_URL, headers={"Metadata-Flavor": "Google"}
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                payload = json.loads(response.read().decode("utf-8"))
            return payload["access_token"], int(payload.get("expires_in", 3600))
        except Exception:
            pass

        try:
            out = subprocess.run(
                ["gcloud", "auth", "print-access-token"],
                capture_output=True, timeout=60,
            )
            if out.returncode == 0:
                token = out.stdout.decode("utf-8").strip()
                if token:
                    return token, 3600
        except (OSError, subprocess.SubprocessError):
            pass

        raise PermanentError(
            "could not obtain a GCS access token from the metadata server or gcloud"
        )

    # -- plumbing -----------------------------------------------------------

    def request(self, method, url, body=None, headers=None, expect=(200, 201)):
        """
        Issue a request and return (status, headers, body).

        Unlike urlopen this does not raise for the status codes the resumable-upload
        protocol uses as ordinary signals -- notably 308, which means "session alive,
        here is how far I have persisted".
        """
        all_headers = {"Authorization": "Bearer " + self.token()}
        all_headers.update(headers or {})
        request = urllib.request.Request(
            url, data=body, headers=all_headers, method=method
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return response.status, dict(response.headers), response.read()
        except urllib.error.HTTPError as e:
            payload = b""
            try:
                payload = e.read()
            except Exception:
                pass
            status = e.code
            if status in expect:
                return status, dict(e.headers or {}), payload
            if status in (401, 403, 404, 410):
                raise PermanentError("{} {} -> HTTP {}: {}".format(
                    method, _strip_query(url), status,
                    payload[:200].decode("utf-8", "replace")))
            raise TransientError("{} {} -> HTTP {}".format(
                method, _strip_query(url), status))
        except (urllib.error.URLError, OSError) as e:
            raise TransientError("{} {} -> {}".format(method, _strip_query(url), e))

    # -- objects ------------------------------------------------------------

    def start_resumable_upload(self, bucket, name):
        """Returns the session URI. Must be persisted before any bytes are sent."""
        url = "{}/b/{}/o?uploadType=resumable&name={}".format(
            GCS_UPLOAD_ROOT, urllib.parse.quote(bucket, safe=""),
            urllib.parse.quote(name, safe=""),
        )
        status, headers, _ = self.request(
            "POST", url, body=json.dumps({"name": name}).encode("utf-8"),
            headers={"Content-Type": "application/json; charset=UTF-8"},
        )
        session = headers.get("Location") or headers.get("location")
        if not session:
            raise TransientError("resumable upload session had no Location header")
        return session

    def session_offset(self, session_uri, total):
        """
        Ask the session how far it has durably persisted.

        This is Route B's frontier oracle, exactly analogous to SEEK_HOLE: the answer
        comes from the storage rather than from a counter we kept. `None` means the
        upload already finished; 0 means nothing is persisted yet.
        """
        status, headers, _ = self.request(
            "PUT", session_uri,
            headers={"Content-Range": "bytes */{}".format(total), "Content-Length": "0"},
            expect=(200, 201, 308),
        )
        if status in (200, 201):
            return None
        header_range = headers.get("Range") or headers.get("range")
        if not header_range:
            return 0
        match = re.match(r"bytes=(\d+)-(\d+)", header_range.strip())
        if not match:
            return 0
        return int(match.group(2)) + 1

    def upload_range(self, session_uri, chunk, offset, total):
        """
        Send `chunk` at `offset`. Returns `(metadata, committed)`:

          * `metadata` is the object metadata once the session completes, else None;
          * `committed` is how far GCS says it has persisted, which is NOT necessarily
            offset + len(chunk). Never assume the local byte count is authoritative --
            a chunk marked complete on that basis leaves a part that does not exist.

        Persisted bytes can never be overwritten, so re-sending an already-committed
        range is harmless, which is what makes a retry safe.
        """
        end = offset + len(chunk) - 1
        status, headers, body = self.request(
            "PUT", session_uri, body=chunk,
            headers={
                "Content-Length": str(len(chunk)),
                "Content-Range": "bytes {}-{}/{}".format(offset, end, total),
            },
            expect=(200, 201, 308),
        )
        if status in (200, 201):
            try:
                return json.loads(body.decode("utf-8")), total
            except ValueError:
                return {}, total

        header_range = headers.get("Range") or headers.get("range")
        committed = 0
        if header_range:
            match = re.match(r"bytes=(\d+)-(\d+)", header_range.strip())
            if match:
                committed = int(match.group(2)) + 1
        return None, committed

    def get_object(self, bucket, name):
        url = "{}/b/{}/o/{}".format(
            GCS_API_ROOT, urllib.parse.quote(bucket, safe=""),
            urllib.parse.quote(name, safe=""),
        )
        _, _, body = self.request("GET", url)
        return json.loads(body.decode("utf-8"))

    def delete_object(self, bucket, name):
        url = "{}/b/{}/o/{}".format(
            GCS_API_ROOT, urllib.parse.quote(bucket, safe=""),
            urllib.parse.quote(name, safe=""),
        )
        try:
            self.request("DELETE", url, expect=(200, 204, 404))
        except PermanentError:
            pass  # already gone

    def compose(self, bucket, destination, sources):
        """
        Concatenate `sources` into `destination` server-side.

        compose transfers no object data at all -- it is a metadata operation billed as
        one Class A op -- so unlike a POSIX merge there is no sequential tail to pay for
        and the whole transfer stays parallel end to end.
        """
        url = "{}/b/{}/o/{}/compose".format(
            GCS_API_ROOT, urllib.parse.quote(bucket, safe=""),
            urllib.parse.quote(destination, safe=""),
        )
        body = json.dumps({
            "sourceObjects": [{"name": name} for name in sources],
            "destination": {"name": destination},
        }).encode("utf-8")
        _, _, payload = self.request(
            "POST", url, body=body,
            headers={"Content-Type": "application/json"},
        )
        return json.loads(payload.decode("utf-8"))

    def download_range(self, bucket, name, start, end):
        """Ranged read of an object, used for the read-back verification pass."""
        url = "{}/b/{}/o/{}?alt=media".format(
            GCS_API_ROOT, urllib.parse.quote(bucket, safe=""),
            urllib.parse.quote(name, safe=""),
        )
        _, _, body = self.request(
            "GET", url,
            headers={"Range": "bytes={}-{}".format(start, end - 1)},
            expect=(200, 206),
        )
        return body


def _strip_query(url):
    """Session URIs carry an upload_id; keep it out of logs."""
    split = urllib.parse.urlsplit(url)
    return urllib.parse.urlunsplit((split.scheme, split.netloc, split.path, "", ""))


def split_gs_url(url):
    """gs://bucket/path/to/object -> ('bucket', 'path/to/object')."""
    if not url.startswith("gs://"):
        raise ValueError("not a gs:// URL: {}".format(url))
    remainder = url[len("gs://"):]
    bucket, _, name = remainder.partition("/")
    if not bucket or not name:
        raise ValueError("gs:// URL has no object name: {}".format(url))
    return bucket, name


def compose_tree(client, bucket, destination, parts, max_sources=GCS_COMPOSE_MAX_SOURCES):
    """
    Compose `parts` (in order) into `destination`, tree-composing when there are more
    than one call can take.

    Sources may themselves be composite, so 800 parts become 25 intermediates and then
    one object. Intermediates are deleted as soon as they have been consumed.
    """
    level = list(parts)
    generation = 0
    created = []

    while len(level) > max_sources:
        next_level = []
        for index in range(0, len(level), max_sources):
            group = level[index:index + max_sources]
            if len(group) == 1:
                next_level.append(group[0])
                continue
            intermediate = "{}.k9pdl.compose/{}-{:05d}".format(
                destination, generation, index // max_sources)
            client.compose(bucket, intermediate, group)
            created.append(intermediate)
            next_level.append(intermediate)
        level = next_level
        generation += 1

    result = client.compose(bucket, destination, level) if len(level) > 1 else None
    if result is None:
        # a single source: compose still gives the destination the right name
        result = client.compose(bucket, destination, level)

    for name in created:
        client.delete_object(bucket, name)
    return result


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


class PosixChunkSink:
    """
    Writes chunks in place into the sparse destination file (Route A).

    Durable progress comes from the file's own extents, so there is nothing to keep in
    sync -- see the module docstring.
    """

    needs_local_file = True

    # 1 MiB balances memory against write granularity; pwrite is durable as soon as the
    # filesystem commits, so there is no un-acknowledged window to bound.
    read_block = READ_BLOCK

    def __init__(self, fd, manifest, chunks, dest):
        self.fd = fd
        self.manifest = manifest
        self.chunks = chunks
        self.dest = dest
        self.use_checkpoints = not manifest.state.get("seek_hole", True)
        self.checkpoint_interval = (
            manifest.state.get("checkpoint_interval") or FALLBACK_CHECKPOINT_INTERVAL
        )
        self._last_checkpoint = {}

    def resume_offset(self, index):
        start, end = self.chunks[index]
        if self.manifest.is_complete(index):
            return end
        if self.use_checkpoints:
            recorded = self.manifest.checkpoint_offset(index)
            offset = min(max(start, recorded), end)
        else:
            offset = chunk_frontier(self.fd, start, end)
            # a frontier must be monotonic and within the chunk; anything else means the
            # derivation cannot be trusted, so rewind to the chunk start
            if not start <= offset <= end:
                log("chunk {}: implausible frontier {}, restarting chunk".format(
                    index, offset))
                offset = start
        self._last_checkpoint[index] = offset
        return offset

    def write(self, index, offset, buf):
        view = memoryview(buf)
        while view:
            written = os.pwrite(self.fd, view, offset)
            if written <= 0:
                raise TransientError("pwrite returned {}".format(written))
            view = view[written:]
            offset += written

        if self.use_checkpoints:
            last = self._last_checkpoint.get(index, 0)
            if offset - last >= self.checkpoint_interval:
                self.manifest.record_checkpoint(index, offset, self.fd)
                self._last_checkpoint[index] = offset

        # None means "the caller's own byte count is authoritative", which it is for a
        # local file: pwrite either wrote the bytes or raised.
        return None

    def chunk_done(self, index):
        self.manifest.record_chunk_done(index, self.fd)

    def sync(self):
        os.fsync(self.fd)


class BucketChunkSink:
    """
    Uploads each chunk as its own GCS object through a resumable upload session, then
    composes them server-side (Route B).

    The VM is a pure relay here: bytes arrive from the source and leave again to GCS
    with no local file at all, which is why this route needs no staging disk and no
    concatenation transfer.

    Resumability is preserved by the same principle as Route A -- ask the storage where
    its durable frontier is rather than trusting a stored counter. A resumable session
    answers that with a 308 plus a Range header, at 256 KiB granularity, so worst-case
    discarded work is under 256 KiB per in-flight chunk. Sessions live 7 days, uploads
    within one must be sequential (which is how chunks are streamed anyway), persisted
    bytes can never be overwritten (so a retry re-sending a committed range is
    harmless), and an incomplete upload is invisible in the bucket -- a preempted part
    leaves no partial object and no ambiguity about what exists.
    """

    needs_local_file = False

    # Deliberately the GCS commit granularity rather than the larger local read block.
    # Bytes that have been read from the source but not yet acknowledged by GCS are lost
    # if the attempt dies, so this is what bounds discarded work to <256 KiB per
    # in-flight part. A bigger block would trade that guarantee for fewer requests.
    read_block = GCS_UPLOAD_GRANULARITY

    def __init__(self, client, bucket, parts_prefix, manifest, chunks):
        self.client = client
        self.bucket = bucket
        self.parts_prefix = parts_prefix
        self.manifest = manifest
        self.chunks = chunks
        self._digests = {}
        self._completed = set()
        self._lock = threading.Lock()

    def part_name(self, index):
        return "{}/{:05d}".format(self.parts_prefix, index)

    def part_names(self):
        return [self.part_name(i) for i in range(len(self.chunks))]

    def _session_for(self, index):
        """
        Get or create this part's session, persisting the URI *before* any bytes are
        sent to it.
        """
        session = self.manifest.session_uri(index)
        if session:
            return session
        with self._lock:
            session = self.manifest.session_uri(index)
            if session:
                return session
            session = self.client.start_resumable_upload(
                self.bucket, self.part_name(index)
            )
            self.manifest.record_session(index, session)
            return session

    def resume_offset(self, index):
        start, end = self.chunks[index]
        if self.manifest.is_complete(index):
            return end

        session = self._session_for(index)
        total = end - start
        try:
            persisted = self.client.session_offset(session, total)
        except PermanentError as e:
            # 404/410 mean the session is gone or expired; only this part restarts
            log("chunk {}: session unusable ({}); starting this part over".format(index, e))
            self.manifest.record_session(index, None)
            return start

        if persisted is None:
            # the session already completed
            self.manifest.record_chunk_done(index, None)
            return end
        # session offsets are relative to the part; the download works in absolute
        # offsets into the source object
        return start + persisted

    def write(self, index, offset, buf):
        start, end = self.chunks[index]
        session = self._session_for(index)

        # Track a running md5 of the part, but only while it stays contiguous from the
        # part's start within this process. A part resumed from a previous attempt
        # cannot be hashed without re-reading it, so the digest is simply unavailable
        # then and verification falls back to the read-back pass.
        with self._lock:
            tracker = self._digests.get(index)
            if tracker is None and offset == start:
                tracker = self._digests[index] = {"md5": hashlib.md5(), "next": start}
            if tracker is not None:
                if tracker["next"] == offset:
                    tracker["md5"].update(buf)
                    tracker["next"] = offset + len(buf)
                else:
                    self._digests[index] = False   # no longer contiguous

        metadata, committed = self.client.upload_range(
            session, buf, offset - start, end - start
        )
        if metadata is not None:
            with self._lock:
                self._completed.add(index)
        # Report GCS's own view of how far it has persisted. The caller must not
        # advance on the local byte count: if a PUT was interrupted, fewer bytes are
        # durable than were sent, and treating the chunk as finished would compose a
        # part that does not exist.
        return start + committed

    def part_digest(self, index):
        """Hex md5 of this part if it was hashed contiguously, else None."""
        tracker = self._digests.get(index)
        if not tracker:
            return None
        start, end = self.chunks[index]
        if tracker["next"] != end:
            return None
        return tracker["md5"].hexdigest()

    def chunk_done(self, index):
        # Only GCS can say a part is finished. If the session never returned a
        # completion, the part is not durable and must not be recorded done -- it would
        # be composed as a missing object.
        if self.manifest.is_complete(index):
            return
        with self._lock:
            completed = index in self._completed
        if not completed:
            raise TransientError(
                "part {} sent all its bytes but the upload session did not "
                "complete".format(index)
            )
        digest = self.part_digest(index)
        if digest:
            self.manifest.record_part_digest(index, digest)
        self.manifest.record_chunk_done(index, None)

    def sync(self):
        return None


class Downloader:
    def __init__(self, source, sink, manifest, chunks, options, progress):
        self.source = source
        self.sink = sink
        self.manifest = manifest
        self.chunks = chunks
        self.options = options
        self.progress = progress
        self.made_progress = False
        self._progress_lock = threading.Lock()

    def resume_offset(self, index):
        return self.sink.resume_offset(index)

    def download_chunk(self, index):
        start, end = self.chunks[index]
        offset = self.resume_offset(index)

        if offset >= end:
            self.sink.chunk_done(index)
            self.progress.add(end - start, resumed=True)
            return

        if offset > start:
            self.progress.add(offset - start, resumed=True)

        attempts = 0
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
                    want = min(self.sink.read_block, end - offset)
                    buf = stream.read(want)
                    if not buf:
                        raise TransientError(
                            "short read at {} ({} bytes short)".format(offset, end - offset)
                        )
                    durable = self.sink.write(index, offset, buf)
                    sent_to = offset + len(buf)
                    self.progress.add(len(buf))
                    with self._progress_lock:
                        self.made_progress = True

                    if durable is None or durable >= sent_to:
                        offset = sent_to
                    else:
                        # The sink persisted less than was sent (an interrupted upload
                        # session). Its answer is authoritative, so rewind to it and
                        # re-open the source there rather than carrying on from a
                        # position the storage never reached.
                        offset = max(offset, durable)
                        raise TransientError(
                            "sink persisted to {} of {} sent".format(durable, sent_to)
                        )
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

        self.sink.chunk_done(index)

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


def gcs_object_md5(metadata):
    """GCS reports md5Hash as base64; convert to hex, or None for a composite object."""
    raw = metadata.get("md5Hash")
    if not raw:
        return None
    try:
        return binascii.hexlify(base64.b64decode(raw)).decode()
    except (binascii.Error, ValueError):
        return None


def verify_bucket_object(client, bucket, name, size, options, block=READ_BUFFER):
    """
    Verify a composed object against the source's declared hash by reading it back.

    This is the expensive case, and it is accepted rather than avoided: check_hash is an
    absolute guarantee, so being unable to verify cheaply forces the read-back rather
    than yielding a silent pass. Same-region GCS-to-GCE egress is free and fast, which
    is what makes it tolerable.

    A composite object has no md5Hash of its own, which is exactly why the stored
    metadata cannot be used here.
    """
    digest = hashlib.md5()
    offset = 0
    while offset < size:
        end = min(offset + block, size)
        payload = client.download_range(bucket, name, offset, end)
        if not payload:
            raise TransientError("read-back returned nothing at {}".format(offset))
        digest.update(payload)
        offset += len(payload)

    actual = digest.hexdigest()
    expected = normalize_expected_md5(options.check_md5)
    if actual != expected:
        raise PermanentError(
            "md5 mismatch after compose: expected {}, got {}".format(expected, actual)
        )
    return actual


def run_bucket_route(options, decision, source, size, chunks, plan_id, manifest_path,
                     marker_path):
    """
    Route B: upload each chunk as its own object through a resumable session, compose
    them server-side, verify, then delete the parts.

    Preferred over stage-then-publish whenever the destination bucket can be addressed,
    because compose transfers no object data -- so there is no sequential tail and the
    entire transfer stays parallel end to end.
    """
    bucket, object_name = split_gs_url(decision.gs_url)
    client = GcsClient(timeout=options.timeout)
    parts_prefix = "{}.k9pdl.parts".format(object_name)

    # The manifest is an object write on this route rather than a tmp+rename, since
    # rename is not atomic on a flat-namespace bucket. It still lands beside the
    # destination so it travels with the data.
    manifest = None if options.no_resume else Manifest.load(manifest_path)
    chunk_size = (chunks[0][1] - chunks[0][0]) if chunks else size
    if manifest is not None and manifest.state.get("plan_id") != plan_id:
        log("manifest describes a different object or layout; restarting")
        manifest.unlink()
        manifest = None
    if manifest is None:
        manifest = Manifest.create(
            manifest_path, plan_id, size, chunk_size, chunks, _FakeStat(size),
            False, 0,
        )
        manifest.state["route"] = ROUTE_BUCKET
    else:
        log("resuming: {}/{} parts already complete".format(
            sum(1 for i in range(len(chunks)) if manifest.is_complete(i)), len(chunks)))

    sink = BucketChunkSink(client, bucket, parts_prefix, manifest, chunks)
    downloader = Downloader(source, sink, manifest, chunks, options, Progress(size))

    try:
        downloader.run()
    except PermanentError as e:
        log("permanent failure: {}".format(e))
        return EXIT_FAIL
    except TransientError as e:
        if downloader.made_progress:
            log("transient failure after progress; requesting requeue: {}".format(e))
            return EXIT_REQUEUE
        log("no forward progress this attempt: {}".format(e))
        return EXIT_FAIL

    # Every part must exist at exactly its planned length before anything is composed.
    # An incomplete resumable upload is invisible in the bucket, so a missing part here
    # means that part never finished rather than that it is partially there.
    part_names = sink.part_names()
    for index, name in enumerate(part_names):
        expected = chunks[index][1] - chunks[index][0]
        try:
            metadata = client.get_object(bucket, name)
        except PermanentError:
            log("part {} is missing after upload; requesting requeue".format(index))
            return EXIT_REQUEUE
        if int(metadata.get("size", -1)) != expected:
            log("part {} is {} bytes, expected {}".format(
                index, metadata.get("size"), expected))
            return EXIT_FAIL
        recorded = manifest.chunk_record(index).get("md5")
        stored = gcs_object_md5(metadata)
        if recorded and stored and recorded != stored:
            log("part {} md5 differs from what GCS stored ({} vs {})".format(
                index, recorded, stored))
            return EXIT_FAIL

    composed = compose_tree(client, bucket, object_name, part_names)
    if int(composed.get("size", -1)) != size:
        log("composed object is {} bytes, expected {}".format(composed.get("size"), size))
        return EXIT_FAIL

    digest = None
    if options.check_md5:
        try:
            digest = verify_bucket_object(client, bucket, object_name, size, options)
        except PermanentError as e:
            log("verification failed: {}".format(e))
            for name in part_names:
                client.delete_object(bucket, name)
            client.delete_object(bucket, object_name)
            manifest.unlink()
            return EXIT_FAIL
    elif options.check_etag:
        log("ETag verification is not available for a composed object; "
            "pass --check-md5 to verify this destination")
        return EXIT_FAIL

    # Parts are deleted explicitly. The GCS JSON API's objects.compose has no
    # deleteSourceObjects parameter (contrary to the design note), so a crash between
    # compose and here leaves orphaned parts; the next run's cleanup below removes them,
    # and they are under a dotted prefix so nothing globs them up meanwhile.
    for name in part_names:
        client.delete_object(bucket, name)

    write_done_marker(marker_path, size, plan_id, digest)
    manifest.unlink()
    log("complete: {} bytes composed from {} parts{}".format(
        size, len(part_names), " (verified)" if digest else ""))
    return EXIT_OK


# --------------------------------------------------------------------------------
# Route C: stage on a real block device, then publish
# --------------------------------------------------------------------------------

# The staged file plus the published copy coexist briefly, and the plan's own disk
# sizing uses a 5% margin, so require the same headroom before committing to a
# work directory.
STAGING_HEADROOM = 1.05

STAGING_SUBDIR = ".k9pdl.staging"


def free_bytes(directory):
    try:
        stats = os.statvfs(directory)
    except OSError:
        return 0
    return stats.f_bavail * stats.f_frsize


def work_directory_candidates(options):
    """
    Where a staged copy could live, in preference order.

    The localization persistent disk comes first because it is the ONLY candidate whose
    staged file survives a preemption -- CANINE_LOCAL_DISK_DIR and TMPDIR are on the
    ephemeral local disk, which dies with the VM, so a staged file there means a clean
    restart from zero rather than a resume. The handler passes the PD explicitly with
    --work-dir when localize_to_persistent_disk is set.
    """
    candidates = list(options.work_dir or [])
    for variable in ("CANINE_LOCAL_DISK_DIR", "TMPDIR"):
        value = os.environ.get(variable)
        if value:
            candidates.append(value)
    candidates.append(tempfile.gettempdir())

    seen = set()
    ordered = []
    for candidate in candidates:
        resolved = os.path.abspath(candidate)
        if resolved not in seen:
            seen.add(resolved)
            ordered.append(resolved)
    return ordered


def select_work_directory(options, size):
    """
    Pick a staging directory that is a real POSIX filesystem with room for the object,
    or None.

    Never silently proceed with a directory that cannot hold the file: running out of
    space mid-publish would leave a truncated object at the destination, which is worse
    than declining the route.
    """
    needed = int(size * STAGING_HEADROOM)
    for candidate in work_directory_candidates(options):
        staging = os.path.join(candidate, STAGING_SUBDIR)
        try:
            os.makedirs(staging, exist_ok=True)
        except OSError as e:
            log("staging candidate {} is unusable: {}".format(candidate, e))
            continue

        # the staging area has to be a real block device, not another FUSE mount
        decision = select_route(os.path.join(staging, "probe"))
        if decision.route != ROUTE_POSIX:
            log("staging candidate {} is not a POSIX filesystem ({})".format(
                candidate, decision.reason))
            continue

        available = free_bytes(staging)
        if available < needed:
            log("staging candidate {} has {} bytes free, needs {}".format(
                candidate, available, needed))
            continue

        return staging, decision
    return None, None


def publish_staged_file(staged, dest, size):
    """
    Copy the staged file to the destination with one sequential streaming write.

    Two deliberate choices for a FUSE object store:

      * No temp name and rename. rename is not atomic on a flat-namespace bucket, so a
        publish-then-rename would have a window where the destination is neither the old
        nor the new object.
      * The object is finalized by close(), not by fsync -- with gcsfuse nothing is
        durable until the handle closes, so the copy must run to completion inside one
        open handle and the size can only be re-checked afterwards.

    A single sequential write is also the only pattern that reaches gcsfuse's
    streaming-write path; anything else stages the whole file locally again and
    re-uploads it.
    """
    directory = os.path.dirname(os.path.abspath(dest))
    if directory and not os.path.isdir(directory):
        os.makedirs(directory, exist_ok=True)

    copied = 0
    with open(staged, "rb") as source_file:
        with open(dest, "wb") as destination_file:
            while True:
                block = source_file.read(READ_BUFFER)
                if not block:
                    break
                destination_file.write(block)
                copied += len(block)
        # the `with` above closed the destination, which is what finalizes the object

    if copied != size:
        raise TransientError(
            "published {} bytes, expected {}".format(copied, size)
        )
    return copied


def run_staged_route(options, decision, source, size, chunks, chunk_size, plan_id,
                     marker_path):
    """
    Route C: chunk-download onto a real block device, verify there, then publish with a
    single sequential copy.

    The generic fallback for a non-POSIX destination that is not a resolvable bucket.
    Strictly worse than Route B -- it pays a full sequential publish and puts up to a
    whole file of staged work at risk -- which is why Route B is preferred whenever the
    destination can be addressed directly.
    """
    dest = options.dest
    staging, _ = select_work_directory(options, size)
    if staging is None:
        # Declining is correct rather than defeatist: a single sequential curl is the
        # only safe pattern left, and for a FUSE object store it is also the only one
        # that avoids read-modify-write and a full-object re-upload.
        return single_stream_fallback(
            options, "no staging directory has room for {} bytes".format(size)
        )

    staged = os.path.join(staging, os.path.basename(dest))
    staged_manifest, staged_marker = sidecar_paths(staged)
    log("staging via {}".format(staged))

    # A staged file that has already been verified can be published again without
    # re-downloading anything -- which is what makes a preemption during the publish
    # cost only the publish.
    verified = read_done_marker(staged_marker)
    digest = None
    if (verified and verified.get("plan_id") == plan_id
            and verified.get("size") == size and os.path.exists(staged)
            and os.path.getsize(staged) == size):
        log("staged copy is already verified; re-publishing without re-downloading")
        digest = verified.get("hash")
    else:
        status, manifest = download_to_local_file(
            options, source, size, chunks, chunk_size, plan_id, staged,
            staged_manifest, probe_seek_hole(staging),
        )
        if status != EXIT_OK:
            return status

        # Verify BEFORE publishing, so a corrupt download never costs an upload.
        try:
            digest = verify(staged, options)
        except PermanentError as e:
            log("verification failed on the staged copy: {}".format(e))
            discard(staged, manifest)
            return EXIT_FAIL

        write_done_marker(staged_marker, size, plan_id, digest)
        if manifest is not None:
            manifest.unlink()

    try:
        publish_staged_file(staged, dest, size)
    except (TransientError, IOError, OSError) as e:
        log("publish failed: {}; the staged copy is kept for the next attempt".format(e))
        return EXIT_REQUEUE

    # Re-check after close(), because that is the point at which the object exists.
    try:
        published = os.path.getsize(dest)
    except OSError as e:
        log("could not stat the published object: {}".format(e))
        return EXIT_REQUEUE
    if published != size:
        log("published object is {} bytes, expected {}".format(published, size))
        return EXIT_REQUEUE

    write_done_marker(marker_path, size, plan_id, digest)

    for path in (staged, staged_marker):
        try:
            os.unlink(path)
        except OSError:
            pass

    log("complete: {} bytes published from {}{}".format(
        size, staged, " (verified)" if digest else ""))
    return EXIT_OK


class _FakeStat:
    """
    Stands in for os.stat on a route with no local file, so the manifest's identity
    fields stay populated without pretending an inode exists.
    """

    def __init__(self, size):
        self.st_dev = 0
        self.st_ino = 0
        self.st_size = size


# --------------------------------------------------------------------------------
# single-stream fallback
# --------------------------------------------------------------------------------

def clear_preallocated_working_file(dest):
    """
    Remove a sparse working file before handing over to a single-stream command.

    This is not tidiness, it prevents silent corruption. The legacy commands resume from
    the destination's own size -- `curl -C -`, and `aws s3api --range "bytes=$SZ-"` before
    it was removed -- which is only meaningful for a file a single stream appended to. Our
    working file is created at its full apparent size upfront, so leaving it in place makes
    curl report "already fully downloaded", exit 0, and accept a file of zeros. Verified:
    curl really does exit 0 there, so nothing downstream notices unless check_hash is set.

    The manifest is what identifies the file as ours. A genuine partial left by a previous
    single-stream attempt has no manifest, and its progress must be preserved -- that is
    the case `curl -C -` exists to handle.
    """
    manifest_path, _ = sidecar_paths(dest)
    if not os.path.exists(manifest_path):
        return
    log("discarding the preallocated working file so the single stream starts clean")
    for path in (dest, manifest_path):
        try:
            os.unlink(path)
        except OSError:
            pass


def single_stream_fallback(options, reason):
    """
    Everything the chunked path declines to handle falls back to the legacy command, which
    is exactly today's behavior and today's resume semantics.
    """
    log("falling back to a single stream: {}".format(reason))
    clear_preallocated_working_file(options.dest)
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

    if urllib.parse.urlsplit((options.url or "").strip()).scheme == "ftp":
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

    # Gate on what the destination filesystem can actually do before writing anything,
    # so no code below assumes dest is POSIX.
    decision = select_route(dest)
    log("route {}: {}".format(decision.route, decision.reason))

    chunks = plan_chunks(size, options.connections, options.min_chunk,
                         options.part_length)
    chunk_size = (chunks[0][1] - chunks[0][0]) if chunks else size
    plan_id = compute_plan_id(
        options.url or "", size, options.check_etag or options.check_md5, chunk_size
    )

    if decision.route == ROUTE_BUCKET:
        return run_bucket_route(options, decision, source, size, chunks, plan_id,
                                manifest_path, marker_path)

    if decision.route == ROUTE_STAGED:
        return run_staged_route(options, decision, source, size, chunks, chunk_size,
                                plan_id, marker_path)

    if decision.route != ROUTE_POSIX:
        return single_stream_fallback(
            options,
            "unhandled route {} ({})".format(decision.route, decision.reason),
        )

    seek_hole = decision.seek_hole
    if not seek_hole:
        log("SEEK_HOLE unsupported here; checkpointing instead of frontier recovery")

    status, manifest = download_to_local_file(
        options, source, size, chunks, chunk_size, plan_id, dest, manifest_path,
        seek_hole,
    )
    if status != EXIT_OK:
        return status

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


def download_to_local_file(options, source, size, chunks, chunk_size, plan_id, target,
                           manifest_path, seek_hole):
    """
    Download into a local POSIX file, in place and resumably.

    Shared by Route A (where the target is the destination) and Route C (where it is a
    staging file on a real block device). Returns `(exit_code, manifest)`; verification,
    publishing and the done marker are the caller's business, because they differ
    between the two.
    """
    fd = open_destination(target, size)
    manifest = None
    try:
        target_stat = os.fstat(fd)

        manifest = None if options.no_resume else Manifest.load(manifest_path)
        if manifest is not None and not manifest.matches(plan_id, target_stat, size):
            log("manifest describes a different object or layout; restarting")
            manifest.unlink()
            manifest = None
            os.ftruncate(fd, 0)
            os.ftruncate(fd, size)
            target_stat = os.fstat(fd)

        if manifest is None:
            manifest = Manifest.create(
                manifest_path, plan_id, size, chunk_size, chunks, target_stat,
                seek_hole, 0 if seek_hole else checkpoint_interval_for(chunk_size),
            )
        else:
            log("resuming: {}/{} chunks already complete".format(
                sum(1 for i in range(len(chunks)) if manifest.is_complete(i)),
                len(chunks)))

        try_lock(fd)

        sink = PosixChunkSink(fd, manifest, chunks, target)
        downloader = Downloader(source, sink, manifest, chunks, options, Progress(size))
        try:
            downloader.run()
        except PermanentError as e:
            log("permanent failure: {}".format(e))
            return EXIT_FAIL, manifest
        except RangeNotSupported as e:
            os.close(fd)
            fd = None
            return single_stream_fallback(options, str(e)), manifest
        except TransientError as e:
            # forward progress decides requeue-vs-fail: returning 5 unconditionally
            # would let a download that can never progress requeue forever, and exit-5
            # requeues are deliberately excluded from the preemption limit
            if downloader.made_progress:
                log("transient failure after progress; requesting requeue: {}".format(e))
                return EXIT_REQUEUE, manifest
            log("no forward progress this attempt: {}".format(e))
            return EXIT_FAIL, manifest

        os.fsync(fd)
        actual_size = os.fstat(fd).st_size
        if actual_size != size:
            log("size mismatch after download: {} != {}".format(actual_size, size))
            return EXIT_FAIL, manifest
    finally:
        if fd is not None:
            os.close(fd)

    return EXIT_OK, manifest


def discard(dest, manifest):
    try:
        os.unlink(dest)
    except OSError:
        pass
    if manifest is not None:
        manifest.unlink()


def build_source(options):
    """
    Pick where bytes come from.

    A URL wins when one is present, because the presigned-URL path is one code path for
    every http source and costs no `aws` process per chunk. The S3 API source is the
    fallback for when presigning is not possible (session-token-only credentials, an
    exotic endpoint): the emitted command passes an empty --url in that case, so an empty
    string here means "presign produced nothing", not "no source given".
    """
    if not (options.url or "").strip() and options.s3_bucket and options.s3_key:
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
    parser.add_argument("--work-dir", action="append", default=[], dest="work_dir",
                        help="preferred staging directory for a non-POSIX destination "
                             "(repeatable, tried in order)")
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

    if not (options.url or "").strip() and not (options.s3_bucket and options.s3_key):
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
