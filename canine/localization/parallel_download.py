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

The manifest kept alongside the file is bookkeeping, not the source of truth, but its
`done` markers still obey the same rule the frontier does: a chunk may only be recorded
complete after an fsync that covers that chunk's bytes. Enforcing that per chunk cost
three fsyncs and a rename each, serialised, and consumed 70% of the worker pool on a
3283-chunk transfer. So the commits are batched onto a single writer thread: workers hand
off and go back to reading, and the writer snapshots whatever has piled up, fsyncs once,
and records exactly that snapshot. A chunk that finishes *during* that fsync is not
covered by it, is not in the snapshot, and waits for the next round. The guarantee is
unchanged; only the number of commits is, and it no longer scales with the chunk count.

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
import collections
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
import zlib
from concurrent.futures import ThreadPoolExecutor

SCHEMA_VERSION = 1

# Tuning defaults for an n1-standard-8 worker, which localization owns exclusively
# (LocalizeToDisk pins cpus-per-task=8 and passes --exclusive). A single TCP stream to
# S3/GDC realistically gets 50-200 MB/s against a ~2 GB/s egress cap, which is the
# whole reason for fanning out.
#
# 16, not 8. The original 8 was one per vCPU, reasoned rather than measured, and the
# streams are IO-blocked rather than CPU-bound so vCPU count was never the right unit.
# Measured against the real GDC source (BENCHMARK_RUNBOOK.md §6.1/§6.2): throughput is
# linear in connections to 16 with no knee against the source -- 16.42 MiB/s at 1
# connection, 227.18 at 16, a 13.85x gain -- because the limit is per-connection, not
# per-signature or per-endpoint. 16 is where the *destination* saturates, not the source,
# so it is a floor on what a faster destination could use rather than a ceiling on the
# source. It is also MAX_CONNECTIONS, so the default is now the cap.
#
# On the LocalizeToDisk path this changes little: §6.5i measured that disk at 43.9 MiB/s
# sustained, which 8 connections already exceed. The gain is on fast destinations -- NFS,
# tmpfs, and the bucket route -- where the source, not the disk, is what is being asked
# for more.
DEFAULT_CONNECTIONS = 16
MAX_CONNECTIONS = 16

# Ranged GETs in flight while reading the compressed sidecar back for --gunzip.
#
# 4, measured. A single sequential stream reads a never-read object at 59.3 MB/s median
# (42.9-72.2 over five reps); depth 4 reads it at 140.3 (129.3-147.7). The ranges do not
# overlap, and the depth order was rotated across reps after a first attempt put depth 1
# first every time and so charged it with whatever penalty running first carries -- the
# by-slot medians came out flat, which is what says the effect is depth and not order.
#
# More is not better and the curve is not monotonic: depth 8 measures 105.2 and depth 16
# 110.6, both well under depth 4, in two independent sweeps. Peak memory is
# DECODE_READAHEAD * READ_BUFFER = 32 MiB.
#
# This parallelises only the FETCH. Inflation stays strictly sequential because gzip
# gives no choice -- which is the same observation that made the post-compose design
# possible at all, applied one layer down.
DEFAULT_DECODE_READAHEAD = 4

# Concurrent resumable sessions used to write the decoded object back, and the slice
# each one carries.
#
# 4, and deliberately not 8. Measured on 1.64 GB: one session 81.3 MB/s, width 2 159.4,
# width 4 301.6, width 8 541.6 -- near-linear, no ceiling in sight. But width 4 already
# puts the upload (301 MB/s) well under the inflater's 155 MB/s, and gzip inflation
# cannot be parallelised, so past 4 the decode stops getting faster and only gets
# hungrier. Peak buffered bytes are (width + 1) * DECODE_SLICE_BYTES = 160 MiB.
#
# The measurement that matters for the default is which stage is slowest, not which
# number is biggest.
DEFAULT_DECODE_UPLOAD_WIDTH = 4

# A slice is buffered whole before it is sent, which is what lets every PUT in it carry
# a real total -- the unknown-total protocol is only needed when the length is not known
# in advance, and by the time a slice is cut it is. 32 MiB keeps the slice count for a
# 1.6 GB object at 51, and a multiple of the commit granularity keeps the arithmetic
# honest if a slice ever has to be sent in pieces.
DECODE_SLICE_BYTES = 32 * 1024 * 1024

# Assembled slices allowed to wait in the upload pool's queue beyond the `width` that
# are actively uploading. This is what decouples the inflater from the uploader: with
# none, it blocks on every slice past the first `width` and the decode measures as
# inflate plus write with no overlap -- 58.8s + 55.1s against a 114.1s decode at a VCF
# compression ratio, where the two stages are within 6% of each other and the overlap
# ceiling is 1.94x.
#
# Measured at a VCF ratio: queue 0 gives 102.2/102.9 s, queue 8 gives 99.2/97.2, queue 16
# gives 98.7 -- a consistent ~4% with no overlap between the groups. Small, but real and
# cheap.
#
# 8 rather than 16 because memory is (width + this) * DECODE_SLICE_BYTES, so 8 costs
# 384 MiB against 16's 640 MiB for the same 4%. That is arithmetic on the buffer sizes,
# not a measured RSS. Localization runs on n1-standard-8 (wolF's LocalizeToDisk asks for
# it explicitly), where 384 MiB is unremarkable next to the relay's measured 397 MiB
# peak; set it to 0 on anything smaller.
DEFAULT_DECODE_QUEUE_SLICES = 8
DEFAULT_MIN_CHUNK = 64 * 1024 * 1024

# The chunk size scales with the object: size / TARGET_CHUNKS, clamped to
# [min_chunk, MAX_CHUNK]. Every request pays a fixed cost before its first byte -- measured
# at 1.1-1.7 s on the GDC API idle and ~2.6 s under load, 0.3 s on S3 -- so fewer, larger
# requests win on a large object, while a small one still needs enough chunks to keep every
# connection busy. Fixed at 64 MiB the GDC API ran at 67% of its best on a 325 GiB object;
# this rule stays within 88% at every size from 1 to 325 GiB on both sources (§13.75).
#
# 32 is nearly gcloud's composite-UPLOAD rule (50 MiB components, at most 32 of them). The
# cap is what differs, and what matters: a large object stays a QUEUE of many chunks (325
# for a 325 GiB BAM, not 32 of 10 GiB), and the queue is what absorbs a straggling
# connection. gcloud's download shape -- one slice per thread -- has no queue at all.
#
# Chunk size may depend on the object's size and nothing else; see plan_chunks.
TARGET_CHUNKS = 32
MAX_CHUNK = 1024 * 1024 * 1024

# Chunk boundaries are aligned to this so that every chunk start is also block-aligned
# on any plausible filesystem. Without it, adjacent chunks share a partial block and
# the frontier arithmetic at chunk boundaries stops being sound.
CHUNK_ALIGN = 1024 * 1024

READ_BUFFER = 8 * 1024 * 1024

# Block size for the bucket route's md5 read-back (verify_bucket_object), at the same
# read-ahead depth as the decode. Measured on cold composites (§13.78): 8 MiB read at
# 118-126 MiB/s, 64 MiB at 234-276. Every ranged GET opens a new TLS connection and ramps
# TCP from zero, ~0.15 s whatever it carries, so a bigger block amortizes it; more depth
# does not (beyond 4 the rate falls to a 150-175 MiB/s plateau).
#
# Fixed, not scaled with the object like the write chunks: the rate depends on the block,
# not the object's size, and each block is held whole in memory, so peak memory is
# depth x block (256 MiB here). Scaling to 1 GiB would buy ~15% for 4 GiB in flight.
# The decode keeps READ_BUFFER: its consumer is a gzip inflater, measured separately.
VERIFY_READBACK_BLOCK = 64 * 1024 * 1024

# Readers used for the verification read-back.
#
# Two competing effects, both measured, and the optimum is where they cross:
#
#   * aggregate read throughput FALLS as readers are added -- 86 / 85 / 76 / 62 MiB/s at
#     1 / 2 / 4 / 8 on a 316 GB pd-standard -- because the device is fixed-bandwidth and
#     extra streams only add interleaving;
#   * but each worker reads and hashes SERIALLY, so one worker leaves the disk idle while
#     it hashes. At md5's measured 700 MiB/s a lone worker achieves
#     1/(1/86 + 1/700) = 77 MiB/s, not 86.
#
# Overlapping the two recovers most of that: N=2 models at 80 MiB/s, ~5% better than one
# worker, and it is the peak -- N=4 falls to 74 and N=8 to 61 as the read penalty
# overtakes the overlap. Emphatically not `connections`, which at the default of 8 made
# the read-back of a 279 GiB object 21 minutes slower than a single reader.
#
# On a destination fast enough that hashing rather than IO binds -- tmpfs, local SSD --
# the balance shifts and more workers would scale nearly linearly to the core count. This
# constant is tuned for a persistent disk, which is where LocalizeToDisk writes.
VERIFY_READ_WORKERS = 2

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

# Every command this script shells out to -- the legacy fallback, the URL refresh, the
# per-chunk aws call -- comes from a bash script and is written in bash. subprocess's
# shell=True uses /bin/sh, which on the Ubuntu worker image is dash: process substitution
# `>(...)`, `[[ ... ]]` and other constructs those commands rely on are syntax errors
# there. So bash is named explicitly rather than inherited.
SHELL = "/bin/bash"

# Exit codes are interpreted by canine's entrypoint: 5 means requeue-and-resume, 15
# means skip the job, and ANY other nonzero value is treated as do-not-retry. So a
# transient failure must never escape as an arbitrary nonzero code.
EXIT_OK = 0
EXIT_FAIL = 1
EXIT_REQUEUE = 5

def fetch_or_exit(fetch):
    """
    Wrap a single-stream fetch so that any failure exits localization explicitly, with a
    code canine reads as an ordinary failure.

    An ordinary failure is the right outcome even for a transient drop: wolF retries a
    failed task (`retry`, 3 by default), and the retry resumes from the partial file
    (`curl -C -`, the append-resume), so it is a bounded retry that loses nothing. Exit 5
    would instead be an UNCAPPED requeue, safe only behind a forward-progress gate.

    Explicit, because `set -e` does not apply inside an `&&` list: a failed fetch in
    `FETCH && verify && decode` let localization.sh carry on, and the task ran without its
    input (measured). Normalized, because canine reads 5 as requeue and 15 as SKIP, and
    curl uses both (5 couldn't resolve proxy, 15 an FTP host failure): passed through, one
    could requeue forever and the other would mark a job done without running it.

    One line, so it can travel in --legacy-cmd.
    """
    return (
        "{{ _k9r=0; {{ {fetch}; }} || _k9r=$?; if [ $_k9r -ne 0 ]; then "
        "echo \"download failed (exit $_k9r); the partial file is kept for the retry to "
        "resume\" >&2; case $_k9r in {requeue}|15) exit {fail};; *) exit $_k9r;; esac; fi; }}"
    ).format(fetch=fetch, requeue=EXIT_REQUEUE, fail=EXIT_FAIL)


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


def chunk_size_for(size, min_chunk=DEFAULT_MIN_CHUNK, max_chunk=MAX_CHUNK):
    """
    The size-scaled chunk: size / TARGET_CHUNKS, clamped to [min_chunk, max_chunk].

    `min_chunk` wins if the two bounds cross, so a caller asking for bigger chunks than the
    cap still gets them. `max_chunk == min_chunk` pins a fixed size, which is how the old
    fixed-64 MiB layout is reproduced for comparison.
    """
    target = -(-int(size) // TARGET_CHUNKS) if size > 0 else 0
    return max(max(1, int(min_chunk)), min(int(max_chunk), target))


def plan_chunks(size, connections, min_chunk, part_length=None, max_chunk=MAX_CHUNK):
    """
    Split `size` into even-sized chunks (with a possibly-shorter tail).

    The chunk size scales with the object (chunk_size_for): size / TARGET_CHUNKS, clamped
    to [min_chunk, max_chunk]. So an object under 2 GiB keeps 64 MiB chunks, and larger
    ones grow toward 1 GiB.

    `connections` is accepted but deliberately IGNORED for the layout, which depends
    only on `size`, `min_chunk`, `max_chunk` and `part_length`. This is a hard requirement, not a
    simplification: `plan_id` is derived from the chunk size, so if the layout moved
    with the connection count then a requeued task landing on a differently-configured
    node would compute a different `plan_id`, discard a perfectly good partial file,
    and re-download the object from scratch. `connections` governs only how many chunks
    are in flight at once (see `Downloader.run`).

    This diverges from the original design sketch, which had
    `n_chunks = clamp(ceil(size / min_chunk), 1, connections)` -- that formula makes the
    layout connection-dependent and contradicts the resumability requirement it appears
    alongside. Scaling with the SIZE instead keeps the layout stable and still yields a
    queue much longer than the connection count (a 50 GB object becomes 47 x 1 GiB rather
    than 8 x 6.4 GB), so the worker pool load-balances instead of each thread owning one
    huge range. Resume granularity does not depend on the chunk size at all: both routes
    resume inside a chunk. What larger chunks buy is fewer requests, each paying a fixed
    wait before its first byte; that is the whole reason they grow (see TARGET_CHUNKS).

    When `part_length` is given (an S3 multipart object whose ETag is an md5-of-md5s),
    boundaries snap to whole parts so the ETag can be computed incrementally during the
    download instead of by a second full read afterwards.
    """
    if size < 0:
        return []
    if size == 0:
        return [(0, 0)]

    target = chunk_size_for(size, min_chunk, max_chunk)

    if part_length:
        # every chunk must span whole parts, so snap up to a multiple of part_length
        chunk_size = max(part_length, align_up(target, part_length))
    else:
        chunk_size = align_up(target, CHUNK_ALIGN)

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

# Named rather than lettered: these strings appear in logs, in manifests and in the
# emitted scripts, where a bare "route B" told an operator nothing about what was
# happening. Each name says what the route does with the bytes.
ROUTE_POSIX = "in-place"          # write chunks straight into dest (dominant PD/NFS path)
ROUTE_BUCKET = "bucket-compose"   # upload parts, compose server-side (bucket-backed dest)
ROUTE_STAGED = "stage-publish"    # stage on a real block device, then publish


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
        """
        Mark a chunk complete, MERGING into whatever is already recorded for it.

        This used to replace the record outright, which quietly destroyed the two other
        things kept per chunk: the resumable-upload session URI, and the per-part md5
        written moments earlier by record_part_digest. The visible consequence was that
        the bucket-compose route's pre-compose check -- comparing each part's md5 against
        what GCS reports for it, the cheap verification that avoids a full read-back -- read
        None every time and so never actually compared anything.
        """
        with self._lock:
            record = self.state.setdefault("chunks", {}).setdefault(str(index), {})
            record["done"] = True
            if digest is not None:
                record["md5"] = digest
            self.flush(data_fd)

    def record_chunks_done(self, indices, data_fd, chunk_digests=None, part_md5s=None):
        """
        Commit a BATCH of completed chunks with a single data fsync and a single manifest
        write.

        Same guarantee as record_chunk_done, amortised: flush() fsyncs `data_fd` before it
        writes anything, so every byte of every chunk named here is durable before any of
        their done markers are. The batch may only contain chunks whose writes had already
        finished when the batch was assembled -- a chunk that completes DURING this fsync
        is not covered by it and must wait for the next one.

        This exists because the per-chunk version costs three fsyncs and a rename each, all
        under _lock, and that is the only work that grows with the chunk count rather than
        the byte count: 683 of 982 worker-seconds on the 3283-chunk run, against 64 chunks
        at 4 GiB. Batching does not make an individual commit cheaper; it makes the number
        of commits independent of the number of chunks.
        """
        if not indices and not part_md5s:
            return
        with self._lock:
            chunks = self.state.setdefault("chunks", {})
            for index in indices:
                record = chunks.setdefault(str(index), {})
                record["done"] = True
                if chunk_digests and chunk_digests.get(index) is not None:
                    record["md5"] = chunk_digests[index]
            if part_md5s:
                parts = self.state.setdefault("part_md5", {})
                for index, md5_hex in part_md5s.items():
                    parts[str(index)] = md5_hex
            self.flush(data_fd)

    def record_checkpoint(self, index, offset, data_fd):
        """Only used on filesystems that failed the SEEK_HOLE probe."""
        with self._lock:
            record = self.state.setdefault("chunks", {}).setdefault(str(index), {})
            record["offset"] = offset
            self.flush(data_fd)

    def record_session(self, index, session_uri):
        """
        Persist a resumable upload session URI (the bucket-compose route).

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

    # ---- S3 part digests, for reproducing a multipart ETag without a read-back -------
    #
    # A separate keyspace from `chunks`, deliberately. On the bucket-compose route one
    # chunk IS one uploaded part, so record_part_digest above can key by chunk index. On
    # the in-place route a chunk spans several S3 parts (87 MiB of 29 MiB parts, for the
    # object this was built for), so part digests need their own index. Sharing the
    # `chunks` dict would conflate two different things that happen to both be called
    # "part".

    def record_part_md5s(self, digests, data_fd):
        """
        Persist finished S3-part digests, committed with the same data-before-metadata
        discipline as everything else: flush() fsyncs `data_fd` first, so a digest can
        never be durable while the bytes it describes are not.

        Called at chunk completion rather than per part -- one manifest write per chunk,
        the cadence that already exists, instead of one per 29 MiB.
        """
        if not digests:
            return
        with self._lock:
            parts = self.state.setdefault("part_md5", {})
            for index, md5_hex in digests.items():
                parts[str(index)] = md5_hex
            self.flush(data_fd)

    def part_md5(self, index):
        return self.state.get("part_md5", {}).get(str(index))

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


class GcsManifest(Manifest):
    """
    A manifest stored as a GCS object rather than through the filesystem.

    The bucket-compose route exists for a destination that is a bucket, and on a
    flat-namespace bucket the base class's commit is not atomic: it writes a temp file and
    renames, but rename there is a server-side copy followed by a delete. A torn manifest is
    not a correctness problem -- it fails to parse, loads as None, and forces a clean
    restart -- but it costs every part being re-uploaded, and the bucket-compose route is
    precisely the case where that filesystem does not behave, so it should not be relying on
    it.

    A single media upload gives the property directly: the object appears whole or not at
    all. There is no temp name, no rename, and no fsync (durability is the service's
    problem once it acknowledges the write).

    Only the persistence is overridden; all the record-keeping -- chunk completion, session
    URIs, per-part digests -- is inherited, so the two manifests cannot drift in what they
    record.
    """

    def __init__(self, client, bucket, name, state):
        # `path` is kept for the inherited logging/identity, but is never opened
        super().__init__(name, state)
        self.client = client
        self.bucket = bucket
        self.name = name

    @property
    def tmp_path(self):
        raise AssertionError("a GCS manifest is written in one request, with no staging")

    @classmethod
    def load(cls, client, bucket, name):
        try:
            body = client.read_object(bucket, name)
        except TransientError as e:
            # Failing to READ the resume state costs re-uploading parts, never
            # correctness -- the same rule flush() follows for failing to write it. The
            # asymmetry matters because this call happens before the downloader's own
            # error handling, so raising here would escape as an "unexpected failure" and
            # exit do-not-retry, turning a GCS blip into a permanently failed job. If the
            # service is genuinely unreachable the first upload fails a moment later, with
            # progress correctly accounted for.
            log("could not read the manifest ({}); starting fresh".format(e))
            return None
        if not body:
            return None
        try:
            state = json.loads(body.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            # A partial object should be impossible here, unlike the filesystem case, but
            # treat unparseable exactly the same: no usable resume state.
            return None
        if not isinstance(state, dict) or state.get("schema_version") != SCHEMA_VERSION:
            return None
        return cls(client, bucket, name, state)

    @classmethod
    def create(cls, client, bucket, name, plan_id, size, chunk_size, chunks):
        state = {
            "schema_version": SCHEMA_VERSION,
            "plan_id": plan_id,
            "size": size,
            "chunk_size": chunk_size,
            "n_chunks": len(chunks),
            "route": ROUTE_BUCKET,
            # no dest inode/size: there is no local file on this route, and the object's
            # own existence is checked before compose instead
            "dest": {},
            "seek_hole": False,
            "checkpoint_interval": 0,
            "chunks": {},
            "writer": {
                "hostname": _hostname(),
                "pid": os.getpid(),
                "boot_id": _boot_id(),
            },
        }
        manifest = cls(client, bucket, name, state)
        manifest.flush(data_fd=None)
        return manifest

    def matches(self, plan_id, dest_stat, size):
        # Only the plan identity is meaningful without a local file.
        return (self.state.get("plan_id") == plan_id
                and self.state.get("size") == size)

    def flush(self, data_fd):
        # Losing a manifest update costs re-uploading parts, never correctness, so a
        # failure here must not fail the transfer -- same rule as the filesystem manifest,
        # but the errors are the client's rather than OSError.
        try:
            self._flush(data_fd)
        except (TransientError, PermanentError, OSError) as e:
            log("could not update the manifest ({}); parts may be re-uploaded".format(e))

    def _flush(self, data_fd):
        self.client.put_object(
            self.bucket, self.name, json.dumps(self.state).encode("utf-8")
        )

    def unlink(self):
        try:
            self.client.delete_object(self.bucket, self.name)
        except (TransientError, PermanentError):
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

def write_done_marker(path, size, plan_id, digest, route=ROUTE_POSIX, gs_url=None,
                      stored_size=None):
    """
    `route` is recorded because the marker is checked before the route is chosen, and
    `gs_url` so that the bucket route's completion can be checked at all.

    `size` is the transferred (source) length, which is what identifies the plan. With
    --gunzip that is NOT what ends up on the destination, so `stored_size` records the
    decompressed length separately. Without it the falsifiability check below compares
    a decompressed file against a compressed length, always disagrees, and re-transfers
    on every run -- a marker that can never be believed is the same as no marker.

    Every route's marker has to be falsifiable. A marker is a hidden sidecar, so
    anything that removes the payload without sweeping dotfiles leaves it behind
    claiming something that is gone -- and a marker believed without evidence reports
    success having transferred nothing.

    The local routes check `os.path.getsize(dest)`. bucket-compose has no local file, and
    that was originally read as "nothing to check", which made it the one route where a
    stale marker was undetectable. Observed in practice: `route-check.bin` deleted from a
    bucket, `.route-check.bin.k9pdl.done` left behind, and the next run would have
    returned EXIT_OK in milliseconds. Recording the URL makes the object itself
    checkable, which is the same evidence the local routes use.

    An absent field in an older marker is read as ROUTE_POSIX, which is the safe
    direction: a mis-applied presence check costs a redundant transfer, a skipped one
    reports success for data that is not there.
    """
    payload = {
        "schema_version": SCHEMA_VERSION,
        "size": size,
        "plan_id": plan_id,
        "hash": digest,
        "route": route,
    }
    if gs_url:
        payload["gs_url"] = gs_url
    if stored_size is not None and stored_size != size:
        payload["stored_size"] = stored_size
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


class IntegrityError(PermanentError):
    """
    The bytes are WRONG -- a digest mismatch, a corrupt gzip stream -- as opposed to
    missing or unreachable.

    A subclass rather than a message convention because the bucket route has to tell
    the two apart and act oppositely on them. Wrong bytes mean the source itself is bad:
    every writer is doomed alike, so clean up and stop (EXIT_FAIL). A 404 on an object
    this attempt created means another writer cleaned it up, which is the signature of
    losing a race: the right answer is to check whether that writer's object landed. The
    verify and decode handlers used to catch bare PermanentError and treat a 404 on a
    vanished sidecar as corruption -- failing, do-not-retry, a job whose output another
    writer had just produced.
    """


_CONTENT_RANGE = re.compile(r"^(?:bytes\s*[=:]?\s*)?(\d+)-(\d+)/(\d+|\*)$", re.IGNORECASE)


def parse_content_range(value):
    """
    Parse a satisfied-range `Content-Range` into (first, last, total), or None.

    `total` is None for `*`. The unit is optional. RFC 9110 requires `bytes 0-0/N`, but
    the GDC API sends `0-0/348693812393`, with no unit, on every 206 (measured). The
    range itself is exact, so rejecting it cost nothing in safety and everything in
    speed: every chunk failed its check and the whole download fell to a single stream.
    `bytes=` and `bytes:` are accepted for the same reason, and are no more ambiguous.
    """
    match = _CONTENT_RANGE.match((value or "").strip())
    if not match:
        return None
    first, last, total = match.groups()
    return int(first), int(last), (None if total == "*" else int(total))


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
                    self.url_refresh_cmd, shell=True, executable=SHELL,
                    capture_output=True, timeout=120,
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
            parsed = parse_content_range(content_range)
            if parsed is None or parsed[:2] != (0, 0):
                raise RangeNotSupported(
                    "unexpected Content-Range {!r}".format(content_range)
                )
            declared = parsed[2]
            if declared is not None and size is not None and declared != size:
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
        parsed = parse_content_range(content_range)
        if parsed is None or parsed[:2] != (start, end - 1):
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
    exotic endpoint).

    It costs one `aws` process per chunk *attempt* -- but only `connections` of them run
    at a time, and each overlaps a whole chunk's transfer, so the cost is a fraction of
    the run rather than a serial pile of startups. For a 279 GiB object at 87 MiB chunks
    that is 3283 invocations; at a plausible 1 s of Python startup and 8 connections,
    about 7 minutes spread across the download. Real, worth avoiding, not decisive --
    and 6.4 measures it directly by running both paths against the same object.

    What this path gets in exchange is that it never expires: every invocation signs
    with live credentials, where a presigned URL has a fixed window (see HandleAWSURL's
    presign_expiry, and refresh_url below).

    It does NOT lose on connection reuse, which was the assumption. `HttpSource` calls
    `urllib.request.urlopen` per chunk, and urllib neither pools nor keeps alive -- it
    sends `Connection: close`. So both paths pay a TCP and TLS handshake per chunk, 3283
    of them for that object. Pooling those into one connection per worker is an
    unmeasured optimization available to HttpSource and not taken.
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
                command, shell=True, executable=SHELL, stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL, env=self.env,
            )
        except OSError as e:
            raise TransientError(str(e))
        return _ProcessStream(process)


class GcsObjectSource:
    """
    Read a gs:// object's STORED bytes through the GCS JSON API.

    For a gzip-encoded gs:// source that has to be decoded rather than copied
    server-side. A server-side rewrite copies stored bytes and metadata verbatim, so it
    cannot decode -- which is how a gzip-encoded reference .dict reached gcsfuse as raw
    gzip. Decoding needs the bytes on a node, and this lets them go through the same
    verified, streaming, keep-as-is decode as every other compressed source, instead of a
    gunzip into a temp file on the boot disk.

    Why not a URL plus an Authorization header: there is no signed URL on this path, and a
    bearer token in a static header expires after about an hour -- mid-transfer on a large
    object. `GcsClient.token()` refreshes, so every request carries a live one.

    Measured on the JSON media endpoint (§13.71), deterministic, the same as the XML
    endpoint signed URLs use:

      * without `Accept-Encoding: gzip` -- and urllib sends `identity` by default -- an
        ordinary gzip-encoded object comes back DECODED with the Range ignored. So every
        request here asks for gzip;
      * with it, ordinary and no-transform objects return the stored bytes and honour
        Range (206);
      * typed application/gzip, the stored bytes come back only as the WHOLE object (200,
        Range ignored). `whole_object_only` records that, the caller plans a single
        chunk, and a resume re-reads from zero and discards up to the offset -- the only
        way to resume against a server that will not range.
    """

    def __init__(self, bucket, name, user_project=None, timeout=DEFAULT_TIMEOUT):
        self.client = GcsClient(timeout=timeout)
        self.url = "{}/b/{}/o/{}?alt=media".format(
            GCS_API_ROOT, urllib.parse.quote(bucket, safe=""),
            urllib.parse.quote(name, safe=""))
        if user_project:
            # requester-pays: the bill goes to the job's project, as `cp
            # --billing-project` does on the server-side path
            self.url += "&userProject=" + urllib.parse.quote(user_project, safe="")
        self.timeout = timeout
        self.whole_object_only = False

    def refresh_url(self):
        return False            # nothing to re-mint: the token refreshes per request

    def _open(self, start, end=None):
        for attempt in (1, 2):
            token = self.client.token()
            request = urllib.request.Request(self.url, headers={
                "Authorization": "Bearer " + token,
                "Accept-Encoding": "gzip",
                "Range": "bytes={}-{}".format(start, "" if end is None else end - 1),
            })
            try:
                return urllib.request.urlopen(request, timeout=self.timeout)
            except urllib.error.HTTPError as e:
                if e.code == 401 and attempt == 1:
                    # an expired token: drop it and resend once with a fresh one. The
                    # cache would otherwise hand the same dead token to every retry.
                    self.client.invalidate_token(token)
                    continue
                if e.code in (403, 404):
                    raise PermanentError("GET gs source -> HTTP {}".format(e.code))
                if e.code == 416:
                    raise PermanentError("GET gs source -> HTTP 416: range not satisfiable")
                raise TransientError("GET gs source -> HTTP {}".format(e.code))
            except (urllib.error.URLError, OSError) as e:
                raise TransientError("GET gs source -> {}".format(e))

    @staticmethod
    def _stored(response, size):
        """Whether a 200 carries the whole STORED object (not a decoded rendition)."""
        encoded = "gzip" in (response.headers.get("Content-Encoding") or "").lower()
        length = response.headers.get("Content-Length")
        return encoded and (length is None or size is None or int(length) == size)

    def probe_range(self, size):
        response = self._open(0, 1)
        try:
            if response.status == 206:
                parsed = parse_content_range(response.headers.get("Content-Range"))
                if parsed is None or parsed[:2] != (0, 0):
                    raise RangeNotSupported("unexpected Content-Range {!r}".format(
                        response.headers.get("Content-Range")))
                declared = parsed[2]
                if declared is not None and size is not None and declared != size:
                    raise RangeNotSupported(
                        "server reports size {} but {} was expected".format(declared, size))
                return
            if response.status == 200 and self._stored(response, size):
                self.whole_object_only = True
                log("the gs source serves its stored bytes only whole (Range ignored); "
                    "reading it as a single stream")
                return
            # A decoded rendition: the stored bytes are not on offer, so neither the
            # digest nor the decode can be done here. Hand over to the fallback.
            raise RangeNotSupported(
                "gs source served a decoded rendition (HTTP {})".format(response.status))
        finally:
            response.close()        # abort rather than drain

    def open_range(self, start, end):
        response = self._open(start, end)
        if response.status == 206:
            content_range = (response.headers.get("Content-Range") or "").strip()
            parsed = parse_content_range(content_range)
            if parsed is None or parsed[:2] != (start, end - 1):
                response.close()
                raise TransientError("Content-Range {!r} does not match requested "
                                     "{}-{}".format(content_range, start, end - 1))
            return response
        if response.status == 200 and self.whole_object_only and self._stored(response, None):
            return _SkippingStream(response, skip=start, limit=end - start)
        response.close()
        raise RangeNotSupported(
            "gs source returned HTTP {} to a ranged request".format(response.status))


class _SkippingStream:
    """A whole-object response presented as the range [skip, skip + limit)."""

    def __init__(self, response, skip, limit):
        self.response = response
        self.remaining = limit
        while skip > 0:
            discarded = response.read(min(skip, READ_BUFFER))
            if not discarded:
                raise TransientError("short read skipping to the resume offset")
            skip -= len(discarded)

    def read(self, n):
        if self.remaining <= 0:
            return b""
        buf = self.response.read(min(n, self.remaining))
        self.remaining -= len(buf)
        return buf

    def close(self):
        self.response.close()


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
# GCS JSON API client (the bucket-compose route)
# --------------------------------------------------------------------------------

GCS_API_ROOT = "https://storage.googleapis.com/storage/v1"
GCS_UPLOAD_ROOT = "https://storage.googleapis.com/upload/storage/v1"


def gcs_custom_time():
    """
    Now, as the RFC 3339 string GCS takes for an object's customTime.

    Stamped on EVERY object this module creates -- parts, slices, sidecars, compose
    intermediates, the manifest, and the final object. The localization bucket's only
    deletion mechanism is a `daysSinceCustomTime` lifecycle rule (LOCALIZATION.md §3,
    and "nothing deletes the bucket", §8), and an object with no customTime is invisible
    to that rule and never expires. Nothing here set it, so every intermediate that
    escaped cleanup -- a lost race, a delete that 503'd twice, a job that hit its retry
    limit mid-relay -- was stored and billed forever. With the stamp, the rule that
    already exists is the backstop for every orphan path, including ones nobody has
    enumerated.

    The cost: a job requeued more than `localization_expiry_days` after its last write
    finds its parts expired and re-downloads, since the stale-manifest handling in
    run_bucket_route drops the manifest when parts are missing. That is the same bound
    the lifecycle rule already puts on idle storage for everything else in the bucket.
    """
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())
METADATA_TOKEN_URL = (
    "http://metadata.google.internal/computeMetadata/v1/"
    "instance/service-accounts/default/token"
)

# GCS persists resumable-upload bytes at this granularity, and every non-final PUT in a
# session must be a multiple of it. This is what bounds discarded work on the bucket-compose
# route.
GCS_UPLOAD_GRANULARITY = 256 * 1024

# How much the bucket route sends per PUT. Must be a multiple of the granularity above,
# because GCS rejects a non-final PUT that is not.
#
# This was GCS_UPLOAD_GRANULARITY itself, chosen to bound bytes-read-but-not-yet-durable
# to <256 KiB per in-flight chunk. Measured, that bound costs a factor of five: a
# resumable PUT costs ~53 ms round-trip regardless of payload, so 256 KiB per request is
# 4.7 MiB/s per stream and ~72 MiB/s at 16 connections -- and 4.7 MiB/s was observed
# within 1% from two unrelated sources (a GCS object and the GDC S3 endpoint), which is
# what identifies the block rather than the network as the cap.
#
# The trade was mispriced rather than wrong. At 8 MiB the worst case is 128 MiB of
# re-read across 16 in-flight chunks -- 0.045% of a 279 GiB object, on a preemption that
# already costs minutes of restart -- while the per-stream ceiling becomes
# transfer-bound instead of latency-bound and the aggregate moves above what the source
# can supply. Paying 5x throughput on every transfer to save 124 MiB on the ones that
# get preempted is the wrong side of that trade.
DEFAULT_UPLOAD_BLOCK = 8 * 1024 * 1024

# How long the manifest writer waits for more completions before paying the fixed cost of
# a rewrite, and the most it will accumulate.
#
# The writer took the whole queue the instant it woke, so batching only happened when
# chunks arrived *during* a commit. On the in-place route they do -- an fsync is slow
# enough that 3283 chunks committed in 2 batches. On the bucket route a commit takes
# ~0.23 s and the next chunk is ~0.55 s away, so every batch was one chunk and `commit`
# grew from 8% of wall at 12 GiB to 43% at full size: 2.14x the chunks but 7.2x the time,
# because each write re-uploads a manifest that now carries 9849 part digests too.
#
# Lingering does not weaken the ordering rule the batching rests on. The queue is still
# snapshotted before the fsync begins, so everything in the snapshot finished writing
# before it -- the snapshot is simply taken later. What it does widen is work at risk: a
# crash during the window re-fetches whatever had not been committed, bounded by the
# window times the completion rate. At full size that is ~4 chunks, against the 279 GiB
# the manifest exists to protect.
COMMIT_LINGER_SECONDS = 2.0
COMMIT_MAX_BATCH = 64

# A single compose call accepts at most this many sources; more are tree-composed.
GCS_COMPOSE_MAX_SOURCES = 32


class GcsClient:
    """
    Minimal GCS JSON API client over urllib, so the bucket-compose route needs nothing
    beyond the standard library.

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

    def invalidate_token(self, stale):
        """
        Drop the cached token after GCS rejected it (401), so the next token() fetches.

        Only if it is still the one rejected: with 16 workers, several may see the same
        expiry, and the first to refresh must not have its fresh token thrown away by the
        rest.
        """
        with self._lock:
            if self._token == stale:
                self._token = None
                self._token_expiry = 0.0

    # How long a token from `gcloud ... print-access-token` is trusted. gcloud returns its
    # CACHED token, which may be minutes from expiry -- it refreshes only inside ~3m45s --
    # and the command reports no lifetime. It was assumed to be 3600 s and cached for 55
    # minutes: a full-size bucket-route run then got `HTTP 401: Invalid Credentials` 23
    # minutes in, and its md5 read-back failed (§13.77). 480 caches for 3 minutes (see
    # token()), inside gcloud's own refresh margin; re-running gcloud costs about a second.
    # A 401 also invalidates the cache (request), so this is the second line of defense.
    SUBPROCESS_TOKEN_LIFETIME = 480

    # Application Default Credentials first, then the active gcloud account, and the
    # metadata server only as a last resort. Order matters more than any of the
    # individual entries.
    TOKEN_SOURCES = (
        ("ADC", ["gcloud", "auth", "application-default", "print-access-token"]),
        ("gcloud account", ["gcloud", "auth", "print-access-token"]),
    )

    @staticmethod
    def adc_file():
        """
        The credentials file ADC would actually resolve to, or None.

        Needed because `gcloud auth application-default print-access-token` **succeeds
        without any ADC file at all**: resolution falls through to the GCE metadata
        server, and gcloud reports success having handed back a *service account* token.
        So the command exiting 0 says nothing about which identity you got, and a
        downloader that logged "using ADC" on the strength of it would be lying about
        the only thing the log line exists to tell you.

        Observed on a real node: `CLOUDSDK_CONFIG=/user_gcloud_config` while the mounted
        user credentials sat at `/root/.config/gcloud/`, so gcloud never saw them, the
        token came back as the compute SA, and the upload 403'd with a service-account
        name in the message. The token call had "worked".

        Checked in ADC's own precedence order.
        """
        explicit = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if explicit and os.path.exists(explicit):
            return explicit
        config = os.environ.get("CLOUDSDK_CONFIG")
        candidates = []
        if config:
            candidates.append(os.path.join(config, "application_default_credentials.json"))
        candidates.append(os.path.expanduser(
            "~/.config/gcloud/application_default_credentials.json"))
        for candidate in candidates:
            if os.path.exists(candidate):
                return candidate
        return None

    def _fetch_token(self):
        """
        Authenticate as the same identity as everything else on this path: ADC.

        **This used to prefer the metadata server**, on the reasoning that workers are GCE
        VMs and it avoids a subprocess. Both premises were true and the conclusion was
        wrong, because the choice is not about cost -- it silently selects *who you are*.
        The metadata server always answers on GCE, so it always won, and every bucket
        write went out as the **compute service account** while the rest of the same job
        ran as the copied user credentials.

        canine takes explicit pains to prevent exactly that. `base.py`'s bucket-mount
        block and `dockerTransient.py`'s rclone path both pin
        `GOOGLE_APPLICATION_CREDENTIALS` to `$CLOUDSDK_CONFIG/application_default_
        credentials.json` before invoking anything, and the comment there names this
        failure: *"the authenticating identity depends on whatever ADC happens to resolve
        to (metadata-server SA vs. the copied user credentials), which silently works in
        one project and fails in another."* A production worker gets its credentials from
        `docker_copy_gcloud_credentials.sh`, not from the instance's service account.

        The consequences of getting it wrong are all quiet ones: a 403 on upload that
        looks like a bucket misconfiguration, or -- worse -- success, with objects written
        and billed under an identity nobody intended and an audit trail that does not
        match the workflow.

        So: ADC, then the active gcloud account, then the metadata server. The subprocess
        the old order was avoiding costs about a second, against a token cached for ~55
        minutes and a transfer measured in hours.

        The metadata server stays as the final fallback rather than being removed: a
        worker with no user credentials at all is a real configuration, and failing to
        authenticate would be worse than authenticating as the SA. It is last because it
        is the answer that is always available, not the one that is usually right.
        """
        adc = self.adc_file()
        for label, command in self.TOKEN_SOURCES:
            if label == "ADC" and adc is None:
                # Skipping rather than trying: the command would succeed via the
                # metadata server and we would report the wrong identity.
                log("k9pdl-auth no ADC credentials file found "
                    "(GOOGLE_APPLICATION_CREDENTIALS, $CLOUDSDK_CONFIG, ~/.config/gcloud)")
                continue
            try:
                out = subprocess.run(command, capture_output=True, timeout=60)
            except (OSError, subprocess.SubprocessError):
                continue
            if out.returncode == 0:
                token = out.stdout.decode("utf-8").strip()
                if token:
                    log("k9pdl-auth using {}{}".format(
                        label, " ({})".format(adc) if label == "ADC" else ""))
                    return token, self.SUBPROCESS_TOKEN_LIFETIME

        request = urllib.request.Request(
            METADATA_TOKEN_URL, headers={"Metadata-Flavor": "Google"}
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                payload = json.loads(response.read().decode("utf-8"))
            # Named, because it means the writes are NOT the user's. On a node that is
            # supposed to have credentials this line is the symptom.
            log("k9pdl-auth using the metadata server (compute service account) -- "
                "ADC and gcloud both unavailable")
            return payload["access_token"], int(payload.get("expires_in", 3600))
        except Exception:
            pass

        raise PermanentError(
            "could not obtain a GCS access token from ADC, gcloud, or the metadata server"
        )

    # -- plumbing -----------------------------------------------------------

    def request(self, method, url, body=None, headers=None, expect=(200, 201)):
        """
        Issue a request and return (status, headers, body).

        Unlike urlopen this does not raise for the status codes the resumable-upload
        protocol uses as ordinary signals -- notably 308, which means "session alive,
        here is how far I have persisted".

        A 401 is an expired token, not a denial: the cached token is dropped and the
        request resent once with a fresh one. Only a second 401 is an error. Resending is
        safe: the body is in memory, and a resumable session ignores bytes it has already
        persisted.
        """
        for attempt in (1, 2):
            token = self.token()
            all_headers = {"Authorization": "Bearer " + token}
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
                if status == 401 and attempt == 1:
                    self.invalidate_token(token)
                    continue
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
            "POST", url,
            body=json.dumps({"name": name,
                             "customTime": gcs_custom_time()}).encode("utf-8"),
            headers={"Content-Type": "application/json; charset=UTF-8"},
        )
        session = headers.get("Location") or headers.get("location")
        if not session:
            raise TransientError("resumable upload session had no Location header")
        return session

    def session_offset(self, session_uri, total):
        """
        Ask the session how far it has durably persisted.

        This is the bucket-compose route's frontier oracle, exactly analogous to SEEK_HOLE:
        the answer comes from the storage rather than from a counter we kept. `None` means
        the upload already finished; 0 means nothing is persisted yet.
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
        # total=None sends "bytes X-Y/*", which is how GCS is told the final length is
        # not yet known. The gunzip pass needs it: the decompressed size cannot be known
        # in advance -- gzip's ISIZE is modulo 2**32, unusable above 4 GiB -- and
        # measuring it would mean decompressing the object twice. The last call passes a
        # real total, and that is what completes the session.
        end = offset + len(chunk) - 1
        status, headers, body = self.request(
            "PUT", session_uri, body=chunk,
            headers={
                "Content-Length": str(len(chunk)),
                "Content-Range": "bytes {}-{}/{}".format(
                    offset, end, "*" if total is None else total),
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

    def put_object(self, bucket, name, body):
        """
        Write a small object, with metadata, in one request.

        A single upload request is atomic: the object either appears whole or not at
        all, with no intermediate state a reader could see. That is the property the
        manifest needs on a bucket, where the tmp-plus-rename commit used on a POSIX
        filesystem does not work -- rename is a server-side copy and delete on a
        flat-namespace bucket, so it is not atomic there.

        `uploadType=multipart`, not `media`: a media upload carries no metadata, so it
        could not set customTime (see gcs_custom_time). Still one request, so still
        atomic.
        """
        url = "{}/b/{}/o?uploadType=multipart".format(
            GCS_UPLOAD_ROOT, urllib.parse.quote(bucket, safe=""))
        metadata = json.dumps({"name": name, "customTime": gcs_custom_time()}).encode()
        boundary = "k9pdl{:032x}".format(random.getrandbits(128))
        while boundary.encode() in body:            # vanishingly unlikely, never wrong
            boundary = "k9pdl{:032x}".format(random.getrandbits(128))
        dash = b"--" + boundary.encode()
        payload = b"".join([
            dash, b"\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n",
            metadata, b"\r\n",
            dash, b"\r\nContent-Type: application/octet-stream\r\n\r\n",
            body, b"\r\n",
            dash, b"--\r\n",
        ])
        self.request("POST", url, body=payload, headers={
            "Content-Type": "multipart/related; boundary=" + boundary})

    def read_object(self, bucket, name):
        """Full contents of an object, or None when it does not exist."""
        url = "{}/b/{}/o/{}?alt=media".format(
            GCS_API_ROOT, urllib.parse.quote(bucket, safe=""),
            urllib.parse.quote(name, safe=""),
        )
        try:
            _, _, body = self.request("GET", url, expect=(200,))
        except PermanentError:
            return None
        return body

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
            "destination": {"name": destination, "customTime": gcs_custom_time()},
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

    `max_sources` below 2 cannot make progress: every group would be a single, every
    single is carried forward unchanged, and the next level would equal the last. That
    is an infinite loop issuing no requests, so it presents as a hang rather than an
    error -- rejected here so the parameter can be exercised safely.
    """
    if max_sources < 2:
        raise ValueError(
            "max_sources must be at least 2 to fold, got {}".format(max_sources))
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

    # Unconditional: a single remaining source still needs composing, because that is
    # what gives the destination its name. This was previously spelled as a `> 1` test
    # falling through to the same call when the result was None, which read as two
    # cases but was one -- and a client whose compose returned None would have composed
    # twice.
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


class _AlreadyDone:
    """
    Sentinel returned by a sink's chunk_ready when the chunk is already recorded complete
    and there is nothing to commit. A distinct object rather than None because None is a
    legitimate commit payload -- "no digest for this chunk" -- and conflating the two would
    silently drop done markers.
    """
    def __repr__(self):
        return "<already done>"


CHUNK_ALREADY_DONE = _AlreadyDone()


class PartHashingSink:
    """
    In-flight md5s of the S3 parts a multipart ETag is built from.

    Shared by both sinks that can produce one, because the rule below is subtle enough
    that a second copy would drift: **a part is hashed only if this process saw it from
    its first byte onward, contiguously.** Anything else is left unrecorded rather than
    recorded wrong -- after a preemption the resumed chunk starts mid-part, and a digest
    over the tail of a part is not a digest of the part.

    That conservatism is what makes the recorded digests safe to trust without
    re-reading: an unrecorded part costs one 29 MiB read, a wrongly recorded one costs a
    corrupt object that verifies.

    Chunk starts are always multiples of part_length (plan_chunks guarantees it) and a
    chunk spans a whole number of parts except possibly the last, so a buffer maps onto
    a contiguous run of parts with no partial-part bookkeeping beyond the object's tail.

    Subclasses provide `part_length`, `size`, and call `_init_part_hashing()`.
    """

    def _init_part_hashing(self, part_length, size):
        self.part_length = part_length
        self.size = size
        self._parts = {}      # part index -> {"md5": ..., "next": expected offset}
        self._pending = {}    # chunk index -> {part index: hex} awaiting commit
        self._parts_lock = threading.Lock()

    def _part_bounds(self, part_index):
        start = part_index * self.part_length
        end = min(start + self.part_length, self.size)
        return start, end

    def _hash(self, chunk_index, offset, buf):
        """Fold `buf` into the md5 of each S3 part it covers."""
        view = memoryview(buf)
        with self._parts_lock:
            while view:
                part_index = offset // self.part_length
                p_start, p_end = self._part_bounds(part_index)
                take = min(len(view), p_end - offset)

                tracker = self._parts.get(part_index)
                if tracker is None:
                    if offset == p_start:
                        tracker = self._parts[part_index] = {
                            "md5": hashlib.md5(), "next": p_start}
                    else:
                        self._parts[part_index] = False      # missed its start
                if tracker:
                    if tracker["next"] == offset:
                        tracker["md5"].update(view[:take])
                        tracker["next"] = offset + take
                        if tracker["next"] == p_end:
                            self._pending.setdefault(chunk_index, {})[part_index] = \
                                tracker["md5"].hexdigest()
                            del self._parts[part_index]
                    else:
                        self._parts[part_index] = False      # no longer contiguous

                view = view[take:]
                offset += take

    def take_part_digests(self, index):
        """Detach this chunk's finished part digests, on the worker that finished it."""
        if self.part_length:
            with self._parts_lock:
                return self._pending.pop(index, None) or {}
        return {}


class PosixChunkSink(PartHashingSink):
    """
    Writes chunks in place into the sparse destination file (the in-place route).

    Durable progress comes from the file's own extents, so there is nothing to keep in
    sync -- see the module docstring.
    """

    needs_local_file = True

    # 1 MiB balances memory against write granularity; pwrite is durable as soon as the
    # filesystem commits, so there is no un-acknowledged window to bound.
    read_block = READ_BLOCK

    def __init__(self, fd, manifest, chunks, dest, part_length=None, size=None):
        self.fd = fd
        self.manifest = manifest
        self.chunks = chunks
        self.dest = dest
        # S3 part length, when the caller wants a multipart ETag. Hashing as the bytes
        # go past removes the full read-back verify() would otherwise perform -- which
        # measures as long as the download itself on a persistent disk.
        self._init_part_hashing(part_length, size)
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
        if self.part_length:
            self._hash(index, offset, buf)

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

    def chunk_ready(self, index):
        """
        Worker-thread half of completion: hand over whatever this chunk has to commit.

        Runs on the worker that finished the chunk, so the part digests are detached from
        _pending at the moment the chunk's last byte was written -- which is what makes it
        safe for a later, batched fsync to cover them. Does no I/O and cannot raise.
        """
        return self.take_part_digests(index)

    def commit(self, batch):
        """
        Writer-thread half: one fsync of the data, one manifest write, for the whole batch.

        The part digests and the done markers go into the SAME manifest write, which is
        strictly stronger than the old per-chunk pair of writes -- a digest could never be
        durable while its done marker was not, and now neither can be durable without the
        other.
        """
        part_md5s = {}
        for _, digests in batch:
            if digests:
                part_md5s.update(digests)
        self.manifest.record_chunks_done(
            [index for index, _ in batch], self.fd, part_md5s=part_md5s)

    def chunk_done(self, index):
        """Synchronous completion: chunk_ready + commit for one chunk."""
        self.commit([(index, self.chunk_ready(index))])

    def sync(self):
        os.fsync(self.fd)


class BucketChunkSink(PartHashingSink):
    """
    Uploads each chunk as its own GCS object through a resumable upload session, then
    composes them server-side (the bucket-compose route).

    The VM is a pure relay here: bytes arrive from the source and leave again to GCS
    with no local file at all, which is why this route needs no staging disk and no
    concatenation transfer.

    Resumability is preserved by the same principle as the in-place route -- ask the storage
    where its durable frontier is rather than trusting a stored counter. A resumable session
    answers that with a 308 plus a Range header, at 256 KiB granularity, so worst-case
    discarded work is under 256 KiB per in-flight chunk. Sessions live 7 days, uploads
    within one must be sequential (which is how chunks are streamed anyway), persisted bytes
    can never be overwritten (so a retry re-sending a committed range is harmless), and an
    incomplete upload is invisible in the bucket -- a preempted part leaves no partial
    object and no ambiguity about what exists.
    """

    needs_local_file = False

    # Set per instance from --upload-block; see DEFAULT_UPLOAD_BLOCK for why it is not
    # the commit granularity. Bytes read from the source but not yet acknowledged by GCS
    # are lost if the attempt dies, so this is what bounds discarded work per in-flight
    # part -- a real guarantee, just a cheaper one than it used to be.
    read_block = DEFAULT_UPLOAD_BLOCK

    def __init__(self, client, bucket, parts_prefix, manifest, chunks,
                 part_length=None, size=None, upload_block=None):
        if upload_block:
            if upload_block % GCS_UPLOAD_GRANULARITY:
                raise PermanentError(
                    "--upload-block must be a multiple of {} bytes; GCS rejects a "
                    "non-final PUT that is not".format(GCS_UPLOAD_GRANULARITY))
            self.read_block = upload_block
        self.client = client
        self.bucket = bucket
        self.parts_prefix = parts_prefix
        self.manifest = manifest
        self.chunks = chunks
        self._digests = {}
        self._completed = set()
        self._lock = threading.Lock()
        # Two independent digest keyspaces, and conflating them is the trap: `_digests`
        # above is per CHUNK (one chunk is one uploaded GCS part, checked against what
        # GCS stored before compose), while the mixin's is per S3 PART of the *source*,
        # which is what a multipart ETag is built from. A chunk spans several S3 parts.
        self._init_part_hashing(part_length, size)

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

        if self.part_length:
            self._hash(index, offset, buf)

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

    def chunk_ready(self, index):
        # Only GCS can say a part is finished. If the session never returned a
        # completion, the part is not durable and must not be recorded done -- it would
        # be composed as a missing object.
        #
        # This check MUST stay on the worker thread. It is the one completion check that
        # can fail, and raising here is what puts the chunk back through the retry loop;
        # raising it on the writer thread instead would strand the worker believing it had
        # succeeded.
        if self.manifest.is_complete(index):
            return CHUNK_ALREADY_DONE
        with self._lock:
            completed = index in self._completed
        if not completed:
            raise TransientError(
                "part {} sent all its bytes but the upload session did not "
                "complete".format(index)
            )
        # (chunk digest, S3-part digests). Opaque to the Downloader, which only passes
        # whatever chunk_ready returns straight back into commit -- the two sinks
        # already return different shapes.
        return (self.part_digest(index), self.take_part_digests(index))

    def commit(self, batch):
        digests = {}
        part_md5s = {}
        for index, state in batch:
            chunk_digest, parts = state if isinstance(state, tuple) else (state, {})
            if chunk_digest:
                digests[index] = chunk_digest
            part_md5s.update(parts)
        self.manifest.record_chunks_done(
            [index for index, _ in batch], None, chunk_digests=digests)
        # Separate write, because record_chunks_done owns the chunk keyspace. Both land
        # in the same manifest object; on this route there is no data fd to fsync first.
        self.manifest.record_part_md5s(part_md5s, None)

    def chunk_done(self, index):
        state = self.chunk_ready(index)
        if state is CHUNK_ALREADY_DONE:
            return
        self.commit([(index, state)])

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
        # Wall time summed over every chunk's read loop, so `streaming / wall` is the
        # mean number of requests actually receiving bytes at once. Without it, a run
        # whose throughput does not move with `connections` is ambiguous between "the
        # source caps aggregate bandwidth, so parallelism cannot help" and "the requests
        # were not concurrent" -- two conclusions with opposite consequences, and no
        # other number in the output distinguishes them.
        self._stream_seconds = 0.0
        self._stream_lock = threading.Lock()
        # The read loop decomposed: time inside `stream.read()` against time inside
        # `sink.write()`. Both run on the worker thread, one after the other, so a
        # worker is never doing both at once -- which makes `read / (read + write)` the
        # share of worker time the network holds the disk idle for, and therefore the
        # headroom a reader/writer split would recover.
        #
        # This exists because every EXTERNAL explanation for the full-size run settling
        # at 56% of the disk has been eliminated by measurement (runbook §6.5c-f: the
        # source does 240 MiB/s at any depth, write concurrency is free, logical scatter
        # and disk fullness cost nothing, one shared inode costs 5%). What remains is
        # inside this loop, and the last two times that was true, guessing produced the
        # wrong answer.
        self._read_seconds = 0.0
        self._write_seconds = 0.0
        self._io_blocks = 0
        self._commit_linger = getattr(options, "commit_linger", None)
        if self._commit_linger is None:
            self._commit_linger = COMMIT_LINGER_SECONDS
        # Wall time summed over every chunk_done -- the manifest rewrite, its fsyncs and
        # the wait for Manifest._lock. This is the only work that grows with the CHUNK
        # COUNT rather than the byte count, and the 279 GiB run fell to 50% of the disk
        # floor where the 4 GiB run reached 83%, with per-stream throughput unchanged at
        # the source rate both times. That points here, and guessing at it once already
        # produced a wrong answer (the fsync-barrier hypothesis, which predicted a large
        # win from fewer chunks and delivered 8%). So measure it.
        self._bookkeeping_seconds = 0.0
        self._bookkeeping_calls = 0
        # Writer-thread accounting. `_commit_batches` is the number that says whether the
        # deferral actually did anything: if the mean batch size is ~1 the writer is being
        # drained as fast as it is filled and nothing was amortised, which looks identical
        # in a throughput figure to the fix working.
        self._commit_seconds = 0.0
        self._commit_batches = 0
        self._commit_chunks = 0
        self._writer = None
        self._writer_queue = []
        self._writer_cv = threading.Condition()
        self._writer_stop = False
        self._writer_error = None

    # ---- deferred manifest commits -------------------------------------------------
    #
    # A chunk's done marker may only be written after an fsync that covers that chunk's
    # bytes. Doing that per chunk costs three fsyncs and a rename each, serialised on
    # Manifest._lock, and consumed 70% of the worker pool on the 3283-chunk run. The work
    # is not removed -- the same guarantee still requires the same fsync -- it is made
    # independent of the chunk count by committing whatever has piled up in one round.
    #
    # The ordering rule the batching rests on: the queue is SNAPSHOTTED before the fsync
    # starts, so every chunk in the snapshot finished writing before the fsync began and is
    # therefore covered by it. A chunk enqueued while that fsync is in flight is NOT
    # covered, is not in the snapshot, and waits for the next round.

    def _start_writer(self):
        with self._writer_cv:
            self._writer_stop = False
            self._writer_error = None
            self._writer_queue = []
            self._writer = threading.Thread(
                target=self._writer_loop, name="k9pdl-manifest", daemon=True)
        self._writer.start()

    def _writer_loop(self):
        while True:
            with self._writer_cv:
                while not self._writer_queue and not self._writer_stop:
                    self._writer_cv.wait()
                if not self._writer_queue:
                    return                      # stopped and drained
                # Let more completions land before rewriting the manifest. Skipped
                # entirely when stopping, so teardown never waits on it.
                if self._commit_linger > 0 and not self._writer_stop:
                    deadline = time.monotonic() + self._commit_linger
                    while (len(self._writer_queue) < COMMIT_MAX_BATCH
                           and not self._writer_stop):
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            break
                        self._writer_cv.wait(remaining)
                batch = self._writer_queue
                self._writer_queue = []
            started = time.monotonic()
            try:
                self.sink.commit(batch)
            except BaseException as e:
                # Never retried here and never re-queued. A failed commit costs those
                # chunks being re-fetched on the next attempt, which is the same price the
                # manifest already pays for any lost update; what it must not do is leave
                # the workers waiting on a thread that is gone.
                with self._writer_cv:
                    self._writer_error = e
                    self._writer_cv.notify_all()
                return
            finally:
                elapsed = time.monotonic() - started
                with self._writer_cv:
                    self._commit_seconds += elapsed
                    self._commit_batches += 1
                    self._commit_chunks += len(batch)

    def _enqueue_done(self, index, state):
        with self._writer_cv:
            if self._writer is None:
                # no writer running (a direct call outside run()); commit inline
                self.sink.commit([(index, state)])
                return
            self._writer_queue.append((index, state))
            self._writer_cv.notify_all()

    def _stop_writer(self):
        """
        Drain and join the writer, returning whatever killed it.

        A plain join, with no timeout: the only unbounded wait inside the loop is the
        commit itself, which is exactly as blocking as it was when a worker ran it, and
        abandoning a live writer would leave it rewriting the manifest behind verify()'s
        back. The thread is a daemon so a genuinely wedged process still dies.
        """
        with self._writer_cv:
            writer, self._writer = self._writer, None
            if writer is None:
                return None
            self._writer_stop = True
            self._writer_cv.notify_all()
        writer.join()
        with self._writer_cv:
            error, self._writer_error = self._writer_error, None
            stranded = len(self._writer_queue)
            self._writer_queue = []
        if error is not None:
            log("the manifest writer failed ({}); {} completed chunk(s) were not "
                "recorded and may be re-fetched".format(error, stranded))
        return error

    def resume_offset(self, index):
        return self.sink.resume_offset(index)

    def download_chunk(self, index):
        start, end = self.chunks[index]
        offset = self.resume_offset(index)

        if offset >= end:
            self._chunk_done(index)
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

            # Counted around the read loop rather than the whole attempt: this measures
            # time with an open request receiving bytes, which is what "how many streams
            # were really running" means. Backoff sleeps and the open itself are excluded
            # deliberately -- counting them would inflate the concurrency figure with
            # time nothing was being transferred.
            stream_started = time.monotonic()
            # Accumulated locally and folded in once per attempt. READ_BLOCK is 1 MiB,
            # so a 279 GiB object is ~285k iterations; taking the shared lock per block
            # would put 285k acquisitions through 16 threads to measure something the
            # lock itself would then distort.
            io = [0.0, 0.0, 0]
            try:
                while offset < end:
                    want = min(self.sink.read_block, end - offset)
                    mark = time.monotonic()
                    buf = stream.read(want)
                    after_read = time.monotonic()
                    io[0] += after_read - mark
                    if not buf:
                        raise TransientError(
                            "short read at {} ({} bytes short)".format(offset, end - offset)
                        )
                    durable = self.sink.write(index, offset, buf)
                    io[1] += time.monotonic() - after_read
                    io[2] += 1
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
                self._count_stream_time(stream_started, io)
            except TransientError as e:
                self._count_stream_time(stream_started, io)
                attempts += 1
                if attempts > self.options.retries:
                    raise
                self._backoff(attempts, "chunk {}: {}".format(index, e))
            except (IOError, OSError) as e:
                self._count_stream_time(stream_started, io)
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

        self._chunk_done(index)

    def _chunk_done(self, index):
        """
        Worker-thread completion. Validates on this thread (so a failure still reaches the
        retry loop) and hands the durable part off to the writer.
        """
        started = time.monotonic()
        try:
            state = self.sink.chunk_ready(index)
        finally:
            with self._stream_lock:
                self._bookkeeping_seconds += time.monotonic() - started
                self._bookkeeping_calls += 1
        if state is CHUNK_ALREADY_DONE:
            return
        self._enqueue_done(index, state)

    def _log_concurrency(self, workers, chunks, wall):
        """
        Report the mean number of requests that were actually receiving bytes at once.

        This exists because a sweep whose throughput does not move with `connections`
        has two readings with opposite consequences -- the source caps aggregate
        bandwidth and parallelism cannot help it, or the requests never ran
        concurrently and there is a defect to find -- and nothing else in the output
        tells them apart. Both look like a flat line.

        `mean` well under `workers` is not automatically a defect: the tail of a
        download has fewer chunks left than workers, and a resumed run may have only a
        handful pending. It is the combination of `mean` near 1 with many pending chunks
        that indicts the downloader.
        """
        if wall <= 0:
            return
        with self._stream_lock:
            streaming = self._stream_seconds
        with self._stream_lock:
            book = self._bookkeeping_seconds
            calls = self._bookkeeping_calls
            read_s = self._read_seconds
            write_s = self._write_seconds
            blocks = self._io_blocks
        with self._writer_cv:
            commit = self._commit_seconds
            batches = self._commit_batches
            committed = self._commit_chunks
        # Seconds to 3 decimals, as k9pdl-io prints read and write: its read + write is a
        # decomposition of this streaming time, and comparing values rounded to different
        # precisions made that check fail whenever streaming came in under 0.05 s.
        log("k9pdl-streams mean {:.2f} of {} workers "
            "({} chunks, {:.3f}s wall, {:.3f}s streaming)".format(
                streaming / wall, workers, chunks, wall, streaming))
        # Worker-seconds, not wall: with `workers` threads the available budget is
        # workers*wall, so this says what share of the pool was doing bookkeeping rather
        # than moving bytes.
        log("k9pdl-bookkeeping {:.1f}s over {} calls (mean {:.3f}s, "
            "{:.1f}% of {} worker-seconds)".format(
                book, calls, book / calls if calls else 0.0,
                100.0 * book / (workers * wall) if wall else 0.0, workers * wall))
        # The read loop split in two. Both halves run on the worker thread in sequence,
        # so `read` is time the network held the disk idle and `write` is time the disk
        # held the network idle -- and `read / (read + write)` is the share a
        # reader/writer split with a queue between them could recover. `other` is
        # whatever is left of the loop: the progress accounting, and on the in-place
        # route the in-transfer md5, which lives inside sink.write() and is therefore
        # counted under `write` rather than separately.
        io = read_s + write_s
        # The ceiling is (read + write) / max(read, write), NOT 1/(1 - read share).
        # Decoupling the two with a queue turns a worker's serial `read then write` into
        # a pipelined `max(read, write)` -- it can hide the smaller half behind the
        # larger one and no more. That is bounded by 2.0, at read == write. The
        # 1/(1 - share) form assumes reads become free, which overstates it without
        # bound and reported infinity the first time a test wrote to a fast local disk.
        slower = max(read_s, write_s)
        log("k9pdl-io read {:.3f}s write {:.3f}s other {:.3f}s over {} blocks "
            "(read {:.0f}% of loop, overlap ceiling {:.2f}x)".format(
                read_s, write_s, max(0.0, streaming - io), blocks,
                100.0 * read_s / io if io else 0.0,
                (io / slower) if slower > 0 else 1.0))
        # The writer's own cost, which is OFF the worker pool and so is reported against
        # the wall clock instead. `mean batch` is the point of the whole mechanism: at 1.0
        # nothing was amortised and the commits are still per-chunk, which a throughput
        # number alone cannot distinguish from the fix working.
        log("k9pdl-commit {:.1f}s over {} batches ({} chunks, mean batch {:.1f}, "
            "{:.1f}% of {:.1f}s wall)".format(
                commit, batches, committed,
                (float(committed) / batches) if batches else 0.0,
                100.0 * commit / wall if wall else 0.0, wall))

    def _count_stream_time(self, started, io=None):
        """
        Account the time this attempt spent with an open request receiving bytes.

        Called from each exit path of the read loop rather than from a `finally`, so
        that `_backoff` sleeps -- which run in the handlers, after the loop -- are
        excluded. Counting them would inflate the concurrency figure with time nothing
        was transferring, which is the one thing this number exists to rule out.

        Every exit path must call it. TestEveryStreamExitIsAccounted checks that against
        the source, because a handler added later without a call would silently bias the
        figure downward -- and a low figure is read as "the requests were not concurrent".
        """
        with self._stream_lock:
            self._stream_seconds += time.monotonic() - started
            if io is not None:
                self._read_seconds += io[0]
                self._write_seconds += io[1]
                self._io_blocks += io[2]

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
        wall_started = time.monotonic()
        errors = []
        self._start_writer()
        try:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = [pool.submit(self.download_chunk, i) for i in pending]
                for future in futures:
                    try:
                        future.result()
                    except Exception as e:  # re-raised below, once every worker has settled
                        errors.append(e)
        finally:
            # Joined before anything reads the manifest or the file -- verify(), finalize()
            # and the done marker all run after run() returns, and every one of them would
            # be racing a live writer otherwise. In `finally` so a worker blowing up still
            # cannot leave the thread running.
            writer_error = self._stop_writer()
        if writer_error is not None:
            errors.append(writer_error)
        self._log_concurrency(workers, len(pending), time.monotonic() - wall_started)
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


def multipart_etag(path, part_length, block=READ_BUFFER, workers=None):
    """
    S3's multipart ETag is md5-of-md5s with a "-N" part-count suffix.

    Each part's md5 is independent of every other, so this is embarrassingly parallel and
    is computed across a thread pool: hashlib releases the GIL for buffers of this size,
    so the hashing genuinely overlaps rather than merely interleaving. On a 300 GB object
    the read-back is the dominant cost of verification, and it is worth using the cores
    the localization node has.

    Deliberately NOT materializing a whole part: S3 part sizes run from 8 MB to several
    GB, so buffering one would make peak memory a property of how the uploader happened
    to chunk the object. Each worker holds `block` bytes, so memory is
    `workers * block` regardless of part size.
    """
    size = os.path.getsize(path)
    if size == 0:
        return None
    n_parts = (size + part_length - 1) // part_length

    if workers is None:
        workers = min(n_parts, max(1, (os.cpu_count() or 2)))

    digests = [None] * n_parts

    def hash_part(index):
        start = index * part_length
        remaining = min(part_length, size - start)
        digest = hashlib.md5()
        # A private handle per worker: a shared one would need locking around every
        # seek/read pair and would serialize exactly what this is parallelizing.
        with open(path, "rb") as fh:
            fh.seek(start)
            while remaining:
                piece = fh.read(min(block, remaining))
                if not piece:
                    break
                digest.update(piece)
                remaining -= len(piece)
        if remaining:
            raise PermanentError(
                "short read hashing part {} of {}".format(index, path)
            )
        digests[index] = digest.digest()

    if workers <= 1:
        for index in range(n_parts):
            hash_part(index)
    else:
        errors = []

        def run(indices):
            for index in indices:
                try:
                    hash_part(index)
                except Exception as e:                       # noqa: BLE001
                    errors.append(e)
                    return

        threads = []
        for offset in range(workers):
            indices = range(offset, n_parts, workers)
            thread = threading.Thread(target=run, args=(indices,), daemon=True)
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()
        if errors:
            raise errors[0]

    if any(d is None for d in digests):
        raise PermanentError("failed to hash every part of {}".format(path))
    return "{}-{}".format(hashlib.md5(b"".join(digests)).hexdigest(), len(digests))


def multipart_etag_from_manifest(path, part_length, manifest, block=READ_BUFFER,
                                 workers=None):
    """
    The multipart ETag, using digests recorded during the transfer and reading back only
    the parts that lack one.

    On the in-place route `verify()` otherwise re-reads the entire object, which on a
    persistent disk costs about as long as the download did -- so the whole point of
    hashing during the transfer is that this reads nothing on the happy path.

    Fallback is PER PART, not all-or-nothing. After a single preemption only the parts
    straddling the resume point are unrecorded, so a handful of 29 MiB reads stands in
    for a 279 GiB one.
    """
    size = os.path.getsize(path)
    if size == 0:
        return None, 0
    n_parts = (size + part_length - 1) // part_length

    digests = [None] * n_parts
    missing = []
    for index in range(n_parts):
        recorded = manifest.part_md5(index) if manifest is not None else None
        if recorded:
            try:
                digests[index] = binascii.unhexlify(recorded)
            except (binascii.Error, ValueError):
                missing.append(index)
        else:
            missing.append(index)

    if missing:
        if workers is None:
            workers = VERIFY_READ_WORKERS
        _hash_parts_into(path, part_length, size, missing, digests, block, workers)

    if any(d is None for d in digests):
        raise PermanentError("failed to hash every part of {}".format(path))
    combined = hashlib.md5(b"".join(digests)).hexdigest()
    return "{}-{}".format(combined, n_parts), len(missing)


def _hash_parts_into(path, part_length, size, indices, digests, block, workers):
    """Hash the given part indices, filling `digests` in place."""
    def hash_one(index):
        start = index * part_length
        remaining = min(part_length, size - start)
        digest = hashlib.md5()
        with open(path, "rb") as fh:
            fh.seek(start)
            while remaining:
                piece = fh.read(min(block, remaining))
                if not piece:
                    break
                digest.update(piece)
                remaining -= len(piece)
        if remaining:
            raise PermanentError(
                "short read hashing part {} of {}".format(index, path))
        digests[index] = digest.digest()

    if workers <= 1 or len(indices) == 1:
        for index in indices:
            hash_one(index)
        return

    errors = []

    def run(subset):
        for index in subset:
            try:
                hash_one(index)
            except Exception as e:                              # noqa: BLE001
                errors.append(e)
                return

    threads = []
    for offset in range(min(workers, len(indices))):
        subset = indices[offset::workers]
        thread = threading.Thread(target=run, args=(subset,), daemon=True)
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()
    if errors:
        raise errors[0]


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


class phase:
    """
    Time a phase and log it in a machine-readable form.

    Localization is not one cost but two -- moving the bytes, then re-reading them to
    hash -- and until now the log reported neither. That made it impossible to answer the
    question that decides whether hashing during the transfer is worth building, or
    whether a smaller instance type would do: how much of the wall clock is the transfer
    and how much is verification.

    The `k9pdl-phase` prefix is there so a harness can parse it without guessing.
    """

    def __init__(self, name, size=None):
        self.name = name
        self.size = size
        self.seconds = None

    def __enter__(self):
        self.start = time.monotonic()
        return self

    def __exit__(self, exc_type, *_):
        self.seconds = time.monotonic() - self.start
        rate = ""
        if self.size and self.seconds > 0:
            rate = ", {:.1f} MB/s".format(self.size / 1e6 / self.seconds)
        log("k9pdl-phase {} {:.1f}s{}{}".format(
            self.name, self.seconds, rate, "" if exc_type is None else " (failed)"))
        return False


def verify(path, options, manifest=None):
    """
    Verification is an absolute guarantee, not a best-effort optimization: when a hash
    was supplied the file is checked before the done marker is written, and being
    unable to check is a hard failure rather than a silent pass.

    Returns the digest that was verified, or None when no check was requested.

    Note what this costs on the in-place route: it is a full read-back of the object.
    The per-part digests computed during the transfer live on BucketChunkSink, so only
    the bucket-compose route can skip this pass -- on the in-place route a 300 GB object is
    downloaded and then read again to hash it. `multipart_etag` at least parallelizes that
    read; a whole-file md5 cannot be parallelized at all, being inherently sequential over
    the byte stream.
    """
    if options.check_etag and options.part_length:
        if manifest is not None:
            # Digests recorded as the bytes went past; reads only the parts that lack
            # one. On the happy path that is none of them.
            actual, reread = multipart_etag_from_manifest(
                path, options.part_length, manifest)
            log("etag from {} recorded part digests, {} re-read".format(
                (os.path.getsize(path) + options.part_length - 1) //
                options.part_length - reread, reread))
        else:
            # A small fixed count, not `connections` -- see VERIFY_READ_WORKERS for the
            # two measured effects that put the optimum at 2.
            actual = multipart_etag(path, options.part_length,
                                    workers=VERIFY_READ_WORKERS)
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


def multipart_etag_from_gcs(client, bucket, name, part_length, size, manifest):
    """
    The source's multipart ETag, from digests recorded as the bytes were relayed.

    The bucket-compose route has no local file to re-read -- the VM is a pure relay --
    so the in-place route's `verify()` cannot be reused here. What can be reused is the
    reason it is cheap: every S3 part's md5 was computed in flight and persisted to the
    manifest, so on the happy path this reads nothing and issues no GET at all.

    Parts that were not recorded -- the ones straddling a preemption's resume point --
    are read back individually from the composed object. Per part, not all-or-nothing:
    after one preemption that is a handful of 29 MiB ranged reads instead of a 279 GiB
    download.

    This does NOT stand alone as proof the object is correct. It establishes that the
    bytes *relayed* match the source's ETag; that those bytes are what GCS actually
    stored is established separately, before compose, by comparing each part's recorded
    md5 against the md5 GCS reports for the uploaded object. Both checks together are
    what make the read-back unnecessary -- which is why the pre-compose check must not
    be skipped when a digest happens to be missing.
    """
    if size == 0:
        return None, 0
    n_parts = (size + part_length - 1) // part_length

    digests = [None] * n_parts
    reread = 0
    for index in range(n_parts):
        recorded = manifest.part_md5(index) if manifest is not None else None
        if recorded:
            try:
                digests[index] = binascii.unhexlify(recorded)
                continue
            except (binascii.Error, ValueError):
                pass
        start = index * part_length
        end = min(start + part_length, size)
        payload = client.download_range(bucket, name, start, end)
        if len(payload) != end - start:
            raise TransientError(
                "short read hashing part {} of {}: {} of {} bytes".format(
                    index, name, len(payload), end - start))
        digests[index] = hashlib.md5(payload).digest()
        reread += 1

    return "{}-{}".format(
        hashlib.md5(b"".join(digests)).hexdigest(), n_parts), reread


class GzipStreamDecoder:
    """
    Incremental gzip decoder that spans member boundaries.

    `zlib.decompressobj(16 + MAX_WBITS)` decodes exactly one gzip member and then sets
    `eof`, leaving everything after it in `unused_data`. Concatenated members are a
    perfectly valid gzip stream, though, and are how bgzip writes -- so a decoder that
    stopped at the first member would silently truncate a BAM or a bgzipped VCF to its
    first block and report success. `gzip.open`, which the in-place route uses, handles
    this for free; here it has to be done by hand.

    Trailing NUL padding after the last member is ignored, matching the reference
    implementation: some writers pad to a block boundary, and those zeros are not a
    malformed member.

    **A stream that ends mid-member is an error.** `decompressobj` does not raise on
    truncation -- it returns whatever it decoded and leaves `eof` false -- so the first
    version of this class decoded two thirds of a truncated file and returned it as the
    whole thing. `gzip.open`, which the in-place route uses, raises EOFError on the same
    bytes, so the bucket route was strictly weaker. It matters exactly when there is no
    advertised digest: with one, the md5 check on the compressed sidecar catches the
    truncation first; without one, an upstream file cut short by an interrupted upload
    decoded into a short destination and exited 0.
    """

    def __init__(self):
        self._decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
        self.produced = 0
        # True while the current member has consumed input but not reached its end.
        self._mid_member = False

    def feed(self, payload):
        out = bytearray()
        data = payload
        while data:
            out += self._decoder.decompress(data)
            if not self._decoder.eof:
                self._mid_member = True
                break
            self._mid_member = False
            data = self._decoder.unused_data
            self._decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
            if not data.strip(b"\x00"):
                break
        self.produced += len(out)
        return bytes(out)

    def flush(self):
        tail = self._decoder.flush()
        self.produced += len(tail)
        if self._mid_member:
            raise IntegrityError(
                "gzip stream ended before the end-of-stream marker was reached "
                "({} bytes decoded); the object is truncated".format(self.produced))
        return tail


class DecodeSliceUploader:
    """
    Write the decoded stream through `width` concurrent resumable sessions.

    A resumable session requires strictly advancing offsets, so one session cannot be
    parallelised at all -- measured at 81.3 MB/s against 541.6 for eight. The way past
    that is the way the relay already goes: cut the stream into slices, give each slice
    its own session, and compose them at the end.

    Slices are buffered whole, which is what makes every PUT carry a real total. The
    `total=None` protocol exists for a length that is not known in advance; a slice's
    length is known the moment it is cut.

    **The output is a composite and therefore has no md5Hash**, where the single-session
    path produces a plain object that does. Nothing on this route verifies that digest
    -- the advertised one covers the compressed bytes, so the decoded object has never
    had a checkable hash -- but it is a real difference in what a consumer can read off
    the metadata, and `--decode-upload-width 1` is the way back.
    """

    def __init__(self, client, bucket, prefix, width, slice_bytes=DECODE_SLICE_BYTES,
                 queue_slices=DEFAULT_DECODE_QUEUE_SLICES):
        self.client = client
        self.bucket = bucket
        self.prefix = prefix
        self.slice_bytes = slice_bytes
        self.total = 0
        # A list of decoded chunks, joined exactly once per slice. The first version
        # accumulated into a bytearray and then re-materialised the slice out of it,
        # which copied every byte three times over -- into the bytearray, out of it, and
        # once more at the call site. At a VCF ratio that is 27 GB of memcpy for a 9 GB
        # object, on the same thread as the inflater.
        self._parts = []
        self._pending = 0
        self._names = []
        self._futures = []
        self._pool = ThreadPoolExecutor(max_workers=width,
                                        thread_name_prefix="k9pdl-decode-write")
        # Backpressure. Without it, submitting every slice to the pool queues them all
        # WITH their payloads attached and the whole object ends up resident -- the pool
        # bounds concurrency, never memory. The read-ahead had the same trap.
        # MORE SLOTS THAN WORKERS. This is what lets the inflater run ahead: extra
        # slices sit in the pool's queue, already assembled, while `width` of them
        # upload. With slots == width the inflater blocked on every slice after the
        # first `width`, which is why the decode measured as inflate + write with no
        # overlap at all. A separate feeder thread was the first attempt at this and
        # was not needed -- the pool already has a queue.
        self._slots = threading.Semaphore(width + queue_slices)

    def _send(self, name, body):
        session = self.client.start_resumable_upload(self.bucket, name)
        offset = 0
        while offset < len(body):
            metadata, committed = self.client.upload_range(
                session, body[offset:], offset, len(body))
            if metadata is not None:
                return
            if committed is None or committed <= offset:
                raise TransientError(
                    "slice {} stalled at {}/{}".format(name, offset, len(body)))
            offset = committed

    def _take(self, count):
        """Pull exactly `count` bytes off the front of the chunk list, one join."""
        taken, got = [], 0
        while got < count:
            chunk = self._parts.pop(0)
            need = count - got
            if len(chunk) > need:
                taken.append(chunk[:need])
                self._parts.insert(0, chunk[need:])   # one small copy, once per slice
                got += need
            else:
                taken.append(chunk)
                got += len(chunk)
        self._pending -= count
        return taken[0] if len(taken) == 1 else b"".join(taken)

    def _emit(self, count):
        body = self._take(count)
        name = "{}.{:05d}".format(self.prefix, len(self._names))
        self._names.append(name)
        self.total += len(body)
        self._slots.acquire()               # blocks the inflater once `width` are busy
        future = self._pool.submit(self._send, name, body)
        future.add_done_callback(lambda _f: self._slots.release())
        self._futures.append(future)

    def feed(self, data):
        if not data:
            return
        self._parts.append(data)
        self._pending += len(data)
        while self._pending >= self.slice_bytes:
            self._emit(self.slice_bytes)

    def finish(self):
        """Flush the tail, wait for every slice, and return their names in order."""
        if self._pending:
            self._emit(self._pending)
        try:
            for future in self._futures:
                future.result()             # re-raises the first upload failure
        finally:
            self._pool.shutdown(wait=True)
        return list(self._names), self.total

    @property
    def names(self):
        """Every slice name this uploader has issued, uploaded or not."""
        return list(self._names)

    def abandon(self, delete):
        """
        Stop after a failure, and remove the slices already written only if `delete`.

        No default, so every caller has to decide. Slice names are deterministic, so
        two writers decoding the same object share them. Deleting on a RETRYABLE
        failure therefore let one writer's transient error pull slices out from under
        another writer's compose -- one hiccup failing two decodes. On a retry the next
        attempt rewrites the same names and deletes them on its way out, so keeping them
        costs nothing. Delete only when the failure is an integrity failure, where the
        source bytes are wrong and every writer fails alike.

        Queued-but-unstarted uploads are cancelled either way; they would otherwise keep
        running on pool threads after the decode had already given up.
        """
        self._pool.shutdown(wait=True, cancel_futures=True)
        if delete:
            best_effort_delete(self.client, self.bucket, self._names)


def ranged_blocks(client, bucket, name, size, block, depth):
    """
    Yield an object's blocks in order, with up to `depth` ranged GETs in flight.

    Order is preserved because the consumer is a gzip inflater and gzip is sequential.
    Only the fetch is parallel -- which is the point: the read was measured at a third
    of what the same object yields once warm, and a read-ahead attacks that without
    touching the part that cannot be parallelised.

    Bounded at `depth` outstanding requests, so peak memory is `depth * block` whatever
    the object's size. `depth <= 1` is the plain sequential loop, kept so the two can be
    compared on one code path rather than two.
    """
    offsets = iter(range(0, size, block))
    if depth <= 1:
        for start in offsets:
            yield client.download_range(bucket, name, start, min(start + block, size))
        return

    def fetch(start):
        return pool.submit(client.download_range, bucket, name,
                           start, min(start + block, size))

    with ThreadPoolExecutor(max_workers=depth,
                            thread_name_prefix="k9pdl-decode-read") as pool:
        inflight = collections.deque()
        for start in offsets:
            inflight.append(fetch(start))
            if len(inflight) >= depth:
                break
        while inflight:
            payload = inflight.popleft().result()
            start = next(offsets, None)
            if start is not None:
                inflight.append(fetch(start))
            yield payload


def decompress_object(client, bucket, source, dest, block=READ_BUFFER,
                      granularity=GCS_UPLOAD_GRANULARITY,
                      readahead=DEFAULT_DECODE_READAHEAD,
                      upload_width=DEFAULT_DECODE_UPLOAD_WIDTH,
                      queue_slices=DEFAULT_DECODE_QUEUE_SLICES):
    """
    Stream `source` through gunzip into a new object `dest`, entirely in the bucket.

    The in-place route's equivalent is `gunzip_to`, which has a local file to rewrite.
    This route has none, so the object is read back in ranges, decoded in flight, and
    written into a fresh resumable upload. Nothing touches disk and nothing is buffered
    beyond one block, which matters because these objects run to hundreds of gigabytes.

    The decompressed length cannot be known in advance -- gzip's ISIZE is modulo 2**32,
    so it is unusable above 4 GiB, and measuring it honestly would mean decompressing
    twice -- so every PUT but the last declares its total as `*`. That is the whole
    reason `upload_range` accepts `total=None`. GCS only accepts an unknown-total PUT
    whose length is a multiple of 256 KiB, which is why output is buffered to
    `granularity` before it is sent.

    Returns `(written, decoded)`. `decoded` is False when the bytes were kept as
    received, for the same two reasons `gunzip_to` keeps them:

      * the object is not gzip at all, despite the advertised Content-Encoding;
      * `dest`'s name promises a gzip-family format and decoding did not produce it,
        meaning the metadata was set on an object that was only compressed once and the
        stored bytes already are what the name promises.

    Both are decided from the first decoded bytes, before any upload starts, so the
    fallback costs one server-side compose rather than a discarded upload.
    """
    size = int(client.get_object(bucket, source).get("size", 0))
    magic = expected_magic(dest)
    decoder = GzipStreamDecoder()

    session = None
    uploader = (DecodeSliceUploader(client, bucket, dest + ".k9pdl.slice", upload_width,
                                    queue_slices=queue_slices)
                if upload_width > 1 else None)
    pending = bytearray()
    offset = 0
    read_at = 0
    checked = magic is None

    # The three stages run strictly in sequence, so each one's total is time the other
    # two spent idle. Measured rather than assumed because the obvious fix -- pipelining
    # them -- was already tried on the relay and REVERTED at -6.2%: that loop turned out
    # 95% write-bound, overlap ceiling 1.05x, so there was nothing to hide. Whether this
    # loop is shaped the same way is an open question, and one nobody should answer by
    # reasoning about it. See k9pdl-io for the relay's equivalent.
    stage = {"read": 0.0, "inflate": 0.0, "write": 0.0}
    blocks = 0

    def keep_as_is(reason):
        # A single-source compose is a server-side copy: no bytes move, and the
        # intermediate is deleted by the caller exactly as it would have been.
        log(reason)
        client.compose(bucket, dest, [source])
        return size, False

    def decide():
        # Called once enough output exists to compare, or at end of stream.
        if bytes(pending[:len(magic)]) == magic:
            return True
        keep_as_is(
            "{} is named for {} content but decoding did not produce it; the "
            "content-encoding metadata is set on a singly-compressed object, so the "
            "stored bytes are kept as-is".format(
                dest.rsplit("/", 1)[-1], magic[:4]))
        return False

    # With a read-ahead, `stage["read"]` is time the decode spent BLOCKED on a block,
    # not time spent fetching -- fetching overlaps itself. That is the number that
    # matters for the pipeline arithmetic, but it is not comparable to a depth-1 run's
    # read total, so the depth is logged alongside it.
    stream = ranged_blocks(client, bucket, source, size, block, readahead)
    try:
        while read_at < size:
            mark = time.monotonic()
            payload = next(stream, None)
            stage["read"] += time.monotonic() - mark
            blocks += 1
            if not payload:
                raise TransientError("read-back returned nothing at {}".format(read_at))
            read_at += len(payload)

            produced = b""
            buffered = False
            try:
                mark = time.monotonic()
                produced = decoder.feed(payload)
                stage["inflate"] += time.monotonic() - mark
                if uploader is None or not checked:
                    # Still deciding the magic question, so it has to be inspectable.
                    pending += produced
                    buffered = True
            except zlib.error as e:
                if session is not None or decoder.produced:
                    # Past the header and part-way through a stream: this is corruption,
                    # not mislabelled metadata, and the caller must not publish a truncation.
                    raise IntegrityError(
                        "could not decompress gs://{}/{}: {}".format(bucket, source, e))
                # The server advertised Content-Encoding: gzip over bytes that are not
                # gzip. Keep them rather than failing the localization over the server's
                # metadata being wrong -- same call gunzip_to makes on BadGzipFile.
                return keep_as_is(
                    "gs://{}/{} was advertised as gzip-encoded but is not gzip ({}); "
                    "keeping the bytes as received".format(bucket, source, e))

            if not checked and len(pending) >= len(magic):
                if not decide():
                    return size, False
                checked = True

            if not checked:
                continue

            if uploader is not None:
                # `pending` is a bytearray only because the magic check needs to look at
                # the first bytes. Once that is settled the decoder's own output goes
                # straight to the slicer -- `bytes(pending)` here copied every byte of a
                # 9 GB object for nothing, on the inflater's thread.
                mark = time.monotonic()
                if pending:
                    uploader.feed(bytes(pending))
                    del pending[:]
                if not buffered:
                    # `buffered` guards the hand-off iteration: when `checked` flips
                    # true on THIS pass, `produced` is already inside `pending` and
                    # feeding it again duplicates a block. Only the magic-check family
                    # reaches that state, which is why it showed up on .bam/.bgz alone.
                    uploader.feed(produced)
                stage["write"] += time.monotonic() - mark
                continue

            # Only whole granules may be sent while the total is still unknown.
            sendable = (len(pending) // granularity) * granularity
            if sendable:
                mark = time.monotonic()
                if session is None:
                    session = client.start_resumable_upload(bucket, dest)
                chunk = bytes(pending[:sendable])
                del pending[:sendable]
                _, committed = client.upload_range(session, chunk, offset, None)
                stage["write"] += time.monotonic() - mark
                if committed is None or committed <= offset:
                    # A granule-aligned PUT that persists nothing is not progress, and
                    # silently buffering the chunk to try again would grow `pending` by the
                    # whole decompressed object. Requeue instead; the parts survive, so the
                    # retry repeats only the decode.
                    raise TransientError(
                        "upload session persisted nothing at {}".format(offset))
                # GCS is authoritative about what persisted, and it is not obliged to be
                # everything that was sent. Whatever it did not take goes back on the front
                # of the buffer and is re-sent with the next granule.
                if committed < offset + len(chunk):
                    pending[:0] = chunk[committed - offset:]
                offset = committed

    except BaseException as exc:
        # Stop the uploader, or its pool keeps writing slices after the decode has
        # already failed. Only a corrupt stream justifies deleting what it wrote.
        if uploader is not None:
            uploader.abandon(delete=isinstance(exc, IntegrityError))
            exc.k9pdl_orphans = uploader.names
        raise
    finally:
        # Shuts the pool down; at most `readahead` blocks are still in flight,
        # which matters on the keep-as-is paths that return from inside the loop.
        stream.close()
    mark = time.monotonic()
    try:
        pending += decoder.flush()
    except IntegrityError:
        # Outside the loop's handler, so it has to stop the uploader itself -- and a
        # truncated stream is corrupt, so what it wrote goes too.
        if uploader is not None:
            uploader.abandon(delete=True)
        raise
    stage["inflate"] += time.monotonic() - mark
    if not checked and not decide():
        return size, False

    if uploader is not None:
        mark = time.monotonic()
        try:
            uploader.feed(bytes(pending))
            names, total = uploader.finish()
            composed = compose_tree(client, bucket, dest, names)
        except BaseException as exc:
            uploader.abandon(delete=False)
            # Kept for a retry, but the caller may discover it lost the race to a
            # writer that already finished -- in which case nobody will retry and these
            # are orphans. Only the caller can tell, so the names travel with the error.
            exc.k9pdl_orphans = uploader.names
            raise
        best_effort_delete(client, bucket, names)
        stage["write"] += time.monotonic() - mark
        if int(composed.get("size", -1)) != total:
            raise TransientError("decoded object is {} bytes, expected {}".format(
                composed.get("size"), total))
        report_decode_stages(stage, blocks, size, total, readahead, upload_width,
                             len(names))
        return total, True

    total = offset + len(pending)
    mark = time.monotonic()
    if session is None:
        session = client.start_resumable_upload(bucket, dest)
    while offset < total:
        metadata, committed = client.upload_range(
            session, bytes(pending), offset, total)
        if metadata is not None:
            stage["write"] += time.monotonic() - mark
            report_decode_stages(stage, blocks, size, total, readahead)
            return total, True
        if committed is None or committed <= offset:
            raise TransientError(
                "final upload made no progress at {}/{}".format(offset, total))
        del pending[:committed - offset]
        offset = committed
    # Everything is sent but no PUT completed the session -- either there was nothing to
    # send at all, or the last one committed its bytes with a 308. A bodiless PUT naming
    # the total finalizes it, which is exactly what session_offset sends.
    client.session_offset(session, total)
    stage["write"] += time.monotonic() - mark
    report_decode_stages(stage, blocks, size, total, readahead)
    return total, True


def report_decode_stages(stage, blocks, compressed, decompressed,
                         readahead=1, upload_width=1, slices=0):
    """
    Emit the decode's read/inflate/write split, in the shape `k9pdl-io` uses.

    The ceiling is `total / max(stage)`, exactly as in `k9pdl-io` and for the same
    reason: pipelining turns a serial `read then inflate then write` into
    `max(read, inflate, write)`, so it can hide the smaller stages behind the largest
    one and no more. Bounded by 3.0 here rather than 2.0, at a perfect three-way tie.
    The 1/(1-share) form is wrong -- it assumes the hidden stages become free, and
    reports infinity when one of them is already negligible.

    `pipe2` is the ceiling for the cheap version of that fix -- one thread reading and
    inflating, another uploading -- which is worth separating out because the inflate is
    CPU and the other two are network, so the two-thread split is the one that does not
    need a third queue. If `pipe2` and the full ceiling are close, the third thread buys
    nothing.
    """
    # No zero guard: every path that reaches here has timed at least the final flush and
    # the PUT that finalized the session, so the total is positive by construction. A
    # guard would only look reassuring -- the case it appears to cover, keep-as-is,
    # returns from decompress_object without calling this at all.
    total = stage["read"] + stage["inflate"] + stage["write"]
    slowest = max(stage.values())
    two_stage = max(stage["read"] + stage["inflate"], stage["write"])
    log("k9pdl-gunzip read {:.3f}s inflate {:.3f}s write {:.3f}s over {} blocks "
        "(readahead {}, width {}, slices {}, {} -> {} bytes, "
        "ceiling {:.2f}x, pipe2 {:.2f}x)".format(
            stage["read"], stage["inflate"], stage["write"], blocks, readahead,
            upload_width, slices,
            compressed, decompressed,
            total / slowest if slowest > 0 else 1.0,
            total / two_stage if two_stage > 0 else 1.0))


def verify_bucket_object(client, bucket, name, size, options, block=VERIFY_READBACK_BLOCK):
    """
    Verify a composed object against the source's declared hash by reading it back.

    This is the expensive case, and it is accepted rather than avoided: check_hash is an
    absolute guarantee, so being unable to verify cheaply forces the read-back rather
    than yielding a silent pass. Same-region GCS-to-GCE egress is free and fast, which
    is what makes it tolerable.

    A composite object has no md5Hash of its own, which is exactly why the stored
    metadata cannot be used here.

    Read through the same bounded read-ahead the decode uses. This is the other full
    sequential read-back on this route and it was the larger one -- roughly 26 s of a
    54 s `--gunzip` run, unattributed because it was not wrapped in a phase. It reads a
    just-composed object, so it pays the cold-read penalty in full: 33-40 MB/s
    sequential against 88-96 warm, and 140 at depth 4.

    md5 is order-dependent, which is fine: `ranged_blocks` yields strictly in order and
    only the fetching overlaps.
    """
    digest = hashlib.md5()
    offset = 0
    stream = ranged_blocks(client, bucket, name, size, block,
                           getattr(options, "decode_readahead",
                                   DEFAULT_DECODE_READAHEAD))
    try:
        while offset < size:
            payload = next(stream, None)
            if not payload:
                raise TransientError("read-back returned nothing at {}".format(offset))
            digest.update(payload)
            offset += len(payload)
    finally:
        stream.close()

    actual = digest.hexdigest()
    expected = normalize_expected_md5(options.check_md5)
    if actual != expected:
        raise IntegrityError(
            "md5 mismatch after compose: expected {}, got {}".format(expected, actual)
        )
    return actual


# How far GCS's timeCreated may trail this node's clock and still count as "during this
# attempt". GCE clocks are NTP-synced to Google's own time source, so the real skew is
# well under a second.
#
# Deliberately small. The first value was five minutes, "as margin", and a generous
# margin is not free: it lets THIS job's own previous attempt -- restarted quickly after
# a requeue -- pass for a racing winner, and that attempt's object may be a compose that
# was never verified. A requeue cycle takes far longer than five seconds.
LANDED_CLOCK_SKEW = 5


def _gcs_timestamp(value):
    """GCS RFC 3339 timestamp -> epoch seconds, or None if absent or unparseable."""
    import datetime
    if not value:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.datetime.strptime(value, fmt).replace(
                tzinfo=datetime.timezone.utc).timestamp()
        except ValueError:
            continue
    return None


def landed_during(client, bucket, name, expected_size, since):
    """
    The destination another writer completed while this attempt was running, or None.

    Presence is not enough, and the first version of the compose-race fix accepted it:
    an object at the destination may be a stale one from an earlier localization of a
    different source, and returning EXIT_OK over it reports success for the wrong bytes
    with nothing downstream able to tell. So the object must also have been CREATED
    after this attempt started. That is exactly the race window -- a writer that
    finished before this attempt began also deleted the manifest, so this attempt would
    have started fresh and composed its own parts rather than lose a race.

    `expected_size` is checked when known; with --gunzip the decoded length is not, and
    the creation time is then the only evidence.

    Callers must not use this once they have composed the destination themselves: the
    object is then their own, and this function cannot tell it from a winner's. See
    `lost_race` in run_bucket_route.
    """
    try:
        metadata = client.get_object(bucket, name)
    except (PermanentError, TransientError):
        return None
    if expected_size is not None and int(metadata.get("size", -1)) != expected_size:
        return None
    created = _gcs_timestamp(metadata.get("timeCreated"))
    if created is None or created < since - LANDED_CLOCK_SKEW:
        return None
    return metadata


def best_effort_delete(client, bucket, names):
    """
    Delete cleanup objects without letting a transient error fail the job.

    Every one of these calls runs after the work is done. `GcsClient.request` does not
    retry, so a single 503 on part 17 of a composed, verified object used to escape as
    TransientError, which `main` does not catch -- the generic handler turned a finished
    localization into EXIT_FAIL, with no done marker written. One retry, then leave the
    orphan (it is under a dotted prefix nothing globs) and say so.
    """
    orphaned = []
    for name in names:
        for attempt in range(2):
            try:
                client.delete_object(bucket, name)
                break
            except TransientError:
                if attempt:
                    orphaned.append(name)
                else:
                    time.sleep(0.5)
    if orphaned:
        log("could not delete {} cleanup object(s), left in place: {}{}".format(
            len(orphaned), ", ".join(orphaned[:3]), " ..." if len(orphaned) > 3 else ""))
    return orphaned


def run_bucket_route(options, decision, source, size, chunks, plan_id, manifest_path,
                     marker_path):
    """
    The bucket-compose route: upload each chunk as its own object through a
    resumable session, compose them server-side, verify, then delete the parts.

    Preferred over stage-then-publish whenever the destination bucket can be addressed,
    because compose transfers no object data -- so there is no sequential tail and the
    entire transfer stays parallel end to end.
    """
    bucket, object_name = split_gs_url(decision.gs_url)
    client = GcsClient(timeout=options.timeout)
    parts_prefix = "{}.k9pdl.parts".format(object_name)
    # Anchors "did another writer finish during this attempt?" -- see landed_during().
    # Wall clock, deliberately -- the one place here that is a timestamp, not a duration:
    # landed_during() compares it against GCS's timeCreated. Every duration uses
    # time.monotonic(), which a clock step (NTP) cannot make negative or inflate.
    attempt_started = time.time()

    # With --gunzip the parts compose into a compressed sidecar, not into the destination:
    # the advertised checksum covers the compressed bytes, so they have to be verified
    # before anything is decoded. Same ordering, and the same sidecar name, as the
    # in-place route -- verify what was received, then transform it.
    compose_target = "{}.k9pdl.gz".format(object_name) if options.gunzip else object_name

    # The manifest lives in the bucket, next to the destination object, so it travels with
    # the data and is committed by a single atomic media upload. It deliberately does NOT
    # go through the mount: on a flat-namespace bucket the filesystem manifest's
    # tmp-plus-rename commit is not atomic, since rename there is a copy followed by a
    # delete -- and this route exists precisely because that filesystem misbehaves.
    manifest_object = "{}.k9pdl.json".format(object_name)
    manifest = (None if options.no_resume
                else GcsManifest.load(client, bucket, manifest_object))
    chunk_size = (chunks[0][1] - chunks[0][0]) if chunks else size
    if manifest is not None and not manifest.matches(plan_id, None, size):
        log("manifest describes a different object or layout; restarting")
        manifest.unlink()
        manifest = None
    if manifest is None:
        manifest = GcsManifest.create(
            client, bucket, manifest_object, plan_id, size, chunk_size, chunks
        )
    else:
        log("resuming: {}/{} parts already complete".format(
            sum(1 for i in range(len(chunks)) if manifest.is_complete(i)), len(chunks)))

    sink = BucketChunkSink(
        client, bucket, parts_prefix, manifest, chunks,
        part_length=options.part_length if options.check_etag else None, size=size,
        upload_block=getattr(options, "upload_block", None))
    downloader = Downloader(source, sink, manifest, chunks, options, Progress(size))

    try:
        with phase("relay", size):
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

    # Where a writer's cleanup can race another writer's work, the loser sees an object
    # it expected vanish. Every such site funnels through here rather than deciding for
    # itself, because all of them used to decide wrong: a vanished part, sidecar or
    # part-under-re-read surfaced as PermanentError, and `main` turns that into
    # EXIT_FAIL -- do not retry -- on a job whose output another writer had just
    # produced. Two workers on one object is designed for (LOCALIZATION.md §3), and a
    # node dying mid-upload produces the same overlap with no timeout involved.
    def lost_race(what, manifest_is_stale, after_own_compose=False, orphans=()):
        log("{}; checking whether another writer finished during this attempt".format(what))
        # Without --gunzip the compose target IS the destination, so once this attempt
        # has composed, the object there may be its own unverified compose -- and
        # accepting it would skip verification outright. Worse, a winner's integrity
        # cleanup deletes the object and then the parts; a compose landing between the
        # two re-creates the corrupt object, and the vanished parts then look like a
        # lost race won by someone else. Presence proves nothing in that state, so it is
        # not consulted. With --gunzip the destination is only ever written at the end
        # of a decode that followed a passing verify, so presence is still evidence.
        can_trust = options.gunzip or not after_own_compose
        landed = (landed_during(client, bucket, object_name,
                                None if options.gunzip else size, attempt_started)
                  if can_trust else None)
        if landed is not None:
            log("{} was completed by another writer ({} bytes); nothing to do".format(
                decision.gs_url, landed.get("size")))
            # Anything this writer re-uploaded after the winner's cleanup is an orphan.
            # Never the destination itself, which is the winner's.
            best_effort_delete(client, bucket, [n for n in part_names + [compose_target]
                                                + list(orphans) if n != object_name])
            manifest.unlink()
            return EXIT_OK
        if manifest_is_stale:
            # The manifest now records completions for bytes that no longer exist.
            # Kept, it would send every later attempt down this same path -- skip the
            # relay, find the part missing, requeue -- forever: canine requeues exit 5
            # with no cap (orchestrator.py counts localization failures only to exclude
            # them from the preemption limit). Dropping it costs one re-download.
            manifest.unlink()
        log("no completed object landed during this attempt; requesting requeue")
        return EXIT_REQUEUE

    def integrity_failure(reason):
        # The object goes FIRST, then the parts. Deleting parts first opened a window
        # in which a racing writer's compose fails, lost_race() finds a fresh object of
        # exactly the right size -- this known-corrupt one -- and reports EXIT_OK for it
        # moments before it is deleted.
        log(reason)
        best_effort_delete(client, bucket, [compose_target])
        best_effort_delete(client, bucket, part_names)
        manifest.unlink()
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
            return lost_race("part {} is missing after upload".format(index),
                             manifest_is_stale=True)
        except TransientError as e:
            log("transient failure checking part {}; requesting requeue: {}".format(
                index, e))
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

    try:
        with phase("compose"):
            composed = compose_tree(client, bucket, compose_target, part_names)
    except PermanentError as e:
        return lost_race("compose failed ({})".format(e), manifest_is_stale=True)
    except TransientError as e:
        log("transient failure composing; requesting requeue: {}".format(e))
        return EXIT_REQUEUE
    if int(composed.get("size", -1)) != size:
        log("composed object is {} bytes, expected {}".format(composed.get("size"), size))
        return EXIT_FAIL

    digest = None
    if options.check_md5:
        try:
            with phase("verify", size):
                digest = verify_bucket_object(
                    client, bucket, compose_target, size, options)
        except IntegrityError as e:
            return integrity_failure("verification failed: {}".format(e))
        except PermanentError as e:
            return lost_race("verify read-back failed ({})".format(e),
                             manifest_is_stale=False, after_own_compose=True)
        except TransientError as e:
            log("transient failure verifying; requesting requeue: {}".format(e))
            return EXIT_REQUEUE
    elif options.check_etag and options.part_length:
        try:
            actual, reread = multipart_etag_from_gcs(
                client, bucket, compose_target, options.part_length, size, manifest)
        except PermanentError as e:
            # Only a part missing its recorded digest is re-read, and a part that is
            # gone was deleted by another writer.
            return lost_race("ETag re-read failed ({})".format(e),
                             manifest_is_stale=True, after_own_compose=True)
        except TransientError as e:
            log("transient failure computing the ETag; requesting requeue: {}".format(e))
            return EXIT_REQUEUE
        log("etag from {} recorded part digests, {} re-read".format(
            (size + options.part_length - 1) // options.part_length - reread, reread))
        expected = options.check_etag.strip().strip('"')
        if actual != expected:
            # Cleaned up like an md5 mismatch. It used to return with the corrupt
            # composed object left at the destination, readable through the mount.
            return integrity_failure("ETag mismatch after compose: expected {}, got {}"
                                     .format(expected, actual))
        digest = actual
    elif options.check_etag:
        # An ETag without a part length is an opaque string, not an md5-of-md5s, so
        # there is nothing to reproduce. Refusing beats passing silently.
        log("ETag verification needs --part-length to reproduce an md5-of-md5s")
        return EXIT_FAIL

    # Decode before anything is cleaned up. The parts and the compressed sidecar are the
    # only copy of the transferred bytes, so if a preemption lands mid-decompress the
    # next attempt repeats compose, verify and decode -- never the download. An
    # unfinished resumable upload is invisible in the bucket, so there is no half-written
    # destination for that attempt to trip over.
    stored_size = None
    if options.gunzip:
        try:
            with phase("gunzip", size):
                stored_size, decoded = decompress_object(
                    client, bucket, compose_target, object_name,
                    readahead=getattr(options, "decode_readahead",
                                      DEFAULT_DECODE_READAHEAD),
                    upload_width=getattr(options, "decode_upload_width",
                                         DEFAULT_DECODE_UPLOAD_WIDTH),
                    queue_slices=getattr(options, "decode_queue_slices",
                                         DEFAULT_DECODE_QUEUE_SLICES))
        except IntegrityError as e:
            # The compressed bytes matched the advertised digest and still do not
            # decode, so the source object itself is broken and no retry can help.
            return integrity_failure("{}".format(e))
        except PermanentError as e:
            # A 404 here is the sidecar or a slice removed by another writer's cleanup,
            # not corruption -- it used to be reported as "the source is broken".
            return lost_race("decode failed ({})".format(e), manifest_is_stale=False,
                             after_own_compose=True,
                             orphans=getattr(e, "k9pdl_orphans", ()))
        except TransientError as e:
            # Requeue, and deliberately leave the parts, the sidecar and the manifest
            # in place: the retry then pays only for the decode.
            log("transient failure decompressing; requesting requeue: {}".format(e))
            return EXIT_REQUEUE
        if decoded:
            log("decompressed {} bytes into {} bytes".format(size, stored_size))
        best_effort_delete(client, bucket, [compose_target])

    # Parts are deleted explicitly. The GCS JSON API's objects.compose has no
    # deleteSourceObjects parameter (contrary to the design note), so a crash between
    # compose and here leaves orphaned parts; they are under a dotted prefix so nothing
    # globs them up. Best-effort: the object is finished, and a cleanup hiccup must not
    # turn that into a failure.
    best_effort_delete(client, bucket, part_names)

    try:
        write_done_marker(marker_path, size, plan_id, digest, route=ROUTE_BUCKET,
                          gs_url=decision.gs_url, stored_size=stored_size)
    except OSError as e:
        # The marker lives on the gcsfuse mount, and it is an optimisation -- it lets a
        # re-run skip the transfer. The object is complete and verified; losing the
        # marker costs a redundant transfer if this localization is ever re-run, and
        # failing the job over it would cost the whole localization now.
        log("could not write the done marker ({}); the object is complete".format(e))
    manifest.unlink()
    log("complete: {} bytes composed from {} parts{}".format(
        size, len(part_names), " (verified)" if digest else ""))
    return EXIT_OK


# --------------------------------------------------------------------------------
# The stage-publish route: stage on a real block device, then publish
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
    The stage-publish route: chunk-download onto a real block device, verify there,
    then publish with a single sequential copy.

    The generic fallback for a non-POSIX destination that is not a resolvable bucket.
    Strictly worse than the bucket-compose route -- it pays a full sequential publish and
    puts up to a whole file of staged work at risk -- which is why the bucket-compose route
    is preferred whenever the destination can be addressed directly.
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
        with phase("download", size):
            status, manifest = download_to_local_file(
                options, source, size, chunks, chunk_size, plan_id, staged,
                staged_manifest, probe_seek_hole(staging),
            )
        if status != EXIT_OK:
            return status

        # Verify BEFORE publishing, so a corrupt download never costs an upload.
        try:
            with phase("verify",
                       size if (options.check_md5 or options.check_etag) else None):
                digest = verify(staged, options, manifest)
        except PermanentError as e:
            log("verification failed on the staged copy: {}".format(e))
            discard(staged, manifest)
            return EXIT_FAIL

        write_done_marker(staged_marker, size, plan_id, digest)
        if manifest is not None:
            manifest.unlink()

    try:
        with phase("publish", size):
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
    # Timed as a "download" phase like every other route, so a benchmark comparing this
    # baseline against the parallel paths compares the same thing. Without it the
    # single-stream row is download-only while the others include their verification,
    # which understated the measured speedup by a third.
    if options.legacy_cmd:
        command = options.legacy_cmd
    else:
        # --fail, or curl writes a 403/404 error body to the destination and exits 0.
        # With check_hash on, verification catches that as a mismatch; with it off the
        # error page would be accepted as the file. A clean nonzero exit is strictly
        # better than relying on a hash that may not be configured.
        # --retry: see CURL_FETCH in file_handlers.py, which measured it safe with -C -
        command = fetch_or_exit(
            "curl --fail --retry 5 -C - -sSL{headers} -o {dest} {url}".format(
                headers="".join(
                    " --header {}".format(shlex.quote(h)) for h in (options.header or [])
                ),
                dest=shlex.quote(options.dest),
                url=shlex.quote(options.url),
            )
        )
    with phase("download", options.size if (options.size or 0) > 0 else None):
        result = subprocess.run(command, shell=True, executable=SHELL)
    return result.returncode


# --------------------------------------------------------------------------------
# orchestration
# --------------------------------------------------------------------------------

GZIP_MAGIC = b"\x1f\x8b"

# What a filename promises the DECODED bytes will be.
#
# `.gz` is only one case. A name can imply any already-compressed format, and several
# formats this stack handles are gzip streams by design: BGZF -- used by .bam, .bcf, and
# the .bai/.tbi/.csi indices -- is a valid gzip container so that standard readers work,
# verified locally (bcftools output begins 1f 8b). Leaving those off a gzip-only list is
# what makes the naive version wrong: a .bam served with Content-Encoding: gzip would be
# decompressed into a raw BAM stream, which is not a valid .bam at all.
#
# Necessarily incomplete -- an unlisted extension is treated as promising plain content,
# which is the common case. Both error directions produce a file a downstream tool
# rejects outright rather than silently wrong data.
NAMED_FORMAT_MAGIC = (
    ((".gz", ".gzip", ".z", ".tgz", ".taz", ".bgz", ".bgzf", ".svgz",
      ".bam", ".bai", ".bcf", ".csi", ".tbi"), GZIP_MAGIC),
    ((".bz2", ".tbz", ".tbz2"), b"BZh"),
    ((".xz", ".txz"), b"\xfd7zXZ\x00"),
    ((".zst", ".tzst"), b"\x28\xb5\x2f\xfd"),
    ((".zip", ".jar", ".whl"), b"PK"),
    ((".7z",), b"7z\xbc\xaf\x27\x1c"),
    ((".cram",), b"CRAM"),
    # Columnar/array containers that are NOT gzip streams. They already localized
    # correctly without being listed, but only via the "advertised as gzip yet is not
    # gzip" fallback -- listing them makes the outcome explicit and tested rather than
    # incidental.
    #
    # Their INTERNAL compression is a separate matter and must never be touched: parquet
    # compresses per column chunk (snappy/gzip/zstd) and HDF5 has a per-dataset gzip
    # filter, both inside the container. Only a transport Content-Encoding is unwrapped
    # here. .hdf is deliberately absent: HDF4 has a different signature, so the extension
    # is ambiguous.
    ((".parquet", ".pq"), b"PAR1"),
    ((".h5", ".hdf5"), b"\x89HDF\r\n\x1a\n"),
)


def expected_magic(path):
    """
    The magic bytes `path`'s name implies its content should start with, or None when the
    name implies plain (unencoded) content.
    """
    name = os.path.basename(path).lower()
    for extensions, magic in NAMED_FORMAT_MAGIC:
        if name.endswith(extensions):
            return magic
    return None


def name_implies_gzip(path):
    """Kept for callers that only care about the gzip family."""
    return expected_magic(path) == GZIP_MAGIC


def gunzip_to(source, dest):
    """
    Decompress `source` into `dest`, resumable-by-restart.

    Written to a temp name, fsynced, then atomically renamed, so a preemption mid-decompress
    leaves either no output or complete output -- never a half-decompressed file that a
    later run might mistake for finished. The compressed source is already verified by the
    time this runs, so a re-run repeats only the decompression and never the download.

    This is the same shape as the stage-publish route's publish step: verify the staged
    bytes, then transform them into the destination, then write the marker.

    One refinement when `dest`'s name already promises gzip content (.gz and friends).
    Two very different things produce `Content-Encoding: gzip` on such an object:

      * it was gzipped TWICE -- a .gz file additionally encoded for transport -- in which
        case removing one layer yields the original uploaded .gz, which is what the name
        promises;
      * it was gzipped ONCE and the content-encoding metadata was set by mistake (a common
        slip when uploading an already-compressed file), in which case the stored bytes
        ALREADY are the .gz the name promises, and decompressing would leave decompressed
        data in a file called .gz.

    The two are distinguished by looking at what one layer of decompression yields: if it
    is itself gzip, the object was doubly compressed. Either way the invariant holds --
    the localized file matches what its name says.
    """
    import gzip

    tmp = dest + ".k9pdl.gz.part"
    written = 0
    try:
        with gzip.open(source, "rb") as compressed:
            fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
            try:
                while True:
                    block = compressed.read(READ_BUFFER)
                    if not block:
                        break
                    view = memoryview(block)
                    while view:
                        count = os.write(fd, view)
                        if count <= 0:
                            raise OSError("write returned {}".format(count))
                        written += count
                        view = view[count:]
                os.fsync(fd)
            finally:
                os.close(fd)
    except gzip.BadGzipFile:
        # The server advertised Content-Encoding: gzip over bytes that are not gzip. The
        # stored bytes are therefore the object itself, mislabelled -- keep them rather
        # than failing the localization over the server's metadata being wrong.
        try:
            os.unlink(tmp)
        except OSError:
            pass
        log("{} was advertised as gzip-encoded but is not gzip; keeping the bytes as "
            "received".format(os.path.basename(source)))
        os.replace(source, dest)
        return os.path.getsize(dest)
    except (OSError, EOFError) as e:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise PermanentError("could not decompress {}: {}".format(source, e))

    magic = expected_magic(dest)
    if magic is not None:
        with open(tmp, "rb") as decoded:
            matches = decoded.read(len(magic)) == magic
        if not matches:
            # The decoded bytes are not the format this name promises, so the stored bytes
            # already were that format and the content-encoding metadata was set on an
            # object that was only compressed once. Keep the stored bytes.
            log("{} is named for {} content but decoding did not produce it; the "
                "content-encoding metadata is set on a singly-compressed object, so the "
                "stored bytes are kept as-is".format(
                    os.path.basename(dest), magic[:4]))
            try:
                os.unlink(tmp)
            except OSError:
                pass
            os.replace(source, dest)
            return os.path.getsize(dest)

    os.rename(tmp, dest)
    return written


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


def marker_object_present(marker, dest, options):
    """
    Whether the composed object a bucket-route marker claims actually exists, at size.

    Returns `(present, why_not)`. One metadata call, against a marker that would
    otherwise be believed on its own.

    The URL is taken from the marker, and derived from the mount table when the marker
    predates that field -- `gs_url_for` reads /proc/mounts and runs none of
    `select_route`'s probes, so this stays cheap and does not need the route decided
    first.

    **When the URL cannot be resolved at all, the marker is not trusted.** That costs a
    redundant transfer for an older marker on a destination whose mount has since gone,
    which is the direction to fail in: the alternative is the behaviour this function
    exists to remove.
    """
    gs_url = marker.get("gs_url")
    if not gs_url:
        mounts = read_mounts()
        mount = resolve_mount(dest, mounts) if mounts else None
        gs_url = gs_url_for(dest, mount) if mount else None
    if not gs_url:
        return False, ("it is a bucket-route marker with no gs:// URL recorded and none "
                       "could be resolved from the mount table, so it cannot be checked")

    try:
        bucket, name = split_gs_url(gs_url)
    except ValueError:
        return False, "its recorded gs:// URL is malformed: {}".format(gs_url)

    try:
        metadata = GcsClient(timeout=options.timeout).get_object(bucket, name)
    except PermanentError:
        return False, "{} does not exist".format(gs_url)
    except TransientError as e:
        # Cannot prove absence, so do not act on it. Re-transferring is expensive and a
        # transient GCS error is not evidence the object is gone.
        return False, "{} could not be checked ({}); re-transferring to be safe".format(
            gs_url, e)

    expected = marker.get("stored_size", options.size)
    actual = int(metadata.get("size", -1))
    if actual != expected:
        return False, "{} is {} bytes, expected {}".format(gs_url, actual, expected)
    return True, ""


def run(options):
    dest = options.dest
    manifest_path, marker_path = sidecar_paths(dest)

    if not options.no_resume:
        marker = read_done_marker(marker_path)
        if marker and marker.get("size") == options.size:
            # The marker alone is not enough: it is a hidden sidecar, so anything that
            # removes the payload without sweeping dotfiles -- a partial cleanup, a
            # `rm` to free space, a disk restored from a snapshot taken mid-write --
            # leaves it behind claiming a file that is gone. Observed: a stale marker
            # made this return EXIT_OK in 0.25s having transferred only the range
            # probe, and localization reported success with no destination at all.
            #
            # Checking the apparent size is sound HERE, unlike anywhere progress is
            # inferred: at completion the file is its full length, and the marker's
            # whole purpose is to certify that a full-length file is real rather than
            # sparse. So this only ever catches absence or replacement. The
            # stage-publish route has always done exactly this (see
            # `os.path.exists(staged) and os.path.getsize(staged) == size`); the
            # primary route was the outlier.
            # bucket-compose leaves no local file, so its evidence is the object in
            # the bucket instead -- see marker_object_present().
            if marker.get("route", ROUTE_POSIX) == ROUTE_BUCKET:
                present, why = marker_object_present(marker, dest, options)
            else:
                try:
                    present = (os.path.getsize(dest)
                               == marker.get("stored_size", options.size))
                except OSError:
                    present = False
                why = "{} is missing or the wrong size".format(dest)
            if present:
                log("already complete per {}".format(os.path.basename(marker_path)))
                return EXIT_OK
            log("{} claims completion but {}; ignoring the marker".format(
                os.path.basename(marker_path), why))

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
            # object_size, not size: when only a prefix of the object is wanted, the
            # server's Content-Range total is the WHOLE object and comparing it against
            # the prefix length declares the server broken. Defaults to size, so a
            # production run -- where size is always the whole object -- is unchanged.
            source.probe_range(options.object_size or size)
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
                         options.part_length,
                         max_chunk=getattr(options, "max_chunk", None) or MAX_CHUNK)
    if getattr(source, "whole_object_only", False):
        # The source cannot range, so N chunks would be N whole-object reads -- each
        # billed as the whole object. One chunk, one stream.
        chunks = [(0, size)] if size else []
    chunk_size = (chunks[0][1] - chunks[0][0]) if chunks else size
    plan_id = compute_plan_id(
        options.url or getattr(options, "gs_source", None) or "", size,
        options.check_etag or options.check_md5, chunk_size
    )

    # Decompression can never happen in flight on ANY route: **gzip is a sequential
    # stream**, so chunk N cannot be decoded without N-1. The compressed bytes have to be
    # materialized somewhere first and rewritten afterwards. in-place does that with a
    # .k9pdl.gz sidecar on disk; bucket-compose does it with a .k9pdl.gz object and a
    # streaming read-back through zlib into a second resumable upload (#24). stage-publish
    # is the one route that still cannot: it stages the bytes and copies them through
    # unchanged, with no transform step to hang this on.
    #
    # The ordering is the same on both routes that can, and is forced: **the advertised
    # digest covers the COMPRESSED bytes**, so verification happens before decompression,
    # and the decompressed output therefore has no digest to check against at all. The
    # most either route offers is "the bytes received were verified, then transformed".
    #
    # Ignoring the flag on the remaining route is not a missed optimization, it is
    # silently wrong output: the object lands still-gzipped, `compose` sets no
    # contentEncoding so GCS will not transcode it on read, and gcsfuse serves the gzip
    # stream verbatim under a name that promises plain content (measured -- gcsfuse
    # ignores contentEncoding entirely). The failure surfaces later, in whatever tool
    # reads the mount, with nothing pointing back here.
    #
    # It also re-creates precisely the inconsistency file_handlers.py:509 records
    # removing -- "previously the same object arrived decompressed via gs:// but
    # compressed via a signed URL" -- since gs:// inputs take gcloud storage cp, which
    # decompresses, while a signed URL for the same object comes through here.
    #
    # So: refuse. A loud stop costs a localization; silent corruption costs whatever
    # downstream conclusions were drawn from the wrong bytes before anyone noticed.
    if options.gunzip and decision.route not in (ROUTE_POSIX, ROUTE_BUCKET):
        log("--gunzip was requested but the {} route cannot decompress: it copies the "
            "staged bytes through unchanged, so the object would be stored "
            "still-compressed under a name promising decoded content. Refusing rather "
            "than writing bytes a reader cannot use. ({})".format(
                decision.route, decision.reason))
        return EXIT_FAIL

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

    # With --gunzip the transfer target is a compressed sidecar, not dest: the advertised
    # checksum covers the compressed bytes, so they must be verified before anything is
    # decompressed. Same ordering as the stage-publish route -- verify what was received,
    # then transform.
    target = dest + ".k9pdl.gz" if options.gunzip else dest
    target_manifest = sidecar_paths(target)[0] if options.gunzip else manifest_path

    with phase("download", size):
        status, manifest = download_to_local_file(
            options, source, size, chunks, chunk_size, plan_id, target, target_manifest,
            seek_hole,
        )
    if status != EXIT_OK:
        return status

    try:
        # On this route verification is a full re-read of the object, so its share of the
        # wall clock is worth knowing on its own.
        with phase("verify", size if (options.check_md5 or options.check_etag) else None):
            digest = verify(target, options, manifest)
    except PermanentError as e:
        log("verification failed: {}".format(e))
        discard(target, manifest)
        return EXIT_FAIL

    stored_size = None
    if options.gunzip:
        try:
            with phase("gunzip", size):
                expanded = gunzip_to(target, dest)
        except PermanentError as e:
            log("{}".format(e))
            discard(target, manifest)
            return EXIT_FAIL
        log("decompressed {} bytes into {} bytes".format(size, expanded))
        stored_size = expanded
        # peak disk is compressed + decompressed; give the space back immediately
        try:
            os.unlink(target)
        except OSError:
            pass

    write_done_marker(marker_path, size, plan_id, digest, stored_size=stored_size)
    if manifest is not None:
        manifest.unlink()
    log("complete: {} bytes{}".format(size, " (verified)" if digest else ""))
    return EXIT_OK


def download_to_local_file(options, source, size, chunks, chunk_size, plan_id, target,
                           manifest_path, seek_hole):
    """
    Download into a local POSIX file, in place and resumably.

    Shared by the in-place route (where the target is the destination) and the stage-publish
    route (where it is a staging file on a real block device). Returns `(exit_code,
    manifest)`; verification, publishing and the done marker are the caller's business,
    because they differ between the two.
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

        sink = PosixChunkSink(fd, manifest, chunks, target,
                              part_length=options.part_length if options.check_etag
                              else None,
                              size=size)
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
    if getattr(options, "gs_source", None):
        bucket, name = split_gs_url(options.gs_source)
        return GcsObjectSource(bucket, name, getattr(options, "user_project", None),
                               timeout=options.timeout)
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
    parser.add_argument("--max-chunk", type=int, default=MAX_CHUNK, dest="max_chunk",
                        help="largest chunk the size-scaled layout may choose; equal to "
                             "--min-chunk pins a fixed size")
    parser.add_argument("--commit-linger", dest="commit_linger", type=float,
                        default=COMMIT_LINGER_SECONDS,
                        help="seconds the manifest writer waits for more chunk "
                             "completions before committing. 0 commits each one "
                             "immediately, which costs a full manifest rewrite per chunk")
    parser.add_argument("--upload-block", dest="upload_block", type=int,
                        default=DEFAULT_UPLOAD_BLOCK,
                        help="bytes per PUT on the bucket-compose route; must be a "
                             "multiple of {}. Larger is faster (a PUT costs a "
                             "round-trip regardless of size) and widens the bound on "
                             "work discarded by a preemption".format(
                                 GCS_UPLOAD_GRANULARITY))
    parser.add_argument("--decode-readahead", dest="decode_readahead", type=int,
                        default=DEFAULT_DECODE_READAHEAD,
                        help="ranged GETs in flight for the bucket route's full-object "
                             "read-backs -- the md5 verification and, with --gunzip, "
                             "the decode (default: %(default)s). Only "
                             "the fetch is parallel; inflation stays sequential. 1 "
                             "restores the old single-stream read. Peak extra memory "
                             "is this times {} bytes".format(READ_BUFFER))
    parser.add_argument("--decode-upload-width", dest="decode_upload_width", type=int,
                        default=DEFAULT_DECODE_UPLOAD_WIDTH,
                        help="concurrent resumable sessions used to write the decoded "
                             "object back, composed at the end (default: %(default)s). "
                             "1 uses a single session, which is ~4x slower but leaves a "
                             "plain object carrying an md5Hash instead of a composite "
                             "that does not")
    parser.add_argument("--decode-queue-slices", dest="decode_queue_slices", type=int,
                        default=DEFAULT_DECODE_QUEUE_SLICES,
                        help="assembled slices allowed to wait in the upload queue "
                             "beyond those actively uploading (default: %(default)s). "
                             "This is what lets the inflater run ahead of the upload; "
                             "memory is (width + this) times the slice size")
    parser.add_argument("--header", action="append", default=[],
                        help="extra request header, 'Name: value' (repeatable)")
    parser.add_argument("--gs-source", dest="gs_source",
                        help="read the STORED bytes of gs://bucket/object through the GCS "
                             "JSON API with a refreshing token. For a gzip-encoded object "
                             "that must be decoded rather than copied server-side; pair "
                             "with --gunzip")
    parser.add_argument("--user-project", dest="user_project",
                        help="billing project for a requester-pays --gs-source")
    parser.add_argument("--s3-bucket", dest="s3_bucket")
    parser.add_argument("--s3-key", dest="s3_key")
    parser.add_argument("--s3-extra-args", dest="s3_extra_args", default="")
    parser.add_argument("--check-md5", dest="check_md5",
                        help="expected whole-file md5 (hex or base64)")
    parser.add_argument("--check-etag", dest="check_etag",
                        help="expected S3 multipart ETag")
    parser.add_argument("--part-length", dest="part_length", type=int,
                        help="S3 multipart part length, for ETag verification")
    # Only a benchmark passes this. A prefix run asks for `--size` bytes of an object
    # that is `--object-size` long, and probe_range's total-size equality check is
    # otherwise correct and otherwise fatal: it sees the full object's length, does not
    # match it against the prefix, raises RangeNotSupported and drops to a single stream.
    # That is what happened -- every row of two GDC sweeps ran the same single curl while
    # reporting per-connection results, and the transfer looked healthy at every other
    # observable.
    parser.add_argument("--object-size", dest="object_size", type=int, default=None,
                        help="total size of the object when --size is only a prefix of "
                             "it; defaults to --size")
    parser.add_argument("--url-refresh-cmd", dest="url_refresh_cmd",
                        help="shell command printing a fresh signed URL")
    parser.add_argument("--work-dir", action="append", default=[], dest="work_dir",
                        help="preferred staging directory for a non-POSIX destination "
                             "(repeatable, tried in order)")
    parser.add_argument("--legacy-cmd", dest="legacy_cmd",
                        help="command to run for the single-stream fallback")
    parser.add_argument("--gunzip", action="store_true", dest="gunzip",
                        help="the body arrives gzip-compressed: download and verify the "
                             "compressed bytes, then decompress into --dest")
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

    if (not (options.url or "").strip() and not (options.s3_bucket and options.s3_key)
            and not getattr(options, "gs_source", None)):
        parser.error("one of --url, --s3-bucket/--s3-key or --gs-source is required")

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
