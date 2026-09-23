#!/usr/bin/env python3
"""
Integration benchmark for the parallel downloader, to be run on a real GCP worker.

Everything in the unit suite runs against fakes. This script exists for the things a fake
cannot establish, and the list is not short:

  * the >=4x speedup claim, and the connection count that actually achieves it;
  * whether the NIC or the persistent disk is the limit (§2 says the PD is the more
    likely one for LocalizeToDisk, but that was reasoning, not measurement);
  * whether SEEK_HOLE passes on the real localization disk. Locally it does NOT -- APFS
    fails the probe -- so every local run exercises the checkpoint fallback and the
    frontier path has never been integration-tested at all;
  * page-cache loss via FALLOC_FL_PUNCH_HOLE, which is Linux-only and skipped locally;
  * the bucket-compose route against real GCS, including the auth path (metadata server, falling back to
    gcloud), resumable sessions and compose -- none of which has touched real
    infrastructure;
  * the stage-publish route against a real gcsfuse mount, if one is being evaluated;
  * what /bin/sh actually is on the controller image, which this repo does not reveal.

Not covered here, because it cannot be driven from the VM being preempted: forcing a real
preemption mid-localization. See `preempt` for what to do by hand.

Deliberately stdlib-only and runnable without canine installed, so it can be dropped onto
a node as-is:

    ./benchmark_localization.py probe                      # seconds, no egress, no cost
    ./benchmark_localization.py sweep  --url URL --size N
    ./benchmark_localization.py resume --url URL --size N
    ./benchmark_localization.py routeb --url URL --size N --gs-url gs://bucket/obj

An S3 source needs neither --size nor --md5: head-object reports the size, and the ETag is
the md5 for a single-part object and the md5-of-md5s for a multipart one. The store does
not have to be Amazon's -- --s3-endpoint-url threads through every aws call, as
HandleAWSURL's aws_endpoint_url does:

    ./benchmark_localization.py probe --s3-bucket B --s3-key K --s3-endpoint-url URL
    ./benchmark_localization.py sweep --s3-bucket B --s3-key K --s3-endpoint-url URL

`probe` is free and answers several open questions immediately; run it first.

Step-by-step procedure, including standing up the node and the pd-standard localization
disk: BENCHMARK_RUNBOOK.md, beside this file.
"""

import argparse
import configparser
import ctypes
import errno
import glob
import concurrent.futures
import hashlib
import json
import math
import os
import platform
import random
import re
import urllib.request
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
# Where parallel_download.py lives, resolved rather than assumed. In the repo it sits at
# ../localization/; when the two files are copied onto a node there is no reason to
# reconstruct that tree, so a sibling copy is checked first. An earlier version of this
# script hardcoded the repo-relative path, which forced anyone deploying it to build a
# ../localization/ directory for a single file -- a layout imposed by this constant rather
# than by anything real.
DOWNLOADER_CANDIDATES = (
    os.path.join(HERE, "parallel_download.py"),                            # side by side
    os.path.join(HERE, os.pardir, "localization", "parallel_download.py"),  # repo layout
)


DOWNLOADER = None      # resolved in main(); see downloader_path


def downloader_path(args=None):
    """The downloader to drive: --downloader, then $K9PDL_DOWNLOADER, then the candidates."""
    explicit = getattr(args, "downloader", None) or os.environ.get("K9PDL_DOWNLOADER")
    if explicit:
        return os.path.abspath(explicit)
    for candidate in DOWNLOADER_CANDIDATES:
        if os.path.exists(candidate):
            return os.path.abspath(candidate)
    return None

MIB = 1024 * 1024
GIB = 1024 * MIB

# Mirrors parallel_download.py's DEFAULT_MIN_CHUNK. Defined once here so the parser
# default and the chunk-plan arithmetic in probe_s3_endpoint cannot drift apart.
DEFAULT_MIN_CHUNK = 64 * MIB
# Mirrors MAX_CONNECTIONS in parallel_download.py. Stated here rather than imported: this
# script is deliberately standalone so it can be copied to a node on its own.
MAX_CONNECTIONS_HINT = 16
# Mirrors parallel_download.py's DEFAULT_CONNECTIONS. Mirrored rather than imported
# because this script is standalone and stdlib-only, so the two are pinned equal by
# TestTheBenchmarkMirrorsTheDownloadersConstants -- `routeb` and `resume` each carried
# their own hardcoded 8, which silently kept the pre-16 value when the default moved.
DEFAULT_CONNECTIONS_HINT = 16
# Mirrors parallel_download.py's DEFAULT_UPLOAD_BLOCK, pinned equal by the same test.
# The bucket route buffers one of these per connection, so it -- not READ_BLOCK -- is
# what sets peak RSS there.
UPLOAD_BLOCK_HINT = 8 * 1024 * 1024

# The emitted commands and the legacy fallbacks use [[ ]] and process substitution, which
# dash rejects -- and /bin/sh in the worker image is dash. Same reason parallel_download.py
# pins it: shell=True would otherwise pick /bin/sh.
SHELL = "/bin/bash"


# --------------------------------------------------------------------------------
# reporting
# --------------------------------------------------------------------------------

def say(message=""):
    sys.stdout.write(message + "\n")
    sys.stdout.flush()


def heading(text):
    say()
    say(text)
    say("-" * len(text))


def human(count):
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(count) < 1024 or unit == "TiB":
            return "{:.2f} {}".format(count, unit)
        count /= 1024.0


def rate(count, seconds):
    if seconds <= 0:
        return "n/a"
    return "{}/s".format(human(count / seconds))


# --------------------------------------------------------------------------------
# host counters
# --------------------------------------------------------------------------------

def nic_bytes():
    """Received bytes across real interfaces, from /proc/net/dev."""
    total = 0
    try:
        with open("/proc/net/dev") as fh:
            for line in fh.read().splitlines()[2:]:
                name, _, rest = line.partition(":")
                if name.strip() in ("lo", ""):
                    continue
                total += int(rest.split()[0])
    except (IOError, OSError, IndexError, ValueError):
        return None
    return total


def disk_written_bytes():
    """
    Bytes written across physical block devices, from /proc/diskstats.

    Partitions and device-mapper entries are skipped so a write is not counted twice.
    """
    total = 0
    try:
        with open("/proc/diskstats") as fh:
            for line in fh:
                fields = line.split()
                if len(fields) < 10:
                    continue
                name = fields[2]
                if re.search(r"\d$", name) and not name.startswith("nvme"):
                    continue  # a partition
                if name.startswith(("dm-", "loop", "ram")):
                    continue
                total += int(fields[9]) * 512
    except (IOError, OSError, ValueError):
        return None
    return total


class Sampler:
    """
    Deltas and peaks for the NIC and disk counters over a run.

    Sampled rather than taken end-to-end so a plateau is visible: if throughput stops
    scaling with connections while NIC utilisation stays well under the cap, the disk is
    the limit, which is the question §2 leaves open.
    """

    def __init__(self, interval=2.0):
        self.interval = interval
        self.samples = []

    def __enter__(self):
        self.start = time.monotonic()
        self.nic0 = nic_bytes()
        self.disk0 = disk_written_bytes()
        self._last = (self.start, self.nic0, self.disk0)
        return self

    def tick(self):
        now = time.monotonic()
        if now - self._last[0] < self.interval:
            return
        nic, disk = nic_bytes(), disk_written_bytes()
        elapsed = now - self._last[0]
        if None not in (nic, self._last[1]):
            self.samples.append({
                "nic_bytes_per_s": (nic - self._last[1]) / elapsed,
                "disk_bytes_per_s": ((disk - self._last[2]) / elapsed
                                     if None not in (disk, self._last[2]) else None),
            })
        self._last = (now, nic, disk)

    def __exit__(self, *exc):
        self.seconds = time.monotonic() - self.start
        nic, disk = nic_bytes(), disk_written_bytes()
        self.nic_total = (nic - self.nic0) if None not in (nic, self.nic0) else None
        self.disk_total = (disk - self.disk0) if None not in (disk, self.disk0) else None

    def peak(self, key):
        values = [s[key] for s in self.samples if s.get(key)]
        return max(values) if values else None


# --------------------------------------------------------------------------------
# probe: free, and answers several open questions
# --------------------------------------------------------------------------------

def probe_seek_hole(directory):
    path = os.path.join(directory, ".benchprobe.{}".format(os.getpid()))
    fd = None
    try:
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o600)
        os.ftruncate(fd, 8 * MIB)
        os.pwrite(fd, b"x" * 65536, 0)
        os.fsync(fd)
        hole = os.lseek(fd, 0, os.SEEK_HOLE)
        return {"supported": 65536 <= hole <= 65536 + 65536, "hole_at": hole}
    except (OSError, ValueError) as e:
        return {"supported": False, "error": str(e)}
    finally:
        if fd is not None:
            os.close(fd)
        try:
            os.unlink(path)
        except OSError:
            pass


def punch_hole(fd, offset, length):
    """
    fallocate(2) with FALLOC_FL_PUNCH_HOLE, through ctypes.

    Python exposes no fallocate() taking a mode and no FALLOC_FL_* constants -- not
    os.fallocate, not os.posix_fallocate. An earlier capability probe tested
    hasattr(os, "fallocate") and therefore reported "unsupported" on every platform,
    including ext4 where the syscall works, which is why the page-cache-loss test never
    ran anywhere.

    Raises OSError when the platform or filesystem cannot do it, which is the honest
    signal -- macOS has no fallocate(2) at all (it uses fcntl F_PUNCHHOLE).
    """
    FALLOC_FL_KEEP_SIZE = 0x01
    FALLOC_FL_PUNCH_HOLE = 0x02
    libc = ctypes.CDLL(None, use_errno=True)
    if not hasattr(libc, "fallocate"):
        raise OSError(errno.ENOSYS, "libc has no fallocate(2)")
    libc.fallocate.argtypes = [ctypes.c_int, ctypes.c_int,
                               ctypes.c_int64, ctypes.c_int64]
    libc.fallocate.restype = ctypes.c_int
    if libc.fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                      offset, length) != 0:
        code = ctypes.get_errno()
        raise OSError(code, os.strerror(code))


def probe_punch_hole(directory):
    path = os.path.join(directory, ".benchpunch.{}".format(os.getpid()))
    try:
        with open(path, "wb") as fh:
            fh.write(b"A" * (4 * MIB))
        fd = os.open(path, os.O_RDWR)
        try:
            punch_hole(fd, MIB, MIB)
            os.fsync(fd)
        finally:
            os.close(fd)
        with open(path, "rb") as fh:
            fh.seek(MIB)
            punched = fh.read(16) == b"\0" * 16
        return {"supported": punched}
    except OSError as e:
        return {"supported": False, "error": str(e)}
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


def probe_mount(directory):
    try:
        with open("/proc/mounts") as fh:
            entries = [l.split() for l in fh if len(l.split()) >= 3]
    except (IOError, OSError):
        return {"available": False}
    target = os.path.realpath(os.path.abspath(directory))
    best = None
    for device, mountpoint, fstype in ((e[0], e[1], e[2]) for e in entries):
        point = mountpoint.rstrip("/") or "/"
        if target == point or point == "/" or target.startswith(point + "/"):
            if best is None or len(point) > len(best[1].rstrip("/") or "/"):
                best = (device, mountpoint, fstype)
    if best is None:
        return {"available": True, "matched": False}
    return {"available": True, "matched": True,
            "device": best[0], "mountpoint": best[1], "fstype": best[2]}


def in_container():
    """
    Same test canine itself uses. It matters here because the localization script runs as
    a SLURM job step and slurmd lives inside the slurm_gcp_docker container, so that is
    the context whose /bin/sh, tool inventory and mount table are the real ones.
    """
    return os.path.exists("/.dockerenv")


def disk_details(device):
    """
    PD type and provisioned size for a mounted localization disk.

    Worth reporting because pd-standard write throughput is provisioned PER GIGABYTE, and
    create_persistent_disk sizes the localization disk at the object size plus 5%. A disk
    that small can be an order of magnitude slower than the NIC, in which case no number
    of connections helps and the sweep will show a flat line.
    """
    # /proc/mounts records the resolved device (/dev/sdb), not the
    # /dev/disk/by-id/google-<name> symlink it was mounted through -- so the disk name
    # has to come from whichever by-id link points at the same device.
    disk = None
    match = re.search(r"google-(.+)$", device or "")
    if match:
        disk = match.group(1)
    else:
        target = os.path.realpath(device) if device else None
        for link in glob.glob("/dev/disk/by-id/google-*"):
            if target and os.path.realpath(link) == target:
                disk = os.path.basename(link)[len("google-"):]
                break
    if not (disk and shutil.which("gcloud")):
        return None
    try:
        out = subprocess.run(
            ["gcloud", "compute", "disks", "describe", disk,
             "--format=value(type,sizeGb)"],
            capture_output=True, text=True, timeout=60)
        if out.returncode != 0:
            return None
        fields = out.stdout.split()
        kind = fields[0].rsplit("/", 1)[-1] if fields else None
        size = int(fields[1]) if len(fields) > 1 else None
        return {"disk": disk, "type": kind, "size_gb": size}
    except (subprocess.SubprocessError, ValueError, OSError):
        return None


# GCP's documented sustained write throughput, provisioned per GB.
PD_WRITE_MB_S_PER_GB = {"pd-standard": 0.12, "pd-balanced": 0.28, "pd-ssd": 0.48}

# Which filesystems are a real localization destination and which are the container
# talking to itself. Probing `overlay` measures the image's own writable layer on the boot
# disk, not the persistent disk a job would localize to -- and drawing conclusions from it
# is worse than having no result, because it looks like a result.
REAL_BACKING = ("ext4", "xfs", "btrfs", "ext3")
CONTAINER_INTERNAL = ("overlay", "overlayfs", "aufs")
MEMORY_BACKED = ("tmpfs", "ramfs")


def backing_kind(fstype):
    if fstype in REAL_BACKING:
        return "block device"
    if fstype in CONTAINER_INTERNAL:
        return "container overlay -- NOT a real destination"
    if fstype in MEMORY_BACKED:
        return "memory"
    if (fstype or "").startswith(("nfs", "fuse")):
        return "network/FUSE"
    return "unknown"


def probe_s3_endpoint(args):
    """
    Capability check for an S3 store, which matters most when it is NOT Amazon's.

    canine supports a custom endpoint throughout -- HandleAWSURL threads
    `--endpoint-url` into head-object, presign and the per-chunk fallback, and builds
    path-style URLs for public objects. But S3-compatible implementations differ in ways
    that decide which code path works, and none of it is discoverable from the repo:

      * whether ranged GETs are honoured (a store or proxy that ignores Range and returns
        the whole body would break chunking);
      * what the ETag means. AWS gives md5 for a single-part object and md5-of-md5s with a
        `-N` suffix for multipart, and `check_hash` relies on exactly that. An opaque or
        non-md5 ETag makes verification fail on correct data;
      * whether presigning works. If it does, every chunk goes through the plain HTTP path;
        if not, the fallback spawns one `aws` process per chunk, which costs throughput.
    """
    extra = s3_extra_args(args)
    endpoint = getattr(args, "s3_endpoint_url", None) or aws_config_endpoint(args)
    creds = aws_credentials_source(args)
    out = {"endpoint": endpoint or "amazon (default)",
           "endpoint_from": ("--s3-endpoint-url" if getattr(args, "s3_endpoint_url", None)
                             else ("aws config" if endpoint else "default")),
           "credentials_found": creds["found"],
           "credentials_from": creds["how"],
           "extra_args": extra}

    heading("S3 endpoint: {}".format(out["endpoint"]))
    say("endpoint via : {}".format(out["endpoint_from"]))
    # the source, never the secret -- see aws_credentials_source
    say("credentials  : {}".format(
        creds["how"] if creds["found"] else "NOT FOUND -- {}".format(creds["how"])))
    if not creds["found"] and not getattr(args, "no_sign_request", False):
        say("               falling back to --no-sign-request, which only works for a")
        say("               public bucket. For a private one, put the credentials at {}"
            .format(aws_credentials_file()))
    say()
    if not shutil.which("aws"):
        say("the `aws` CLI is missing, so no S3 path can work here")
        out["aws"] = False
        return out
    out["aws"] = True

    def aws(*words, timeout=120):
        command = "aws {} {}".format(extra, " ".join(words))
        try:
            return subprocess.run(command, shell=True, executable=SHELL,
                                  capture_output=True, text=True, timeout=timeout)
        except subprocess.SubprocessError as e:
            return subprocess.CompletedProcess(command, 1, "", str(e))

    # The same helper resolve_source uses, rather than a second copy of the head logic.
    # It performs the `--part-number 1` head that HandleAWSURL does
    # (file_handlers.py:1189-1205), which is the only way to learn the part length --
    # head-object reports how MANY parts there are, never how big they are.
    try:
        meta = s3_object_metadata(args)
    except RuntimeError as e:
        say("head-object FAILED: {}".format(e))
        say("without this nothing else can be measured -- check credentials, the")
        say("endpoint URL, and whether the bucket needs --no-sign-request")
        out["head_object"] = False
        return out

    out["head_object"] = True
    out["size"] = meta["size"]
    out["etag"] = meta["etag"]
    out["parts_count"] = meta["parts_count"]
    out["part_length"] = meta["part_length"]
    say("size        : {}".format(human(out["size"]) if out["size"] else "?"))

    # what the ETag means decides whether check_hash can work at all
    etag = out["etag"]
    if re.fullmatch(r"[0-9a-f]{32}", etag or ""):
        out["etag_kind"] = "md5"
        say("etag        : {}  -- plain md5, usable directly".format(etag))
    elif re.fullmatch(r"[0-9a-f]{32}-\d+", etag or ""):
        out["etag_kind"] = "multipart"
        say("etag        : {}  -- multipart (md5-of-md5s)".format(etag))
        out["stride_verified"] = meta.get("stride_verified")
        out["stride_check"] = meta.get("stride_check")
        part = out["part_length"]
        if meta.get("stride_verified") is False:
            say("parts       : {} parts, but part 1's length is NOT the stride -- {}"
                .format(out["parts_count"], meta.get("stride_check")))
            say("              S3 only requires non-final parts to be >= 5 MiB, so a")
            say("              5 MiB part followed by a 100 MiB one is legal. Striding")
            say("              by the wrong value gives a wrong md5-of-md5s, and")
            say("              verify() would discard a byte-perfect download as an")
            say("              ETag mismatch. ETag verification is DISABLED here.")
        if part:
            # measured from head-object --part-number 1, not size/parts: the final part
            # is short, so dividing understates the real part length.
            say("parts       : {} x {} stride, last part {} ({})".format(
                out["parts_count"], human(part),
                human(meta.get("last_part_length") or 0),
                meta.get("stride_check") or "unchecked"))
            min_chunk = getattr(args, "min_chunk", None) or DEFAULT_MIN_CHUNK
            chunk = max(part, -(-min_chunk // part) * part)
            out["implied_chunk"] = chunk
            out["implied_chunks"] = -(-out["size"] // chunk) if out["size"] else None
            say("chunk plan  : {} ({} whole parts) -> {} chunks at --min-chunk {}".format(
                human(chunk), chunk // part, out["implied_chunks"], human(min_chunk)))
        else:
            say("              WARNING: multipart ETag but no part length, so chunks")
            say("              cannot be snapped to part boundaries and the ETag would")
            say("              have to be verified by a full read-back instead.")
    else:
        out["etag_kind"] = "opaque"
        say("etag        : {!r}  -- NOT an AWS-style md5".format(etag))
        say("              This store does not follow AWS ETag semantics, so hash")
        say("              verification against the ETag would fail on correct data.")
        say("              Localize these inputs with check_hash off, or supply an md5")
        say("              out of band.")

    # ranged GET, the assumption the whole design rests on.
    #
    # The body goes to a temp FILE, not /dev/stdout. Two reasons, both learned the hard
    # way against a real BAM: the bytes are binary (BGZF starts 1f 8b) and capturing them
    # as text raises UnicodeDecodeError; and if the store were to IGNORE Range -- exactly
    # what this check exists to detect -- capturing stdout would pull the entire object
    # into memory. With an outfile, stdout is just the JSON metadata, which is text.
    out["accept_ranges"] = meta.get("accept_ranges")
    probe_size = min(1024, out["size"] or 1024)
    handle, tmp_path = tempfile.mkstemp(prefix=".k9pdl-range-")
    os.close(handle)
    try:
        ranged = aws(
            "s3api get-object --bucket {} --key {} --range {} {}".format(
                shlex.quote(args.s3_bucket), shlex.quote(args.s3_key),
                shlex.quote("bytes=0-{}".format(probe_size - 1)),
                shlex.quote(tmp_path)),
            timeout=60)
        got = os.path.getsize(tmp_path) if os.path.exists(tmp_path) else 0
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    out["range_bytes_returned"] = got
    # honoured means the RIGHT number of bytes came back, not merely that it exited 0 --
    # a store that ignores Range succeeds and returns everything.
    out["range_supported"] = ranged.returncode == 0 and got == probe_size
    if out["range_supported"]:
        say("ranged GET  : yes ({} bytes for a {}-byte request)".format(got, probe_size))
    elif ranged.returncode != 0:
        say("ranged GET  : FAILED -- {}".format(
            (ranged.stderr or "").strip().splitlines()[-1:] or ranged.returncode))
    else:
        say("ranged GET  : NOT HONOURED -- asked for {} bytes, got {}. Chunked download"
            .format(probe_size, got))
        say("              cannot work against this endpoint.")
    if out["accept_ranges"]:
        say("accept-ranges: {} (advertised by head-object)".format(out["accept_ranges"]))

    # presign, which decides which source the downloader uses
    if getattr(args, "no_sign_request", False):
        out["presign"] = None
        say("presign     : n/a -- a public bucket needs none, and HandleAWSURL builds a")
        say("              path-style URL against the endpoint directly")
    else:
        presigned = aws("s3 presign s3://{}/{}".format(args.s3_bucket, args.s3_key))
        url = presigned.stdout.strip()
        out["presign"] = presigned.returncode == 0 and bool(url)
        if out["presign"]:
            out["presigned_url"] = url
            say("presign     : yes -> the plain HTTP path, no `aws` process per chunk")
        else:
            say("presign     : NO -> falls back to S3ApiSource, which spawns one `aws`")
            say("              process per chunk. Expect lower throughput, and benchmark")
            say("              that path specifically with --s3-bucket/--s3-key.")
    return out


def command_probe(args=None):
    result = {"platform": platform.platform(), "python": sys.version.split()[0],
              "in_container": in_container()}

    heading("context")
    if in_container():
        say("Running INSIDE a container, which is the right place: the localization")
        say("script runs as a SLURM job step and slurmd lives in the")
        say("slurm_gcp_docker container.")
    else:
        say("WARNING: not running inside a container.")
        say()
        say("worker_startup_script.sh bind-mounts only /mnt/nfs -- NOT /mnt/rwdisks -- so")
        say("the localization disk is mounted in the CONTAINER's mount namespace and is")
        say("invisible here. /bin/sh and the tool inventory below are the host's, not the")
        say("ones the emitted commands actually run against.")
        say()
        say("Re-run as:  docker exec slurm <path>/benchmark_localization.py probe")

    heading("host")
    say("platform      : {}".format(result["platform"]))
    say("python        : {}".format(result["python"]))
    try:
        cpus = os.cpu_count()
        with open("/proc/meminfo") as fh:
            total_kb = int(re.search(r"MemTotal:\s+(\d+)", fh.read()).group(1))
        result["cpus"], result["memory"] = cpus, total_kb * 1024
        say("cpus / memory : {} / {}".format(cpus, human(total_kb * 1024)))
    except Exception:
        pass

    heading("/bin/sh identity")
    say("This is an open question the repo cannot answer: emitted commands are bash and")
    say("use [[ ]] and process substitution, which dash rejects.")
    try:
        link = os.path.realpath("/bin/sh")
        version = subprocess.run(["/bin/sh", "-c", "echo ${BASH_VERSION:-<not bash>}"],
                                 capture_output=True, text=True, timeout=10).stdout.strip()
        result["bin_sh"] = {"realpath": link, "bash_version": version}
        say("/bin/sh -> {}   BASH_VERSION={}".format(link, version))
    except Exception as e:
        say("could not determine: {}".format(e))

    heading("tools the emitted commands rely on")
    result["tools"] = {}
    # gcsfuse is not used by the emitted commands, but §6.6 cannot mount a bucket
    # without it -- better to learn that here than after setting up the route.
    for tool in ("bash", "curl", "python3", "gzip", "gunzip", "od", "aws", "gcloud",
                 "gsutil", "stat", "md5sum", "gcsfuse"):
        path = shutil.which(tool)
        result["tools"][tool] = path
        say("  {:<9} {}{}".format(
            tool, path or "MISSING",
            "   (needed only for the bucket-compose/stage-publish routes, §6.6)"
            if tool == "gcsfuse" and not path else ""))

    heading("candidate destinations")
    result["destinations"] = {}
    candidates = [d for d in [
        os.environ.get("CANINE_LOCAL_DISK_DIR"),
        "/mnt/nfs",
        tempfile.gettempdir(),
    ] if d] + sorted(glob.glob("/mnt/rwdisks/*"))
    for directory in candidates:
        if not os.path.isdir(directory):
            continue
        mount = probe_mount(directory)
        seek = probe_seek_hole(directory)
        punch = probe_punch_hole(directory)
        free = None
        try:
            stats = os.statvfs(directory)
            free = stats.f_bavail * stats.f_frsize
        except OSError:
            pass
        disk = disk_details(mount.get("device", "")) if mount.get("matched") else None
        result["destinations"][directory] = {
            "mount": mount, "seek_hole": seek, "punch_hole": punch, "free": free,
            "disk": disk}
        say("{}".format(directory))
        say("    fstype     : {}".format(mount.get("fstype", "?")))
        say("    free       : {}".format(human(free) if free else "?"))
        if disk:
            per_gb = PD_WRITE_MB_S_PER_GB.get(disk["type"])
            ceiling = (per_gb * disk["size_gb"]) if (per_gb and disk["size_gb"]) else None
            say("    pd type    : {} at {} GB".format(disk["type"], disk["size_gb"]))
            if ceiling:
                # Capped at the per-instance ceiling; the per-GB figure binds well below
                # it at the sizes create_persistent_disk actually provisions.
                say("    write cap  : ~{:.0f} MB/s (provisioned per-GB){}".format(
                    min(ceiling, 240.0),
                    "  <-- far below the ~2 GB/s NIC; expect a flat sweep"
                    if ceiling < 500 else ""))
        say("    SEEK_HOLE  : {}{}".format(
            "yes" if seek["supported"] else "NO -- checkpoint fallback would be used",
            "" if seek["supported"] else " (hole_at={})".format(seek.get("hole_at"))))
        say("    punch-hole : {}".format("yes" if punch["supported"] else "no"))
        kind = backing_kind(mount.get("fstype"))
        result["destinations"][directory]["backing"] = kind
        say("    backing    : {}".format(kind))

    if getattr(args, "s3_bucket", None) and getattr(args, "s3_key", None):
        result["s3"] = probe_s3_endpoint(args)

    heading("what this means")
    dests = result["destinations"]
    real = {d: v for d, v in dests.items() if v.get("backing") == "block device"}
    frontier = [d for d, v in real.items() if v["seek_hole"]["supported"]]

    if not real:
        say("No candidate is backed by a block device. Every directory above is the")
        say("container's own overlay or memory, so their SEEK_HOLE and punch-hole")
        say("results describe the image's writable layer -- NOT the disk a job would")
        say("localize to.")
        say()
        say("This is the expected state before §4 creates and mounts the localization")
        say("disk. Re-run `probe` afterwards; that result is the one that matters, and")
        say("nothing here should be recorded as an answer about the frontier path.")
        return result

    if frontier:
        say("SEEK_HOLE passes on block-device-backed: {}".format(", ".join(frontier)))
        say("-> the frontier path is live for real destinations. It has NEVER been")
        say("   integration-tested, because it fails the probe on the development")
        say("   machine (APFS), so this is its first exposure.")
    else:
        say("SEEK_HOLE fails on every block-device-backed candidate -- downloads there")
        say("would use the checkpoint fallback. Worth understanding before trusting any")
        say("resume numbers.")

    ignored = [d for d in dests if d not in real]
    if ignored:
        say()
        say("Ignored as not-a-real-destination: {}".format(", ".join(ignored)))
    return result


# --------------------------------------------------------------------------------
# running the downloader
# --------------------------------------------------------------------------------

def peak_rss(pid):
    """
    VmHWM for a live process: the high-water mark, so polling it cannot miss a spike
    between samples the way sampling VmRSS would.
    """
    try:
        with open("/proc/{}/status".format(pid)) as fh:
            match = re.search(r"VmHWM:\s+(\d+) kB", fh.read())
        return int(match.group(1)) * 1024 if match else None
    except (IOError, OSError, ValueError):
        return None


def _aws_ini(path):
    """
    Parse an aws credentials/config file.

    RawConfigParser rather than ConfigParser: these files are not ours, and the default
    parser performs `%` interpolation on values, which turns a stray percent sign in
    somebody else's secret into a crash. Nothing here needs interpolation.
    """
    parser = configparser.RawConfigParser()
    with open(path) as handle:
        parser.read_file(handle)
    return parser


def aws_credentials_file():
    """Path the `aws` CLI would read, honouring the standard override."""
    return os.environ.get("AWS_SHARED_CREDENTIALS_FILE") or os.path.expanduser(
        os.path.join("~", ".aws", "credentials"))


def aws_config_file():
    return os.environ.get("AWS_CONFIG_FILE") or os.path.expanduser(
        os.path.join("~", ".aws", "config"))


def aws_profile(args=None):
    return (getattr(args, "s3_profile", None)
            or os.environ.get("AWS_PROFILE") or "default")


def aws_credentials_source(args=None):
    """
    Where the `aws` CLI will get credentials from, described but never quoted.

    Credentials belong in the canonical file, not on a command line: an argument is
    visible in `ps` to every user on the box and lands in shell history, and keys issued
    by someone else (GDC, in this case) should not be handled that carelessly. So this
    only ever reports the *source* -- a path and a profile name. It does not read the
    secret, and nothing in this script prints or forwards one.
    """
    if os.environ.get("AWS_ACCESS_KEY_ID"):
        return {"found": True, "how": "AWS_ACCESS_KEY_ID in the environment"}

    path, profile = aws_credentials_file(), aws_profile(args)
    if not os.path.exists(path):
        return {"found": False, "how": "no {}".format(path), "path": path}

    try:
        parser = _aws_ini(path)
    except (configparser.Error, OSError) as e:
        return {"found": False, "path": path,
                "how": "could not parse {} ({})".format(path, e)}

    if parser.has_option(profile, "aws_access_key_id"):
        return {"found": True, "path": path, "profile": profile,
                "how": "{} [{}]".format(path, profile)}
    return {"found": False, "path": path, "profile": profile,
            "how": "{} exists but has no [{}] with aws_access_key_id".format(
                path, profile)}


def aws_config_endpoint(args=None):
    """
    `endpoint_url` from the aws config file, so a non-Amazon endpoint need not be passed
    on the command line either.
    """
    path, profile = aws_config_file(), aws_profile(args)
    if not os.path.exists(path):
        return None
    try:
        parser = _aws_ini(path)
    except (configparser.Error, OSError):
        return None
    # the config file spells non-default profiles "[profile name]"; the credentials file
    # spells them "[name]". Try both so either layout works.
    for section in ("default" if profile == "default" else "profile " + profile, profile):
        if parser.has_option(section, "endpoint_url"):
            return parser.get(section, "endpoint_url").strip()
    return None


def s3_extra_args(args):
    """
    The `aws` flags HandleAWSURL would assemble: an explicit endpoint for a store that is
    not Amazon's, and unsigned requests where there is nothing to sign with.

    The endpoint comes from --s3-endpoint-url if given, otherwise from the aws config
    file, so neither it nor the credentials need to appear in a command line.
    `--no-sign-request` is added automatically when no credentials can be found, because
    that is the only thing that could then work -- and it is what makes the failure a
    clear 403 rather than a confusing signature error.
    """
    parts = []
    endpoint = getattr(args, "s3_endpoint_url", None) or aws_config_endpoint(args)
    if endpoint:
        parts.append("--endpoint-url {}".format(shlex.quote(endpoint)))

    profile = getattr(args, "s3_profile", None)
    if profile:
        parts.append("--profile {}".format(shlex.quote(profile)))

    if getattr(args, "no_sign_request", False):
        parts.append("--no-sign-request")
    elif not aws_credentials_source(args)["found"]:
        parts.append("--no-sign-request")

    if getattr(args, "s3_extra_args", None):
        parts.append(args.s3_extra_args)
    return " ".join(parts)


def s3_legacy_command(args, dest, size):
    """
    HandleAWSURL's own fallback command, reproduced so the connections=1 row is the real
    legacy baseline for an S3 source.

    It cannot be the synthesized `curl -C -` the URL path gets: on the S3 API path there
    is no URL to curl. Uses process substitution, hence bash -- which the downloader
    already runs legacy commands under.

    `bytes=$SZ-` is open-ended, which is right in production -- `self.size` there is
    always the object's real size -- and wrong here for the same reason the URL baseline
    was: under a truncated `--size` it fetches the whole object while the parallel rows
    fetch `size` bytes, so the two are not comparable and the destination fills. Under
    --prefix the range gets an explicit upper bound.
    """
    upper = size - 1 if getattr(args, "prefix", False) else ""
    return (
        "[ -f {path} ] && SZ=$(stat --printf '%s' {path}) || SZ=0; "
        "if [ $SZ != {size} ]; then "
        "aws s3api {extra} get-object --bucket {bucket} --key {key} "
        '--range "bytes=$SZ-{upper}" >(cat >> {path}) > /dev/null; fi'
    ).format(path=shlex.quote(dest), size=size, extra=s3_extra_args(args),
             bucket=shlex.quote(args.s3_bucket), key=shlex.quote(args.s3_key),
             upper=upper)


def url_legacy_command(args, dest, size):
    """
    A single-stream baseline that fetches exactly `size` bytes.

    The downloader's own fallback synthesizes `curl -C - -sSL -o dest url` with no range,
    which is right for production -- there `--size` is always the whole object -- but
    wrong for a prefix benchmark: the parallel rows plan chunks over [0, size) while the
    connections=1 row would pull the entire object. Against a 279 GiB object with
    `--size 12 GiB` that meant curl quietly downloading all 279 GiB into a 16 GiB tmpfs.

    `--fail` matters as much as the range. Without it curl writes an HTTP error body to
    the output file and exits 0, so a 403 would present as a very fast success -- and in
    prefix mode there is no verification to catch it.
    """
    headers = "".join(" --header {}".format(shlex.quote(h))
                      for h in (getattr(args, "header", None) or []))
    return "curl --fail -sSL{headers} -r 0-{last} -o {dest} {url}".format(
        headers=headers, last=size - 1,
        dest=shlex.quote(dest), url=shlex.quote(args.url))


def source_args(args, dest, size):
    """
    Turn the parsed source options into the downloader's own arguments.

    Three source shapes, matching what the handlers emit:

      * `--url`             plain HTTP, or an S3 presigned URL minted host-side;
      * `--s3-bucket/--s3-key` with an empty `--url`, which is how build_source selects
        S3ApiSource -- one `aws` process per chunk, the path that covers
        session-token-only credentials and endpoints that cannot presign;
      * both, when you want to measure the presigned path against a non-Amazon store.
    """
    # getattr throughout: this is called with whatever namespace the subcommand built,
    # and `probe` has no --url. Mixing direct access with getattr made it crash on one
    # shape while tolerating another.
    url = (getattr(args, "url", None) or "").strip()

    def object_size_args():
        """
        The `--object-size` a prefix run needs, for EITHER source shape.

        Shared rather than duplicated because duplicating it is exactly how it broke: the
        S3 branch returned early with its own --legacy-cmd and never reached the --url
        branch's --object-size, so --prefix was silently accepted and silently ignored.
        probe_range then compared the whole object's length against the prefix, declared
        the server broken, and dropped to a single stream -- so an S3 prefix sweep
        measured one `aws` process per row while reporting 1/4/8/16 connections. That is
        the same failure --object-size was added to fix on the URL path, and it looked
        healthy at every other observable there too.
        """
        if not getattr(args, "prefix", False):
            return []
        total = getattr(args, "object_size", None)
        return ["--object-size", str(total)] if total else []

    if getattr(args, "s3_bucket", None) and getattr(args, "s3_key", None) and not url:
        # --legacy-cmd unconditionally here: there is no URL for the downloader to
        # synthesize a curl from, so the connections=1 baseline has to be handed in.
        # s3_legacy_command bounds its own range under --prefix.
        return ["--url", "",                      # empty: "presign produced nothing"
                "--s3-bucket", args.s3_bucket,
                "--s3-key", args.s3_key,
                # `--opt=value`, not `--opt value`: the value itself starts with dashes
                # (e.g. "--no-sign-request"), and argparse treats such a token as an
                # option unless it happens to contain a space.
                "--s3-extra-args={}".format(s3_extra_args(args)),
                "--legacy-cmd", s3_legacy_command(args, dest, size),
                ] + header_args(args) + object_size_args()

    base = ["--url", url] + header_args(args)
    if getattr(args, "prefix", False):
        # Only under --prefix: otherwise the connections=1 row fetches the whole object
        # rather than `size` bytes. Without --prefix the downloader synthesizes its own.
        base += ["--legacy-cmd", url_legacy_command(args, dest, size)]
    return base + object_size_args()


def header_args(args):
    """
    Headers to forward. The downloader applies them to every ranged GET and, via
    single_stream_fallback, to the synthesized `curl` too -- so the connections=1
    baseline authenticates the same way the parallel rows do. Without that, a private
    source fails on every row and the sweep has nothing to measure.
    """
    out = []
    for header in getattr(args, "header", None) or []:
        out += ["--header", header]
    return out


HEARTBEAT_INTERVAL = 300
STALL_ECHO_INTERVAL = 30

# Lines that explain why progress has stopped. A stall shows up as the percentage not
# moving between heartbeats, and at that point the only question worth answering is which
# of these is happening -- so they are echoed on their own, much shorter, clock.
#
# Both of the downloader's stall messages are here. ENOSPC waits in a bounded loop
# (ENOSPC_MAX_WAIT, 600s) and retries back off exponentially, so neither is a hang, but
# from outside they are indistinguishable from one without the text.
STALL_MARKERS = ("retrying in", "ENOSPC", "falling back", "Traceback")


def _drain_stderr(stream, sink, echo_every=HEARTBEAT_INTERVAL, out=None,
                  stall_every=STALL_ECHO_INTERVAL):
    """
    Read the child's stderr to EOF, keeping every byte and echoing enough of it to tell a
    slow run from a stopped one.

    Three jobs, all learned the hard way. Reading continuously is what stops the 64 KiB
    pipe filling and deadlocking a long run (see run_download). Echoing a progress line is
    what makes a long run distinguishable from a hung one: `pdl sweep` printed nothing
    between the header and the result row, so a 279 GiB run and a wedged downloader looked
    identical for hours, and the only way to tell them apart was to stat the destination
    from another shell.

    The third exists because the first version of this function had the first two and was
    still not enough. A run stalled at 96%; the heartbeat proved the pipe was being read,
    and then said nothing about why the percentage had stopped moving -- because retries
    and ENOSPC are not progress lines, and progress lines were all it echoed. The two
    messages that explain a stall were precisely the two it filtered out. So stall markers
    get their own, shorter clock, with the suppressed count carried along so a retry storm
    reads as a storm rather than as one unlucky chunk.

    Everything is still captured in full for the parser -- the echo is a view, never a
    filter.
    """
    out = out or sys.stderr
    last_echo = 0.0
    last_stall = 0.0
    suppressed = 0

    def emit(line, extra=""):
        out.write("        ... {}{}\n".format(line, extra))
        out.flush()

    for raw in iter(stream.readline, b""):
        sink.append(raw)
        if not echo_every:
            continue
        now = time.time()
        line = raw.decode("utf-8", "replace").rstrip()

        if any(marker in line for marker in STALL_MARKERS):
            if now - last_stall < stall_every:
                suppressed += 1
                continue
            last_stall = now
            emit(line, " [+{} more suppressed]".format(suppressed) if suppressed else "")
            suppressed = 0
            continue

        if now - last_echo < echo_every:
            continue
        if "% (" not in line:      # the Progress line; anything else is not a heartbeat
            continue
        last_echo = now
        emit(line)
    if suppressed:
        # Flush at EOF, or a storm that ends -- which is what a failing run looks like --
        # reports only its first line and reads as one unlucky chunk.
        emit("[+{} more suppressed]".format(suppressed))
    try:
        stream.close()
    except (IOError, OSError):
        pass


def run_download(source, dest, size, connections, min_chunk, extra=(), verification=None,
                 kill_after_bytes=None):
    """
    One download, measured. Returns a dict of results.

    `source` is the list of source arguments from source_args().

    With connections=1 the downloader declines and falls back to the legacy single stream,
    so that row is genuinely today's behaviour and the right baseline for the speedup
    claim -- it is not the new code throttled to one connection.
    """
    command = [sys.executable, DOWNLOADER] + list(source) + [
        "--dest", dest, "--size", str(size),
        "--connections", str(connections), "--min-chunk", str(min_chunk)]
    if verification is not None:
        command += verification.downloader_args
    command += list(extra)

    with Sampler() as sampler:
        process = subprocess.Popen(command, stdout=subprocess.DEVNULL,
                                   stderr=subprocess.PIPE)
        # Drain stderr on its own thread, and do it for the whole life of the process.
        #
        # This loop used to poll() without reading a byte until the child exited. A pipe
        # holds 64 KiB; the downloader logs a ~70-byte progress line every
        # PROGRESS_INTERVAL (5s), so after roughly 900 lines -- about 78 MINUTES -- the
        # buffer is full, the child blocks in write() forever, and this loop polls a
        # process that can now never exit. The result is a hang with no output, at a
        # threshold that sits under the full-size run and comfortably over every other
        # measurement, which is exactly why it survived: 4 GiB sweeps, resume runs and
        # probes all finish long before it.
        #
        # The symptom is indistinguishable from a slow source or a wedged downloader, and
        # `--json` never gets written, so an overnight run yields nothing at all.
        captured = []
        drain = threading.Thread(target=_drain_stderr,
                                 args=(process.stderr, captured), daemon=True)
        drain.start()
        killed = False
        killed_at = None
        rss = 0
        while True:
            # RSS is read before poll(), so the last sample is taken while the process is
            # still alive; /proc/<pid> is gone by the time poll() reports an exit.
            current = peak_rss(process.pid)
            if current:
                rss = max(rss, current)
            if process.poll() is not None:
                break
            sampler.tick()
            if kill_after_bytes is not None and not killed:
                try:
                    # ALLOCATED blocks, not apparent size. The downloader creates the
                    # destination sparse with ftruncate at the full object size, so
                    # os.path.getsize() reads the final size on the very first tick and
                    # every kill fired immediately -- the three "kills at 25/50/75%" each
                    # landed after 132 bytes, which is the range probe, and the resume run
                    # therefore measured a fresh download rather than a resume. st_blocks
                    # counts what is really on disk and grows with the transfer.
                    allocated = os.stat(dest).st_blocks * 512
                    if allocated >= kill_after_bytes:
                        process.send_signal(signal.SIGKILL)
                        killed = True
                        killed_at = allocated
                except OSError:
                    pass
            time.sleep(0.25)
        # The child has exited, so the drain thread sees EOF and finishes. Joined with a
        # timeout rather than forever: a stuck reader must not turn a completed download
        # into a hang, which is the failure being fixed here.
        drain.join(30)
        stderr = b"".join(captured).decode("utf-8", "replace")

    # The downloader emits "k9pdl-phase <name> <secs>s[, rate]" per phase. Capturing the
    # split matters because localization is two costs, not one: moving the bytes, then
    # re-reading them to hash. Which dominates decides whether hashing during the
    # transfer is worth building and whether a smaller instance type would do.
    phases = {}
    for match in re.finditer(r"k9pdl-phase (\w+) ([\d.]+)s", stderr):
        phases[match.group(1)] = float(match.group(2))

    # "k9pdl-streams mean N.NN of M workers (...)". The mean number of requests actually
    # receiving bytes at once, which is what separates "the source caps aggregate
    # bandwidth" from "the requests were never concurrent" on a sweep that comes out
    # flat. None on the connections=1 row, which runs a legacy command and never enters
    # the worker pool.
    streams = re.search(r"k9pdl-streams mean ([\d.]+) of (\d+) workers", stderr)

    # The downloader announces this and then behaves correctly, so nothing downstream
    # notices: the bytes arrive, the hash matches, the throughput is real. What is not
    # real is the connection count in the table. Two whole GDC sweeps were reported as
    # per-connection results when every row was the same single curl.
    fell_back = re.search(r"falling back to a single stream: (.*)", stderr)

    # The --gunzip pass, when one ran. Absent means one of three different things and
    # the caller has to be able to tell them apart: the flag was off, the flag was on but
    # the server's metadata was wrong so the bytes were kept as received, or the route
    # refused. The decompressed length is the only number available -- the advertised
    # digest covers the compressed bytes, so the output has nothing to verify against.
    expanded = re.search(r"decompressed (\d+) bytes into (\d+) bytes", stderr)

    # Per-chunk bookkeeping, the only cost that grows with chunk count rather than bytes.
    book = re.search(r"k9pdl-bookkeeping ([\d.]+)s over (\d+) calls "
                     r"\(mean ([\d.]+)s, ([\d.]+)% of", stderr)

    # The read loop split in two. Both halves are on the worker thread in sequence, so
    # `read %` is the share of worker time the network holds the disk idle for, and the
    # ceiling is what decoupling them could recover -- bounded by 2x, since a queue can
    # only hide the smaller half behind the larger.
    split = re.search(r"k9pdl-io read ([\d.]+)s write ([\d.]+)s other ([\d.]+)s "
                      r"over (\d+) blocks \(read (\d+)% of loop, "
                      r"overlap ceiling ([\d.]+)x\)", stderr)

    # The deferred commit. `mean batch` is the number that says whether the deferral did
    # anything: at 1.0 the writer is drained as fast as it is filled, nothing was
    # amortised, and the commits are still effectively per-chunk -- which is
    # indistinguishable from the fix working if you look only at throughput.
    commit = re.search(r"k9pdl-commit ([\d.]+)s over (\d+) batches "
                       r"\((\d+) chunks, mean batch ([\d.]+), ([\d.]+)% of", stderr)

    return {
        "phases": phases,
        "decompressed_from": int(expanded.group(1)) if expanded else None,
        "decompressed_to": int(expanded.group(2)) if expanded else None,
        "fell_back": fell_back.group(1).strip() if fell_back else None,
        "bookkeeping_seconds": float(book.group(1)) if book else None,
        "bookkeeping_calls": int(book.group(2)) if book else None,
        "bookkeeping_mean": float(book.group(3)) if book else None,
        "bookkeeping_pct_workers": float(book.group(4)) if book else None,
        "read_seconds": float(split.group(1)) if split else None,
        "write_seconds": float(split.group(2)) if split else None,
        "loop_other_seconds": float(split.group(3)) if split else None,
        "io_blocks": int(split.group(4)) if split else None,
        "read_pct_of_loop": int(split.group(5)) if split else None,
        "overlap_ceiling": float(split.group(6)) if split else None,
        "commit_seconds": float(commit.group(1)) if commit else None,
        "commit_batches": int(commit.group(2)) if commit else None,
        "commit_chunks": int(commit.group(3)) if commit else None,
        "commit_mean_batch": float(commit.group(4)) if commit else None,
        "commit_pct_wall": float(commit.group(5)) if commit else None,
        "mean_streams": float(streams.group(1)) if streams else None,
        "workers": int(streams.group(2)) if streams else None,
        "connections": connections,
        "returncode": process.returncode,
        "seconds": round(sampler.seconds, 2),
        "killed": killed,
        # File progress when the kill fired, which is what the threshold is expressed
        # in. NOT the attempt's own transferred bytes: on a resume the file already
        # holds the earlier attempts' work, so a later attempt reaches a higher
        # absolute threshold having moved far fewer bytes itself. Comparing the two
        # flagged a perfectly good run as premature -- every attempt moved ~1.01 GiB,
        # which is precisely what correct resume looks like.
        "killed_at_bytes": killed_at,
        "peak_rss": rss or None,
        "nic_bytes": sampler.nic_total,
        "disk_bytes": sampler.disk_total,
        "peak_nic_bytes_per_s": sampler.peak("nic_bytes_per_s"),
        "peak_disk_bytes_per_s": sampler.peak("disk_bytes_per_s"),
        "stderr_tail": stderr.strip().splitlines()[-3:],
    }


def file_md5(path, block=8 * MIB):
    digest = hashlib.md5()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(block), b""):
            digest.update(chunk)
    return digest.hexdigest()


VERIFY_READ_WORKERS = 2


def _part_digest(path, index, part_length, block):
    """One part's md5, read through its own handle so workers do not share a file offset."""
    part = hashlib.md5()
    remaining = part_length
    with open(path, "rb") as fh:
        fh.seek(index * part_length)
        while remaining:
            chunk = fh.read(min(block, remaining))
            if not chunk:
                break
            part.update(chunk)
            remaining -= len(chunk)
    return part.digest()


def file_multipart_etag(path, part_length, block=8 * MIB, workers=VERIFY_READ_WORKERS):
    """
    AWS's multipart ETag: md5 of the concatenated per-part md5 digests, then `-<n parts>`.

    Computed here so the benchmark's verdict is independent of the downloader's own. If we
    simply reported what the downloader concluded, "md5 ok" would mean no more than "it did
    not notice a problem". That independence is why this cost cannot be optimised away by
    reusing the in-transfer part digests, and a multipart ETag cannot be sampled -- it is
    only checkable by reading every byte.

    WHY TWO WORKERS, AND NOT MORE. On the 316 GB pd-standard this runs against, aggregate
    read throughput FALLS with concurrency: 86 / 85 / 76 / 62 MiB/s at 1 / 2 / 4 / 8
    readers (see test_verify_does_not_scale_readers_with_connections, where passing
    `connections` made a 279 GiB read-back 21 minutes slower). Meanwhile hashing is only
    about a seventh of read time -- md5 runs ~600-760 MB/s on one core against ~90 MB/s of
    disk -- so overlapping it perfectly saves at most ~15%. At 4 readers the 12% read loss
    eats nearly that whole prize; at 8 it costs more than the prize is worth. Two is where
    the two curves cross, and it is what the downloader uses, so the benchmark matches it.

    Threads, not processes. hashlib releases the GIL, so threads already scale md5 near
    linearly (753 -> 4987 MB/s over 8), while processes measured strictly slower at every
    width (536 -> 4208) purely on spawn cost, before any of the IPC or per-process file
    handles a real implementation would need. Multiprocessing would only win if the GIL
    were held here, and it is not. Keeping up with 86 MiB/s needs ~0.11 cores of md5, so
    the CPU was never the constraint in the first place.

    `workers` is exposed so the balance can be re-measured on other hardware; it must not
    change the answer, only the time taken to reach it.
    """
    size = os.path.getsize(path)
    parts = (size + part_length - 1) // part_length

    if workers and workers > 1 and parts > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            # map, not as_completed: part order is the digest, so it cannot be
            # reassembled from whatever finishes first.
            part_digests = list(pool.map(
                lambda i: _part_digest(path, i, part_length, block), range(parts)))
    else:
        part_digests = [_part_digest(path, i, part_length, block) for i in range(parts)]

    combined = hashlib.md5(b"".join(part_digests)).hexdigest()
    return "{}-{}".format(combined, len(part_digests))


def s3_object_metadata(args):
    """
    Size and ETag from head-object, exactly as HandleAWSURL derives them.

    For a multipart object the part length comes from a second head with
    `--part-number 1`, which is the only way to learn it -- head-object reports how many
    parts there are but not how big they are.
    """
    extra = s3_extra_args(args)

    def head(*words):
        command = "aws {} s3api head-object --bucket {} --key {} {}".format(
            extra, shlex.quote(args.s3_bucket), shlex.quote(args.s3_key), " ".join(words))
        result = subprocess.run(command, shell=True, executable=SHELL,
                                capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            raise RuntimeError("head-object failed: {}".format(
                result.stderr.strip().splitlines()[-1:] or result.returncode))
        return json.loads(result.stdout)

    meta = head()
    out = {"size": meta.get("ContentLength"),
           "etag": (meta.get("ETag") or "").strip('"'),
           "parts_count": meta.get("PartsCount") or 1,
           # carried through because probe reports it and the refactor that moved this
           # helper's caller onto it silently dropped the field, which a test caught
           "accept_ranges": meta.get("AcceptRanges"),
           "part_length": None,
           # Not "are all the parts the same size" -- with two parts they nearly never
           # are, since the last is the remainder. The question is narrower and is the
           # one that matters: does striding the file by part 1's length reproduce the
           # real part boundaries, so that md5-of-md5s reproduces the ETag?
           "stride_verified": None,
           "stride_check": None}
    if out["parts_count"] <= 1:
        return out

    count, size = out["parts_count"], out["size"]
    first = head("--part-number 1").get("ContentLength")
    out["part_length"] = first

    def optional_head(*words):
        """
        A confirmation HEAD that must not break a path which already works.

        HandleAWSURL only ever asks for part 1, so an endpoint could support that and
        reject or mishandle other part numbers. Treating such a failure as fatal would
        regress a working configuration in order to run a check, which is the wrong
        trade -- so an unavailable confirmation downgrades to "could not verify" and the
        part-1 stride is used, exactly as before this check existed.
        """
        try:
            return head(*words).get("ContentLength")
        except (RuntimeError, ValueError):
            return None

    # S3 does NOT require parts to be equal -- only that non-final parts are >= 5 MiB.
    # So the length of part 1 is not by itself the stride, and striding a 279 GB file by
    # the wrong value produces a wrong md5-of-md5s: verify() would then reject a
    # byte-perfect download as an ETag mismatch and discard it. Two extra HEADs turn that
    # silent false failure into a detected condition.
    #
    # The identity below is the strong part: if every non-final part were `first`, the
    # total must be (count - 1) * first + last. A differing interior part breaks it unless
    # another compensates exactly, and checking part 2 as well closes the easy cases.
    last = optional_head("--part-number {}".format(count))
    out["last_part_length"] = last

    if first is None or size is None or last is None:
        out["stride_verified"] = None
        out["stride_check"] = "unconfirmed (endpoint did not answer for part {})".format(
            count)
        return out

    reasons = []
    # For exactly two parts this identity is trivially true -- first + last is the size
    # by definition -- so `last <= first` below is the only informative check there. It
    # is also sufficient: with one non-final part, striding by it gives [0, first) and
    # [first, size), which are the real boundaries.
    expected = (count - 1) * first + last
    if expected != size:
        reasons.append(
            "(count-1)*first + last = {} but the object is {}".format(expected, size))
    if last > first:
        # legal in S3 -- only non-final parts must be >= 5 MiB -- but then part 1 is not
        # the stride, e.g. a 5 MiB part followed by a 100 MiB one
        reasons.append("last part {} exceeds part 1 {}".format(last, first))
    if count >= 3:
        second = optional_head("--part-number 2")
        out["second_part_length"] = second
        if second is not None and second != first:
            reasons.append("part 2 is {}, part 1 is {}".format(second, first))

    out["stride_verified"] = not reasons
    out["stride_check"] = "; ".join(reasons) if reasons else "confirmed"
    if reasons:
        # The stride is unknown, so the ETag cannot be reproduced from it. Better no
        # verification than a verification that fails on correct data.
        out["part_length"] = None
    return out


class Verification:
    """
    How the bytes get checked, and by what.

    An S3 source needs no `--md5` from the operator: head-object already carries the
    ETag, which for a single-part object *is* the md5 and for a multipart object is the
    md5-of-md5s the downloader can compute during the transfer. Requiring an explicit
    digest would be asking for something the store already told us.

    The exception is a store that is not Amazon's and does not follow AWS ETag semantics.
    An opaque ETag is not a digest of anything we can reproduce, so verification is
    skipped rather than reported as a failure on correct data.
    """

    def __init__(self, kind, value=None, part_length=None, reason=None):
        self.kind = kind                 # "md5" | "etag" | None
        self.value = value
        self.part_length = part_length
        self.reason = reason

    @property
    def downloader_args(self):
        if self.kind == "md5":
            return ["--check-md5", self.value]
        if self.kind == "etag":
            return ["--check-etag", self.value, "--part-length", str(self.part_length)]
        return []

    def check(self, path, workers=VERIFY_READ_WORKERS):
        """True, False, or None when there is nothing to check against."""
        if self.kind == "md5":
            return file_md5(path) == self.value
        if self.kind == "etag":
            return file_multipart_etag(path, self.part_length, workers=workers) == self.value
        return None

    @property
    def label(self):
        if self.kind == "md5":
            return "md5 {}".format(self.value)
        if self.kind == "etag":
            return "multipart etag {} ({} byte parts)".format(self.value, self.part_length)
        return "NOT VERIFIED -- {}".format(self.reason or "no digest available")


def announce_verification(verification, dest, out=None, interval=HEARTBEAT_INTERVAL,
                          workers=VERIFY_READ_WORKERS):
    """
    Run the benchmark's own verification, saying so first and ticking while it works.

    Returns `(verdict, seconds)`. The seconds matter as much as the verdict, and used not to
    be returned at all: on the 279 GiB run this cost **1h45m**, appeared in no column, no
    phase and no total, and the report printed "the post-hoc read-back was skipped entirely"
    directly underneath twenty-one of its own heartbeats. `phases: verify 0.0s` was correct
    about the DOWNLOADER -- #19 really does assemble the ETag from in-flight part digests --
    and said nothing about this function, which is a different read-back that did happen.

    Timing it also turned out to be the most useful measurement in that run. 278.91 GiB in
    at least 6300 s is 43-45 MiB/s, against the 85 MiB/s file_multipart_etag's own docstring
    records for the same operation on this class of device, and within 10% of what the
    downloader's writes achieve. Every figure suggesting the disk can do 79-88 MiB/s was
    measured over <=16 GiB; both full-extent measurements say ~45 (see the runbook, 6.5g).
    A cost nobody was billing for was the only hour-plus sample of the destination we had.

    This is a full single-threaded re-read of the destination in Python, deliberately
    independent of the downloader's verdict (see file_multipart_etag). On a 279 GiB object
    against a ~90 MiB/s device that is the better part of an hour -- comparable to the
    download it is checking -- and until now it printed nothing at all.

    It also sits OUTSIDE run_download, so the drain thread's heartbeat has already stopped:
    the child has exited, no more progress lines exist, and the display freezes on the last
    percentage the download happened to emit. That is how a completed 279 GiB transfer came
    to look like a run "stuck at 96%" -- 96% was simply the last thing ever printed, and
    everything after it was this function, silent.

    The reported throughput is unaffected either way; run_download's Sampler has already
    closed. This is purely about the operator being able to tell work from a wedge.
    """
    out = out or sys.stderr
    if verification.kind is None:
        return None, 0.0

    size = os.path.getsize(dest)
    out.write("        verifying {} against the {} -- a full re-read, "
              "expect minutes\n".format(human(size), verification.label.split()[0]))
    out.flush()

    done = threading.Event()

    def tick():
        started = time.time()
        while not done.wait(interval):
            out.write("        ... still verifying ({} elapsed)\n".format(
                human_seconds(time.time() - started)))
            out.flush()

    beat = threading.Thread(target=tick, daemon=True)
    beat.start()
    started = time.time()
    try:
        verdict = verification.check(dest, workers=workers)
    finally:
        done.set()
    elapsed = time.time() - started
    out.write("        re-read {} in {} ({})\n".format(
        human(size), human_seconds(elapsed), rate(size, elapsed)))
    out.flush()
    return verdict, elapsed


def human_seconds(seconds):
    minutes, seconds = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return "{}h{:02d}m".format(hours, minutes)
    return "{}m{:02d}s".format(minutes, seconds)


def parsed_headers(args):
    out = {}
    for item in getattr(args, "header", None) or []:
        name, _, value = item.partition(":")
        if value:
            out[name.strip()] = value.strip()
    return out


def url_object_size(args):
    """
    The object's total length, from `Content-Range` on a one-byte ranged GET.

    Needed only in --prefix mode, and needed there because probe_range compares the
    server's declared total against the size it was given. Handed a prefix length it
    concludes the server is not honouring Range, raises RangeNotSupported, and the
    downloader drops to a single stream -- which is exactly what happened: every row of
    two GDC sweeps ran the same single curl while the table reported them as 1, 4, 8, 12
    and 16 connections. Nothing else looked wrong; the byte count, the wire ratio and the
    throughput were all consistent with a healthy transfer, because it was one.

    Returns None if the total cannot be determined, which leaves --object-size unset and
    the old behaviour in place rather than guessing.
    """
    request = urllib.request.Request(args.url, headers=parsed_headers(args))
    request.add_header("Range", "bytes=0-0")
    try:
        response = urllib.request.urlopen(request, timeout=60)
    except Exception as e:                      # noqa: BLE001 -- any failure is "unknown"
        say("could not determine the object size ({}); --object-size unset".format(e))
        return None
    try:
        content_range = (response.headers.get("Content-Range") or "").strip()
    finally:
        response.close()
    match = re.match(r"^bytes 0-0/(\d+)$", content_range)
    if not match:
        say("Content-Range was {!r}; --object-size unset".format(content_range))
        return None
    return int(match.group(1))


def redact_url(url):
    """
    Drop the query string, keeping enough of the URL to identify the object.

    A presigned S3 URL carries `X-Amz-Credential` -- which contains the access key ID --
    and `X-Amz-Signature`, which together are a bearer credential for that object until
    the window closes. Printing it put both on the terminal, into tmux scrollback, into
    whatever the operator pasted the output into, and into any log of the run. The
    object path is what a reader needs; the signature is not.
    """
    if "?" not in (url or ""):
        return url
    base, _, query = url.partition("?")
    return "{}?<{} bytes of query string redacted>".format(base, len(query))


def container_path_note():
    """
    Say when a written path is inside the container.

    Every invocation goes through `docker exec`, so `--json /tmp/x.json` lands in the
    container's /tmp and not the node's -- and "results written to /tmp/x.json" reads as
    though it were the node's. That cost one failed command, and worse, the runbook's own
    teardown scp'd $NODE:/tmp/*.json, which would have collected nothing and then deleted
    the instance holding the only copy.
    """
    return "  (inside the container -- `docker cp` it out before teardown)" \
        if os.path.exists("/.dockerenv") else ""


def describe_downloader():
    """
    Path and md5 of the downloader being measured.

    A stale copy in the container is invisible otherwise, and it does not fail -- it
    quietly omits whatever the new build was supposed to report. That happened: a run
    made specifically to read the concurrency figure came back without it, and the
    missing line was indistinguishable from concurrency of zero. Comparing this md5
    against the workstation's is one glance.
    """
    if not DOWNLOADER:
        return "not found"
    try:
        with open(DOWNLOADER, "rb") as handle:
            digest = hashlib.md5(handle.read()).hexdigest()
    except OSError as e:
        return "{} (unreadable: {})".format(DOWNLOADER, e)
    return "{} (md5 {})".format(DOWNLOADER, digest)


def describe_source(args):
    # getattr throughout: this is called with whatever namespace the subcommand built,
    # and `probe` has no --url. Mixing direct access with getattr made it crash on one
    # shape while tolerating another.
    url = (getattr(args, "url", None) or "").strip()
    if getattr(args, "s3_bucket", None) and getattr(args, "s3_key", None) and not url:
        return "s3://{}/{} via {} (S3ApiSource)".format(
            args.s3_bucket, args.s3_key,
            args.s3_endpoint_url or "amazon")
    return redact_url(args.url)


def resolve_source(args):
    """
    Fill in size and verification from the object itself where possible.

    Returns (size, Verification). An explicit --md5/--size always wins, so a deliberate
    override stays possible.
    """
    size, verification = args.size, None

    if args.md5:
        verification = Verification("md5", args.md5)

    if getattr(args, "s3_bucket", None) and getattr(args, "s3_key", None):
        meta = s3_object_metadata(args)
        if size is None:
            size = meta["size"]
        if getattr(args, "prefix", False) and not getattr(args, "object_size", None):
            # head-object already told us the whole object's length, so the prefix run
            # gets --object-size for free -- no extra round trip, and none of the
            # url_object_size probe's failure modes. Without it probe_range compares the
            # full length against the prefix and drops every row to a single stream.
            args.object_size = meta["size"]
        if verification is None and getattr(args, "prefix", False):
            # head-object describes the WHOLE object. Deriving from it under --prefix
            # gives the 279 GiB ETag at 9849 parts, which a 12 GiB prefix (424 parts)
            # cannot match -- so verify() raises, discard() removes the file, and every
            # row exits 1. The sweep then reports NO USABLE RESULT instead of a knee,
            # and the cause is nowhere in the output. A note in the runbook was not
            # enough; refuse here, and say what to do instead.
            verification = Verification(
                None, reason="--prefix: head-object describes the whole object, whose "
                             "ETag a prefix cannot match. Pass --md5 of the prefix to "
                             "verify, or verify at full size (runbook 6.3/6.4)")
        if verification is None:
            etag = meta["etag"]
            if re.fullmatch(r"[0-9a-f]{32}", etag or ""):
                verification = Verification("md5", etag)
            elif meta["parts_count"] > 1 and meta["part_length"]:
                if re.fullmatch(r"[0-9a-f]{32}-\d+", etag or ""):
                    verification = Verification("etag", etag,
                                                part_length=meta["part_length"])
                else:
                    verification = Verification(
                        None, reason="multipart, but the ETag {!r} is not AWS-style "
                                     "md5-of-md5s".format(etag))
            elif meta["parts_count"] > 1 and meta.get("stride_verified") is False:
                verification = Verification(
                    None, reason="part 1 is not the stride ({}), so md5-of-md5s cannot "
                                 "be reproduced".format(meta.get("stride_check")))
            else:
                verification = Verification(
                    None, reason="the ETag {!r} is not an md5; this store does not follow "
                                 "AWS semantics".format(etag))

    if verification is None:
        verification = Verification(
            None, reason="no --md5 given and the source carries no usable digest")
    if size is None:
        raise SystemExit("--size is required for this source (only S3 can report it)")

    # Once, here, rather than per row: source_args runs for every setting and this is a
    # network round trip.
    if getattr(args, "prefix", False) and (getattr(args, "url", None) or "").strip():
        args.object_size = url_object_size(args)
    return size, verification


# --------------------------------------------------------------------------------
# sweep
# --------------------------------------------------------------------------------

def command_sweep(args):
    """
    §8.5's connection sweep. 1 connection is the legacy single stream and the baseline
    the >=4x claim is measured against.
    """
    size, verification = resolve_source(args)
    args.size = size
    results = []
    heading("connection sweep")
    say("object    : {}".format(describe_source(args)))
    say("size      : {}".format(human(size)))
    say("dest dir  : {}".format(args.dest_dir))
    say("downloader: {}".format(describe_downloader()))
    if getattr(args, "prefix", False):
        total = getattr(args, "object_size", None)
        say("object size: {}".format(
            "{} -- fetching a {} prefix".format(human(total), human(size)) if total
            else "UNKNOWN -- probe_range will reject the prefix and every row will "
                 "fall back to a single stream"))
    say("verify    : {}".format(verification.label))
    if getattr(args, "prefix", False):
        say("baseline  : ranged curl (--prefix), so connections=1 fetches the same "
            "{} as the parallel rows".format(human(size)))
    say("egress    : ~{} per setting, {} settings".format(
        human(size), len(args.connections)))
    if size < GIB:
        say()
        say("WARNING: {} is too small for this to mean anything. Per-connection setup"
            .format(human(size)))
        say("dominates, and the numbers below should not be recorded as the benchmark.")
        say("Treat this run as a smoke test of the harness.")
    say()
    say("{:>5}  {:>9}  {:>13}  {:>13}  {:>13}  {:>10}  {:>4}".format(
        "conns", "seconds", "throughput", "peak NIC", "peak disk", "peak RSS",
        "hash"))

    baseline = None
    for connections in args.connections:
        dest = os.path.join(args.dest_dir, "bench.{}.bin".format(connections))
        for stale in (dest, dest + ".k9pdl.gz"):
            try:
                os.unlink(stale)
            except OSError:
                pass
        for sidecar in glob.glob(os.path.join(
                args.dest_dir, ".bench.{}.bin.k9pdl.*".format(connections))):
            os.unlink(sidecar)

        outcome = run_download(source_args(args, dest, args.size), dest, args.size,
                               connections, args.min_chunk,
                               verification=verification)
        if outcome["returncode"] == 0 and os.path.exists(dest):
            outcome["verified"], outcome["reread_seconds"] = announce_verification(
                verification, dest, workers=getattr(args, "verify_workers",
                                                    VERIFY_READ_WORKERS))
        else:
            outcome["verified"], outcome["reread_seconds"] = False, None

        # A failed or unverified run is NOT a measurement. Computing size/elapsed
        # regardless once produced "48.00 GiB/s" over a 2 GB/s NIC and a verdict of
        # "MEETS the target" for five runs that had downloaded nothing -- the elapsed
        # time being how long it took to fail. Throughput is only recorded for a run
        # that finished and verified.
        outcome["ok"] = outcome["returncode"] == 0 and outcome["verified"] is not False
        if outcome["ok"]:
            throughput = args.size / outcome["seconds"] if outcome["seconds"] else 0
            outcome["throughput_bytes_per_s"] = throughput
            if connections <= 1:
                baseline = throughput
            outcome["speedup_vs_single_stream"] = (
                round(throughput / baseline, 2) if baseline else None)
        else:
            outcome["throughput_bytes_per_s"] = None
            outcome["speedup_vs_single_stream"] = None
        results.append(outcome)

        if not outcome["ok"]:
            say("{:>5}  {:>9}  {:>13}  FAILED (rc={})".format(
                connections, outcome["seconds"], "--", outcome["returncode"]))
            for line in outcome["stderr_tail"]:
                say("         {}".format(line[:100]))
        else:
            say("{:>5}  {:>9}  {:>13}  {:>13}  {:>13}  {:>10}  {:>4}".format(
                connections, outcome["seconds"], rate(args.size, outcome["seconds"]),
                rate(outcome["peak_nic_bytes_per_s"] or 0, 1),
                rate(outcome["peak_disk_bytes_per_s"] or 0, 1),
                human(outcome["peak_rss"]) if outcome["peak_rss"] else "?",
                "ok" if outcome["verified"] else "-"))
        if outcome["phases"]:
            say("        phases: {}".format("  ".join(
                "{} {:.1f}s".format(k, v) for k, v in outcome["phases"].items())))
        if outcome.get("fell_back"):
            say("        NOT PARALLEL: fell back to a single stream -- {}".format(
                outcome["fell_back"]))
        if outcome.get("mean_streams") is not None:
            say("        streams: {:.2f} of {} concurrent on average".format(
                outcome["mean_streams"], outcome["workers"]))
        if outcome.get("read_seconds") is not None:
            say("        io     : read {:.1f}s / write {:.1f}s / other {:.1f}s "
                "({}% read, overlap ceiling {:.2f}x)".format(
                    outcome["read_seconds"], outcome["write_seconds"],
                    outcome["loop_other_seconds"], outcome["read_pct_of_loop"],
                    outcome["overlap_ceiling"]))
        if outcome.get("bookkeeping_seconds") is not None:
            say("        chunk_ready: {:.1f}s over {} calls (mean {:.3f}s, {:.0f}% of the"
                " worker pool)".format(
                    outcome["bookkeeping_seconds"], outcome["bookkeeping_calls"],
                    outcome["bookkeeping_mean"], outcome["bookkeeping_pct_workers"]))
        if outcome.get("commit_seconds") is not None:
            # A mean batch at 1.0 means the commits were never amortised. Named rather
            # than left to be inferred from the throughput, because a run where the
            # deferral achieved nothing looks exactly like one where it was not needed.
            note = "" if outcome["commit_mean_batch"] > 1.05 else \
                "   <-- NOT BATCHED: every commit was one chunk"
            say("        commit : {:.1f}s over {} batches ({} chunks, mean batch {:.1f},"
                " {:.0f}% of wall, off the worker pool){}".format(
                    outcome["commit_seconds"], outcome["commit_batches"],
                    outcome["commit_chunks"], outcome["commit_mean_batch"],
                    outcome["commit_pct_wall"], note))
        # NIC bytes over payload bytes. ~1 means every byte crossed the wire once, which
        # is the claim that the object was chunked into disjoint ranges rather than
        # fetched N times over. A server that IGNORES Range returns the whole object to
        # every request -- the gzip-transcoded GCS case -- and this ratio goes to the
        # connection count. probe_range is supposed to stop that before any parallel work
        # starts, but the ratio is the observable, and a cost alarm besides: GCS bills
        # for the whole object per request.
        if outcome.get("nic_bytes") and args.size:
            ratio = outcome["nic_bytes"] / float(args.size)
            note = "" if ratio < 1.5 else "   <-- DUPLICATE FETCHING"
            say("        wire   : {:.2f}x payload ({} on the NIC){}".format(
                ratio, human(outcome["nic_bytes"]), note))
        if not args.keep:
            # The sidecars go with it. Unlinking only the payload leaves
            # `.bench.N.bin.k9pdl.done` behind, and a completion marker with no file is
            # the state that made a later run exit 0 after 108 bytes. The pre-run sweep
            # above cleans the row it is about to use, so those orphans only ever
            # surfaced for connection counts nobody re-ran -- which is worse, not
            # better: the trap was invisible and dated from a different session.
            for leftover in [dest, dest + ".k9pdl.gz"] + glob.glob(os.path.join(
                    args.dest_dir, ".bench.{}.bin.k9pdl.*".format(connections))):
                try:
                    os.unlink(leftover)
                except OSError:
                    pass

    # Only a run that finished AND verified is a measurement -- see the `ok` assignment
    # in the loop above for the failure this guards against.
    usable = [r for r in results if r["ok"]]
    if not usable:
        heading("NO USABLE RESULT")
        say("Every setting failed or did not verify, so there is nothing to report and")
        say("no speedup to claim. The stderr above names the reason; the most common is")
        say("a source the downloader cannot read -- a private GCS object over plain")
        say("https needs an auth header (see §6.1), and an expired presigned URL looks")
        say("the same.")
        say()
        say("Nothing here should be recorded as a benchmark result.")
        return {"sweep": results, "usable": 0}
    if len(usable) < len(results):
        say()
        say("NOTE: {} of {} settings failed and are excluded from what follows."
            .format(len(results) - len(usable), len(results)))

    # download vs verify: the split that decides §10's machine-type question and whether
    # in-transfer hashing is worth building
    # `in`, not `.get()`: a verify of 0.0s is falsy, and a fast verify is precisely the
    # result that argues the node can be sized down. Dropping it would hide that.
    # `verified is not None`: a run that verified NOTHING reports verify 0.0s, and
    # concluding "verification is a small share, so hashing during the transfer would buy
    # little" from that is a statement about work that never happened. The --prefix runs
    # against the real source cannot verify at all (a slice cannot match the object's
    # ETag), so this fired on every one of them.
    split_rows = [r for r in usable
                  if "verify" in r["phases"] and r.get("verified") is not None]
    splits = [r["phases"] for r in split_rows]
    unverified = [r for r in usable if "verify" in r["phases"]
                  and r.get("verified") is None]
    if unverified and not splits:
        heading("download vs verify")
        say("Not available: {} of {} settings verified nothing, so their verify phase is"
            .format(len(unverified), len(usable)))
        say("0.0s because no hashing was done -- not because hashing is cheap. Nothing")
        say("here says whether hashing during the transfer is worth it; that needs a run")
        say("with a real digest (§6.3 / §6.4 at full size).")
        say()
    if splits:
        heading("download vs verify")
        dl = sum(s.get("download", 0) for s in splits) / len(splits)
        vf = sum(s["verify"] for s in splits) / len(splits)
        total = dl + vf
        say("mean download : {:.1f}s ({:.0f}%)".format(dl, 100*dl/total if total else 0))
        say("mean verify   : {:.1f}s ({:.0f}%)".format(vf, 100*vf/total if total else 0))
        # The benchmark's OWN re-read, which is not part of the downloader's phases and is
        # not optional: file_multipart_etag exists so the verdict does not come from the
        # code under test. Unreported, it made a 1h45m read-back invisible on the 279 GiB
        # run while the text below announced that the read-back had been skipped.
        # `is not None`, not truthiness: on a small test object the re-read is a few
        # microseconds and rounds to 0.0s, which is still a read-back that happened. The
        # whole defect being fixed here was a real cost reported as zero; re-introducing it
        # for fast destinations would be the same bug with a smaller blast radius.
        rereads = [r["reread_seconds"] for r in split_rows
                   if r.get("reread_seconds") is not None]
        mean_reread = sum(rereads) / len(rereads) if rereads else None
        if mean_reread is not None:
            say("mean re-read  : {:.1f}s  ({}) -- the BENCHMARK's independent check,"
                .format(mean_reread, rate(args.size, mean_reread)))
            say("                not the downloader's, and not counted in the split above")
        say()
        is_multipart = bool(getattr(args, "s3_bucket", None)
                            or getattr(args, "part_length", None))
        if total and vf/total > 0.25:
            say("Verification is {:.0f}% of the wall clock, a full re-read of the object."
                .format(100*vf/total))
            if is_multipart:
                say("For a MULTIPART ETag this is avoidable and already implemented: each")
                say("part's md5 is computed as the bytes stream past, and verify() logs")
                say("'etag from N recorded part digests, M re-read'. If M is large here,")
                say("the digests are not surviving -- investigate rather than accept it.")
            else:
                say("This source is verified by a WHOLE-FILE md5, which is inherently")
                say("sequential over the byte stream and cannot be computed from parts.")
                say("The read-back is therefore unavoidable for this object; only a")
                say("multipart ETag source can skip it.")
        elif total and vf / total < 0.02 and is_multipart:
            # A near-zero verify against a multipart ETag is in-transfer hashing WORKING,
            # not evidence that it was unnecessary. The old wording read #19's success as
            # an argument against building it: the read-back is absent precisely because
            # the digests were accumulated as the bytes went past. Measured on the 279 GiB
            # BAM -- verify 0.0s, hash ok -- where the avoided re-read would have been
            # ~52 minutes at the read rate from §4.1.
            say("Verification cost {:.1f}s against a MULTIPART ETag, which means the".format(vf))
            say("digest was assembled from part md5s recorded during the transfer and")
            say("THE DOWNLOADER did no post-hoc read-back. That is #19 working: a")
            say("re-read of {} at this device's rate would have been the".format(
                human(args.size)))
            say("dominant cost of localizing this object.")
            say()
            if mean_reread is not None:
                # Do not say "skipped entirely" with the benchmark's own read-back on the
                # same page. It is a different read-back, it took 1h45m on the 279 GiB run,
                # and claiming it away is how that hour became invisible.
                say("It was NOT skipped by this benchmark: the 'mean re-read' line above is")
                say("{} of independent verification, which happened.".format(
                    human_seconds(mean_reread)))
                say("That cost is the harness proving the result, not the cost of")
                say("localizing the object in production -- but it is also the only")
                say("hour-scale sample of this destination in the run, so read its rate.")
                say()
            say("Do NOT read this as 'verification is cheap'. It is cheap here because")
            say("it was moved into the transfer. Confirm with the downloader's")
            say("'etag from N recorded part digests, M re-read' line -- a large M means")
            say("the digests are not surviving and the saving is partly illusory.")
        else:
            say("Verification is a small share of THIS run, so a smaller instance type")
            say("may do (§10).")
            if not is_multipart:
                say("Note this says nothing about hashing during the transfer: that")
                say("applies to multipart ETags, and this run did not use one.")

    heading("verdict")

    # Compare the DOWNLOAD phase, not total wall clock. Total mixes phases: this used to
    # report 1.3x where the like-for-like figure was 1.9x, because every route times its
    # own verification into the total while the amount of verification differs by route.
    def download_seconds(row):
        return row["phases"].get("download") or row["seconds"]

    comparable = all("download" in r["phases"] for r in usable)
    basis = "download phase" if comparable else "total wall clock, phases unavailable"
    best = min(usable, key=download_seconds)
    base_row = next((r for r in usable if r["connections"] <= 1), None)

    say("fastest        : {} connections at {} ({})".format(
        best["connections"], rate(args.size, download_seconds(best)), basis))
    if base_row is not None:
        speedup = download_seconds(base_row) / download_seconds(best)
        say("single stream  : {}".format(rate(args.size, download_seconds(base_row))))
        say("speedup        : {:.2f}x vs a single stream (§8.5 wants >=4x)".format(speedup))
        say("               : {}".format(
            "MEETS the target" if speedup >= 4 else "DOES NOT meet the target"))
        if not comparable:
            say("               : basis is total time -- some rows logged no phases, so")
            say("                 this understates parallel rows that verified inline")
        say()
        say("Note this is the speedup against THIS source. A source that is already fast")
        say("single-stream leaves little for parallelism to win; the figure that matters")
        say("for the project is the one against the source that is slow today.")

        # A flat sweep has two readings with opposite consequences, and reporting only
        # "DOES NOT meet the target" leaves the operator to guess which one applies.
        # The concurrency figure decides it, so say what it decided.
        if speedup < 1.5:
            parallel = [r for r in usable if r.get("mean_streams") is not None]
            say()
            say("throughput did not move with the connection count, which has two")
            say("readings. The streams figure above separates them:")
            if not parallel:
                say()
                say("  No streams figure was reported, so THIS RUN CANNOT TELL YOU WHICH.")
                say("  Re-run against a downloader that emits k9pdl-streams.")
            else:
                achieved = max(r["mean_streams"] for r in parallel)
                asked = max(r["workers"] or 0 for r in parallel)
                say()
                say("  best concurrency achieved: {:.2f} of {} requests at once"
                    .format(achieved, asked))
                if achieved >= 0.7 * asked:
                    say()
                    say("  The requests WERE concurrent, so the ceiling is on the far")
                    say("  side of the wire: this source caps aggregate bandwidth, not")
                    say("  per-connection bandwidth. More connections cannot help it,")
                    say("  and no amount of tuning here will. That is a finding about")
                    say("  the source, not a failure of the downloader -- but it does")
                    say("  mean the >=4x target is unreachable against this source.")
                else:
                    say()
                    say("  The requests were NOT concurrent -- {:.2f} streams on average"
                        .format(achieved))
                    say("  against {} workers. The flat throughput is a defect in the"
                        .format(asked))
                    say("  download path, not a property of the source. Do not record")
                    say("  this as a measurement of the source.")
    else:
        say("no connections=1 row, so there is no baseline to compare against")

    # Independent of the speedup block below, which is skipped entirely when there is no
    # connections=1 row -- so its own "no streams figure" branch did not fire on a
    # single-setting run, and the run reported nothing at all about concurrency.
    dropped = [r for r in usable if r["connections"] > 1 and r.get("fell_back")]
    if dropped:
        heading("NOT A PARALLEL MEASUREMENT")
        say("{} of {} parallel settings fell back to a single stream, so the connection"
            .format(len(dropped), len([r for r in usable if r["connections"] > 1])))
        say("count in the table above is fiction for those rows -- they ran the same one")
        say("request. Reason given:")
        say()
        for row in dropped:
            say("  {:>3} connections: {}".format(row["connections"], row["fell_back"]))
        say()
        say("Nothing here measures parallelism. The most likely cause in --prefix mode is")
        say("a missing --object-size: probe_range compares the server's declared total")
        say("against the size it was given, and a prefix does not match it.")
        say()

    silent = [r for r in usable
              if r["connections"] > 1 and r.get("mean_streams") is None
              and not r.get("fell_back")]
    if silent:
        heading("NO CONCURRENCY FIGURE")
        say("{} of {} parallel settings reported no `streams:` line, so this run does"
            .format(len(silent), len([r for r in usable if r["connections"] > 1])))
        say("NOT tell you whether the requests were concurrent -- which is the question a")
        say("flat sweep exists to answer.")
        say()
        say("The usual cause is a stale downloader in the container: `k9pdl-streams` is")
        say("emitted by parallel_download.py, not by this script, so updating only the")
        say("benchmark leaves the figure missing while everything else looks new. Check")
        say("the `downloader:` md5 in the header against your working copy.")
        say()

    wire = [r["nic_bytes"] / float(args.size) for r in usable
            if r.get("nic_bytes") and args.size]
    if wire and max(wire) >= 1.5:
        heading("DUPLICATE FETCHING")
        say("At least one setting put {:.2f}x the payload on the NIC. Every byte should".
            format(max(wire)))
        say("cross the wire once; a ratio near the connection count means the server")
        say("ignored Range and returned the whole object to each request. Check the")
        say("probe output -- and note that GCS bills for the whole object per request,")
        say("so this is a cost incident and not only a wrong measurement.")
        say()

    # §8.5 wants memory bounded regardless of object size: the design's claim is that
    # only READ_BLOCK per connection is ever buffered, so RSS should be roughly flat
    # across the sweep and utterly unrelated to the 50 GB being moved.
    memory = [r["peak_rss"] for r in usable if r["peak_rss"]]
    if memory:
        say()
        say("peak RSS       : {} across the sweep".format(human(max(memory))))
        # Budget from what is actually in flight. The old fixed 256 MiB ceiling and its
        # "~1 MiB per connection" claim both predate --upload-block: the bucket route
        # buffers one block per connection, so at 16 x 8 MiB the working set is
        # legitimately ~400 MiB and a correct run was reported as "HIGHER than
        # expected". A guard that cries wolf on the default configuration is worse than
        # none, because the next reader discounts it.
        #
        # x4 rather than x1: measured at 397 MiB for 16 x 8 MiB, i.e. ~3.1x the raw
        # buffer once the request body and allocator overhead are counted.
        block = max(getattr(args, "upload_block", None) or UPLOAD_BLOCK_HINT, MIB)
        conns = max(args.connections) if isinstance(args.connections, list) \
            else args.connections
        budget = max(256 * MIB, 4 * conns * block)
        say("               : {} -- expected under {} for {} connections x {} buffered"
            .format("bounded as designed" if max(memory) < budget
                    else "HIGHER than expected; check for accumulation",
                    human(budget), conns, human(block)))
        say("               : RSS must not grow with OBJECT size -- measured flat at "
            "~32 MiB across a 32x range (64 MiB to 2 GiB)")

    # the NIC-vs-disk question §2 leaves open
    peak_nic = max((r["peak_nic_bytes_per_s"] or 0) for r in usable)
    peak_disk = max((r["peak_disk_bytes_per_s"] or 0) for r in usable)
    plateau_rows = [r for r in usable if r["connections"] > 1]
    # sustained rate at whichever setting went fastest -- the number that decides whether
    # the device is actually the ceiling
    fastest_row = max(usable, key=lambda r: r["throughput_bytes_per_s"] or 0)
    best_disk_mean = ((fastest_row.get("disk_bytes") or 0) / fastest_row["seconds"]
                      if fastest_row.get("seconds") else 0)
    say()
    say("peak NIC       : {}   (n1-standard-8 cap is ~2 GB/s)".format(rate(peak_nic, 1)))

    # /proc/diskstats only sees block devices, so a tmpfs or overlay destination reports
    # ~0 disk writes -- which the comparison below would read as "the disk is the limit"
    # when there is no disk in the path at all.
    dest_mount = probe_mount(args.dest_dir)
    dest_backing = backing_kind(dest_mount.get("fstype"))
    if dest_backing != "block device":
        say("peak disk write: not applicable -- {} is {}".format(
            args.dest_dir, dest_backing))
        say("-> no block device in the path, so this run says nothing about the disk.")
        say("   It measures the source and the NIC, which is what a tmpfs destination")
        say("   is for. Compare against the same sweep to the localization disk (§6.2).")
    else:
        say("peak disk write: {}".format(rate(peak_disk, 1)))
        # Mean, not peak. Writes land in page cache and the kernel flushes at device
        # speed, so PEAK disk hits the device's rate at every setting -- measured 87.65
        # MiB/s peak on the connections=1 row whose actual throughput was 15.96 MiB/s,
        # a 5.5x gap. Peak therefore carries no information about saturation, and the
        # heuristic built on it fired on every run that touched a disk.
        if best_disk_mean:
            say("mean disk write: {}  (at the fastest setting)".format(
                rate(best_disk_mean, 1)))
    if dest_backing == "block device" and best_disk_mean and peak_disk:
        headroom = 1 - best_disk_mean / float(peak_disk)
        still_climbing = (len(plateau_rows) >= 2
                          and max(plateau_rows, key=lambda r: r["throughput_bytes_per_s"]
                                  )["connections"] == max(r["connections"]
                                                          for r in plateau_rows))
        if headroom < 0.15:
            say("-> the DISK is the limit: sustained writes are within {:.0f}% of the"
                .format(headroom * 100))
            say("   device's peak. Raising connections will not help; a larger PD would.")
        elif headroom < 0.35:
            # The band this lands in matters: at 82% of what the device demonstrably
            # absorbs, "not saturated" is technically true and reads as though there
            # were plenty of room. There is not -- the remaining gap is overlap loss
            # between writing and reading, not spare bandwidth, and more connections
            # cannot recover it because each stream is already at its source rate.
            say("-> MOSTLY disk-bound: sustained writes are at {:.0f}% of the device's"
                .format((1 - headroom) * 100))
            say("   peak, so the remaining {:.0f}% is overlap loss rather than spare"
                .format(headroom * 100))
            say("   bandwidth. More connections will not recover it -- check whether")
            say("   per-stream throughput is already at the source's rate, in which")
            say("   case the only wins left are a faster destination or better overlap")
            say("   between the write path and the network.")
        elif still_climbing:
            say("-> NOT disk-bound yet: sustained writes are {:.0f}% below the device's"
                .format(headroom * 100))
            say("   peak AND throughput was still rising at the highest setting tried.")
            say("   Something other than the disk is holding it back -- check the")
            say("   `streams:` figures. Per-stream throughput at its source rate with")
            say("   few streams active means workers are blocked between chunks, not")
            say("   on the network.")
        else:
            say("-> sustained writes are {:.0f}% below the device's peak, so the disk is"
                .format(headroom * 100))
            say("   not saturated; the source or the download path is the limit.")
    elif peak_nic > 1.4 * GIB:
        say("-> the NIC is close to its cap, so the network is the limit.")

    plateau = plateau_rows
    if len(plateau) >= 2:
        top = max(r["throughput_bytes_per_s"] for r in plateau)
        knee = min((r["connections"] for r in plateau
                    if r["throughput_bytes_per_s"] >= 0.95 * top), default=None)
        highest = max(r["connections"] for r in plateau)
        say()
        if knee is not None and knee >= highest:
            # "within 5% of its best from 16 upward" is arithmetically true when 16 is
            # both the best and the highest setting tried -- and reading it as a knee is
            # the opposite of the truth, which is that throughput was still climbing when
            # the range ran out. Recommending the top of an exhausted range as the default
            # is how a cap gets mistaken for a plateau.
            say("NO KNEE FOUND: {} connections was both the fastest and the highest"
                .format(highest))
            say("setting tried, so throughput was still climbing when the range ran out.")
            say("This does not identify a default -- it says the sweep was too narrow.")
            say("Extend the range (MAX_CONNECTIONS caps the downloader at {}), or pick"
                .format(MAX_CONNECTIONS_HINT))
            say("the default from the DESTINATION's ceiling instead: at a write limit of")
            say("W, the useful connection count is about W / (per-stream throughput),")
            say("and more than that only buys idle sockets.")
        else:
            say("throughput is within 5% of its best from {} connections upward,"
                .format(knee))
            say("which is the value to set as the default (currently 8).")
    return {"sweep": results}


# --------------------------------------------------------------------------------
# resume
# --------------------------------------------------------------------------------

def premature(outcome):
    """
    Did this attempt get killed before it had done any real work?

    Half the target is the threshold: a kill is inherently approximate -- the monitor
    polls every 250 ms -- but landing under half means the trigger is not tracking
    progress at all, which is a defect in the harness rather than a fast download.
    """
    target = outcome.get("kill_target")
    if not target or not outcome.get("killed"):
        return False
    # Against file progress at kill time, in the same units as the threshold. If the
    # monitor never managed to read a size, treat that as premature: it means the
    # trigger was not tracking anything.
    reached = outcome.get("killed_at_bytes")
    if reached is None:
        return True
    return reached < target * 0.5


def command_resume(args):
    """
    Resumability against the real filesystem, which is where it matters: the frontier path
    only runs where SEEK_HOLE passes, and that is not the development machine.
    """
    size, verification = resolve_source(args)
    args.size = size
    heading("resume behaviour")
    say("object         : {}".format(describe_source(args)))
    say("verify         : {}".format(verification.label))
    seek = probe_seek_hole(args.dest_dir)
    say("SEEK_HOLE here : {}".format(
        "yes -- frontier recovery" if seek["supported"] else "no -- checkpoint fallback"))

    dest = os.path.join(args.dest_dir, "bench.resume.bin")
    # Two patterns, because glob's leading `*` does NOT match a leading dot -- so
    # "*bench.resume.bin*" removed the payload and left `.bench.resume.bin.k9pdl.done`
    # behind. The next run then saw a completion marker for a file that no longer
    # existed, exited 0 after 108 bytes, and reported `final hash WRONG` plus a
    # traceback from the punch-hole section. command_sweep had this right already.
    stale_patterns = [os.path.join(args.dest_dir, "*bench.resume.bin*"),
                      os.path.join(args.dest_dir, ".bench.resume.bin*")]
    for stale in sorted(set(sum((glob.glob(pat) for pat in stale_patterns), []))):
        os.unlink(stale)

    attempts = []
    total_nic = 0
    for attempt in range(1, args.max_attempts + 1):
        kill_at = None
        if attempt < args.max_attempts:
            kill_at = int(args.size * (0.25 * attempt))
        outcome = run_download(source_args(args, dest, args.size), dest, args.size,
                               args.connections, args.min_chunk,
                               verification=verification, kill_after_bytes=kill_at)
        total_nic += outcome["nic_bytes"] or 0
        attempts.append(outcome)
        outcome["kill_target"] = kill_at
        say("attempt {}: rc={} {} seconds, killed={}, NIC {}{}".format(
            attempt, outcome["returncode"], outcome["seconds"], outcome["killed"],
            human(outcome["nic_bytes"] or 0),
            "  <-- KILLED BEFORE IT GOT ANYWHERE" if premature(outcome) else ""))
        if outcome["returncode"] == 0 and not outcome["killed"]:
            break

    heading("verdict")
    correct = verification.check(dest) if os.path.exists(dest) else False
    say("final hash     : {}".format(
        "no digest to check against" if correct is None
        else ("CORRECT" if correct else "WRONG")))
    say("total received : {} for a {} object".format(human(total_nic), human(args.size)))

    # A kill that lands before its target tests nothing: the next attempt starts from
    # roughly nothing and the run measures a fresh download, which then reports a
    # flatteringly low refetch figure. Observed: three kills aimed at 25/50/75% all fired
    # after 132 bytes -- the range probe -- because the trigger read the apparent size of
    # a sparse preallocated file. The overhead then came out at 0.8%, which is the
    # protocol overhead of one clean download and nothing to do with resume.
    duds = [o for o in attempts if premature(o)]
    kills = sum(1 for o in attempts if o.get("killed"))
    if duds:
        say()
        say("NOT A RESUME MEASUREMENT: {} of {} kills landed before reaching even half"
            .format(len(duds), sum(1 for o in attempts if o["kill_target"])))
        say("of their target, so the attempts after them started from nothing and the")
        say("overhead below is a fresh download's protocol overhead, not refetched work.")
        for o in duds:
            say("  target {:>10}, file reached {:>10}".format(
                human(o["kill_target"]),
                "unknown" if o.get("killed_at_bytes") is None
                else human(o["killed_at_bytes"])))
        say()
    elif kills == 0:
        # Zero kills is not a clean run, it is no experiment at all -- and the old
        # wording rendered as "With 0 kills, a per-attempt loss of a whole chunk would
        # show up as roughly 0.00 B of overhead", comparing nothing against nothing and
        # reading as a pass. Observed when a stale done-marker made attempt 1 exit 0
        # after 108 bytes.
        say()
        say("NOT A RESUME MEASUREMENT: nothing was killed, so no resume happened.")
        if attempts and (attempts[0].get("nic_bytes") or 0) < args.size * 0.5:
            say("Attempt 1 exited {} having transferred {}, far short of the object --"
                .format(attempts[0].get("returncode"), human(attempts[0].get("nic_bytes") or 0)))
            say("the usual cause is a stale .k9pdl.done marker left beside the")
            say("destination, which short-circuits the download. Check for hidden")
            say("sidecars in the destination directory.")
        say()
    if total_nic and args.size:
        waste = total_nic - args.size
        say("refetched      : {} ({:.1f}% overhead){}".format(
            human(max(0, waste)), 100.0 * max(0, waste) / args.size,
            "  -- MEANINGLESS, see above" if (duds or kills == 0) else ""))
        say()
        say("The claim is that only the uncommitted tail is refetched. With {} kills,".format(
            len(attempts) - 1))
        say("a per-attempt loss of a whole chunk would show up as roughly")
        say("{} of overhead; a frontier/checkpoint working correctly shows far less."
            .format(human((len(attempts) - 1) * args.min_chunk * args.connections)))

    if seek["supported"] and probe_punch_hole(args.dest_dir)["supported"]:
        heading("page-cache loss (punch-hole)")
        say("This is the case SIGKILL cannot produce -- bytes the process wrote are gone")
        say("because the VM vanished before they were committed. Skipped on the dev")
        say("machine for lack of FALLOC_FL_PUNCH_HOLE, so it runs here for the first time.")
        # Guarded, because this section presumes the attempts above produced a file. When
        # they did not -- a stale done-marker short-circuiting attempt 1 -- os.open raised
        # FileNotFoundError and the run ended in a traceback, which buries the actual
        # failure (already printed above) under an unrelated one.
        if not os.path.exists(dest):
            say()
            say("SKIPPED: {} does not exist, so the attempts above produced no file to"
                .format(os.path.basename(dest)))
            say("punch holes in. Fix that failure first -- this section cannot run.")
            return {"resume": attempts, "verified": correct,
                    "verification": verification.label,
                    "punch_hole": "skipped -- no destination file"}
        fd = os.open(dest, os.O_RDWR)
        try:
            punch_hole(fd, args.size // 3, min(64 * MIB, args.size // 4))
            os.fsync(fd)
        finally:
            os.close(fd)
        marker = os.path.join(os.path.dirname(dest),
                              "." + os.path.basename(dest) + ".k9pdl.done")
        try:
            os.unlink(marker)
        except OSError:
            pass
        outcome = run_download(source_args(args, dest, args.size), dest, args.size,
                               args.connections, args.min_chunk,
                               verification=verification)
        after = verification.check(dest) if os.path.exists(dest) else False
        say("re-run rc={}, refetched {}, hash {}".format(
            outcome["returncode"], human(outcome["nic_bytes"] or 0),
            "unchecked" if after is None else ("CORRECT" if after else "WRONG")))

    return {"resume": attempts, "verified": correct, "verification": verification.label,
            "total_nic_bytes": total_nic}


# --------------------------------------------------------------------------------
# the bucket-compose route
# --------------------------------------------------------------------------------

BUCKET_CLAIM_POLL_SECONDS = 60      # the emitted wait loop's sleep
BUCKET_CREATE_POLL_CEILING = 60     # 30 polls x 2s before the label is readable


def command_claim(args):
    """
    Size `bucket_upload_wait_tries` from what a legitimate upload actually costs.

    The constant gates the **claim**, not the transfer: a sibling waits
    `bucket_upload_wait_tries` x 60s between `wolf=working` and `wolf=success`, and on
    timeout declares a live upload dead and hands the object to a take-over worker --
    silently, as a warning, costing a duplicate transfer every time. So the number to
    beat is the whole produce phase for the largest real input SET, not the throughput
    of one object.

    Three things this deliberately does differently from `routeb`, each because getting
    it wrong is how the constant was mis-sized in the first place (see
    update_localization.md 13.49, where a pd-standard DOWNLOAD figure was used to size a
    relay):

      * **Repeats.** A ceiling is a question about the tail. One run is a point, and the
        slowest relay is what has to fit, so the recommendation is built from the max.
      * **Scales to the input set.** The timeout covers every input in one localization.
        Measuring one object and sizing for a set of twelve under-sizes by 12x.
      * **Adds the overheads it cannot measure.** Bucket-create polling, label updates
        and the customTime stamping pass all sit inside the claim and outside the
        transfer. They are named and added rather than quietly ignored.
    """
    size, verification = resolve_source(args)
    args.size = size
    heading("sizing bucket_upload_wait_tries")
    say("gs url  : {}".format(args.gs_url))
    say("size    : {} relayed per run".format(human(size)))
    say("per loc : {} -- the largest real localization's non-GCS bytes".format(
        human(getattr(args, "localization_bytes", None) or size)))
    say("repeats : {}".format(args.repeat))
    say()

    dest = os.path.join(args.mount_dir, os.path.basename(args.gs_url))
    runs = []
    for attempt in range(args.repeat):
        for stale in (dest, dest + ".k9pdl.gz"):
            try:
                os.unlink(stale)
            except OSError:
                pass
        outcome = run_download(source_args(args, dest, size), dest, size,
                               args.connections, args.min_chunk,
                               verification=verification)
        ok = outcome["returncode"] == 0
        runs.append({"seconds": outcome["seconds"], "ok": ok,
                     "returncode": outcome["returncode"]})
        say("relay {}: {:.1f}s {}".format(
            attempt + 1, outcome["seconds"], "" if ok else
            "FAILED rc={}".format(outcome["returncode"])))
        if not ok:
            for line in outcome["stderr_tail"]:
                say("    {}".format(line[:100]))

    good = [r["seconds"] for r in runs if r["ok"]]
    say()
    if not good:
        heading("NO USABLE RESULT")
        say("Every relay failed, so there is no duration to size a timeout from.")
        return {"claim": {"runs": runs, "usable": 0}}

    slowest = max(good)
    # Scale by BYTES, not by a count of inputs. The count was the wrong unit: a set of
    # one 279 GiB BAM plus a 9 MB index is four inputs and ~1.0x the time, while two
    # BAMs is two inputs and 2.0x. And only non-GCS inputs count at all -- gs:// sources
    # take canine's server_side path, a GCS-to-GCS rewrite that moves no bytes through
    # this VM.
    total_bytes = getattr(args, "localization_bytes", None) or args.size
    multiple = total_bytes / float(args.size) if args.size else 1.0
    per_set = slowest * multiple
    overhead = BUCKET_CREATE_POLL_CEILING
    claim_seconds = per_set + overhead
    recommended = int(math.ceil(claim_seconds * args.safety / BUCKET_CLAIM_POLL_SECONDS))

    say("slowest relay          : {:.1f}s  (mean {:.1f}s over {} ok run(s))".format(
        slowest, sum(good) / len(good), len(good)))
    say("x {} per localization   : {:.1f}s  ({:.2f}x this object)".format(
        human(total_bytes), per_set, multiple))
    say("+ create/label overhead: {:.1f}s  (bucket-create polling ceiling)".format(overhead))
    say("= claim duration       : {:.1f}s = {:.2f} h".format(
        claim_seconds, claim_seconds / 3600.0))
    say()
    say("x{:.1f} safety          -> bucket_upload_wait_tries >= {}  ({:.2f} h)".format(
        args.safety, recommended, recommended * BUCKET_CLAIM_POLL_SECONDS / 3600.0))
    say()

    for candidate, label in ((60, "60 (the original)"), (180, "180 (current default)")):
        verdict = "ENOUGH" if candidate >= recommended else "TOO SMALL"
        say("  {:<22} {:>5.2f} h   {}".format(label,
            candidate * BUCKET_CLAIM_POLL_SECONDS / 3600.0, verdict))
    say()

    # Guards. Each of these has a matching way to read the table above as saying
    # something it does not.
    if len(good) < 3:
        say("ONE-SHOT: {} usable run(s). A timeout has to clear the SLOWEST legitimate".format(
            len(good)))
        say("upload, and this has barely sampled the distribution. Do not lower the")
        say("constant on this; --repeat 3 is the minimum worth acting on.")
        say()
    elif max(good) / min(good) > 1.5:
        say("WIDE SPREAD: slowest is {:.1f}x the fastest. The source or GCS ingest is".format(
            max(good) / min(good)))
        say("variable, so the tail is wider than {} runs can show. Prefer the larger".format(
            len(good)))
        say("candidate, or raise --safety.")
        say()
    if size < 96 * GIB:
        say("SHORT MEASUREMENT: {} per object. §6.5i found this hardware delivers a 2x".format(
            human(size)))
        say("burst for its first ~56 GiB, so a small relay reports roughly double the")
        say("sustained rate -- which UNDER-sizes a timeout. Measure at >=96 GiB before")
        say("lowering anything.")
        say()
    if multiple <= 1.0:
        say("SIZED FOR ONE OBJECT: the timeout covers a whole localization. If your")
        say("largest set relays more than {} from non-GCS sources, pass".format(
            human(args.size)))
        say("--localization-bytes with that total or this sizes for the easiest case.")
        say()

    say("Also outside this measurement, and outside the transfer: the customTime")
    say("stamping pass scales with object COUNT, and a take-over worker's own retry")
    say("budget sits on top. The recommendation is a floor, not a target.")

    return {"claim": {
        "runs": runs,
        "slowest_seconds": slowest,
        "claim_seconds": claim_seconds,
        "localization_bytes": total_bytes,
        "localization_multiple": multiple,
        "safety": args.safety,
        "recommended_tries": recommended,
        "sufficient_at_60": recommended <= 60,
        "sufficient_at_180": recommended <= 180,
    }}


def command_routeb(args):
    """
    the bucket-compose route against real GCS. Never exercised outside a fake, and the auth path in
    particular has no local coverage at all.
    """
    size, verification = resolve_source(args)
    args.size = size
    heading("the bucket-compose route (bucket destination) against real GCS")
    say("gs url : {}".format(args.gs_url))
    say("verify : {}".format(verification.label))
    say()
    say("Checks the things the fake cannot: that the metadata server (or gcloud) yields a")
    say("usable token, that resumable sessions behave as the 308/Range protocol says, and")
    say("that compose produces the right object.")

    token_source = None
    try:
        request = subprocess.run(
            ["curl", "-s", "-H", "Metadata-Flavor: Google",
             "http://metadata.google.internal/computeMetadata/v1/"
             "instance/service-accounts/default/token"],
            capture_output=True, text=True, timeout=10)
        if request.returncode == 0 and "access_token" in request.stdout:
            token_source = "metadata server"
    except Exception:
        pass
    if token_source is None and shutil.which("gcloud"):
        out = subprocess.run(["gcloud", "auth", "print-access-token"],
                             capture_output=True, text=True, timeout=60)
        if out.returncode == 0 and out.stdout.strip():
            token_source = "gcloud fallback"
    say("token source : {}".format(token_source or "NONE -- the bucket-compose route cannot run"))
    if token_source is None:
        return {"routeb": {"token": None}}

    dest = os.path.join(args.mount_dir, os.path.basename(args.gs_url))
    say("dest         : {} (must be on the gcsfuse mount for the route to be chosen)"
        .format(dest))
    extra = []
    if getattr(args, "gunzip", False):
        extra.append("--gunzip")
        say("gunzip       : on -- parts compose into <object>.k9pdl.gz, that sidecar is")
        say("               verified, then it is read back through zlib into a second")
        say("               resumable upload. --md5 must cover the COMPRESSED bytes")
    outcome = run_download(source_args(args, dest, args.size), dest, args.size,
                           args.connections, args.min_chunk, verification=verification,
                           extra=extra)
    say("rc={} in {} seconds".format(outcome["returncode"], outcome["seconds"]))
    for line in outcome["stderr_tail"]:
        say("  {}".format(line))

    result = {"token_source": token_source, "outcome": outcome}
    if getattr(args, "gunzip", False):
        # The decompressed object has no digest to check -- the advertised one covers the
        # compressed bytes -- so what is recorded instead is the length it reached and
        # how long the pass took. "Did not decode" is a legitimate result when the
        # server's metadata was wrong, and has to be distinguishable from a refusal.
        decompressed_to = outcome["decompressed_to"]
        result["gunzip"] = {
            "ran": decompressed_to is not None,
            "compressed_bytes": outcome["decompressed_from"],
            "decompressed_bytes": decompressed_to,
            "seconds": outcome["phases"].get("gunzip"),
        }
        say("gunzip phase : {}".format(
            "{} -> {} bytes in {}s".format(
                outcome["decompressed_from"], decompressed_to,
                outcome["phases"].get("gunzip"))
            if decompressed_to is not None
            else "did not decode -- kept as received, or refused; read the stderr"))
    return {"routeb": result}


# --------------------------------------------------------------------------------
# preemption: cannot be automated from the VM being preempted
# --------------------------------------------------------------------------------

PREEMPT_STEPS = """\
Forcing a real preemption cannot be driven from the VM being preempted -- it stops
executing. It also needs the SLURM requeue path to observe the resume, so it belongs in a
real wolF run rather than in this script. The steps:

 1. Submit a wolF task whose only work is a large LocalizeToDisk, so localization IS the
    job:
        wolf.LocalizeToDisk(files = {"big": "<the 50 GB URL>"})

 2. Watch for the localization disk to be created and partially written:
        gcloud compute disks list --filter="name~canine-"
        gcloud compute ssh <worker> --command 'ls -la /mnt/rwdisks/*/'
    Confirm the .k9pdl.json manifest exists and the .k9pdl.done marker does NOT.

 3. From ANOTHER machine, kill the worker outright -- delete rather than stop, so it is a
    preemption and not a clean shutdown:
        gcloud compute instances delete <worker> --quiet

 4. Confirm SLURM requeues the task and a new worker re-attaches the same disk. The log
    should show canine's "Resuming creation of persistent disk", then the downloader's
    "resuming: N/M chunks already complete".

 5. Let it finish and check:
      * the localized file's md5 matches the source;
      * the disk was labelled finished=yes only AFTER completion;
      * total bytes billed are close to one object, not two. The downloader logs
        "N transferred" per attempt; sum them.

 6. Repeat with an NFS destination (a plain `url`-mode input, no
    localize_to_persistent_disk) -- it takes a different path through the localizer.

What to watch for specifically, since these are unverified rather than merely untested:

  * If SEEK_HOLE passes on the PD (see `probe`), this is the FIRST time frontier recovery
    runs for real. Every local test used the checkpoint fallback.
  * §12.2's disk-resize daemon now runs for the localization disk. Confirm it fires and
    that `gcloud compute disks resize` succeeds from the worker's service account.
  * A preemption between "download finished" and "disk labelled finished=yes" should
    resume without re-downloading, because the .k9pdl.done marker survives. Worth
    engineering deliberately: kill the worker right after the marker appears.
"""


def command_preempt(_args):
    heading("forced preemption: manual procedure")
    say(PREEMPT_STEPS)
    return {"preempt": "manual"}


# --------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------

def build_parser():
    parser = argparse.ArgumentParser(
        prog="benchmark_localization.py",
        description="Integration benchmark for the parallel downloader, on a real worker.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Start with `probe`: it costs nothing and answers several open questions.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    def add_s3(p):
        """
        S3 source options. The store does not have to be Amazon's: canine threads a
        custom endpoint through head-object, presign and the per-chunk fallback, and this
        mirrors HandleAWSURL's own arguments so the benchmark exercises the same paths.
        """
        group = p.add_argument_group(
            "S3 source (Amazon or any S3-compatible store)",
            "Give --url for the presigned/HTTP path, or --s3-bucket/--s3-key to drive "
            "S3ApiSource, which is what runs when presigning is unavailable.")
        group.add_argument("--s3-bucket")
        group.add_argument("--s3-key")
        group.add_argument("--s3-endpoint-url", metavar="URL",
                           help="for a store that is not Amazon's; becomes "
                                "--endpoint-url on every aws call, as "
                                "HandleAWSURL's aws_endpoint_url does")
        group.add_argument("--no-sign-request", action="store_true",
                           help="force unsigned requests. Added automatically when no "
                                "credentials can be found, so it is only needed to "
                                "override credentials that do exist")
        group.add_argument("--s3-profile", metavar="NAME",
                           help="profile in ~/.aws/credentials and ~/.aws/config "
                                "(default: $AWS_PROFILE, else 'default')")
        group.add_argument("--s3-extra-args", default="",
                           help="any further aws flags, passed through verbatim")

    def add_common(p, need_url=True):
        if need_url:
            p.add_argument("--url", default="",
                           help="object to download; omit when using "
                                "--s3-bucket/--s3-key")
            p.add_argument("--size", type=int,
                           help="its size in bytes; not needed for an S3 source, where "
                                "head-object reports it")
            p.add_argument("--md5", help="expected md5. Not needed for an S3 source: the "
                                         "ETag is the md5 for a single-part object and "
                                         "the md5-of-md5s for a multipart one, and both "
                                         "are used automatically")
        p.add_argument("--dest-dir", default="/mnt/rwdisks",
                       help="where to write; use the localization disk to measure the "
                            "path that matters (default: %(default)s)")
        p.add_argument("--min-chunk", type=int, default=DEFAULT_MIN_CHUNK)
        p.add_argument("--verify-workers", type=int, default=VERIFY_READ_WORKERS,
                       help="readers for the benchmark's own ETag re-read "
                            "(default: %(default)s). More is not better: on a 316 GB "
                            "pd-standard read throughput falls with concurrency "
                            "(86/85/76/62 MiB/s at 1/2/4/8) while hashing is only ~1/7 "
                            "of read time, so the whole prize is ~15%% and 4+ readers "
                            "spend more than it is worth. Exposed to re-measure that "
                            "balance on other hardware, not to tune a run")
        p.add_argument("--prefix", action="store_true",
                       help="--size is a PREFIX of a larger object. Ranges the "
                            "single-stream baseline so it fetches the same bytes as the "
                            "parallel rows; without this it fetches the whole object")
        p.add_argument("--header", action="append", default=[], metavar="H",
                       help="request header, repeatable; forwarded to every ranged GET "
                            "and to the single-stream fallback. A private GCS object "
                            "over plain https needs "
                            "'Authorization: Bearer $(gcloud auth print-access-token)'")
        p.add_argument("--downloader", metavar="PATH",
                       help="parallel_download.py to drive; defaults to one beside this "
                            "script, else the repo's ../localization/ copy")
        p.add_argument("--json", metavar="PATH", help="also write results as JSON")
        add_s3(p)

    probe = sub.add_parser("probe", help="free environment report; run this first")
    add_s3(probe)
    probe.add_argument("--min-chunk", type=int, default=DEFAULT_MIN_CHUNK,
                       help="used only to report the chunk plan a multipart object "
                            "would get (default: %(default)s)")
    probe.add_argument("--json", metavar="PATH", help="also write results as JSON")

    sweep = sub.add_parser("sweep", help="connection sweep, speedup, NIC-vs-disk limit")
    add_common(sweep)
    sweep.add_argument("--upload-block", dest="upload_block", type=int,
                       default=UPLOAD_BLOCK_HINT,
                       help="bytes per PUT on the bucket route; sets peak RSS with "
                            "--connections")
    sweep.add_argument("--connections", type=int, nargs="+",
                       default=[1, 4, 8, 12, 16],
                       help="1 is the single-stream baseline (default: %(default)s)")
    sweep.add_argument("--keep", action="store_true", help="keep the downloaded files")

    resume = sub.add_parser("resume", help="SIGKILL resume, refetch accounting, punch-hole")
    add_common(resume)
    resume.add_argument("--connections", type=int, default=DEFAULT_CONNECTIONS_HINT)
    resume.add_argument("--max-attempts", type=int, default=4)

    routeb = sub.add_parser("routeb", help="the bucket-compose route against real GCS")
    add_common(routeb)
    routeb.add_argument("--gs-url", required=True, help="gs://bucket/object destination")
    routeb.add_argument("--mount-dir", required=True,
                        help="the gcsfuse mount the destination lives on")
    routeb.add_argument("--connections", type=int, default=DEFAULT_CONNECTIONS_HINT)
    routeb.add_argument("--gunzip", action="store_true",
                        help="exercise the post-compose decompress pass. The source must "
                             "be served Content-Encoding: gzip, and --md5 must be the "
                             "digest of the COMPRESSED bytes -- that is what the server "
                             "advertises and what gets verified. Only the fake GCS has "
                             "ever run the unknown-total PUT sequence this uses")

    claim = sub.add_parser(
        "claim", help="size bucket_upload_wait_tries from repeated relay timings")
    add_common(claim)
    claim.add_argument("--gs-url", required=True, help="gs://bucket/object destination")
    claim.add_argument("--mount-dir", required=True,
                       help="the gcsfuse mount the destination lives on")
    claim.add_argument("--connections", type=int, default=MAX_CONNECTIONS_HINT)
    claim.add_argument("--repeat", type=int, default=3,
                       help="relays to time; a ceiling is a tail question, not a mean one")
    claim.add_argument("--localization-bytes", dest="localization_bytes", type=int,
                       help="total bytes the largest real localization relays -- i.e. the "
                            "sum over its non-GCS inputs, since gs:// sources are copied "
                            "server-side and never touch this path. Defaults to --size. "
                            "The timeout covers a whole localization, not one object")
    claim.add_argument("--safety", type=float, default=2.0,
                       help="multiplier applied to the slowest observed relay")

    sub.add_parser("preempt", help="print the manual forced-preemption procedure")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)

    global DOWNLOADER
    DOWNLOADER = downloader_path(args)
    if DOWNLOADER is None:
        say("cannot find parallel_download.py. Put it beside this script, or pass")
        say("--downloader PATH, or set $K9PDL_DOWNLOADER. Looked in:")
        for candidate in DOWNLOADER_CANDIDATES:
            say("  {}".format(os.path.abspath(candidate)))
        return 1

    handlers = {
        "probe": command_probe,
        "sweep": command_sweep,
        "resume": command_resume,
        "routeb": command_routeb,
        "claim": command_claim,
        "preempt": command_preempt,
    }
    result = handlers[args.command](args)

    path = getattr(args, "json", None)
    if path and result:
        with open(path, "w") as fh:
            json.dump(result, fh, indent=2, default=str)
        say()
        say("results written to {}{}".format(path, container_path_note()))
    return 0


if __name__ == "__main__":
    sys.exit(main())

# k9pdl-eof
