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
import errno
import glob
import hashlib
import json
import os
import platform
import random
import re
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
DOWNLOADER = os.path.join(HERE, os.pardir, "localization", "parallel_download.py")

MIB = 1024 * 1024
GIB = 1024 * MIB

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


def probe_punch_hole(directory):
    if not (hasattr(os, "fallocate") and hasattr(os, "FALLOC_FL_PUNCH_HOLE")):
        return {"supported": False, "reason": "no FALLOC_FL_PUNCH_HOLE on this platform"}
    path = os.path.join(directory, ".benchpunch.{}".format(os.getpid()))
    try:
        with open(path, "wb") as fh:
            fh.write(b"A" * (4 * MIB))
        fd = os.open(path, os.O_RDWR)
        try:
            os.fallocate(fd, os.FALLOC_FL_PUNCH_HOLE | os.FALLOC_FL_KEEP_SIZE,
                         MIB, MIB)
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
    name = os.path.basename(os.path.realpath(device))
    match = re.search(r"google-(.+)$", device) or re.search(r"^(canine-.+)$", name)
    if not (match and shutil.which("gcloud")):
        return None
    disk = match.group(1)
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

    head = aws("s3api head-object --bucket {} --key {}".format(
        shlex.quote(args.s3_bucket), shlex.quote(args.s3_key)))
    if head.returncode != 0:
        say("head-object FAILED: {}".format(head.stderr.strip().splitlines()[:2]))
        say("without this nothing else can be measured -- check credentials, the")
        say("endpoint URL, and whether the bucket needs --no-sign-request")
        out["head_object"] = False
        return out

    try:
        meta = json.loads(head.stdout)
    except ValueError:
        meta = {}
    out["head_object"] = True
    out["size"] = meta.get("ContentLength")
    out["etag"] = (meta.get("ETag") or "").strip('"')
    out["parts_count"] = meta.get("PartsCount")
    say("size        : {}".format(human(out["size"]) if out["size"] else "?"))

    # what the ETag means decides whether check_hash can work at all
    etag = out["etag"]
    if re.fullmatch(r"[0-9a-f]{32}", etag or ""):
        out["etag_kind"] = "md5"
        say("etag        : {}  -- plain md5, usable directly".format(etag))
    elif re.fullmatch(r"[0-9a-f]{32}-\d+", etag or ""):
        out["etag_kind"] = "multipart"
        say("etag        : {}  -- multipart (md5-of-md5s)".format(etag))
        say("              PartsCount={}, so the downloader snaps chunks to part"
            .format(out["parts_count"]))
        say("              boundaries and computes the ETag during the transfer.")
        if not out["parts_count"]:
            say("              WARNING: multipart-shaped ETag but no PartsCount, so the")
            say("              part length is unknown and verification cannot use it.")
    else:
        out["etag_kind"] = "opaque"
        say("etag        : {!r}  -- NOT an AWS-style md5".format(etag))
        say("              This store does not follow AWS ETag semantics, so hash")
        say("              verification against the ETag would fail on correct data.")
        say("              Localize these inputs with check_hash off, or supply an md5")
        say("              out of band.")

    # ranged GET, the assumption the whole design rests on
    probe_size = min(1024, out["size"] or 1024)
    ranged = aws("s3api get-object --bucket {} --key {} --range {} /dev/stdout".format(
        shlex.quote(args.s3_bucket), shlex.quote(args.s3_key),
        shlex.quote("bytes=0-{}".format(probe_size - 1))))
    out["range_supported"] = ranged.returncode == 0
    say("ranged GET  : {}".format("yes" if out["range_supported"] else "NO -- chunked "
                                  "download cannot work against this endpoint"))

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
    for tool in ("bash", "curl", "python3", "gzip", "gunzip", "od", "aws", "gcloud",
                 "gsutil", "stat", "md5sum"):
        path = shutil.which(tool)
        result["tools"][tool] = path
        say("  {:<9} {}".format(tool, path or "MISSING"))

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

    if getattr(args, "s3_bucket", None) and getattr(args, "s3_key", None):
        result["s3"] = probe_s3_endpoint(args)

    heading("what this means")
    frontier = [d for d, v in result["destinations"].items() if v["seek_hole"]["supported"]]
    if frontier:
        say("SEEK_HOLE passes on: {}".format(", ".join(frontier)))
        say("-> the frontier path is live here. It has NEVER been integration-tested,")
        say("   because it fails the probe on the development machine (APFS).")
    else:
        say("SEEK_HOLE passes nowhere -- every download would use the 8 MiB checkpoint")
        say("fallback. Worth understanding before trusting the resume numbers.")
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
    """
    return (
        "[ -f {path} ] && SZ=$(stat --printf '%s' {path}) || SZ=0; "
        "if [ $SZ != {size} ]; then "
        "aws s3api {extra} get-object --bucket {bucket} --key {key} "
        '--range "bytes=$SZ-" >(cat >> {path}) > /dev/null; fi'
    ).format(path=shlex.quote(dest), size=size, extra=s3_extra_args(args),
             bucket=shlex.quote(args.s3_bucket), key=shlex.quote(args.s3_key))


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
    if getattr(args, "s3_bucket", None) and getattr(args, "s3_key", None) and not url:
        return ["--url", "",                      # empty: "presign produced nothing"
                "--s3-bucket", args.s3_bucket,
                "--s3-key", args.s3_key,
                # `--opt=value`, not `--opt value`: the value itself starts with dashes
                # (e.g. "--no-sign-request"), and argparse treats such a token as an
                # option unless it happens to contain a space.
                "--s3-extra-args={}".format(s3_extra_args(args)),
                "--legacy-cmd", s3_legacy_command(args, dest, size)]
    return ["--url", url]


def run_download(source, dest, size, connections, min_chunk, extra=(), verification=None,
                 kill_after_bytes=None):
    """
    One download, measured. Returns a dict of results.

    `source` is the list of source arguments from source_args().

    With connections=1 the downloader declines and falls back to the legacy single stream,
    so that row is genuinely today's behaviour and the right baseline for the speedup
    claim -- it is not the new code throttled to one connection.
    """
    command = [sys.executable, os.path.abspath(DOWNLOADER)] + list(source) + [
        "--dest", dest, "--size", str(size),
        "--connections", str(connections), "--min-chunk", str(min_chunk)]
    if verification is not None:
        command += verification.downloader_args
    command += list(extra)

    with Sampler() as sampler:
        process = subprocess.Popen(command, stdout=subprocess.DEVNULL,
                                   stderr=subprocess.PIPE)
        killed = False
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
                    if os.path.getsize(dest) >= kill_after_bytes:
                        process.send_signal(signal.SIGKILL)
                        killed = True
                except OSError:
                    pass
            time.sleep(0.25)
        stderr = process.stderr.read().decode("utf-8", "replace")

    # The downloader emits "k9pdl-phase <name> <secs>s[, rate]" per phase. Capturing the
    # split matters because localization is two costs, not one: moving the bytes, then
    # re-reading them to hash. Which dominates decides whether hashing during the
    # transfer is worth building and whether a smaller instance type would do.
    phases = {}
    for match in re.finditer(r"k9pdl-phase (\w+) ([\d.]+)s", stderr):
        phases[match.group(1)] = float(match.group(2))

    return {
        "phases": phases,
        "connections": connections,
        "returncode": process.returncode,
        "seconds": round(sampler.seconds, 2),
        "killed": killed,
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


def file_multipart_etag(path, part_length, block=8 * MIB):
    """
    AWS's multipart ETag: md5 of the concatenated per-part md5 digests, then `-<n parts>`.

    Computed here so the benchmark's verdict is independent of the downloader's own. If we
    simply reported what the downloader concluded, "md5 ok" would mean no more than "it did
    not notice a problem".
    """
    part_digests = []
    with open(path, "rb") as fh:
        while True:
            part = hashlib.md5()
            remaining = part_length
            while remaining:
                chunk = fh.read(min(block, remaining))
                if not chunk:
                    break
                part.update(chunk)
                remaining -= len(chunk)
            if remaining == part_length:
                break                       # read nothing: end of file
            part_digests.append(part.digest())
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
           "part_length": None}
    if out["parts_count"] > 1:
        out["part_length"] = head("--part-number 1").get("ContentLength")
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

    def check(self, path):
        """True, False, or None when there is nothing to check against."""
        if self.kind == "md5":
            return file_md5(path) == self.value
        if self.kind == "etag":
            return file_multipart_etag(path, self.part_length) == self.value
        return None

    @property
    def label(self):
        if self.kind == "md5":
            return "md5 {}".format(self.value)
        if self.kind == "etag":
            return "multipart etag {} ({} byte parts)".format(self.value, self.part_length)
        return "NOT VERIFIED -- {}".format(self.reason or "no digest available")


def describe_source(args):
    # getattr throughout: this is called with whatever namespace the subcommand built,
    # and `probe` has no --url. Mixing direct access with getattr made it crash on one
    # shape while tolerating another.
    url = (getattr(args, "url", None) or "").strip()
    if getattr(args, "s3_bucket", None) and getattr(args, "s3_key", None) and not url:
        return "s3://{}/{} via {} (S3ApiSource)".format(
            args.s3_bucket, args.s3_key,
            args.s3_endpoint_url or "amazon")
    return args.url


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
            else:
                verification = Verification(
                    None, reason="the ETag {!r} is not an md5; this store does not follow "
                                 "AWS semantics".format(etag))

    if verification is None:
        verification = Verification(
            None, reason="no --md5 given and the source carries no usable digest")
    if size is None:
        raise SystemExit("--size is required for this source (only S3 can report it)")
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
    say("verify    : {}".format(verification.label))
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
            outcome["verified"] = verification.check(dest)
        else:
            outcome["verified"] = False
        throughput = args.size / outcome["seconds"] if outcome["seconds"] else 0
        outcome["throughput_bytes_per_s"] = throughput
        if connections <= 1:
            baseline = throughput
        outcome["speedup_vs_single_stream"] = (
            round(throughput / baseline, 2) if baseline else None)
        results.append(outcome)

        say("{:>5}  {:>9}  {:>13}  {:>13}  {:>13}  {:>10}  {:>4}".format(
            connections, outcome["seconds"], rate(args.size, outcome["seconds"]),
            rate(outcome["peak_nic_bytes_per_s"] or 0, 1),
            rate(outcome["peak_disk_bytes_per_s"] or 0, 1),
            human(outcome["peak_rss"]) if outcome["peak_rss"] else "?",
            "ok" if outcome["verified"] else
            ("-" if outcome["verified"] is None else "BAD")))
        if outcome["phases"]:
            say("        phases: {}".format("  ".join(
                "{} {:.1f}s".format(k, v) for k, v in outcome["phases"].items())))
        if not args.keep:
            try:
                os.unlink(dest)
            except OSError:
                pass

    # download vs verify: the split that decides §10's machine-type question and whether
    # in-transfer hashing is worth building
    # `in`, not `.get()`: a verify of 0.0s is falsy, and a fast verify is precisely the
    # result that argues the node can be sized down. Dropping it would hide that.
    splits = [r["phases"] for r in results if "verify" in r["phases"]]
    if splits:
        heading("download vs verify")
        dl = sum(s.get("download", 0) for s in splits) / len(splits)
        vf = sum(s["verify"] for s in splits) / len(splits)
        total = dl + vf
        say("mean download : {:.1f}s ({:.0f}%)".format(dl, 100*dl/total if total else 0))
        say("mean verify   : {:.1f}s ({:.0f}%)".format(vf, 100*vf/total if total else 0))
        say()
        if total and vf/total > 0.25:
            say("Verification is {:.0f}% of the wall clock. On the in-place route that is a".format(
                100*vf/total))
            say("full re-read of the object, and it is avoidable: chunk boundaries are")
            say("already snapped to S3 part boundaries, so each part's md5 could be")
            say("computed as the bytes stream past instead of afterwards.")
            say("-> worth building. It also means the node's cores are doing real work,")
            say("   so do not size the instance down on the assumption localization is")
            say("   pure IO.")
        else:
            say("Verification is a small share, so hashing during the transfer would buy")
            say("little, and the cores are mostly idle -- a smaller instance type is")
            say("worth investigating (§10).")

    heading("verdict")
    best = max(results, key=lambda r: r["throughput_bytes_per_s"])
    say("fastest        : {} connections at {}".format(
        best["connections"], rate(args.size, best["seconds"])))
    if baseline:
        say("speedup        : {}x vs a single stream (§8.5 wants >=4x)".format(
            best["speedup_vs_single_stream"]))
        say("               : {}".format(
            "MEETS the target" if (best["speedup_vs_single_stream"] or 0) >= 4
            else "DOES NOT meet the target"))

    # §8.5 wants memory bounded regardless of object size: the design's claim is that
    # only READ_BLOCK per connection is ever buffered, so RSS should be roughly flat
    # across the sweep and utterly unrelated to the 50 GB being moved.
    memory = [r["peak_rss"] for r in results if r["peak_rss"]]
    if memory:
        say()
        say("peak RSS       : {} across the sweep".format(human(max(memory))))
        budget = args.min_chunk  # a generous ceiling: one chunk, not one READ_BLOCK
        say("               : {} -- the claim is ~1 MiB per connection buffered".format(
            "bounded as designed" if max(memory) < max(256 * MIB, budget)
            else "HIGHER than expected; check for accumulation"))

    # the NIC-vs-disk question §2 leaves open
    peak_nic = max((r["peak_nic_bytes_per_s"] or 0) for r in results)
    peak_disk = max((r["peak_disk_bytes_per_s"] or 0) for r in results)
    say()
    say("peak NIC       : {}   (n1-standard-8 cap is ~2 GB/s)".format(rate(peak_nic, 1)))
    say("peak disk write: {}".format(rate(peak_disk, 1)))
    if peak_nic and peak_disk:
        if peak_disk < peak_nic * 0.8:
            say("-> the DISK looks like the limit, as §2 predicted for LocalizeToDisk.")
            say("   Raising connections further will not help; a larger PD would.")
        elif peak_nic > 1.4 * GIB:
            say("-> the NIC is close to its cap, so the network is the limit.")
        else:
            say("-> neither is saturated; the source may be the limit.")

    plateau = [r for r in results if r["connections"] > 1]
    if len(plateau) >= 2:
        top = max(r["throughput_bytes_per_s"] for r in plateau)
        knee = min((r["connections"] for r in plateau
                    if r["throughput_bytes_per_s"] >= 0.95 * top), default=None)
        say()
        say("throughput is within 5% of its best from {} connections upward,".format(knee))
        say("which is the value to set as the default (currently 8).")
    return {"sweep": results}


# --------------------------------------------------------------------------------
# resume
# --------------------------------------------------------------------------------

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
    for stale in glob.glob(os.path.join(args.dest_dir, "*bench.resume.bin*")):
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
        say("attempt {}: rc={} {} seconds, killed={}, NIC {}".format(
            attempt, outcome["returncode"], outcome["seconds"], outcome["killed"],
            human(outcome["nic_bytes"] or 0)))
        if outcome["returncode"] == 0 and not outcome["killed"]:
            break

    heading("verdict")
    correct = verification.check(dest) if os.path.exists(dest) else False
    say("final hash     : {}".format(
        "no digest to check against" if correct is None
        else ("CORRECT" if correct else "WRONG")))
    say("total received : {} for a {} object".format(human(total_nic), human(args.size)))
    if total_nic and args.size:
        waste = total_nic - args.size
        say("refetched      : {} ({:.1f}% overhead)".format(
            human(max(0, waste)), 100.0 * max(0, waste) / args.size))
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
        fd = os.open(dest, os.O_RDWR)
        try:
            os.fallocate(fd, os.FALLOC_FL_PUNCH_HOLE | os.FALLOC_FL_KEEP_SIZE,
                         args.size // 3, min(64 * MIB, args.size // 4))
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
    outcome = run_download(source_args(args, dest, args.size), dest, args.size,
                           args.connections, args.min_chunk, verification=verification)
    say("rc={} in {} seconds".format(outcome["returncode"], outcome["seconds"]))
    for line in outcome["stderr_tail"]:
        say("  {}".format(line))
    return {"routeb": {"token_source": token_source, "outcome": outcome}}


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
        p.add_argument("--min-chunk", type=int, default=64 * MIB)
        p.add_argument("--json", metavar="PATH", help="also write results as JSON")
        add_s3(p)

    probe = sub.add_parser("probe", help="free environment report; run this first")
    add_s3(probe)
    probe.add_argument("--json", metavar="PATH", help="also write results as JSON")

    sweep = sub.add_parser("sweep", help="connection sweep, speedup, NIC-vs-disk limit")
    add_common(sweep)
    sweep.add_argument("--connections", type=int, nargs="+",
                       default=[1, 4, 8, 12, 16],
                       help="1 is the single-stream baseline (default: %(default)s)")
    sweep.add_argument("--keep", action="store_true", help="keep the downloaded files")

    resume = sub.add_parser("resume", help="SIGKILL resume, refetch accounting, punch-hole")
    add_common(resume)
    resume.add_argument("--connections", type=int, default=8)
    resume.add_argument("--max-attempts", type=int, default=4)

    routeb = sub.add_parser("routeb", help="the bucket-compose route against real GCS")
    add_common(routeb)
    routeb.add_argument("--gs-url", required=True, help="gs://bucket/object destination")
    routeb.add_argument("--mount-dir", required=True,
                        help="the gcsfuse mount the destination lives on")
    routeb.add_argument("--connections", type=int, default=8)

    sub.add_parser("preempt", help="print the manual forced-preemption procedure")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)

    if not os.path.exists(DOWNLOADER):
        say("cannot find the downloader at {}".format(DOWNLOADER))
        return 1

    handlers = {
        "probe": command_probe,
        "sweep": command_sweep,
        "resume": command_resume,
        "routeb": command_routeb,
        "preempt": command_preempt,
    }
    result = handlers[args.command](args)

    path = getattr(args, "json", None)
    if path and result:
        with open(path, "w") as fh:
            json.dump(result, fh, indent=2, default=str)
        say()
        say("results written to {}".format(path))
    return 0


if __name__ == "__main__":
    sys.exit(main())

# k9pdl-eof
