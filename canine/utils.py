import typing
import os
import sys
import select
import io
import re
import warnings
import logging
import datetime
import threading
from collections import namedtuple
import functools
import shlex
import subprocess
import google.auth
import google.api_core.exceptions
import google.cloud.storage
import paramiko
import shutil
import time
import numpy as np
import pandas as pd
import requests
import hashlib

def isatty(*streams: typing.IO) -> bool:
    """
    Returns true if all of the provided streams are ttys
    """
    for stream in streams:
        try:
            if not (hasattr(stream, 'fileno') and os.isatty(stream.fileno())):
                return False
        except io.UnsupportedOperation:
            return False
    return True

class ArgumentHelper(dict):
    """
    Helper class for setting arguments to slurm commands
    Used only to handle keyword arguments to console commands
    Should not be responsible for positionals
    """

    def __init__(self, *flags: str, **params: typing.Any):
        """
        Creates a new ArgumentHelper
        Flags can be passed as positional arguments
        Parameters can be passed as keyword arguments
        """
        object.__setattr__(self, 'defaults', {})
        object.__setattr__(self, 'flags', [item for item in flags])
        object.__setattr__(self, 'params', {k:v for k,v in params.items()})
        for key, val in [*self.params.items()]:
            if val is True:
                self.flags.append(key)
                del self.params[key]

    def __repr__(self) -> str:
        return '<ArgumentHelper{}>'.format(
            self.commandline
        )

    def __getattr__(self, name):
        return self[name]

    def __setattr__(self, name, value):
        self[name] = value

    def __delattr__(self, name):
        del self[name]

    def __getitem__(self, name):
        if name in self.params:
            return self.params[name]
        if name in self.flags:
            return True
        if name in self.defaults:
            return self.defaults[name]

    def __setitem__(self, name, value):
        if value is True:
            self.flags.append(name)
        elif value is False:
            object.__setattr__(self, 'flags', [flag for flag in self.flags if flag != name])
        else:
            self.params[name] = value

    def __delitem__(self, name):
        if name in self.params:
            del self.params[name]
        elif name in self.flags:
            object.__setattr__(self, 'flags', [flag for flag in self.flags if flag != name])
        else:
            raise KeyError("No such argument {}".format(name))

    @staticmethod
    def translate(flag) -> str:
        """Converts acceptable python strings to command line args"""
        return flag.replace('_', '-')

    @property
    def commandline(self) -> str:
        """Expands the arguments to command line form"""
        return '{short_prespace}{short_flags}{short_params}{long_flags}{params}'.format(
            short_prespace=' -' if len([f for f in self.flags if len(f) == 1]) else '',
            short_flags=''.join(flag for flag in self.flags if len(flag)==1),
            short_params=''.join(
                ' -{}={}'.format(self.translate(key), shlex.quote(value))
                for key, value in self.params.items()
                if len(key) == 1
            ),
            long_flags=''.join(' --{}'.format(self.translate(flag)) for flag in self.flags if len(flag) > 1),
            params=''.join(
                ' --{}={}'.format(self.translate(key), shlex.quote(value))
                for key, value in self.params.items()
                if len(key) > 1
            )
        )

    def setdefaults(self, **kwargs: typing.Any):
        self.defaults.update(kwargs)

def make_interactive(channel: paramiko.Channel) -> typing.Tuple[int, typing.BinaryIO, typing.BinaryIO]:
    """
    Manages an interactive command
    Takes in a paramiko.Channel shared by stdin, stdout, stderr of a currently running command
    The current interpreter stdin is duplicated and written to the command's stdin
    The command's stdout and stderr are written to the interpreter's stdout and stderr
    and also buffered in a ByteStream for later reading
    Returns (exit status, Stdout buffer, Stderr bufer)
    """
    infd = sys.stdin.fileno()
    channelfd = channel.fileno()
    poll = select.poll()
    poll.register(infd, select.POLLIN+select.POLLPRI+select.POLLERR+select.POLLHUP)
    poll.register(channelfd, select.POLLIN+select.POLLPRI+select.POLLERR+select.POLLHUP)
    stdout = io.BytesIO()
    stderr = io.BytesIO()
    while not channel.exit_status_ready():
        for fd, event in poll.poll(0.5):
            if fd == infd and event & (select.POLLIN + select.POLLPRI):
                # Text available on python stdin
                channel.send(os.read(infd, 4096))
        if channel.recv_ready():
            content = channel.recv(4096)
            sys.stdout.write(content.decode())
            sys.stdout.flush()
            stdout.write(content)
        if channel.recv_stderr_ready():
            content = channel.recv_stderr(4096)
            sys.stderr.write(content.decode())
            sys.stderr.flush()
            stderr.write(content)
    if channel.recv_ready():
        content = channel.recv(4096)
        sys.stdout.write(content.decode())
        sys.stdout.flush()
        stdout.write(content)
    if channel.recv_stderr_ready():
        content = channel.recv_stderr(4096)
        sys.stderr.write(content.decode())
        sys.stderr.flush()
        stderr.write(content)
    stdout.seek(0,0)
    stderr.seek(0,0)
    return channel.recv_exit_status(), stdout, stderr

def get_default_gcp_zone():
    try:
        response = requests.get(
            'http://metadata.google.internal/computeMetadata/v1/instance/zone',
            headers={
                'Metadata-Flavor': 'Google'
            }
        )
        if response.status_code == 200:
            return os.path.basename(response.text)
    except requests.exceptions.ConnectionError:
        pass
    # not on GCE instance, check env
    try:
        response = subprocess.run('gcloud config get-value compute/zone', shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if response.returncode == 0 and b'(unset)' not in response.stdout.strip():
            return response.stdout.strip().decode()
    except subprocess.CalledProcessError:
        pass
    # gcloud config not happy, just return default
    return 'us-central1-a'

__DEFAULT_GCP_PROJECT__ = None

def get_default_gcp_project():
    """
    Returns the currently configured default project
    """
    global __DEFAULT_GCP_PROJECT__
    try:
        if __DEFAULT_GCP_PROJECT__ is None:
            __DEFAULT_GCP_PROJECT__ = google.auth.default()[1]
    except google.auth.exceptions.GoogleAuthError:
        warnings.warn(
            "Unable to load gcloud credentials. Some features may not function properly",
            stacklevel=1
        )
    return __DEFAULT_GCP_PROJECT__

STORAGE_CLIENT = None
storage_client_creation_lock = threading.Lock()

def gcloud_storage_client():
    global STORAGE_CLIENT
    with storage_client_creation_lock:
        if STORAGE_CLIENT is None:
            # this is the expensive operation
            STORAGE_CLIENT = google.cloud.storage.Client()
    return STORAGE_CLIENT

def _sanitize_bucket_name_component(s: str) -> str:
    """
    Lowercases and replaces any run of characters not valid in a GCS bucket
    name with a single hyphen, trimming leading/trailing hyphens.
    """
    s = re.sub(r'[^a-z0-9-]+', '-', s.lower()).strip('-')
    return s if s else "default"

def _zone_to_region(zone: str) -> str:
    """
    Derives a GCP region from a zone (e.g. "us-central1-a" -> "us-central1").
    Standard (non-zonal) bucket locations must be a region, not a zone.
    """
    return zone.rsplit('-', 1)[0]

@functools.lru_cache(maxsize=None)
def get_project_number(project: str) -> str:
    """
    Numeric ID of `project`. Used in localization bucket names because, unlike
    the project ID, it is stable across project renames. Cached: this shells out
    once per project per process.
    """
    proc = subprocess.run(
        ["gcloud", "projects", "describe", project, "--format=value(projectNumber)"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    check_call(
        "gcloud projects describe {}".format(project),
        proc.returncode, io.BytesIO(proc.stdout), io.BytesIO(proc.stderr)
    )
    number = proc.stdout.decode().strip()
    if not number.isdigit():
        raise ValueError("Could not resolve a numeric project ID for {!r} (got {!r})".format(project, number))
    return number

## Longest hash that still fits in a 63-char bucket name for every current GCP
## region. With a 12-digit project number the budget is
## 63 - len("wolf-") - len(project_number) - len(region) - 2 separators, which
## bottoms out at 21 for "northamerica-northeast1" (23 chars, the longest region
## name). A fixed 21 is used everywhere so names are uniform regardless of where
## the cluster runs. 21 hex = 84 bits; collision odds at 1e6 buckets are ~5e-14.
LOCALIZATION_BUCKET_HASH_LEN = 21

def localization_bucket_name(project_number: str, region: str, content_hash: str) -> str:
    """
    Deterministic name of the bucket backing one localization:
    wolf-<project_number>-<region>-<21 chars of content_hash>.

    The region is part of the name because buckets are regional -- one bucket
    cannot serve two regions, so the same content localized in two regions needs
    two distinct (globally unique) names. The project number, rather than the
    project ID, keeps the name stable across project renames.

    `content_hash` is the same hash_set() value used for the old RODISK name, so
    identical input sets still converge on one bucket.
    """
    name = "wolf-{}-{}-{}".format(
      _sanitize_bucket_name_component(str(project_number)),
      _sanitize_bucket_name_component(region),
      content_hash[:LOCALIZATION_BUCKET_HASH_LEN],
    )
    # A longer future project number or a new, longer region name must fail here
    # rather than reach GCS as an invalid name.
    if not re.match(r'^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$', name):
        raise ValueError("Computed an invalid GCS bucket name: {!r}".format(name))
    return name

def get_or_create_workflow_bucket(zone: str, project: str, workflow_name: typing.Optional[str] = None) -> str:
    """
    Get or create the standard regional bucket backing bucket-mounted
    (RODISK-replacement) localization for one workflow. The name is
    deterministic -- canine-<project>-<sanitized workflow_name> -- so
    repeated or concurrent runs of the same workflow reuse the same
    bucket rather than creating a new one each time. Also idempotently
    ensures a "delete 5 days after last touched" lifecycle rule is present
    (keyed on customTime, not object age -- see BUCKET_FUSE_MIGRATION.md).
    Raises on a genuine creation failure (permissions, quota, bad zone).
    """
    client = gcloud_storage_client()
    prefix = "canine-{}-".format(project)
    name_budget = 63 - len(prefix)
    sanitized = _sanitize_bucket_name_component(workflow_name or "default")[:name_budget]
    bucket_name = prefix + sanitized

    bucket = client.bucket(bucket_name)
    if not bucket.exists():
        try:
            bucket = client.create_bucket(bucket_name, project=project, location=_zone_to_region(zone))
        except google.api_core.exceptions.Conflict:
            # bucket was created concurrently by another run of the same
            # workflow; this is the expected reuse case, not an error
            bucket = client.bucket(bucket_name)
            bucket.reload()

    has_lifecycle_rule = any(
        rule.get("action", {}).get("type") == "Delete" and "daysSinceCustomTime" in rule.get("condition", {})
        for rule in bucket.lifecycle_rules
    )
    if not has_lifecycle_rule:
        bucket.add_lifecycle_delete_rule(days_since_custom_time=5)
        bucket.patch()

    return bucket_name

def get_or_create_rapid_cache(bucket: str, zone: str, ttl: str = "7d", ingest_on_write: bool = True):
    """
    Get or create a Rapid Cache (formerly Anywhere Cache) instance for
    `bucket` in `zone`. Callers should treat failure here as best-effort/
    non-fatal: Rapid Cache degrades gracefully to normal bucket latency on
    a miss or absent cache, so a failure to provision it should not fail
    the workflow the way a bucket-creation failure does.
    """
    list_proc = subprocess.run(
        ["gcloud", "storage", "buckets", "anywhere-caches", "list", "gs://{}".format(bucket), "--format=value(zone)"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    check_call(
        "gcloud storage buckets anywhere-caches list gs://{}".format(bucket),
        list_proc.returncode, io.BytesIO(list_proc.stdout), io.BytesIO(list_proc.stderr)
    )
    if zone in list_proc.stdout.decode().split():
        return

    create_cmd = [
        "gcloud", "storage", "buckets", "anywhere-caches", "create",
        "gs://{}".format(bucket), zone, "--ttl={}".format(ttl)
    ]
    if ingest_on_write:
        create_cmd.append("--enable-ingest-on-write")
    create_proc = subprocess.run(create_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    check_call(' '.join(create_cmd), create_proc.returncode, io.BytesIO(create_proc.stdout), io.BytesIO(create_proc.stderr))

def touch_object(bucket: str, path: str):
    """
    Bumps an object's customTime metadata to now, so a GCS Object Lifecycle
    Management rule keyed on daysSinceCustomTime treats this object as
    freshly touched (its 5-day clock restarts) rather than expiring it
    based on when it was first uploaded.
    """
    blob = gcloud_storage_client().bucket(bucket).blob(path)
    blob.custom_time = datetime.datetime.now(datetime.timezone.utc)
    blob.patch()

def check_call(cmd:str, rc: int, stdout: typing.Optional[typing.BinaryIO] = None, stderr: typing.Optional[typing.BinaryIO] = None):
    """
    Checks that the rc is 0
    If not, flush stdout and stderr streams and raise a CalledProcessError
    """
    if rc != 0:
        if stdout is not None:
            sys.stdout.write(stdout.read().decode())
            sys.stdout.flush()
        if stderr is not None:
            sys.stderr.write(stderr.read().decode())
            sys.stderr.flush()
        raise subprocess.CalledProcessError(rc, cmd)

predefined_mtypes = {
    # cost / CPU in each of the predefined tracks
    'n1-standard': (0.0475, 0.01),
    'n1-highmem': (0.0592, 0.0125),
    'n1-highcpu': (0.03545, 0.0075),
    'n2-standard': (0.4855, 0.01175),
    'n2-highmem': (0.0655, 0.01585),
    'n2-highcpu': (0.03585, 0.00865),
    'm1-ultramem': (0.1575975, 0.0332775),
    'm2-ultramem': (0.2028173077, 0), # no preemptible version
    'c2-standard': (0.0522, 0.012625)
}

CustomPricing = namedtuple('CustomPricing', [
    'cpu_cost',
    'mem_cost',
    'ext_cost',
    'preempt_cpu_cost',
    'preempt_mem_cost',
    'preempt_ext_cost'
])

custom_mtypes = {
    # cost / CPU, extended memory cost, preemptible cpu, preemptible extended memory
    'n1-custom': CustomPricing(
        0.033174, 0.004446, 0.00955,
        0.00698, 0.00094, 0.002014 #extends > 6.5gb/core
    ),
    'n2-custom': CustomPricing(
        0.033174, 0.004446, 0.00955,
        0.00802, 0.00108, 0.002310 # extends > 7gb/core
    )
}

fixed_cost = {
    # For fixed machine types, direct mapping of cost
    'm1-megamem-96': (10.6740, 2.26),
    'f1-micro': (0.0076, 0.0035),
    'g1-small': (0.0257, 0.007)
}

gpu_pricing = {
    'nvidia-tesla-t4': (0.95, 0.29),
    'nvidia-tesla-p4': (0.60, 0.216),
    'nvidia-tesla-v100': (2.48, 0.74),
    'nvidia-tesla-p100': (1.46, 0.43),
    'nvidia-tesla-k80': (0.45, 0.135)
}

@functools.lru_cache()
def _get_mtype_cost(mtype: str) -> typing.Tuple[float, float]:
    """
    Returns the hourly cost of a google VM based on machine type.
    Returns a tuple of (non-preemptible cost, preemptible cost)
    """
    if mtype in fixed_cost:
        return fixed_cost[mtype]
    components = mtype.split('-')
    if len(components) < 3:
        raise ValueError("mtype {} not in expected format".format(mtype))
    track = '{}-{}'.format(components[0], components[1])
    if 'custom' in mtype:
        # (n1-|n2-)?custom-(\d+)-(\d+)(-ext)?
        if components[0] == 'custom':
            components = ['n1'] + components
            track = '{}-{}'.format(components[0], components[1])
        if len(components) not in {4, 5}:
            raise ValueError("Custom mtype {} not in expected format".format(mtype))
        cores = int(components[2])
        mem = int(components[3]) / 1024
        if track == 'n1-custom':
            reg_mem = min(mem, cores * 6.5)
        else:
            reg_mem = min(mem, cores * 8)
        ext_mem = mem - reg_mem
        price_model = custom_mtypes[track]
        return (
            (price_model.cpu_cost * cores) + (price_model.mem_cost * reg_mem) + (price_model.ext_cost * ext_mem),
            (price_model.preempt_cpu_cost * cores) + (price_model.preempt_mem_cost * reg_mem) + (price_model.preempt_ext_cost * ext_mem)
        )
    if track not in predefined_mtypes:
        raise ValueError("mtype family {} not defined".format(track))
    cores = int(components[2])
    return (
        predefined_mtypes[track][0] * cores,
        predefined_mtypes[track][1] * cores
    )

def gcp_hourly_cost(mtype: str, preemptible: bool = False, ssd_size: int = 0, hdd_size: int = 0, gpu_type: typing.Optional[str] = None, gpu_count: int = 0) -> float:
    """
    Gets the hourly cost of a GCP VM based on its machine type and disk size.
    Does not include any sustained usage discounts. Actual pricing may vary based
    on compute region
    """
    mtype_cost, preemptible_cost = _get_mtype_cost(mtype)
    return (
        (preemptible_cost if preemptible else mtype_cost) +
        (0.00023287671232876715 * ssd_size) +
        (0.00005479452055 * hdd_size) +
        (
            0 if gpu_type is None or gpu_count < 1
            else (gpu_pricing[gpu_type][1 if preemptible else 0] * gpu_count)
        )
    )

# rmtree_retry removed in favor of AbstractTransport.rmtree

from threading import Lock
write_lock = Lock()
read_lock = Lock()

def pandas_write_hdf5_buffered(df: pd.DataFrame, key: str, buf: io.BufferedWriter):
    """
	Write a Pandas dataframe in HDF5 format to a buffer.
    """

    ## I am getting
    ##   HDF5ExtError("Unable to open/create file '/dev/null'")
    ##   unable to truncate a file which is already open
    with write_lock:
        with pd.HDFStore(
          "/dev/null",
          mode = "w",
          driver = "H5FD_CORE",
          driver_core_backing_store = 0
        ) as store:
            # output columns routinely hold Python lists (e.g. multiple output files
            # per job), which PyTables' fixed HDF5 format can only store by pickling --
            # a performance-only tradeoff we're accepting, not a bug (see comment history)
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category = pd.errors.PerformanceWarning)
                store["results"] = df
            buf.write(store._handle.get_file_image())

def pandas_read_hdf5_buffered(key: str, buf: io.BufferedReader) -> pd.DataFrame:
    """
	Read a Pandas dataframe in HDF5 format from a buffer.
    """
    ## Without this lock, job avoidance breaks when starting two jobs simultaneously!!
    with read_lock:
        with pd.HDFStore(
          "dummy_hdf5",
          mode = "r",
          driver = "H5FD_CORE",
          driver_core_backing_store = 0,
          driver_core_image = buf.read()
        ) as store:
            return store[key]

def base32(buf: bytes):
    """
    Convert a byte array into a base32 encoded string
    """
    table = np.array(list("abcdefghijklmnopqrstuvwxyz012345"))

    bits = np.unpackbits(np.frombuffer(buf, dtype = np.uint8))
    bits = np.pad(bits, (0, 5 - (len(bits) % 5)), constant_values = 0).reshape(-1, 5)
    return "".join(table[np.ravel(bits@2**np.c_[4:-1:-1])])

def sha1_base32(buf: bytes, n: int = None):
    """
    Return a base32 representation of the first n bytes of SHA1(buf).
    If n = None, the entire buffer will be encoded.
    """

    return base32(hashlib.sha1(buf).digest()[slice(0, n)])
  
## Hook for get external logging module

CANINE_GET_LOGGER_HOOK = None

class canine_logging:

    @staticmethod
    def set_get_logger_hook(func):
        global CANINE_GET_LOGGER_HOOK
        CANINE_GET_LOGGER_HOOK = func

    @staticmethod
    def log(level, msg, *args, **kwargs):
        if not CANINE_GET_LOGGER_HOOK:
            return print(msg)
        else:
            return CANINE_GET_LOGGER_HOOK().log(level, msg, *args, **kwargs)

    @staticmethod
    def info(msg):
        if not CANINE_GET_LOGGER_HOOK:
            return print(msg)
        else:
            return CANINE_GET_LOGGER_HOOK().info(msg)

    ## Increased logging level. By default, we want to log our staff with info1.
    ## This is to distinguish our logs from prefect logs so that we can filter them
    ## in an interactive session.
    @staticmethod
    def info1(msg):
        if not CANINE_GET_LOGGER_HOOK:
            return print(msg)
        else:
            return CANINE_GET_LOGGER_HOOK().log(logging.INFO + 1, msg)

    @staticmethod
    def info2(msg):
        if not CANINE_GET_LOGGER_HOOK:
            return print(msg)
        else:
            return CANINE_GET_LOGGER_HOOK().log(logging.INFO + 2, msg)

    @staticmethod
    def warning(msg):
        if not CANINE_GET_LOGGER_HOOK:
            return print(msg, file=sys.stderr)
        else:
            return CANINE_GET_LOGGER_HOOK().warning(msg)

    @staticmethod
    def debug(msg):
        if not CANINE_GET_LOGGER_HOOK:
            return print(msg)
        else:
            return CANINE_GET_LOGGER_HOOK().debug(msg)

    @staticmethod
    def error(msg):
        if not CANINE_GET_LOGGER_HOOK:
            return print(msg, file=sys.stderr)
        else:
            return CANINE_GET_LOGGER_HOOK().error(msg)
    
    @staticmethod
    def print(*args, **kwargs):
        "print-like logging function"
        ## kwargs will be passed to print, but won't be used if logging hook is enabled
        if not CANINE_GET_LOGGER_HOOK:
            return print(*args, **kwargs)
        args = [str(x) for x in args]
        msg = " ".join(args)
        return CANINE_GET_LOGGER_HOOK().log(logging.INFO+1, msg) # info1
    
# Redirect warnings to logging
logging.captureWarnings(True)
