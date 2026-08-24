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

CLUSTER_CONFIG_PREFIX = "_cluster_conf"

def cluster_config_prefix(namespace: typing.Optional[str] = None) -> str:
    """
    Object prefix holding one cluster's mirrored config.

    Namespaced per controller VM, and that is load-bearing rather than tidy.
    The bucket is canine-<project>-<workflow_name>, and wolF never passes
    workflow_name, so *every* cluster in a project shares
    canine-<project>-default. Workers now prefer the mirrored copy over the NFS
    one (cluster_conf_paths.sh), so an unnamespaced prefix would let one
    controller's slurm.conf -- with its own ControlMachine and NodeName list --
    be picked up by another controller's workers, which then register nowhere.

    The controller hostname is the same discriminator used for the credential
    secret name; see user_credentials_secret_name.
    """
    if not namespace:
        return CLUSTER_CONFIG_PREFIX
    return "{}/{}".format(CLUSTER_CONFIG_PREFIX, _sanitize_secret_component(namespace))

def upload_cluster_config(
    bucket: str, local_dir: str = "/mnt/nfs/clust_conf",
    namespace: typing.Optional[str] = None
) -> bool:
    """
    Mirror the cluster's generated Slurm/canine configuration
    (slurm.conf, slurmdbd.conf, cgroup.conf, nodetypes.json, host_LuT.pickle,
    backend_conf.pickle) into gs://<bucket>/_cluster_conf/.

    This is the upload half of moving cluster config off the shared NFS mount
    (NFS-FUSE-IMPLEMENTATION-PLAN.md phase 2). It is deliberately *additive*:
    the config is still written to, and still read from, `local_dir` exactly as
    before. Nothing consumes the uploaded copy yet -- the reader side is
    retargeted only once it can be validated against a live cluster. That means
    a failure here can never prevent the cluster from booting, so this returns
    False and logs rather than raising.

    Note on ordering: this must run *after* the backend has provisioned
    storage_bucket, which happens after init_slurm() populates `local_dir`.

    Returns True if the upload succeeded.
    """
    if not os.path.isdir(local_dir):
        canine_logging.info1(
            "Cluster config directory {} does not exist; skipping config upload".format(local_dir)
        )
        return False

    dest = "gs://{}/{}".format(bucket, cluster_config_prefix(namespace))
    # rsync, not `cp -r`: `gcloud storage cp -r <dir>/. <dest>/` nests the
    # source directory's own basename under <dest> (verified -- it produced
    # <dest>/<tmpdirname>/slurm/slurm.conf), whereas rsync mirrors the
    # directory's *contents*, which is what the fetch side expects. rsync is
    # also idempotent across the repeated cluster startups this sees.
    #
    # slurmdbd.conf is excluded deliberately. It is written 0600 owned by the
    # `slurm` user (provision_server.py:171) while canine runs as the invoking
    # user, so reading it here fails with EACCES -- observed on a live cluster,
    # where it was the one file of six that failed to mirror.
    #
    # Excluding it is correct rather than a workaround: slurmdbd runs only on
    # the controller (started at provision_server.py:200; the worker entrypoint
    # starts slurmd only), the file is regenerated locally on every cluster
    # start, and no worker ever reads it. Its restrictive permissions are
    # precisely the signal that it is not worker-distributable.
    cmd = 'gcloud storage rsync -r --exclude {} {} {}'.format(
        shlex.quote(r'slurm/slurmdbd\.conf$'), shlex.quote(local_dir), shlex.quote(dest)
    )
    proc = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode != 0:
        canine_logging.warning(
            "Could not upload cluster config to {} (continuing; the NFS copy is still "
            "authoritative): {}".format(dest, proc.stderr.decode().strip())
        )
        return False
    canine_logging.info1("Mirrored cluster config to {}".format(dest))
    return True

#
# User credential distribution via Secret Manager
# (NFS-FUSE-IMPLEMENTATION-PLAN.md section 2.6)
#
# Workers must act as the *user*, not a service account -- pipelines pull from
# external sources whose data cannot be shared with an SA -- so credentials have
# to be distributed somehow. Today that is a copy on the NFS share; these
# helpers replace it with a Secret Manager secret so it survives the mount going
# away.
#
# gcloud CLI rather than google-cloud-secret-manager, to avoid adding a
# dependency; the CLI is already a hard requirement.

# Rolling dead-man's switch, NOT a lifetime cap: the controller pushes
# expire_time forward on a timer, so a workflow of any length keeps its
# credentials. If renewal stops (cluster died), the secret self-deletes within
# one TTL. Renewal is an admin operation and therefore free, so this is short.
CREDENTIAL_SECRET_TTL_SECONDS = 3600

# The whole gcloud config dir except logs/, mirroring what
# fetch_credentials_from_nfs (docker_copy_gcloud_credentials.sh) copies, which
# excludes exactly the same one entry.
#
# An earlier version allowlisted four files instead. That looked tidier but
# dropped active_config, access_tokens.db and legacy_credentials/, and a worker
# booted from it did NOT authenticate as the user: gcloud fell back to the VM's
# default compute service account, which has no access to the workflow bucket,
# and the job then died in localization trying to create the missing
# access_tokens.db inside the nested task container.
#
# logs/ is the only entry worth excluding -- ~20MB, and useless on a worker.
# What remains compresses to ~5KB, well inside Secret Manager's 64KiB payload
# cap.
CREDENTIAL_EXCLUDE = {"logs"}

# At least one of these must be present for the config dir to be worth
# publishing at all.
CREDENTIAL_REQUIRED_ANY = ("credentials.db", "application_default_credentials.json")

def _sanitize_secret_component(s: str) -> str:
    """
    Secret IDs must match [a-zA-Z0-9_-]{1,255}. Note this differs from bucket
    names -- uppercase and underscore are legal here -- so do NOT reuse
    _sanitize_bucket_name_component, which would needlessly lowercase.
    """
    s = re.sub(r'[^a-zA-Z0-9_-]+', '-', s).strip('-')
    return s if s else "default"

def user_credentials_secret_name(vm_name: str, unix_user: str, workspace_name: str) -> str:
    """
    canine-adc-<vm>-<user>-<workspace>.

    workspace rather than cluster_name: wolF hardcodes cluster_name to "wolf"
    (wolf/workflow.py), so it never varies and cannot discriminate. The <vm>
    component is what separates users on different controller VMs -- multiple
    users on ONE VM is not a supported configuration and is refused by
    dockerTransient (see _assert_container_owned_by_current_user).
    """
    return "canine-adc-{}-{}-{}".format(
        _sanitize_secret_component(vm_name),
        _sanitize_secret_component(unix_user),
        _sanitize_secret_component(workspace_name),
    )[:255]

def _gcloud(args: typing.List[str]) -> subprocess.CompletedProcess:
    return subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def put_user_credentials_secret(
    project: str,
    secret_name: str,
    gcloud_config_dir: typing.Optional[str] = None,
    ttl_seconds: int = CREDENTIAL_SECRET_TTL_SECONDS,
    accessor_service_account: typing.Optional[str] = None,
) -> bool:
    """
    Publish the user's gcloud credentials as a new version of `secret_name`,
    create the secret with a TTL if absent, destroy the previous version, and
    optionally grant a service account read access.

    Best-effort: returns False and logs rather than raising, so a failure here
    degrades to the NFS credential copy rather than blocking cluster startup.
    """
    if gcloud_config_dir is None:
        gcloud_config_dir = os.path.expanduser("~/.config/gcloud")
    if not os.path.isdir(gcloud_config_dir):
        canine_logging.warning(
            "No gcloud config at {}; skipping credential secret".format(gcloud_config_dir)
        )
        return False

    present = sorted(f for f in os.listdir(gcloud_config_dir) if f not in CREDENTIAL_EXCLUDE)
    if not any(f in present for f in CREDENTIAL_REQUIRED_ANY):
        canine_logging.warning(
            "No credential files found in {}; skipping credential secret".format(gcloud_config_dir)
        )
        return False

    import tempfile, tarfile
    try:
        with tempfile.NamedTemporaryFile(suffix=".tgz") as tf:
            with tarfile.open(tf.name, "w:gz") as tar:
                for f in present:
                    tar.add(os.path.join(gcloud_config_dir, f), arcname=f)
            size = os.path.getsize(tf.name)
            if size > 60 * 1024:
                canine_logging.warning(
                    "Credential payload is {} bytes, close to Secret Manager's 64KiB "
                    "cap; skipping".format(size)
                )
                return False

            exists = _gcloud(["gcloud", "secrets", "describe", secret_name,
                              "--project", project]).returncode == 0
            if not exists:
                # --quiet is required, not cosmetic: `secrets create --ttl`
                # prompts "This secret and all of its versions will be
                # automatically deleted ... continue (Y/n)?" and hard-fails in a
                # non-interactive session.
                p = _gcloud(["gcloud", "secrets", "create", secret_name,
                             "--project", project, "--replication-policy", "automatic",
                             "--ttl", "{}s".format(ttl_seconds), "--quiet"])
                if p.returncode != 0:
                    err = p.stderr.decode()
                    # "already exists" here means the describe above raced with
                    # eventual consistency -- observed immediately after a
                    # create, where describe briefly 404s on a secret that does
                    # exist. Treat it as success and carry on to add a version,
                    # rather than aborting a publish that can actually proceed.
                    if "already exists" in err:
                        refresh_credentials_secret_expiry(project, secret_name, ttl_seconds)
                    else:
                        canine_logging.warning(
                            "Could not create credential secret {}; workers will fall back "
                            "to the NFS copy: {}".format(secret_name, err.strip())
                        )
                        return False
            else:
                refresh_credentials_secret_expiry(project, secret_name, ttl_seconds)

            # Note the version numbers before adding, so we can destroy the old
            # one: Secret Manager bills per *enabled* version per month, so a
            # cluster refreshing per submission would otherwise accumulate them.
            before = _gcloud(["gcloud", "secrets", "versions", "list", secret_name,
                              "--project", project, "--filter", "state=enabled",
                              "--format", "value(name)"])
            prior = [v for v in before.stdout.decode().split() if v.strip()] \
                    if before.returncode == 0 else []

            p = _gcloud(["gcloud", "secrets", "versions", "add", secret_name,
                         "--project", project, "--data-file", tf.name])
            if p.returncode != 0:
                canine_logging.warning(
                    "Could not add credential secret version; workers will fall back to "
                    "the NFS copy: {}".format(p.stderr.decode().strip())
                )
                return False

            for v in prior:
                _gcloud(["gcloud", "secrets", "versions", "destroy", v,
                         "--secret", secret_name, "--project", project, "--quiet"])
    except Exception as e:
        canine_logging.warning("Could not publish credential secret: {}".format(e))
        return False

    if accessor_service_account:
        grant_secret_accessor(project, secret_name, accessor_service_account)

    canine_logging.info1("Published user credentials to secret {}".format(secret_name))
    return True

def grant_secret_accessor(project: str, secret_name: str, service_account: str) -> bool:
    """
    Let the worker service account read the secret. Idempotent.

    NOTE: this grants the *project default compute SA* in the normal case, which
    every worker shares -- so per-user secret names prevent accidental mixing but
    not deliberate access. Real isolation would need per-user service accounts.
    """
    p = _gcloud(["gcloud", "secrets", "add-iam-policy-binding", secret_name,
                 "--project", project,
                 "--member", "serviceAccount:{}".format(service_account),
                 "--role", "roles/secretmanager.secretAccessor",
                 "--quiet"])
    if p.returncode != 0:
        canine_logging.warning(
            "Could not grant {} access to secret {}; workers will fall back to the NFS "
            "copy: {}".format(service_account, secret_name, p.stderr.decode().strip())
        )
        return False
    return True

def refresh_credentials_secret_expiry(
    project: str, secret_name: str, ttl_seconds: int = CREDENTIAL_SECRET_TTL_SECONDS
) -> bool:
    """
    Push the secret's expiry forward -- the keepalive half of the rolling TTL.

    Must be driven by something that lives as long as the CLUSTER, not as long
    as the wolF driver: wolF sets shutdown_on_exit=False, so the controller
    container deliberately outlives the driver and Slurm keeps creating workers
    after it exits. A driver-hosted keepalive would let the secret expire under
    a live cluster.
    """
    # --quiet for the same reason as create: setting a TTL prompts for
    # confirmation and would otherwise hang/fail non-interactively.
    p = _gcloud(["gcloud", "secrets", "update", secret_name, "--project", project,
                 "--ttl", "{}s".format(ttl_seconds), "--quiet"])
    return p.returncode == 0

def delete_user_credentials_secret(project: str, secret_name: str) -> bool:
    """
    Remove the secret at cluster teardown. The TTL is the backstop for the case
    where teardown never runs (SIGKILL, dead VM), which is the common case since
    wolF sets shutdown_on_exit=False.
    """
    p = _gcloud(["gcloud", "secrets", "delete", secret_name,
                 "--project", project, "--quiet"])
    return p.returncode == 0

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
