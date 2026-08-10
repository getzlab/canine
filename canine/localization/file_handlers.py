import abc
import google.cloud.storage
import google.auth
import glob, google_crc32c, json, hashlib, base64, binascii, os, re, requests, shlex, subprocess, threading
import pandas as pd
import urllib.parse

from google.auth.transport.requests import AuthorizedSession
from ..utils import sha1_base32, canine_logging

# Checksum algorithms we can verify on a compute node, mapped to how we verify them.
# The coreutils tools are guaranteed present; google_crc32c is in the worker image
# (and is already a canine dependency), and python3 is relied on unconditionally by
# other emitted commands, so a crc32c gate is safe to emit.
_CHECKSUM_DIGEST_BYTES = {"md5": 16, "sha1": 20, "sha256": 32, "crc32c": 4}
_CHECKSUM_COREUTILS = {"md5": "md5sum", "sha1": "sha1sum", "sha256": "sha256sum"}

# Preference order when a server advertises more than one. md5 first because it is
# what the other handlers already gate on; crc32c last because it needs python3
# rather than coreutils — but it is not optional, since a GCS composite object
# advertises *only* crc32c.
_CHECKSUM_PREFERENCE = ("md5", "sha256", "sha1", "crc32c")


def parse_header_block(raw):
    """
    Parse the headers out of `curl -I` output.

    With `-L`, curl emits one header block per response in the redirect chain,
    separated by blank lines. Only the final block describes the object that will
    actually be downloaded, so intermediate 301/302 blocks must be discarded —
    grepping the whole output can otherwise pick up a redirect's Content-Length.

    Returns a dict with lowercased header names. Repeated headers are collected into a
    comma-joined value, which is how x-goog-hash arrives when a server emits it twice.
    """
    blocks = [b for b in re.split(r"\r?\n\r?\n", raw.strip()) if b.strip()]
    headers = {}
    for line in (blocks[-1].splitlines() if blocks else []):
        if ":" not in line:
            continue  # the "HTTP/1.1 200 OK" status line
        k, v = line.split(":", 1)
        k = k.strip().lower()
        v = v.strip()
        headers[k] = "{},{}".format(headers[k], v) if k in headers else v
    return headers


def _b64_to_hex(value, algorithm):
    """
    Convert a base64 digest to lowercase hex, or return None if it is not a
    well-formed digest of the expected length for `algorithm`.
    """
    try:
        raw = base64.b64decode(value, validate = True)
    except (binascii.Error, ValueError):
        return None
    if len(raw) != _CHECKSUM_DIGEST_BYTES[algorithm]:
        return None
    return binascii.hexlify(raw).decode()


def extract_content_checksum(headers):
    """
    Find a content checksum in HTTP response headers that can be verified against the
    downloaded file. Returns `(algorithm, hex_digest)`, or `(None, None)`.

    Recognised sources, since no single one is universal:
      * `x-goog-hash: crc32c=...,md5=...`  — GCS, including signed URLs
      * `content-md5: <base64>`            — RFC 9110
      * `digest` / `repr-digest`           — RFC 3230 / RFC 9530
      * `x-amz-checksum-<algo>: <base64>`  — S3
      * `etag: "<32 hex>"`                 — only when it is a bare hex md5

    A digest is deliberately *not* returned when the response is content-encoded: the
    digest then covers the encoded bytes while the file lands decoded, so gating on it
    would fail on every correct download. This is the gzip decompressive-transcoding
    case, where checksums cannot validate the localized file at all.
    """
    encoding = headers.get("content-encoding", "").strip().lower()
    if encoding and encoding != "identity":
        return None, None

    candidates = {}

    # GCS: x-goog-hash: crc32c=AAAAAA==,md5=1B2M2Y8AsgTpgAmY7PhCfg==
    for part in headers.get("x-goog-hash", "").split(","):
        if "=" not in part:
            continue
        algo, _, value = part.strip().partition("=")
        algo = algo.strip().lower()
        if algo in _CHECKSUM_DIGEST_BYTES:
            hexed = _b64_to_hex(value.strip(), algo)
            if hexed is not None:
                candidates.setdefault(algo, hexed)

    if "content-md5" in headers:
        hexed = _b64_to_hex(headers["content-md5"], "md5")
        if hexed is not None:
            candidates.setdefault("md5", hexed)

    # RFC 3230 `digest: md5=<b64>`; RFC 9530 `repr-digest: sha-256=:<b64>:`
    for header in ("repr-digest", "digest"):
        for part in headers.get(header, "").split(","):
            if "=" not in part:
                continue
            algo, _, value = part.strip().partition("=")
            algo = algo.strip().lower().replace("-", "")
            value = value.strip().strip(":")
            if algo in _CHECKSUM_DIGEST_BYTES:
                hexed = _b64_to_hex(value, algo)
                if hexed is not None:
                    candidates.setdefault(algo, hexed)

    for algo in _CHECKSUM_DIGEST_BYTES:
        header = "x-amz-checksum-{}".format(algo)
        if header in headers:
            hexed = _b64_to_hex(headers[header], algo)
            if hexed is not None:
                candidates.setdefault(algo, hexed)

    # ETag is only an md5 when it is a bare 32-hex string. A "-N" suffix means a
    # multipart/composite object, where the ETag is an md5-of-md5s and does not match
    # the file's own md5; weak validators and opaque tags are not digests at all.
    etag = headers.get("etag", "").strip()
    if not etag.startswith("W/"):
        etag = etag.strip('"')
        if re.fullmatch(r"[0-9a-fA-F]{32}", etag):
            candidates.setdefault("md5", etag.lower())

    for algo in _CHECKSUM_PREFERENCE:
        if algo in candidates:
            return algo, candidates[algo]
    return None, None


# Default simultaneous ranged GETs. A single TCP stream to S3/GDC realistically gets
# 50-200 MB/s against an n1-standard-8's ~2 GB/s egress cap, and LocalizeToDisk owns the
# whole node exclusively (cpus-per-task=8, --exclusive), so the budget can be claimed
# unconditionally: no runtime negotiation, no per-node semaphore.
DEFAULT_DOWNLOAD_CONNECTIONS = 8
DEFAULT_DOWNLOAD_MIN_CHUNK = 64 * 1024 * 1024

PDL_SCRIPT_NAME = "parallel_download.py"
PDL_EOF_SENTINEL = "# k9pdl-eof"


def _pdl_installed_path():
    """
    Absolute path of the copy that ships inside the installed package.

    Interpolated host-side, which is what makes this work in the controller context
    where CANINE_ROOT may be unset or point at a staging directory that has not been
    populated yet.
    """
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), PDL_SCRIPT_NAME)


def _pdl_path(legacy_cmd=None):
    """
    Emit shell that resolves a *usable* parallel_download.py into $K9_PDL, and the way
    to invoke it into $K9_PDL_RUN.

    Three things this has to get right.

    Resolution is by first usable path, not by defaulting on CANINE_ROOT. The same
    command string also runs on the controller for deduplicated common inputs, where
    CANINE_ROOT may be unset or may point at a staging directory that has not been
    populated yet -- `pick_common_inputs` runs before the staging copy.

    The installed package copy is tried FIRST, so the compute node normally does not
    depend on the shared mount at all. That matters if CANINE_ROOT ever becomes a
    gcsfuse mount: its stat cache can serve a stale negative or stale-size entry for the
    length of the TTL, so a freshly staged script may briefly look missing or truncated.

    "Usable" means present *and* complete, checked via the trailing sentinel. A
    partially-visible or truncated copy is skipped rather than executed, which is the
    whole reason the sentinel exists.

    Invocation prefers direct execution and falls back to `python3 <path>`, because the
    exec bit cannot be relied on: gcsfuse cannot represent one, a mount may be noexec,
    and chmod can fail under NFS root_squash. Correctness never depends on the bit.
    """
    candidates = '{} "${{CANINE_ROOT:-}}/{}"'.format(
        shlex.quote(_pdl_installed_path()), PDL_SCRIPT_NAME
    )
    lines = [
        'K9_PDL=; for p in ' + candidates + '; do [ -f "$p" ] && tail -n1 "$p" | '
        "grep -q '^" + PDL_EOF_SENTINEL + "$' && { K9_PDL=\"$p\"; break; } || :; done",
        '[ -n "$K9_PDL" ] && { [ -x "$K9_PDL" ] && K9_PDL_RUN="$K9_PDL" || '
        'K9_PDL_RUN="python3 $K9_PDL"; } || :',
    ]
    return lines


def _pdl_command(
    url,
    dest,
    size,
    headers=(),
    md5=None,
    etag=None,
    part_length=None,
    s3=None,
    url_refresh_cmd=None,
    legacy_cmd=None,
    connections=DEFAULT_DOWNLOAD_CONNECTIONS,
    min_chunk=DEFAULT_DOWNLOAD_MIN_CHUNK,
    work_dirs=(),
):
    """
    Emit the lines that download one object with the parallel downloader, falling back
    to `legacy_cmd` when the downloader cannot be found or declines the object.

    `legacy_cmd` is load-bearing rather than defensive: it is the existing, unmodified
    single-stream command, so every degraded path is byte-for-byte today's behavior.
    It is used when no usable script resolves, and passed through with --legacy-cmd so
    the downloader can also use it in-script when the server turns out not to honor
    ranges.

    Deliberately no bash-level `.k9pdl.done` short-circuit. The design note calls for
    one, but the marker is JSON carrying the size it attests to, so checking it properly
    in shell means invoking python3 -- exactly the startup cost the check was meant to
    avoid. The downloader performs the check itself as its first action, so the marker
    remains the single source of truth for completion rather than being reimplemented in
    a second language where the two could drift.

    No heredocs, and no line may contain the literal #DEBUG_OMIT: debug.sh regenerates a
    runnable script by filtering that marker out, so an emitted line carrying it would
    be silently dropped.

    `dest` is interpolated VERBATIM and must already be shell-safe. Every handler passes
    `self.localized_path`, which is built by quoting the directory and the basename
    separately and then joining them -- so it already carries quote characters, and every
    other emitted line (the mkdir guard, the curl target, the md5 gate) interpolates it
    raw too. Quoting it again here yields a doubly-quoted path that resolves to the wrong
    file.
    """
    arguments = ["--dest", str(dest), "--size", str(int(size))]
    if url:
        arguments += ["--url", shlex.quote(str(url))]
    arguments += ["--connections", str(int(connections))]
    arguments += ["--min-chunk", str(int(min_chunk))]

    for header in headers or ():
        arguments += ["--header", shlex.quote(str(header))]

    if s3:
        arguments += ["--s3-bucket", shlex.quote(str(s3["bucket"]))]
        arguments += ["--s3-key", shlex.quote(str(s3["key"]))]
        if s3.get("extra_args"):
            arguments += ["--s3-extra-args", shlex.quote(str(s3["extra_args"]))]

    # An ETag is only verifiable as an md5-of-md5s when the part length is known; without
    # it there is nothing to compare against, so it is not passed at all.
    if etag and part_length:
        arguments += ["--check-etag", shlex.quote(str(etag))]
        arguments += ["--part-length", str(int(part_length))]
    elif md5:
        arguments += ["--check-md5", shlex.quote(str(md5))]

    if url_refresh_cmd:
        arguments += ["--url-refresh-cmd", shlex.quote(str(url_refresh_cmd))]
    for work_dir in work_dirs or ():
        arguments += ["--work-dir", shlex.quote(str(work_dir))]
    if legacy_cmd:
        arguments += ["--legacy-cmd", shlex.quote(str(legacy_cmd))]

    invocation = "$K9_PDL_RUN " + " ".join(arguments)

    lines = _pdl_path(legacy_cmd)
    if legacy_cmd:
        lines.append(
            'if [ -n "$K9_PDL" ]; then ' + invocation + "; else "
            "echo 'parallel_download.py not found; using the single-stream path' >&2; "
            + legacy_cmd + "; fi"
        )
    else:
        lines.append(invocation)
    return lines


class FileType(abc.ABC):
    """
    Stores properties of and instructions for handling a given file type:
    * localization command
    * size
    * hash
    """

    localization_mode = None # to be overridden in child classes

    def __init__(self, path, transport = None, **kwargs):
        """
        path: path/URL to file
        transport: Canine transport object for handling local/remote files (currently not used)
        localization_mode: how this file will be handled in localization.job_setup_teardown
          must be one of:
          * url: path is a remote URL that must be handled with a special
                 download command
          * stream: stream remote URL into a FIFO, rather than downloading
          * ro_disk: path is a URL to mount a persistent disk read-only
          * local: path is a local file
          * string: path is a string literal
          - None: path is a string literal (for backwards compatibility)
        """
        self.path = path
        self.localized_path = path # path where file got localized to. needs to be manually updated
        self.transport = transport # currently not used
        self.extra_args = kwargs

        # Resolved once here rather than per-handler: the flag has to be consulted
        # from a single place so that route-dependent verification (which method is
        # sound for a given destination) has exactly one decision point.
        self.check_hash = self._resolve_check_hash(kwargs)

        self._size = None
        self._hash = None

    @staticmethod
    def _resolve_check_hash(kwargs):
        """
        Resolve the integrity-check flag from either spelling.

        `check_hash` is the preferred name. The check is not necessarily an md5: a
        multipart S3 object is verified against its md5-of-md5s ETag, and a composite
        object on a bucket destination has no md5 at all. So the flag means "verify
        integrity by the best method available", not "compute an md5".

        `check_md5` is the original name and is used extensively by downstream wolF
        pipelines, so it must keep working indefinitely. It is a silent alias — no
        DeprecationWarning, since every existing caller uses it and warning would put
        noise on essentially every localization for no benefit.

        Supplying both spellings with conflicting values is a caller bug rather than
        something to guess at: silently picking one could disable integrity checking
        without anyone noticing.
        """
        has_hash = "check_hash" in kwargs
        has_md5 = "check_md5" in kwargs

        if has_hash and has_md5 and bool(kwargs["check_hash"]) != bool(kwargs["check_md5"]):
            raise ValueError(
                "Conflicting integrity check flags: check_hash = {!r} but check_md5 = {!r}. "
                "`check_md5` is a deprecated alias for `check_hash`; pass only one of "
                "them, or give both the same value.".format(
                    kwargs["check_hash"], kwargs["check_md5"]
                )
            )

        if has_hash:
            return bool(kwargs["check_hash"])
        if has_md5:
            return bool(kwargs["check_md5"])
        return False

    @property
    def check_hash(self):
        """
        Whether this file's integrity must be verified after localization.
        """
        return self._check_hash

    @check_hash.setter
    def check_hash(self, value):
        self._check_hash = bool(value)

        # Keep extra_args carrying both spellings with the resolved value. extra_args
        # is passed around as a plain dict (e.g. wolF's get_file_handler(**extra_args))
        # and call sites may index either name, so leaving them inconsistent would let
        # two call sites disagree about whether to verify.
        extra_args = getattr(self, "extra_args", None)
        if extra_args is not None:
            extra_args["check_hash"] = self._check_hash
            extra_args["check_md5"] = self._check_hash

    @property
    def check_md5(self):
        """
        Deprecated alias for `check_hash`; see `_resolve_check_hash`. Readable and
        assignable so that any subclass, test or downstream consumer touching
        `handler.check_md5` behaves exactly as it did before.
        """
        return self.check_hash

    @check_md5.setter
    def check_md5(self, value):
        self.check_hash = value

    def _probe_http_metadata(self, curl_args = ""):
        """
        Fetch response headers for `self.url` once and record both the size and any
        verifiable content checksum.

        Sets `self._size` and `self.content_checksum` — a `(algorithm, hex_digest)`
        tuple, `(None, None)` when the server advertises nothing usable. Raises
        ValueError if no Content-Length is available, matching prior behaviour.

        Deliberately separate from `hash`/`_get_hash()`: those identify the *input*
        (and for URL handlers are derived from the URL, precisely because the server
        cannot be trusted to name the content), whereas this is a digest of the bytes
        used to verify a completed download.
        """
        resp = subprocess.run(
          "curl -sIL {args} {url}".format(args = curl_args, url = shlex.quote(self.url)),
          shell = True, capture_output = True
        )
        headers = parse_header_block(resp.stdout.decode(errors = "replace"))

        if "content-length" not in headers:
            raise ValueError("Could not get file header size")
        try:
            self._size = int(headers["content-length"])
        except ValueError:
            raise ValueError("Could not get file header size")

        self.content_checksum = extract_content_checksum(headers)

        if self.check_hash and self.content_checksum[0] is None:
            canine_logging.warning(
              "check_hash was requested for {}, but the server advertises no usable "
              "content checksum, so the download cannot be verified. Recognised "
              "headers are x-goog-hash, Content-MD5, Digest/Repr-Digest, "
              "x-amz-checksum-*, and a bare hex ETag.".format(self.url)
            )

        return headers

    def _hash_check_command(self):
        """
        Emit the shell gate that verifies a localized file against the checksum the
        server advertised. Returns [] when there is nothing to verify.

        Same shape as the md5 gates the GDC/DRS handlers already emit: compare, and on
        mismatch delete the corrupt file and exit 1.
        """
        if not self.check_hash:
            return []

        algorithm, digest = getattr(self, "content_checksum", (None, None))
        if algorithm is None:
            return []

        fail = "{{ echo 'deleting corrupted file' ; rm -f {path} ; exit 1 ; }}".format(
          path = self.localized_path
        )

        if algorithm in _CHECKSUM_COREUTILS:
            return ["[[ $({prog} {path} | sed -r 's/  .*$//') == {digest} ]] || {fail}".format(
              prog = _CHECKSUM_COREUTILS[algorithm],
              path = self.localized_path,
              digest = digest,
              fail = fail,
            )]

        # crc32c has no coreutils equivalent; google_crc32c is in the worker image
        return ['python3 -c "import sys,google_crc32c;h=google_crc32c.Checksum();'
                "f=open(sys.argv[1],'rb');"
                "[h.update(c) for c in iter(lambda: f.read(1048576), b'')];"
                'sys.exit(0 if h.hexdigest().decode() == sys.argv[2] else 1)" '
                '{path} {digest} || {fail}'.format(
                  path = self.localized_path, digest = digest, fail = fail
                )]

    @property
    def size(self):
        """
        Returns size of this file in bytes
        """
        if self._size is None:
            self._size = self._get_size()
        return self._size

    def _get_size(self):
        pass

    @property
    def hash(self):
        """
        Returns a hash for this file
        """
        if self._hash is None:
            self._hash = self._get_hash()
        return self._hash

    def _get_hash(self):
        """
        Base class assume self.path is a string literal
        """
        return sha1_base32(bytearray(self.path, "utf-8"), 4)

    def localization_command(self, dest):
        """
        Returns a command to localize this file
        """
        pass

    def __str__(self):
        """
        Some functions (e.g. orchestrator.make_output_DF) may be passed FileType
        objects, but expect strings corresponding to the file path.
        """
        return self.path

    def __repr__(self):
        return "<{cl}: {path}>".format(
          cl = self.__class__.__name__,
          path = self.path
        )

class StringLiteral(FileType):
    """
    Since the base FileType class also works for string literals, alias
    the StringLiteral class for clarification
    """
    localization_mode = "string"

def hash_set(x):
    assert isinstance(x, set)
    x = list(sorted(x))
    return hashlib.md5(json.dumps(x).encode()).hexdigest()

#
# define file type handlers

## Google Cloud Storage {{{

STORAGE_CLIENT = None
storage_client_creation_lock = threading.Lock()

def gcloud_storage_client():
    global STORAGE_CLIENT
    with storage_client_creation_lock:
        if STORAGE_CLIENT is None:
            # this is the expensive operation
            STORAGE_CLIENT = google.cloud.storage.Client()
    return STORAGE_CLIENT

class GSFileNotExists(Exception):
    pass

class HandleGSURL(FileType):
    localization_mode = "url"

    def get_requester_pays(self) -> bool:
        """
        Returns True if the requested gs:// object or bucket resides in a
        requester pays bucket
        """
        # Try GCS API first (fastest)
        try:
            bucket = re.match(r"gs://(.*?)/.*", self.path)[1]
            gcs_cl = gcloud_storage_client()
            bucket_obj = google.cloud.storage.Bucket(gcs_cl, bucket, user_project = self.extra_args.get("project"))
            bucket_obj.reload() 
            return bucket_obj.requester_pays
        except Exception as e:
            # Fallback to gcloud storage approach when GCS API fails (e.g., 403 permissions)
            canine_logging.info1(f"GCS API failed for bucket {bucket}, falling back to gcloud storage: {e}")

            # Extract bucket and path like base class does
            if self.path.startswith('gs://'):
                path = self.path[5:]
            else:
                path = self.path
            bucket = path.split('/')[0]

            # Try gcloud storage buckets describe command
            command = 'gcloud storage buckets describe gs://{} --format="value(requester_pays)"'.format(bucket)
            ret = subprocess.run(command, shell = True, capture_output = True)
            text = ret.stderr

            if ret.returncode == 0 or b'404' not in text:
                # Check both stderr (for error messages) and stdout (for success messages)
                return (
                    b'requester pays bucket but no user project provided' in text
                    or ret.stdout.strip() == b'True'
                )
            else:
                # Try again ls-ing the object itself
                # sometimes permissions can disallow bucket inspection
                # but allow object inspection
                command = 'gcloud storage ls gs://{}'.format(path)
                ret = subprocess.run(command, shell = True, capture_output = True)
                text = ret.stderr

                if ret.returncode != 0 and b'404' in text:
                    canine_logging.error(text.decode())
                    raise subprocess.CalledProcessError(ret.returncode, command)

                # Check if this indicates requester pays
                return b'requester pays bucket but no user project provided' in text

    def __init__(self, path, **kwargs):
        super().__init__(path, **kwargs)

        # remove any trailing slashes, in case path refers to a directory
        self.path = path.rstrip("/")

        # check if this bucket is requester pays
        self.rp_string = ""
        if self.get_requester_pays():
            if "project" not in self.extra_args:
                raise ValueError(f"File {self.path} resides in a requester-pays bucket but no user project provided")
            self.rp_string = f' --billing-project={self.extra_args["project"]}'

        # is this URL a directory?
        self.is_dir = False

    def blob(self):
        assert self.path.startswith("gs://")
        res = re.search("^gs://([^/]+)/(.*)$", self.path)
        bucket = res[1]
        obj_name = res[2]

        gcs_cl = gcloud_storage_client()

        bucket_obj = google.cloud.storage.Bucket(gcs_cl, bucket, user_project = self.extra_args.get("project"))

        # check whether this path exists, and whether it's a directory
        
        # check whether object exists
        blob_obj = google.cloud.storage.Blob(bucket=bucket_obj, name=obj_name)
        exists = blob_obj.exists(gcs_cl)

        if exists:
            # Need to do this so we have the hash attribute later
            blob_obj.reload()
        else:
            ## If not, try checking to see if it's a directory
        
            # list_blobs is completely ignorant of "/" as a delimiter
            # prefix = "dir/b" will list
            # dir/b (may not even exist as a standalone "directory")
            # dir/b/file1
            # dir/b/file2
            # dir/boy
            for b in gcs_cl.list_blobs(bucket_obj, prefix = obj_name):
                # a blob starting with <obj_name>/ is a directory
                if b.name.startswith(obj_name + "/"):
                    exists = True
                    self.is_dir = True
                    blob_obj = b
                    break

        if not exists:
            raise GSFileNotExists("{} does not exist.".format(self.path))

        if self.is_dir:
            return gcs_cl.list_blobs(bucket_obj, prefix = obj_name + "/")
        else:
            return [blob_obj]

    def _get_size(self):
        sz = 0
        for b in self.blob():
            sz += b.size
        return sz

    def _get_hash(self):
        blob = self.blob()

        # if it's a directory, hash the set of CRCs within
        if self.is_dir:
            canine_logging.info1(f"Hashing directory {self.path}. This may take some time.")
            files = set()
            for b in blob:
                files.add(b.crc32c)
            return hash_set(files)

        # for backwards compatibility, if it's a file, return the file directly
        # TODO: for cleaner code, we really should just always return a set and hash it
        else:
            return binascii.hexlify(base64.b64decode(blob[0].crc32c)).decode().lower()

    def localization_command(self, dest):
        dest_dir = shlex.quote(os.path.dirname(dest))
        dest_file = shlex.quote(os.path.basename(dest))
        self.localized_path = os.path.join(dest_dir, dest_file)
        return ("[ ! -d {dest_dir} ] && mkdir -p {dest_dir} || :; ".format(dest_dir = self.localized_path if self.is_dir else dest_dir)) + f'CLOUDSDK_STORAGE_TRACKER_DIR="{dest_dir}/.gcloud_tracker_dir" gcloud storage cp {self.rp_string} -r -n -L "{dest_dir}/.gcloud_manifest" {self.path} {dest_dir}/{dest_file if not self.is_dir else ""}'

class HandleGSURLStream(HandleGSURL):
    localization_mode = "stream"

    def localization_command(self, dest):
        return "\n".join(['gcloud storage ls {} {} > /dev/null'.format(self.rp_string, shlex.quote(self.path)),
        'if [[ -e {0} ]]; then rm {0}; fi'.format(dest),
        'mkfifo {}'.format(dest),
        "gcloud storage cat {} {} > {} &".format(
            self.rp_string,
            shlex.quote(self.path),
            dest
        )])

# }}}

## GCP Authorized Session {{{

GCP_AUTH_SESSION = None
gcp_auth_session_creation_lock = threading.Lock()

def gcp_auth_session():
    global GCP_AUTH_SESSION
    with gcp_auth_session_creation_lock:
        if GCP_AUTH_SESSION is None:
            # this is the expensive operation
            GCP_AUTH_SESSION = AuthorizedSession(
                google.auth.default(['https://www.googleapis.com/auth/userinfo.profile',
                                     'https://www.googleapis.com/auth/userinfo.email'])[0])
    return GCP_AUTH_SESSION

# }}}

## AWS S3 {{{

class HandleAWSURL(FileType):
    localization_mode = "url"

    # TODO: use boto3 API; overhead for calling out to aws shell command might be high
    #       this would also allow us to run on systems that don't have the aws tool installed
    # TODO: support directories

    def __init__(self, path, **kwargs):
        """
        Optional arguments:
        * aws_access_key_id
        * aws_secret_access_key
        * aws_endpoint_url
        """
        super().__init__(path, **kwargs)

        # remove any trailing slashes, in case path refers to a directory
        self.path = path.rstrip("/")

        # keys get passed via environment variable
        self.command_env = {}
        self.command_env["AWS_ACCESS_KEY_ID"] = self.extra_args.get("aws_access_key_id")
        self.command_env["AWS_SECRET_ACCESS_KEY"] = self.extra_args.get("aws_secret_access_key")
        self.command_env_str = " ".join([f"{k}={v}" for k, v in self.command_env.items() if v is not None])

        # compute extra arguments for s3 commands
        # TODO: add requester pays check here
        self.aws_endpoint_url = self.extra_args.get("aws_endpoint_url")

        self.s3_extra_args = []
        if self.command_env["AWS_ACCESS_KEY_ID"] is None and self.command_env["AWS_SECRET_ACCESS_KEY"] is None:
            self.s3_extra_args += ["--no-sign-request" ]
        if self.aws_endpoint_url is not None:
            self.s3_extra_args += [f"--endpoint-url {self.aws_endpoint_url}"]
        self.s3_extra_args_str = " ".join(self.s3_extra_args)

        # get header for object
        try:
            res = re.search("^s3://([^/]+)/(.*)$", self.path)
            bucket = res[1]
            obj = res[2]
        except:
            raise ValueError(f"{self.path} is not a valid s3:// URL!")

        head_resp = subprocess.run(
          "{env} aws s3api {extra_args} head-object --bucket {bucket} --key {obj}".format(
            env = self.command_env_str,
            extra_args = self.s3_extra_args_str,
            bucket = bucket,
            obj = obj
          ),
          shell = True,
          capture_output = True
        )

        if head_resp.returncode == 254:
            if b"(404)" in head_resp.stderr:
                # check if it's truly a 404 or a directory; we do not yet support these
                ls_resp = subprocess.run(
                  "{env} aws s3api {extra_args} list-objects-v2 --bucket {bucket} --prefix {obj} --max-items 2".format(
                    env = self.command_env_str,
                    extra_args = self.s3_extra_args_str,
                    bucket = bucket,
                    obj = obj
                  ),
                  shell = True,
                  capture_output = True
                )
                if len(ls_resp.stdout) == 0:
                    raise ValueError(f"Object {self.path} does not exist in bucket!")

                ls_resp_headers = json.loads(ls_resp.stdout)
                if len(ls_resp_headers["Contents"]) > 1:
                    raise ValueError(f"Object {self.path} is a directory; we do not yet support localizing those from s3.")
            elif b"(403)" in head_resp.stderr:
                raise ValueError(f"You do not have permission to access {self.path}!")
            else:
                raise ValueError(f"Error accessing S3 file:\n{head_resp.stderr.decode()}")
        elif head_resp.returncode != 0:
            raise ValueError(f"Unknown AWS S3 error occurred:\n{head_resp.stderr.decode()}")

        self.headers = json.loads(head_resp.stdout)

        # Check for multiple parts to enable local hash calculation
        if self.headers.get("PartsCount", 1) > 1:
            part1_head_resp = subprocess.run(
              "{env} aws s3api {extra_args} head-object --bucket {bucket} --key {obj} --part-number 1".format(
                env = self.command_env_str,
                extra_args = self.s3_extra_args_str,
                bucket = bucket,
                obj = obj
              ),
              shell = True,
              capture_output = True
            )
            if part1_head_resp.returncode == 254:
                raise ValueError(f"Error accessing S3 file:\n{part1_head_resp.stderr.decode()}")
            if part1_head_resp.returncode != 0:
                raise ValueError(f"Unknown AWS S3 error occurred:\n{part1_head_resp.stderr.decode()}")
            self.headers["PartLength"] = json.loads(part1_head_resp.stdout)["ContentLength"]
    def _get_hash(self):
        return self.headers["ETag"].replace('"', '')

    def _get_size(self):
        return self.headers["ContentLength"]

    def localization_command(self, dest):
        dest_dir = shlex.quote(os.path.dirname(dest))
        dest_file = shlex.quote(os.path.basename(dest))
        self.localized_path = os.path.join(dest_dir, dest_file)

        cmd = [
          f"[ ! -d {dest_dir} ] && mkdir -p {dest_dir} || :",
          f"[ -f {self.localized_path} ] && SZ=$(stat --printf '%s' {self.localized_path}) || SZ=0",
          f"if [ $SZ != {self.size} ]; then",
          '{env} aws s3api {extra_args} get-object --bucket {bucket} --key {file} --range "bytes=$SZ-" >(cat >> {dest}) > /dev/null'.format(
            env = self.command_env_str,
            extra_args = self.s3_extra_args_str,
            bucket = self.path.split("/")[2],
            file = "/".join(self.path.split("/")[3:]),
            dest = self.localized_path
          ),
          "fi"
        ]
        if self.check_hash:
            if "PartLength" in self.headers:
                chunk_size = self.headers["PartLength"]
                chunks = self.headers["PartsCount"]
                cmd += [
                  "md5hash=$(python3 << CODE",
                  "import hashlib, multiprocessing",
                   "def hash_chunk(args):",
                  "    i, cs, f = args",
                  "    fh = open(f, 'rb')",
                  "    fh.seek(i * cs)",
                  "    return hashlib.md5(fh.read(cs)).digest()",
                  f"chunk_size = {chunk_size}",
                  f"chunks = {chunks}",
                  f"fp = '{self.localized_path}'",
                  "pool = multiprocessing.Pool()",
                  "results = pool.map(hash_chunk, [(i, chunk_size, fp) for i in range(chunks)])",
                  "pool.close()",
                  "print(hashlib.md5(b''.join(results)).hexdigest() + '-' + str(chunks))",
                  "CODE",
                  ")"
                ]
            else:
                cmd += [f"md5hash=$(md5sum {self.localized_path} | cut -d ' ' -f 1)"]
            cmd += [f'[[ "$md5hash" == "{self.hash}" ]] || {{ echo "deleting corrupted file" 1>&2 ; rm -f {self.localized_path} ; exit 1 ; }}']

        return "\n".join(cmd)

class HandleAWSURLStream(HandleAWSURL):
    localization_mode = "stream"

    def localization_command(self, dest):
        dest_dir = shlex.quote(os.path.dirname(dest))
        dest_file = shlex.quote(os.path.basename(dest))
        self.localized_path = os.path.join(dest_dir, dest_file)

        return "\n".join([
          # NB: not an f-string. {0} must survive to .format(dest) below — as an
          # f-string it is evaluated first and substitutes the literal 0.
          'if [[ -e {0} ]]; then rm {0}; fi'.format(dest),
          f"[ ! -d {dest_dir} ] && mkdir -p {dest_dir} || :",
          'mkfifo {}'.format(dest),
          "{env} aws s3 {extra_args} cp {url} {path} &".format(
            env = self.command_env_str,
            extra_args = self.s3_extra_args_str,
            url = self.path,
            path = dest
          )
        ])

# }}}

## GDC HTTPS URLs {{{
class HandleGDCHTTPURL(FileType):
    localization_mode = "url"
    gdc_drs_root = "drs://dg.4dfc:"

    def __init__(self, path, **kwargs):
        super().__init__(path, **kwargs)

        self.token = self.extra_args.get("token")
        self.token_flag = f'--header  "X-Auth-Token: {self.token}"' if self.token is not None else ''

        # parse URL
        self.url = self.path
        url_parse = re.match(r"^(https://api\.(?:awg\.)?gdc\.cancer\.gov)/(?:files|data)/([0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12})", self.url)
        if url_parse is None:
            raise ValueError("Invalid GDC ID '{}'".format(self.url))

        self.prefix = url_parse[1]
        self.uuid = url_parse[2]

        try:
            self.uri = type(self).gdc_drs_root + self.uuid
            self.drs_obj = HandleDRSURI(self.uri, **self.extra_args)
        except:
            canine_logging.warning("Re-attempting with GDC API")
            self.drs_obj = None

            # the actual filename is encoded in the content-disposition header;
            # save this to self.path
            # since the filesize and hashes are also encoded in the header, populate
            # these fields now
            resp_headers = subprocess.run(
              'curl -s -D - -o /dev/full {token_flag} {file}'.format(
                token_flag = self.token_flag,
                file = self.path
              ),
              shell = True,
              capture_output = True
            )
            try:
                headers = pd.DataFrame(
                  [x.split(": ") for x in resp_headers.stdout.decode().split("\r\n")[1:]],
                  columns=["header", "value"],
                ).set_index("header")["value"]

                self.path = re.match(".*filename=(.*)$", headers["Content-Disposition"])[1]
                self._size = int(headers["Content-Length"])
                self._hash = headers["Content-MD5"]
            except:
                canine_logging.error("Error resolving GDC file; see details:")
                canine_logging.error(resp_headers.stdout.decode())
                raise
        if self.drs_obj is not None:
            # if we have a DRS object, use its properties
            self.path = self.drs_obj.path
            self._size = self.drs_obj.size
            self._hash = self.drs_obj.hash
            self.url = self.drs_obj.uri
            self.token = None
        self.localized_path = self.path

    def localization_command(self, dest):
        if self.drs_obj is not None:
            return self.drs_obj.localization_command(dest)
        dest_dir = shlex.quote(os.path.dirname(dest))
        dest_file = shlex.quote(os.path.basename(dest))
        self.localized_path = os.path.join(dest_dir, dest_file)
        cmd = []
        if self.token is not None:
            cmd += ["[ ! -d {dest_dir} ] && mkdir -p {dest_dir} || :; curl -C - -o {path} {token} '{url}'".format(dest_dir = dest_dir, path = self.localized_path, token = self.token_flag, url = self.url)]
        else:
            cmd += ["[ ! -d {dest_dir} ] && mkdir -p {dest_dir} || :; curl -C - -o {path} '{url}'".format(dest_dir = dest_dir, path = self.localized_path, url = self.url)]

        # ensure that file downloaded properly
        if self.check_hash:
            cmd += [f"[[ $(md5sum {self.localized_path} | sed -r 's/  .*$//') == {self.hash} ]] || {{ echo 'deleting corrupted file' ; rm -f {self.localized_path} ; exit 1 ; }}"]

        return "\n".join(cmd)

class HandleGDCHTTPURLStream(HandleGDCHTTPURL):
    localization_mode="stream"

    def localization_command(self, dest):
        
        dest_dir = shlex.quote(os.path.dirname(dest))
        dest_file = shlex.quote(os.path.basename(dest))
        self.localized_path = os.path.join(dest_dir, dest_file)
        cmd = []
        
        #clean exisiting file if it exists
        cmd += ['if [[ -e {0} ]]; then rm {0}; fi'.format(dest)]
        
        #create dir if it doesnt exist
        cmd += ["[ ! -d {dest_dir} ] && mkdir -p {dest_dir} || :;".format(dest_dir = dest_dir)]
        
        #create fifo object
        cmd += ['mkfifo {}'.format(dest)]
        
        #stream into fifo object
        if self.token is not None:
            cmd += ["curl -C - -o {path} {token} '{url}' &".format(path = self.localized_path, token = self.token_flag, url = self.url)]
        else:
            cmd += ["curl -C - -o {path} '{url}' &".format(dest_dir = dest_dir, path = self.localized_path, url = self.url)]

        return "\n".join(cmd)

# }}}

class HandleDRSURI(FileType):
    localization_mode = "url"
    drs_resolver = "https://drshub.dsde-prod.broadinstitute.org/api/v4/drs/resolve"

    def __init__(self, path, **kwargs):
        super().__init__(path, **kwargs)

        # parse URL
        self.uri = self.path
        uri_parse = re.match(r"^drs://(?:[A-Za-z0-9._]+/)?[A-Za-z0-9._]+:[A-Za-z0-9.-_~%]+",
                             self.uri)
        if uri_parse is None:
            raise ValueError(f"Invalid DRS URI '{self.uri}'")

        # NB: this is read during __init__, so it relies on super().__init__() above
        # having already resolved the flag. It does more than gate a later md5 check —
        # it decides whether to ask drshub for hashes at all, so it cannot be dropped
        # in favour of only checking the flag at localization time.
        fields = ["size", "fileName", "accessUrl"]
        if self.check_hash:
            fields += ["hashes"]
        data = {"url": self.uri, "fields": fields}

        drshub_session = gcp_auth_session()
        resp = drshub_session.post(type(self).drs_resolver,
                                   headers={"Content-type": "application/json"}, json=data)

        try:
            metadata = resp.json()
            
            # Extract the actual filename from the metadata
            if "fileName" in metadata and metadata["fileName"]:
                provided_filename = metadata["fileName"]
                canine_logging.info1(f"DRShub-provided fileName: {provided_filename}")
                
                # Check if the fileName is just the UUID (common issue with DRShub)
                # Extract UUID from the DRS URI for comparison
                uri_uuid = self.uri.split(':')[-1] if ':' in self.uri else None
                
                if provided_filename == uri_uuid:
                    # DRShub gave us the UUID as filename, try to extract real filename from accessUrl
                    canine_logging.warning(f"DRShub returned UUID as fileName for {self.uri}, attempting to extract real filename from accessUrl")
                    
                    try:
                        if "accessUrl" in metadata and ("url" in metadata["accessUrl"]):
                            signed_url = metadata["accessUrl"]["url"]

                            # Extract filename from the signed URL path
                            # URLs typically look like: https://domain.com/bucket/uuid/actual_filename.ext?params
                            parsed_url = urllib.parse.urlparse(signed_url)
                            url_path = parsed_url.path
                            
                            # Split path and get the last part (should be the real filename)
                            path_parts = url_path.strip('/').split('/')
                            if len(path_parts) >= 2:
                                real_filename = path_parts[-1]  # Last part should be the real filename
                                if real_filename and (real_filename != uri_uuid):
                                    canine_logging.info1(f"Extracted real filename from accessUrl: {real_filename}")
                                    self.path = real_filename
                                else:
                                    canine_logging.warning(f"Could not extract valid filename from accessUrl path: {url_path}")
                                    self.path = provided_filename  # Fall back to UUID
                            else:
                                canine_logging.warning(f"Unexpected accessUrl path format: {url_path}")
                                self.path = provided_filename  # Fall back to UUID
                        else:
                            canine_logging.warning(f"No accessUrl in DRShub response, using provided fileName: {provided_filename}")
                            self.path = provided_filename
                    except Exception as e:
                        canine_logging.warning(f"Error extracting filename from accessUrl: {e}, using provided fileName: {provided_filename}")
                        self.path = provided_filename
                else:
                    # DRShub gave us a proper filename
                    self.path = provided_filename
            else:
                # This should not happen - if DRShub doesn't provide fileName, something is wrong
                canine_logging.error(f"DRShub did not provide fileName for {self.uri}")
                canine_logging.error(f"Available fields: {list(metadata.keys())}")
                raise ValueError(f"DRShub response missing fileName for {self.uri}")
            
            self._size = metadata.get("size")
            self._hash = metadata.get("hashes", {}).get("md5")
            
        except Exception as e:
            try:
                msg = json.dumps(resp.json())
            except:
                msg = resp.text
            canine_logging.error("Error resolving DRS URI; see details:")
            canine_logging.error(f"Response code: {resp.status_code}")
            canine_logging.error(f"Error: {e}")
            canine_logging.error(msg)
            raise
        self.localized_path = self.path

    def localization_command(self, dest):
        dest_dir = shlex.quote(os.path.dirname(dest))
        dest_file = shlex.quote(os.path.basename(dest))
        self.localized_path = os.path.join(dest_dir, dest_file)
        data_str = json.dumps({"url": self.uri, "fields": ["accessUrl"]})
        signed_url = f'$(curl -S -X POST --url "{type(self).drs_resolver}" ' + \
                     '-H "authorization: Bearer $(gcloud auth print-access-token)" ' + \
                     f'-H "content-type: application/json" --data \'{data_str}\' | ' + \
                     'python3 -c \'import json,sys; print(json.load(sys.stdin)["accessUrl"]["url"])\')'
        cmd = [f'signed_url={signed_url}',
               f'[ ! -d {dest_dir} ] && mkdir -p {dest_dir} || :; curl -C - -o {self.localized_path} "$signed_url"']

        # ensure that file downloaded properly
        if self.check_hash:
            cmd += [f"[[ $(md5sum {self.localized_path} | sed -r 's/  .*$//') == {self.hash} ]] || {{ echo 'deleting corrupted file' ; rm -f {self.localized_path} ; exit 1 ; }}"]

        return "\n".join(cmd)

class HandleDRSURIStream(HandleDRSURI):
    localization_mode = "stream"

    def localization_command(self, dest):

        dest_dir = shlex.quote(os.path.dirname(dest))
        dest_file = shlex.quote(os.path.basename(dest))
        self.localized_path = os.path.join(dest_dir, dest_file)
        cmd = []

        # clean existing file if it exists
        cmd += ['if [[ -e {0} ]]; then rm {0}; fi'.format(dest)]

        # create dir if it doesn't exist
        cmd += ["[ ! -d {dest_dir} ] && mkdir -p {dest_dir} || :;".format(dest_dir=dest_dir)]

        # create fifo object
        cmd += ['mkfifo {}'.format(dest)]

        # get signed URL
        data_str = json.dumps({"url": self.uri, "fields": ["accessUrl"]})
        signed_url = f'$(curl -S -X POST --url "{type(self).drs_resolver}" ' + \
                     '-H "authorization: Bearer $(gcloud auth print-access-token)" ' + \
                     f'-H "content-type: application/json" --data \'{data_str}\' | ' + \
                     'python3 -c \'import json,sys; print(json.load(sys.stdin)["accessUrl"]["url"])\')'
        cmd += [f'signed_url={signed_url}']

        # stream into fifo object
        cmd += ['curl -C - -o {path} "$signed_url" &'.format(path=self.localized_path)]

        return "\n".join(cmd)


class HandleGCSSignedURL(FileType):
    localization_mode = "url"

    def __init__(self, path, **kwargs):
        super().__init__(path, **kwargs)
        
        self.url = self.path
        # Extract the object path from signed GCS URLs to preserve the original filename
        # Signed URLs look like: https://storage.googleapis.com/bucket/path/to/file.ext?GoogleAccessId=...
        # or: https://storage.cloud.google.com/bucket/path/to/file.ext?GoogleAccessId=...
        url_parse = re.match(r"https://storage\.(?:googleapis|cloud\.google)\.com/[^/]+/(.+?)(?:\?|$)", self.url)
        if url_parse is None:
            raise ValueError(f"Signed GCS URL format not recognized: {self.url}")
        
        # Keep the full object path to preserve directory structure and original filename
        object_path = url_parse[1]
        self.path = os.path.basename(object_path)  # Use just the filename for localization
        self.localized_path = self.path

        # get file size and any advertised content checksum from server. GCS returns
        # x-goog-hash on signed URLs, so check_hash is verifiable here — a composite
        # object advertises crc32c only, a plain one both crc32c and md5.
        self._probe_http_metadata()

    def _get_hash(self):
        return sha1_base32(bytearray(self.url, "utf-8"), 4)

    def localization_command(self, dest):
        dest_dir = shlex.quote(os.path.dirname(dest))
        dest_file = shlex.quote(os.path.basename(dest))
        self.localized_path = os.path.join(dest_dir, dest_file)
        cmd = []
        cmd += ["[ ! -d {dest_dir} ] && mkdir -p {dest_dir} || :; curl -C - -o {path} '{url}'".format(dest_dir = dest_dir, path = self.localized_path, url = self.url)]

        # ensure that file downloaded properly, if the server gave us a checksum
        cmd += self._hash_check_command()
        return "\n".join(cmd)

class HandleOtherURL(FileType):
    localization_mode = "url"

    def __init__(self, path, **kwargs):
        super().__init__(path, **kwargs)
        
        self.url = self.path
        url_parse = re.match("(?:http|https|ftp)://.*\/(.*)$", self.url)
        if url_parse is None:
            raise ValueError(f"URL {self.url} format not recognized")
        self.path = url_parse[1]
        self.localized_path = self.path

        # get file size and any advertised content checksum from server
        self._probe_http_metadata()

    def _get_hash(self):
        # cannot trust the server to provide a stable identity for this input, so the
        # hashed URL is used instead. Note this is separate from content_checksum,
        # which *is* taken from the server and is only used to verify a download.
        return sha1_base32(bytearray(self.url, "utf-8"), 4)

    def localization_command(self, dest):
        dest_dir = shlex.quote(os.path.dirname(dest))
        dest_file = shlex.quote(os.path.basename(dest))
        self.localized_path = os.path.join(dest_dir, dest_file)
        cmd = []
        cmd += ["[ ! -d {dest_dir} ] && mkdir -p {dest_dir} || :; curl -C - -o {path} '{url}'".format(dest_dir = dest_dir, path = self.localized_path, url = self.url)]

        # ensure that file downloaded properly, if the server gave us a checksum
        cmd += self._hash_check_command()
        return "\n".join(cmd)

## Regular files {{{

class HandleRegularFile(FileType):
    localization_mode = "local"

    def __init__(self, path, **kwargs):
        super().__init__(path, **kwargs)

        # remove any trailing slashes, in case path refers to a directory
        self.path = path.rstrip("/")

    def _get_size(self):
        if os.path.isdir(self.path):
            total_size = 0
            for path, dirs, files in os.walk(self.path):
                for f in files:
                    file_path = os.path.join(path, f)
                    total_size += os.path.getsize(file_path)
            return(total_size)
        else:
            return os.path.getsize(self.path)

    def _get_hash(self):
        # if Canine-generated checksum exists, use it
        k9_crc = os.path.join(os.path.dirname(self.path), "." + os.path.basename(self.path) + ".crc32c")
        if os.path.exists(k9_crc):
            with open(k9_crc, "r") as f:
                return f.read().rstrip()

        # otherwise, compute it
        hash_alg = google_crc32c.Checksum()
        buffer_size = 8 * 1024

        # check if it's a directory
        isdir = False
        if os.path.isdir(self.path):
            files = glob.iglob(self.path + "/**", recursive = True)
            isdir = True
        else:
            files = [self.path]

        for f in files: 
            if os.path.isdir(f):
                continue

            file_size_MiB = int(os.path.getsize(self.path)/1024**2)

            # if we are hashing a whole directory, output a message for each file
            if isdir:
                canine_logging.info1(f"Hashing file {f} ({file_size_MiB} MiB)")

            ct = 0
            with open(f, "rb") as fp:
                while True:
                    # output message every 100 MiB
                    if ct > 0 and not ct % int(100*1024**2/buffer_size):
                        canine_logging.info1(f"Hashing file {self.path}; {int(buffer_size*ct/1024**2)}/{file_size_MiB} MiB completed")

                    data = fp.read(buffer_size)
                    if not data:
                        break
                    hash_alg.update(data)
                    ct += 1

        return hash_alg.hexdigest().decode().lower()

# }}}

## Read-only disks {{{

class HandleRODISKURL(FileType):
    localization_mode = "ro_disk"

    # file size is unnknowable

    # hash is be based on disk hash URL (if present) and/or filename
    # * for single file RODISKS, hash will be disk name
    # * for batch RODISKS, hash will be disk name + filename
    def _get_hash(self):
        roURL = re.match(r"rodisk://([^/]+)/(.*)", self.path)
        if roURL is None or roURL[2] == "":
            raise ValueError("Invalid RODISK URL specified ({})!".format(self.path))

        # we can only compare RODISK URLs based on the URL string, since
        # actually hashing the contents would entail mounting them.
        # most RODISK URLs will contain a hash of their contents, but
        # if they don't, then we warn the user that we may be inadvertently
        # avoiding
        if not roURL[1].startswith("canine-"):
            canine_logging.debug("RODISK input {} cannot be hashed; this job may be inadvertently avoided.".format(self.path))

        # single file/directory RODISKs will contain the CRC32C of the file(s)
        if roURL[1].startswith("canine-crc32c-"):
            return roURL[1][14:]

        # for BatchLocalDisk multifile RODISKs (or non-hashed URLs), the whole URL
        # serves as a hash for the file 
        return self.path

    # handler will be command to attach/mount the RODISK
    # (currently implemented in base.py)

# }}}

def get_file_handler(path, url_map = None, **kwargs):
    url_map = {
      r"^gs://" : HandleGSURL,
      r"^s3://" : HandleAWSURL,
      r"^drs://" : HandleDRSURI,
      r"^https://api.gdc.cancer.gov" : HandleGDCHTTPURL,
      r"^https://api.awg.gdc.cancer.gov" : HandleGDCHTTPURL,
      r"^https://storage\.(?:googleapis|cloud\.google)\.com/" : HandleGCSSignedURL,
      r"^rodisk://" : HandleRODISKURL,
      r"^(?:ftp|https|http)://" : HandleOtherURL
    } if url_map is None else url_map

    # zerothly, if path is already a FileType object, return as-is
    if isinstance(path, FileType):
        return path

    # assume path is a string-like object from here on out
    path = str(path)

    # firstly, check if the path is a regular file
    if os.path.exists(path):
        return HandleRegularFile(path, **kwargs)

    # next, consult the mapping of path URL -> handler
    for pat, handler in url_map.items():
        if re.match(pat, path) is not None:
            return handler(path, **kwargs)

    # otherwise, assume it's a string literal; use the base class
    return StringLiteral(path, **kwargs)
