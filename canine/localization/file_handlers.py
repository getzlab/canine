import abc
import google.cloud.storage
import google.auth
import glob, google_crc32c, json, hashlib, base64, binascii, os, re, requests, shlex, subprocess, threading
import pandas as pd
import urllib.parse

from google.auth.transport.requests import AuthorizedSession
from ..utils import sha1_base32, canine_logging, gcloud_storage_client

# Imported rather than duplicated. The standalone-script constraint is one-directional:
# parallel_download.py must not import canine, so it stays runnable by hand on a node
# where canine is absent -- but it is a module inside this package, so importing FROM it
# is an ordinary intra-package import, and costs ~30 ms of stdlib against a canine chain
# that already pulls in google.cloud.storage and pandas.
#
# Single-sourcing matters most for NAMED_FORMAT_MAGIC: the emitted fallback and the
# downloader have to agree about which files are ambiguous, or the localized file would
# depend on which route ran. Duplicating the table made that a property to be tested for
# rather than one that holds by construction.
#
# The two DEFAULT_* names differ only because "download_connections" is the handler-level
# option name; the values are the same objects.
from .parallel_download import (
    NAMED_FORMAT_MAGIC,
    expected_magic,
    DEFAULT_CONNECTIONS as DEFAULT_DOWNLOAD_CONNECTIONS,
    DEFAULT_MIN_CHUNK as DEFAULT_DOWNLOAD_MIN_CHUNK,
)

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


def extract_content_checksum(headers, transcoded = False):
    """
    Find a content checksum in HTTP response headers that can be verified against the
    downloaded file. Returns `(algorithm, hex_digest)`, or `(None, None)`.

    Recognised sources, since no single one is universal:
      * `x-goog-hash: crc32c=...,md5=...`  — GCS, including signed URLs
      * `content-md5: <base64>`            — RFC 9110
      * `digest` / `repr-digest`           — RFC 3230 / RFC 9530
      * `x-amz-checksum-<algo>: <base64>`  — S3
      * `etag: "<32 hex>"`                 — only when it is a bare hex md5

    A digest is usable exactly when the bytes that land on disk are the bytes it covers.
    Advertised digests describe the *stored* representation, and nothing in this stack
    decompresses in flight -- urllib does not, and `curl` without `--compressed` does not
    -- so what is received is what is stored, and the digest normally applies.

    The one exception is `transcoded=True`: the object is stored compressed but the server
    decompressed it for us, so the digest covers bytes we never see. Gating on it would
    fail every correct download. Callers detect that case and pass it in.

    Note a `Content-Encoding` header is NOT itself grounds for refusal, which an earlier
    version of this got wrong. S3 does not transcode: it serves a gzip-stored object as
    stored, with `Content-Encoding: gzip` and an ETag over those same bytes -- so the
    digest is perfectly usable, and refusing it left S3 gzip objects unverifiable for no
    reason. Confirmed on a real gzip-stored GCS object too: its `x-goog-hash` md5 equals
    its ETag, both over the stored bytes.
    """
    if transcoded:
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


PDL_SCRIPT_NAME = "parallel_download.py"
PDL_EOF_SENTINEL = "# k9pdl-eof"

# Assumed decompression ratio when a gzip-transcoded object's real size cannot be
# determined. Genomics text compresses 4-10x, so this is at the pessimistic end of
# typical rather than a worst case -- the disk-resize daemon covers the rest, and
# over-estimating costs permanent storage on a disk that only ever grows.
GZIP_FALLBACK_RATIO = 5



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
    url_expr=None,
    env_prefix="",
    gunzip=False,
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
    if url_expr:
        # a shell expression evaluated on the node, so interpolated verbatim -- quoting
        # it would stop the expansion and pass the literal text as the URL
        arguments += ["--url", str(url_expr)]
    elif url:
        arguments += ["--url", shlex.quote(str(url))]
    arguments += ["--connections", str(int(connections))]
    arguments += ["--min-chunk", str(int(min_chunk))]

    for header in headers or ():
        arguments += ["--header", shlex.quote(str(header))]

    if gunzip:
        # Ask for the stored bytes explicitly and keep them compressed until verified.
        # This is the pattern gcloud storage cp uses: decompressing in flight would change
        # the byte count mid-transfer, which makes ranged and resumable downloads
        # impossible -- the same reason the chunked writer cannot consume a decoded stream.
        arguments += ["--header", shlex.quote("Accept-Encoding: gzip")]
        arguments += ["--gunzip"]

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

    # The credential env prefix goes on the invocation so the downloader process, and any
    # `aws` subprocess it spawns for the per-chunk fallback, inherit the keys. Same idiom
    # the handler already uses for its own aws calls.
    invocation = (env_prefix + " " if env_prefix else "") + \
        "$K9_PDL_RUN " + " ".join(arguments)

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

        # Parallel-download options, arriving via extra_args from
        # wolF task.conf["localization"] -> get_file_handler(**extra_args).
        self.parallel_download = bool(kwargs.get("parallel_download", True))
        self.download_connections = int(
            kwargs.get("download_connections", DEFAULT_DOWNLOAD_CONNECTIONS)
        )
        self.download_min_chunk = int(
            kwargs.get("download_min_chunk", DEFAULT_DOWNLOAD_MIN_CHUNK)
        )

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

        Sets `self._size` (the number of bytes that will cross the wire),
        `self.body_is_compressed`, and `self.content_checksum` — a
        `(algorithm, hex_digest)` tuple, `(None, None)` when nothing usable is
        advertised. Raises ValueError if no size can be determined.

        Deliberately separate from `hash`/`_get_hash()`: those identify the *input*
        (and for URL handlers are derived from the URL, precisely because the server
        cannot be trusted to name the content), whereas this is a digest of the bytes
        used to verify a completed download.
        """
        # Probe with the same Accept-Encoding the download will send, so what is measured
        # is what will actually be fetched. urllib sets `Accept-Encoding: identity` on
        # every request (verified), whereas curl sends no such header by default -- and
        # per RFC 9110 those are not equivalent: omitting the header means any coding is
        # acceptable, so a server may legitimately compress, while `identity` asks it not
        # to. Left unmatched, the probe could describe a compressed representation whose
        # size and digest we then compare against uncompressed bytes.
        resp = subprocess.run(
          "curl -sIL -H 'Accept-Encoding: identity' {args} {url}".format(
            args = curl_args, url = shlex.quote(self.url)
          ),
          shell = True, capture_output = True
        )
        headers = parse_header_block(resp.stdout.decode(errors = "replace"))

        # Whether to decompress after downloading is decided ONLY on positive,
        # GCS-specific evidence that the stored object is gzip and the stored bytes are
        # what is being served: `x-goog-stored-content-encoding`. That is the case
        # measured against a real object, and the only one whose semantics we know.
        #
        # Deliberately NOT keyed on `Content-Encoding: gzip` alone. A generic server can
        # send that alongside a normal Content-Length, and the historical behavior for
        # such a URL is that `curl -C - -o` (no `--compressed`) writes the bytes as
        # received -- so the localized file has always been the compressed one. Treating
        # the header alone as a decompress signal would silently change what lands on
        # disk for those inputs, which is exactly the kind of change that breaks a
        # pipeline mysteriously. Widening this needs its own evidence.
        received_encoded = "gzip" in [
            part.strip() for part in headers.get("content-encoding", "").lower().split(",")
        ]
        stored_encoded = "gzip" in [
            part.strip()
            for part in headers.get("x-goog-stored-content-encoding", "").lower().split(",")
        ]

        # `Content-Encoding: gzip` means the body is a transport-encoded representation of
        # the file, so the localized file must be the DECODED content: a task reading it
        # expects the actual data, not a gzip stream. This matches RFC 9110 (a
        # content-coding is a property of the representation and the recipient decodes it)
        # and it matches what `gs://` inputs already get, since `gcloud storage cp`
        # decompresses locally -- previously the same object arrived decompressed via
        # gs:// but compressed via a signed URL.
        #
        # Note this changes what lands on disk for such URLs: the old
        # `curl -C - -o` command wrote the bytes as received, i.e. still compressed.
        self.body_is_compressed = received_encoded

        # Stored compressed but served decompressed -- the digest covers bytes we never
        # see, so it cannot be used. This is the only case where a digest must be refused.
        transcoded = stored_encoded and not received_encoded

        # Content-Length first: it is the standard header and most servers send it, gzip
        # or not (and per RFC 9110 it describes the encoded body, which is what crosses
        # the wire -- exactly what the chunk plan needs). The x-goog- fallback exists
        # because a gzip-stored GCS object sends no Content-Length at all, on either a
        # plain request or one with `Accept-Encoding: gzip`. Requiring Content-Length
        # made such objects unlocalizable through this handler. (The original
        # pre-conversion code failed on them too: its `grep -i Content-Length` did match
        # the x-goog- header, but the regex applied to it was anchored, so the match
        # failed and the bare `except` turned it into this same error.)
        size = headers.get("content-length")
        if size is None:
            size = headers.get("x-goog-stored-content-length")
        if size is None:
            raise ValueError("Could not get file header size")
        try:
            self._size = int(size)
        except ValueError:
            raise ValueError("Could not get file header size")

        # When the body is kept compressed until verified, the advertised digest covers
        # exactly the bytes being checked, so it is usable. Verified on a real object: its
        # x-goog-hash md5 equals its ETag, both over the stored bytes.
        self.content_checksum = extract_content_checksum(headers, transcoded = transcoded)

        if self.check_hash and self.content_checksum[0] is None:
            canine_logging.warning(
              "check_hash was requested for {}, but the server advertises no usable "
              "content checksum, so the download cannot be verified. Recognised "
              "headers are x-goog-hash, Content-MD5, Digest/Repr-Digest, "
              "x-amz-checksum-*, and a bare hex ETag.".format(self.url)
            )

        return headers

    def _use_parallel_download(self, url=None):
        """
        Whether this input should go through the parallel downloader at all.

        `download_connections` of 0 or 1, or `parallel_download=False`, emit the legacy
        command directly rather than invoking the downloader only for it to fall back --
        that keeps the opt-out byte-for-byte today's command with no interpreter startup.
        ftp is declined for the same reason: it is not rangeable, so the fallback is a
        foregone conclusion.
        """
        if not self.parallel_download or self.download_connections <= 1:
            return False
        target = url if url is not None else getattr(self, "url", None)
        if target and urllib.parse.urlsplit(str(target)).scheme == "ftp":
            return False
        return True

    def _download_and_verify_lines(self, build_legacy, prefix="", url=None, url_expr=None,
                                   s3=None, url_refresh_cmd=None, etag=None,
                                   part_length=None, checksum=None, env_prefix=""):
        """
        Emit the download for this input, plus whatever verification it still needs.

        `prefix` is prepended to the first emitted line so a handler can keep its
        directory guard on the same line as the download, leaving the surrounding shell
        unchanged.

        Verification is handed to the downloader when it can do it -- an md5, or a
        multipart ETag with a known part length. It verifies before writing the
        completion marker and deletes a corrupt file itself, so also emitting the shell
        gate would re-read the whole object for no additional guarantee: a second full
        pass over a 50 GB input. The gate is still emitted for algorithms the downloader
        does not implement (sha1, sha256, crc32c) and for the legacy path, where nothing
        else checks.

        `checksum` overrides the header-derived one. Some handlers learn the content md5
        from their own metadata service rather than from response headers -- for those
        `self.hash` IS the content digest, whereas for a plain URL handler it is only a
        URL-derived identity, so the two cannot be conflated.

        `url_expr` is for a URL that is not known host-side: a shell expression that
        evaluates to it at run time, interpolated verbatim. DRS needs this because its
        signed URL is minted by a command in the emitted script.
        """
        def with_prefix(lines):
            if prefix and lines:
                return [prefix + lines[0]] + list(lines[1:])
            return list(lines)

        gunzip = bool(getattr(self, "body_is_compressed", False))

        # `build_legacy` is a callable rather than a finished string so the compressed
        # pipeline can retarget the download at a sidecar. Rewriting the string instead
        # was actively wrong: replacing the destination path also hit the same substring
        # inside the URL, producing `-o /d/o.vcf.k9pdl.gz 'https://h/d/o.vcf.k9pdl.gz'`.
        legacy_cmd = build_legacy(self.localized_path)

        if gunzip:
            # The fallback has to produce the same file as the primary path, or which one
            # ran would change what the task reads. So it downloads to a compressed
            # sidecar, verifies THOSE bytes (which is what the advertised digest covers),
            # decompresses, and only then drops the sidecar.
            #
            # Deliberately NOT `curl --compressed`: combined with `-C -` that is a
            # silent-corruption hazard, because the resume offset is taken from the local
            # *decompressed* size but interpreted by the server as an offset into the
            # *compressed* stream. Downloading the encoded bytes and decompressing
            # afterwards keeps `-C -` resuming against the bytes it actually counted.
            #
            # Chained with && so a failed transfer never decompresses a partial file, and
            # a failed stage leaves the sidecar for the next attempt to resume from.
            sidecar = self.localized_path + ".k9pdl.gz"
            stages = [build_legacy(sidecar)]
            stages += self._hash_check_command(checksum, path = sidecar)

            magic = expected_magic(self.path or "")
            if magic is not None:
                # The name promises an already-compressed format, which two different
                # situations produce. Doubly wrapped (that format additionally encoded for
                # transport): removing one layer yields the original upload. Singly
                # compressed with the encoding metadata set by mistake: the stored bytes
                # ALREADY are that format, and decoding would leave the wrong thing in a
                # file whose name says otherwise -- a decompressed BAM in a .bam, say,
                # which samtools cannot read. Distinguished by whether the decoded bytes
                # actually start with the format's magic. Either way the localized file
                # matches what its name says.
                partial = self.localized_path + ".k9pdl.part"
                stages += [
                    "gunzip -c {sidecar} > {partial}".format(
                        sidecar = sidecar, partial = partial),
                    ("if [ \"$(od -An -N{n} -tx1 {partial} | tr -d ' \\n')\" = {hex} ]; "
                     "then mv {partial} {dest}; "
                     "else mv {sidecar} {dest}; rm -f {partial}; fi").format(
                        n = len(magic), hex = magic.hex(),
                        partial = partial, sidecar = sidecar,
                        dest = self.localized_path),
                ]
            else:
                stages += ["gunzip -c {sidecar} > {dest}".format(
                    sidecar = sidecar, dest = self.localized_path)]

            stages += ["rm -f {sidecar}".format(sidecar = sidecar)]
            legacy_cmd = " && ".join(stages)

        if not self._use_parallel_download(url if url_expr is None else None):
            # The compressed pipeline verifies the sidecar itself, so a trailing gate here
            # would re-check the DECOMPRESSED file against a digest covering the
            # compressed bytes -- a guaranteed false failure.
            if gunzip:
                return with_prefix([legacy_cmd])
            return with_prefix([legacy_cmd]) + self._hash_check_command(checksum)

        algorithm, digest = checksum or getattr(self, "content_checksum", (None, None))
        md5 = digest if (self.check_hash and algorithm == "md5") else None
        if etag and part_length and self.check_hash:
            downloader_verifies = True
        else:
            etag = part_length = None
            downloader_verifies = md5 is not None

        lines = _pdl_command(
            url if url is not None else getattr(self, "url", None),
            self.localized_path,
            self.size,
            headers=self._download_headers(),
            md5=md5,
            etag=etag,
            part_length=part_length,
            s3=s3,
            url_refresh_cmd=url_refresh_cmd,
            legacy_cmd=legacy_cmd,
            connections=self.download_connections,
            min_chunk=self.download_min_chunk,
            url_expr=url_expr,
            env_prefix=env_prefix,
            gunzip=gunzip,
        )
        lines = with_prefix(lines)
        if not downloader_verifies:
            lines += self._hash_check_command(checksum)
        return lines

    def _download_headers(self):
        """
        Request headers the downloader must send. Overridden where a handler needs an
        auth header; the base case sends none.

        Note the Accept-Encoding header for a compressed body is added by _pdl_command
        alongside --gunzip, so the two cannot get out of step.
        """
        return ()

    def _hash_check_command(self, checksum=None, path=None):
        """
        Emit the shell gate that verifies a localized file against a known checksum.
        Returns [] when there is nothing to verify.

        `checksum` overrides the header-derived one, for handlers that learn the content
        digest from their own metadata service.

        Same shape as the md5 gates the GDC/DRS handlers already emit: compare, and on
        mismatch delete the corrupt file and exit 1.
        """
        if not self.check_hash:
            return []

        algorithm, digest = checksum or getattr(self, "content_checksum", (None, None))
        if algorithm is None or digest is None:
            return []

        target = path if path is not None else self.localized_path
        fail = "{{ echo 'deleting corrupted file' ; rm -f {path} ; exit 1 ; }}".format(
          path = target
        )

        if algorithm in _CHECKSUM_COREUTILS:
            return ["[[ $({prog} {path} | sed -r 's/  .*$//') == {digest} ]] || {fail}".format(
              prog = _CHECKSUM_COREUTILS[algorithm],
              path = target,
              digest = digest,
              fail = fail,
            )]

        # crc32c has no coreutils equivalent; google_crc32c is in the worker image
        return ['python3 -c "import sys,google_crc32c;h=google_crc32c.Checksum();'
                "f=open(sys.argv[1],'rb');"
                "[h.update(c) for c in iter(lambda: f.read(1048576), b'')];"
                'sys.exit(0 if h.hexdigest().decode() == sys.argv[2] else 1)" '
                '{path} {digest} || {fail}'.format(
                  path = target, digest = digest, fail = fail
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

            if ret.returncode == 0:
                # Check both stderr (for error messages) and stdout (for success messages)
                return (
                    b'requester pays bucket but no user project provided' in text
                    or ret.stdout.strip() == b'True'
                )
            else:
                # Try again ls-ing the object itself
                # sometimes permissions can disallow bucket inspection
                # (e.g. missing storage.buckets.get, as with many third-party
                # requester-pays buckets) but allow object inspection -- retry
                # regardless of why the describe call failed, not just on 404
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
            if not self.extra_args.get("allow_requester_pays", False):
                raise ValueError(f"File {self.path} resides in a requester-pays bucket, but access to "
                                  "requester-pays buckets is disabled (allow_requester_pays=False)")
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
        """
        Total size of the object(s), in the bytes that will actually land on disk.

        This is not simply `sum(b.size)`. An object stored with
        `Content-Encoding: gzip` is served *decompressed* to any client that does not ask
        for gzip -- GCS calls this decompressive transcoding -- but its `size` metadata is
        the **stored, compressed** byte count. `gcloud storage cp` writes the decompressed
        bytes, so sizing the localization disk from `size` under-provisions it by the
        compression ratio. Genomics text (VCF/BED/GTF/FASTA) routinely compresses 4-10x,
        and the disk is sized from this number with only a 5% margin, so the failure mode
        is ENOSPC partway through localization.

        The disk-resize daemon is what makes that non-fatal; this estimate only decides how
        good the starting point is. A wildly low guess still works, it just costs many
        resizes and a slow start.
        """
        total = 0
        for blob in self.blob():
            total += self._blob_localized_size(blob)
        return total

    def _blob_localized_size(self, blob):
        """
        How many bytes this blob occupies once localized, accounting for transcoding.

        Predicting a decompressed size is not reliably possible in general -- GCS exposes
        no decompressed-size field -- so this tries the sources in descending order of
        trustworthiness and says which one it used. Guessing silently would be worse than
        guessing loudly.
        """
        encoding = (getattr(blob, "content_encoding", None) or "").strip().lower()
        if encoding != "gzip":
            return blob.size

        # 1. An explicitly supplied size, or one recorded in the object's own custom
        #    metadata. Authoritative, because a human or a producing pipeline said so.
        override = self.extra_args.get("uncompressed_size")
        if override is None:
            metadata = getattr(blob, "metadata", None) or {}
            override = metadata.get("uncompressed_size") or metadata.get("uncompressedSize")
        if override is not None:
            try:
                size = int(override)
                canine_logging.info1(
                    "{} is gzip-transcoded; using the declared uncompressed size "
                    "{} rather than the stored {}".format(self.path, size, blob.size)
                )
                return size
            except (TypeError, ValueError):
                canine_logging.warning(
                    "Ignoring unparseable uncompressed_size {!r} for {}".format(
                        override, self.path)
                )

        # 2. The gzip ISIZE trailer, read with a ranged GET of the last four bytes. Only
        #    trustworthy below 4 GiB: ISIZE is the decompressed length mod 2**32, so a
        #    larger object wraps, and a multi-member stream reports only its last member.
        if blob.size is not None and blob.size < (1 << 32):
            trailer = self._read_gzip_isize(blob)
            if trailer is not None and trailer >= blob.size:
                canine_logging.info1(
                    "{} is gzip-transcoded; using the gzip ISIZE trailer {} rather than "
                    "the stored {}".format(self.path, trailer, blob.size)
                )
                return trailer

        # 3. A conservative multiplier. Deliberately last, and deliberately loud: it is a
        #    guess, and the resize daemon is what makes being wrong survivable.
        estimate = blob.size * GZIP_FALLBACK_RATIO
        canine_logging.warning(
            "{} is gzip-transcoded and its decompressed size is unknown; estimating "
            "{} ({}x the stored {}). The localization disk will grow on demand if this "
            "is too low.".format(self.path, estimate, GZIP_FALLBACK_RATIO, blob.size)
        )
        return estimate

    @staticmethod
    def _read_gzip_isize(blob):
        """
        Read the four-byte ISIZE trailer of a gzip member.

        Requires the *stored* bytes, so the read must not be transcoded: the download_as
        call below asks for a byte range, and GCS serves raw bytes for a ranged read of a
        gzip object rather than decompressing it.
        """
        try:
            tail = blob.download_as_bytes(start = blob.size - 4, end = blob.size - 1,
                                          raw_download = True)
        except Exception as e:
            canine_logging.warning(
                "Could not read the gzip trailer of {}: {}".format(blob.name, e)
            )
            return None
        if not tail or len(tail) != 4:
            return None
        return int.from_bytes(tail, "little")

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
        return ("[ ! -d {dest_dir} ] && mkdir -p {dest_dir} || :; ".format(dest_dir = self.localized_path if self.is_dir else dest_dir)) + f'CLOUDSDK_STORAGE_TRACKER_DIR="{dest_dir}/.gcloud_tracker_dir" {self._sliced_download_env()}gcloud storage cp {self.rp_string} -r -n -L "{dest_dir}/.gcloud_manifest" {self.path} {dest_dir}/{dest_file if not self.is_dir else ""}'

    def _sliced_download_env(self):
        """
        Tune `gcloud storage cp`'s own sliced download to the same node budget the
        parallel downloader uses.

        This handler is deliberately NOT converted: gcloud already does sliced downloads
        and already resumes via the tracker directory and manifest it is passed, so there
        is nothing to replace -- only to size correctly. Reusing download_min_chunk and
        download_connections means one set of knobs tunes both paths rather than two that
        can drift.

        process_count is left alone on purpose: gcloud defaults it to the core count,
        which on the exclusively-reserved n1-standard-8 already *is* the node budget, so
        overriding it would only risk contradicting that.
        """
        components = max(1, self.download_connections) if self.parallel_download else 1
        return (
            'CLOUDSDK_STORAGE_SLICED_OBJECT_DOWNLOAD_THRESHOLD={threshold} '
            'CLOUDSDK_STORAGE_SLICED_OBJECT_DOWNLOAD_MAX_COMPONENTS={components} '
            'CLOUDSDK_STORAGE_THREAD_COUNT={components} '
        ).format(threshold = self.download_min_chunk, components = components)

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

    # Twelve hours. The default `aws s3 presign` window is one hour, which a 279 GiB
    # object at 60 MB/s outlasts -- and an expired signature is a 403, which the
    # downloader treats as permanent. Overridable per-file so an unusually slow source
    # or an unusually strict signing policy can move it in either direction.
    default_presign_expiry = 12 * 60 * 60

    # TODO: use boto3 API; overhead for calling out to aws shell command might be high
    #       this would also allow us to run on systems that don't have the aws tool installed
    # TODO: support directories

    def __init__(self, path, **kwargs):
        """
        Optional arguments:
        * aws_access_key_id
        * aws_secret_access_key
        * aws_endpoint_url
        * presign_expiry (seconds; default 12 h)
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

        # int() rather than trusting the caller: this lands inside an emitted shell
        # command, so a string carrying anything shell-significant would be injected
        # into the presign call. A bad value should raise here, host-side.
        self.presign_expiry = int(
            self.extra_args.get("presign_expiry") or self.default_presign_expiry
        )

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

        bucket = self.path.split("/")[2]
        key = "/".join(self.path.split("/")[3:])

        # The fallback keeps its append-resume, unchanged from the original command.
        #
        # It infers how much is already downloaded from the destination's size, which is
        # only sound because the downloader now discards a preallocated working file
        # before handing over (see clear_preallocated_working_file). Without that guard
        # this would see a full-size sparse file and skip the download entirely; with it,
        # a size-based resume is exactly as safe here as `curl -C -` is for the other
        # handlers -- and dropping it would mean the fallback path stopped being
        # resumable, which the resumability requirement does not allow.
        #
        # The `if [ $SZ != size ]` guard also makes the whole block idempotent, which
        # matters because localization.sh is re-run in full after a preemption.
        # Kept to a single line: this string is also embedded as a --legacy-cmd argument
        # and inside an if/else branch, and a multi-line value there is needlessly
        # fragile.
        def legacy(target):
            return (
                "[ -f {path} ] && SZ=$(stat --printf '%s' {path}) || SZ=0; "
                "if [ $SZ != {size} ]; then "
                '{env} aws s3api {extra_args} get-object --bucket {bucket} --key {key} '
                '--range "bytes=$SZ-" >(cat >> {path}) > /dev/null; fi'
            ).format(
                path = target,
                size = self.size,
                env = self.command_env_str,
                extra_args = self.s3_extra_args_str,
                bucket = bucket,
                key = key,
            )

        cmd = [f"[ ! -d {dest_dir} ] && mkdir -p {dest_dir} || :"]

        url_refresh_cmd = None
        if "--no-sign-request" in self.s3_extra_args:
            # A public bucket needs no presigning, so the object URL is built host-side
            # and there is nothing to mint on the node.
            url, url_expr = self._public_object_url(bucket, key), None
        else:
            # Presigning has to happen on the node: the credentials live there, not here.
            # Feeding a presigned URL into the generic ranged-HTTP path means one code
            # path for every source and no `aws` process per chunk.
            #
            # `--expires-in`, because the default is one hour and the objects this exists
            # for do not finish in one hour. A 279 GiB BAM at the ~60 MB/s measured
            # against the GDC endpoint takes about 80 minutes, so the signature expires
            # mid-transfer; `open_range` then gets a 403, which is a PermanentError, so
            # the download fails do-not-retry with most of the object already on disk.
            # The exact case this work exists to fix.
            #
            # The cost of a longer window is a longer-lived credential in the emitted
            # script and in the environment of whatever the node runs, which is why this
            # is 12 hours rather than the 7 days SigV4 permits.
            #
            # Passed as --url-refresh-cmd as well, the way HandleDRSURI does: a clock
            # skew, a retried task resuming near the end of the window, or an object
            # slower than 12 hours all still expire, and re-minting resumes in place
            # where a PermanentError throws the transfer away.
            presigner = "{env} aws s3 {extra_args} presign {url} --expires-in {expiry}".format(
                env = self.command_env_str,
                extra_args = self.s3_extra_args_str,
                url = self.path,
                expiry = self.presign_expiry,
            ).lstrip()
            # `|| :` keeps a presign failure from aborting the script under set -e, and an
            # empty result makes the downloader fall through to its per-chunk
            # `aws s3api get-object --range` source instead -- which is what covers
            # session-token-only credentials and exotic endpoints.
            cmd += ["export K9_S3_URL=$({presigner} 2>/dev/null || :)".format(
                presigner = presigner)]
            url, url_expr, url_refresh_cmd = None, '"$K9_S3_URL"', presigner

        checksum = etag = part_length = None
        if self.check_hash:
            if self.headers.get("PartsCount", 1) > 1 and "PartLength" in self.headers:
                # A multipart ETag is an md5-of-md5s, so chunk boundaries are snapped to
                # whole parts and the downloader computes it during the transfer. This
                # replaces the post-hoc multiprocessing md5 pass, saving a full extra read
                # of the object.
                etag = self.hash
                part_length = self.headers["PartLength"]
            else:
                # For a single-part object the ETag *is* the whole-file md5.
                checksum = ("md5", self.hash)

        cmd += self._download_and_verify_lines(
            legacy,
            url = url,
            url_expr = url_expr,
            s3 = {"bucket": bucket, "key": key,
                  "extra_args": self.s3_extra_args_str},
            url_refresh_cmd = url_refresh_cmd,
            etag = etag,
            part_length = part_length,
            checksum = checksum,
            env_prefix = self.command_env_str,
        )
        return "\n".join(cmd)

    def _public_object_url(self, bucket, key):
        """
        Object URL for a bucket that needs no signing. Path-style against a custom
        endpoint, virtual-hosted style otherwise.
        """
        if self.aws_endpoint_url:
            return "{}/{}/{}".format(self.aws_endpoint_url.rstrip("/"), bucket, key)
        return "https://{}.s3.amazonaws.com/{}".format(bucket, key)

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
        def legacy(target):
            if self.token is not None:
                return "curl -C - -o {path} {token} '{url}'".format(
                    path = target, token = self.token_flag, url = self.url
                )
            return "curl -C - -o {path} '{url}'".format(path = target, url = self.url)

        # self.hash is the content md5 for this handler (from the DRS record, or from the
        # Content-MD5 header when falling back to the GDC API) -- unlike a plain URL
        # handler, where hash is only a URL-derived identity.
        cmd += self._download_and_verify_lines(
            legacy,
            prefix = "[ ! -d {dest_dir} ] && mkdir -p {dest_dir} || :; ".format(
                dest_dir = dest_dir
            ),
            checksum = ("md5", self.hash) if self.hash else None,
        )
        return "\n".join(cmd)

    def _download_headers(self):
        """
        The GDC token travels as a request header. It is already interpolated into the
        emitted command today, so this does not widen its exposure -- and the downloader
        redacts headers and query strings from its own logging.
        """
        if self.token is None:
            return ()
        return ("X-Auth-Token: {}".format(self.token),)

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
        # The resolver is now a value in its own right rather than being inlined into the
        # assignment, because it serves twice: once to mint the URL up front, and again as
        # --url-refresh-cmd so the downloader can re-mint it on a 403 and resume in place.
        # A signed URL can expire mid-transfer, and re-minting beats starting over.
        resolver = f'curl -S -X POST --url "{type(self).drs_resolver}" ' + \
                   '-H "authorization: Bearer $(gcloud auth print-access-token)" ' + \
                   f'-H "content-type: application/json" --data \'{data_str}\' | ' + \
                   'python3 -c \'import json,sys; print(json.load(sys.stdin)["accessUrl"]["url"])\''
        # Exported, not just assigned: the fallback command passed via --legacy-cmd
        # references "$signed_url", and the downloader runs it in its own subshell.
        # A plain shell variable would not be inherited there, so the fallback would
        # curl an empty URL. The resolver snippet itself is unchanged.
        cmd = [f'export signed_url=$({resolver})']

        def legacy(target):
            return f'curl -C - -o {target} "$signed_url"'
        # The URL is not known host-side, so it is passed as a shell expression that the
        # node evaluates. self.hash is the content md5 from the DRS record.
        cmd += self._download_and_verify_lines(
            legacy,
            prefix = f'[ ! -d {dest_dir} ] && mkdir -p {dest_dir} || :; ',
            url_expr = '"$signed_url"',
            url_refresh_cmd = resolver,
            checksum = ("md5", self.hash) if self.hash else None,
        )
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
        # the legacy single-stream command, kept verbatim: it is both the
        # parallel_download=False opt-out and the in-script no-range fallback
        cmd += self._download_and_verify_lines(
            lambda target: "curl -C - -o {path} '{url}'".format(
                path = target, url = self.url
            ),
            prefix = "[ ! -d {dest_dir} ] && mkdir -p {dest_dir} || :; ".format(
                dest_dir = dest_dir
            ),
        )
        return "\n".join(cmd)

class HandleOtherURL(FileType):
    localization_mode = "url"

    def __init__(self, path, **kwargs):
        super().__init__(path, **kwargs)
        
        self.url = self.path
        split = urllib.parse.urlsplit(self.url)
        if split.scheme not in ("http", "https", "ftp") or not split.netloc:
            raise ValueError(f"URL {self.url} format not recognized")

        # The filename comes from the URL *path* only, with the query string dropped.
        # The previous regex took everything after the last "/", which for any signed URL
        # swept the whole query in: an S3 SigV4 URL localized to a file literally named
        # `sample.bam?X-Amz-Algorithm=...&X-Amz-Signature=...`. Three things go wrong with
        # that -- such a name usually exceeds the 255-byte limit, the signature changes on
        # every attempt so the name is not stable, and an unstable basename also changes
        # the localization disk's name hash and so defeats disk reuse. HandleGCSSignedURL
        # already stripped the query for the same reason; this brings the generic handler
        # into line.
        name = os.path.basename(split.path.rstrip("/"))
        if not name:
            raise ValueError(f"URL {self.url} format not recognized")
        self.path = name
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
        cmd += self._download_and_verify_lines(
            lambda target: "curl -C - -o {path} '{url}'".format(
                path = target, url = self.url
            ),
            prefix = "[ ! -d {dest_dir} ] && mkdir -p {dest_dir} || :; ".format(
                dest_dir = dest_dir
            ),
        )
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

## Bucket-mounted (RODISK replacement) reads {{{

class HandleBucketMountURL(FileType):
    localization_mode = "bucket_mount"

    # file size is unknowable without mounting

    # hash is based on the bucketmount URL itself (bucket + content hash),
    # since actually hashing the contents would entail mounting them --
    # same reasoning as HandleRODISKURL
    def _get_hash(self):
        bmURL = re.match(r"bucketmount://([^/]+)/([^/]+)/(.*)", self.path)
        if bmURL is None or bmURL[3] == "":
            raise ValueError("Invalid bucketmount URL specified ({})!".format(self.path))

        # the content address lives in the bucket name ("wolf-<project>-<region>
        # -<hash>") for a per-localization bucket, or in the first object path
        # segment ("canine-<hash>") for the legacy shared-bucket layout
        if not (bmURL[1].startswith("wolf-") or bmURL[2].startswith("canine-")):
            canine_logging.debug("Bucket-mount input {} cannot be hashed; this job may be inadvertently avoided.".format(self.path))

        # the whole URL (bucket + content hash + file path) serves as the hash
        return self.path

    # handler will be command to gcsfuse-mount the bucket prefix
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
      r"^bucketmount://" : HandleBucketMountURL,
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
