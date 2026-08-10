import pytest
from unittest.mock import patch
from canine.localization.file_handlers import (
    parse_header_block,
    extract_content_checksum,
    get_file_handler,
    hash_set,
    FileType,
    StringLiteral,
    HandleGSURL,
    HandleAWSURL,
    HandleRODISKURL,
    HandleDRSURI,
    HandleGCSSignedURL,
    HandleGDCHTTPURL,
    HandleOtherURL,
    HandleGSURLStream,
    HandleAWSURLStream,
    HandleGDCHTTPURLStream,
    HandleDRSURIStream,
)


# ---------------------------------------------------------------------------
# hash_set
# ---------------------------------------------------------------------------

class TestHashSet:

    def test_deterministic(self):
        s = {"b", "a", "c"}
        assert hash_set(s) == hash_set(s)

    def test_order_independent(self):
        assert hash_set({"a", "b", "c"}) == hash_set({"c", "a", "b"})

    def test_different_sets_differ(self):
        assert hash_set({"a", "b"}) != hash_set({"a", "c"})

    def test_single_element(self):
        h = hash_set({"only"})
        assert isinstance(h, str) and len(h) == 32  # md5 hex

    def test_requires_set_type(self):
        with pytest.raises(AssertionError):
            hash_set(["a", "b"])


# ---------------------------------------------------------------------------
# FileType base class hash
# ---------------------------------------------------------------------------

class TestFileTypeBaseHash:

    def test_same_path_same_hash(self):
        f1 = StringLiteral("gs://bucket/file.txt")
        f2 = StringLiteral("gs://bucket/file.txt")
        assert f1.hash == f2.hash

    def test_different_paths_different_hashes(self):
        f1 = StringLiteral("gs://bucket/a.txt")
        f2 = StringLiteral("gs://bucket/b.txt")
        assert f1.hash != f2.hash

    def test_hash_is_cached(self):
        f = StringLiteral("some_path")
        h1 = f.hash
        h2 = f.hash
        assert h1 is h2  # same object (cached)


# ---------------------------------------------------------------------------
# HandleRODISKURL._get_hash
# ---------------------------------------------------------------------------

class TestHandleRODISKURLHash:

    def test_crc32c_disk_returns_hash_suffix(self):
        f = HandleRODISKURL("rodisk://canine-crc32c-abc123def456/myfile.txt")
        assert f._get_hash() == "abc123def456"

    def test_non_crc32c_disk_returns_full_url(self):
        url = "rodisk://canine-someotherdisk/myfile.txt"
        f = HandleRODISKURL(url)
        assert f._get_hash() == url

    def test_non_canine_disk_returns_full_url(self):
        url = "rodisk://user-custom-disk/data.bam"
        f = HandleRODISKURL(url)
        assert f._get_hash() == url

    def test_invalid_url_no_path_raises(self):
        with pytest.raises(ValueError):
            HandleRODISKURL("rodisk://diskonly").  _get_hash()

    def test_invalid_url_completely_malformed_raises(self):
        with pytest.raises(ValueError):
            HandleRODISKURL("not-a-rodisk-url")._get_hash()


# ---------------------------------------------------------------------------
# get_file_handler — URL routing dispatch
# ---------------------------------------------------------------------------

class TestGetFileHandler:
    """All tests mock os.path.exists to return False so local-file branch is skipped."""

    @pytest.fixture(autouse=True)
    def no_local_files(self):
        with patch("os.path.exists", return_value=False):
            yield

    def test_filetype_object_returned_as_is(self):
        ft = StringLiteral("already_a_filetype")
        assert get_file_handler(ft) is ft

    def test_gs_url(self):
        h = get_file_handler("gs://bucket/path/to/file.txt")
        assert isinstance(h, HandleGSURL)
        assert h.path == "gs://bucket/path/to/file.txt"

    def test_s3_url(self):
        # HandleAWSURL runs `aws s3api head-object` in __init__; mock to avoid needing AWS CLI
        with patch.object(HandleAWSURL, "__init__", return_value=None):
            h = get_file_handler("s3://bucket/key")
        assert isinstance(h, HandleAWSURL)

    def test_drs_url(self):
        # HandleDRSURI calls drshub API in __init__; mock to avoid needing GCP credentials
        with patch.object(HandleDRSURI, "__init__", return_value=None):
            h = get_file_handler("drs://drs.server.org/object-id")
        assert isinstance(h, HandleDRSURI)

    _GDC_UUID = "550e8400-e29b-41d4-a716-446655440000"

    def test_gdc_api_url(self):
        # HandleGDCHTTPURL calls drshub/GDC API in __init__; mock to avoid live requests
        with patch.object(HandleGDCHTTPURL, "__init__", return_value=None):
            h = get_file_handler(f"https://api.gdc.cancer.gov/data/{self._GDC_UUID}")
        assert isinstance(h, HandleGDCHTTPURL)

    def test_gdc_awg_url(self):
        with patch.object(HandleGDCHTTPURL, "__init__", return_value=None):
            h = get_file_handler(f"https://api.awg.gdc.cancer.gov/data/{self._GDC_UUID}")
        assert isinstance(h, HandleGDCHTTPURL)

    def test_gcs_signed_url_googleapis(self):
        h = get_file_handler("https://storage.googleapis.com/bucket/file.txt")
        assert isinstance(h, HandleGCSSignedURL)

    def test_gcs_signed_url_cloud_google(self):
        h = get_file_handler("https://storage.cloud.google.com/bucket/file.txt")
        assert isinstance(h, HandleGCSSignedURL)

    def test_rodisk_url(self):
        h = get_file_handler("rodisk://canine-crc32c-abc/file.txt")
        assert isinstance(h, HandleRODISKURL)

    def test_generic_https_url(self):
        # HandleOtherURL runs `curl -sIL` in __init__; mock to avoid network
        with patch.object(HandleOtherURL, "__init__", return_value=None):
            h = get_file_handler("https://example.com/data.tar.gz")
        assert isinstance(h, HandleOtherURL)

    def test_generic_http_url(self):
        with patch.object(HandleOtherURL, "__init__", return_value=None):
            h = get_file_handler("http://example.com/data.tar.gz")
        assert isinstance(h, HandleOtherURL)

    def test_ftp_url(self):
        with patch.object(HandleOtherURL, "__init__", return_value=None):
            h = get_file_handler("ftp://ftp.example.org/file.vcf")
        assert isinstance(h, HandleOtherURL)

    def test_plain_string_becomes_string_literal(self):
        h = get_file_handler("just_a_filename.txt")
        assert isinstance(h, StringLiteral)
        assert h.path == "just_a_filename.txt"

    def test_local_file_returns_regular_file_handler(self):
        with patch("os.path.exists", return_value=True):
            from canine.localization.file_handlers import HandleRegularFile
            h = get_file_handler("/local/path/file.txt")
            assert isinstance(h, HandleRegularFile)

    def test_path_coerced_to_string(self):
        # Non-string path-like objects should be str()'d
        h = get_file_handler(42)
        assert isinstance(h, StringLiteral)
        assert h.path == "42"


# ---------------------------------------------------------------------------
# check_hash / check_md5 alias
# ---------------------------------------------------------------------------

class TestCheckHashResolution:
    """
    `check_hash` is the preferred spelling; `check_md5` is the original name and is
    used extensively by downstream wolF pipelines, so it must keep working. The flag
    is resolved once on FileType so that route-dependent verification has a single
    decision point.

    StringLiteral is used as the concrete handler throughout because it does no
    network work in __init__ — the resolution lives on the shared base class, so any
    subclass exercises it.
    """

    def test_neither_key_defaults_to_false(self):
        assert StringLiteral("x").check_hash is False

    @pytest.mark.parametrize("value", [True, False])
    def test_check_hash_alone(self, value):
        assert StringLiteral("x", check_hash=value).check_hash is value

    @pytest.mark.parametrize("value", [True, False])
    def test_check_md5_alone_is_honoured(self, value):
        assert StringLiteral("x", check_md5=value).check_hash is value

    @pytest.mark.parametrize("value", [True, False])
    def test_both_keys_agreeing_is_fine(self, value):
        """A caller mid-migration may reasonably pass both."""
        h = StringLiteral("x", check_hash=value, check_md5=value)
        assert h.check_hash is value

    def test_conflicting_keys_raise(self):
        """
        Guessing here could silently disable integrity checking, so a disagreement is
        treated as a caller bug rather than resolved by precedence.
        """
        with pytest.raises(ValueError, match="Conflicting integrity check flags"):
            StringLiteral("x", check_hash=True, check_md5=False)
        with pytest.raises(ValueError, match="Conflicting integrity check flags"):
            StringLiteral("x", check_hash=False, check_md5=True)

    def test_values_are_coerced_to_bool(self):
        assert StringLiteral("x", check_hash=1).check_hash is True
        assert StringLiteral("x", check_md5=0).check_hash is False
        # truthiness, not equality, decides conflict
        assert StringLiteral("x", check_hash=1, check_md5=True).check_hash is True


class TestCheckMd5Alias:

    def test_check_md5_reads_back_equal_to_check_hash(self):
        for kwargs in ({}, {"check_hash": True}, {"check_md5": True}, {"check_md5": False}):
            h = StringLiteral("x", **kwargs)
            assert h.check_md5 == h.check_hash

    def test_assignment_through_either_name_is_visible_in_both(self):
        h = StringLiteral("x")
        h.check_md5 = True
        assert h.check_hash is True and h.check_md5 is True
        h.check_hash = False
        assert h.check_md5 is False and h.check_hash is False

    def test_assignment_coerces_to_bool(self):
        h = StringLiteral("x")
        h.check_md5 = "yes"
        assert h.check_hash is True

    def test_extra_args_carries_both_spellings(self):
        """
        extra_args is passed around as a plain dict (e.g. wolF's
        get_file_handler(**extra_args)) and call sites may index either name, so both
        must reflect the resolved value.
        """
        h = StringLiteral("x", check_md5=True)
        assert h.extra_args["check_md5"] is True
        assert h.extra_args["check_hash"] is True

    def test_extra_args_stays_in_sync_after_assignment(self):
        h = StringLiteral("x", check_md5=True)
        h.check_hash = False
        assert h.extra_args["check_md5"] is False
        assert h.extra_args["check_hash"] is False

    def test_extra_args_normalised_when_neither_key_given(self):
        h = StringLiteral("x")
        assert h.extra_args["check_md5"] is False
        assert h.extra_args["check_hash"] is False


class TestCheckHashEmittedCommandsUnchanged:
    """
    Both spellings must produce byte-identical localization commands. This is the
    guard that the alias is a rename and not a behaviour change.
    """

    _HEADERS = (
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Length: 1024\r\n"
        b"Content-MD5: 1B2M2Y8AsgTpgAmY7PhCfg==\r\n"
        b"\r\n"
    )

    def _cmd(self, headers=None, **kwargs):
        # the URL handlers probe size/checksums with a live `curl -sIL`; stub it
        class FakeCompleted:
            stdout = headers if headers is not None else self._HEADERS

        with patch("os.path.exists", return_value=False), \
             patch("canine.localization.file_handlers.subprocess.run",
                   return_value=FakeCompleted()):
            h = get_file_handler("https://example.com/data.bam", **kwargs)
        return h.localization_command("/dest/dir/data.bam")

    def test_both_spellings_produce_identical_commands(self):
        assert self._cmd(check_md5=True) == self._cmd(check_hash=True)
        assert self._cmd(check_md5=False) == self._cmd(check_hash=False)

    def test_omitting_the_flag_matches_passing_false(self):
        assert self._cmd() == self._cmd(check_hash=False)

    def test_flag_actually_changes_the_command(self):
        """
        Sanity check that the tests above are not comparing two no-ops.

        Since this handler was converted to the parallel downloader, the md5 is verified
        by passing --check-md5 rather than by a shell md5sum gate -- the downloader checks
        before writing its completion marker, so emitting the gate too would re-read the
        whole object for nothing. Either way the flag has to change the command.
        """
        with_check = self._cmd(check_hash=True)
        without = self._cmd(check_hash=False)
        assert with_check != without
        assert "--check-md5" in with_check and "--check-md5" not in without
        assert "md5sum" not in with_check, "gate emitted on top of --check-md5"

    def test_no_gate_emitted_when_server_advertises_no_checksum(self):
        """
        Not every http server offers a digest. The flag then has nothing to verify
        against, which is warned about at construction rather than silently gating on
        a value we do not have.
        """
        bare = b"HTTP/1.1 200 OK\r\nContent-Length: 1024\r\n\r\n"
        assert self._cmd(headers=bare, check_hash=True) == self._cmd(headers=bare)


class TestDRSResolution:
    """
    HandleDRSURI reads the flag during __init__ to decide whether to ask drshub for
    hashes. That use is not merely a gate on a later md5 check, so it must survive the
    consolidation — and it depends on super().__init__() having resolved the flag
    before it runs.
    """

    _UUID = "abc-123"
    URI = "drs://dg.4dfc:" + _UUID

    def _resolve(self, metadata=None, **kwargs):
        """Returns (list of per-call field lists, the constructed handler)."""
        calls = []

        class FakeResponse:
            status_code = 200
            ok = True

            def json(self):
                return metadata if metadata is not None else {
                    "size": 1024,
                    "fileName": "f.bam",
                    "hashes": {"md5": "d41d8cd98f00b204e9800998ecf8427e"},
                    "accessUrl": {"url": "https://example.com/bucket/uuid/f.bam?sig=x"},
                }

        class FakeSession:
            def post(self, url, headers=None, json=None):
                calls.append(json["fields"])
                return FakeResponse()

        with patch("canine.localization.file_handlers.gcp_auth_session",
                   return_value=FakeSession()):
            handler = HandleDRSURI(self.URI, **kwargs)
        return calls, handler

    def test_hashes_requested_when_flag_set_via_check_md5(self):
        calls, _ = self._resolve(check_md5=True)
        assert "hashes" in calls[0]

    def test_hashes_requested_when_flag_set_via_check_hash(self):
        calls, _ = self._resolve(check_hash=True)
        assert "hashes" in calls[0]

    def test_hashes_not_requested_when_flag_unset(self):
        calls, _ = self._resolve()
        assert "hashes" not in calls[0]

    def test_resolver_called_exactly_once(self):
        """
        All potentially-needed fields are requested up front, so resolution is a
        single round trip rather than a contingent follow-up call.
        """
        calls, _ = self._resolve(check_hash=True)
        assert len(calls) == 1

    def test_access_url_requested_up_front(self):
        calls, _ = self._resolve()
        assert "accessUrl" in calls[0]

    def test_filename_recovered_from_access_url_without_a_second_call(self):
        """
        drshub sometimes reports the bare UUID as fileName; the real name comes from
        the last path segment of the signed accessUrl. That used to need a second
        resolver call.
        """
        calls, handler = self._resolve(metadata={
            "size": 1024,
            "fileName": self._UUID,   # drshub gave us the UUID
            "accessUrl": {"url": "https://dom.com/bucket/uuid/real_name.bam?sig=x"},
        })
        assert len(calls) == 1
        assert handler.path == "real_name.bam"

    def test_falls_back_to_provided_filename_when_access_url_absent(self):
        _, handler = self._resolve(metadata={
            "size": 1024, "fileName": self._UUID,
        })
        assert handler.path == self._UUID

    def test_proper_filename_is_used_as_is(self):
        _, handler = self._resolve(metadata={
            "size": 2048, "fileName": "sample.cram",
            "accessUrl": {"url": "https://dom.com/bucket/uuid/other.bam?sig=x"},
        })
        assert handler.path == "sample.cram"
        assert handler.size == 2048

    def test_missing_filename_raises(self):
        with pytest.raises(ValueError):
            self._resolve(metadata={"size": 1024})


# ---------------------------------------------------------------------------
# HTTP header checksum parsing
# ---------------------------------------------------------------------------

class TestParseHeaderBlock:

    def test_lowercases_names_and_strips_values(self):
        h = parse_header_block("HTTP/1.1 200 OK\r\nContent-Length:  42 \r\n")
        assert h["content-length"] == "42"

    def test_status_line_is_not_a_header(self):
        h = parse_header_block("HTTP/1.1 200 OK\r\nETag: \"abc\"\r\n")
        assert set(h) == {"etag"}

    def test_only_final_redirect_block_is_used(self):
        """
        `curl -sIL` emits a block per hop. A 302's Content-Length describes the
        redirect body, not the object, so grepping the whole output picks the wrong
        size — which is what the old single-regex approach did.
        """
        raw = (
            "HTTP/1.1 302 Found\r\nContent-Length: 0\r\nLocation: /real\r\n"
            "\r\n"
            "HTTP/1.1 200 OK\r\nContent-Length: 5000\r\n"
        )
        assert parse_header_block(raw)["content-length"] == "5000"

    def test_repeated_headers_are_joined(self):
        raw = ("HTTP/1.1 200 OK\r\n"
               "x-goog-hash: crc32c=AAAAAA==\r\n"
               "x-goog-hash: md5=1B2M2Y8AsgTpgAmY7PhCfg==\r\n")
        assert parse_header_block(raw)["x-goog-hash"] == \
            "crc32c=AAAAAA==,md5=1B2M2Y8AsgTpgAmY7PhCfg=="

    def test_empty_input(self):
        assert parse_header_block("") == {}


class TestExtractContentChecksum:
    # md5 and crc32c of b"" -- convenient known values
    EMPTY_MD5_B64 = "1B2M2Y8AsgTpgAmY7PhCfg=="
    EMPTY_MD5_HEX = "d41d8cd98f00b204e9800998ecf8427e"

    def test_no_checksum_headers(self):
        assert extract_content_checksum({"content-length": "10"}) == (None, None)

    def test_x_goog_hash_prefers_md5_over_crc32c(self):
        """crc32c needs python3; md5 is verifiable with coreutils, so prefer it."""
        algo, digest = extract_content_checksum(
            {"x-goog-hash": "crc32c=AAAAAA==,md5=" + self.EMPTY_MD5_B64})
        assert (algo, digest) == ("md5", self.EMPTY_MD5_HEX)

    def test_x_goog_hash_crc32c_only(self):
        """A GCS composite object advertises crc32c and no md5."""
        algo, digest = extract_content_checksum({"x-goog-hash": "crc32c=AAAAAA=="})
        assert algo == "crc32c" and digest == "00000000"

    def test_content_md5(self):
        assert extract_content_checksum({"content-md5": self.EMPTY_MD5_B64}) == \
            ("md5", self.EMPTY_MD5_HEX)

    def test_rfc3230_digest(self):
        assert extract_content_checksum({"digest": "md5=" + self.EMPTY_MD5_B64}) == \
            ("md5", self.EMPTY_MD5_HEX)

    def test_rfc9530_repr_digest_strips_sf_binary_colons(self):
        sha256 = "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="
        algo, digest = extract_content_checksum(
            {"repr-digest": "sha-256=:{}:".format(sha256)})
        assert algo == "sha256" and len(digest) == 64

    def test_amz_checksum(self):
        sha256 = "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="
        algo, _ = extract_content_checksum({"x-amz-checksum-sha256": sha256})
        assert algo == "sha256"

    def test_bare_hex_etag_is_treated_as_md5(self):
        assert extract_content_checksum({"etag": '"%s"' % self.EMPTY_MD5_HEX}) == \
            ("md5", self.EMPTY_MD5_HEX)

    def test_multipart_etag_is_rejected(self):
        """A "-N" suffix means md5-of-md5s, which never equals the file's own md5."""
        assert extract_content_checksum(
            {"etag": '"%s-3"' % self.EMPTY_MD5_HEX}) == (None, None)

    def test_opaque_and_weak_etags_are_rejected(self):
        assert extract_content_checksum({"etag": '"abc-123"'}) == (None, None)
        assert extract_content_checksum(
            {"etag": 'W/"%s"' % self.EMPTY_MD5_HEX}) == (None, None)

    def test_wrong_length_digest_is_rejected(self):
        """Guards against treating some other base64 blob as a digest."""
        assert extract_content_checksum({"content-md5": "dG9vIHNob3J0"}) == (None, None)

    def test_malformed_base64_is_rejected(self):
        assert extract_content_checksum({"content-md5": "not!valid!base64"}) == (None, None)

    def test_content_encoded_response_yields_no_checksum(self):
        """
        A digest on an encoded response covers the *encoded* bytes while the file
        lands decoded, so gating on it would fail every correct download. This is the
        gzip decompressive-transcoding case.
        """
        assert extract_content_checksum(
            {"content-md5": self.EMPTY_MD5_B64, "content-encoding": "gzip"}) == (None, None)

    def test_identity_encoding_is_not_treated_as_encoded(self):
        assert extract_content_checksum(
            {"content-md5": self.EMPTY_MD5_B64, "content-encoding": "identity"})[0] == "md5"


class TestHashCheckCommandIsValidShell:
    """
    The gate is emitted into localization.sh and runs under `set -e` on the compute
    node, so it has to be syntactically valid bash and must not carry a #DEBUG_OMIT
    marker (debug.sh strips those lines when regenerating a runnable script).
    """

    def _gate(self, algorithm, digest):
        h = StringLiteral("x", check_hash=True)
        h.localized_path = "'/dest dir'/'data.bam'"   # handlers embed quotes like this
        h.content_checksum = (algorithm, digest)
        return h._hash_check_command()

    @pytest.mark.parametrize("algorithm,digest", [
        ("md5", "d41d8cd98f00b204e9800998ecf8427e"),
        ("sha1", "da39a3ee5e6b4b0d3255bfef95601890afd80709"),
        ("sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
        ("crc32c", "00000000"),
    ])
    def test_gate_is_valid_bash(self, algorithm, digest):
        import subprocess as sp
        gate = self._gate(algorithm, digest)
        assert len(gate) == 1
        r = sp.run(["bash", "-n"], input=gate[0], text=True, capture_output=True)
        assert r.returncode == 0, r.stderr
        assert "#DEBUG_OMIT" not in gate[0]

    def test_no_gate_without_the_flag(self):
        h = StringLiteral("x")
        h.content_checksum = ("md5", "d41d8cd98f00b204e9800998ecf8427e")
        assert h._hash_check_command() == []

    def test_no_gate_without_a_checksum(self):
        h = StringLiteral("x", check_hash=True)
        h.content_checksum = (None, None)
        assert h._hash_check_command() == []

    def test_no_gate_when_handler_never_probed(self):
        """Handlers that do no HTTP probe have no content_checksum attribute at all."""
        assert StringLiteral("x", check_hash=True)._hash_check_command() == []


# ---------------------------------------------------------------------------
# stream handlers: stale-FIFO guard
# ---------------------------------------------------------------------------

class TestStreamHandlerStaleFifoGuard:
    """
    Each *Stream handler creates a FIFO at `dest`, so it must first clear anything
    already there — a leftover FIFO from a previous attempt would otherwise make
    `mkfifo` fail under `set -e` and abort localization.

    HandleAWSURLStream had this guard written as an f-string:

        f"if [[ -e {0} ]]; then rm {0}; fi".format(dest)

    The f-string is evaluated before .format() runs, so `{0}` became the literal `0`
    and the emitted line was `if [[ -e 0 ]]; then rm 0; fi` — testing and removing a
    file named "0" in the cwd instead of the FIFO. Parameterised over every stream
    handler so the same slip cannot reappear in a sibling.
    """

    DEST = "/mnt/nfs/jobs/1/inputs/sample.bam"

    def _command(self, cls):
        """Build the localization command without running any handler __init__."""
        handler = cls.__new__(cls)          # bypass network-dependent __init__
        handler.path = "s3://bucket/sample.bam"
        handler.url = "https://example.com/sample.bam"
        handler.uri = "drs://dg.4dfc:abc-123"
        handler.localized_path = self.DEST
        handler.command_env_str = ""
        handler.s3_extra_args_str = ""
        handler.token_flag = ""
        handler.token = None
        handler.rp_string = ""
        # size/hash/check_hash are read-only properties, so seed their backing fields
        handler._size = 1024
        handler._hash = "d41d8cd98f00b204e9800998ecf8427e"
        handler._check_hash = False
        handler.extra_args = {}
        return handler.localization_command(self.DEST)

    @pytest.mark.parametrize("cls", [
        HandleGSURLStream,
        HandleAWSURLStream,
        HandleGDCHTTPURLStream,
        HandleDRSURIStream,
    ])
    def test_guard_references_the_real_destination(self, cls):
        cmd = self._command(cls)
        assert "if [[ -e {0} ]]".format(self.DEST) in cmd, \
            "stale-FIFO guard does not reference dest:\n" + cmd

    @pytest.mark.parametrize("cls", [
        HandleGSURLStream,
        HandleAWSURLStream,
        HandleGDCHTTPURLStream,
        HandleDRSURIStream,
    ])
    def test_guard_does_not_degenerate_to_a_literal_zero(self, cls):
        cmd = self._command(cls)
        assert "if [[ -e 0 ]]" not in cmd
        assert "rm 0;" not in cmd

    @pytest.mark.parametrize("cls", [
        HandleGSURLStream,
        HandleAWSURLStream,
        HandleGDCHTTPURLStream,
        HandleDRSURIStream,
    ])
    def test_emitted_command_is_valid_bash(self, cls):
        import subprocess as sp
        cmd = self._command(cls)
        r = sp.run(["bash", "-n"], input=cmd, text=True, capture_output=True)
        assert r.returncode == 0, r.stderr + "\n---\n" + cmd
