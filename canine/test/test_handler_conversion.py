"""
Conversion contract for the URL handlers.

The rule from the design doc is that each rewrite keeps the surrounding shell
character-for-character and swaps only the download line. These commands are known-good
on cluster nodes, so the surrounding idioms -- the quoting preamble, the directory guard,
the md5 gate -- are the template rather than something to tidy up.

`parallel_download=False` produces exactly the pre-conversion command, which makes it
usable as the baseline to diff against.
"""

import base64
import hashlib
import os
import subprocess

import pytest

from unittest.mock import patch

from canine.localization import file_handlers as fh
from pdl_server import Server

MIB = 1024 * 1024
SIZE = 50 * MIB
EMPTY_MD5_B64 = "1B2M2Y8AsgTpgAmY7PhCfg=="
EMPTY_MD5_HEX = "d41d8cd98f00b204e9800998ecf8427e"

DEST = "/mnt/rwdisks/canine-abc/inputs/sample.bam"

URLS = {
    "other": "https://example.org/data/sample.bam",
    "gcs_signed": "https://storage.googleapis.com/bkt/data/sample.bam?X-Goog-Signature=aa",
}


def headers_with(*extra):
    lines = [b"HTTP/1.1 200 OK", b"Content-Length: " + str(SIZE).encode()]
    lines += [item.encode() if isinstance(item, str) else item for item in extra]
    return b"\r\n".join(lines) + b"\r\n\r\n"


def emit(url, raw_headers=None, dest=DEST, **kwargs):
    class Fake:
        stdout = raw_headers if raw_headers is not None else headers_with(
            "Content-MD5: " + EMPTY_MD5_B64)

    with patch("os.path.exists", return_value=False), \
         patch("canine.localization.file_handlers.subprocess.run", return_value=Fake()):
        handler = fh.get_file_handler(url, **kwargs)
    return handler.localization_command(dest)


def bash_ok(script):
    return subprocess.run(["bash", "-n"], input=script, text=True, capture_output=True)


def debug_sh_transform(script):
    lines = [line for line in script.split("\n") if "#DEBUG_OMIT" not in line]
    if lines:
        lines[-1] = lines[-1].split(" - <<")[0]
    return "\n".join(lines)


ALL_URLS = pytest.mark.parametrize("kind", sorted(URLS))


# ---------------------------------------------------------------------------
# the surrounding shell is unchanged
# ---------------------------------------------------------------------------

class TestSurroundingShellUnchanged:

    @ALL_URLS
    def test_directory_guard_is_character_identical(self, kind):
        """
        The guard is the known-good idiom every handler shares, including the trailing
        `|| :` that keeps a failing test from aborting the script under set -e.
        """
        expected = "[ ! -d /mnt/rwdisks/canine-abc/inputs ] && " \
                   "mkdir -p /mnt/rwdisks/canine-abc/inputs || :; "
        parallel = emit(URLS[kind], check_md5=True)
        legacy = emit(URLS[kind], check_md5=True, parallel_download=False)
        assert parallel.startswith(expected)
        assert legacy.startswith(expected)

    @ALL_URLS
    def test_only_the_download_line_differs(self, kind):
        """
        The conversion contract, checked directly. With verification off, the two forms
        should differ in exactly one place: how the bytes are fetched.
        """
        parallel = emit(URLS[kind], check_md5=False).split("\n")
        legacy = emit(URLS[kind], check_md5=False, parallel_download=False).split("\n")

        assert len(legacy) == 1, "the legacy form should still be a single line"
        guard = "[ ! -d /mnt/rwdisks/canine-abc/inputs ] && " \
                "mkdir -p /mnt/rwdisks/canine-abc/inputs || :; "
        assert legacy[0].startswith(guard) and parallel[0].startswith(guard)
        # everything after the guard is the download, and only that changed
        assert legacy[0][len(guard):].startswith("curl -C - -o")
        assert parallel[0][len(guard):].startswith("K9_PDL=")

    @ALL_URLS
    def test_localized_path_still_embeds_quotes(self, kind):
        """
        localized_path is built by quoting the directory and basename separately and then
        joining. Downstream code depends on that exact string, so the conversion must not
        "fix" it.
        """
        class Fake:
            stdout = headers_with()

        with patch("os.path.exists", return_value=False), \
             patch("canine.localization.file_handlers.subprocess.run", return_value=Fake()):
            handler = fh.get_file_handler(URLS[kind])

        # each component is quoted independently, and shlex.quote only adds quotes where
        # they are actually needed -- so the directory is quoted here and the basename is
        # not, and the result is a path that is only valid *after* shell parsing
        handler.localization_command("/mnt/my dir/sample.bam")
        assert handler.localized_path == "'/mnt/my dir'/sample.bam"

        handler.localization_command("/mnt/plain/my sample.bam")
        assert handler.localized_path == "/mnt/plain/'my sample.bam'"

    @ALL_URLS
    def test_a_quoted_dest_resolves_correctly_through_the_shell(self, kind, tmp_path):
        """
        Checks the quoting by using it rather than by matching the string: the emitted
        command has to put the file exactly where dest said, spaces and all.
        """
        payload = os.urandom(64 * 1024)
        directory = tmp_path / "dir with spaces"
        directory.mkdir()
        dest = str(directory / "my sample.bam")

        with Server(payload) as server:
            with patch("os.path.exists", return_value=False):
                handler = fh.get_file_handler(server.url("sample.bam"),
                                              parallel_download=False)
            script = "#!/bin/bash\nset -e\n" + handler.localization_command(dest) + "\n"
            result = subprocess.run(["bash", "-e", "-c", script], capture_output=True,
                                    text=True, timeout=120)

        assert result.returncode == 0, result.stderr
        assert open(dest, "rb").read() == payload

    @ALL_URLS
    def test_opt_out_is_byte_for_byte_the_legacy_command(self, kind):
        legacy = emit(URLS[kind], parallel_download=False, check_md5=False)
        assert legacy == (
            "[ ! -d /mnt/rwdisks/canine-abc/inputs ] && "
            "mkdir -p /mnt/rwdisks/canine-abc/inputs || :; "
            "curl -C - -o /mnt/rwdisks/canine-abc/inputs/sample.bam '{}'".format(
                URLS[kind])
        )


# ---------------------------------------------------------------------------
# when the parallel path is declined
# ---------------------------------------------------------------------------

class TestParallelDownloadIsDeclined:

    @pytest.mark.parametrize("connections", [0, 1])
    def test_low_connection_counts_use_the_legacy_command(self, connections):
        """
        Emitted directly rather than invoking the downloader only for it to fall back:
        the opt-out should cost no interpreter startup.
        """
        script = emit(URLS["other"], download_connections=connections, check_md5=False)
        assert "K9_PDL" not in script
        assert "curl -C - -o" in script

    def test_ftp_uses_the_legacy_command(self):
        """ftp is not rangeable, so the fallback is a foregone conclusion."""
        script = emit("ftp://ftp.example.org/data/sample.bam", check_md5=False)
        assert "K9_PDL" not in script
        assert "curl -C - -o" in script

    def test_explicit_opt_out(self):
        script = emit(URLS["other"], parallel_download=False, check_md5=False)
        assert "K9_PDL" not in script


# ---------------------------------------------------------------------------
# verification is done once, by whichever layer can do it
# ---------------------------------------------------------------------------

class TestVerificationIsNotDuplicated:

    def test_md5_is_handed_to_the_downloader_and_the_gate_is_omitted(self):
        """
        The downloader verifies before writing the completion marker and deletes a
        corrupt file itself. Emitting the shell gate as well would re-read the whole
        object for no additional guarantee -- a second full pass over a 50 GB input.
        """
        script = emit(URLS["other"], check_md5=True)
        assert "--check-md5 " + EMPTY_MD5_HEX in script
        assert "md5sum" not in script, "shell gate emitted on top of --check-md5"

    def test_an_algorithm_the_downloader_cannot_check_keeps_the_shell_gate(self):
        """
        A composite GCS object advertises crc32c only. The downloader implements md5 and
        multipart ETags, so this one has to be checked by the emitted shell.
        """
        script = emit(URLS["gcs_signed"], check_md5=True,
                      raw_headers=headers_with("x-goog-hash: crc32c=RXo1Ng=="))
        assert "--check-md5" not in script
        assert "google_crc32c" in script, "crc32c gate not emitted"

    def test_no_verification_requested_means_neither(self):
        script = emit(URLS["other"], check_md5=False)
        assert "--check-md5" not in script
        assert "md5sum" not in script

    def test_no_advertised_checksum_means_neither(self):
        script = emit(URLS["other"], check_md5=True, raw_headers=headers_with())
        assert "--check-md5" not in script
        assert "md5sum" not in script

    def test_legacy_path_keeps_its_gate(self):
        """Nothing else checks on the legacy path, so the gate must stay."""
        script = emit(URLS["other"], check_md5=True, parallel_download=False)
        assert "md5sum" in script


# ---------------------------------------------------------------------------
# emitted-script contract
# ---------------------------------------------------------------------------

class TestEmittedScriptContract:

    VARIANTS = [
        dict(check_md5=True),
        dict(check_md5=False),
        dict(check_md5=True, parallel_download=False),
        dict(check_md5=True, download_connections=16, download_min_chunk=4 * MIB),
        dict(check_md5=True, download_connections=1),
    ]

    @ALL_URLS
    @pytest.mark.parametrize("kwargs", VARIANTS)
    def test_is_valid_bash(self, kind, kwargs):
        script = emit(URLS[kind], **kwargs)
        result = bash_ok(script)
        assert result.returncode == 0, result.stderr + "\n---\n" + script

    @ALL_URLS
    @pytest.mark.parametrize("kwargs", VARIANTS)
    def test_survives_the_debug_sh_transform(self, kind, kwargs):
        script = emit(URLS[kind], **kwargs)
        assert "#DEBUG_OMIT" not in script
        result = bash_ok(debug_sh_transform(script))
        assert result.returncode == 0, result.stderr

    @ALL_URLS
    def test_contains_no_heredoc(self, kind):
        assert "<<" not in emit(URLS[kind], check_md5=True)

    @ALL_URLS
    def test_options_reach_the_command(self, kind):
        script = emit(URLS[kind], check_md5=True, download_connections=12,
                      download_min_chunk=8 * MIB)
        assert "--connections 12" in script
        assert "--min-chunk {}".format(8 * MIB) in script

    def test_signed_url_query_string_is_not_split(self):
        """A signed URL's query carries `&`, which unquoted would background the command."""
        script = emit(URLS["gcs_signed"], check_md5=True)
        assert bash_ok(script).returncode == 0


# ---------------------------------------------------------------------------
# end to end
# ---------------------------------------------------------------------------

class TestEmittedCommandActuallyDownloads:

    def _run(self, script, cwd=None):
        return subprocess.run(["bash", "-e", "-c", script], capture_output=True,
                              text=True, timeout=300, cwd=cwd)

    def test_handler_command_downloads_and_verifies(self, tmp_path):
        """
        The whole chain: real handler, real header probe against a live server, emitted
        command run through bash, byte-exact result.
        """
        payload = os.urandom(4 * MIB + 321)
        digest = hashlib.md5(payload).hexdigest()
        with Server(payload) as server:
            server.state.content_md5 = base64.b64encode(
                hashlib.md5(payload).digest()).decode()
            with patch("os.path.exists", return_value=False):
                handler = fh.get_file_handler(
                    server.url("sample.bam"), check_md5=True,
                    download_min_chunk=MIB, download_connections=4,
                )
            assert handler.size == len(payload)
            assert handler.content_checksum == ("md5", digest)

            dest = str(tmp_path / "out" / "sample.bam")
            script = "#!/bin/bash\nset -e\n" + handler.localization_command(dest) + "\n"
            result = self._run(script)

        assert result.returncode == 0, result.stdout + result.stderr
        assert hashlib.md5(open(dest, "rb").read()).hexdigest() == digest

    def test_corrupt_download_is_rejected_with_exit_one(self, tmp_path):
        """
        A wrong checksum must delete the file and fail. Exit 1 is do-not-retry as far as
        canine's entrypoint is concerned, which is right for a corrupt object.
        """
        payload = os.urandom(2 * MIB)
        with Server(payload) as server:
            server.state.content_md5 = base64.b64encode(b"\0" * 16).decode()
            with patch("os.path.exists", return_value=False):
                handler = fh.get_file_handler(
                    server.url("sample.bam"), check_md5=True,
                    download_min_chunk=MIB, download_connections=2,
                )
            dest = str(tmp_path / "sample.bam")
            script = "#!/bin/bash\nset -e\n" + handler.localization_command(dest) + "\n"
            result = self._run(script)

        assert result.returncode != 0
        assert not os.path.exists(dest)

    def test_legacy_opt_out_downloads_too(self, tmp_path):
        """The opt-out has to remain a working path, not just a well-formed string."""
        payload = os.urandom(MIB + 5)
        with Server(payload) as server:
            with patch("os.path.exists", return_value=False):
                handler = fh.get_file_handler(
                    server.url("sample.bam"), parallel_download=False,
                )
            dest = str(tmp_path / "sample.bam")
            script = "#!/bin/bash\nset -e\n" + handler.localization_command(dest) + "\n"
            result = self._run(script)

        assert result.returncode == 0, result.stderr
        assert open(dest, "rb").read() == payload

    def test_a_range_ignoring_server_still_produces_the_file(self, tmp_path):
        """
        The in-script fallback: the mandatory probe declines the object and the legacy
        command finishes the job.
        """
        payload = os.urandom(3 * MIB)
        with Server(payload) as server:
            with patch("os.path.exists", return_value=False):
                handler = fh.get_file_handler(
                    server.url("sample.bam"), download_min_chunk=MIB,
                    download_connections=4,
                )
            server.state.support_range = False
            dest = str(tmp_path / "sample.bam")
            script = "#!/bin/bash\nset -e\n" + handler.localization_command(dest) + "\n"
            result = self._run(script)

        assert result.returncode == 0, result.stdout + result.stderr
        assert open(dest, "rb").read() == payload

    def test_rerun_transfers_nothing(self, tmp_path):
        payload = os.urandom(3 * MIB + 7)
        with Server(payload) as server:
            server.state.content_md5 = base64.b64encode(
                hashlib.md5(payload).digest()).decode()
            with patch("os.path.exists", return_value=False):
                handler = fh.get_file_handler(
                    server.url("sample.bam"), check_md5=True,
                    download_min_chunk=MIB, download_connections=4,
                )
            dest = str(tmp_path / "sample.bam")
            script = "#!/bin/bash\nset -e\n" + handler.localization_command(dest) + "\n"
            assert self._run(script).returncode == 0
            before = server.state.snapshot()["sent"]
            assert self._run(script).returncode == 0
            after = server.state.snapshot()["sent"]

        assert after == before, "re-run moved {} bytes".format(after - before)


# ---------------------------------------------------------------------------
# HandleGDCHTTPURL and HandleDRSURI
# ---------------------------------------------------------------------------

DRS_URI = "drs://dg.4dfc:abc-123"
DRS_MD5 = "d41d8cd98f00b204e9800998ecf8427e"
GDC_UUID = "550e8400-e29b-41d4-a716-446655440000"
GDC_URL = "https://api.gdc.cancer.gov/data/" + GDC_UUID


def drs_handler(access_url="https://storage.googleapis.com/b/o?sig=x", **kwargs):
    class FakeResponse:
        status_code = 200
        ok = True

        def json(self):
            return {"size": SIZE, "fileName": "sample.bam",
                    "hashes": {"md5": DRS_MD5},
                    "accessUrl": {"url": access_url}}

    class FakeSession:
        def post(self, url, headers=None, json=None):
            return FakeResponse()

    with patch("canine.localization.file_handlers.gcp_auth_session",
               return_value=FakeSession()):
        return fh.HandleDRSURI(DRS_URI, **kwargs)


def gdc_handler(**kwargs):
    """
    A GDC handler on its API fallback path, i.e. with no DRS object -- otherwise it just
    delegates to HandleDRSURI.
    """
    handler = fh.HandleGDCHTTPURL.__new__(fh.HandleGDCHTTPURL)
    handler.extra_args = dict(kwargs)
    handler.check_hash = fh.FileType._resolve_check_hash(kwargs)
    handler.parallel_download = bool(kwargs.get("parallel_download", True))
    handler.download_connections = int(kwargs.get(
        "download_connections", fh.DEFAULT_DOWNLOAD_CONNECTIONS))
    handler.download_min_chunk = int(kwargs.get(
        "download_min_chunk", fh.DEFAULT_DOWNLOAD_MIN_CHUNK))
    handler.drs_obj = None
    handler.token = kwargs.get("token")
    handler.token_flag = (
        '--header  "X-Auth-Token: {}"'.format(handler.token)
        if handler.token is not None else ''
    )
    handler.url = GDC_URL
    handler.path = "sample.bam"
    handler.localized_path = "sample.bam"
    handler._size = SIZE
    handler._hash = DRS_MD5
    return handler


class TestGDCConversion:

    def test_token_travels_as_a_request_header(self):
        script = gdc_handler(token="s3cr3t", check_md5=True).localization_command(DEST)
        assert "--header 'X-Auth-Token: s3cr3t'" in script

    def test_no_token_emits_no_header(self):
        script = gdc_handler(check_md5=True).localization_command(DEST)
        assert "--header" not in script

    def test_token_exposure_is_not_widened(self):
        """
        The token is already interpolated into the emitted command today, so appearing
        once in the header argument is not new exposure -- but it must not also be
        duplicated into extra places.
        """
        script = gdc_handler(token="s3cr3t", check_md5=True).localization_command(DEST)
        # once as the --header value, once inside the --legacy-cmd argument, and once in
        # the else branch that runs the legacy command directly
        assert script.count("s3cr3t") == 3

    def test_content_md5_is_handed_to_the_downloader(self):
        """
        For this handler self.hash IS the content md5, unlike a plain URL handler where it
        is a URL-derived identity.
        """
        script = gdc_handler(check_md5=True).localization_command(DEST)
        assert "--check-md5 " + DRS_MD5 in script
        assert "md5sum" not in script

    def test_legacy_path_keeps_the_original_gate(self):
        script = gdc_handler(token="t", check_md5=True,
                            parallel_download=False).localization_command(DEST)
        assert "md5sum" in script and DRS_MD5 in script
        assert '--header  "X-Auth-Token: t"' in script, "legacy curl flag must be intact"

    def test_opt_out_is_the_original_command(self):
        script = gdc_handler(token="t", check_md5=False,
                            parallel_download=False).localization_command(DEST)
        assert script == (
            "[ ! -d /mnt/rwdisks/canine-abc/inputs ] && "
            "mkdir -p /mnt/rwdisks/canine-abc/inputs || :; "
            'curl -C - -o /mnt/rwdisks/canine-abc/inputs/sample.bam '
            '--header  "X-Auth-Token: t" \'{}\''.format(GDC_URL)
        )

    @pytest.mark.parametrize("kwargs", [
        dict(check_md5=True), dict(check_md5=False), dict(token="t", check_md5=True),
        dict(token="t", check_md5=True, parallel_download=False),
    ])
    def test_is_valid_bash(self, kwargs):
        script = gdc_handler(**kwargs).localization_command(DEST)
        result = bash_ok(script)
        assert result.returncode == 0, result.stderr + "\n---\n" + script
        assert "#DEBUG_OMIT" not in script
        assert bash_ok(debug_sh_transform(script)).returncode == 0

    def test_delegates_to_drs_when_a_drs_object_resolved(self):
        handler = gdc_handler(check_md5=True)
        handler.drs_obj = drs_handler(check_md5=True)
        script = handler.localization_command(DEST)
        assert "signed_url" in script, "should have delegated to the DRS handler"


class TestDRSConversion:

    def test_resolver_snippet_is_unchanged(self):
        """
        The resolution step is reused verbatim; only its surroundings changed. Pinning the
        exact string keeps a future edit from quietly altering a known-good command.
        """
        import json as _json
        data_str = _json.dumps({"url": DRS_URI, "fields": ["accessUrl"]})
        expected = (
            'curl -S -X POST --url "{}" '
            '-H "authorization: Bearer $(gcloud auth print-access-token)" '
            "-H \"content-type: application/json\" --data '{}' | "
            "python3 -c 'import json,sys; print(json.load(sys.stdin)[\"accessUrl\"][\"url\"])'"
        ).format(fh.HandleDRSURI.drs_resolver, data_str)

        script = drs_handler(check_md5=True).localization_command(DEST)
        first = script.split("\n")[0]
        assert first == "export signed_url=$({})".format(expected)

    def test_the_same_snippet_is_reused_as_the_refresh_command(self):
        """
        A signed URL can expire mid-transfer. Re-minting it lets the download resume in
        place instead of starting over, and the command to do that is the one already
        being used to mint it.
        """
        script = drs_handler(check_md5=True).localization_command(DEST)
        assert "--url-refresh-cmd" in script
        assert script.count("drshub.dsde-prod.broadinstitute.org") == 2

    def test_signed_url_is_exported(self):
        """
        The --legacy-cmd fallback references "$signed_url" and the downloader runs it in
        its own subshell, which would not inherit a plain shell variable -- the fallback
        would curl an empty URL.
        """
        script = drs_handler(check_md5=True).localization_command(DEST)
        assert script.startswith("export signed_url=")

    def test_url_is_passed_as_a_shell_expression(self):
        """
        The URL is not known host-side. Quoting it would pass the literal text
        `$signed_url` as the URL instead of its value.
        """
        script = drs_handler(check_md5=True).localization_command(DEST)
        assert '--url "$signed_url"' in script
        assert "--url '$signed_url'" not in script

    def test_content_md5_is_handed_to_the_downloader(self):
        script = drs_handler(check_md5=True).localization_command(DEST)
        assert "--check-md5 " + DRS_MD5 in script
        assert "md5sum" not in script

    def test_no_verification_requested_means_neither(self):
        script = drs_handler(check_md5=False).localization_command(DEST)
        assert "--check-md5" not in script and "md5sum" not in script

    def test_legacy_path_is_the_original_command(self):
        script = drs_handler(check_md5=False,
                            parallel_download=False).localization_command(DEST)
        lines = script.split("\n")
        assert len(lines) == 2
        assert lines[1] == (
            "[ ! -d /mnt/rwdisks/canine-abc/inputs ] && "
            "mkdir -p /mnt/rwdisks/canine-abc/inputs || :; "
            'curl -C - -o /mnt/rwdisks/canine-abc/inputs/sample.bam "$signed_url"'
        )

    @pytest.mark.parametrize("kwargs", [
        dict(check_md5=True), dict(check_md5=False),
        dict(check_md5=True, parallel_download=False),
        dict(check_md5=True, download_connections=16),
    ])
    def test_is_valid_bash(self, kwargs):
        script = drs_handler(**kwargs).localization_command(DEST)
        result = bash_ok(script)
        assert result.returncode == 0, result.stderr + "\n---\n" + script
        assert "#DEBUG_OMIT" not in script
        assert bash_ok(debug_sh_transform(script)).returncode == 0

    def test_no_heredoc(self):
        assert "<<" not in drs_handler(check_md5=True).localization_command(DEST)


class TestDRSEndToEnd:
    """
    Runs the emitted DRS command for real: a local server stands in for both the resolver
    (printing an access URL) and the object store.
    """

    def _script_for(self, tmp_path, server, payload, resolver_script, **kwargs):
        handler = drs_handler(access_url=server.url("sample.bam"), **kwargs)
        handler._size = len(payload)
        dest = str(tmp_path / "sample.bam")
        command = handler.localization_command(dest)
        # swap the real drshub call for a local stand-in, keeping the shell shape intact
        first, rest = command.split("\n", 1)
        assert first.startswith("export signed_url=$(")
        command = "export signed_url=$({})\n{}".format(resolver_script, rest)
        return dest, "#!/bin/bash\nset -e\n" + command + "\n"

    def test_downloads_via_the_resolved_url(self, tmp_path):
        payload = os.urandom(3 * MIB + 5)
        with Server(payload) as server:
            resolver = "printf '%s' {}".format(server.url("sample.bam"))
            dest, script = self._script_for(tmp_path, server, payload, resolver,
                                            check_md5=False)
            result = subprocess.run(["bash", "-e", "-c", script], capture_output=True,
                                    text=True, timeout=300)
        assert result.returncode == 0, result.stdout + result.stderr
        assert open(dest, "rb").read() == payload

    def test_legacy_fallback_can_see_the_exported_url(self, tmp_path):
        """
        The concrete consequence of exporting: with no usable downloader the fallback has
        to still know the URL.
        """
        payload = os.urandom(MIB)
        with Server(payload) as server:
            resolver = "printf '%s' {}".format(server.url("sample.bam"))
            dest, script = self._script_for(tmp_path, server, payload, resolver,
                                            check_md5=False)
            # make every candidate unresolvable so the legacy branch is taken
            script = script.replace(fh._pdl_installed_path(), "/nonexistent/pdl.py")
            script = script.replace('"${CANINE_ROOT:-}/parallel_download.py"',
                                    '"/nonexistent/staged.py"')
            result = subprocess.run(["bash", "-e", "-c", script], capture_output=True,
                                    text=True, timeout=300)
        assert result.returncode == 0, result.stdout + result.stderr
        assert open(dest, "rb").read() == payload


# ---------------------------------------------------------------------------
# HandleAWSURL
# ---------------------------------------------------------------------------

S3_PATH = "s3://mybucket/data/sample.bam"
SINGLE_PART_ETAG = "d41d8cd98f00b204e9800998ecf8427e"
MULTIPART_ETAG = "abc123def456abc123def456abc12345-800"

MULTIPART_HEADERS = {
    "ContentLength": 50 * 1024 * MIB,
    "ETag": '"{}"'.format(MULTIPART_ETAG),
    "PartsCount": 800,
    "PartLength": 64 * MIB,
}
SINGLE_PART_HEADERS = {
    "ContentLength": MIB,
    "ETag": '"{}"'.format(SINGLE_PART_ETAG),
}


def aws_handler(headers=None, **kwargs):
    """
    Build the handler without its __init__, which shells out to `aws s3api head-object`.
    Every field the emitted command depends on is set the way __init__ would.
    """
    handler = fh.HandleAWSURL.__new__(fh.HandleAWSURL)
    handler.extra_args = dict(kwargs)
    handler.check_hash = fh.FileType._resolve_check_hash(kwargs)
    handler.parallel_download = bool(kwargs.get("parallel_download", True))
    handler.download_connections = int(kwargs.get(
        "download_connections", fh.DEFAULT_DOWNLOAD_CONNECTIONS))
    handler.download_min_chunk = int(kwargs.get(
        "download_min_chunk", fh.DEFAULT_DOWNLOAD_MIN_CHUNK))
    handler.path = S3_PATH
    handler.aws_endpoint_url = kwargs.get("aws_endpoint_url")
    handler.command_env = {
        "AWS_ACCESS_KEY_ID": kwargs.get("aws_access_key_id"),
        "AWS_SECRET_ACCESS_KEY": kwargs.get("aws_secret_access_key"),
    }
    handler.command_env_str = " ".join(
        "{}={}".format(k, v) for k, v in handler.command_env.items() if v is not None)
    handler.s3_extra_args = []
    if (handler.command_env["AWS_ACCESS_KEY_ID"] is None
            and handler.command_env["AWS_SECRET_ACCESS_KEY"] is None):
        handler.s3_extra_args += ["--no-sign-request"]
    if handler.aws_endpoint_url is not None:
        handler.s3_extra_args += ["--endpoint-url {}".format(handler.aws_endpoint_url)]
    handler.s3_extra_args_str = " ".join(handler.s3_extra_args)
    handler.headers = dict(headers or SINGLE_PART_HEADERS)
    handler._size = handler.headers["ContentLength"]
    handler._hash = handler.headers["ETag"].replace('"', '')
    return handler


PRIVATE = dict(aws_access_key_id="AKIA", aws_secret_access_key="sk")


class TestAWSUnsafePatternsRemoved:

    def test_stat_based_append_resume_is_gone(self):
        """
        The old command inferred how much was already downloaded from the destination's
        size. That is exactly the assumption that breaks once a file is created at its
        full apparent size upfront: it would see a complete file and skip the download.
        """
        script = aws_handler(MULTIPART_HEADERS, check_md5=True,
                            **PRIVATE).localization_command(DEST)
        assert "stat --printf" not in script
        assert 'bytes=$SZ-' not in script
        assert "SZ=" not in script

    def test_post_hoc_multiprocessing_md5_pass_is_gone(self):
        """
        A multipart ETag is now computed during the transfer, so the separate hashing pass
        that re-read the whole object afterwards is unnecessary.
        """
        script = aws_handler(MULTIPART_HEADERS, check_md5=True,
                            **PRIVATE).localization_command(DEST)
        assert "multiprocessing" not in script
        assert "md5hash=" not in script
        assert "<<" not in script, "the heredoc should be gone entirely"

    def test_legacy_command_overwrites_rather_than_appends(self):
        """
        The fallback must be safe against a preallocated destination. `aws s3 cp`
        overwrites; the old append-with-range did not.
        """
        script = aws_handler(check_md5=False, **PRIVATE).localization_command(DEST)
        assert "aws s3 " in script and " cp " in script
        assert ">> " not in script


class TestAWSPresign:

    def test_private_bucket_presigns_on_the_node(self):
        """
        Credentials live on the VM, not here, so the URL cannot be minted host-side.
        Feeding a presigned URL into the generic ranged-HTTP path gives one code path for
        every source and costs no `aws` process per chunk.
        """
        script = aws_handler(check_md5=False, **PRIVATE).localization_command(DEST)
        assert "aws s3" in script and "presign" in script
        assert '--url "$K9_S3_URL"' in script

    def test_presign_failure_does_not_abort_the_script(self):
        """
        Under set -e an unguarded failure would kill the whole localization; the intended
        outcome is falling through to the per-chunk aws s3api source.
        """
        script = aws_handler(check_md5=False, **PRIVATE).localization_command(DEST)
        presign_line = [l for l in script.split("\n") if "presign" in l][0]
        assert presign_line.rstrip().endswith("|| :)")
        assert bash_ok(script).returncode == 0

    def test_s3_api_fallback_args_are_always_passed(self):
        """
        An empty presign result makes the downloader use its per-chunk aws s3api source,
        which is what covers session-token-only credentials and exotic endpoints.
        """
        script = aws_handler(check_md5=False, **PRIVATE).localization_command(DEST)
        assert "--s3-bucket mybucket" in script
        assert "--s3-key data/sample.bam" in script

    def test_public_bucket_uses_a_plain_url_and_does_not_presign(self):
        """Nothing to sign, so there is nothing to mint on the node."""
        script = aws_handler(check_md5=False).localization_command(DEST)
        assert "presign" not in script
        assert "--url https://mybucket.s3.amazonaws.com/data/sample.bam" in script

    def test_custom_endpoint_uses_path_style(self):
        script = aws_handler(check_md5=False,
                            aws_endpoint_url="https://minio.local").localization_command(DEST)
        assert "--url https://minio.local/mybucket/data/sample.bam" in script

    def test_credentials_are_exported_to_the_downloader(self):
        """
        The downloader spawns `aws` for the per-chunk fallback, so it has to inherit the
        keys. Same env-prefix idiom the handler already uses for its own aws calls.
        """
        script = aws_handler(check_md5=False, **PRIVATE).localization_command(DEST)
        invocation = [l for l in script.split("\n") if "$K9_PDL_RUN" in l][0]
        assert "AWS_ACCESS_KEY_ID=AKIA" in invocation
        assert "AWS_SECRET_ACCESS_KEY=sk" in invocation


class TestAWSVerification:

    def test_multipart_uses_the_etag_and_part_length(self):
        """
        Chunk boundaries snap to whole parts so the md5-of-md5s can be computed during the
        transfer rather than by a second full read.
        """
        script = aws_handler(MULTIPART_HEADERS, check_md5=True,
                            **PRIVATE).localization_command(DEST)
        assert "--check-etag " + MULTIPART_ETAG in script
        assert "--part-length {}".format(64 * MIB) in script
        assert "--check-md5" not in script
        assert "md5sum" not in script

    def test_single_part_etag_is_the_whole_file_md5(self):
        script = aws_handler(SINGLE_PART_HEADERS, check_md5=True,
                            **PRIVATE).localization_command(DEST)
        assert "--check-md5 " + SINGLE_PART_ETAG in script
        assert "--check-etag" not in script

    def test_multipart_without_a_part_length_falls_back_to_md5(self):
        """
        Without the part length there is nothing to compare an md5-of-md5s against, so
        asking for an ETag check would request something impossible.
        """
        headers = dict(MULTIPART_HEADERS)
        del headers["PartLength"]
        script = aws_handler(headers, check_md5=True, **PRIVATE).localization_command(DEST)
        assert "--check-etag" not in script

    def test_no_verification_requested_means_none(self):
        script = aws_handler(MULTIPART_HEADERS, check_md5=False,
                            **PRIVATE).localization_command(DEST)
        assert "--check-etag" not in script and "--check-md5" not in script
        assert "md5sum" not in script


class TestAWSEmittedScriptContract:

    VARIANTS = [
        dict(check_md5=True), dict(check_md5=False),
        dict(check_md5=True, parallel_download=False),
        dict(check_md5=True, download_connections=1),
        dict(check_md5=True, aws_endpoint_url="https://minio.local"),
    ]

    @pytest.mark.parametrize("headers", [SINGLE_PART_HEADERS, MULTIPART_HEADERS])
    @pytest.mark.parametrize("kwargs", VARIANTS)
    @pytest.mark.parametrize("private", [True, False])
    def test_is_valid_bash(self, headers, kwargs, private):
        options = dict(kwargs)
        if private:
            options.update(PRIVATE)
        script = aws_handler(headers, **options).localization_command(DEST)
        result = bash_ok(script)
        assert result.returncode == 0, result.stderr + "\n---\n" + script
        assert "#DEBUG_OMIT" not in script
        assert bash_ok(debug_sh_transform(script)).returncode == 0

    def test_directory_guard_is_preserved(self):
        script = aws_handler(check_md5=True, **PRIVATE).localization_command(DEST)
        assert script.split("\n")[0] == (
            "[ ! -d /mnt/rwdisks/canine-abc/inputs ] && "
            "mkdir -p /mnt/rwdisks/canine-abc/inputs || :"
        )

    def test_multiword_extra_args_stay_one_argument(self):
        script = aws_handler(check_md5=False,
                            aws_endpoint_url="https://minio.local").localization_command(DEST)
        assert "--s3-extra-args '--no-sign-request --endpoint-url https://minio.local'" \
            in script


# ---------------------------------------------------------------------------
# localizer-level defaults and HandleGSURL tuning
# ---------------------------------------------------------------------------

class TestLocalizerDefaults:
    """
    Options set once on the localizer have to reach every handler it constructs. Both
    internal get_file_handler call sites previously passed only project and token, with a
    TODO noting the gap.
    """

    def _defaults(self, **kwargs):
        """Calls the real implementation, so the test cannot drift from the code."""
        from canine.localization.base import AbstractLocalizer
        return AbstractLocalizer.build_file_handler_defaults(
            kwargs.get("parallel_download", True),
            kwargs.get("download_connections", fh.DEFAULT_DOWNLOAD_CONNECTIONS),
            kwargs.get("download_min_chunk", fh.DEFAULT_DOWNLOAD_MIN_CHUNK),
            kwargs.get("check_hash"),
        )

    def test_check_hash_is_omitted_when_not_set(self):
        """
        Including check_hash=False by default would look like a contradictory flag next to
        an input's own check_md5=True and raise, so an unset value must not be forwarded.
        """
        assert "check_hash" not in self._defaults()

    def test_check_hash_is_forwarded_when_set(self):
        assert self._defaults(check_hash=True)["check_hash"] is True
        assert self._defaults(check_hash=False)["check_hash"] is False

    def test_download_options_are_forwarded(self):
        defaults = self._defaults(parallel_download=False, download_connections=12,
                                  download_min_chunk=4 * MIB)
        assert defaults["parallel_download"] is False
        assert defaults["download_connections"] == 12
        assert defaults["download_min_chunk"] == 4 * MIB

    def test_handler_honors_forwarded_defaults(self):
        """The end-to-end point: the defaults actually change the emitted command."""
        defaults = self._defaults(download_connections=12, download_min_chunk=4 * MIB)
        script = emit(URLS["other"], check_md5=True, **defaults)
        assert "--connections 12" in script
        assert "--min-chunk {}".format(4 * MIB) in script

    def test_a_per_input_value_can_override_a_localizer_default(self):
        defaults = self._defaults(download_connections=12)
        merged = dict(defaults)
        merged["download_connections"] = 2
        script = emit(URLS["other"], check_md5=True, **merged)
        assert "--connections 2" in script

    def test_localizer_signature_accepts_the_options(self):
        """Guards against the kwargs being silently swallowed by **kwargs."""
        import inspect
        from canine.localization.base import AbstractLocalizer
        parameters = inspect.signature(AbstractLocalizer.__init__).parameters
        for name in ("parallel_download", "download_connections",
                     "download_min_chunk", "check_hash"):
            assert name in parameters, name


def gs_handler(is_dir=False, **kwargs):
    handler = fh.HandleGSURL.__new__(fh.HandleGSURL)
    handler.extra_args = dict(kwargs)
    handler.check_hash = fh.FileType._resolve_check_hash(kwargs)
    handler.parallel_download = bool(kwargs.get("parallel_download", True))
    handler.download_connections = int(kwargs.get(
        "download_connections", fh.DEFAULT_DOWNLOAD_CONNECTIONS))
    handler.download_min_chunk = int(kwargs.get(
        "download_min_chunk", fh.DEFAULT_DOWNLOAD_MIN_CHUNK))
    handler.path = "gs://bkt/data/sample.bam"
    handler.rp_string = ""
    handler.is_dir = is_dir
    return handler


class TestGSURLTuning:
    """
    This handler is deliberately not converted: gcloud storage cp already does sliced
    downloads and already resumes through the tracker directory and manifest it is passed,
    so there is nothing to replace -- only to size correctly.
    """

    def test_still_uses_gcloud_storage_cp(self):
        script = gs_handler().localization_command(DEST)
        assert "gcloud storage cp" in script

    def test_tracker_dir_and_manifest_are_preserved(self):
        """These are what make gcloud's own transfer resumable; they must not be lost."""
        script = gs_handler().localization_command(DEST)
        assert "CLOUDSDK_STORAGE_TRACKER_DIR=" in script
        assert ".gcloud_manifest" in script

    def test_sliced_download_is_tuned_from_the_same_knobs(self):
        """One set of options governs both paths rather than two that can drift."""
        script = gs_handler(download_connections=12,
                           download_min_chunk=8 * MIB).localization_command(DEST)
        assert "CLOUDSDK_STORAGE_SLICED_OBJECT_DOWNLOAD_THRESHOLD={}".format(8 * MIB) \
            in script
        assert "CLOUDSDK_STORAGE_SLICED_OBJECT_DOWNLOAD_MAX_COMPONENTS=12" in script
        assert "CLOUDSDK_STORAGE_THREAD_COUNT=12" in script

    def test_opt_out_disables_slicing(self):
        script = gs_handler(parallel_download=False).localization_command(DEST)
        assert "CLOUDSDK_STORAGE_SLICED_OBJECT_DOWNLOAD_MAX_COMPONENTS=1" in script
        assert "CLOUDSDK_STORAGE_THREAD_COUNT=1" in script

    def test_a_single_connection_disables_slicing(self):
        script = gs_handler(download_connections=1).localization_command(DEST)
        assert "CLOUDSDK_STORAGE_SLICED_OBJECT_DOWNLOAD_MAX_COMPONENTS=1" in script

    def test_process_count_is_left_to_gcloud(self):
        """
        gcloud defaults it to the core count, which on the exclusively-reserved
        n1-standard-8 already is the node budget; overriding would only risk contradicting
        that.
        """
        assert "CLOUDSDK_STORAGE_PROCESS_COUNT" not in \
            gs_handler().localization_command(DEST)

    @pytest.mark.parametrize("is_dir", [False, True])
    @pytest.mark.parametrize("kwargs", [
        dict(), dict(parallel_download=False), dict(download_connections=16),
    ])
    def test_is_valid_bash(self, is_dir, kwargs):
        script = gs_handler(is_dir=is_dir, **kwargs).localization_command(DEST)
        result = bash_ok(script)
        assert result.returncode == 0, result.stderr + "\n---\n" + script
        assert bash_ok(debug_sh_transform(script)).returncode == 0

    def test_no_downloader_is_invoked(self):
        """Confirms the handler was left on its own path rather than converted."""
        script = gs_handler().localization_command(DEST)
        assert "K9_PDL" not in script
