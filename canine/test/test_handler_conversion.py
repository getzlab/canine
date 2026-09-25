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
import shlex
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
        assert legacy[0][len(guard):].startswith(fh.fetch_or_exit(fh.CURL_FETCH + " -C - -o")[:40])
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
            + fh.fetch_or_exit(
                fh.CURL_FETCH + " -C - -o /mnt/rwdisks/canine-abc/inputs/sample.bam '{}'".format(
                    URLS[kind]))
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
        assert fh.CURL_FETCH + " -C - -o" in script

    def test_ftp_uses_the_legacy_command(self):
        """ftp is not rangeable, so the fallback is a foregone conclusion."""
        script = emit("ftp://ftp.example.org/data/sample.bam", check_md5=False)
        assert "K9_PDL" not in script
        assert fh.CURL_FETCH + " -C - -o" in script

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
            + fh.fetch_or_exit(
                fh.CURL_FETCH + ' -C - -o /mnt/rwdisks/canine-abc/inputs/sample.bam '
                '--header  "X-Auth-Token: t" \'{}\''.format(GDC_URL))
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
            + fh.fetch_or_exit(
                fh.CURL_FETCH + ' -C - -o /mnt/rwdisks/canine-abc/inputs/sample.bam "$signed_url"')
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


class TestLegacyCurlRefusesErrorBodies:
    """
    Every legacy curl carries `--fail --retry 5` (CURL_FETCH). Without --fail an HTTP error
    body is written AS the file and curl exits 0 -- measured on the worker image's curl
    7.81 against the real GDC API, whose 80-byte "Not authorized" JSON became the
    "download". These run each handler's real emitted legacy command with bash, with no
    hash check: the unprotected case, where the error body used to be accepted.
    """

    PAYLOAD = os.urandom(2 * MIB + 4321)

    def _run(self, command, tmp_path):
        script = "#!/bin/bash\nset -e\n" + command + "\n"
        return subprocess.run(["bash", "-e", "-c", script], capture_output=True, text=True,
                              timeout=120)

    def _handlers(self, server):
        other = fh.get_file_handler(server.url("sample.bam"), parallel_download=False,
                                    check_md5=False)
        gdc = gdc_handler(token="t", check_md5=False, parallel_download=False)
        gdc.url = server.url("sample.bam")
        drs = drs_handler(access_url=server.url("sample.bam"), check_md5=False,
                          parallel_download=False)
        return {"other": other, "gdc": gdc, "drs": drs}

    def _command(self, kind, handler, dest, server):
        command = handler.localization_command(dest)
        if kind == "drs":
            # swap the drshub call for a local stand-in, as TestDRSEndToEnd does
            first, rest = command.split("\n", 1)
            assert first.startswith("export signed_url=$(")
            command = "export signed_url=$(printf '%s' {})\n{}".format(
                server.url("sample.bam"), rest)
        return command

    @pytest.mark.parametrize("kind", ["other", "gdc", "drs"])
    def test_an_error_response_fails_and_writes_nothing(self, tmp_path, kind):
        dest = str(tmp_path / "sample.bam")
        with Server(self.PAYLOAD) as server:
            handler = self._handlers(server)[kind]
            server.state.force_status = 403
            result = self._run(self._command(kind, handler, dest, server), tmp_path)
        assert result.returncode != 0, "an error body was accepted as the download"
        assert not os.path.exists(dest) or b"AccessDenied" not in open(dest, "rb").read()

    @pytest.mark.parametrize("kind", ["other", "gdc", "drs"])
    def test_transient_server_errors_are_retried(self, tmp_path, kind):
        dest = str(tmp_path / "sample.bam")
        with Server(self.PAYLOAD) as server:
            handler = self._handlers(server)[kind]
            server.state.fail_next = 2
            result = self._run(self._command(kind, handler, dest, server), tmp_path)
        assert result.returncode == 0, result.stderr
        assert open(dest, "rb").read() == self.PAYLOAD

    def test_a_partial_file_survives_an_error_and_then_resumes(self, tmp_path):
        dest = str(tmp_path / "sample.bam")
        with Server(self.PAYLOAD) as server:
            handler = self._handlers(server)["other"]
            open(dest, "wb").write(self.PAYLOAD[:MIB])
            server.state.force_status = 403
            assert self._run(handler.localization_command(dest), tmp_path).returncode != 0
            assert open(dest, "rb").read() == self.PAYLOAD[:MIB]
            server.state.force_status = None
            result = self._run(handler.localization_command(dest), tmp_path)
        assert result.returncode == 0, result.stderr
        assert open(dest, "rb").read() == self.PAYLOAD

    def test_the_compressed_pipeline_does_not_place_an_error_body(self, tmp_path):
        """
        The worst case before: a wrong-sized arrival reads as "decoded by the server", and
        was moved into place with a warning.
        """
        import gzip
        blob = gzip.compress(os.urandom(MIB).hex().encode())
        dest = str(tmp_path / "o.txt")
        with Server(blob) as server:
            server.state.stored_gzip = True
            handler = fh.get_file_handler(server.url("o.txt"), parallel_download=False,
                                          check_md5=False)
            assert handler.body_is_compressed
            server.state.force_status = 403
            result = self._run(handler.localization_command(dest), tmp_path)
        assert result.returncode != 0
        assert not os.path.exists(dest)

    def test_the_downloaders_own_fallback_matches(self):
        """Its synthesized curl is used when a handler passes no --legacy-cmd."""
        import inspect
        from canine.localization import parallel_download as pdl
        assert '"curl --fail --retry 5 -C - ' in inspect.getsource(pdl)
        source = inspect.getsource(pdl.single_stream_fallback)
        assert "fetch_or_exit(" in source, "the synthesized fallback must exit explicitly too"
        assert fh.CURL_FETCH == "curl --fail --retry 5"


class TestLegacyFailuresExitExplicitly:
    """
    A failed legacy download must stop localization.sh with an ordinary failure. wolF then
    retries the task (3 times by default), and the retry resumes from the partial file.

    Three ways that went wrong before, each tested here:

    * **`set -e` does not apply inside an `&&` list.** A failed fetch or decode in
      `FETCH && verify && decode` let the script carry on, and the task ran without its
      input.
    * **Two curl exit codes collide with canine's special codes.** curl's 5 and 15 read
      as requeue (forever) and SKIP.
    * **The exit-33 handler discarded the real code.** Any other failure came out as 1.
    """

    PAYLOAD = os.urandom(2 * MIB + 777)

    @staticmethod
    def _script(command):
        # the shape of localization.sh: set -e, then the next input's line
        return "#!/bin/bash\nset -e\n" + command + "\necho NEXT_INPUT_REACHED\n"

    def _bash(self, command):
        return subprocess.run(["bash", "-c", self._script(command)], capture_output=True,
                              text=True, timeout=120)

    @pytest.mark.parametrize("code, expected", [(0, 0), (56, 56), (22, 22), (5, 1), (15, 1)])
    def test_exit_codes_are_passed_through_except_canines_special_ones(self, code, expected):
        result = self._bash(fh.fetch_or_exit("(exit {})".format(code)))
        assert result.returncode == expected
        assert ("NEXT_INPUT_REACHED" in result.stdout) == (expected == 0)

    def test_a_failure_inside_an_and_list_still_stops_the_script(self):
        """The shape of the compressed pipeline: FETCH && verify && decode."""
        result = self._bash(fh.fetch_or_exit("false") + " && echo DECODED")
        assert result.returncode != 0
        assert "NEXT_INPUT_REACHED" not in result.stdout

    def test_a_dropped_connection_fails_keeps_the_partial_and_the_retries_finish(self, tmp_path):
        """
        Each attempt is what a wolF retry runs: the same command, over the partial the
        last one left. It must fail as an ordinary failure (never 5 or 15), and the
        attempts together must produce the object exactly.
        """
        dest = str(tmp_path / "sample.bam")
        with Server(self.PAYLOAD) as server:
            server.state.drop_after = 300 * 1024
            handler = fh.get_file_handler(server.url("sample.bam"), parallel_download=False,
                                          check_md5=False)
            codes = []
            for _ in range(20):
                result = self._bash(handler.localization_command(dest))
                codes.append(result.returncode)
                if result.returncode == 0:
                    break
        assert codes[-1] == 0, codes
        assert all(c not in (5, 15) for c in codes), codes
        assert len(codes) > 2, "the drop never happened, so this tested nothing"
        assert open(dest, "rb").read() == self.PAYLOAD

    def _compressed(self, server, dest, **kwargs):
        handler = fh.get_file_handler(server.url("o.txt"), parallel_download=False,
                                      check_md5=False, **kwargs)
        assert handler.body_is_compressed
        return handler.localization_command(dest)

    def test_a_failed_decode_stops_the_script(self, tmp_path):
        """The object claims gzip and is not; gunzip fails in the middle of an && list."""
        dest = str(tmp_path / "o.txt")
        with Server(os.urandom(64 * 1024)) as server:
            server.state.stored_gzip = True
            result = self._bash(self._compressed(server, dest))
        assert result.returncode != 0
        assert "NEXT_INPUT_REACHED" not in result.stdout

    def test_a_failed_compressed_fetch_stops_the_script(self, tmp_path):
        import gzip
        dest = str(tmp_path / "o.txt")
        with Server(gzip.compress(os.urandom(MIB).hex().encode())) as server:
            server.state.stored_gzip = True
            command = self._compressed(server, dest)
            server.state.force_status = 403
            result = self._bash(command)
        assert result.returncode != 0
        assert "NEXT_INPUT_REACHED" not in result.stdout

    def test_the_exit_33_handler_keeps_the_real_exit_code(self, tmp_path):
        """A 403 while resuming a sidecar is curl's own failure code, not a plain 1."""
        import gzip
        blob = gzip.compress(os.urandom(MIB).hex().encode())
        dest = str(tmp_path / "o.txt")
        with Server(blob) as server:
            server.state.stored_gzip = True
            command = self._compressed(server, dest)
            open(dest + ".k9pdl.gz", "wb").write(blob[:1000])
            server.state.force_status = 403
            result = self._bash(command)
        assert result.returncode not in (0, 1, 5, 15), (result.returncode, result.stderr[-300:])
        assert os.path.getsize(dest + ".k9pdl.gz") == 1000, "the partial must survive"

    def test_a_truncated_compressed_body_is_not_placed_as_the_file(self, tmp_path):
        """
        No Content-Length and a dropped connection: curl exits 0 with a short body. It must
        not be taken for a server-decoded one and moved into place.
        """
        import gzip
        blob = gzip.compress(os.urandom(MIB).hex().encode())
        dest = str(tmp_path / "o.txt")
        with Server(blob) as server:
            server.state.stored_gzip = True          # which, in the fake, omits Content-Length
            command = self._compressed(server, dest)
            server.state.drop_after = 50 * 1024
            result = self._bash(command)
        assert result.returncode != 0
        assert "NEXT_INPUT_REACHED" not in result.stdout
        assert not os.path.exists(dest), "a truncated gzip stream was placed as the file"
        assert "truncated gzip stream" in result.stderr
        assert os.path.getsize(dest + ".k9pdl.gz") == 50 * 1024, "kept for the retry to resume"

    def test_a_server_decoded_body_is_still_placed(self, tmp_path):
        """The case the size check exists for must be unaffected."""
        import gzip
        plain = os.urandom(MIB).hex().encode()
        dest = str(tmp_path / "o.txt")
        with Server(gzip.compress(plain)) as server:
            server.state.stored_gzip = True
            server.state.decoded_payload = plain
            command = self._compressed(server, dest)
            server.state.always_decode = True
            result = self._bash(command)
        assert result.returncode == 0, result.stderr[-300:]
        assert open(dest, "rb").read() == plain
        assert "arrived already decoded" in result.stderr


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


def _flag_value(script, flag):
    """
    The value of `flag` in an emitted script, unquoted.

    The emitter passes these through shlex.quote, so a naive substring check on the
    script would pass on a value that the node's shell then parses into something else.
    Splitting the way the shell will is the only way to assert on what actually runs.
    """
    line = [ln for ln in script.splitlines() if flag in ln]
    assert len(line) == 1, "expected exactly one {} line, got {}".format(flag, len(line))
    tokens = shlex.split(line[0])
    return tokens[tokens.index(flag) + 1]


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
    # from the class attribute, not a literal, so it cannot drift from the default
    handler.presign_expiry = int(kwargs.get(
        "presign_expiry") or fh.HandleAWSURL.default_presign_expiry)
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

    def test_the_primary_path_does_not_resume_from_the_destination_size(self):
        """
        Size-based resume is unsound for the chunked path, where the file is created at its
        full apparent size upfront -- it would see a complete file and skip the download.
        The primary path recovers progress from the file's durable extents instead.

        It survives in the *fallback* only, where the file really is one a single stream
        appended to, and only because the downloader discards a preallocated working file
        before handing over. See TestAWSFallbackIsResumable.
        """
        script = aws_handler(MULTIPART_HEADERS, check_md5=True,
                            **PRIVATE).localization_command(DEST)
        invocation = [l for l in script.split("\n") if "$K9_PDL_RUN" in l][0]
        primary = invocation.split("--legacy-cmd")[0]
        assert "stat --printf" not in primary
        assert 'bytes=$SZ-' not in primary

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

    def test_the_fallback_is_safe_against_a_preallocated_destination(self):
        """
        The append-resume is only sound because the downloader discards a preallocated
        working file first. This asserts the two halves stay together: if the guard were
        removed, the fallback would silently accept a full-size sparse file.
        """
        from canine.localization import parallel_download as pdl
        assert hasattr(pdl, "clear_preallocated_working_file"), \
            "the fallback's append-resume depends on this guard existing"
        script = aws_handler(check_md5=False, **PRIVATE).localization_command(DEST)
        assert 'bytes=$SZ-' in script


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


class _Blob:
    def __init__(self, size, content_encoding=None, md5_hash=None, crc32c="AAAAAA=="):
        self.size = size
        self.content_encoding = content_encoding
        self.md5_hash = md5_hash
        self.crc32c = crc32c
        self.name = "ref/genome.dict"
        # declared, so sizing never reaches for the ISIZE trailer over the network
        self.metadata = {"uncompressed_size": str(size * 4)}


def encoded_gs_handler(blobs, is_dir=False, **kwargs):
    handler = gs_handler(is_dir=is_dir, **kwargs)
    handler.path = "gs://bkt/ref/genome.dict"
    handler._size = None
    handler.blob = lambda: (handler.blob_calls.append(1), blobs)[1]
    handler.blob_calls = []
    return handler


GZ_MD5 = hashlib.md5(b"stored gzip bytes").digest()


class TestAGzipEncodedGsObject:
    """
    `Content-Encoding: gzip` on a gs:// object cannot survive the bucket route's
    server-side copy, which carries the stored bytes verbatim. The handler classifies it
    from the metadata planning already fetched and localizes it through the downloader's
    verified, streaming decode.
    """

    def test_an_encoded_object_is_transport_gzip(self):
        assert encoded_gs_handler([_Blob(100, "gzip")]).transport_gzip

    @pytest.mark.parametrize("encoding", [None, "", "identity", "br"])
    def test_other_encodings_are_not(self, encoding):
        assert not encoded_gs_handler([_Blob(100, encoding)]).transport_gzip

    def test_the_encoding_is_compared_case_and_space_insensitively(self):
        assert encoded_gs_handler([_Blob(100, " GZIP ")]).transport_gzip

    def test_a_directory_is_never_transport_gzip(self):
        """Its encoded objects are gzip_members; the directory itself is not one."""
        h = encoded_gs_handler([_Blob(100, "gzip"), _Blob(50, "gzip")], is_dir=True)
        assert not h.transport_gzip

    def test_classification_costs_no_extra_metadata_fetch(self):
        """blob() is not cached, and the planner visits every gs:// input."""
        h = encoded_gs_handler([_Blob(100, "gzip")])
        h.size
        h.transport_gzip
        h.transport_gzip
        assert len(h.blob_calls) == 1

    def test_a_size_cached_without_the_metadata_is_refetched(self):
        """Reading that as "not encoded" would be the plain copy, and the original bug."""
        h = encoded_gs_handler([_Blob(100, "gzip")])
        h._size = 12345
        assert h.transport_gzip
        assert len(h.blob_calls) == 1

    def _command(self, blob=None, **kwargs):
        h = encoded_gs_handler([blob or _Blob(777, "gzip", base64.b64encode(GZ_MD5).decode())],
                               **kwargs)
        return h, h.downloader_command(DEST)

    def test_reads_the_object_over_the_json_api_and_decodes(self):
        _, script = self._command()
        assert "--gs-source gs://bkt/ref/genome.dict" in script
        assert "--gunzip" in script
        assert "--url" not in script

    def test_transfers_the_stored_length(self):
        """The plan is over the stored bytes; the decoded length is not what crosses the wire."""
        _, script = self._command()
        assert "--size 777 " in script

    def test_verifies_the_stored_md5(self):
        _, script = self._command()
        assert "--check-md5 " + GZ_MD5.hex() in script

    def test_a_composite_object_is_decoded_unverified(self):
        _, script = self._command(_Blob(777, "gzip", md5_hash=None))
        assert "--gs-source" in script
        assert "--check-md5" not in script

    @pytest.mark.parametrize("check_hash", [True, False])
    def test_verifies_whatever_check_hash_says(self, check_hash):
        """gcloud storage cp, which this replaces, always validates; so does this."""
        _, script = self._command(check_hash=check_hash)
        assert "--check-md5 " + GZ_MD5.hex() in script

    def test_requester_pays_bills_the_user_project(self):
        h = encoded_gs_handler([_Blob(777, "gzip")], project="my-proj")
        h.rp_string = " --billing-project=my-proj"
        assert "--user-project my-proj" in h.downloader_command(DEST)

    def test_no_user_project_otherwise(self):
        """Billing a project the object does not ask for would be a charge nobody chose."""
        h = encoded_gs_handler([_Blob(777, "gzip")], project="my-proj")
        assert "--user-project" not in h.downloader_command(DEST)

    def test_falls_back_to_gcloud_storage_cp(self):
        """Which also decodes, so the fallback cannot reintroduce the gzip stream."""
        _, script = self._command()
        assert "--legacy-cmd" in script
        assert "gcloud storage cp" in script

    def test_an_unencoded_object_is_refused(self):
        """--gunzip would decode a plain object's bytes -- or fail on them."""
        h = encoded_gs_handler([_Blob(777, None)])
        with pytest.raises(ValueError):
            h.downloader_command(DEST)

    def test_targets_the_given_destination(self):
        h, script = self._command()
        assert h.localized_path == DEST
        assert "--dest " + DEST in script

    @pytest.mark.parametrize("kwargs", [dict(), dict(check_hash=False), dict(project="p")])
    def test_is_valid_bash(self, kwargs):
        h = encoded_gs_handler([_Blob(777, "gzip", base64.b64encode(GZ_MD5).decode())], **kwargs)
        if "project" in kwargs:
            h.rp_string = " --billing-project=p"
        result = bash_ok(h.downloader_command(DEST))
        assert result.returncode == 0, result.stderr


def _named(name, *args, **kwargs):
    blob = _Blob(*args, **kwargs)
    blob.name = name
    return blob


def encoded_gs_directory(blobs, **kwargs):
    handler = encoded_gs_handler(blobs, is_dir=True, **kwargs)
    handler.path = "gs://bkt/funcotator"
    return handler


FUNCOTATOR = [
    _named("funcotator/MANIFEST.txt", 201, "gzip", base64.b64encode(GZ_MD5).decode()),
    _named("funcotator/gencode/hg38/gencode.v43.pc_transcripts.dict", 6266373, "gzip"),
    _named("funcotator/gnomAD_exome/hg38/gnomAD_exome.tar.gz", 900, None),
    _named("funcotator/dbsnp/hg38/dbsnp.vcf.idx", 300, "identity"),
]


class TestAGzipEncodedObjectInADirectory:
    """
    The Funcotator data sources directory has 35 of its 46 objects stored with
    `Content-Encoding: gzip`, among them a GATK `.dict`. The directory's server-side copy
    would carry each one verbatim, just as for a single object, so the handler names them
    from the listing sizing already fetched and each becomes a single-object handler for
    the decode.
    """

    def test_only_the_encoded_objects_are_members(self):
        members = encoded_gs_directory(FUNCOTATOR).gzip_members
        assert [m.path for m in members] == [
            "gs://bkt/funcotator/MANIFEST.txt",
            "gs://bkt/funcotator/gencode/hg38/gencode.v43.pc_transcripts.dict",
        ]

    def test_each_member_is_a_gzip_encoded_single_object(self):
        for member in encoded_gs_directory(FUNCOTATOR).gzip_members:
            assert not member.is_dir
            assert member.transport_gzip

    def test_names_are_relative_to_the_directory(self):
        h = encoded_gs_directory(FUNCOTATOR)
        assert [h.relative_name(m) for m in h.gzip_members] == [
            "MANIFEST.txt", "gencode/hg38/gencode.v43.pc_transcripts.dict"]

    def test_a_member_decodes_its_own_stored_bytes(self):
        """The stored length and md5 are the member's, not the directory's."""
        member = encoded_gs_directory(FUNCOTATOR).gzip_members[0]
        script = member.downloader_command("/mnt/localize/b/in/funcotator/MANIFEST.txt")
        assert "--gs-source gs://bkt/funcotator/MANIFEST.txt" in script
        assert "--size 201 " in script
        assert "--check-md5 " + GZ_MD5.hex() in script

    def test_a_member_is_sized_decoded(self):
        """The directory's disk estimate already counted it that way; so does the member."""
        member = encoded_gs_directory(FUNCOTATOR).gzip_members[0]
        assert member.size == 201 * 4                 # _Blob declares uncompressed_size

    def test_classification_costs_no_extra_listing(self):
        h = encoded_gs_directory(FUNCOTATOR)
        h.size
        members = h.gzip_members
        h.gzip_members
        [(m.transport_gzip, m.size) for m in members]
        assert len(h.blob_calls) == 1

    def test_members_share_the_requester_pays_resolution(self):
        """The constructor would re-check requester pays once per member."""
        h = encoded_gs_directory(FUNCOTATOR, project="my-proj")
        h.rp_string = " --billing-project=my-proj"
        with patch.object(fh.HandleGSURL, "get_requester_pays",
                          side_effect=AssertionError("re-resolved")):
            members = h.gzip_members
        assert {m.rp_string for m in members} == {" --billing-project=my-proj"}

    def test_members_do_not_share_the_directorys_arguments(self):
        """check_hash's setter writes into extra_args; a member must not reach back."""
        h = encoded_gs_directory(FUNCOTATOR)
        h.gzip_members[0].check_hash = True
        assert h.extra_args.get("check_hash") in (None, False)

    def test_a_size_cached_without_the_listing_is_relisted(self):
        """Reading that as "nothing encoded" would be the plain copy, and the bug."""
        h = encoded_gs_directory(FUNCOTATOR)
        h._size = 12345
        assert len(h.gzip_members) == 2

    def test_an_unencoded_directory_has_none(self):
        assert encoded_gs_directory(FUNCOTATOR[2:]).gzip_members == []

    def test_a_single_object_has_none(self):
        """Its own encoding is transport_gzip."""
        assert encoded_gs_handler([_Blob(100, "gzip")]).gzip_members == []


class TestTheSignedUrlOutlivesTheTransfer:
    """
    The presigned URL was minted with the AWS CLI default window of one hour and never
    re-minted. A 279 GiB object at the ~60 MB/s measured against the GDC endpoint takes
    about 80 minutes, so the signature expired mid-transfer; HttpSource.open_range got a
    403, HttpSource.refresh_url returned False for want of a --url-refresh-cmd, and the
    403 became a PermanentError -- exit 1, do-not-retry, with most of the object already
    written. The exact workload this work exists to speed up was the one guaranteed to
    hit it.
    """

    def test_the_presign_has_an_explicit_window(self):
        script = aws_handler(**PRIVATE).localization_command("/dest/f")
        assert "presign" in script
        assert "--expires-in {}".format(12 * 60 * 60) in script, script

    def test_the_window_is_longer_than_the_motivating_transfer(self):
        """
        279 GiB at 60 MB/s is ~80 minutes. A window that does not clear that is the bug
        with a bigger number in it, so assert the margin rather than the literal.
        """
        seconds_needed = (279 * 1024 ** 3) / (60 * 1000 ** 2)
        assert fh.HandleAWSURL.default_presign_expiry > 2 * seconds_needed

    def test_the_downloader_can_re_mint_the_url(self):
        script = aws_handler(**PRIVATE).localization_command("/dest/f")
        assert "--url-refresh-cmd" in script, script

    def test_the_refresh_command_presigns_the_same_object(self):
        script = aws_handler(**PRIVATE).localization_command("/dest/f")
        refresh = _flag_value(script, "--url-refresh-cmd")
        assert "presign" in refresh and S3_PATH in refresh, refresh

    def test_the_refresh_command_carries_the_endpoint_and_credentials(self):
        """
        A refresh that reaches Amazon instead of the GDC endpoint, or that signs with
        nothing, produces a URL that 403s -- and the downloader would treat the refresh
        as having succeeded.
        """
        script = aws_handler(
            aws_endpoint_url="https://s3.example.org", **PRIVATE
        ).localization_command("/dest/f")
        refresh = _flag_value(script, "--url-refresh-cmd")
        assert "--endpoint-url https://s3.example.org" in refresh, refresh
        assert "AWS_ACCESS_KEY_ID=AKIA" in refresh, refresh

    def test_the_refresh_command_is_the_command_that_minted_the_url(self):
        """
        Not merely similar: if the two drift, a refresh silently changes which object or
        endpoint is being read partway through a transfer.
        """
        script = aws_handler(**PRIVATE).localization_command("/dest/f")
        minting = [line for line in script.splitlines()
                   if line.startswith("export K9_S3_URL=")][0]
        refresh = _flag_value(script, "--url-refresh-cmd")
        assert refresh in minting, (refresh, minting)

    def test_a_public_object_neither_presigns_nor_refreshes(self):
        """Nothing to sign with, so a refresh command would be a command that fails."""
        script = aws_handler().localization_command("/dest/f")
        assert "presign" not in script
        assert "--url-refresh-cmd" not in script

    def test_the_window_is_overridable(self):
        script = aws_handler(presign_expiry=600, **PRIVATE).localization_command("/dest/f")
        assert "--expires-in 600" in script, script

    def test_the_window_cannot_carry_shell_metacharacters(self):
        """
        This value is interpolated into an emitted command, so a string is an injection
        site. int() rejects it host-side rather than on the node.
        """
        with pytest.raises(ValueError):
            aws_handler(presign_expiry="600; rm -rf /", **PRIVATE)


class TestTheRealConstructorAgreesWithTheFixtures:
    """
    Two test modules build HandleAWSURL with `__new__` and hand-set the fields __init__
    would, because __init__ shells out to `aws s3api head-object`. Adding presign_expiry
    broke both -- loudly, as 28 AttributeErrors, which is the benign direction. The
    dangerous direction is a fixture that keeps setting a field __init__ has stopped
    setting, or sets a different value: then every test passes against a handler that no
    longer exists.

    So exercise the emitted-script path once through the real constructor. If the two
    diverge, the emitted command differs here and nowhere else.
    """

    HEAD = {"ContentLength": 50 * 1024 * MIB,
            "ETag": '"{}"'.format(MULTIPART_ETAG),
            "PartsCount": 800,
            "PartLength": 64 * MIB}

    def build(self, **kwargs):
        import json as _json
        completed = subprocess.CompletedProcess(
            args="", returncode=0, stdout=_json.dumps(self.HEAD).encode(), stderr=b"")
        with patch.object(fh.subprocess, "run", return_value=completed) as run:
            handler = fh.HandleAWSURL(S3_PATH, **kwargs)
        assert run.called, "head-object was not the call that was mocked"
        return handler

    def test_the_real_constructor_sets_the_presign_window(self):
        assert self.build(**PRIVATE).presign_expiry == 12 * 60 * 60

    def test_the_real_constructor_emits_the_same_presign_and_refresh(self):
        real = self.build(**PRIVATE).localization_command("/dest/f")
        assert "--expires-in {}".format(12 * 60 * 60) in real, real
        refresh = _flag_value(real, "--url-refresh-cmd")
        assert "presign" in refresh and S3_PATH in refresh, refresh

    def test_the_fixture_emits_what_the_real_constructor_emits(self):
        """
        The whole point: same inputs, same script. A difference here means the fixtures
        have drifted and every assertion made through them is about a fiction.
        """
        real = self.build(**PRIVATE).localization_command("/dest/f")
        fake = aws_handler(headers=dict(self.HEAD), **PRIVATE).localization_command("/dest/f")
        assert real == fake

    def test_the_override_survives_the_real_constructor(self):
        assert self.build(presign_expiry=600, **PRIVATE).presign_expiry == 600

    def test_a_nonsense_window_is_rejected_host_side(self):
        with pytest.raises(ValueError):
            self.build(presign_expiry="600; rm -rf /", **PRIVATE)


class TestAWSFallbackIsResumable:
    """
    The resumability requirement covers the fallback too: anything the chunked path
    declines still has to resume rather than restart. This handler's append-resume is
    only sound because the downloader discards a preallocated working file before
    handing over -- but given that guard, dropping it would have been a regression for
    no benefit.
    """

    def test_fallback_resumes_from_the_existing_size(self):
        script = aws_handler(check_md5=False, **PRIVATE).localization_command(DEST)
        assert "stat --printf '%s'" in script
        assert 'bytes=$SZ-' in script

    def test_fallback_skips_an_already_complete_file(self):
        """
        localization.sh is re-run in full after a preemption, so the block has to be
        idempotent.
        """
        script = aws_handler(SINGLE_PART_HEADERS, check_md5=False,
                            **PRIVATE).localization_command(DEST)
        assert "if [ $SZ != {} ]".format(SINGLE_PART_HEADERS["ContentLength"]) in script

    def test_fallback_is_a_single_line(self):
        """It is embedded as a --legacy-cmd argument and inside an if/else branch."""
        script = aws_handler(check_md5=False, **PRIVATE).localization_command(DEST)
        legacy_lines = [l for l in script.split("\n") if "bytes=$SZ-" in l]
        assert len(legacy_lines) == 1

    def test_fallback_is_valid_bash(self):
        script = aws_handler(check_md5=False, **PRIVATE).localization_command(DEST)
        result = bash_ok(script)
        assert result.returncode == 0, result.stderr + "\n---\n" + script

    def test_fallback_uses_bash_only_constructs_deliberately(self):
        """
        Process substitution is a bashism, which is why the downloader names bash
        explicitly instead of letting subprocess pick /bin/sh (dash on the worker image).
        """
        script = aws_handler(check_md5=False, **PRIVATE).localization_command(DEST)
        assert ">(cat >>" in script
        import subprocess as sp
        assert sp.run(["sh", "-n"], input=script, text=True,
                      capture_output=True).returncode != 0, \
            "expected this to be bash-only; if sh accepts it the comment is wrong"


# ---------------------------------------------------------------------------
# gzip-stored objects (verified against a real GCS object)
# ---------------------------------------------------------------------------

# Headers copied from a real gzip-stored reference file in a Getz Lab bucket. The
# significant parts: content-encoding: gzip is PRESENT, there is NO content-length at all,
# the size lives in x-goog-stored-content-length, the x-goog-hash md5 equals the ETag (both
# over the stored bytes), and ranges are accepted. Both a plain request and one with
# Accept-Encoding: gzip returned byte-identical headers.
STORED_GZIP_SIZE = 6266373
STORED_GZIP_MD5 = "853f4cd545dcefd9a537546f82bd6d2a"
STORED_GZIP_HEADERS = (
    b"HTTP/2 200 \r\n"
    b"content-type: text/plain; charset=us-ascii\r\n"
    b"cache-control: no-transform\r\n"
    b'etag: "' + STORED_GZIP_MD5.encode() + b'"\r\n'
    b"x-goog-stored-content-encoding: gzip\r\n"
    b"x-goog-stored-content-length: " + str(STORED_GZIP_SIZE).encode() + b"\r\n"
    b"content-encoding: gzip\r\n"
    b"x-goog-hash: crc32c=cYnNEg==\r\n"
    b"x-goog-hash: md5=hT9M1UXc79mlN1Rvgr1tKg==\r\n"
    b"accept-ranges: bytes\r\n"
    b"\r\n"
)


class TestStoredGzipObject:

    def _handler(self, **kwargs):
        class Fake:
            stdout = STORED_GZIP_HEADERS

        with patch("os.path.exists", return_value=False), \
             patch("canine.localization.file_handlers.subprocess.run",
                   return_value=Fake()):
            return fh.get_file_handler(URLS["gcs_signed"], **kwargs)

    def test_does_not_raise_without_a_content_length(self):
        """
        Regression for a real, pre-existing failure: such an object has no
        content-length, so requiring one made it unlocalizable through this handler. The
        original code failed too -- its grep matched x-goog-stored-content-length but the
        regex it then applied was anchored, so the match failed and the bare except turned
        it into the same error.
        """
        assert self._handler(check_md5=True).size == STORED_GZIP_SIZE

    def test_size_is_the_compressed_length(self):
        """
        That is what crosses the wire, so it is what the chunk plan and range requests must
        be built from. The decompressed length is a separate quantity, used for disk sizing.
        """
        assert self._handler().size == STORED_GZIP_SIZE

    def test_body_is_recognized_as_compressed(self):
        assert self._handler().body_is_compressed is True

    def test_the_advertised_digest_is_used(self):
        """
        It covers the stored bytes, which are exactly what gets verified before any
        decompression -- confirmed by the md5 equalling the ETag on the real object.
        """
        assert self._handler(check_md5=True).content_checksum == ("md5", STORED_GZIP_MD5)

    def test_raw_bytes_are_requested_explicitly(self):
        """
        Decompressing in flight would change the byte count mid-transfer, which makes
        ranged and resumable downloads impossible -- the same reason gcloud storage cp
        sets do_not_decompress for these objects.
        """
        script = self._handler(check_md5=True).localization_command(DEST)
        assert "--header 'Accept-Encoding: gzip'" in script

    def test_decompression_is_requested(self):
        script = self._handler(check_md5=True).localization_command(DEST)
        assert "--gunzip" in script

    def test_verification_is_handed_to_the_downloader(self):
        """It must verify the compressed bytes before decompressing them."""
        script = self._handler(check_md5=True).localization_command(DEST)
        assert "--check-md5 " + STORED_GZIP_MD5 in script
        # the only md5sum in the command belongs to the fallback pipeline, where it gates
        # the compressed sidecar; the primary path does not re-check after the downloader
        primary = script.split("--legacy-cmd")[0]
        assert "md5sum" not in primary

    def test_a_plain_object_asks_for_neither(self):
        plain = (b"HTTP/1.1 200 OK\r\nContent-Length: 1024\r\n"
                 b"Content-MD5: " + EMPTY_MD5_B64.encode() + b"\r\n\r\n")

        class Fake:
            stdout = plain

        with patch("os.path.exists", return_value=False), \
             patch("canine.localization.file_handlers.subprocess.run",
                   return_value=Fake()):
            handler = fh.get_file_handler(URLS["gcs_signed"], check_md5=True)
        script = handler.localization_command(DEST)
        assert "--gunzip" not in script
        assert "Accept-Encoding" not in script

    def test_is_valid_bash(self):
        script = self._handler(check_md5=True).localization_command(DEST)
        result = bash_ok(script)
        assert result.returncode == 0, result.stderr
        assert bash_ok(debug_sh_transform(script)).returncode == 0


class TestGzipEndToEnd:

    def test_downloads_verifies_and_decompresses(self, tmp_path):
        """
        The whole chain against a server that reproduces the real object's headers:
        parallel download of the compressed bytes, verify against the stored md5,
        decompress into dest.
        """
        import base64
        import gzip
        import hashlib

        plain = b"@HD\tVN:1.6\n" + b"@SQ\tSN:chr1\tLN:248956422\n" * 40000
        blob = gzip.compress(plain)
        md5_b64 = base64.b64encode(hashlib.md5(blob).digest()).decode()

        with Server(blob) as server:
            server.state.stored_gzip = True
            headers = (
                # the full header set the real object sends: decompression requires the
                # stored-encoding signal as well as the body arriving encoded
                "HTTP/1.1 200 OK\r\ncontent-encoding: gzip\r\n"
                "x-goog-stored-content-encoding: gzip\r\n"
                "x-goog-stored-content-length: {}\r\n"
                "x-goog-hash: md5={}\r\n\r\n".format(len(blob), md5_b64)
            ).encode()

            class Fake:
                stdout = headers

            with patch("os.path.exists", return_value=False), \
                 patch("canine.localization.file_handlers.subprocess.run",
                       return_value=Fake()):
                handler = fh.get_file_handler(
                    server.url("gencode.dict"), check_md5=True,
                    download_min_chunk=MIB, download_connections=4,
                )
            dest = str(tmp_path / "gencode.dict")
            script = "#!/bin/bash\nset -e\n" + handler.localization_command(dest) + "\n"
            result = subprocess.run(["bash", "-e", "-c", script], capture_output=True,
                                    text=True, timeout=300)

        assert result.returncode == 0, result.stdout + result.stderr
        assert open(dest, "rb").read() == plain, "destination is not the decompressed data"
        assert not os.path.exists(dest + ".k9pdl.gz"), \
            "compressed sidecar not cleaned up; peak disk stays doubled"

    def test_a_corrupt_compressed_body_is_caught_before_decompression(self, tmp_path):
        """
        Verification happens on the compressed bytes, so a bad download must fail without
        producing a destination file at all.
        """
        import gzip

        blob = gzip.compress(b"payload" * 10000)
        with Server(blob) as server:
            server.state.stored_gzip = True
            headers = (
                "HTTP/1.1 200 OK\r\ncontent-encoding: gzip\r\n"
                "x-goog-stored-content-encoding: gzip\r\n"
                "x-goog-stored-content-length: {}\r\n"
                "x-goog-hash: md5=AAAAAAAAAAAAAAAAAAAAAA==\r\n\r\n".format(len(blob))
            ).encode()

            class Fake:
                stdout = headers

            with patch("os.path.exists", return_value=False), \
                 patch("canine.localization.file_handlers.subprocess.run",
                       return_value=Fake()):
                handler = fh.get_file_handler(
                    server.url("o.dict"), check_md5=True, download_min_chunk=MIB,
                    download_connections=2,
                )
            dest = str(tmp_path / "o.dict")
            script = "#!/bin/bash\nset -e\n" + handler.localization_command(dest) + "\n"
            result = subprocess.run(["bash", "-e", "-c", script], capture_output=True,
                                    text=True, timeout=300)

        assert result.returncode != 0
        assert not os.path.exists(dest), "wrote a destination despite failing verification"


class TestSignedUrlFilenames:
    """
    A signed URL's query string must not become part of the localized filename. This was
    pre-existing in HandleOtherURL, which took everything after the last "/";
    HandleGCSSignedURL already stripped the query.

    It is not cosmetic. An S3 SigV4 query is several hundred characters, so the resulting
    name usually exceeds the 255-byte filename limit; the signature changes on every
    attempt, so the name is unstable; and an unstable basename also changes the
    localization disk's name hash, defeating disk reuse.
    """

    def _handler(self, url):
        class Fake:
            stdout = b"HTTP/1.1 200 OK\r\ncontent-length: 500000\r\n\r\n"

        with patch("os.path.exists", return_value=False), \
             patch("canine.localization.file_handlers.subprocess.run",
                   return_value=Fake()):
            return fh.get_file_handler(url)

    S3_SIGNED = ("https://mybucket.s3.amazonaws.com/data/sample.bam"
                 "?X-Amz-Algorithm=AWS4-HMAC-SHA256"
                 "&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20260810%2Fus-east-1%2Fs3"
                 "%2Faws4_request&X-Amz-Date=20260810T221753Z&X-Amz-Expires=900"
                 "&X-Amz-SignedHeaders=host&X-Amz-Signature=" + "f" * 64)

    def test_query_string_is_not_part_of_the_filename(self):
        assert self._handler(self.S3_SIGNED).path == "sample.bam"

    def test_the_old_behavior_would_have_exceeded_the_filename_limit(self):
        """Shows the bug had teeth rather than being merely untidy."""
        import re as _re
        legacy = _re.match(r"(?:http|https|ftp)://.*\/(.*)$", self.S3_SIGNED)[1]
        assert len(legacy.encode()) > 255
        assert len(self._handler(self.S3_SIGNED).path.encode()) <= 255

    def test_the_filename_is_stable_across_re_signing(self):
        """
        A changing basename also changes the localization disk's name hash, so disk reuse
        and job avoidance would silently stop working.
        """
        first = self._handler(self.S3_SIGNED).path
        second = self._handler(self.S3_SIGNED.replace("f" * 64, "a" * 64)).path
        assert first == second == "sample.bam"

    def test_fragment_is_dropped_too(self):
        assert self._handler("http://h/a/b/file.vcf.gz?t=1#frag").path == "file.vcf.gz"

    def test_a_plain_url_is_unaffected(self):
        assert self._handler("https://example.org/refs/hg38.fa").path == "hg38.fa"

    @pytest.mark.parametrize("url", ["https://example.org", "https://example.org/"])
    def test_a_url_with_no_path_segment_raises(self, url):
        with pytest.raises(ValueError):
            self._handler(url)

    def test_a_trailing_slash_uses_the_last_segment(self):
        """
        Improves on the old behavior rather than matching it: the previous regex produced
        an EMPTY filename for a trailing-slash URL (no error, just a broken name). Using
        the last segment is at least a usable guess, and a URL with no segment at all now
        fails early with a clear error instead of silently yielding "".
        """
        assert self._handler("https://example.org/dir/").path == "dir"


class TestS3SignedUrlVerification:
    """
    S3 does not transcode: a gzip-stored object is served as stored, with
    `Content-Encoding: gzip` and an ETag over those same bytes. Since nothing decompresses
    in flight, the file on disk is what the ETag covers, so it is verifiable -- an earlier
    version of the encoding rule refused it, leaving such objects unverifiable for no
    reason.
    """

    def _handler(self, raw_headers, **kwargs):
        class Fake:
            stdout = raw_headers

        with patch("os.path.exists", return_value=False), \
             patch("canine.localization.file_handlers.subprocess.run",
                   return_value=Fake()):
            return fh.get_file_handler(
                "https://b.s3.amazonaws.com/d/o.bam?X-Amz-Signature=a", **kwargs)

    GZIP_STORED = (b"HTTP/1.1 200 OK\r\ncontent-encoding: gzip\r\n"
                   b"content-length: 500000\r\n"
                   b'etag: "' + EMPTY_MD5_HEX.encode() + b'"\r\n'
                   b"accept-ranges: bytes\r\n\r\n")

    def test_a_gzip_stored_s3_object_is_still_verifiable(self):
        handler = self._handler(self.GZIP_STORED, check_md5=True)
        assert handler.content_checksum == ("md5", EMPTY_MD5_HEX)

    def test_it_is_decompressed(self):
        """
        `Content-Encoding: gzip` means the body is a transport encoding of the file, so
        the localized file must be the decoded content -- a task reading it expects the
        data, not a gzip stream. This also makes an S3 signed URL agree with the same
        object fetched over gs://, which gcloud already decompresses locally.

        This IS a behavior change: the old `curl -C - -o` wrote the bytes as received.
        """
        handler = self._handler(self.GZIP_STORED, check_md5=True)
        assert handler.body_is_compressed is True
        assert "--gunzip" in handler.localization_command(DEST)

    def test_size_comes_from_content_length(self):
        """
        The x-goog- fallback is GCS-specific; a standard Content-Length must always win,
        and is what every non-GCS server sends.
        """
        assert self._handler(self.GZIP_STORED).size == 500000

    def test_content_length_wins_over_the_google_fallback(self):
        both = (b"HTTP/1.1 200 OK\r\ncontent-length: 111\r\n"
                b"x-goog-stored-content-length: 999\r\n\r\n")
        assert self._handler(both).size == 111

    def test_a_stored_gzip_object_is_fetched_as_stored_and_verified(self):
        """
        This used to be "a transcoded response refuses the digest": stored-encoding
        present, Content-Encoding absent, read as "served decoded, so the digest covers
        bytes we never see". Measured (§13.70), that header shape is simply what a HEAD
        returns for ANY transcoding-eligible GCS object -- no Content-Encoding whatever
        Accept-Encoding asks for -- and GCS serves the stored bytes to any request that
        sends `Accept-Encoding: gzip`, ranges included. So the right reading is: request
        the stored bytes, and the stored digest covers exactly them.

        Refusing it sent every such object down one unverified single-stream curl.
        """
        stored = (b"HTTP/1.1 200 OK\r\nx-goog-stored-content-length: 20000000\r\n"
                  b"x-goog-stored-content-encoding: gzip\r\n"
                  b"x-goog-hash: md5=hT9M1UXc79mlN1Rvgr1tKg==\r\n\r\n")
        handler = self._handler(stored, check_md5=True)
        assert handler.body_is_compressed is True
        assert handler.content_checksum == ("md5", "853f4cd545dcefd9a537546f82bd6d2a")
        assert handler._size == 20000000
        cmd = handler.localization_command(DEST)
        assert "--check-md5 853f4cd545dcefd9a537546f82bd6d2a" in cmd
        assert "--gunzip" in cmd
        # both the downloader AND the legacy fallback must ask for the stored bytes,
        # or GCS transcodes and the digest check fails on decoded bytes
        assert cmd.count("Accept-Encoding: gzip") >= 2, cmd

    def test_the_stored_length_wins_over_a_decoded_content_length(self):
        """
        The chunk plan ranges over the STORED bytes. A Content-Length alongside the
        stored-encoding header would describe the decoded representation, and planning
        chunks from it would request ranges past the end of the object.
        """
        both = (b"HTTP/1.1 200 OK\r\ncontent-length: 99999999\r\n"
                b"x-goog-stored-content-length: 20000000\r\n"
                b"x-goog-stored-content-encoding: gzip\r\n\r\n")
        assert self._handler(both)._size == 20000000

    def test_multipart_etag_is_still_rejected(self):
        multipart = (b"HTTP/1.1 200 OK\r\ncontent-length: 500000\r\n"
                     b'etag: "' + EMPTY_MD5_HEX.encode() + b'-42"\r\n\r\n')
        assert self._handler(multipart, check_md5=True).content_checksum == (None, None)


class TestTransportCompressionIsNotRequested:
    """
    A standard server's on-the-fly compression is the case that RFC 9110 says should be
    decoded -- the entity is the decompressed content, unlike the S3 stored-compressed
    case. It mostly does not arise because we never opt in, and these tests pin that
    rather than leaving it as an assumption.
    """

    def test_the_probe_asks_for_identity(self):
        """
        Matching what the download sends. Under RFC 9110 omitting Accept-Encoding permits
        the server to compress, while `identity` asks it not to -- so an unmatched probe
        could describe a representation that is never downloaded.
        """
        captured = {}

        class Fake:
            stdout = b"HTTP/1.1 200 OK\r\ncontent-length: 10\r\n\r\n"

        def spy(cmd, **kwargs):
            captured["cmd"] = cmd
            return Fake()

        with patch("os.path.exists", return_value=False), \
             patch("canine.localization.file_handlers.subprocess.run", side_effect=spy):
            fh.get_file_handler("https://example.org/o.bam")
        assert "Accept-Encoding: identity" in captured["cmd"]

    def test_the_downloader_asks_for_identity_by_default(self):
        """urllib's own default, relied on here, so it is worth asserting."""
        from canine.localization import parallel_download as pdl
        source = pdl.HttpSource("https://example.org/o")
        request = source._request({"Range": "bytes=0-0"})
        # urllib adds the header at send time when absent, so absence here is the point:
        # nothing in our code overrides it toward gzip
        assert "gzip" not in str(request.headers).lower()

    def test_a_pre_compressed_static_response_is_decompressed(self):
        """
        Indistinguishable on the wire from the S3 case, and treated the same -- both are
        decoded, because a content-coding is a transport property and the task reading the
        file expects the decoded content.
        """
        headers = (b"HTTP/1.1 200 OK\r\ncontent-encoding: gzip\r\n"
                   b"content-length: 500000\r\n"
                   b"content-md5: " + EMPTY_MD5_B64.encode() + b"\r\n\r\n")

        class Fake:
            stdout = headers

        with patch("os.path.exists", return_value=False), \
             patch("canine.localization.file_handlers.subprocess.run",
                   return_value=Fake()):
            handler = fh.get_file_handler("https://example.org/o.vcf", check_md5=True)

        assert handler.body_is_compressed is True
        assert "--gunzip" in handler.localization_command(DEST)
        # still verifiable: the digest covers the compressed bytes, which are what gets
        # verified -- before decompression, not after
        assert handler.content_checksum == ("md5", EMPTY_MD5_HEX)

    def test_a_compressed_response_with_no_length_raises(self):
        """
        On-the-fly compression that ignored the identity request: chunked, no
        Content-Length, no precomputable digest. Raising is the right outcome -- the
        alternative is proceeding with a size that describes different bytes. Same
        behavior as before this work, which also required Content-Length.
        """
        headers = b"HTTP/1.1 200 OK\r\ncontent-encoding: gzip\r\n\r\n"

        class Fake:
            stdout = headers

        with patch("os.path.exists", return_value=False), \
             patch("canine.localization.file_handlers.subprocess.run",
                   return_value=Fake()):
            with pytest.raises(ValueError, match="Could not get file header size"):
                fh.get_file_handler("https://example.org/o.vcf")

    def test_either_encoding_signal_decides_decompression(self):
        """
        The rule, stated once: a body arriving gzip-encoded, OR a GCS object stored
        gzip-encoded, triggers decompression. The stored-only case used to mean "served
        decoded, nothing to do" -- but it is how a HEAD reports every transcoding-eligible
        GCS object (measured, §13.70), and the download asks for the stored bytes.
        """
        variants = {
            "neither": b"content-length: 10\r\n",
            "received only": b"content-length: 10\r\ncontent-encoding: gzip\r\n",
            "stored only": b"content-length: 10\r\nx-goog-stored-content-encoding: gzip\r\n",
            "both": (b"x-goog-stored-content-length: 10\r\n"
                     b"content-encoding: gzip\r\n"
                     b"x-goog-stored-content-encoding: gzip\r\n"),
        }
        results = {}
        for label, block in variants.items():
            class Fake:
                stdout = b"HTTP/1.1 200 OK\r\n" + block + b"\r\n"

            with patch("os.path.exists", return_value=False), \
                 patch("canine.localization.file_handlers.subprocess.run",
                       return_value=Fake()):
                handler = fh.get_file_handler("https://example.org/o.vcf")
            results[label] = handler.body_is_compressed

        assert results == {"neither": False, "received only": True,
                           "stored only": True, "both": True}


class TestTheFetchDetectsWhatArrived:
    """
    Behaviour measured on real GCS signed URLs (§13.70), modelled in the fake:

      * a HEAD for an ordinary gzip-encoded object carries only
        x-goog-stored-content-encoding -- no Content-Encoding -- so keying detection on
        Content-Encoding missed every one of them;
      * typed application/gzip, a request with no Range is served DECODED even when it
        asks for gzip, while any Range gets the stored bytes (200, Range ignored);
      * GCS's anonymous/edge path can serve decoded bytes whatever was asked for.

    The pipeline asks for the stored bytes but decides from what actually arrived.
    """

    def _run(self, tmp_path, configure=None, partial=None, **kwargs):
        import base64
        import gzip
        import hashlib

        plain = b"@HD\tVN:1.6\n" + b"@SQ\tSN:chr1\tLN:248956422\n" * 3000
        blob = gzip.compress(plain)
        md5_b64 = base64.b64encode(hashlib.md5(blob).digest()).decode()
        dest = str(tmp_path / "o.dict")
        offset = None
        if partial is not None:
            # a fraction of the COMPRESSED length: this plaintext compresses to a few
            # hundred bytes, so a fixed byte count can silently exceed the whole file
            offset = int(len(blob) * partial)
            assert 0 < offset < len(blob)
            open(dest + ".k9pdl.gz", "wb").write(blob[:offset])

        with Server(blob) as server:
            server.state.stored_gzip = True
            server.state.decoded_payload = plain
            if configure:
                configure(server.state)
            # the HEAD a real transcoding-eligible object returns: no Content-Encoding
            headers = ("HTTP/1.1 200 OK\r\n"
                       "x-goog-stored-content-encoding: gzip\r\n"
                       "x-goog-stored-content-length: {}\r\n"
                       "x-goog-hash: md5={}\r\n\r\n").format(len(blob), md5_b64).encode()

            class Fake:
                stdout = headers

            with patch("os.path.exists", return_value=False), \
                 patch("canine.localization.file_handlers.subprocess.run",
                       return_value=Fake()):
                handler = fh.get_file_handler(server.url("o.dict"), check_hash=True,
                                              **kwargs)
            assert handler.body_is_compressed, "the probe missed a stored-gzip object"
            script = "#!/bin/bash\nset -e\n" + handler.localization_command(dest) + "\n"
            result = subprocess.run(["bash", "-e", "-c", script], capture_output=True,
                                    text=True, timeout=300)
            ranges = list(server.state.seen_ranges)
        self.offset = offset
        return result, dest, plain, ranges

    def test_the_ordinary_case_is_verified_and_decoded(self, tmp_path):
        """
        Modelled on a real ordinary object: served decoded to any request that does not
        ask for gzip. So this fails -- on the result, not on the command string -- if
        the fetch stops asking for the stored bytes.
        """
        def ordinary(state):
            state.decode_unless_accepts_gzip = True

        result, dest, plain, _ = self._run(tmp_path, ordinary, parallel_download=False)
        assert result.returncode == 0, result.stderr
        assert open(dest, "rb").read() == plain
        assert "arrived already decoded" not in result.stderr

    def test_a_body_decoded_anyway_is_placed_not_deleted(self, tmp_path):
        """
        It cannot be verified -- the digest covers bytes that never arrived -- but it is
        the right content, and the old pipeline deleted it as "corrupted", turning an
        unverified success into a failure.
        """
        def edge(state):
            state.always_decode = True

        result, dest, plain, _ = self._run(tmp_path, edge, parallel_download=False)
        assert result.returncode == 0, result.stderr
        assert open(dest, "rb").read() == plain
        assert "arrived already decoded" in result.stderr
        assert "deleting corrupted" not in result.stdout + result.stderr
        assert not os.path.exists(dest + ".k9pdl.gz")

    def test_a_fresh_fetch_uses_a_range_to_get_the_stored_bytes(self, tmp_path):
        """
        application/gzip-typed objects are decoded unless the request has a Range. Those
        are exactly the mislabelled .gz uploads the keep-as-is check exists for, and a
        decoded arrival also skips verification.
        """
        def typed_gzip(state):
            state.decode_unless_ranged = True

        result, dest, plain, ranges = self._run(tmp_path, typed_gzip,
                                                parallel_download=False)
        assert result.returncode == 0, result.stderr
        assert open(dest, "rb").read() == plain
        assert "arrived already decoded" not in result.stderr, (
            "the fresh fetch had no Range, so the server decoded it and it went unverified")
        assert ranges == ["bytes=0-"], ranges

    def test_the_downloader_path_reaches_the_same_result(self, tmp_path):
        """Range ignored, so the downloader declines and hands over to the pipeline."""
        def typed_gzip(state):
            state.decode_unless_ranged = True

        result, dest, plain, _ = self._run(tmp_path, typed_gzip,
                                           download_min_chunk=MIB, download_connections=4)
        assert result.returncode == 0, result.stderr
        assert open(dest, "rb").read() == plain
        assert "arrived already decoded" not in result.stderr

    def test_a_resume_never_sends_the_fresh_range(self, tmp_path):
        """
        A custom Range header REPLACES the one `curl -C -` computes. Sent on a resume it
        re-requests from zero and appends -- every byte already on disk duplicated.
        """
        result, dest, plain, ranges = self._run(tmp_path, partial=0.5,
                                                parallel_download=False)
        assert result.returncode == 0, result.stderr
        assert open(dest, "rb").read() == plain
        assert ranges == ["bytes={}-".format(self.offset)], ranges

    def test_an_unresumable_partial_restarts_instead_of_failing_forever(self, tmp_path):
        """
        Against a server that ignores Range, `curl -C -` exits 33 on a surviving partial
        -- and the partial survives by design, so without a restart every retry fails
        identically.
        """
        def typed_gzip(state):
            state.decode_unless_ranged = True

        result, dest, plain, ranges = self._run(tmp_path, typed_gzip, partial=0.5,
                                                parallel_download=False)
        assert result.returncode == 0, result.stderr
        assert open(dest, "rb").read() == plain
        assert ranges == ["bytes={}-".format(self.offset), "bytes=0-"], ranges


class TestAllPathsProduceTheSameFile:
    """
    A compressed body must decompress on every route, not just the primary one. Which path
    runs depends on configuration and on whether the server honors Range, so if they
    disagreed the localized file would differ for reasons invisible to the pipeline.
    """

    def _run(self, tmp_path, label, **kwargs):
        import gzip
        import hashlib

        plain = b"@HD\tVN:1.6\n" + b"@SQ\tSN:chr1\tLN:248956422\n" * 3000
        blob = gzip.compress(plain)
        digest = hashlib.md5(blob).hexdigest()

        with Server(blob) as server:
            server.state.stored_gzip = True
            headers = (
                "HTTP/1.1 200 OK\r\ncontent-encoding: gzip\r\n"
                "content-length: {}\r\netag: \"{}\"\r\n\r\n".format(len(blob), digest)
            ).encode()

            class Fake:
                stdout = headers

            with patch("os.path.exists", return_value=False), \
                 patch("canine.localization.file_handlers.subprocess.run",
                       return_value=Fake()):
                handler = fh.get_file_handler(server.url("o.dict"), check_md5=True,
                                              **kwargs)
            dest = str(tmp_path / "o.dict")
            script = "#!/bin/bash\nset -e\n" + handler.localization_command(dest) + "\n"
            if label == "no script":
                # force every resolver candidate to miss, so the else branch runs
                script = script.replace(fh._pdl_installed_path(), "/nonexistent/pdl.py")
                script = script.replace('"${CANINE_ROOT:-}/parallel_download.py"',
                                        '"/nonexistent/staged.py"')
            result = subprocess.run(["bash", "-e", "-c", script], capture_output=True,
                                    text=True, timeout=300)
        return result, dest, plain

    def test_parallel_path(self, tmp_path):
        result, dest, plain = self._run(tmp_path, "parallel", download_min_chunk=MIB,
                                        download_connections=4)
        assert result.returncode == 0, result.stdout + result.stderr
        assert open(dest, "rb").read() == plain

    def test_opt_out_path(self, tmp_path):
        result, dest, plain = self._run(tmp_path, "opt-out", parallel_download=False)
        assert result.returncode == 0, result.stdout + result.stderr
        assert open(dest, "rb").read() == plain

    def test_no_downloader_available(self, tmp_path):
        result, dest, plain = self._run(tmp_path, "no script", download_min_chunk=MIB)
        assert result.returncode == 0, result.stdout + result.stderr
        assert open(dest, "rb").read() == plain

    def test_the_compressed_sidecar_is_cleaned_up(self, tmp_path):
        """Peak disk is compressed + decompressed; the sidecar must not linger."""
        result, dest, _ = self._run(tmp_path, "opt-out", parallel_download=False)
        assert result.returncode == 0
        assert not os.path.exists(dest + ".k9pdl.gz")

    def test_the_fallback_verifies_before_decompressing(self, tmp_path):
        """
        The digest covers the compressed bytes, so it has to be checked on the sidecar. A
        gate applied after decompression would compare the decoded file against a digest
        of the encoded one -- a guaranteed false failure on every correct download.
        """
        import gzip
        import hashlib

        blob = gzip.compress(b"payload" * 5000)
        with Server(blob) as server:
            server.state.stored_gzip = True
            headers = (
                "HTTP/1.1 200 OK\r\ncontent-encoding: gzip\r\n"
                "content-length: {}\r\netag: \"{}\"\r\n\r\n".format(len(blob), "0" * 32)
            ).encode()

            class Fake:
                stdout = headers

            with patch("os.path.exists", return_value=False), \
                 patch("canine.localization.file_handlers.subprocess.run",
                       return_value=Fake()):
                handler = fh.get_file_handler(server.url("o.dict"), check_md5=True,
                                              parallel_download=False)
            dest = str(tmp_path / "o.dict")
            script = "#!/bin/bash\nset -e\n" + handler.localization_command(dest) + "\n"
            result = subprocess.run(["bash", "-e", "-c", script], capture_output=True,
                                    text=True, timeout=300)

        assert result.returncode != 0
        assert not os.path.exists(dest), "decompressed despite failing verification"

    def test_the_url_is_not_corrupted_when_retargeting_to_the_sidecar(self):
        """
        Regression. The sidecar target was first produced by string-replacing the
        destination path in the finished command -- which also matched the same substring
        inside the URL, emitting
        `-o /d/o.vcf.k9pdl.gz 'https://h/d/o.vcf.k9pdl.gz'`. The command is now built
        against a target rather than rewritten.
        """
        headers = (b"HTTP/1.1 200 OK\r\ncontent-encoding: gzip\r\n"
                   b"content-length: 500000\r\n"
                   b'etag: "' + EMPTY_MD5_HEX.encode() + b'"\r\n\r\n')

        class Fake:
            stdout = headers

        # a URL whose tail is exactly the destination path is what triggered it
        url = "https://example.org/d/o.vcf"
        with patch("os.path.exists", return_value=False), \
             patch("canine.localization.file_handlers.subprocess.run",
                   return_value=Fake()):
            handler = fh.get_file_handler(url, check_md5=True, parallel_download=False)
        script = handler.localization_command("/d/o.vcf")
        assert url + "'" in script or "'{}'".format(url) in script, \
            "the URL was rewritten:\n" + script
        assert "o.vcf.k9pdl.gz'" not in script.replace("-o /d/o.vcf.k9pdl.gz", "")


class TestFallbackHandlesDoubleCompression:
    """
    The emitted fallback must resolve the .gz-name ambiguity the same way the downloader
    does, or the localized file would depend on which path ran.
    """

    VCF = b"##fileformat=VCFv4.2\n" + b"chr1\t1\t.\tA\tT\t.\t.\t.\n" * 800

    def _localize(self, tmp_path, stored, name):
        import hashlib

        digest = hashlib.md5(stored).hexdigest()
        with Server(stored) as server:
            server.state.stored_gzip = True
            headers = (
                "HTTP/1.1 200 OK\r\ncontent-encoding: gzip\r\ncontent-length: {}\r\n"
                "etag: \"{}\"\r\n\r\n".format(len(stored), digest)
            ).encode()

            class Fake:
                stdout = headers

            with patch("os.path.exists", return_value=False), \
                 patch("canine.localization.file_handlers.subprocess.run",
                       return_value=Fake()):
                handler = fh.get_file_handler(server.url(name), check_md5=True,
                                              parallel_download=False)
            dest = str(tmp_path / name)
            script = "#!/bin/bash\nset -e\n" + handler.localization_command(dest) + "\n"
            result = subprocess.run(["bash", "-e", "-c", script], capture_output=True,
                                    text=True, timeout=300)
        return result, dest

    def test_doubly_compressed_yields_the_original_gz(self, tmp_path):
        import gzip
        result, dest = self._localize(
            tmp_path, gzip.compress(gzip.compress(self.VCF)), "d.vcf.gz")
        assert result.returncode == 0, result.stdout + result.stderr
        assert open(dest, "rb").read() == gzip.compress(self.VCF)

    def test_singly_compressed_gz_name_keeps_the_stored_bytes(self, tmp_path):
        import gzip
        stored = gzip.compress(self.VCF)
        result, dest = self._localize(tmp_path, stored, "d.vcf.gz")
        assert result.returncode == 0, result.stdout + result.stderr
        assert open(dest, "rb").read() == stored

    def test_a_plain_name_is_decompressed(self, tmp_path):
        import gzip
        result, dest = self._localize(tmp_path, gzip.compress(self.VCF), "d.vcf")
        assert result.returncode == 0, result.stdout + result.stderr
        assert open(dest, "rb").read() == self.VCF

    def test_no_intermediate_files_are_left(self, tmp_path):
        import gzip
        result, dest = self._localize(tmp_path, gzip.compress(self.VCF), "d.vcf.gz")
        assert result.returncode == 0
        leftovers = [p.name for p in tmp_path.iterdir() if p.name != "d.vcf.gz"]
        assert leftovers == [], leftovers

    def test_the_conditional_is_valid_bash(self, tmp_path):
        import gzip

        class Fake:
            stdout = (b"HTTP/1.1 200 OK\r\ncontent-encoding: gzip\r\n"
                      b"content-length: 100\r\n\r\n")

        with patch("os.path.exists", return_value=False), \
             patch("canine.localization.file_handlers.subprocess.run",
                   return_value=Fake()):
            handler = fh.get_file_handler("https://h/d/a.vcf.gz",
                                          parallel_download=False)
        script = handler.localization_command("/mnt/x y/a.vcf.gz")
        assert bash_ok(script).returncode == 0, bash_ok(script).stderr
        assert bash_ok(debug_sh_transform(script)).returncode == 0


class TestNameMatchesContent:
    """
    The invariant that matters to consumers, stated in their terms: several pipelines
    dispatch on the file extension, so the localized file must be what its name says.

    This is the property the double-compression rule produces, and it is why the gzip
    decision is safe for extension-dispatching callers. The single case that changed --
    a plain-named file that used to receive gzip bytes -- was precisely a name lying about
    its content, so the change removes an inconsistency rather than introducing one. The
    metadata-inconsistency case (a .gz whose content-encoding was set by mistake) does not
    change at all.
    """

    VCF = b"##fileformat=VCFv4.2\n" * 300

    def _localize(self, tmp_path, stored, name, **kwargs):
        import hashlib

        digest = hashlib.md5(stored).hexdigest()
        with Server(stored) as server:
            server.state.stored_gzip = True
            headers = (
                "HTTP/1.1 200 OK\r\ncontent-encoding: gzip\r\ncontent-length: {}\r\n"
                "etag: \"{}\"\r\n\r\n".format(len(stored), digest)
            ).encode()

            class Fake:
                stdout = headers

            with patch("os.path.exists", return_value=False), \
                 patch("canine.localization.file_handlers.subprocess.run",
                       return_value=Fake()):
                handler = fh.get_file_handler(server.url(name), check_md5=True, **kwargs)
            dest = str(tmp_path / name)
            script = "#!/bin/bash\nset -e\n" + handler.localization_command(dest) + "\n"
            result = subprocess.run(["bash", "-e", "-c", script], capture_output=True,
                                    text=True, timeout=300)
        assert result.returncode == 0, result.stdout + result.stderr
        return open(dest, "rb").read()

    def _stored_for(self, label):
        import bz2
        import gzip
        bam = gzip.compress(b"BAM\x01" + b"\x00" * 800)
        return {
            "plain-gzip-encoded": ("d.vcf", gzip.compress(self.VCF)),
            "gz-single": ("d.vcf.gz", gzip.compress(self.VCF)),
            "gz-double": ("d.vcf.gz", gzip.compress(gzip.compress(self.VCF))),
            "bam-single": ("d.bam", bam),
            "bam-double": ("d.bam", gzip.compress(bam)),
            "bz2-single": ("d.vcf.bz2", bz2.compress(self.VCF)),
            "bz2-double": ("d.vcf.bz2", gzip.compress(bz2.compress(self.VCF))),
            "dict-gzip-encoded": ("d.dict", gzip.compress(self.VCF)),
        }[label]

    ALL = ["plain-gzip-encoded", "gz-single", "gz-double", "bam-single", "bam-double",
           "bz2-single", "bz2-double", "dict-gzip-encoded"]

    @pytest.mark.parametrize("label", ALL)
    @pytest.mark.parametrize("route", [{}, {"parallel_download": False}])
    def test_the_content_matches_the_extension(self, tmp_path, label, route):
        """
        Checked on both routes, since a pipeline cannot know which one ran.
        """
        name, stored = self._stored_for(label)
        got = self._localize(tmp_path, stored, name, download_min_chunk=MIB, **route)
        magic = fh.expected_magic(name)
        if magic is None:
            assert not got.startswith(b"\x1f\x8b"), \
                "{}: a plain-named file received compressed bytes".format(name)
        else:
            assert got.startswith(magic), \
                "{}: content does not match what the name promises".format(name)

    def test_the_metadata_mistake_case_is_unchanged_from_before(self, tmp_path):
        """
        The case most likely to exist in the wild -- an already-compressed file uploaded
        with the content-encoding metadata set by mistake -- localizes to exactly the
        bytes the old single-stream command produced, so nothing depending on it moves.
        """
        import gzip
        stored = gzip.compress(self.VCF)
        assert self._localize(tmp_path, stored, "d.vcf.gz",
                              parallel_download=False) == stored
