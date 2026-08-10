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
