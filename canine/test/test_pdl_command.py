"""
Emitted-script contract tests for the _pdl_path / _pdl_command helpers.

These guard the remote-execution assumptions that unit-testing the Python alone would
miss. The commands are bash strings concatenated into a per-job localization.sh that
runs on a compute node under `set -e`, and the same strings are also executed on the
controller for deduplicated common inputs -- so what matters is how the shell behaves,
not what the Python returned.
"""

import os
import shutil
import stat
import subprocess

import pytest

from canine.localization import file_handlers as fh
from pdl_server import Server

MIB = 1024 * 1024


def bash_ok(script, extra=""):
    """Syntax-check a script the way the compute node's shell would parse it."""
    return subprocess.run(["bash", "-n"], input=script + extra, text=True,
                          capture_output=True)


def debug_sh_transform(script):
    """
    Apply what debug.sh does when it regenerates a runnable script: drop #DEBUG_OMIT
    lines and rewrite a trailing ` - <<` heredoc invocation.
    """
    lines = [line for line in script.split("\n") if "#DEBUG_OMIT" not in line]
    if lines:
        lines[-1] = lines[-1].split(" - <<")[0]
    return "\n".join(lines)


def run_script(script, env=None, cwd=None):
    full = dict(os.environ)
    if env:
        full.update(env)
    return subprocess.run(["bash", "-e", "-c", script], capture_output=True, text=True,
                          env=full, cwd=cwd, timeout=300)


def command_for(dest, url, size, **kwargs):
    return "\n".join(fh._pdl_command(url, dest, size, **kwargs))


# ---------------------------------------------------------------------------
# shell contract
# ---------------------------------------------------------------------------

class TestEmittedShellContract:

    def _variants(self, dest="/tmp/obj.bin"):
        legacy = "curl -C - -o {} 'https://h/o'".format(dest)
        return {
            "plain": dict(),
            "with legacy": dict(legacy_cmd=legacy),
            "with md5": dict(md5="d41d8cd98f00b204e9800998ecf8427e", legacy_cmd=legacy),
            "with etag": dict(etag='"abc-3"', part_length=8 * MIB, legacy_cmd=legacy),
            "with headers": dict(headers=["X-Auth-Token: s3cr3t"], legacy_cmd=legacy),
            "with s3": dict(url=None, s3={"bucket": "b", "key": "k/o.bam",
                                          "extra_args": "--endpoint-url https://e"}),
            "with refresh": dict(url_refresh_cmd="curl -s x | python3 -c 'pass'"),
            "with work dirs": dict(work_dirs=["/mnt/rwdisks/canine-x", "/local"]),
        }

    @pytest.mark.parametrize("label", list(_variants(None)))
    def test_is_valid_bash(self, label):
        kwargs = dict(self._variants()[label])
        url = kwargs.pop("url", "https://h/o.bam")
        script = command_for("/tmp/obj.bin", url, 100 * MIB, **kwargs)
        result = bash_ok(script)
        assert result.returncode == 0, result.stderr + "\n---\n" + script

    @pytest.mark.parametrize("label", list(_variants(None)))
    def test_survives_the_debug_sh_transform(self, label):
        kwargs = dict(self._variants()[label])
        url = kwargs.pop("url", "https://h/o.bam")
        script = command_for("/tmp/obj.bin", url, 100 * MIB, **kwargs)
        result = bash_ok(debug_sh_transform(script))
        assert result.returncode == 0, result.stderr

    @pytest.mark.parametrize("label", list(_variants(None)))
    def test_contains_no_debug_omit_marker(self, label):
        """
        debug.sh filters those lines out, so an emitted line carrying the marker would be
        silently dropped from the regenerated script.
        """
        kwargs = dict(self._variants()[label])
        url = kwargs.pop("url", "https://h/o.bam")
        assert "#DEBUG_OMIT" not in command_for("/tmp/obj.bin", url, 100 * MIB, **kwargs)

    @pytest.mark.parametrize("label", list(_variants(None)))
    def test_contains_no_heredoc(self, label):
        """
        The staged-file approach makes heredocs unnecessary, and they are fragile under
        debug.sh's rewriting.
        """
        kwargs = dict(self._variants()[label])
        url = kwargs.pop("url", "https://h/o.bam")
        script = command_for("/tmp/obj.bin", url, 100 * MIB, **kwargs)
        assert "<<" not in script

    def test_still_valid_when_appended_to_other_commands(self):
        """
        Commands are concatenated into one localization.sh, so a block that only parses
        in isolation is not good enough.
        """
        first = command_for("/tmp/a.bin", "https://h/a", MIB, legacy_cmd="true")
        second = command_for("/tmp/b.bin", "https://h/b", MIB, legacy_cmd="true")
        combined = "#!/bin/bash\nset -e\n" + first + "\n" + second + "\n"
        assert bash_ok(combined).returncode == 0

    def test_secrets_are_quoted_not_split(self):
        script = command_for("/tmp/o.bin", "https://h/o", MIB,
                             headers=["X-Auth-Token: has spaces and 'quotes'"])
        assert bash_ok(script).returncode == 0


# ---------------------------------------------------------------------------
# script resolution (§8.2 d/e)
# ---------------------------------------------------------------------------

class TestScriptResolution:
    """
    The resolver runs in two very different contexts: on a compute node where the shared
    mount is populated, and on the controller during pick_common_inputs where it is not
    yet. Both must find a usable script.
    """

    RESOLVE = "\n".join(fh._pdl_path()) + '\necho "K9_PDL=$K9_PDL"\necho "RUN=$K9_PDL_RUN"'

    def test_finds_the_installed_package_copy_with_canine_root_unset(self, tmp_path):
        """The controller context: CANINE_ROOT is not set at all."""
        script = "unset CANINE_ROOT\n" + self.RESOLVE
        result = run_script(script)
        assert result.returncode == 0, result.stderr
        assert fh._pdl_installed_path() in result.stdout

    def test_finds_the_installed_copy_when_staging_is_empty(self, tmp_path):
        """
        pick_common_inputs runs before the staging copy, so CANINE_ROOT can be set and
        point at a directory that does not have the script yet.
        """
        result = run_script(self.RESOLVE, env={"CANINE_ROOT": str(tmp_path)})
        assert result.returncode == 0, result.stderr
        assert fh._pdl_installed_path() in result.stdout

    def test_prefers_the_installed_copy_over_a_populated_staging_dir(self, tmp_path):
        """
        Installed-first is deliberate: if CANINE_ROOT becomes a gcsfuse mount, its stat
        cache can serve a stale or truncated view of a freshly staged script, so the node
        should not depend on mount freshness when it does not have to.
        """
        staged = tmp_path / fh.PDL_SCRIPT_NAME
        shutil.copyfile(fh._pdl_installed_path(), str(staged))
        result = run_script(self.RESOLVE, env={"CANINE_ROOT": str(tmp_path)})
        assert fh._pdl_installed_path() in result.stdout
        assert str(staged) not in result.stdout

    def test_falls_back_to_the_staged_copy_when_the_package_copy_is_gone(self, tmp_path,
                                                                        monkeypatch):
        """Simulates a node where canine is not installed, only the staging mount."""
        staged = tmp_path / fh.PDL_SCRIPT_NAME
        shutil.copyfile(fh._pdl_installed_path(), str(staged))
        monkeypatch.setattr(fh, "_pdl_installed_path",
                            lambda: str(tmp_path / "absent" / fh.PDL_SCRIPT_NAME))
        script = "\n".join(fh._pdl_path()) + '\necho "K9_PDL=$K9_PDL"'
        result = run_script(script, env={"CANINE_ROOT": str(tmp_path)})
        assert result.returncode == 0, result.stderr
        assert str(staged) in result.stdout

    def test_a_truncated_candidate_is_skipped(self, tmp_path, monkeypatch):
        """
        The sentinel's whole purpose: a partially-visible copy must be skipped, not
        executed. Running half a script is worse than not finding one.
        """
        truncated = tmp_path / "pkg" / fh.PDL_SCRIPT_NAME
        truncated.parent.mkdir()
        with open(fh._pdl_installed_path()) as source:
            complete = source.read()
        truncated.write_text(complete[: len(complete) // 2])
        assert fh.PDL_EOF_SENTINEL not in truncated.read_text().splitlines()[-1]

        good = tmp_path / "staged" / fh.PDL_SCRIPT_NAME
        good.parent.mkdir()
        shutil.copyfile(fh._pdl_installed_path(), str(good))

        monkeypatch.setattr(fh, "_pdl_installed_path", lambda: str(truncated))
        script = "\n".join(fh._pdl_path()) + '\necho "K9_PDL=$K9_PDL"'
        result = run_script(script, env={"CANINE_ROOT": str(good.parent)})
        assert str(good) in result.stdout
        assert str(truncated) not in result.stdout

    def test_empty_candidate_is_skipped(self, tmp_path, monkeypatch):
        empty = tmp_path / "pkg" / fh.PDL_SCRIPT_NAME
        empty.parent.mkdir()
        empty.write_text("")
        monkeypatch.setattr(fh, "_pdl_installed_path", lambda: str(empty))
        script = "\n".join(fh._pdl_path()) + '\necho "K9_PDL=$K9_PDL"'
        result = run_script(script, env={"CANINE_ROOT": str(tmp_path / "nowhere")})
        assert result.returncode == 0, result.stderr
        assert "K9_PDL=" in result.stdout and str(empty) not in result.stdout

    def test_resolution_does_not_abort_under_set_e(self, tmp_path):
        """
        A failing test as the last command of a line would abort the whole localization,
        so every probe has to be guarded.
        """
        script = "set -e\n" + "\n".join(fh._pdl_path()) + "\necho survived"
        result = run_script(script, env={"CANINE_ROOT": str(tmp_path / "absent")})
        assert result.returncode == 0, result.stderr
        assert "survived" in result.stdout


class TestInvocationMode:

    def test_executable_copy_is_invoked_directly(self, tmp_path, monkeypatch):
        staged = tmp_path / fh.PDL_SCRIPT_NAME
        shutil.copyfile(fh._pdl_installed_path(), str(staged))
        os.chmod(str(staged), 0o755)
        monkeypatch.setattr(fh, "_pdl_installed_path", lambda: str(staged))
        script = "\n".join(fh._pdl_path()) + '\necho "RUN=$K9_PDL_RUN"'
        result = run_script(script)
        assert "RUN={}".format(staged) in result.stdout
        assert "python3" not in result.stdout

    def test_non_executable_copy_falls_back_to_python3(self, tmp_path, monkeypatch):
        """
        gcsfuse cannot represent an exec bit, a mount may be noexec, and chmod can fail
        under root_squash -- so correctness must not depend on the bit.
        """
        staged = tmp_path / fh.PDL_SCRIPT_NAME
        shutil.copyfile(fh._pdl_installed_path(), str(staged))
        os.chmod(str(staged), 0o644)
        monkeypatch.setattr(fh, "_pdl_installed_path", lambda: str(staged))
        script = "\n".join(fh._pdl_path()) + '\necho "RUN=$K9_PDL_RUN"'
        result = run_script(script)
        assert "RUN=python3 {}".format(staged) in result.stdout


class TestLegacyFallback:

    def test_legacy_command_runs_when_no_script_resolves(self, tmp_path, monkeypatch):
        """
        Never invoke an empty path. If nothing usable is found the emitted command must
        take the existing single-stream path instead.
        """
        monkeypatch.setattr(fh, "_pdl_installed_path",
                            lambda: str(tmp_path / "absent" / fh.PDL_SCRIPT_NAME))
        sentinel = tmp_path / "legacy-ran"
        script = command_for(str(tmp_path / "o.bin"), "https://h/o", MIB,
                             legacy_cmd="touch {}".format(sentinel))
        result = run_script(script, env={"CANINE_ROOT": str(tmp_path / "empty")})
        assert result.returncode == 0, result.stderr
        assert sentinel.exists(), "legacy command did not run"

    def test_legacy_command_is_passed_through_for_the_in_script_fallback(self):
        legacy = "curl -C - -o /tmp/o.bin 'https://h/o'"
        script = command_for("/tmp/o.bin", "https://h/o", MIB, legacy_cmd=legacy)
        assert "--legacy-cmd" in script


# ---------------------------------------------------------------------------
# argument construction
# ---------------------------------------------------------------------------

class TestArguments:

    def test_etag_needs_a_part_length_to_be_verifiable(self):
        """
        Without the part length there is nothing to compare an md5-of-md5s against, so
        passing the ETag alone would ask for a check that cannot be performed.
        """
        without = command_for("/tmp/o", "https://h/o", MIB, etag='"abc-3"')
        assert "--check-etag" not in without
        with_length = command_for("/tmp/o", "https://h/o", MIB, etag='"abc-3"',
                                  part_length=8 * MIB)
        assert "--check-etag" in with_length and "--part-length" in with_length

    def test_etag_takes_precedence_over_md5(self):
        script = command_for("/tmp/o", "https://h/o", MIB, etag='"a-2"',
                             part_length=MIB, md5="0" * 32)
        assert "--check-etag" in script and "--check-md5" not in script

    def test_s3_source_needs_no_url(self):
        script = command_for("/tmp/o", None, MIB,
                             s3={"bucket": "b", "key": "k"})
        assert "--s3-bucket" in script and "--url" not in script

    def test_connection_and_chunk_defaults_are_explicit(self):
        script = command_for("/tmp/o", "https://h/o", MIB)
        assert "--connections {}".format(fh.DEFAULT_DOWNLOAD_CONNECTIONS) in script
        assert "--min-chunk {}".format(fh.DEFAULT_DOWNLOAD_MIN_CHUNK) in script

    def test_overrides_are_honored(self):
        script = command_for("/tmp/o", "https://h/o", MIB, connections=12,
                             min_chunk=4 * MIB)
        assert "--connections 12" in script
        assert "--min-chunk {}".format(4 * MIB) in script

    def test_work_dirs_are_repeatable(self):
        script = command_for("/tmp/o", "https://h/o", MIB,
                             work_dirs=["/a", "/b"])
        assert script.count("--work-dir") == 2

    def test_dest_is_interpolated_verbatim(self):
        """
        Handlers pass self.localized_path, which is already shell-quoted. Re-quoting it
        would produce a doubly-quoted path pointing at the wrong file.
        """
        pre_quoted = "'/mnt/my disk'/'o b.bin'"
        script = command_for(pre_quoted, "https://h/o", MIB)
        assert "--dest " + pre_quoted in script
        assert bash_ok(script).returncode == 0

    def test_a_pre_quoted_dest_resolves_to_the_intended_path(self, tmp_path):
        """
        Verifies the quoting end to end rather than by inspection: run the emitted
        --dest through the shell and check where it actually lands.
        """
        import shlex as _shlex
        directory = tmp_path / "my dir"
        directory.mkdir()
        pre_quoted = "{}/{}".format(_shlex.quote(str(directory)),
                                    _shlex.quote("o b.bin"))
        script = "printf '%s' " + pre_quoted
        result = run_script("touch {}; ls {}".format(pre_quoted, pre_quoted))
        assert result.returncode == 0, result.stderr
        assert (directory / "o b.bin").exists()


# ---------------------------------------------------------------------------
# end to end under set -e (§8.2 f)
# ---------------------------------------------------------------------------

class TestGeneratedScriptRunsEndToEnd:

    def test_several_inputs_in_one_script(self, tmp_path):
        """
        The real shape: a multi-input localization.sh running under `set -e`, where an
        unguarded failing test in any block would abort the whole thing.
        """
        payloads = {
            "a.bin": os.urandom(3 * MIB + 11),
            "b.bin": os.urandom(2 * MIB),
            "c.bin": os.urandom(999),
        }
        import hashlib

        # one server per payload, so each object is served from its own range space
        blocks = ["#!/bin/bash", "set -e"]
        servers = []
        try:
            for name, data in payloads.items():
                server = Server(data)
                servers.append(server)
                dest = str(tmp_path / name)
                blocks += fh._pdl_command(
                    server.url(name), dest, len(data),
                    md5=hashlib.md5(data).hexdigest(),
                    min_chunk=MIB, connections=4,
                    legacy_cmd="curl -sS -o {} {}".format(dest, server.url(name)),
                )
            script = "\n".join(blocks) + "\n"
            assert bash_ok(script).returncode == 0, "generated script is not valid bash"
            result = run_script(script)
        finally:
            for server in servers:
                server.close()

        assert result.returncode == 0, result.stdout + result.stderr
        for name, data in payloads.items():
            written = (tmp_path / name).read_bytes()
            assert written == data, "{} differs".format(name)

    def test_marker_makes_a_second_run_cheap(self, tmp_path):
        import hashlib
        data = os.urandom(3 * MIB + 7)
        dest = str(tmp_path / "obj.bin")
        with Server(data) as server:
            block = fh._pdl_command(server.url(), dest, len(data),
                                    md5=hashlib.md5(data).hexdigest(),
                                    min_chunk=MIB, connections=4)
            script = "#!/bin/bash\nset -e\n" + "\n".join(block) + "\n"
            first = run_script(script)
            assert first.returncode == 0, first.stderr
            before = server.state.snapshot()["sent"]
            second = run_script(script)
            after = server.state.snapshot()["sent"]

        assert second.returncode == 0, second.stderr
        assert after == before, "re-run transferred {} bytes".format(after - before)

    def test_script_is_reusable_from_a_different_cwd(self, tmp_path):
        """
        The resolver interpolates an absolute package path host-side, so the command must
        not depend on where it is run from.
        """
        import hashlib
        data = os.urandom(2 * MIB)
        dest = str(tmp_path / "obj.bin")
        with Server(data) as server:
            block = fh._pdl_command(server.url(), dest, len(data),
                                    md5=hashlib.md5(data).hexdigest(),
                                    min_chunk=MIB, connections=2)
            script = "#!/bin/bash\nset -e\n" + "\n".join(block) + "\n"
            result = run_script(script, cwd="/")
        assert result.returncode == 0, result.stderr
        assert (tmp_path / "obj.bin").read_bytes() == data
