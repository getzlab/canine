"""
Tests for benchmark_localization.py's credential resolution.

Not throughput measurement -- that needs a real worker. This covers the part with actual
logic in it, and the part where a mistake leaks a secret: credentials are read from the
canonical aws location rather than passed on a command line, because an argument is
visible in `ps` to every user on the box and lands in shell history. When the keys are
issued by someone else, that matters.
"""

import glob
import hashlib
import io
import inspect
import importlib.util
import os
import shlex
import subprocess
import sys
import tempfile
import threading
import time

import pytest

_spec = importlib.util.spec_from_file_location(
    "bench", os.path.join(os.path.dirname(__file__), "benchmark_localization.py"))
bench = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bench)

# Deliberately fabricated. The test asserts these never appear in output, and
# using a real key to prove a leak-check works would itself be the leak.
SECRET = "FAKEsecretFAKEsecretFAKEsecretFAKEsecret"
KEY_ID = "FAKEKEYIDFAKEKEYID00"


@pytest.fixture
def aws_home(tmp_path, monkeypatch):
    """A private ~/.aws, so a developer's real credentials are never consulted."""
    aws = tmp_path / ".aws"
    aws.mkdir()
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(aws / "credentials"))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(aws / "config"))
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    return aws


def args_for(command="probe", **kw):
    argv = [command]
    if command != "probe":
        argv += ["--dest-dir", "/tmp", "--size", "10"]
    for flag, value in kw.items():
        argv += ["--" + flag.replace("_", "-")] + ([] if value is True else [str(value)])
    return bench.build_parser().parse_args(argv)


class TestCredentialsComeFromTheCanonicalFile:

    def test_found_in_the_default_profile(self, aws_home):
        (aws_home / "credentials").write_text(
            "[default]\naws_access_key_id = {}\naws_secret_access_key = {}\n".format(
                KEY_ID, SECRET))
        source = bench.aws_credentials_source(args_for())
        assert source["found"]
        assert "[default]" in source["how"]

    def test_the_secret_is_never_in_the_reported_source(self, aws_home):
        (aws_home / "credentials").write_text(
            "[default]\naws_access_key_id = {}\naws_secret_access_key = {}\n".format(
                KEY_ID, SECRET))
        source = bench.aws_credentials_source(args_for())
        blob = repr(source)
        assert SECRET not in blob, "leaked the secret access key"
        assert KEY_ID not in blob, "leaked the access key id"

    def test_nothing_puts_credentials_on_the_command_line(self, aws_home):
        (aws_home / "credentials").write_text(
            "[default]\naws_access_key_id = {}\naws_secret_access_key = {}\n".format(
                KEY_ID, SECRET))
        a = args_for("sweep", s3_bucket="b", s3_key="k")
        extra = bench.s3_extra_args(a)
        assert SECRET not in extra and KEY_ID not in extra
        assert "--profile" not in extra          # no profile asked for
        source = bench.source_args(a, "/d/f", 10)
        assert not any(SECRET in x or KEY_ID in x for x in source)

    def test_a_named_profile_is_honoured(self, aws_home):
        (aws_home / "credentials").write_text(
            "[gdc]\naws_access_key_id = {}\naws_secret_access_key = {}\n".format(
                KEY_ID, SECRET))
        assert not bench.aws_credentials_source(args_for())["found"]
        found = bench.aws_credentials_source(args_for(s3_profile="gdc"))
        assert found["found"] and "[gdc]" in found["how"]
        assert "--profile gdc" in bench.s3_extra_args(args_for(s3_profile="gdc"))

    def test_env_credentials_are_recognised(self, aws_home, monkeypatch):
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", KEY_ID)
        source = bench.aws_credentials_source(args_for())
        assert source["found"]
        assert KEY_ID not in repr(source)

    def test_a_percent_in_a_secret_does_not_crash_the_parser(self, aws_home):
        """
        RawConfigParser, not ConfigParser: these files are not ours, and `%`
        interpolation on a foreign secret would raise instead of parsing.
        """
        (aws_home / "credentials").write_text(
            "[default]\naws_access_key_id = A\naws_secret_access_key = ab%cd%ef\n")
        assert bench.aws_credentials_source(args_for())["found"]

    def test_a_malformed_file_is_reported_not_raised(self, aws_home):
        (aws_home / "credentials").write_text("this is not ini\n= = =\n")
        source = bench.aws_credentials_source(args_for())
        assert not source["found"]
        assert "could not parse" in source["how"]

    def test_missing_file_says_so(self, aws_home):
        source = bench.aws_credentials_source(args_for())
        assert not source["found"] and "no " in source["how"]


class TestUnsignedFallback:

    def test_added_when_no_credentials_exist(self, aws_home):
        assert "--no-sign-request" in bench.s3_extra_args(args_for(s3_bucket="b"))

    def test_not_added_when_credentials_exist(self, aws_home):
        (aws_home / "credentials").write_text(
            "[default]\naws_access_key_id = A\naws_secret_access_key = B\n")
        assert "--no-sign-request" not in bench.s3_extra_args(args_for(s3_bucket="b"))

    def test_explicit_flag_overrides_existing_credentials(self, aws_home):
        (aws_home / "credentials").write_text(
            "[default]\naws_access_key_id = A\naws_secret_access_key = B\n")
        assert "--no-sign-request" in bench.s3_extra_args(
            args_for(s3_bucket="b", no_sign_request=True))


class TestEndpointResolution:

    def test_read_from_the_aws_config_file(self, aws_home):
        (aws_home / "config").write_text(
            "[default]\nendpoint_url = https://store.example.org\n")
        assert bench.aws_config_endpoint(args_for()) == "https://store.example.org"
        assert "--endpoint-url https://store.example.org" in \
            bench.s3_extra_args(args_for(s3_bucket="b"))

    def test_profile_sections_are_prefixed_in_the_config_file(self, aws_home):
        (aws_home / "config").write_text(
            "[profile gdc]\nendpoint_url = https://gdc.example.org\n")
        assert bench.aws_config_endpoint(args_for(s3_profile="gdc")) == \
            "https://gdc.example.org"

    def test_the_flag_wins_over_the_config_file(self, aws_home):
        (aws_home / "config").write_text(
            "[default]\nendpoint_url = https://from-config.example.org\n")
        extra = bench.s3_extra_args(
            args_for(s3_bucket="b", s3_endpoint_url="https://from-flag.example.org"))
        assert "from-flag" in extra and "from-config" not in extra

    def test_absent_config_means_amazon(self, aws_home):
        assert bench.aws_config_endpoint(args_for()) is None
        assert "--endpoint-url" not in bench.s3_extra_args(args_for(s3_bucket="b"))


class TestDownloaderResolution:
    """
    The benchmark used to hardcode ../localization/parallel_download.py, which forced
    anyone deploying two files onto a node to reconstruct that directory tree for a single
    file. The layout was imposed by the constant, not by anything real.
    """

    def test_a_sibling_copy_is_found_first(self, tmp_path, monkeypatch):
        monkeypatch.setattr(bench, "HERE", str(tmp_path))
        sibling = tmp_path / "parallel_download.py"
        sibling.write_text("# downloader\n")
        monkeypatch.setattr(bench, "DOWNLOADER_CANDIDATES", (
            str(sibling), str(tmp_path / "up" / "localization" / "parallel_download.py")))
        assert bench.downloader_path() == str(sibling)

    def test_the_repo_layout_still_works(self, tmp_path, monkeypatch):
        repo = tmp_path / "localization"
        repo.mkdir()
        target = repo / "parallel_download.py"
        target.write_text("# downloader\n")
        monkeypatch.setattr(bench, "DOWNLOADER_CANDIDATES", (
            str(tmp_path / "parallel_download.py"), str(target)))
        assert bench.downloader_path() == str(target)

    def test_explicit_flag_wins(self, tmp_path, monkeypatch):
        sibling = tmp_path / "parallel_download.py"
        sibling.write_text("x")
        chosen = tmp_path / "elsewhere.py"
        chosen.write_text("x")
        monkeypatch.setattr(bench, "DOWNLOADER_CANDIDATES", (str(sibling),))
        args = bench.build_parser().parse_args(
            ["sweep", "--url", "u", "--size", "1", "--dest-dir", "/d",
             "--downloader", str(chosen)])
        assert bench.downloader_path(args) == str(chosen)

    def test_environment_override(self, tmp_path, monkeypatch):
        chosen = tmp_path / "from-env.py"
        chosen.write_text("x")
        monkeypatch.setenv("K9PDL_DOWNLOADER", str(chosen))
        monkeypatch.setattr(bench, "DOWNLOADER_CANDIDATES", ())
        assert bench.downloader_path() == str(chosen)

    def test_none_when_nothing_is_found(self, tmp_path, monkeypatch):
        monkeypatch.delenv("K9PDL_DOWNLOADER", raising=False)
        monkeypatch.setattr(bench, "DOWNLOADER_CANDIDATES",
                            (str(tmp_path / "nope.py"),))
        assert bench.downloader_path() is None

    def test_the_real_repo_layout_resolves(self):
        """Guards against the candidate list drifting from the actual repo."""
        assert bench.downloader_path() is not None, \
            "cannot find parallel_download.py from the repo checkout"


class TestRunbookVariablesAreDefinedBeforeUse:
    """
    The runbook is a procedure someone follows top to bottom, so a variable referenced
    before the section that exports it is a defect in the document -- it renders as an
    empty flag rather than an error.

    This has happened three times: $PROJECT/$ZONE/$NODE were exported on the workstation
    and used on the node, ~/.aws was mounted before it existed, and $S3_* were used in §3
    but defined in §6.4. Each was caught by a human reading carefully. This checks it
    mechanically instead.
    """

    RUNBOOK = os.path.join(os.path.dirname(__file__), "BENCHMARK_RUNBOOK.md")

    # Provided by the shell or by a construct the checker does not model.
    AMBIENT = {
        "PATH", "HOME", "PWD", "USER", "SHELL", "PS1", "IFS", "OLDPWD", "RANDOM",
        "BASH_VERSION", "GOOGLE_APPLICATION_CREDENTIALS", "CLOUDSDK_CONFIG",
        "1", "2", "3", "@", "*", "?", "$", "!", "#", "0",
    }

    def undefined_uses(self):
        """
        Position-aware within a line, because `el=$((...)); [ $el -eq 0 ]` assigns and
        uses on one line and a line-granular check calls that a defect. Uses are compared
        against definitions that appear earlier in the document, including earlier in the
        same line.
        """
        import re
        with open(self.RUNBOOK) as handle:
            text = handle.read()
        defined, problems = set(self.AMBIENT), []
        for block in re.findall(r"```bash\n(.*?)```", text, re.S):
            for line in block.split("\n"):
                if line.strip().startswith("#"):
                    continue
                events = []
                for m in re.finditer(r"\$\{?([A-Za-z_][A-Za-z0-9_]*)\}?", line):
                    events.append((m.start(), "use", m.group(1)))
                for m in re.finditer(r"([A-Za-z_][A-Za-z0-9_]*)=", line):
                    events.append((m.start(), "def", m.group(1)))
                for m in re.finditer(r"\bfor\s+([A-Za-z_][A-Za-z0-9_]*)\s+in\b", line):
                    events.append((m.start(), "def", m.group(1)))
                for _, kind, name in sorted(events):
                    if kind == "def":
                        defined.add(name)
                    elif name not in defined:
                        problems.append((name, line.strip()))
        return problems

    def test_no_variable_is_used_before_it_is_defined(self):
        problems = self.undefined_uses()
        assert not problems, "\n".join(
            "${} used before definition: {}".format(n, l[:70]) for n, l in problems)

    def test_the_checker_would_notice(self, tmp_path, monkeypatch):
        """A checker that cannot fail is not a check."""
        broken = tmp_path / "broken.md"
        broken.write_text("```bash\necho $NEVER_SET\n```\n")
        monkeypatch.setattr(self, "RUNBOOK", str(broken))
        assert any(n == "NEVER_SET" for n, _ in self.undefined_uses())

    def test_same_line_assignment_then_use_is_not_a_defect(self, tmp_path, monkeypatch):
        """`el=$(...); [ $el -eq 0 ]` is fine; a line-granular check called it a defect."""
        ok = tmp_path / "ok.md"
        ok.write_text("```bash\nel=$(( 1 + 1 )); [ $el -eq 0 ] && el=1\n```\n")
        monkeypatch.setattr(self, "RUNBOOK", str(ok))
        assert self.undefined_uses() == []

    def test_use_before_assignment_on_the_same_line_is_still_caught(
            self, tmp_path, monkeypatch):
        broken = tmp_path / "broken.md"
        broken.write_text("```bash\necho $LATER; LATER=1\n```\n")
        monkeypatch.setattr(self, "RUNBOOK", str(broken))
        assert any(n == "LATER" for n, _ in self.undefined_uses())


class TestRangeProbeHandlesBinaryBodies:
    """
    The range probe crashed on the first real object it was pointed at:

        UnicodeDecodeError: 'utf-8' codec can't decode byte 0x8b in position 1

    0x8b at position 1 is gzip magic -- a BAM is BGZF. The probe was writing the body to
    /dev/stdout and capturing it with text=True, so any binary object killed it, which is
    every object this tool exists for.

    The second bug in the same call was latent and worse: capturing stdout meant that a
    store which IGNORED Range -- precisely what the check detects -- would have had its
    entire object pulled into memory.
    """

    def fake_aws(self, tmp_path, body, honour_range=True):
        """An `aws` stand-in writing a real body to the outfile it is given."""
        script = tmp_path / "aws"
        script.write_text(
            "#!/usr/bin/env python3\n"
            "import base64, json, os, sys\n"
            "argv = sys.argv[1:]\n"
            "BODY = base64.b64decode({!r})\n"
            "HONOUR = {!r}\n"
            "if 'head-object' in argv:\n"
            "    if '--part-number' in argv:\n"
            "        print(json.dumps({{'ContentLength': len(BODY)}}))\n"
            "    else:\n"
            "        print(json.dumps({{'ContentLength': len(BODY),\n"
            "                          'ETag': '\\\"d41d8cd98f00b204e9800998ecf8427e-3\\\"',\n"
            "                          'PartsCount': 3, 'AcceptRanges': 'bytes'}}))\n"
            "    sys.exit(0)\n"
            "if 'get-object' in argv:\n"
            "    out = argv[-1]\n"
            "    n = 1024\n"
            "    if '--range' in argv:\n"
            "        spec = argv[argv.index('--range')+1].split('=')[1]\n"
            "        a, _, b = spec.partition('-')\n"
            "        n = int(b) - int(a) + 1\n"
            "    data = BODY[:n] if HONOUR else BODY\n"
            "    open(out, 'wb').write(data)\n"
            "    print(json.dumps({{'ContentLength': len(data)}}))\n"
            "    sys.exit(0)\n"
            "sys.exit(2)\n".format(__import__("base64").b64encode(body).decode(),
                                   honour_range))
        script.chmod(0o755)
        return script

    def probe_with(self, tmp_path, monkeypatch, body, honour_range=True):
        self.fake_aws(tmp_path, body, honour_range)
        monkeypatch.setenv("PATH", "{}:{}".format(tmp_path, os.environ["PATH"]))
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "FAKE")
        args = bench.build_parser().parse_args(
            ["probe", "--s3-bucket", "b", "--s3-key", "k"])
        return bench.probe_s3_endpoint(args)

    def test_a_gzip_magic_body_does_not_raise(self, tmp_path, monkeypatch, capsys):
        """The exact failure: BGZF starts 1f 8b, which is not valid UTF-8."""
        body = b"\x1f\x8b\x08\x04" + os.urandom(4092)
        out = self.probe_with(tmp_path, monkeypatch, body)
        assert out["range_supported"] is True
        assert out["range_bytes_returned"] == 1024

    def test_arbitrary_binary_is_fine(self, tmp_path, monkeypatch, capsys):
        out = self.probe_with(tmp_path, monkeypatch, bytes(range(256)) * 20)
        assert out["range_supported"] is True

    def test_a_store_ignoring_range_is_detected_not_buffered(
            self, tmp_path, monkeypatch, capsys):
        """
        The check's whole purpose. Exiting 0 is not enough -- the byte count must match,
        or a store returning the entire object reads as success.
        """
        body = b"\x1f\x8b" + os.urandom(60000)
        out = self.probe_with(tmp_path, monkeypatch, body, honour_range=False)
        assert out["range_supported"] is False
        assert out["range_bytes_returned"] == len(body)
        assert "NOT HONOURED" in capsys.readouterr().out

    def test_no_temp_file_is_left_behind(self, tmp_path, monkeypatch, capsys):
        before = set(glob.glob(os.path.join(tempfile.gettempdir(), ".k9pdl-range-*")))
        self.probe_with(tmp_path, monkeypatch, b"\x1f\x8b" + os.urandom(4094))
        after = set(glob.glob(os.path.join(tempfile.gettempdir(), ".k9pdl-range-*")))
        assert after == before

    def test_accept_ranges_is_reported(self, tmp_path, monkeypatch, capsys):
        out = self.probe_with(tmp_path, monkeypatch, b"\x1f\x8b" + os.urandom(4094))
        assert out["accept_ranges"] == "bytes"


class TestTheStrideIsVerifiedNotAssumed:
    """
    The md5-of-md5s only reproduces the ETag if every non-final part is striden at its
    true length. S3 does NOT require parts to be equal -- only that non-final ones are
    >= 5 MiB -- so `head-object --part-number 1` is not by itself the stride.

    Getting it wrong is not a benign miss: verify() raises PermanentError on an ETag
    mismatch and discards the file, so a byte-perfect 279 GB download would be deleted
    and the job marked do-not-retry.
    """

    def fake_aws(self, tmp_path, monkeypatch, size, count, lengths):
        """`lengths` maps part number -> ContentLength."""
        script = tmp_path / "aws"
        script.write_text(
            "#!/usr/bin/env python3\n"
            "import json, sys\n"
            "argv = sys.argv[1:]\n"
            "LEN = {!r}\n"
            "if '--part-number' in argv:\n"
            "    n = int(argv[argv.index('--part-number')+1])\n"
            "    print(json.dumps({{'ContentLength': LEN[n]}}))\n"
            "else:\n"
            "    print(json.dumps({{'ContentLength': {}, 'PartsCount': {},\n"
            "                      'ETag': '\\\"{}-{}\\\"'}}))\n".format(
                lengths, size, count, "a" * 32, count))
        script.chmod(0o755)
        monkeypatch.setenv("PATH", "{}:{}".format(tmp_path, os.environ["PATH"]))
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "FAKE")
        return bench.build_parser().parse_args(
            ["probe", "--s3-bucket", "b", "--s3-key", "k"])

    def test_uniform_parts_yield_a_stride(self, tmp_path, monkeypatch):
        # 4 parts of 10, last of 5 -> 3*10 + 5 = 35
        args = self.fake_aws(tmp_path, monkeypatch, 35, 4,
                             {1: 10, 2: 10, 3: 10, 4: 5})
        meta = bench.s3_object_metadata(args)
        assert meta["stride_verified"] is True
        assert meta["part_length"] == 10

    def test_a_differing_interior_part_is_caught(self, tmp_path, monkeypatch):
        # parts 10, 20, 5 -> the identity (3-1)*10 + 5 = 25 != 35
        args = self.fake_aws(tmp_path, monkeypatch, 35, 3, {1: 10, 2: 20, 3: 5})
        meta = bench.s3_object_metadata(args)
        assert meta["stride_verified"] is False
        assert meta["part_length"] is None, "must not offer a stride it cannot trust"

    def test_a_differing_second_part_is_caught_even_if_the_sum_works(
            self, tmp_path, monkeypatch):
        """
        The case the sum identity alone would miss: 10 + 5 + 15 + 5 = 35 and
        (4-1)*10 + 5 = 35, so only comparing part 2 to part 1 catches it.
        """
        args = self.fake_aws(tmp_path, monkeypatch, 35, 4, {1: 10, 2: 5, 3: 15, 4: 5})
        meta = bench.s3_object_metadata(args)
        assert meta["stride_verified"] is False

    def test_a_last_part_larger_than_the_first_is_caught(self, tmp_path, monkeypatch):
        args = self.fake_aws(tmp_path, monkeypatch, 25, 2, {1: 10, 2: 15})
        meta = bench.s3_object_metadata(args)
        assert meta["stride_verified"] is False

    def test_two_parts_of_different_lengths_are_normal(self, tmp_path, monkeypatch):
        """
        With two parts the last is the remainder, so part 1 != part 2 is the usual case
        rather than a problem -- striding by part 1 still gives [0, 10) and [10, 15),
        which are the real boundaries. The property is "part 1 is the stride", not "all
        parts are equal".
        """
        args = self.fake_aws(tmp_path, monkeypatch, 15, 2, {1: 10, 2: 5})
        meta = bench.s3_object_metadata(args)
        assert meta["stride_verified"] is True and meta["part_length"] == 10

    def test_two_equal_parts_also_fine(self, tmp_path, monkeypatch):
        args = self.fake_aws(tmp_path, monkeypatch, 20, 2, {1: 10, 2: 10})
        assert bench.s3_object_metadata(args)["stride_verified"] is True

    def test_a_small_first_part_before_a_large_one_is_caught(self, tmp_path, monkeypatch):
        """
        Legal in S3 and the reason `last <= first` is the informative check when there
        are only two parts, where the sum identity is trivially satisfied.
        """
        args = self.fake_aws(tmp_path, monkeypatch, 105, 2, {1: 5, 2: 100})
        meta = bench.s3_object_metadata(args)
        assert meta["stride_verified"] is False
        assert meta["part_length"] is None

    def test_verification_is_declined_rather_than_wrong(self, tmp_path, monkeypatch):
        args = self.fake_aws(tmp_path, monkeypatch, 35, 3, {1: 10, 2: 20, 3: 5})
        args = bench.build_parser().parse_args(
            ["sweep", "--s3-bucket", "b", "--s3-key", "k", "--dest-dir", "/d"])
        size, verification = bench.resolve_source(args)
        assert size == 35
        assert verification.kind is None
        assert "not the stride" in verification.label

    def test_an_endpoint_that_rejects_other_part_numbers_still_works(
            self, tmp_path, monkeypatch):
        """
        A guard must not regress a working configuration. HandleAWSURL only ever asks for
        part 1, so an endpoint could support that and reject the rest; the confirmation
        then downgrades to unconfirmed and the part-1 stride is used as before.
        """
        script = tmp_path / "aws"
        script.write_text(
            "#!/usr/bin/env python3\n"
            "import json, sys\n"
            "argv = sys.argv[1:]\n"
            "if '--part-number' in argv:\n"
            "    n = int(argv[argv.index('--part-number')+1])\n"
            "    if n != 1:\n"
            "        sys.stderr.write('InvalidArgument\\n'); sys.exit(255)\n"
            "    print(json.dumps({'ContentLength': 10}))\n"
            "else:\n"
            "    print(json.dumps({'ContentLength': 35, 'PartsCount': 4,\n"
            "                      'ETag': '\"" + "a" * 32 + "-4\"'}))\n")
        script.chmod(0o755)
        monkeypatch.setenv("PATH", "{}:{}".format(tmp_path, os.environ["PATH"]))
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "FAKE")
        args = bench.build_parser().parse_args(
            ["probe", "--s3-bucket", "b", "--s3-key", "k"])
        meta = bench.s3_object_metadata(args)
        assert meta["stride_verified"] is None, "should be unconfirmed, not failed"
        assert meta["part_length"] == 10, "must still use the part-1 stride"
        assert "unconfirmed" in meta["stride_check"]


class TestDiskIdentification:
    """
    /proc/mounts records the resolved device, not the by-id symlink a GCE disk was mounted
    through, so the disk name has to be recovered by matching the symlink target. Without
    this the probe silently omits the PD type and size -- which is the cross-check against
    §4.1's `dd`.
    """

    def gcloud_returning(self, monkeypatch, stdout):
        import subprocess as sp
        monkeypatch.setattr(bench.shutil, "which", lambda _: "/usr/bin/gcloud")
        monkeypatch.setattr(bench.subprocess, "run",
                            lambda cmd, **kw: sp.CompletedProcess(cmd, 0, stdout, ""))

    def test_a_google_by_id_path_is_read_directly(self, monkeypatch):
        self.gcloud_returning(monkeypatch, "pd-standard\t316\n")
        out = bench.disk_details("/dev/disk/by-id/google-canine-bench-1789049197")
        assert out["disk"] == "canine-bench-1789049197"
        assert out["type"] == "pd-standard" and out["size_gb"] == 316

    def test_a_resolved_device_is_matched_back_through_the_symlinks(
            self, tmp_path, monkeypatch):
        """The case that actually occurs on a node: /proc/mounts says /dev/sdb."""
        real = tmp_path / "sdb"
        real.write_text("")
        link = tmp_path / "google-canine-bench-42"
        link.symlink_to(real)
        monkeypatch.setattr(bench.glob, "glob", lambda pattern: [str(link)])
        self.gcloud_returning(monkeypatch, "pd-standard\t316\n")
        assert bench.disk_details(str(real))["disk"] == "canine-bench-42"

    def test_an_unmatched_device_gives_up_quietly(self, monkeypatch):
        monkeypatch.setattr(bench.glob, "glob", lambda pattern: [])
        self.gcloud_returning(monkeypatch, "")
        assert bench.disk_details("/dev/sdc") is None


class TestAFailedRunIsNotAMeasurement:
    """
    The sweep once reported "48.00 GiB/s" over a 2 GB/s NIC and concluded "MEETS the
    target" for five settings that had downloaded nothing: it computed size/elapsed
    regardless of the exit code, and elapsed was how long the download took to *fail*.
    The only honest signal in the output was a `BAD` hash column that the verdict ignored.

    A measuring tool inventing measurements is the worst version of this whole class of
    bug, so it is pinned here.
    """

    def sweep_with(self, tmp_path, monkeypatch, returncode, verified):
        """Drive command_sweep with run_download stubbed to a chosen outcome."""
        calls = []

        def fake_run_download(source, dest, size, connections, min_chunk, **kw):
            calls.append(connections)
            if returncode == 0:
                with open(dest, "wb") as fh:
                    fh.write(b"x" * 16)
            return {"connections": connections, "returncode": returncode,
                    "seconds": 0.25, "killed": False, "peak_rss": 4096,
                    "phases": {}, "nic_bytes": 0, "disk_bytes": 0,
                    "peak_nic_bytes_per_s": None, "peak_disk_bytes_per_s": None,
                    "stderr_tail": ["curl: (22) The requested URL returned error: 403"]}

        monkeypatch.setattr(bench, "run_download", fake_run_download)
        monkeypatch.setattr(bench, "resolve_source",
                            lambda a: (12 * 1024 ** 3,
                                       bench.Verification("md5", "deadbeef")
                                       if verified else
                                       bench.Verification(None, reason="none")))
        args = bench.build_parser().parse_args(
            ["sweep", "--url", "https://h/o", "--size", str(12 * 1024 ** 3),
             "--dest-dir", str(tmp_path), "--connections", "1", "4", "8"])
        return bench.command_sweep(args), calls

    def test_no_throughput_is_reported_for_a_failed_run(self, tmp_path, monkeypatch,
                                                        capsys):
        result, _ = self.sweep_with(tmp_path, monkeypatch, returncode=1, verified=True)
        out = capsys.readouterr().out
        assert result["usable"] == 0
        assert "NO USABLE RESULT" in out
        assert "GiB/s" not in out and "MB/s" not in out, \
            "invented a rate for runs that failed:\n" + out
        assert "MEETS the target" not in out
        for row in result["sweep"]:
            assert row["throughput_bytes_per_s"] is None
            assert row["speedup_vs_single_stream"] is None

    def test_the_failure_reason_is_shown_not_swallowed(self, tmp_path, monkeypatch,
                                                       capsys):
        """It captured the 403 all along and printed only a throughput."""
        self.sweep_with(tmp_path, monkeypatch, returncode=1, verified=True)
        assert "403" in capsys.readouterr().out

    def test_a_run_that_completes_but_fails_verification_is_also_excluded(
            self, tmp_path, monkeypatch, capsys):
        """
        rc=0 with a hash mismatch is still not a measurement -- it means the bytes are
        wrong, so their rate is meaningless.
        """
        result, _ = self.sweep_with(tmp_path, monkeypatch, returncode=0, verified=True)
        # the stub writes 16 bytes, so the md5 cannot match
        assert result["usable"] == 0
        assert "NO USABLE RESULT" in capsys.readouterr().out

    def test_a_partial_failure_is_excluded_but_the_rest_still_reports(
            self, tmp_path, monkeypatch, capsys):
        payload = b"y" * 4096
        digest = hashlib.md5(payload).hexdigest()

        def fake_run_download(source, dest, size, connections, min_chunk, **kw):
            if connections == 4:            # one setting fails
                return {"connections": connections, "returncode": 1, "seconds": 0.1,
                        "killed": False, "peak_rss": 4096, "phases": {},
                        "nic_bytes": 0, "disk_bytes": 0,
                        "peak_nic_bytes_per_s": None, "peak_disk_bytes_per_s": None,
                        "stderr_tail": ["boom"]}
            with open(dest, "wb") as fh:
                fh.write(payload)
            return {"connections": connections, "returncode": 0, "seconds": 1.0,
                    "killed": False, "peak_rss": 4096, "phases": {},
                    "nic_bytes": 0, "disk_bytes": 0,
                    "peak_nic_bytes_per_s": None, "peak_disk_bytes_per_s": None,
                    "stderr_tail": []}

        monkeypatch.setattr(bench, "run_download", fake_run_download)
        monkeypatch.setattr(bench, "resolve_source",
                            lambda a: (len(payload),
                                       bench.Verification("md5", digest)))
        args = bench.build_parser().parse_args(
            ["sweep", "--url", "https://h/o", "--size", str(len(payload)),
             "--dest-dir", str(tmp_path), "--connections", "1", "4", "8"])
        result = bench.command_sweep(args)
        out = capsys.readouterr().out
        assert "1 of 3 settings failed" in out or "1 of 3 settings failed" in out
        assert "verdict" in out, "the surviving settings should still be reported"
        assert sum(1 for r in result["sweep"] if r["ok"]) == 2


class TestHeadersReachTheDownloader:
    """
    A private GCS object over plain https needs an Authorization header, and the runbook
    tells you to pass one -- which did nothing until the benchmark accepted `--header`.
    Both the ranged-GET path and the connections=1 curl fallback need it, or the baseline
    row fails while the others succeed.
    """

    def test_headers_are_forwarded_for_a_url_source(self):
        args = bench.build_parser().parse_args(
            ["sweep", "--url", "https://h/o", "--size", "10", "--dest-dir", "/d",
             "--header", "Authorization: Bearer tok"])
        source = bench.source_args(args, "/d/f", 10)
        assert "--header" in source
        assert "Authorization: Bearer tok" in source

    def test_multiple_headers_are_all_forwarded(self):
        args = bench.build_parser().parse_args(
            ["sweep", "--url", "https://h/o", "--size", "10", "--dest-dir", "/d",
             "--header", "A: 1", "--header", "B: 2"])
        source = bench.source_args(args, "/d/f", 10)
        assert source.count("--header") == 2
        assert "A: 1" in source and "B: 2" in source

    def test_headers_are_forwarded_for_an_s3_source_too(self):
        args = bench.build_parser().parse_args(
            ["sweep", "--s3-bucket", "b", "--s3-key", "k", "--size", "10",
             "--dest-dir", "/d", "--header", "X: y"])
        source = bench.source_args(args, "/d/f", 10)
        assert "X: y" in source

    def test_no_headers_means_no_flag(self):
        args = bench.build_parser().parse_args(
            ["sweep", "--url", "https://h/o", "--size", "10", "--dest-dir", "/d"])
        assert "--header" not in bench.source_args(args, "/d/f", 10)

    def test_the_downloader_accepts_what_the_benchmark_emits(self):
        """Checked against the real parser, not a hand-written expectation."""
        import importlib.util, os as _os
        spec = importlib.util.spec_from_file_location(
            "pdl", _os.path.join(_os.path.dirname(__file__), _os.pardir,
                                 "localization", "parallel_download.py"))
        pdl = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(pdl)
        args = bench.build_parser().parse_args(
            ["sweep", "--url", "https://h/o", "--size", "10", "--dest-dir", "/d",
             "--header", "Authorization: Bearer tok"])
        opts = pdl.build_parser().parse_args(
            bench.source_args(args, "/d/f", 10) +
            ["--dest", "/d/f", "--size", "10", "--connections", "4"])
        assert opts.header == ["Authorization: Bearer tok"]


class TestTheVerdictComparesLikeWithLike:
    """
    The sweep reported a 1.3x speedup where the like-for-like figure was 1.89x. The
    connections=1 row takes the legacy curl path, which never calls verify(), so its
    total was download-only while every parallel row's total included 27.7s of read-back.
    Comparing totals across routes that verify differently is not a comparison.
    """

    def sweep(self, tmp_path, monkeypatch, rows, dest_dir=None, s3=False):
        payload = b"z" * 8192
        digest = hashlib.md5(payload).hexdigest()

        def fake_run_download(source, dest, size, connections, min_chunk, **kw):
            spec = rows[connections]
            with open(dest, "wb") as fh:
                fh.write(payload)
            return {"connections": connections, "returncode": 0,
                    "seconds": spec["total"], "killed": False, "peak_rss": 1 << 20,
                    "phases": spec["phases"], "nic_bytes": 0, "disk_bytes": 0,
                    "peak_nic_bytes_per_s": 1e8, "peak_disk_bytes_per_s": 1e3,
                    "stderr_tail": []}

        monkeypatch.setattr(bench, "run_download", fake_run_download)
        monkeypatch.setattr(bench, "resolve_source",
                            lambda a: (len(payload), bench.Verification("md5", digest)))
        argv = ["sweep", "--url", "https://h/o", "--size", str(len(payload)),
                "--dest-dir", str(dest_dir or tmp_path),
                "--connections"] + [str(c) for c in sorted(rows)]
        if s3:
            argv += ["--s3-bucket", "b", "--s3-key", "k"]
        return bench.command_sweep(bench.build_parser().parse_args(argv))

    def test_speedup_uses_the_download_phase(self, tmp_path, monkeypatch, capsys):
        # mirrors the real run: baseline download-only, parallel row download+verify
        self.sweep(tmp_path, monkeypatch, {
            1: {"total": 118.7, "phases": {"download": 118.7}},
            4: {"total": 91.14, "phases": {"download": 62.9, "verify": 27.7}},
        })
        out = capsys.readouterr().out
        assert "download phase" in out
        # 118.7 / 62.9 = 1.89, not 91.14-based 1.30
        assert "1.89x" in out, out
        assert "1.30x" not in out

    def test_it_says_when_the_basis_is_not_comparable(self, tmp_path, monkeypatch,
                                                      capsys):
        self.sweep(tmp_path, monkeypatch, {
            1: {"total": 100.0, "phases": {}},
            4: {"total": 50.0, "phases": {"download": 25.0, "verify": 25.0}},
        })
        out = capsys.readouterr().out
        assert "phases unavailable" in out
        assert "understates" in out

    def test_no_disk_verdict_for_a_memory_destination(self, tmp_path, monkeypatch,
                                                     capsys):
        """
        /proc/diskstats sees only block devices, so a tmpfs destination reports ~0 disk
        writes -- which the old heuristic read as "the DISK looks like the limit" on a
        run with no disk in it at all.
        """
        monkeypatch.setattr(bench, "probe_mount",
                            lambda d: {"available": True, "matched": True,
                                       "fstype": "tmpfs", "device": "tmpfs",
                                       "mountpoint": str(tmp_path)})
        self.sweep(tmp_path, monkeypatch, {
            1: {"total": 10.0, "phases": {"download": 10.0}},
            4: {"total": 5.0, "phases": {"download": 5.0}},
        })
        out = capsys.readouterr().out
        assert "not applicable" in out
        assert "DISK looks like the limit" not in out
        assert "says nothing about the disk" in out

    def test_a_whole_file_md5_is_not_advertised_as_avoidable(self, tmp_path, monkeypatch,
                                                            capsys):
        """
        In-transfer hashing only applies to a multipart ETag. A whole-file md5 is
        sequential over the byte stream and cannot be assembled from parts, so telling
        the reader the read-back is avoidable would be wrong.
        """
        self.sweep(tmp_path, monkeypatch, {
            1: {"total": 10.0, "phases": {"download": 5.0, "verify": 5.0}},
            4: {"total": 10.0, "phases": {"download": 5.0, "verify": 5.0}},
        })
        out = capsys.readouterr().out
        assert "WHOLE-FILE md5" in out
        assert "unavoidable" in out
        assert "could be computed" not in out

    def test_a_multipart_source_points_at_the_recorded_digests(self, tmp_path,
                                                              monkeypatch, capsys):
        self.sweep(tmp_path, monkeypatch, {
            1: {"total": 10.0, "phases": {"download": 5.0, "verify": 5.0}},
            4: {"total": 10.0, "phases": {"download": 5.0, "verify": 5.0}},
        }, s3=True)
        out = capsys.readouterr().out
        assert "MULTIPART" in out
        assert "already implemented" in out


class TestThePrefixBaselineFetchesOnlyThePrefix:
    """
    The connections=1 row is the legacy path: the downloader declines at
    `connections <= 1` and synthesizes `curl -C - -sSL -o dest url`, which carries no
    range. Correct in production, where --size is always the whole object; wrong in a
    sweep, where --size names a slice. Against the 279 GiB BAM with --size 12 GiB it
    pulled all 279 GiB into a 16 GiB tmpfs -- so the two rows being compared were not
    fetching the same bytes, and the mount filled.
    """

    def parse(self, *extra):
        return bench.build_parser().parse_args(
            ["sweep", "--url", "https://h/o", "--size", "1024",
             "--dest-dir", "/d"] + list(extra))

    def test_prefix_supplies_a_ranged_legacy_command(self):
        source = bench.source_args(self.parse("--prefix"), "/d/f", 1024)
        assert "--legacy-cmd" in source
        command = source[source.index("--legacy-cmd") + 1]
        assert "-r 0-1023" in command, command

    def test_the_range_covers_exactly_the_requested_bytes(self):
        """An off-by-one here is a truncated or over-long baseline, silently."""
        for size in (1, 2, 1024, 12 * 1024 ** 3):
            command = bench.url_legacy_command(self.parse("--prefix"), "/d/f", size)
            assert " -r 0-{} ".format(size - 1) in command, command

    def test_without_prefix_no_legacy_command_is_supplied(self):
        """
        Production's unranged fallback stays the default -- the flag has to be the thing
        that changes behavior, or this test would pass on a benchmark that always ranged
        (and so never exercised the fallback the handlers actually emit).
        """
        assert "--legacy-cmd" not in bench.source_args(self.parse(), "/d/f", 1024)

    def test_the_baseline_fails_on_an_http_error(self):
        """
        Without --fail curl writes the error body to the destination and exits 0, so a
        403 presents as a very fast success -- and in prefix mode there is no
        verification to catch it.
        """
        command = bench.url_legacy_command(self.parse("--prefix"), "/d/f", 1024)
        assert "--fail" in command

    def test_headers_reach_the_ranged_baseline(self):
        args = self.parse("--prefix", "--header", "Authorization: Bearer tok")
        command = bench.url_legacy_command(args, "/d/f", 1024)
        assert "--header 'Authorization: Bearer tok'" in command, command

    def test_the_command_is_shell_safe(self):
        args = self.parse("--prefix")
        args.url = "https://h/o?sig=a&b=c"
        command = bench.url_legacy_command(args, "/d/f oo", 1024)
        assert shlex.split(command)[-1] == "https://h/o?sig=a&b=c"
        assert "/d/f oo" in shlex.split(command)

    def test_the_sweep_says_which_baseline_it_used(self, capsys):
        """
        Two commands that look identical in the output and differ in what they fetch is
        how this went unnoticed. The header has to distinguish them.
        """
        args = self.parse("--prefix")
        args.connections = [1]
        try:
            bench.command_sweep(args)
        except BaseException:
            pass
        assert "ranged curl" in capsys.readouterr().out


class TestAPrefixDoesNotBorrowTheWholeObjectsEtag:
    """
    head-object describes the whole object. Deriving verification from it under --prefix
    yields the 9849-part ETag of a 279 GiB object, which a 12 GiB prefix (424 parts)
    cannot match -- so verify() raises, the file is discarded, and every row exits 1.
    The sweep reports NO USABLE RESULT and the cause appears nowhere in the output.
    """

    def s3_args(self, *extra):
        return bench.build_parser().parse_args(
            ["sweep", "--url", "https://presigned/o", "--s3-bucket", "b",
             "--s3-key", "k", "--size", str(12 * 1024 ** 3),
             "--dest-dir", "/d"] + list(extra))

    def whole_object_meta(self):
        return {"size": 279 * 1024 ** 3, "etag": "d" * 32 + "-9849",
                "parts_count": 9849, "part_length": 29 * 1024 ** 2,
                "stride_verified": True, "stride_check": "ok"}

    def test_prefix_declines_the_derived_etag(self, monkeypatch):
        monkeypatch.setattr(bench, "s3_object_metadata",
                            lambda args: self.whole_object_meta())
        _, verification = bench.resolve_source(self.s3_args("--prefix"))
        assert verification.kind is None
        assert "prefix" in verification.reason

    def test_without_prefix_the_etag_is_still_derived(self, monkeypatch):
        """The guard must not disable §6.4's full-size verification."""
        monkeypatch.setattr(bench, "s3_object_metadata",
                            lambda args: self.whole_object_meta())
        _, verification = bench.resolve_source(self.s3_args())
        assert verification.kind == "etag"
        assert verification.value == "d" * 32 + "-9849"

    def test_an_explicit_md5_still_wins_under_prefix(self, monkeypatch):
        """An operator who computed the prefix's md5 can still verify against it."""
        monkeypatch.setattr(bench, "s3_object_metadata",
                            lambda args: self.whole_object_meta())
        _, verification = bench.resolve_source(
            self.s3_args("--prefix", "--md5", "a" * 32))
        assert verification.kind == "md5"
        assert verification.value == "a" * 32

    def test_the_declined_reason_reaches_the_operator(self, monkeypatch):
        monkeypatch.setattr(bench, "s3_object_metadata",
                            lambda args: self.whole_object_meta())
        _, verification = bench.resolve_source(self.s3_args("--prefix"))
        assert "NOT VERIFIED" in verification.label
        assert "6.3/6.4" in verification.label


class TestTheS3BaselineIsBoundedToo:
    """
    The same defect as the URL baseline, in the other legacy command. `bytes=$SZ-` is
    open-ended: right in production, where self.size is the object's real size, and wrong
    under a truncated --size, where it fetches all 279 GiB while the parallel rows fetch
    12. Found by asking why the baseline curls rather than using `aws s3api` -- which for
    an s3:// source production does, making this the comparator that actually matters.
    """

    def parse(self, *extra):
        return bench.build_parser().parse_args(
            ["sweep", "--s3-bucket", "b", "--s3-key", "k", "--size", "1024",
             "--dest-dir", "/d"] + list(extra))

    def test_prefix_bounds_the_range(self):
        command = bench.s3_legacy_command(self.parse("--prefix"), "/d/f", 1024)
        assert '"bytes=$SZ-1023"' in command, command

    def test_without_prefix_the_range_stays_open_ended(self):
        """
        Production parity: HandleAWSURL emits bytes=$SZ- and resumes by appending. If the
        benchmark bounded it unconditionally it would stop reproducing the command it
        exists to measure.
        """
        command = bench.s3_legacy_command(self.parse(), "/d/f", 1024)
        assert '"bytes=$SZ-"' in command, command

    def test_the_bound_is_the_last_byte_not_the_length(self):
        for size in (1, 2, 1024, 12 * 1024 ** 3):
            command = bench.s3_legacy_command(self.parse("--prefix"), "/d/f", size)
            assert '"bytes=$SZ-{}"'.format(size - 1) in command, command

    def test_the_s3_baseline_is_what_an_s3_source_gets(self):
        """--url absent selects S3ApiSource, whose baseline must be the aws command."""
        source = bench.source_args(self.parse("--prefix"), "/d/f", 1024)
        command = source[source.index("--legacy-cmd") + 1]
        assert "aws s3api" in command and "curl" not in command, command

    def test_production_is_not_bounded_by_this_change(self):
        """
        The guard belongs to the benchmark only. HandleAWSURL's own fallback must keep
        its open-ended range -- it always downloads a whole object, and bounding it there
        would break resume for anything whose size the handler got wrong.
        """
        with open(os.path.join(os.path.dirname(__file__), os.pardir,
                               "localization", "file_handlers.py")) as handle:
            assert '--range "bytes=$SZ-"' in handle.read()


class TestDuplicateFetchingIsVisible:
    """
    16 connections each fetching the whole object, rather than a disjoint sixteenth of
    it, would look like a correct-but-slow run: same wall time, same throughput figure, a
    valid file at the end. What gives it away is NIC bytes -- 16x the payload instead of
    1x. `probe_range` is meant to prevent it (a server that ignores Range returns the
    whole object to every request), but prevention is not observation, and on GCS it is a
    billing event as well as a wrong number.
    """

    def sweep(self, tmp_path, monkeypatch, ratios):
        payload = b"z" * 8192
        digest = hashlib.md5(payload).hexdigest()

        def fake_run_download(source, dest, size, connections, min_chunk, **kw):
            with open(dest, "wb") as fh:
                fh.write(payload)
            return {"connections": connections, "returncode": 0, "seconds": 10.0,
                    "killed": False, "peak_rss": 1 << 20,
                    "phases": {"download": 9.0, "verify": 1.0},
                    "nic_bytes": int(ratios[connections] * len(payload)),
                    "disk_bytes": len(payload),
                    "peak_nic_bytes_per_s": 1e8, "peak_disk_bytes_per_s": 1e3,
                    "mean_streams": None, "workers": None, "stderr_tail": []}

        monkeypatch.setattr(bench, "run_download", fake_run_download)
        monkeypatch.setattr(bench, "resolve_source",
                            lambda a: (len(payload), bench.Verification("md5", digest)))
        argv = ["sweep", "--url", "https://h/o", "--size", str(len(payload)),
                "--dest-dir", str(tmp_path),
                "--connections"] + [str(c) for c in sorted(ratios)]
        bench.command_sweep(bench.build_parser().parse_args(argv))

    def test_a_one_to_one_ratio_is_not_flagged(self, tmp_path, monkeypatch, capsys):
        self.sweep(tmp_path, monkeypatch, {1: 1.02, 16: 1.03})
        assert "DUPLICATE FETCHING" not in capsys.readouterr().out

    def test_sixteen_times_the_payload_is_flagged(self, tmp_path, monkeypatch, capsys):
        self.sweep(tmp_path, monkeypatch, {1: 1.02, 16: 16.1})
        out = capsys.readouterr().out
        assert "DUPLICATE FETCHING" in out
        assert "ignored Range" in out
        assert "bills for the whole object" in out, "the cost consequence must be stated"

    def test_the_ratio_is_shown_per_row(self, tmp_path, monkeypatch, capsys):
        """Visible on every run, not only when it trips the threshold."""
        self.sweep(tmp_path, monkeypatch, {1: 1.02, 16: 1.03})
        assert "x payload" in capsys.readouterr().out

    def test_the_threshold_tolerates_protocol_overhead(self, tmp_path, monkeypatch, capsys):
        """
        nic_total counts TLS, framing and any other traffic on the box, so the ratio is
        never exactly 1. A threshold firing at 1.05 would cry wolf on every real run.
        """
        self.sweep(tmp_path, monkeypatch, {1: 1.2, 16: 1.4})
        assert "DUPLICATE FETCHING" not in capsys.readouterr().out


class TestAMissingConcurrencyFigureIsLoud:
    """
    A 4 GiB run made specifically to read the concurrency figure came back without it.
    The benchmark was current -- its own new `wire:` line printed -- but the downloader in
    the container was stale, and `k9pdl-streams` comes from the downloader. So the number
    the run existed to produce was simply absent, and absent is indistinguishable from
    "concurrency was zero", which is the opposite conclusion.

    The verdict did have a branch for this, but it lived inside the speedup block, which
    is skipped when there is no connections=1 row. On a single-setting run it never fired.
    """

    def sweep(self, tmp_path, monkeypatch, connections, streams):
        payload = b"z" * 8192
        digest = hashlib.md5(payload).hexdigest()

        def fake_run_download(source, dest, size, conns, min_chunk, **kw):
            with open(dest, "wb") as fh:
                fh.write(payload)
            return {"connections": conns, "returncode": 0, "seconds": 10.0,
                    "killed": False, "peak_rss": 1 << 20,
                    "phases": {"download": 10.0},
                    "nic_bytes": len(payload), "disk_bytes": len(payload),
                    "peak_nic_bytes_per_s": 1e8, "peak_disk_bytes_per_s": 1e3,
                    "mean_streams": streams.get(conns),
                    "workers": conns if streams.get(conns) else None,
                    "stderr_tail": []}

        downloader = tmp_path / "parallel_download.py"
        downloader.write_bytes(b"# stand-in for the real downloader\n")
        monkeypatch.setattr(bench, "DOWNLOADER", str(downloader))
        monkeypatch.setattr(bench, "run_download", fake_run_download)
        monkeypatch.setattr(bench, "resolve_source",
                            lambda a: (len(payload), bench.Verification("md5", digest)))
        bench.command_sweep(bench.build_parser().parse_args(
            ["sweep", "--url", "https://h/o", "--size", str(len(payload)),
             "--dest-dir", str(tmp_path),
             "--connections"] + [str(c) for c in connections]))

    def test_a_single_setting_run_without_the_figure_says_so(
            self, tmp_path, monkeypatch, capsys):
        """The exact shape of the run that went wrong: one row, no baseline, no figure."""
        self.sweep(tmp_path, monkeypatch, [16], {})
        out = capsys.readouterr().out
        assert "NO CONCURRENCY FIGURE" in out
        assert "stale downloader" in out, "the likely cause must be named"

    def test_the_figure_present_is_not_flagged(self, tmp_path, monkeypatch, capsys):
        self.sweep(tmp_path, monkeypatch, [16], {16: 15.8})
        assert "NO CONCURRENCY FIGURE" not in capsys.readouterr().out

    def test_the_connections_one_row_is_not_expected_to_have_one(
            self, tmp_path, monkeypatch, capsys):
        """
        connections=1 takes the legacy command and never enters the worker pool, so it
        has no concurrency to report. Counting it would make the warning fire on every
        correct sweep, and a warning that always fires is not read.
        """
        self.sweep(tmp_path, monkeypatch, [1, 16], {16: 15.8})
        assert "NO CONCURRENCY FIGURE" not in capsys.readouterr().out

    def test_the_header_identifies_the_downloader(self, tmp_path, monkeypatch, capsys):
        self.sweep(tmp_path, monkeypatch, [16], {16: 15.8})
        out = capsys.readouterr().out
        assert "downloader:" in out
        assert "md5" in out.split("downloader:")[1].splitlines()[0], out

    def test_the_downloader_md5_is_the_file_actually_used(self, tmp_path, monkeypatch):
        """A digest of the wrong file is worse than none -- it would read as confirmation."""
        fake = tmp_path / "parallel_download.py"
        fake.write_bytes(b"# not the real one\n")
        monkeypatch.setattr(bench, "DOWNLOADER", str(fake))
        expected = hashlib.md5(fake.read_bytes()).hexdigest()
        assert expected in bench.describe_downloader()

    def test_a_missing_downloader_does_not_crash_the_header(self, monkeypatch):
        monkeypatch.setattr(bench, "DOWNLOADER", None)
        assert bench.describe_downloader() == "not found"


class TestWrittenPathsSayWhichFilesystem:
    """
    `pdl` is `docker exec`, so --json writes to the CONTAINER's /tmp. "results written to
    /tmp/x.json" reads as the node's path. That cost a failed command, and the runbook's
    teardown scp'd $NODE:/tmp/*.json -- collecting nothing, then deleting the instance
    holding the only copy of every result.
    """

    def test_inside_a_container_the_note_is_added(self, monkeypatch):
        monkeypatch.setattr(bench.os.path, "exists", lambda p: p == "/.dockerenv")
        note = bench.container_path_note()
        assert "inside the container" in note
        assert "docker cp" in note, "the note must say how to get the file out"

    def test_outside_a_container_there_is_no_note(self, monkeypatch):
        monkeypatch.setattr(bench.os.path, "exists", lambda p: False)
        assert bench.container_path_note() == ""

    def test_the_runbook_does_not_collect_from_the_node_tmp(self):
        """
        The specific command that would have lost the results. Pinned because it looks
        correct and its failure mode is silent -- an empty scp followed by a delete.
        """
        with open(os.path.join(os.path.dirname(__file__), "BENCHMARK_RUNBOOK.md")) as fh:
            text = fh.read()
        assert '"$NODE:/tmp/*.json"' not in text
        assert "docker cp" in text.split("## 9.")[1], \
            "9 must copy the results out of the container first"


class TestASilentFallbackIsNotAParallelMeasurement:
    """
    Both GDC sweeps reported results for connections 1, 4, 8, 12 and 16 while every row
    ran the same single curl. probe_range compares the server's declared total against
    the size it was given; in --prefix mode that total is the whole 279 GiB object and
    the size is a 4 GiB slice, so it concluded Range was not honoured, raised
    RangeNotSupported, and the downloader fell back to a single stream.

    Nothing else looked wrong. The bytes arrived, `wire:` was 1.01x, the throughput was
    real -- 16.62, 16.69, 16.51, 15.81, 16.73 MiB/s, which reads as a flat source and was
    in fact a constant experiment. The conclusion drawn from it (that this source caps
    aggregate bandwidth) was about nothing at all.
    """

    def sweep(self, tmp_path, monkeypatch, rows):
        payload = b"z" * 8192
        digest = hashlib.md5(payload).hexdigest()

        def fake_run_download(source, dest, size, conns, min_chunk, **kw):
            with open(dest, "wb") as fh:
                fh.write(payload)
            spec = rows[conns]
            return {"connections": conns, "returncode": 0, "seconds": 10.0,
                    "killed": False, "peak_rss": 1 << 20,
                    "phases": {"download": 10.0}, "nic_bytes": len(payload),
                    "disk_bytes": len(payload), "peak_nic_bytes_per_s": 1e8,
                    "peak_disk_bytes_per_s": 1e3, "stderr_tail": [],
                    "fell_back": spec.get("fell_back"),
                    "mean_streams": spec.get("streams"),
                    "workers": conns if spec.get("streams") else None}

        downloader = tmp_path / "parallel_download.py"
        downloader.write_bytes(b"# stand-in\n")
        monkeypatch.setattr(bench, "DOWNLOADER", str(downloader))
        monkeypatch.setattr(bench, "run_download", fake_run_download)
        monkeypatch.setattr(bench, "resolve_source",
                            lambda a: (len(payload), bench.Verification("md5", digest)))
        bench.command_sweep(bench.build_parser().parse_args(
            ["sweep", "--url", "https://h/o", "--size", str(len(payload)),
             "--dest-dir", str(tmp_path),
             "--connections"] + [str(c) for c in sorted(rows)]))

    def test_a_fallen_back_row_is_marked(self, tmp_path, monkeypatch, capsys):
        self.sweep(tmp_path, monkeypatch,
                   {16: {"fell_back": "server reports size 299481061742 but 4294967296 "
                                      "was expected"}})
        out = capsys.readouterr().out
        assert "NOT PARALLEL" in out
        assert "299481061742" in out, "the server's own reason must be shown"

    def test_the_verdict_refuses_to_call_it_a_measurement(self, tmp_path, monkeypatch,
                                                          capsys):
        self.sweep(tmp_path, monkeypatch, {16: {"fell_back": "range not honoured"}})
        out = capsys.readouterr().out
        assert "NOT A PARALLEL MEASUREMENT" in out
        assert "--object-size" in out, "the likely cause must be named"

    def test_a_real_parallel_run_is_not_flagged(self, tmp_path, monkeypatch, capsys):
        self.sweep(tmp_path, monkeypatch, {16: {"streams": 15.8}})
        out = capsys.readouterr().out
        assert "NOT PARALLEL" not in out
        assert "NOT A PARALLEL MEASUREMENT" not in out

    def test_a_fallback_does_not_also_trip_the_missing_figure_warning(
            self, tmp_path, monkeypatch, capsys):
        """
        A fallen-back row legitimately has no streams figure. Reporting both would bury
        the real cause under a guess about a stale downloader.
        """
        self.sweep(tmp_path, monkeypatch, {16: {"fell_back": "range not honoured"}})
        assert "NO CONCURRENCY FIGURE" not in capsys.readouterr().out

    def test_the_connections_one_row_may_fall_back_without_complaint(
            self, tmp_path, monkeypatch, capsys):
        """connections=1 IS the fallback -- it is the baseline, not a defect."""
        self.sweep(tmp_path, monkeypatch,
                   {1: {"fell_back": "connections <= 1"}, 16: {"streams": 15.8}})
        assert "NOT A PARALLEL MEASUREMENT" not in capsys.readouterr().out


class TestThePrefixRunLearnsTheObjectSize:
    """Without --object-size the prefix rows are not parallel at all."""

    def args(self, *extra):
        return bench.build_parser().parse_args(
            ["sweep", "--url", "https://h/o", "--size", "1024",
             "--dest-dir", "/d"] + list(extra))

    def test_the_size_is_forwarded_when_known(self):
        args = self.args("--prefix")
        args.object_size = 299481061742
        source = bench.source_args(args, "/d/f", 1024)
        assert "--object-size" in source
        assert source[source.index("--object-size") + 1] == "299481061742"

    def test_nothing_is_forwarded_without_prefix(self):
        args = self.args()
        args.object_size = 299481061742
        assert "--object-size" not in bench.source_args(args, "/d/f", 1024)

    def test_an_unknown_size_forwards_nothing(self):
        """Better to leave the flag off than to guess a total."""
        args = self.args("--prefix")
        args.object_size = None
        assert "--object-size" not in bench.source_args(args, "/d/f", 1024)

    def test_the_total_comes_from_content_range(self, monkeypatch):
        class FakeResponse:
            headers = {"Content-Range": "bytes 0-0/299481061742"}

            def close(self):
                pass

        monkeypatch.setattr(bench.urllib.request, "urlopen",
                            lambda *a, **kw: FakeResponse())
        assert bench.url_object_size(self.args("--prefix")) == 299481061742

    def test_a_missing_content_range_is_reported_not_guessed(self, monkeypatch, capsys):
        class FakeResponse:
            headers = {}

            def close(self):
                pass

        monkeypatch.setattr(bench.urllib.request, "urlopen",
                            lambda *a, **kw: FakeResponse())
        assert bench.url_object_size(self.args("--prefix")) is None
        assert "Content-Range" in capsys.readouterr().out

    def test_headers_reach_the_size_probe(self, monkeypatch):
        """A private object needs the auth header here too, or the probe 403s."""
        seen = {}

        class FakeResponse:
            headers = {"Content-Range": "bytes 0-0/10"}

            def close(self):
                pass

        def fake_urlopen(request, **kw):
            seen["headers"] = dict(request.headers)
            return FakeResponse()

        monkeypatch.setattr(bench.urllib.request, "urlopen", fake_urlopen)
        bench.url_object_size(self.args("--prefix", "--header", "Authorization: Bearer t"))
        assert seen["headers"].get("Authorization") == "Bearer t"
        assert seen["headers"].get("Range") == "bytes=0-0"

    def test_the_header_warns_when_the_size_is_unknown(self, capsys, tmp_path):
        args = self.args("--prefix")
        args.object_size = None
        args.connections = [16]
        args.dest_dir = str(tmp_path)
        try:
            bench.command_sweep(args)
        except BaseException:
            pass
        out = capsys.readouterr().out
        assert "UNKNOWN" in out and "fall back to a single stream" in out


class TestAnExhaustedRangeIsNotAKnee:
    """
    The GDC sweep printed "throughput is within 5% of its best from 16 connections
    upward, which is the value to set as the default". Arithmetically true and exactly
    backwards: 16 was both the fastest and the highest setting tried, so throughput was
    still climbing when the range ran out. Per-stream rate was flat at ~16 MiB/s from 1
    to 16 connections -- there is no knee in that data at all.

    Recommending the top of an exhausted range as the default is how a test-range cap
    gets mistaken for a source plateau.
    """

    def sweep(self, tmp_path, monkeypatch, rates, verified=True):
        payload = b"z" * 8192
        digest = hashlib.md5(payload).hexdigest()

        def fake_run_download(source, dest, size, conns, min_chunk, **kw):
            with open(dest, "wb") as fh:
                fh.write(payload)
            seconds = len(payload) / float(rates[conns])
            return {"connections": conns, "returncode": 0, "seconds": seconds,
                    "killed": False, "peak_rss": 1 << 20,
                    "phases": {"download": seconds, "verify": 0.0},
                    "nic_bytes": len(payload), "disk_bytes": len(payload),
                    "peak_nic_bytes_per_s": 1e8, "peak_disk_bytes_per_s": 1e3,
                    "stderr_tail": [], "fell_back": None,
                    "mean_streams": float(conns) * 0.9, "workers": conns}

        downloader = tmp_path / "parallel_download.py"
        downloader.write_bytes(b"# stand-in\n")
        monkeypatch.setattr(bench, "DOWNLOADER", str(downloader))
        monkeypatch.setattr(bench, "run_download", fake_run_download)
        monkeypatch.setattr(
            bench, "resolve_source",
            lambda a: (len(payload),
                       bench.Verification("md5", digest) if verified
                       else bench.Verification(None, reason="a prefix has no digest")))
        bench.command_sweep(bench.build_parser().parse_args(
            ["sweep", "--url", "https://h/o", "--size", str(len(payload)),
             "--dest-dir", str(tmp_path),
             "--connections"] + [str(c) for c in sorted(rates)]))

    def test_still_climbing_at_the_top_is_reported_as_no_knee(
            self, tmp_path, monkeypatch, capsys):
        """The real shape: linear in connections, fastest at the highest setting."""
        self.sweep(tmp_path, monkeypatch, {1: 16e6, 4: 60e6, 8: 123e6, 12: 164e6, 16: 227e6})
        out = capsys.readouterr().out
        assert "NO KNEE FOUND" in out
        assert "too narrow" in out
        assert "which is the value to set as the default" not in out

    def test_a_real_plateau_still_names_a_default(self, tmp_path, monkeypatch, capsys):
        """The guard must not swallow the case the message exists for."""
        self.sweep(tmp_path, monkeypatch, {1: 16e6, 4: 100e6, 8: 101e6, 16: 99e6})
        out = capsys.readouterr().out
        assert "NO KNEE FOUND" not in out
        assert "within 5% of its best from 4 connections" in out

    def test_the_destination_ceiling_is_offered_instead(self, tmp_path, monkeypatch,
                                                        capsys):
        """
        With no knee, the useful default comes from the destination's write limit, not
        from the source -- which is the actual situation: pd-standard saturates around 6
        connections while the source was still scaling at 16.
        """
        self.sweep(tmp_path, monkeypatch, {1: 16e6, 8: 123e6, 16: 227e6})
        assert "DESTINATION" in capsys.readouterr().out


class TestAnUnverifiedRunSaysNothingAboutHashingCost:
    """
    Every --prefix run reports `verify 0.0s`, because a slice of an object cannot match
    that object's ETag so there is nothing to check. The sweep read that as "verification
    is a small share, so hashing during the transfer would buy little" -- a conclusion
    about work that never happened, and the opposite of what §13.35 built #19 for.
    """

    def sweep(self, tmp_path, monkeypatch, verified):
        return TestAnExhaustedRangeIsNotAKnee.sweep(
            self, tmp_path, monkeypatch, {1: 16e6, 8: 123e6}, verified=verified)

    def test_an_unverified_sweep_declines_the_conclusion(self, tmp_path, monkeypatch,
                                                         capsys):
        self.sweep(tmp_path, monkeypatch, verified=False)
        out = capsys.readouterr().out
        assert "Not available" in out
        assert "not because hashing is cheap" in out
        assert "would buy little" not in out

    def test_a_verified_sweep_still_reports_the_split(self, tmp_path, monkeypatch,
                                                      capsys):
        self.sweep(tmp_path, monkeypatch, verified=True)
        out = capsys.readouterr().out
        assert "mean download" in out
        assert "Not available" not in out


class TestSaturationUsesMeanNotPeakDisk:
    """
    "the DISK looks like the limit. Raising connections further will not help" fired on a
    run whose throughput was still rising at the highest setting tried, in the same table.

    The heuristic compared PEAK disk against peak NIC. Writes land in page cache and the
    kernel flushes at device speed, so peak disk reaches the device's rate regardless of
    the actual data rate: the connections=1 row measured 87.65 MiB/s peak disk while
    moving 15.96 MiB/s, a 5.5x gap. Peak carries no saturation information at all, and
    every disk-destined run tripped the same conclusion.
    """

    def sweep(self, tmp_path, monkeypatch, rows, fstype="ext4"):
        payload = b"z" * 8192
        digest = hashlib.md5(payload).hexdigest()

        def fake_run_download(source, dest, size, conns, min_chunk, **kw):
            with open(dest, "wb") as fh:
                fh.write(payload)
            spec = rows[conns]
            seconds = len(payload) / float(spec["rate"])
            return {"connections": conns, "returncode": 0, "seconds": seconds,
                    "killed": False, "peak_rss": 1 << 20,
                    "phases": {"download": seconds},
                    "nic_bytes": len(payload),
                    # disk_bytes/seconds is the sustained rate; peak is set high to mimic
                    # page-cache flush bursts
                    "disk_bytes": int(spec["rate"] * seconds),
                    "peak_nic_bytes_per_s": 300 * bench.MIB,
                    "peak_disk_bytes_per_s": 95 * bench.MIB,
                    "stderr_tail": [], "fell_back": None,
                    "mean_streams": spec.get("streams"), "workers": conns}

        downloader = tmp_path / "parallel_download.py"
        downloader.write_bytes(b"# stand-in\n")
        monkeypatch.setattr(bench, "DOWNLOADER", str(downloader))
        monkeypatch.setattr(bench, "run_download", fake_run_download)
        monkeypatch.setattr(bench, "probe_mount", lambda d: {"fstype": fstype})
        monkeypatch.setattr(bench, "resolve_source",
                            lambda a: (len(payload), bench.Verification("md5", digest)))
        bench.command_sweep(bench.build_parser().parse_args(
            ["sweep", "--url", "https://h/o", "--size", str(len(payload)),
             "--dest-dir", str(tmp_path),
             "--connections"] + [str(c) for c in sorted(rows)]))

    def test_still_climbing_is_not_called_disk_bound(self, tmp_path, monkeypatch, capsys):
        """Well short of the device and still rising: the disk is not the story."""
        self.sweep(tmp_path, monkeypatch,
                   {1: {"rate": 16 * bench.MIB},
                    8: {"rate": 30 * bench.MIB, "streams": 1.9},
                    16: {"rate": 40 * bench.MIB, "streams": 2.5}})
        out = capsys.readouterr().out
        assert "NOT disk-bound yet" in out
        assert "Raising connections will not help" not in out
        assert "streams" in out, "it must point at the figure that explains the gap"

    def test_the_measured_shape_is_called_mostly_disk_bound(self, tmp_path, monkeypatch,
                                                            capsys):
        """
        72.10 MiB/s sustained against a device that demonstrated 87.57 -- 82% of it.
        "not saturated" was technically true and read as though there were room; the
        remaining 18% is overlap loss between the write path and the network, which more
        connections cannot recover because each stream is already at its source rate.
        """
        self.sweep(tmp_path, monkeypatch,
                   {1: {"rate": 16 * bench.MIB},
                    16: {"rate": 72 * bench.MIB, "streams": 4.6}})
        out = capsys.readouterr().out
        assert "MOSTLY disk-bound" in out
        assert "overlap loss" in out
        assert "faster destination" in out
        assert "NOT disk-bound yet" not in out

    def test_a_genuinely_saturated_disk_is_reported_as_such(self, tmp_path, monkeypatch,
                                                            capsys):
        self.sweep(tmp_path, monkeypatch,
                   {1: {"rate": 16 * bench.MIB},
                    8: {"rate": 92 * bench.MIB, "streams": 7.5},
                    16: {"rate": 90 * bench.MIB, "streams": 7.6}})
        out = capsys.readouterr().out
        assert "the DISK is the limit" in out
        assert "NOT disk-bound yet" not in out

    def test_the_mean_rate_is_shown_alongside_the_peak(self, tmp_path, monkeypatch,
                                                       capsys):
        self.sweep(tmp_path, monkeypatch,
                   {1: {"rate": 16 * bench.MIB}, 8: {"rate": 55 * bench.MIB}})
        out = capsys.readouterr().out
        assert "peak disk write" in out and "mean disk write" in out

    def test_a_memory_destination_still_says_nothing_about_the_disk(
            self, tmp_path, monkeypatch, capsys):
        self.sweep(tmp_path, monkeypatch,
                   {1: {"rate": 16 * bench.MIB}, 8: {"rate": 227 * bench.MIB}},
                   fstype="tmpfs")
        out = capsys.readouterr().out
        assert "says nothing about the disk" in out
        assert "the DISK is the limit" not in out
        assert "NOT disk-bound yet" not in out


class TestNearZeroVerifyIsTheFeatureWorking:
    """
    The full-size run reported `verify 0.0s` with `hash ok` against a multipart ETag --
    #19 working exactly as designed: the digest was assembled from part md5s recorded
    during the transfer, so the post-hoc read-back of 279 GiB never happened. At the
    91.6 MiB/s read rate from §4.1 that is ~52 minutes avoided.

    The report then said "Verification is a small share, so hashing during the transfer
    would buy little" -- reading the feature's success as evidence it was unnecessary.
    The same sentence would recommend removing the thing that produced the 0.0s.
    """

    def sweep(self, tmp_path, monkeypatch, verify_seconds, s3=False):
        payload = b"z" * 8192
        digest = hashlib.md5(payload).hexdigest()

        def fake_run_download(source, dest, size, conns, min_chunk, **kw):
            with open(dest, "wb") as fh:
                fh.write(payload)
            return {"connections": conns, "returncode": 0,
                    "seconds": 100.0 + verify_seconds, "killed": False,
                    "peak_rss": 1 << 20,
                    "phases": {"download": 100.0, "verify": verify_seconds},
                    "nic_bytes": len(payload), "disk_bytes": len(payload),
                    "peak_nic_bytes_per_s": 1e8, "peak_disk_bytes_per_s": 1e3,
                    "stderr_tail": [], "fell_back": None,
                    "mean_streams": 3.0, "workers": conns,
                    "bookkeeping_seconds": None, "bookkeeping_calls": None,
                    "bookkeeping_mean": None, "bookkeeping_pct_workers": None}

        downloader = tmp_path / "parallel_download.py"
        downloader.write_bytes(b"# stand-in\n")
        monkeypatch.setattr(bench, "DOWNLOADER", str(downloader))
        monkeypatch.setattr(bench, "run_download", fake_run_download)
        monkeypatch.setattr(bench, "resolve_source",
                            lambda a: (len(payload), bench.Verification("md5", digest)))
        argv = ["sweep", "--url", "https://h/o", "--size", str(len(payload)),
                "--dest-dir", str(tmp_path), "--connections", "16"]
        if s3:
            argv += ["--s3-bucket", "b", "--s3-key", "k"]
        bench.command_sweep(bench.build_parser().parse_args(argv))

    def test_a_zero_verify_on_a_multipart_source_credits_the_transfer(
            self, tmp_path, monkeypatch, capsys):
        self.sweep(tmp_path, monkeypatch, 0.0, s3=True)
        out = capsys.readouterr().out
        assert "read-back was skipped entirely" in out
        assert "would buy little" not in out
        assert "Do NOT read this as" in out, "the misreading must be pre-empted"

    def test_it_points_at_the_line_that_would_disprove_it(
            self, tmp_path, monkeypatch, capsys):
        """A large M means the digests are not surviving and the saving is illusory."""
        self.sweep(tmp_path, monkeypatch, 0.0, s3=True)
        assert "M re-read" in capsys.readouterr().out

    def test_a_zero_verify_without_a_multipart_source_claims_nothing(
            self, tmp_path, monkeypatch, capsys):
        """
        Whole-file md5 is sequential and cannot be assembled from parts, so a cheap
        verify there says nothing about in-transfer hashing either way.
        """
        self.sweep(tmp_path, monkeypatch, 0.0, s3=False)
        out = capsys.readouterr().out
        assert "read-back was skipped entirely" not in out
        assert "did not use one" in out

    def test_an_expensive_verify_still_recommends_the_fix(
            self, tmp_path, monkeypatch, capsys):
        self.sweep(tmp_path, monkeypatch, 60.0, s3=True)
        out = capsys.readouterr().out
        assert "avoidable and already implemented" in out


class TestTheResumeKillMustLandWhereItAims:
    """
    `pdl resume` aims three SIGKILLs at 25/50/75% of the object. All three landed after
    132 bytes -- the one-byte range probe and its headers -- because the trigger read
    `os.path.getsize(dest)`, and the downloader creates the destination sparse with
    ftruncate at the FULL object size. So the apparent size is final on the first tick,
    every kill fires immediately, and the last attempt performs a fresh download.

    The run then reported "refetched 31.79 MiB (0.8% overhead)" against a claim that a
    broken frontier would show ~3 GiB. It looked like a strong pass. It was the protocol
    overhead of one clean download, and the resume path was never exercised.

    Same sparse-file hazard the downloader already guards elsewhere:
    clear_preallocated_working_file exists because a size-based check "would see a
    full-size sparse file and skip the download entirely".
    """

    def test_the_trigger_uses_allocated_blocks_not_apparent_size(self, tmp_path):
        """
        A sparse file the size of the object with almost nothing written: the trigger
        must see ~0, not the full length.
        """
        path = tmp_path / "sparse.bin"
        with open(path, "wb") as handle:
            handle.truncate(4 * 1024 ** 3)
            handle.write(b"x" * 4096)
        apparent = os.path.getsize(str(path))
        allocated = os.stat(str(path)).st_blocks * 512
        assert apparent >= 4 * 1024 ** 3
        assert allocated < apparent / 100, (allocated, apparent)

    def test_the_source_no_longer_reads_apparent_size(self):
        # Code only: the comment explaining the bug names getsize, and a naive substring
        # check fails on the explanation of the thing it is checking for.
        code = "\n".join(line.split("#", 1)[0]
                         for line in inspect.getsource(bench.run_download).splitlines())
        assert "st_blocks" in code
        assert "getsize" not in code, "apparent size is the bug"

    def test_a_kill_short_of_its_target_is_premature(self):
        assert bench.premature({"kill_target": 1 << 30, "killed": True,
                                "killed_at_bytes": 4096, "nic_bytes": 132})

    def test_a_kill_near_its_target_is_not(self):
        assert not bench.premature({"kill_target": 1 << 30, "killed": True,
                                    "killed_at_bytes": int(0.9 * (1 << 30)),
                                    "nic_bytes": int(0.9 * (1 << 30))})

    def test_an_unkilled_attempt_is_never_premature(self):
        """The final attempt runs to completion by design."""
        assert not bench.premature({"kill_target": None, "killed": False,
                                    "killed_at_bytes": None, "nic_bytes": 1 << 32})

    def test_a_resumed_attempt_is_judged_on_file_progress_not_its_own_transfer(self):
        """
        The measured run: three kills at 25/50/75% of 4 GiB, and EVERY attempt
        transferred ~1.01 GiB because the file already held the earlier attempts' work.
        Comparing a per-attempt increment against a cumulative threshold flagged
        attempt 3 (target 3 GiB, own transfer 1.01 GiB) as premature -- a false alarm on
        the best resume result we have.
        """
        third = {"kill_target": 3 * (1 << 30), "killed": True,
                 "killed_at_bytes": 3 * (1 << 30) + (1 << 20),
                 "nic_bytes": 1 << 30}
        assert not bench.premature(third), "a correct resume must not be flagged"

    def test_an_unreadable_kill_point_counts_as_premature(self):
        """
        If the monitor never read a size, the trigger was not tracking anything -- which
        is the defect this exists to catch, so it must not fail open.
        """
        assert bench.premature({"kill_target": 1 << 30, "killed": True,
                                "killed_at_bytes": None, "nic_bytes": 1 << 30})

    def test_the_verdict_refuses_to_call_it_a_resume_measurement(self, capsys):
        attempts = [{"kill_target": 1 << 30, "killed": True, "nic_bytes": 132,
                     "killed_at_bytes": 4096, "returncode": -9, "seconds": 0.25},
                    {"kill_target": None, "killed": False, "nic_bytes": 4 << 30,
                     "killed_at_bytes": None, "returncode": 0, "seconds": 68.0}]
        assert [a for a in attempts if bench.premature(a)]
        # the message itself is asserted through command_resume in the runbook-level
        # test below; here the classifier is the unit under test
        assert not bench.premature(attempts[1])


class TestTheSweepLeavesNoOrphanedMarkers:
    """
    After each row the sweep unlinked `bench.N.bin` but not `.bench.N.bin.k9pdl.done`.
    Six orphaned markers were found on the bench disk, dating from earlier sessions --
    and a completion marker with no file is exactly the state that made a later run exit
    0 after 108 bytes, report `final hash WRONG` and then traceback.

    The pre-run sweep cleans the row it is about to use, so the orphans only ever
    surfaced for connection counts nobody re-ran. That is worse than an obvious failure:
    the trap was invisible and survived across sessions.
    """

    def test_a_completed_row_removes_its_sidecars(self, tmp_path, monkeypatch):
        payload = b"z" * 8192
        digest = hashlib.md5(payload).hexdigest()

        def fake_run_download(source, dest, size, conns, min_chunk, **kw):
            with open(dest, "wb") as fh:
                fh.write(payload)
            # the downloader's sidecars, named as sidecar_paths() would
            directory, base = os.path.split(dest)
            for suffix in ("done", "json"):
                with open(os.path.join(directory,
                                       ".{}.k9pdl.{}".format(base, suffix)), "w") as fh:
                    fh.write("{}")
            return {"connections": conns, "returncode": 0, "seconds": 1.0,
                    "killed": False, "peak_rss": 1 << 20,
                    "phases": {"download": 1.0}, "nic_bytes": len(payload),
                    "disk_bytes": len(payload), "peak_nic_bytes_per_s": 1e8,
                    "peak_disk_bytes_per_s": 1e3, "stderr_tail": [], "fell_back": None,
                    "mean_streams": 3.0, "workers": conns,
                    "bookkeeping_seconds": None, "bookkeeping_calls": None,
                    "bookkeeping_mean": None, "bookkeeping_pct_workers": None}

        downloader = tmp_path / "parallel_download.py"
        downloader.write_bytes(b"# stand-in\n")
        monkeypatch.setattr(bench, "DOWNLOADER", str(downloader))
        monkeypatch.setattr(bench, "run_download", fake_run_download)
        monkeypatch.setattr(bench, "resolve_source",
                            lambda a: (len(payload), bench.Verification("md5", digest)))
        bench.command_sweep(bench.build_parser().parse_args(
            ["sweep", "--url", "https://h/o", "--size", str(len(payload)),
             "--dest-dir", str(tmp_path), "--connections", "4", "16"]))

        left = sorted(os.path.basename(f) for f in glob.glob(str(tmp_path / "*bench*"))
                      + glob.glob(str(tmp_path / ".*bench*")))
        assert left == [], "orphans left behind: {}".format(left)

    def test_keep_still_keeps_everything(self, tmp_path, monkeypatch):
        """--keep exists so a run can be inspected afterwards; it must not be broken."""
        source = inspect.getsource(bench.command_sweep)
        assert "if not args.keep:" in source


class TestAChattyDownloadIsNotADeadlock:
    """
    PRODUCTION-OF-THE-MEASUREMENT BUG. run_download polled the child without reading a
    byte of its stderr until it exited:

        process = subprocess.Popen(..., stderr=subprocess.PIPE)
        while True:
            if process.poll() is not None: break
            ...
        stderr = process.stderr.read()

    A pipe holds 64 KiB. The downloader logs a ~70-byte progress line every
    PROGRESS_INTERVAL (5s), so after roughly 900 lines -- about 78 MINUTES -- the buffer
    fills, the child blocks in write() forever, and the loop polls a process that can
    never exit.

    The threshold is why it survived: every short measurement (4 GiB sweeps, resume runs,
    probes) finishes long before it, and only the full-size run crosses it. It presents as
    a silent hang with no row, no --json, and a destination that stops growing -- which is
    indistinguishable from a wedged source, and cost an overnight 279 GiB run.
    """

    def drain(self, payload, **kw):
        read, write = os.pipe()
        os.write(write, payload)
        os.close(write)
        sink, out = [], io.StringIO()
        bench._drain_stderr(os.fdopen(read, "rb"), sink, out=out, **kw)
        return sink, out.getvalue()

    def chatty(self, tmp_path, lines, width=70):
        """A stand-in downloader that floods stderr, then writes the destination."""
        script = tmp_path / "chatty.py"
        script.write_text(
            "import sys\n"
            "args = sys.argv\n"
            "dest = args[args.index('--dest') + 1]\n"
            "size = int(args[args.index('--size') + 1])\n"
            "sys.stderr.write('[k9pdl] k9pdl-phase download 1.0s, 1.0 MB/s\\n')\n"
            "for i in range({lines}):\n"
            "    sys.stderr.write(('[k9pdl] {{:.1f}}% ({{}}/{n} bytes)'\n"
            "                      .format(i / 10.0, i)).ljust({w}) + '\\n')\n"
            "    sys.stderr.flush()\n"
            "open(dest, 'wb').write(b'x' * size)\n".format(
                lines=lines, n=lines, w=width))
        return str(script)

    def download(self, tmp_path, monkeypatch, lines=3000, deadline=90):
        """
        Call run_download with a deadline, because the bug under test is a HANG.

        Every assertion here has to be reachable: run it inline and a regression wedges
        the whole suite with no failure and no output, which is the same unhelpful
        silence the fix exists to remove. The thread is a daemon and the child dies of
        EPIPE once the interpreter drops the read end, so a timeout leaves nothing behind.
        """
        monkeypatch.setattr(bench, "DOWNLOADER", self.chatty(tmp_path, lines))
        outcome = []
        worker = threading.Thread(
            target=lambda: outcome.append(bench.run_download(
                ["--url", "https://h/o"], str(tmp_path / "out.bin"), 64, 4, 1 << 20)),
            daemon=True)
        worker.start()
        worker.join(deadline)
        assert not worker.is_alive(), (
            "run_download did not return within {}s: the child is blocked writing to a "
            "full stderr pipe and poll() will never report an exit".format(deadline))
        return outcome[0]

    def test_a_child_that_outgrows_the_pipe_still_completes(self, tmp_path, monkeypatch):
        """~200 KiB of stderr, comfortably past the 64 KiB a pipe holds."""
        assert self.download(tmp_path, monkeypatch)["returncode"] == 0

    def test_the_whole_of_a_long_stderr_is_captured(self, tmp_path, monkeypatch):
        """
        Draining must not become sampling. Every parsed number -- phases, streams,
        bookkeeping, commit -- comes out of this text, and the phase line is emitted
        FIRST here precisely so that dropping the head of the stream would lose it.
        """
        outcome = self.download(tmp_path, monkeypatch)
        assert outcome["phases"] == {"download": 1.0}, outcome["phases"]

    def test_the_old_unread_pipe_really_does_deadlock(self, tmp_path):
        """
        Mutation check. Reproduce the original loop exactly -- poll to exit, never read --
        and it must fail to exit, or the test above proves nothing.
        """
        process = subprocess.Popen(
            [sys.executable, self.chatty(tmp_path, 3000),
             "--dest", str(tmp_path / "m.bin"), "--size", "8"],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        try:
            deadline = time.time() + 15
            while time.time() < deadline and process.poll() is None:
                time.sleep(0.25)
            assert process.poll() is None, (
                "the child exited with its stderr unread, so 3000 lines fit in the pipe "
                "on this platform and the deadlock cannot be reproduced here -- raise "
                "the line count until it does")
        finally:
            process.kill()
            process.wait()

    def test_a_long_run_emits_a_heartbeat(self):
        """
        The other half of the defect: `pdl sweep` printed nothing between the header and
        the result row, so a slow run and a hung one looked the same for hours. Progress
        lines are echoed on a slow clock so the heartbeat cannot become the wall of output
        it is reading.
        """
        sink, out = self.drain(
            b"[k9pdl] 1.0% (1/100 bytes, 1 transferred)\n"
            b"[k9pdl] chunk 3: fetched\n"
            b"[k9pdl] 2.0% (2/100 bytes, 2 transferred)\n", echo_every=0.0001)

        assert len(sink) == 3, sink                        # nothing dropped from capture
        assert "1.0% (1/100 bytes" in out, out
        assert "chunk 3: fetched" not in out, out          # not a progress line

    def test_the_heartbeat_is_rate_limited(self):
        sink, out = self.drain(b"".join(
            b"[k9pdl] %d.0%% (%d/100 bytes, %d transferred)\n" % (i, i, i)
            for i in range(50)), echo_every=3600)

        assert len(sink) == 50
        assert len(out.splitlines()) <= 1, out


class TestAStallSaysWhyItStalled:
    """
    SECOND-ORDER INSTRUMENTATION DEFECT, found by the fix for the first one.

    The heartbeat above proved the pipe was being read. It did not say why a run stalled
    at 96%, because the downloader's two stall messages -- `retrying in Xs` and `ENOSPC,
    waiting Ns` -- are not progress lines, and progress lines were the only thing echoed.
    The filter excluded exactly the lines that explain a stall, so the operator was left
    where they started: stat the file from another shell and guess.

    Worse, the S3 path discards the underlying error (`S3ApiSource.open_range` runs `aws`
    with stderr=DEVNULL), so a credential expiry or a 403 surfaces only as a retry count.
    Echoing the retry line is the only in-band signal that anything is wrong at all.
    """

    def drain(self, payload, **kw):
        read, write = os.pipe()
        os.write(write, payload)
        os.close(write)
        sink, out = [], io.StringIO()
        bench._drain_stderr(os.fdopen(read, "rb"), sink, out=out, **kw)
        return sink, out.getvalue()

    def test_a_retry_is_echoed_without_waiting_for_the_heartbeat(self):
        """
        The whole point: a stall must be explained on a clock much shorter than the
        five-minute heartbeat, or the explanation arrives after the operator has already
        killed the run.
        """
        sink, out = self.drain(
            b"[k9pdl] chunk 7: HTTP 403 -- retrying in 4.0s (attempt 2/8)\n",
            echo_every=3600, stall_every=3600)

        assert "retrying in 4.0s" in out, out
        assert len(sink) == 1

    def test_enospc_is_echoed(self):
        sink, out = self.drain(
            b"[k9pdl] chunk 12: ENOSPC, waiting 8s for the disk to grow\n",
            echo_every=3600, stall_every=3600)
        assert "ENOSPC" in out, out

    def test_a_retry_storm_reads_as_a_storm(self):
        """
        Rate-limited like the heartbeat, or a failing endpoint buries the terminal. But
        the suppressed count has to travel with it: one echoed retry line looks like one
        unlucky chunk, which is a completely different diagnosis from a dead credential.
        """
        sink, out = self.drain(b"".join(
            b"[k9pdl] chunk %d: retrying in 1.0s (attempt 2/8)\n" % i
            for i in range(200)), echo_every=3600, stall_every=3600)

        assert len(sink) == 200
        assert len(out.splitlines()) == 2, out
        assert "[+199 more suppressed]" in out, out

    def test_the_suppressed_count_survives_the_stream_ending(self):
        """
        The count is carried to the next echo, and at EOF there is no next echo. A storm
        that ends -- which is what a failing run looks like -- would otherwise report its
        first line and nothing else, reading as one unlucky chunk.
        """
        sink, out = self.drain(
            b"[k9pdl] chunk 1: retrying in 1.0s (attempt 2/8)\n"
            b"[k9pdl] chunk 2: retrying in 1.0s (attempt 3/8)\n",
            echo_every=3600, stall_every=3600)

        assert len(sink) == 2
        assert "[+1 more suppressed]" in out, out

    def test_stalls_and_progress_keep_separate_clocks(self):
        """
        A stall must not be starved by a recent heartbeat, nor vice versa. Sharing one
        clock would mean a progress line 4 minutes ago silences the retry explaining why
        there will not be another one.
        """
        sink, out = self.drain(
            b"[k9pdl] 96.0% (96/100 bytes, 96 transferred)\n"
            b"[k9pdl] chunk 7: retrying in 4.0s (attempt 2/8)\n",
            echo_every=3600, stall_every=3600)

        assert "96.0%" in out, out
        assert "retrying in 4.0s" in out, out

    def test_the_guard_fails_if_stalls_go_back_through_the_heartbeat_filter(self):
        """
        Mutation check. The defect was a filter that let only progress lines through, so
        reproduce exactly that -- one clock, `% (` required -- and confirm the retry is
        lost. Without this the tests above would still pass against a build that echoed
        everything unconditionally, which is a different bug.
        """
        payload = (b"[k9pdl] 96.0% (96/100 bytes, 96 transferred)\n"
                   b"[k9pdl] chunk 7: retrying in 4.0s (attempt 2/8)\n")
        echoed = []
        last = [0.0]
        for raw in payload.splitlines():
            line = raw.decode()
            if time.time() - last[0] < 3600:
                continue
            if "% (" not in line:
                continue
            last[0] = time.time()
            echoed.append(line)

        assert not any("retrying" in line for line in echoed), (
            "the old single-clock progress-only filter echoed a retry line, so the "
            "defect being guarded against cannot be reproduced and the guard is vacuous")
