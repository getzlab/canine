"""
Tests for benchmark_localization.py's credential resolution.

Not throughput measurement -- that needs a real worker. This covers the part with actual
logic in it, and the part where a mistake leaks a secret: credentials are read from the
canonical aws location rather than passed on a command line, because an argument is
visible in `ps` to every user on the box and lands in shell history. When the keys are
issued by someone else, that matters.
"""

import glob
import importlib.util
import os
import tempfile

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
        import re
        with open(self.RUNBOOK) as handle:
            text = handle.read()
        defined, problems = set(self.AMBIENT), []
        for block in re.findall(r"```bash\n(.*?)```", text, re.S):
            for line in block.split("\n"):
                stripped = line.strip()
                if stripped.startswith("#"):
                    continue
                for name in re.findall(r"\$\{?([A-Za-z_][A-Za-z0-9_]*)\}?", line):
                    if name not in defined:
                        problems.append((name, stripped))
                for name in re.findall(r"([A-Za-z_][A-Za-z0-9_]*)=", stripped):
                    defined.add(name)
                for name in re.findall(r"\bfor\s+([A-Za-z_][A-Za-z0-9_]*)\s+in\b",
                                      stripped):
                    defined.add(name)
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
