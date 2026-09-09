"""
Tests for benchmark_localization.py's credential resolution.

Not throughput measurement -- that needs a real worker. This covers the part with actual
logic in it, and the part where a mistake leaks a secret: credentials are read from the
canonical aws location rather than passed on a command line, because an argument is
visible in `ps` to every user on the box and lands in shell history. When the keys are
issued by someone else, that matters.
"""

import importlib.util
import os

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
