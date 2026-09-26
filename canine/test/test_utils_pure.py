"""
Pure unit tests for canine.utils — no SLURM cluster, no GCP credentials required.
The legacy test_utils.py has randomized ArgumentHelper round-trip tests; this file
adds deterministic, scenario-specific coverage including pricing and hashing.
"""
import ast
import inspect
import io
import re
import subprocess
import warnings
from unittest.mock import MagicMock, patch

import pytest
import requests
from canine.utils import (
    ArgumentHelper,
    get_default_gcp_zone,
    _get_mtype_cost,
    gcp_hourly_cost,
    base32,
    sha1_base32,
    check_call,
    localization_bucket_name,
    LOCALIZATION_BUCKET_HASH_LEN,
)


@pytest.fixture(autouse=True)
def clear_mtype_cache():
    """_get_mtype_cost uses lru_cache; clear between tests."""
    _get_mtype_cost.cache_clear()
    yield
    _get_mtype_cost.cache_clear()


# ---------------------------------------------------------------------------
# ArgumentHelper
# ---------------------------------------------------------------------------

class TestArgumentHelper:

    def test_empty_produces_empty_commandline(self):
        assert ArgumentHelper().commandline == ""

    def test_long_flag(self):
        assert "--requeue" in ArgumentHelper("requeue").commandline

    def test_short_flag_has_single_dash(self):
        cl = ArgumentHelper("v").commandline
        assert " -v" in cl
        assert "--v" not in cl

    def test_long_param(self):
        assert "--job-name=myjob" in ArgumentHelper(job_name="myjob").commandline

    def test_short_param(self):
        assert "-n=4" in ArgumentHelper(n="4").commandline

    def test_underscore_translated_to_hyphen_in_params(self):
        cl = ArgumentHelper(cpus_per_task="8").commandline
        assert "--cpus-per-task=8" in cl
        assert "cpus_per_task" not in cl

    def test_underscore_translated_to_hyphen_in_flags(self):
        cl = ArgumentHelper("no_requeue").commandline
        assert "--no-requeue" in cl

    def test_translate_static(self):
        assert ArgumentHelper.translate("cpus_per_task") == "cpus-per-task"
        assert ArgumentHelper.translate("already-hyphenated") == "already-hyphenated"

    def test_true_kwarg_becomes_flag(self):
        ah = ArgumentHelper(requeue=True)
        assert "--requeue" in ah.commandline
        assert "requeue" in ah.flags

    def test_false_kwarg_removes_flag(self):
        ah = ArgumentHelper("requeue")
        ah["requeue"] = False
        assert "--requeue" not in ah.commandline

    def test_setitem_string_goes_to_params(self):
        ah = ArgumentHelper()
        ah["mem"] = "16G"
        assert "--mem=16G" in ah.commandline

    def test_setitem_true_adds_flag(self):
        ah = ArgumentHelper()
        ah["exclusive"] = True
        assert "--exclusive" in ah.commandline

    def test_delitem_param(self):
        ah = ArgumentHelper(partition="gpu")
        del ah["partition"]
        assert "--partition" not in ah.commandline

    def test_delitem_flag(self):
        ah = ArgumentHelper("requeue")
        del ah["requeue"]
        assert "--requeue" not in ah.commandline

    def test_delitem_missing_key_raises(self):
        with pytest.raises(KeyError):
            del ArgumentHelper()["nonexistent"]

    def test_getitem_flag_returns_true(self):
        assert ArgumentHelper("requeue")["requeue"] is True

    def test_getitem_param_returns_value(self):
        assert ArgumentHelper(mem="8G")["mem"] == "8G"

    def test_multiple_flags_and_params_all_present(self):
        cl = ArgumentHelper("requeue", "exclusive", job_name="test", mem="4G").commandline
        assert "--requeue" in cl
        assert "--exclusive" in cl
        assert "--job-name=test" in cl
        assert "--mem=4G" in cl

    def test_values_are_shell_quoted(self):
        # shlex.quote wraps values with spaces in single quotes
        cl = ArgumentHelper(job_name="my job").commandline
        assert "'my job'" in cl


# ---------------------------------------------------------------------------
# _get_mtype_cost
# ---------------------------------------------------------------------------

class TestGetMtypeCost:

    def test_n1_standard_4(self):
        non_pre, pre = _get_mtype_cost("n1-standard-4")
        assert non_pre == pytest.approx(0.0475 * 4)
        assert pre == pytest.approx(0.01 * 4)

    def test_n1_standard_1(self):
        non_pre, pre = _get_mtype_cost("n1-standard-1")
        assert non_pre == pytest.approx(0.0475)
        assert pre == pytest.approx(0.01)

    def test_n2_highmem_8(self):
        non_pre, pre = _get_mtype_cost("n2-highmem-8")
        assert non_pre == pytest.approx(0.0655 * 8)
        assert pre == pytest.approx(0.01585 * 8)

    def test_c2_standard_4(self):
        non_pre, pre = _get_mtype_cost("c2-standard-4")
        assert non_pre == pytest.approx(0.0522 * 4)

    def test_fixed_f1_micro(self):
        non_pre, pre = _get_mtype_cost("f1-micro")
        assert non_pre == pytest.approx(0.0076)
        assert pre == pytest.approx(0.0035)

    def test_fixed_g1_small(self):
        non_pre, pre = _get_mtype_cost("g1-small")
        assert non_pre == pytest.approx(0.0257)
        assert pre == pytest.approx(0.007)

    def test_custom_n1_within_regular_memory(self):
        # 4 CPUs, 16 GB — within the 6.5 GB/core = 26 GB limit → no extended memory
        from canine.utils import custom_mtypes
        non_pre, pre = _get_mtype_cost("n1-custom-4-16384")
        pm = custom_mtypes["n1-custom"]
        cores, mem_gb = 4, 16384 / 1024
        expected_non = pm.cpu_cost * cores + pm.mem_cost * mem_gb
        assert non_pre == pytest.approx(expected_non)

    def test_custom_n1_bare_form(self):
        # Bare 'custom-X-Y' form should resolve to n1-custom pricing, identical to 'n1-custom-X-Y'.
        assert _get_mtype_cost("custom-4-16384") == _get_mtype_cost("n1-custom-4-16384")

    def test_unknown_family_raises(self):
        with pytest.raises(ValueError):
            _get_mtype_cost("n9-unknown-4")

    def test_malformed_mtype_raises(self):
        with pytest.raises(ValueError):
            _get_mtype_cost("n1standard")

    def test_result_is_cached(self):
        r1 = _get_mtype_cost("n1-standard-4")
        r2 = _get_mtype_cost("n1-standard-4")
        assert r1 is r2

    def test_no_preemptible_for_m2(self):
        # m2-ultramem has preemptible cost = 0 per the table
        _, pre = _get_mtype_cost("m2-ultramem-208")
        assert pre == 0.0


# ---------------------------------------------------------------------------
# gcp_hourly_cost
# ---------------------------------------------------------------------------

class TestGcpHourlyCost:

    def test_non_preemptible_costs_more_than_preemptible(self):
        assert gcp_hourly_cost("n1-standard-4", preemptible=False) > \
               gcp_hourly_cost("n1-standard-4", preemptible=True)

    def test_ssd_adds_cost(self):
        assert gcp_hourly_cost("n1-standard-4", ssd_size=100) > \
               gcp_hourly_cost("n1-standard-4")

    def test_hdd_adds_cost(self):
        assert gcp_hourly_cost("n1-standard-4", hdd_size=500) > \
               gcp_hourly_cost("n1-standard-4")

    def test_gpu_adds_cost(self):
        assert gcp_hourly_cost("n1-standard-4", gpu_type="nvidia-tesla-t4", gpu_count=1) > \
               gcp_hourly_cost("n1-standard-4")

    def test_zero_gpu_count_no_gpu_cost(self):
        base = gcp_hourly_cost("n1-standard-4")
        with_zero = gcp_hourly_cost("n1-standard-4", gpu_type="nvidia-tesla-t4", gpu_count=0)
        assert base == pytest.approx(with_zero)

    def test_none_gpu_type_no_gpu_cost(self):
        base = gcp_hourly_cost("n1-standard-4")
        with_none = gcp_hourly_cost("n1-standard-4", gpu_type=None, gpu_count=2)
        assert base == pytest.approx(with_none)

    def test_multiple_gpus_cost_scales(self):
        one_gpu = gcp_hourly_cost("n1-standard-4", gpu_type="nvidia-tesla-t4", gpu_count=1)
        two_gpu = gcp_hourly_cost("n1-standard-4", gpu_type="nvidia-tesla-t4", gpu_count=2)
        base = gcp_hourly_cost("n1-standard-4")
        from canine.utils import gpu_pricing
        assert two_gpu - base == pytest.approx(2 * gpu_pricing["nvidia-tesla-t4"][0])


# ---------------------------------------------------------------------------
# base32 / sha1_base32
# ---------------------------------------------------------------------------

class TestBase32:

    def test_deterministic(self):
        b = b"\x01\x02\x03"
        assert base32(b) == base32(b)

    def test_different_inputs_differ(self):
        assert base32(b"\x01") != base32(b"\x02")

    def test_output_is_lowercase_alphanumeric(self):
        result = base32(b"\xDE\xAD\xBE\xEF" * 4)
        assert result.isalnum() and result == result.lower()

    def test_known_single_byte(self):
        # 0xFF = 11111111, padded to 10 bits: 1111111100
        # groups of 5: 11111 11100 → indices 31, 28 → '5', '2'
        # (table: abcdefghijklmnopqrstuvwxyz012345, index 28 = '2')
        assert base32(b"\xff") == "52"


class TestSha1Base32:

    def test_deterministic(self):
        assert sha1_base32(b"hello world", 4) == sha1_base32(b"hello world", 4)

    def test_different_inputs_differ(self):
        assert sha1_base32(b"abc", 4) != sha1_base32(b"xyz", 4)

    def test_n_4_gives_7_chars(self):
        # 4 bytes = 32 bits; ceil(32/5) = 7 base32 characters
        assert len(sha1_base32(b"test", n=4)) == 7

    def test_n_none_encodes_full_sha1(self):
        # SHA1 = 20 bytes = 160 bits; 160 % 5 == 0, but the padding expression
        # `5 - (len(bits) % 5)` yields 5 (not 0) when perfectly divisible,
        # adding one extra group → 33 characters, not 32.
        assert len(sha1_base32(b"test", n=None)) == 33


# ---------------------------------------------------------------------------
# check_call
# ---------------------------------------------------------------------------

class TestCheckCall:

    def test_zero_rc_no_exception(self):
        check_call("cmd", 0)

    def test_nonzero_rc_raises_called_process_error(self):
        with pytest.raises(subprocess.CalledProcessError) as exc:
            check_call("mycommand", 1)
        assert exc.value.returncode == 1
        assert exc.value.cmd == "mycommand"

    def test_streams_printed_on_failure(self, capsys):
        with pytest.raises(subprocess.CalledProcessError):
            check_call("cmd", 2,
                       stdout=io.BytesIO(b"out content\n"),
                       stderr=io.BytesIO(b"err content\n"))
        captured = capsys.readouterr()
        assert "out content" in captured.out
        assert "err content" in captured.err

    def test_none_streams_no_crash(self):
        with pytest.raises(subprocess.CalledProcessError):
            check_call("cmd", 1, stdout=None, stderr=None)


# ---------------------------------------------------------------------------
# localization_bucket_name
# ---------------------------------------------------------------------------

MD5 = "0123456789abcdef0123456789abcdef"  # 32 hex chars, as hash_set() returns
PN = "406002258908"                       # 12 digits, as real project numbers are


class TestLocalizationBucketName:

    def test_shape(self):
        assert localization_bucket_name(PN, "us-central1", MD5) == \
          "wolf-406002258908-us-central1-" + MD5[:21]

    def test_stable_for_identical_inputs(self):
        assert localization_bucket_name(PN, "us-central1", MD5) == \
               localization_bucket_name(PN, "us-central1", MD5)

    def test_differs_across_regions(self):
        """Buckets are regional: the same content in two regions needs two names."""
        assert localization_bucket_name(PN, "us-central1", MD5) != \
               localization_bucket_name(PN, "us-east1", MD5)

    def test_differs_across_content(self):
        other = "f" * 32
        assert localization_bucket_name(PN, "us-central1", MD5) != \
               localization_bucket_name(PN, "us-central1", other)

    @pytest.mark.parametrize("region", [
        "us-east1", "us-central1", "europe-west1", "asia-southeast1",
        "southamerica-east1", "australia-southeast1", "northamerica-northeast1",
    ])
    def test_fits_63_chars_in_every_region(self, region):
        """northamerica-northeast1 (23 chars) is the boundary case: exactly 63."""
        name = localization_bucket_name(PN, region, MD5)
        assert len(name) <= 63, "{} -> {} chars".format(region, len(name))

    def test_longest_region_is_exactly_at_the_boundary(self):
        name = localization_bucket_name(PN, "northamerica-northeast1", MD5)
        assert len(name) == 63

    def test_hash_truncated_not_full_md5(self):
        name = localization_bucket_name(PN, "us-central1", MD5)
        assert name.endswith(MD5[:LOCALIZATION_BUCKET_HASH_LEN])
        assert MD5 not in name  # the full 32-char hash would overflow

    def test_rejects_oversized_name(self):
        """A longer future project number must fail loudly, not reach GCS."""
        with pytest.raises(ValueError):
            localization_bucket_name("9" * 40, "northamerica-northeast1", MD5)

    def test_lowercase_and_legal_charset(self):
        name = localization_bucket_name(PN, "US-CENTRAL1", MD5)
        assert name == name.lower()
        assert re.match(r'^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$', name)


# ---------------------------------------------------------------------------
# get_default_gcp_zone
# ---------------------------------------------------------------------------

def _unreachable_metadata():
    return patch("canine.utils.requests.get",
                 side_effect=requests.exceptions.ConnectionError())


def _metadata_says(zone_path, status=200):
    resp = MagicMock()
    resp.status_code = status
    resp.text = zone_path
    return patch("canine.utils.requests.get", return_value=resp)


def _gcloud_says(stdout, returncode=0):
    return patch("canine.utils.subprocess.run",
                 return_value=MagicMock(returncode=returncode, stdout=stdout))


class TestGetDefaultGcpZone:
    """
    A zone this function makes up is worse than no zone at all: it silently
    places the Anywhere Cache and localization buckets away from the cluster,
    which costs money and logs nothing. So the last resort is an exception.
    """

    @pytest.fixture(autouse=True)
    def clear_zone_cache(self):
        get_default_gcp_zone.cache_clear()
        yield
        get_default_gcp_zone.cache_clear()

    def test_metadata_server_wins(self):
        with _metadata_says("projects/406002258908/zones/us-central1-c"):
            assert get_default_gcp_zone() == "us-central1-c"

    def test_metadata_response_is_reduced_to_the_bare_zone(self):
        """The endpoint returns a full resource path; only the last segment is a zone."""
        with _metadata_says("projects/406002258908/zones/europe-west4-a"):
            assert "/" not in get_default_gcp_zone()

    def test_falls_through_to_gcloud_config(self):
        with _unreachable_metadata(), _gcloud_says(b"us-east1-b\n"):
            assert get_default_gcp_zone() == "us-east1-b"

    def test_raises_when_both_sources_are_exhausted(self):
        with _unreachable_metadata(), _gcloud_says(b"(unset)\n"):
            with pytest.raises(ValueError) as e:
                get_default_gcp_zone()
        assert "compute/zone" in str(e.value), "the error must say how to fix it"

    def test_raises_when_gcloud_itself_fails(self):
        with _unreachable_metadata(), _gcloud_says(b"ERROR: not authenticated", returncode=1):
            with pytest.raises(ValueError):
                get_default_gcp_zone()

    def test_non_200_from_metadata_is_not_treated_as_a_zone(self):
        with _metadata_says("nope", status=404), _gcloud_says(b"(unset)\n"):
            with pytest.raises(ValueError):
                get_default_gcp_zone()

    def test_error_does_not_name_a_zone_as_if_it_were_usable(self):
        """The removed fallback looked like a working answer; that was the danger."""
        with _unreachable_metadata(), _gcloud_says(b"(unset)\n"):
            with pytest.raises(ValueError) as e:
                get_default_gcp_zone()
        assert "us-central1-a" not in str(e.value)

    def test_success_is_cached(self):
        with _metadata_says("projects/1/zones/us-central1-c") as m:
            get_default_gcp_zone()
            get_default_gcp_zone()
        assert m.call_count == 1

    def test_failure_is_not_cached(self):
        """
        lru_cache does not memoize exceptions, so a process that starts before
        the zone is configured recovers without a restart.
        """
        with _unreachable_metadata(), _gcloud_says(b"(unset)\n"):
            with pytest.raises(ValueError):
                get_default_gcp_zone()
        with _metadata_says("projects/1/zones/us-central1-c"):
            assert get_default_gcp_zone() == "us-central1-c"


class TestNoZoneLookupAtImportTime:

    def test_localization_base_has_no_module_level_zone(self):
        from canine.localization import base
        assert not hasattr(base, "ZONE")

    def test_no_module_scope_call_anywhere_in_localization_base(self):
        """
        CI regression guard. Now that this function raises, a single module-scope
        call makes `import canine` fail outright on any machine that is not on
        GCE and has no compute/zone set -- GitHub Actions included, which is
        where the whole suite runs.
        """
        from canine.localization import base
        with warnings.catch_warnings():
            # base.py has pre-existing invalid escapes in its embedded bash;
            # re-parsing it here would otherwise re-emit them as suite noise
            warnings.simplefilter("ignore", SyntaxWarning)
            tree = ast.parse(inspect.getsource(base))
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue  # calls inside a def are lazy by definition
            for sub in ast.walk(node):
                if isinstance(sub, ast.Call) and getattr(sub.func, "id", None) == "get_default_gcp_zone":
                    pytest.fail("module-scope zone lookup at base.py line {}".format(sub.lineno))
