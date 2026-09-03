"""
Pure unit tests for canine.cost -- no SLURM cluster, no live GCP credentials
required. Catalog API interaction is exercised against synthetic SKU fixtures,
not a real network call.
"""
import json
import os
import threading
import time
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from canine import cost


# ---------------------------------------------------------------------------
# AllocTRES / memory / timestamp parsing
# ---------------------------------------------------------------------------

class TestParseMemToMb:
    def test_gigabytes(self):
        assert cost._parse_mem_to_mb("4G") == 4096.0

    def test_megabytes(self):
        assert cost._parse_mem_to_mb("4096M") == 4096.0

    def test_kilobytes(self):
        assert cost._parse_mem_to_mb("1024K") == 1.0

    def test_terabytes(self):
        assert cost._parse_mem_to_mb("1T") == 1024.0 * 1024.0

    def test_bare_number_assumed_mb(self):
        assert cost._parse_mem_to_mb("512") == 512.0

    def test_empty_string(self):
        assert cost._parse_mem_to_mb("") is None

    def test_malformed(self):
        assert cost._parse_mem_to_mb("notanumberG") is None


class TestParseAllocTres:
    def test_normal(self):
        cpus, mem = cost.parse_alloc_tres("cpu=2,mem=4G,node=1,billing=2")
        assert cpus == 2
        assert mem == 4096.0

    def test_missing_cpu_key(self):
        cpus, mem = cost.parse_alloc_tres("mem=4G,node=1")
        assert cpus is None
        assert mem == 4096.0

    def test_missing_mem_key(self):
        cpus, mem = cost.parse_alloc_tres("cpu=2,node=1")
        assert cpus == 2
        assert mem is None

    def test_dash_placeholder(self):
        assert cost.parse_alloc_tres("-") == (None, None)

    def test_none_input(self):
        assert cost.parse_alloc_tres(None) == (None, None)

    def test_nan_input(self):
        assert cost.parse_alloc_tres(float("nan")) == (None, None)

    def test_malformed_cpu_value(self):
        cpus, mem = cost.parse_alloc_tres("cpu=notanumber,mem=4G")
        assert cpus is None
        assert mem == 4096.0


class TestResolveJobResources:
    """
    Confirmed live: some clusters' sacct only reports AllocTRES as e.g.
    "billing=1+" with no cpu=/mem= keys at all, even though the same row's
    NCPUS/ReqMem fields are fully populated -- this is the real-world case
    that motivated the NCPUS/ReqMem fallback.
    """

    def test_alloc_tres_has_both_fields_ignores_fallback(self):
        cpus, mem = cost.resolve_job_resources("cpu=2,mem=4G", ncpus=99, req_mem="99G")
        assert cpus == 2
        assert mem == 4096.0

    def test_alloc_tres_missing_both_fields_falls_back_to_ncpus_and_reqmem(self):
        cpus, mem = cost.resolve_job_resources("billing=1+", ncpus=1, req_mem="3G")
        assert cpus == 1
        assert mem == 3072.0

    def test_alloc_tres_missing_cpu_only_falls_back_for_cpu_only(self):
        cpus, mem = cost.resolve_job_resources("mem=4G", ncpus=2, req_mem="99G")
        assert cpus == 2
        assert mem == 4096.0  # from AllocTRES, not the (deliberately wrong) fallback

    def test_alloc_tres_missing_mem_only_falls_back_for_mem_only(self):
        cpus, mem = cost.resolve_job_resources("cpu=2", ncpus=99, req_mem="4G")
        assert cpus == 2  # from AllocTRES, not the (deliberately wrong) fallback
        assert mem == 4096.0

    def test_no_fallback_values_available_returns_none(self):
        cpus, mem = cost.resolve_job_resources("billing=1+")
        assert cpus is None
        assert mem is None

    def test_reqmem_strips_trailing_per_cpu_or_per_node_suffix(self):
        cpus, mem = cost.resolve_job_resources("billing=1+", ncpus=1, req_mem="3Gn")
        assert mem == 3072.0
        cpus, mem = cost.resolve_job_resources("billing=1+", ncpus=1, req_mem="3Gc")
        assert mem == 3072.0

    def test_unparseable_ncpus_leaves_cpus_none(self):
        cpus, mem = cost.resolve_job_resources("billing=1+", ncpus="not-a-number", req_mem="3G")
        assert cpus is None
        assert mem == 3072.0


class TestParseSacctTime:
    def test_unknown_is_nat(self):
        assert pd.isna(cost._parse_sacct_time("Unknown"))

    def test_dash_is_nat(self):
        assert pd.isna(cost._parse_sacct_time("-"))

    def test_none_is_nat(self):
        assert pd.isna(cost._parse_sacct_time(None))

    def test_valid_timestamp_parses(self):
        t = cost._parse_sacct_time("2026-01-01T00:00:00")
        assert t == pd.Timestamp("2026-01-01T00:00:00")


# ---------------------------------------------------------------------------
# capacity lookups
# ---------------------------------------------------------------------------

class TestLoadHostLut:
    def test_missing_file_returns_empty_frame(self, tmp_path):
        result = cost.load_host_lut(str(tmp_path / "nonexistent.pickle"))
        assert result.empty
        assert "machine_type" in result.columns

    def test_reads_real_pickle(self, tmp_path):
        df = pd.DataFrame(
          {"machine_type": ["n1-highcpu-8"], "preemptible": [True], "accelerator_count": [None], "accelerator_type": [None]},
          index = pd.Index(["worker1"], name = "idx"),
        )
        path = tmp_path / "host_LuT.pickle"
        df.to_pickle(path)
        result = cost.load_host_lut(str(path))
        assert result.loc["worker1", "machine_type"] == "n1-highcpu-8"


class TestLoadNodeTypes:
    def test_missing_file_returns_empty_frame(self, tmp_path):
        result = cost.load_node_types(str(tmp_path / "nonexistent.json"))
        assert result.empty

    def test_dedupes_preemptible_and_nonpreemptible_variants(self, tmp_path):
        path = tmp_path / "nodetypes.json"
        path.write_text(json.dumps([
          {"type": "n1-highcpu-8", "cpus": "8", "realmemory": "7000", "weight": "1", "number": 1000, "preemptible": True},
          {"type": "n1-highcpu-8", "cpus": "8", "realmemory": "7000", "weight": "1", "number": 1000, "preemptible": False},
        ]))
        result = cost.load_node_types(str(path))
        assert len(result) == 1
        assert result.loc["n1-highcpu-8", "cpus"] == 8
        assert result.loc["n1-highcpu-8", "realmemory"] == 7000.0


class TestNodeCapacity:
    def setup_method(self):
        self.host_lut = pd.DataFrame(
          {"machine_type": ["n1-highcpu-8"]},
          index = pd.Index(["worker1"], name = "idx"),
        )
        self.node_types = pd.DataFrame(
          {"cpus": [8], "realmemory": [7000.0]},
          index = pd.Index(["n1-highcpu-8"], name = "type"),
        )

    def test_found(self):
        vcpus, mem_mb = cost.node_capacity("worker1", self.host_lut, self.node_types)
        assert vcpus == 8
        assert mem_mb == 7000.0

    def test_node_not_in_host_lut(self):
        assert cost.node_capacity("worker99", self.host_lut, self.node_types) == (None, None)

    def test_machine_type_not_in_node_types(self):
        host_lut = pd.DataFrame({"machine_type": ["unknown-type"]}, index = pd.Index(["worker1"]))
        assert cost.node_capacity("worker1", host_lut, self.node_types) == (None, None)


# ---------------------------------------------------------------------------
# Catalog API SKU matching (synthetic fixtures -- no live network access)
# ---------------------------------------------------------------------------

def make_sku(description, region, family="Compute", tiered_rate_usd=0.05):
    units = int(tiered_rate_usd)
    nanos = int(round((tiered_rate_usd - units) * 1e9))
    return {
      "description": description,
      "category": {"resourceFamily": family},
      "serviceRegions": [region],
      "pricingInfo": [{"pricingExpression": {"tieredRates": [{"unitPrice": {"units": str(units), "nanos": nanos}}]}}],
    }


class TestSkuUnitPriceUsd:
    def test_units_and_nanos_combine(self):
        sku = make_sku("x", "us-central1", tiered_rate_usd=1.5)
        assert cost._sku_unit_price_usd(sku) == pytest.approx(1.5)

    def test_sub_dollar_price(self):
        sku = make_sku("x", "us-central1", tiered_rate_usd=0.031611)
        assert cost._sku_unit_price_usd(sku) == pytest.approx(0.031611)


class TestMatchComputeEnginePrice:
    def test_matches_on_demand_n1(self):
        skus = [
          make_sku("N1 Predefined Instance Core running in Americas", "us-central1", tiered_rate_usd=0.031611),
          make_sku("N1 Predefined Instance Ram running in Americas", "us-central1", tiered_rate_usd=0.004237),
          make_sku("Preemptible N1 Predefined Instance Core running in Americas", "us-central1", tiered_rate_usd=0.006655),
          make_sku("N2 Predefined Instance Core running in Americas", "us-central1", tiered_rate_usd=0.999),
        ]
        result = cost.match_compute_engine_price(skus, "n1-highcpu-8", "us-central1", preemptible=False)
        assert result == pytest.approx((0.031611, 0.004237))

    def test_matches_preemptible_variant(self):
        skus = [
          make_sku("N1 Predefined Instance Core running in Americas", "us-central1", tiered_rate_usd=0.031611),
          make_sku("Preemptible N1 Predefined Instance Core running in Americas", "us-central1", tiered_rate_usd=0.006655),
          make_sku("Preemptible N1 Predefined Instance Ram running in Americas", "us-central1", tiered_rate_usd=0.000892),
        ]
        result = cost.match_compute_engine_price(skus, "n1-highcpu-8", "us-central1", preemptible=True)
        assert result == pytest.approx((0.006655, 0.000892))

    def test_no_match_in_wrong_region(self):
        skus = [
          make_sku("N1 Predefined Instance Core running in Americas", "europe-west1", tiered_rate_usd=0.03),
          make_sku("N1 Predefined Instance Ram running in Americas", "europe-west1", tiered_rate_usd=0.004),
        ]
        assert cost.match_compute_engine_price(skus, "n1-highcpu-8", "us-central1", preemptible=False) is None

    def test_unrecognized_family_returns_none(self):
        assert cost.match_compute_engine_price([], "m1-ultramem-40", "us-central1", preemptible=False) is None

    def test_missing_ram_sku_returns_none(self):
        skus = [make_sku("N1 Predefined Instance Core running in Americas", "us-central1")]
        assert cost.match_compute_engine_price(skus, "n1-highcpu-8", "us-central1", preemptible=False) is None


class TestMatchAcceleratorPrice:
    def test_matches_gpu_model(self):
        skus = [
          make_sku("Nvidia Tesla T4 GPU running in Americas", "us-central1", tiered_rate_usd=0.35),
          make_sku("Nvidia Tesla V100 GPU running in Americas", "us-central1", tiered_rate_usd=2.48),
        ]
        result = cost.match_accelerator_price(skus, "nvidia-tesla-t4", "us-central1", preemptible=False)
        assert result == pytest.approx(0.35)

    def test_no_match_returns_none(self):
        assert cost.match_accelerator_price([], "nvidia-tesla-t4", "us-central1", preemptible=False) is None


# ---------------------------------------------------------------------------
# get_price (cache + API-call mocking)
# ---------------------------------------------------------------------------

class TestInvalidateBillingClient:
    def test_resets_singleton_forcing_rebuild(self):
        with patch("canine.cost._BILLING_CLIENT", "sentinel-client"):
            cost._invalidate_billing_client()
            assert cost._BILLING_CLIENT is None


class TestGetPrice:
    def setup_method(self):
        self.node_types = pd.DataFrame({"cpus": [8], "realmemory": [7168.0]}, index=pd.Index(["n1-highcpu-8"], name="type"))

    def test_unknown_machine_type_returns_none_without_api_call(self):
        with patch("canine.cost.get_billing_client") as mock_client:
            result = cost.get_price("unknown-type", "us-central1-a", False, node_types=self.node_types)
        assert result is None
        mock_client.assert_not_called()

    def test_cache_hit_skips_api_call(self, tmp_path):
        cache_path = tmp_path / "price_cache.json"
        key = cost._price_cache_key("n1-highcpu-8", "us-central1-a", False, None, 0)
        cache_path.write_text(json.dumps({key: 1.23}))
        with patch("canine.cost.PRICE_CACHE_PATH", str(cache_path)), \
             patch("canine.cost.get_billing_client") as mock_client:
            result = cost.get_price("n1-highcpu-8", "us-central1-a", False, node_types=self.node_types)
        assert result == 1.23
        mock_client.assert_not_called()

    def test_cache_miss_calls_api_and_persists(self, tmp_path):
        cache_path = tmp_path / "price_cache.json"

        fake_client = MagicMock()
        services = fake_client.services.return_value
        list_request = MagicMock()
        services.list.return_value = list_request
        list_request.execute.return_value = {"services": [{"displayName": "Compute Engine", "name": "services/6F81-5844-456A"}]}
        services.list_next.return_value = None

        skus_request = MagicMock()
        services.skus.return_value.list.return_value = skus_request
        skus_request.execute.return_value = {"skus": [
          make_sku("N1 Predefined Instance Core running in Americas", "us-central1", tiered_rate_usd=0.03),
          make_sku("N1 Predefined Instance Ram running in Americas", "us-central1", tiered_rate_usd=0.004),
        ]}
        services.skus.return_value.list_next.return_value = None

        with patch("canine.cost.PRICE_CACHE_PATH", str(cache_path)), \
             patch("canine.cost.get_billing_client", return_value=fake_client), \
             patch("canine.cost._COMPUTE_ENGINE_SERVICE_NAME", None):
            result = cost.get_price("n1-highcpu-8", "us-central1-a", False, node_types=self.node_types)

        expected = 0.03 * 8 + 0.004 * (7168.0 / 1024)
        assert result == pytest.approx(expected)
        assert json.loads(cache_path.read_text())[cost._price_cache_key("n1-highcpu-8", "us-central1-a", False, None, 0)] == pytest.approx(expected)

    def test_concurrent_cache_miss_is_serialized_not_raced(self, tmp_path):
        # Confirmed live: googleapiclient's httplib2-backed client isn't safe
        # for concurrent .execute() calls from multiple threads on the same
        # instance -- several wolF tasks finishing around the same time and
        # racing into a cold price-cache entry crashed the whole process with
        # "malloc(): unsorted double linked list corrupted". This drives many
        # threads through a real cache miss concurrently and confirms (a) the
        # mocked API is never entered by two threads at once, and (b) it's
        # only ever actually called once (the double-checked cache re-read
        # after acquiring the lock is what makes the other N-1 threads no-ops).
        cache_path = tmp_path / "price_cache.json"
        concurrent = [0]
        max_concurrent = [0]

        def make_execute(response):
            def _execute():
                concurrent[0] += 1
                max_concurrent[0] = max(max_concurrent[0], concurrent[0])
                time.sleep(0.05)
                concurrent[0] -= 1
                return response
            return _execute

        fake_client = MagicMock()
        services = fake_client.services.return_value
        list_request = MagicMock()
        services.list.return_value = list_request
        list_request.execute.side_effect = make_execute(
          {"services": [{"displayName": "Compute Engine", "name": "services/x"}]}
        )
        services.list_next.return_value = None

        skus_request = MagicMock()
        services.skus.return_value.list.return_value = skus_request
        skus_request.execute.side_effect = make_execute({"skus": [
          make_sku("N1 Predefined Instance Core running in Americas", "us-central1", tiered_rate_usd=0.03),
          make_sku("N1 Predefined Instance Ram running in Americas", "us-central1", tiered_rate_usd=0.004),
        ]})
        services.skus.return_value.list_next.return_value = None

        results = []
        with patch("canine.cost.PRICE_CACHE_PATH", str(cache_path)), \
             patch("canine.cost.get_billing_client", return_value=fake_client), \
             patch("canine.cost._COMPUTE_ENGINE_SERVICE_NAME", None):
            threads = [
              threading.Thread(target=lambda: results.append(
                cost.get_price("n1-highcpu-8", "us-central1-a", False, node_types=self.node_types)
              ))
              for _ in range(8)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        expected = 0.03 * 8 + 0.004 * (7168.0 / 1024)
        assert all(r == pytest.approx(expected) for r in results)
        assert max_concurrent[0] == 1
        assert list_request.execute.call_count == 1
        assert skus_request.execute.call_count == 1

    def test_api_exception_returns_none(self, tmp_path):
        cache_path = tmp_path / "price_cache.json"
        with patch("canine.cost.PRICE_CACHE_PATH", str(cache_path)), \
             patch("canine.cost.get_billing_client", side_effect=RuntimeError("no credentials")):
            result = cost.get_price("n1-highcpu-8", "us-central1-a", False, node_types=self.node_types)
        assert result is None

    def test_non_transient_error_does_not_retry(self, tmp_path):
        # a RuntimeError (e.g. "Could not find Compute Engine in Cloud Billing
        # Catalog services") is a logic/config problem retrying can't fix --
        # confirm it's still a single attempt, not caught by the new transient
        # retry loop.
        cache_path = tmp_path / "price_cache.json"
        mock_get_client = MagicMock(side_effect=RuntimeError("no credentials"))
        with patch("canine.cost.PRICE_CACHE_PATH", str(cache_path)), \
             patch("canine.cost.get_billing_client", mock_get_client):
            result = cost.get_price("n1-highcpu-8", "us-central1-a", False, node_types=self.node_types)
        assert result is None
        assert mock_get_client.call_count == 1

    def test_retries_on_transient_ssl_error_then_succeeds(self, tmp_path):
        # Confirmed live: "[SSL: RECORD_LAYER_FAILURE] record layer failure" --
        # a stale keep-alive connection on the long-lived shared client going
        # bad between infrequent price lookups. A failed fetch is never
        # cached, so without a retry this would keep failing (and degrading
        # every future not-yet-cached recipe to is_provisional) for the rest
        # of the cluster's life.
        import ssl
        cache_path = tmp_path / "price_cache.json"

        fake_client = MagicMock()
        services = fake_client.services.return_value
        list_request = MagicMock()
        services.list.return_value = list_request
        list_request.execute.return_value = {"services": [{"displayName": "Compute Engine", "name": "services/x"}]}
        services.list_next.return_value = None

        skus_request = MagicMock()
        services.skus.return_value.list.return_value = skus_request
        skus_request.execute.side_effect = [
          ssl.SSLError("[SSL: RECORD_LAYER_FAILURE] record layer failure"),
          {"skus": [
            make_sku("N1 Predefined Instance Core running in Americas", "us-central1", tiered_rate_usd=0.03),
            make_sku("N1 Predefined Instance Ram running in Americas", "us-central1", tiered_rate_usd=0.004),
          ]},
        ]
        services.skus.return_value.list_next.return_value = None

        with patch("canine.cost.PRICE_CACHE_PATH", str(cache_path)), \
             patch("canine.cost.get_billing_client", return_value=fake_client), \
             patch("canine.cost._invalidate_billing_client") as mock_invalidate, \
             patch("canine.cost.time.sleep"), \
             patch("canine.cost._COMPUTE_ENGINE_SERVICE_NAME", None):
            result = cost.get_price("n1-highcpu-8", "us-central1-a", False, node_types=self.node_types)

        expected = 0.03 * 8 + 0.004 * (7168.0 / 1024)
        assert result == pytest.approx(expected)
        mock_invalidate.assert_called_once()
        assert skus_request.execute.call_count == 2
        # the successful retry's price is still cached normally
        assert json.loads(cache_path.read_text())[cost._price_cache_key("n1-highcpu-8", "us-central1-a", False, None, 0)] == pytest.approx(expected)

    def test_gives_up_after_max_attempts_on_persistent_transient_error(self, tmp_path):
        import ssl
        cache_path = tmp_path / "price_cache.json"

        fake_client = MagicMock()
        services = fake_client.services.return_value
        list_request = MagicMock()
        services.list.return_value = list_request
        list_request.execute.side_effect = ssl.SSLError("[SSL: RECORD_LAYER_FAILURE] record layer failure")
        services.list_next.return_value = None

        with patch("canine.cost.PRICE_CACHE_PATH", str(cache_path)), \
             patch("canine.cost.get_billing_client", return_value=fake_client), \
             patch("canine.cost._invalidate_billing_client") as mock_invalidate, \
             patch("canine.cost.time.sleep"), \
             patch("canine.cost._COMPUTE_ENGINE_SERVICE_NAME", None):
            result = cost.get_price("n1-highcpu-8", "us-central1-a", False, node_types=self.node_types)

        assert result is None
        assert mock_invalidate.call_count == cost._MAX_FETCH_ATTEMPTS
        assert list_request.execute.call_count == cost._MAX_FETCH_ATTEMPTS
        assert not cache_path.exists()  # a total failure is never cached

    def test_nan_accelerator_fields_do_not_crash(self, tmp_path):
        # host_LuT.pickle stores NaN (not None/0) for accelerator_type/
        # accelerator_count on every non-GPU node -- confirmed live: this
        # crashed int(accelerator_count) with "cannot convert float NaN to
        # integer" for nearly every real job, since NaN is truthy in Python and
        # a plain `x or default` fallback never actually catches it.
        cache_path = tmp_path / "price_cache.json"
        fake_client = MagicMock()
        services = fake_client.services.return_value
        list_request = MagicMock()
        services.list.return_value = list_request
        list_request.execute.return_value = {"services": [{"displayName": "Compute Engine", "name": "services/x"}]}
        services.list_next.return_value = None
        skus_request = MagicMock()
        services.skus.return_value.list.return_value = skus_request
        skus_request.execute.return_value = {"skus": [
          make_sku("N1 Predefined Instance Core running in Americas", "us-central1", tiered_rate_usd=0.03),
          make_sku("N1 Predefined Instance Ram running in Americas", "us-central1", tiered_rate_usd=0.004),
        ]}
        services.skus.return_value.list_next.return_value = None

        with patch("canine.cost.PRICE_CACHE_PATH", str(cache_path)), \
             patch("canine.cost.get_billing_client", return_value=fake_client), \
             patch("canine.cost._COMPUTE_ENGINE_SERVICE_NAME", None):
            result = cost.get_price(
              "n1-highcpu-8", "us-central1-a", False,
              accelerator_type=float("nan"), accelerator_count=float("nan"),
              node_types=self.node_types,
            )

        assert result is not None
        assert result > 0


class TestMakeLivePriceSource:
    def test_real_host_lut_row_shape_with_nan_accelerator_fields(self, tmp_path):
        # mirrors what host_LuT.pickle actually contains for a non-GPU node
        # (accelerator_type/accelerator_count as NaN, via provision_server.py's
        # regex .str.extract()) -- this is the exact shape that crashed in
        # production. Only the network boundary (get_billing_client) is mocked,
        # so this exercises the real get_price()/_price_cache_key() logic the
        # bug was actually in, through the full _source() chain.
        host_lut = pd.DataFrame(
          {"machine_type": ["n1-standard-8"], "preemptible": [False],
           "accelerator_type": [float("nan")], "accelerator_count": [float("nan")]},
          index=pd.Index(["wolf-test-worker1"]),
        )
        node_types = pd.DataFrame({"cpus": [8], "realmemory": [28200.0]}, index=pd.Index(["n1-standard-8"]))

        cache_path = tmp_path / "price_cache.json"
        fake_client = MagicMock()
        services = fake_client.services.return_value
        list_request = MagicMock()
        services.list.return_value = list_request
        list_request.execute.return_value = {"services": [{"displayName": "Compute Engine", "name": "services/x"}]}
        services.list_next.return_value = None
        skus_request = MagicMock()
        services.skus.return_value.list.return_value = skus_request
        skus_request.execute.return_value = {"skus": [
          make_sku("N1 Predefined Instance Core running in Americas", "us-east1", tiered_rate_usd=0.03),
          make_sku("N1 Predefined Instance Ram running in Americas", "us-east1", tiered_rate_usd=0.004),
        ]}
        services.skus.return_value.list_next.return_value = None

        with patch("canine.cost.PRICE_CACHE_PATH", str(cache_path)), \
             patch("canine.cost.get_billing_client", return_value=fake_client), \
             patch("canine.cost._COMPUTE_ENGINE_SERVICE_NAME", None):
            price_source = cost.make_live_price_source("us-east1-b", host_lut=host_lut, node_types=node_types)
            result = price_source("wolf-test-worker1")  # must not raise

        assert result is not None
        assert result > 0

    def test_unknown_node_returns_none(self):
        host_lut = pd.DataFrame({"machine_type": []}, index=pd.Index([]))
        price_source = cost.make_live_price_source("us-east1-b", host_lut=host_lut, node_types=pd.DataFrame())
        assert price_source("unknown-node") is None


# ---------------------------------------------------------------------------
# estimate_task_cost
# ---------------------------------------------------------------------------

def make_acct_df(rows):
    """rows: {jid: [attempt_dict, ...]}"""
    return pd.DataFrame({"attempts": list(rows.values())}, index=list(rows.keys()))


class TestEstimateTaskCost:
    def setup_method(self):
        self.host_lut = pd.DataFrame(
          {"machine_type": ["n1-highcpu-8", "n1-highcpu-8"], "preemptible": [True, True]},
          index = pd.Index(["worker1", "worker2"]),
        )
        self.node_types = pd.DataFrame({"cpus": [8], "realmemory": [8192.0]}, index=pd.Index(["n1-highcpu-8"]))

    def test_single_attempt_single_node(self):
        acct = make_acct_df({
          "1_1": [{"NodeList": "worker1", "Start": "2026-01-01T00:00:00", "End": "2026-01-01T01:00:00", "AllocTRES": "cpu=4,mem=4096M", "CPUTimeRAW": 14400}],
        })
        price_source = lambda node: 3600.0  # $3600/hr -> $1/sec, for round numbers
        result = cost.estimate_task_cost(acct, price_source, host_lut=self.host_lut, node_types=self.node_types)
        # 1 hour = 3600s, cpu_frac=4/8=0.5, mem_frac=4096/8192=0.5 -> max=0.5
        # cost = (3600/3600) * 3600 * 0.5 = 1800
        assert result.loc["1_1", "cost_usd"] == pytest.approx(1800.0)
        assert not result.loc["1_1", "missing_capacity_data"]
        assert not result.loc["1_1", "is_provisional"]

    def test_alloc_tres_without_cpu_mem_falls_back_to_ncpus_and_reqmem(self):
        # Confirmed live: AllocTRES can come back as just "billing=1+" on some
        # clusters, with no cpu=/mem= keys at all -- NCPUS/ReqMem are the real
        # fallback source, not a hypothetical.
        acct = make_acct_df({
          "1_1": [{
            "NodeList": "worker1", "Start": "2026-01-01T00:00:00", "End": "2026-01-01T01:00:00",
            "AllocTRES": "billing=1+", "CPUTimeRAW": 14400, "NCPUS": 4, "ReqMem": "4096M",
          }],
        })
        price_source = lambda node: 3600.0
        result = cost.estimate_task_cost(acct, price_source, host_lut=self.host_lut, node_types=self.node_types)
        assert result.loc["1_1", "cost_usd"] == pytest.approx(1800.0)
        assert not result.loc["1_1", "missing_capacity_data"]

    def test_multi_attempt_preemption_sums_across_nodes(self):
        acct = make_acct_df({
          "1_1": [
            {"NodeList": "worker1", "Start": "2026-01-01T00:00:00", "End": "2026-01-01T00:30:00", "AllocTRES": "cpu=4,mem=4096M", "CPUTimeRAW": 7200},
            {"NodeList": "worker2", "Start": "2026-01-01T00:35:00", "End": "2026-01-01T01:35:00", "AllocTRES": "cpu=4,mem=4096M", "CPUTimeRAW": 14400},
          ],
        })
        price_source = lambda node: 3600.0
        result = cost.estimate_task_cost(acct, price_source, host_lut=self.host_lut, node_types=self.node_types)
        # attempt 1: 1800s * 0.5 = 900; attempt 2: 3600s * 0.5 = 1800; total = 2700
        assert result.loc["1_1", "cost_usd"] == pytest.approx(2700.0)

    def test_no_attempts_flags_missing_capacity_data(self):
        acct = make_acct_df({"1_1": []})
        result = cost.estimate_task_cost(acct, lambda node: 1.0, host_lut=self.host_lut, node_types=self.node_types)
        assert result.loc["1_1", "cost_usd"] == 0.0
        assert result.loc["1_1", "missing_capacity_data"]

    def test_unrecognized_node_flags_missing_capacity_data(self):
        acct = make_acct_df({
          "1_1": [{"NodeList": "worker-unknown", "Start": "2026-01-01T00:00:00", "End": "2026-01-01T01:00:00", "AllocTRES": "cpu=4,mem=4096M", "CPUTimeRAW": 14400}],
        })
        result = cost.estimate_task_cost(acct, lambda node: 1.0, host_lut=self.host_lut, node_types=self.node_types)
        assert result.loc["1_1", "cost_usd"] == 0.0
        assert result.loc["1_1", "missing_capacity_data"]

    def test_missing_price_flags_provisional(self):
        acct = make_acct_df({
          "1_1": [{"NodeList": "worker1", "Start": "2026-01-01T00:00:00", "End": "2026-01-01T01:00:00", "AllocTRES": "cpu=4,mem=4096M", "CPUTimeRAW": 14400}],
        })
        result = cost.estimate_task_cost(acct, lambda node: None, host_lut=self.host_lut, node_types=self.node_types)
        assert result.loc["1_1", "cost_usd"] == 0.0
        assert result.loc["1_1", "is_provisional"]
        assert not result.loc["1_1", "missing_capacity_data"]

    def test_exclusive_node_job_gets_full_attribution_no_special_case(self):
        # LocalizeToDisk-style job requesting the whole node -- formula alone
        # should give 100% attribution, no special-casing needed.
        acct = make_acct_df({
          "1_1": [{"NodeList": "worker1", "Start": "2026-01-01T00:00:00", "End": "2026-01-01T01:00:00", "AllocTRES": "cpu=8,mem=8192M", "CPUTimeRAW": 28800}],
        })
        result = cost.estimate_task_cost(acct, lambda node: 3600.0, host_lut=self.host_lut, node_types=self.node_types)
        # cpu_frac=1.0, mem_frac=1.0 -> cost = 1 * 3600 * 1.0 = 3600 (full node-hour)
        assert result.loc["1_1", "cost_usd"] == pytest.approx(3600.0)

    def test_total_running_seconds_sums_across_attempts(self):
        # Distinct from the acct row's own "Elapsed" (only the single attempt
        # picked as "final" by grouper()) -- this is the sum of every
        # attempt's own duration, which is what should be compared against
        # n_preempted/cost_usd to avoid the "Elapsed looks tiny next to a
        # heavily-preempted job's cost" confusion confirmed live.
        acct = make_acct_df({
          "1_1": [
            {"NodeList": "worker1", "Start": "2026-01-01T00:00:00", "End": "2026-01-01T00:00:10", "AllocTRES": "cpu=4,mem=4096M", "CPUTimeRAW": 40},
            {"NodeList": "worker1", "Start": "2026-01-01T00:05:00", "End": "2026-01-01T00:05:24", "AllocTRES": "cpu=4,mem=4096M", "CPUTimeRAW": 96},
          ],
        })
        result = cost.estimate_task_cost(acct, lambda node: 3600.0, host_lut=self.host_lut, node_types=self.node_types)
        assert result.loc["1_1", "total_running_seconds"] == pytest.approx(34.0)  # 10 + 24

    def test_total_running_seconds_counted_even_when_capacity_or_price_missing(self):
        # A job's real runtime is a fact about what sacct observed -- it
        # shouldn't disappear just because canine.cost doesn't happen to know
        # that node's machine type or a live price for it (those instead flag
        # missing_capacity_data/is_provisional, tracked separately).
        acct = make_acct_df({
          "1_1": [
            {"NodeList": "worker-unknown", "Start": "2026-01-01T00:00:00", "End": "2026-01-01T00:00:10", "AllocTRES": "cpu=4,mem=4096M", "CPUTimeRAW": 40},
          ],
        })
        result = cost.estimate_task_cost(acct, lambda node: None, host_lut=self.host_lut, node_types=self.node_types)
        assert result.loc["1_1", "total_running_seconds"] == pytest.approx(10.0)
        assert result.loc["1_1", "cost_usd"] == 0.0
        assert result.loc["1_1", "missing_capacity_data"]

    def test_total_running_seconds_excludes_unparseable_timestamps(self):
        acct = make_acct_df({
          "1_1": [{"NodeList": "worker1", "Start": "Unknown", "End": "Unknown", "AllocTRES": "cpu=4,mem=4096M", "CPUTimeRAW": 0}],
        })
        result = cost.estimate_task_cost(acct, lambda node: 3600.0, host_lut=self.host_lut, node_types=self.node_types)
        assert result.loc["1_1", "total_running_seconds"] == 0.0


# ---------------------------------------------------------------------------
# estimate_node_undersubscription
# ---------------------------------------------------------------------------

def make_node_snapshot(rows):
    return pd.DataFrame(rows)


class TestEstimateNodeUndersubscription:
    def setup_method(self):
        self.host_lut = pd.DataFrame({"machine_type": ["faketype"]}, index=pd.Index(["workerX"]))
        self.node_types = pd.DataFrame({"cpus": [4], "realmemory": [8192.0]}, index=pd.Index(["faketype"]))
        self.price_source = lambda node: 3600.0  # $1/sec, for round numbers

    def test_alloc_tres_without_cpu_mem_falls_back_to_ncpus_and_reqmem(self):
        snapshot = make_node_snapshot([
          {
            "NodeList": "workerX", "Start": "2026-01-01T00:00:00", "End": "2026-01-01T00:01:40",
            "AllocTRES": "billing=1+", "NCPUS": 4, "ReqMem": "8192M",
          },
        ])
        result = cost.estimate_node_undersubscription(snapshot, self.price_source, host_lut=self.host_lut, node_types=self.node_types)
        row = result.iloc[0]
        # full-node allocation via the NCPUS/ReqMem fallback -> zero waste
        assert row["wasted_cost_usd"] == pytest.approx(0.0)

    def test_empty_snapshot_returns_empty_frame(self):
        result = cost.estimate_node_undersubscription(pd.DataFrame(), self.price_source, host_lut=self.host_lut, node_types=self.node_types)
        assert result.empty

    def test_sequential_jobs_hand_calculated(self):
        # job A: 100s @ 50% utilization (cpu-bound: 2/4 cpus, 2048/8192 mem)
        # job B: 100s @ 100% utilization (full node)
        snapshot = make_node_snapshot([
          {"NodeList": "workerX", "Start": "2026-01-01T00:00:00", "End": "2026-01-01T00:01:40", "AllocTRES": "cpu=2,mem=2048M"},
          {"NodeList": "workerX", "Start": "2026-01-01T00:01:40", "End": "2026-01-01T00:03:20", "AllocTRES": "cpu=4,mem=8192M"},
        ])
        result = cost.estimate_node_undersubscription(snapshot, self.price_source, host_lut=self.host_lut, node_types=self.node_types)
        row = result.iloc[0]
        # node_window = 200s, node_cost = 1/sec * 200 = 200
        assert row["node_cost_usd"] == pytest.approx(200.0)
        # wasted = 100s * (1-0.5) + 100s * (1-1.0) = 50 + 0 = 50
        assert row["wasted_cost_usd"] == pytest.approx(50.0)

    def test_overlapping_jobs_hand_calculated(self):
        # job C: [0,100) cpu=1,mem=1024M (frac 0.25/0.125 -> util 0.25)
        # job D: [50,150) cpu=1,mem=1024M (same fracs)
        # [0,50): util=0.25 waste=0.75 * 50 = 37.5
        # [50,100): both active, cpu_sum=2/4=0.5, mem_sum=2048/8192=0.25 -> util=0.5, waste=0.5*50=25
        # [100,150): util=0.25, waste=0.75*50=37.5
        # total wasted = 100; node_window=150
        snapshot = make_node_snapshot([
          {"NodeList": "workerX", "Start": "2026-01-01T00:00:00", "End": "2026-01-01T00:01:40", "AllocTRES": "cpu=1,mem=1024M"},
          {"NodeList": "workerX", "Start": "2026-01-01T00:00:50", "End": "2026-01-01T00:02:30", "AllocTRES": "cpu=1,mem=1024M"},
        ])
        result = cost.estimate_node_undersubscription(snapshot, self.price_source, host_lut=self.host_lut, node_types=self.node_types)
        row = result.iloc[0]
        assert row["node_cost_usd"] == pytest.approx(150.0)
        assert row["wasted_cost_usd"] == pytest.approx(100.0)

    def test_still_running_job_clipped_to_observed_window(self):
        # job with no End yet ("Unknown") should be clipped to the node's overall
        # observed window rather than crash or extend indefinitely.
        snapshot = make_node_snapshot([
          {"NodeList": "workerX", "Start": "2026-01-01T00:00:00", "End": "2026-01-01T00:01:40", "AllocTRES": "cpu=4,mem=8192M"},
          {"NodeList": "workerX", "Start": "2026-01-01T00:00:30", "End": "Unknown", "AllocTRES": "cpu=1,mem=1024M"},
        ])
        result = cost.estimate_node_undersubscription(snapshot, self.price_source, host_lut=self.host_lut, node_types=self.node_types)
        row = result.iloc[0]
        assert row["observed_end"] == pd.Timestamp("2026-01-01T00:01:40")
        # first job alone occupies full node for the whole window -> zero waste
        assert row["wasted_cost_usd"] == pytest.approx(0.0)

    def test_unrecognized_node_flags_missing_capacity_data(self):
        snapshot = make_node_snapshot([
          {"NodeList": "worker-unknown", "Start": "2026-01-01T00:00:00", "End": "2026-01-01T00:01:00", "AllocTRES": "cpu=1,mem=1024M"},
        ])
        result = cost.estimate_node_undersubscription(snapshot, self.price_source, host_lut=self.host_lut, node_types=self.node_types)
        row = result.iloc[0]
        assert row["missing_capacity_data"]
        assert row["node_cost_usd"] is None

    def test_missing_price_flags_provisional(self):
        snapshot = make_node_snapshot([
          {"NodeList": "workerX", "Start": "2026-01-01T00:00:00", "End": "2026-01-01T00:01:00", "AllocTRES": "cpu=1,mem=1024M"},
        ])
        result = cost.estimate_node_undersubscription(snapshot, lambda node: None, host_lut=self.host_lut, node_types=self.node_types)
        row = result.iloc[0]
        assert row["is_provisional"]

    def test_two_accounts_sharing_a_node_both_counted(self):
        # a node shared by jobs from two different Accounts -- undersubscription
        # calc must see both, not just wolF's own.
        snapshot = make_node_snapshot([
          {"NodeList": "workerX", "Start": "2026-01-01T00:00:00", "End": "2026-01-01T00:01:40", "AllocTRES": "cpu=2,mem=2048M", "Account": "wolf-run-abcd"},
          {"NodeList": "workerX", "Start": "2026-01-01T00:00:00", "End": "2026-01-01T00:01:40", "AllocTRES": "cpu=2,mem=2048M", "Account": "other-team"},
        ])
        result = cost.estimate_node_undersubscription(snapshot, self.price_source, host_lut=self.host_lut, node_types=self.node_types)
        row = result.iloc[0]
        # combined: cpu_sum=4/4=1.0, mem_sum=4096/8192=0.5 -> util=1.0 -> zero waste
        assert row["wasted_cost_usd"] == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# group_sacct_by_job
# ---------------------------------------------------------------------------

class TestGroupSacctByJob:
    def test_empty_input_returns_empty(self):
        result = cost.group_sacct_by_job(pd.DataFrame())
        assert result.empty

    def test_single_attempt_job(self):
        raw = pd.DataFrame({
          "State": ["COMPLETED"], "CPUTimeRAW": [100], "Submit": ["2026-01-01 00:00:00"],
          "NodeList": ["worker1"], "Start": ["2026-01-01T00:00:05"], "End": ["2026-01-01T00:01:00"],
          "AllocTRES": ["cpu=1,mem=1G"],
        }, index=["1_1"])
        result = cost.group_sacct_by_job(raw)
        row = result.loc["1_1"]
        assert row["n_preempted"] == 0
        assert row["CPUTimeRAW"] == 100
        assert row["attempts"] == [{"NodeList": "worker1", "Start": "2026-01-01T00:00:05", "End": "2026-01-01T00:01:00", "AllocTRES": "cpu=1,mem=1G", "CPUTimeRAW": 100}]

    def test_multi_attempt_job_collapses_and_sums(self):
        raw = pd.DataFrame({
          "State": ["PREEMPTED", "COMPLETED"], "CPUTimeRAW": [100, 200],
          "Submit": ["2026-01-01 00:00:00", "2026-01-01 00:05:00"],
          "NodeList": ["worker1", "worker2"],
          "Start": ["2026-01-01T00:00:05", "2026-01-01T00:05:05"],
          "End": ["2026-01-01T00:02:00", "2026-01-01T00:08:00"],
          "AllocTRES": ["cpu=2,mem=2G", "cpu=2,mem=2G"],
        }, index=["1_1", "1_1"])
        result = cost.group_sacct_by_job(raw)
        row = result.loc["1_1"]
        assert row["State"] == "COMPLETED"       # last attempt
        assert row["NodeList"] == "worker2"      # last attempt
        assert row["CPUTimeRAW"] == 300           # summed
        assert row["n_preempted"] == 1
        assert len(row["attempts"]) == 2
        assert row["attempts"][0]["NodeList"] == "worker1"
        assert row["attempts"][1]["NodeList"] == "worker2"

    def test_multiple_distinct_jobs(self):
        raw = pd.DataFrame({
          "State": ["COMPLETED", "COMPLETED"], "CPUTimeRAW": [50, 75],
          "Submit": ["2026-01-01 00:00:00", "2026-01-01 00:00:00"],
          "NodeList": ["worker1", "worker2"],
          "Start": ["2026-01-01T00:00:05", "2026-01-01T00:00:05"],
          "End": ["2026-01-01T00:01:00", "2026-01-01T00:01:00"],
          "AllocTRES": ["cpu=1,mem=1G", "cpu=1,mem=1G"],
        }, index=["1_1", "1_2"])
        result = cost.group_sacct_by_job(raw)
        assert set(result.index) == {"1_1", "1_2"}
        assert result.loc["1_1", "CPUTimeRAW"] == 50
        assert result.loc["1_2", "CPUTimeRAW"] == 75

    def test_constant_submit_across_attempts_still_orders_by_start(self):
        # Real SLURM behavior: Submit doesn't change across requeues of the
        # same JobID, so sorting on it doesn't reliably produce chronological
        # order (pandas' sort isn't stable for ties). The chronologically
        # last attempt is deliberately placed FIRST in raw row order here, so
        # a naive "whatever's physically last" assumption can't accidentally
        # pass.
        raw = pd.DataFrame({
          "State": ["COMPLETED", "PREEMPTED"], "CPUTimeRAW": [50, 20],
          "Submit": ["2026-01-01 00:00:00", "2026-01-01 00:00:00"],
          "NodeList": ["worker-final", "worker-early"],
          "Start": ["2026-01-01T00:10:00", "2026-01-01T00:00:05"],
          "End": ["2026-01-01T00:10:30", "2026-01-01T00:00:15"],
          "AllocTRES": ["cpu=1,mem=1G", "cpu=1,mem=1G"],
        }, index=["1_1", "1_1"])
        result = cost.group_sacct_by_job(raw)
        row = result.loc["1_1"]
        assert row["State"] == "COMPLETED"
        assert row["NodeList"] == "worker-final"  # chronologically last, not whichever row happened to be last in raw order
        assert row["n_preempted"] == 1


# ---------------------------------------------------------------------------
# _normalize_disk_type / match_disk_price / get_disk_price_per_gb_month(_hour)
# ---------------------------------------------------------------------------

class TestNormalizeDiskType:
    @pytest.mark.parametrize("raw,expected", [
      ("standard", "pd-standard"), ("pd-standard", "pd-standard"),
      ("ssd", "pd-ssd"), ("pd-ssd", "pd-ssd"),
      ("balanced", "pd-balanced"), ("pd-balanced", "pd-balanced"),
      ("PD-SSD", "pd-ssd"),  # case-insensitive
    ])
    def test_known_aliases(self, raw, expected):
        assert cost._normalize_disk_type(raw) == expected

    def test_unrecognized_type_returns_none(self):
        assert cost._normalize_disk_type("hyperdisk-extreme") is None

    def test_none_or_empty_returns_none(self):
        assert cost._normalize_disk_type(None) is None
        assert cost._normalize_disk_type("") is None


def make_disk_sku(description, region, family="Storage", tiered_rate_usd=0.04):
    return make_sku(description, region, family=family, tiered_rate_usd=tiered_rate_usd)


class TestMatchDiskPrice:
    def test_matches_standard_pd(self):
        skus = [
          make_disk_sku("Storage PD Capacity", "us-central1", tiered_rate_usd=0.04),
          make_disk_sku("SSD backed PD Capacity", "us-central1", tiered_rate_usd=0.17),
        ]
        assert cost.match_disk_price(skus, "pd-standard", "us-central1") == pytest.approx(0.04)

    def test_matches_ssd_pd(self):
        skus = [make_disk_sku("SSD backed PD Capacity", "us-central1", tiered_rate_usd=0.17)]
        assert cost.match_disk_price(skus, "pd-ssd", "us-central1") == pytest.approx(0.17)

    def test_excludes_regional_variant(self):
        skus = [make_disk_sku("Regional Storage PD Capacity", "us-central1", tiered_rate_usd=0.08)]
        assert cost.match_disk_price(skus, "pd-standard", "us-central1") is None

    def test_no_match_in_wrong_region(self):
        skus = [make_disk_sku("Storage PD Capacity", "europe-west1", tiered_rate_usd=0.04)]
        assert cost.match_disk_price(skus, "pd-standard", "us-central1") is None

    def test_wrong_resource_family_excluded(self):
        skus = [make_disk_sku("Storage PD Capacity", "us-central1", family="Compute", tiered_rate_usd=0.04)]
        assert cost.match_disk_price(skus, "pd-standard", "us-central1") is None

    def test_unrecognized_disk_type_returns_none(self):
        assert cost.match_disk_price([], "hyperdisk-extreme", "us-central1") is None


class TestGetDiskPricePerGbMonth:
    def test_unrecognized_disk_type_returns_none_without_api_call(self):
        with patch("canine.cost.get_billing_client") as mock_client:
            result = cost.get_disk_price_per_gb_month("hyperdisk-extreme", "us-central1-a")
        assert result is None
        mock_client.assert_not_called()

    def test_cache_hit_skips_api_call(self, tmp_path):
        cache_path = tmp_path / "price_cache.json"
        key = cost._disk_price_cache_key("pd-standard", "us-central1-a")
        cache_path.write_text(json.dumps({key: 0.04}))
        with patch("canine.cost.PRICE_CACHE_PATH", str(cache_path)), \
             patch("canine.cost.get_billing_client") as mock_client:
            result = cost.get_disk_price_per_gb_month("pd-standard", "us-central1-a")
        assert result == 0.04
        mock_client.assert_not_called()

    def test_cache_miss_calls_api_and_persists(self, tmp_path):
        cache_path = tmp_path / "price_cache.json"
        fake_client = MagicMock()
        services = fake_client.services.return_value
        list_request = MagicMock()
        services.list.return_value = list_request
        list_request.execute.return_value = {"services": [{"displayName": "Compute Engine", "name": "services/x"}]}
        services.list_next.return_value = None
        skus_request = MagicMock()
        services.skus.return_value.list.return_value = skus_request
        skus_request.execute.return_value = {"skus": [make_disk_sku("Storage PD Capacity", "us-central1", tiered_rate_usd=0.04)]}
        services.skus.return_value.list_next.return_value = None

        with patch("canine.cost.PRICE_CACHE_PATH", str(cache_path)), \
             patch("canine.cost.get_billing_client", return_value=fake_client), \
             patch("canine.cost._COMPUTE_ENGINE_SERVICE_NAME", None):
            result = cost.get_disk_price_per_gb_month("pd-standard", "us-central1-a")

        assert result == pytest.approx(0.04)
        cached = json.loads(cache_path.read_text())
        assert cached[cost._disk_price_cache_key("pd-standard", "us-central1-a")] == pytest.approx(0.04)

    def test_disk_and_compute_cache_keys_never_collide(self, tmp_path):
        # "disk|" prefix keeps get_disk_price_per_gb_month() distinct from
        # get_price() in the same on-disk cache file: a real "pd-standard"
        # compute-price cache entry must not be mistaken for a disk-price
        # cache hit, forcing a real (here, mocked) API round trip instead.
        cache_path = tmp_path / "price_cache.json"
        cache_path.write_text(json.dumps({
          cost._price_cache_key("pd-standard", "us-central1-a", False, None, 0): 99.0,
        }))

        fake_client = MagicMock()
        services = fake_client.services.return_value
        list_request = MagicMock()
        services.list.return_value = list_request
        list_request.execute.return_value = {"services": [{"displayName": "Compute Engine", "name": "services/x"}]}
        services.list_next.return_value = None
        skus_request = MagicMock()
        services.skus.return_value.list.return_value = skus_request
        skus_request.execute.return_value = {"skus": []}  # no match -- not the point of this test
        services.skus.return_value.list_next.return_value = None

        mock_get_client = MagicMock(return_value=fake_client)
        with patch("canine.cost.PRICE_CACHE_PATH", str(cache_path)), \
             patch("canine.cost.get_billing_client", mock_get_client), \
             patch("canine.cost._COMPUTE_ENGINE_SERVICE_NAME", None):
            result = cost.get_disk_price_per_gb_month("pd-standard", "us-central1-a")
        assert result is None
        mock_get_client.assert_called_once()

    def test_no_match_returns_none(self, tmp_path):
        cache_path = tmp_path / "price_cache.json"
        fake_client = MagicMock()
        services = fake_client.services.return_value
        list_request = MagicMock()
        services.list.return_value = list_request
        list_request.execute.return_value = {"services": [{"displayName": "Compute Engine", "name": "services/x"}]}
        services.list_next.return_value = None
        skus_request = MagicMock()
        services.skus.return_value.list.return_value = skus_request
        skus_request.execute.return_value = {"skus": []}
        services.skus.return_value.list_next.return_value = None

        with patch("canine.cost.PRICE_CACHE_PATH", str(cache_path)), \
             patch("canine.cost.get_billing_client", return_value=fake_client), \
             patch("canine.cost._COMPUTE_ENGINE_SERVICE_NAME", None):
            result = cost.get_disk_price_per_gb_month("pd-standard", "us-central1-a")
        assert result is None
        assert not cache_path.exists()


class TestGetDiskPricePerGbHour:
    def test_divides_by_avg_hours_per_month(self, tmp_path):
        cache_path = tmp_path / "price_cache.json"
        key = cost._disk_price_cache_key("pd-standard", "us-central1-a")
        cache_path.write_text(json.dumps({key: 73.0}))
        with patch("canine.cost.PRICE_CACHE_PATH", str(cache_path)):
            result = cost.get_disk_price_per_gb_hour("pd-standard", "us-central1-a")
        assert result == pytest.approx(73.0 / cost.AVG_HOURS_PER_MONTH)

    def test_none_when_underlying_price_unavailable(self):
        with patch("canine.cost.get_disk_price_per_gb_month", return_value=None):
            assert cost.get_disk_price_per_gb_hour("pd-standard", "us-central1-a") is None


# ---------------------------------------------------------------------------
# get_live_disk_info / make_live_boot_disk_price_source / make_live_disk_gb_hour_price_source
# ---------------------------------------------------------------------------

class TestGetLiveDiskInfo:
    def setup_method(self):
        cost._DISK_INFO_CACHE.clear()

    def test_successful_lookup(self):
        fake_client = MagicMock()
        fake_client.disks.return_value.get.return_value.execute.return_value = {
          "sizeGb": "50", "type": "https://www.googleapis.com/compute/v1/projects/p/zones/z/diskTypes/pd-ssd",
        }
        with patch("canine.cost.get_gce_disk_client", return_value=fake_client):
            size_gb, disk_type = cost.get_live_disk_info("worker1", "us-central1-a", "my-project")
        assert size_gb == pytest.approx(50.0)
        assert disk_type == "pd-ssd"

    def test_api_failure_returns_none_none(self):
        fake_client = MagicMock()
        fake_client.disks.return_value.get.return_value.execute.side_effect = RuntimeError("disk not found")
        with patch("canine.cost.get_gce_disk_client", return_value=fake_client):
            result = cost.get_live_disk_info("worker-gone", "us-central1-a", "my-project")
        assert result == (None, None)

    def test_cache_hit_skips_api_call(self):
        fake_client = MagicMock()
        fake_client.disks.return_value.get.return_value.execute.return_value = {
          "sizeGb": "25", "type": ".../diskTypes/pd-standard",
        }
        with patch("canine.cost.get_gce_disk_client", return_value=fake_client):
            first = cost.get_live_disk_info("worker1", "us-central1-a", "my-project", ttl_seconds=300)
            fake_client.disks.return_value.get.reset_mock()
            second = cost.get_live_disk_info("worker1", "us-central1-a", "my-project", ttl_seconds=300)
        assert first == second == (25.0, "pd-standard")
        fake_client.disks.return_value.get.assert_not_called()

    def test_cache_expires_after_ttl(self):
        fake_client = MagicMock()
        fake_client.disks.return_value.get.return_value.execute.return_value = {
          "sizeGb": "25", "type": ".../diskTypes/pd-standard",
        }
        with patch("canine.cost.get_gce_disk_client", return_value=fake_client):
            cost.get_live_disk_info("worker1", "us-central1-a", "my-project", ttl_seconds=0)
            cost.get_live_disk_info("worker1", "us-central1-a", "my-project", ttl_seconds=0)
        assert fake_client.disks.return_value.get.call_count == 2


class TestMakeLiveBootDiskPriceSource:
    def test_composes_size_and_gb_hour_price(self):
        with patch("canine.cost.get_live_disk_info", return_value=(50.0, "pd-ssd")), \
             patch("canine.cost.get_disk_price_per_gb_hour", return_value=0.0002):
            price_source = cost.make_live_boot_disk_price_source("us-central1-a", "my-project")
            result = price_source("worker1")
        assert result == pytest.approx(50.0 * 0.0002)

    def test_missing_disk_info_returns_none(self):
        with patch("canine.cost.get_live_disk_info", return_value=(None, None)):
            price_source = cost.make_live_boot_disk_price_source("us-central1-a", "my-project")
            assert price_source("worker-gone") is None

    def test_missing_disk_price_returns_none(self):
        with patch("canine.cost.get_live_disk_info", return_value=(50.0, "pd-ssd")), \
             patch("canine.cost.get_disk_price_per_gb_hour", return_value=None):
            price_source = cost.make_live_boot_disk_price_source("us-central1-a", "my-project")
            assert price_source("worker1") is None


class TestMakeLiveDiskGbHourPriceSource:
    def test_delegates_to_get_disk_price_per_gb_hour(self):
        with patch("canine.cost.get_disk_price_per_gb_hour", return_value=0.0001) as mock_get:
            price_source = cost.make_live_disk_gb_hour_price_source("us-central1-a")
            result = price_source("pd-standard")
        assert result == pytest.approx(0.0001)
        mock_get.assert_called_once_with("pd-standard", "us-central1-a")


# ---------------------------------------------------------------------------
# estimate_task_cost -- disk_price_source
# ---------------------------------------------------------------------------

class TestEstimateTaskCostDiskPriceSource:
    def setup_method(self):
        self.host_lut = pd.DataFrame(
          {"machine_type": ["n1-highcpu-8", "n1-highcpu-8"], "preemptible": [True, True]},
          index = pd.Index(["worker1", "worker2"]),
        )
        self.node_types = pd.DataFrame({"cpus": [8], "realmemory": [8192.0]}, index=pd.Index(["n1-highcpu-8"]))

    def test_none_disk_price_source_reproduces_prior_behavior_exactly(self):
        # Regression safety: omitting disk_price_source must not change
        # cost_usd/is_provisional/missing_capacity_data at all relative to
        # before this feature existed.
        acct = make_acct_df({
          "1_1": [{"NodeList": "worker1", "Start": "2026-01-01T00:00:00", "End": "2026-01-01T01:00:00", "AllocTRES": "cpu=4,mem=4096M", "CPUTimeRAW": 14400}],
        })
        result = cost.estimate_task_cost(acct, lambda node: 3600.0, host_lut=self.host_lut, node_types=self.node_types)
        assert result.loc["1_1", "cost_usd"] == pytest.approx(1800.0)
        assert result.loc["1_1", "disk_cost_usd"] == 0.0
        assert result.loc["1_1", "disk_cost_provisional"]  # never priced -- flagged, not silently zero

    def test_disk_cost_prorated_same_as_compute(self):
        acct = make_acct_df({
          "1_1": [{"NodeList": "worker1", "Start": "2026-01-01T00:00:00", "End": "2026-01-01T01:00:00", "AllocTRES": "cpu=4,mem=4096M", "CPUTimeRAW": 14400}],
        })
        result = cost.estimate_task_cost(
          acct, lambda node: 3600.0, disk_price_source=lambda node: 36.0,
          host_lut=self.host_lut, node_types=self.node_types,
        )
        # same max(cpu_frac, mem_frac)=0.5 proration as compute: (36/3600)*3600*0.5 = 18
        assert result.loc["1_1", "cost_usd"] == pytest.approx(1800.0)
        assert result.loc["1_1", "disk_cost_usd"] == pytest.approx(18.0)
        assert not result.loc["1_1", "disk_cost_provisional"]

    def test_disk_pricing_failure_does_not_affect_compute_cost_or_flags(self):
        acct = make_acct_df({
          "1_1": [{"NodeList": "worker1", "Start": "2026-01-01T00:00:00", "End": "2026-01-01T01:00:00", "AllocTRES": "cpu=4,mem=4096M", "CPUTimeRAW": 14400}],
        })
        result = cost.estimate_task_cost(
          acct, lambda node: 3600.0, disk_price_source=lambda node: None,
          host_lut=self.host_lut, node_types=self.node_types,
        )
        assert result.loc["1_1", "cost_usd"] == pytest.approx(1800.0)
        assert not result.loc["1_1", "is_provisional"]
        assert result.loc["1_1", "disk_cost_usd"] == 0.0
        assert result.loc["1_1", "disk_cost_provisional"]

    def test_compute_pricing_failure_does_not_prevent_disk_pricing(self):
        # The inverse isolation case: compute price missing shouldn't prevent
        # disk cost (which reuses the same resource_frac/elapsed_seconds)
        # from still being computed.
        acct = make_acct_df({
          "1_1": [{"NodeList": "worker1", "Start": "2026-01-01T00:00:00", "End": "2026-01-01T01:00:00", "AllocTRES": "cpu=4,mem=4096M", "CPUTimeRAW": 14400}],
        })
        result = cost.estimate_task_cost(
          acct, lambda node: None, disk_price_source=lambda node: 36.0,
          host_lut=self.host_lut, node_types=self.node_types,
        )
        assert result.loc["1_1", "cost_usd"] == 0.0
        assert result.loc["1_1", "is_provisional"]
        assert result.loc["1_1", "disk_cost_usd"] == pytest.approx(18.0)
        assert not result.loc["1_1", "disk_cost_provisional"]

    def test_missing_capacity_data_skips_disk_pricing_too(self):
        acct = make_acct_df({
          "1_1": [{"NodeList": "worker-unknown", "Start": "2026-01-01T00:00:00", "End": "2026-01-01T01:00:00", "AllocTRES": "cpu=4,mem=4096M", "CPUTimeRAW": 14400}],
        })
        result = cost.estimate_task_cost(
          acct, lambda node: 3600.0, disk_price_source=lambda node: 36.0,
          host_lut=self.host_lut, node_types=self.node_types,
        )
        assert result.loc["1_1", "missing_capacity_data"]
        assert result.loc["1_1", "disk_cost_usd"] == 0.0


# ---------------------------------------------------------------------------
# estimate_scratch_disk_cost
# ---------------------------------------------------------------------------

class TestEstimateScratchDiskCost:
    def test_prices_full_size_and_duration_no_proration(self):
        cost_usd, is_provisional = cost.estimate_scratch_disk_cost(10, "standard", 3600.0, lambda disk_type: 0.01)
        assert cost_usd == pytest.approx(0.10)  # 10GB * $0.01/GB-hr * 1hr
        assert not is_provisional

    def test_normalizes_disk_type_before_calling_price_source(self):
        seen = {}
        def price_source(disk_type):
            seen["disk_type"] = disk_type
            return 0.01
        cost.estimate_scratch_disk_cost(10, "ssd", 3600.0, price_source)
        assert seen["disk_type"] == "pd-ssd"

    def test_unrecognized_disk_type_is_provisional(self):
        cost_usd, is_provisional = cost.estimate_scratch_disk_cost(10, "hyperdisk-extreme", 3600.0, lambda disk_type: 0.01)
        assert cost_usd == 0.0
        assert is_provisional

    def test_missing_price_is_provisional(self):
        cost_usd, is_provisional = cost.estimate_scratch_disk_cost(10, "standard", 3600.0, lambda disk_type: None)
        assert cost_usd == 0.0
        assert is_provisional

    def test_zero_size_is_provisional(self):
        cost_usd, is_provisional = cost.estimate_scratch_disk_cost(0, "standard", 3600.0, lambda disk_type: 0.01)
        assert cost_usd == 0.0
        assert is_provisional

    def test_negative_or_missing_duration_is_provisional(self):
        assert cost.estimate_scratch_disk_cost(10, "standard", None, lambda disk_type: 0.01) == (0.0, True)
        assert cost.estimate_scratch_disk_cost(10, "standard", -1.0, lambda disk_type: 0.01) == (0.0, True)


# ---------------------------------------------------------------------------
# estimate_window_cost_usd
# ---------------------------------------------------------------------------

class TestEstimateWindowCostUsd:
    def test_prices_elapsed_hours(self):
        result = cost.estimate_window_cost_usd(2.0, "2026-01-01T00:00:00", "2026-01-01T02:00:00")
        assert result == pytest.approx(4.0)

    def test_none_price_returns_none(self):
        assert cost.estimate_window_cost_usd(None, "2026-01-01T00:00:00", "2026-01-01T02:00:00") is None

    def test_none_window_returns_none(self):
        assert cost.estimate_window_cost_usd(2.0, None, "2026-01-01T02:00:00") is None
        assert cost.estimate_window_cost_usd(2.0, "2026-01-01T00:00:00", None) is None

    def test_negative_window_clamped_to_zero(self):
        result = cost.estimate_window_cost_usd(2.0, "2026-01-01T02:00:00", "2026-01-01T00:00:00")
        assert result == pytest.approx(0.0)
