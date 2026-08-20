"""
Pure unit tests for canine.cost -- no SLURM cluster, no live GCP credentials
required. Catalog API interaction is exercised against synthetic SKU fixtures,
not a real network call.
"""
import json
import os
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

    def test_api_exception_returns_none(self, tmp_path):
        cache_path = tmp_path / "price_cache.json"
        with patch("canine.cost.PRICE_CACHE_PATH", str(cache_path)), \
             patch("canine.cost.get_billing_client", side_effect=RuntimeError("no credentials")):
            result = cost.get_price("n1-highcpu-8", "us-central1-a", False, node_types=self.node_types)
        assert result is None


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
