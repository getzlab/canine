# vim: set expandtab:

"""
Cost estimation shared by wolF's real-time per-task/pair/run cost tracking and its
post-run BigQuery reconciliation tool. Both consumers call the same functions here
(estimate_task_cost / estimate_node_undersubscription), parameterized by a
`price_source` callable -- live Cloud Billing Catalog API pricing for the real-time
path, actual GCP-invoiced pricing for the reconciliation path -- so the two produce
numbers via identical logic, differing only in how accurate their price inputs are.

Capacity lookups (host_LuT.pickle / nodetypes.json) are read-only here: both files
are written by slurm_gcp_docker's provision_server.py at controller boot, on the
same NFS mount wolF/canine already use. Nothing in this module writes to either.
"""

import json
import os
import threading

import googleapiclient.discovery as gd
import pandas as pd

from .utils import canine_logging

HOST_LUT_PATH = "/mnt/nfs/clust_conf/slurm/host_LuT.pickle"
NODE_TYPES_PATH = "/mnt/nfs/clust_conf/slurm/nodetypes.json"
PRICE_CACHE_PATH = "/mnt/nfs/clust_conf/canine/price_cache.json"

############ capacity lookups (host_LuT.pickle / nodetypes.json) ############

def load_host_lut(path = None):
    """
    node hostname -> {machine_type, preemptible, accelerator_type, accelerator_count}.
    Returns an empty DataFrame (not an error) if the file doesn't exist, so callers
    degrade to missing_capacity_data rather than crashing -- e.g. when running
    outside a live wolF-managed cluster.
    """
    # `path` defaults are resolved here, at call time, rather than bound as a
    # parameter default (which would be evaluated once at function-definition
    # time and never see a later `unittest.mock.patch("...HOST_LUT_PATH", ...)`).
    path = HOST_LUT_PATH if path is None else path
    if not os.path.exists(path):
        return pd.DataFrame(columns = ["machine_type", "preemptible", "accelerator_count", "accelerator_type"])
    return pd.read_pickle(path)


def load_node_types(path = None):
    """
    machine_type -> {cpus, realmemory (MB)} capacity. nodetypes.json lists most
    machine types twice (once per preemptible/non-preemptible node range); CPU/mem
    capacity doesn't depend on preemptibility, so this dedupes on "type" alone --
    preemptibility only matters for pricing, handled separately by get_price().
    """
    path = NODE_TYPES_PATH if path is None else path
    if not os.path.exists(path):
        return pd.DataFrame(columns = ["cpus", "realmemory"]).rename_axis("type")
    node_types = pd.read_json(path)
    node_types = node_types.drop_duplicates(subset = "type", keep = "first")
    return node_types.astype({"cpus": int, "realmemory": float}).set_index("type")[["cpus", "realmemory"]]


def node_capacity(node_name, host_lut, node_types):
    """
    Returns (vcpus, mem_mb) for a node, or (None, None) if the node isn't in
    host_lut or its machine type isn't in node_types. Callers should treat a None
    result as missing_capacity_data, not guess at a fallback capacity.
    """
    if node_name not in host_lut.index:
        return None, None
    machine_type = host_lut.loc[node_name, "machine_type"]
    if machine_type not in node_types.index:
        return None, None
    row = node_types.loc[machine_type]
    return int(row["cpus"]), float(row["realmemory"])


############ AllocTRES / sacct timestamp parsing ############

_MEM_UNIT_MULTIPLIERS_TO_MB = {"K": 1/1024, "M": 1, "G": 1024, "T": 1024*1024}

def _parse_mem_to_mb(value):
    """'4G' -> 4096.0, '4096M' -> 4096.0, '512K' -> 0.5, bare number assumed MB."""
    value = str(value).strip()
    if not value:
        return None
    unit = value[-1].upper()
    if unit in _MEM_UNIT_MULTIPLIERS_TO_MB:
        try:
            return float(value[:-1]) * _MEM_UNIT_MULTIPLIERS_TO_MB[unit]
        except ValueError:
            return None
    try:
        return float(value)
    except ValueError:
        return None


def parse_alloc_tres(alloc_tres):
    """
    Parse a sacct AllocTRES string (e.g. "cpu=2,mem=4G,node=1,billing=2") into
    (alloc_cpus, alloc_mem_mb). AllocTRES always reports a job's total granted
    resources (not per-CPU), regardless of whether the original request used
    --mem or --mem-per-cpu, so no per-node/per-CPU ambiguity applies here (that
    ambiguity is a property of ReqMem's older format, not AllocTRES).

    Returns (None, None) if the string is missing/malformed/doesn't contain both
    keys -- e.g. a job submitted by some other tool/account that reports TRES
    differently. Never guesses.
    """
    if not isinstance(alloc_tres, str) or not alloc_tres or alloc_tres == "-":
        return None, None
    fields = {}
    for kv in alloc_tres.split(","):
        if "=" not in kv:
            continue
        k, v = kv.split("=", 1)
        fields[k.strip()] = v.strip()
    try:
        alloc_cpus = int(fields["cpu"]) if "cpu" in fields else None
    except ValueError:
        alloc_cpus = None
    alloc_mem_mb = _parse_mem_to_mb(fields["mem"]) if "mem" in fields else None
    return alloc_cpus, alloc_mem_mb


def _parse_sacct_time(value):
    """sacct reports "Unknown" for jobs that haven't started/finished yet."""
    if value in (None, "Unknown", "-", ""):
        return pd.NaT
    try:
        return pd.Timestamp(value)
    except (ValueError, TypeError):
        return pd.NaT


def group_sacct_by_job(raw_acct_df):
    """
    Collapses a raw, possibly-multi-row-per-JobID sacct DataFrame (e.g. from a
    query using sacct's "-D" duplicates flag, which shows every preemption/
    requeue attempt as a separate row -- as Orchestrator.query_sacct_for_nodes()
    returns) into one row per JobID, with a summed CPUTimeRAW, the last (by
    Submit) attempt's other fields, and an "attempts" column holding a
    per-attempt breakdown.

    This is the same shape/algorithm as the grouper() closure nested inside
    Orchestrator.wait_for_jobs_to_finish() -- duplicated here (rather than
    imported) since that one is a private nested function, not a reusable
    export; both implement the same collapsing logic and should be kept in
    sync if either changes.

    Used by wolf.cost_reconciliation to reconstruct a per-task,
    estimate_task_cost()-shaped view out of query_sacct_for_nodes()'s
    cross-tenant, ungrouped output, by first filtering to one task's own JobIDs
    and then grouping.
    """
    if raw_acct_df.empty:
        return raw_acct_df

    def _grouper(g):
        g = g.sort_values("Submit")
        final = g.iloc[-1].copy()
        if "CPUTimeRAW" in g.columns:
            final.at["CPUTimeRAW"] = g["CPUTimeRAW"].sum()
        final.at["Submit"] = g["Submit"].iloc[0]
        final["n_preempted"] = len(g) - 1
        attempt_cols = [c for c in ["NodeList", "Start", "End", "AllocTRES", "CPUTimeRAW"] if c in g.columns]
        final["attempts"] = g[attempt_cols].to_dict("records")
        return final

    return raw_acct_df.groupby(raw_acct_df.index).apply(_grouper)


############ live pricing (Cloud Billing Catalog API, cached on disk) ############

_BILLING_CLIENT = None
_BILLING_CLIENT_BUILD_PID = os.getpid()
_BILLING_CLIENT_LOCK = threading.Lock()

_COMPUTE_ENGINE_SERVICE_NAME = None


def get_billing_client():
    """
    Lazy, thread-safe, per-forked-process client singleton -- mirrors
    canine.backends.imageTransient.get_gce_client()'s existing pattern, since
    wolF forks per flow run and a client built in the parent isn't safe to reuse
    in a child. Uses googleapiclient.discovery (already a canine dependency);
    no new dependency needed.
    """
    global _BILLING_CLIENT, _BILLING_CLIENT_BUILD_PID
    with _BILLING_CLIENT_LOCK:
        if _BILLING_CLIENT is None or os.getpid() != _BILLING_CLIENT_BUILD_PID:
            _BILLING_CLIENT_BUILD_PID = os.getpid()
            _BILLING_CLIENT = gd.build('cloudbilling', 'v1')
    return _BILLING_CLIENT


def _price_cache_key(machine_type, zone, preemptible, accelerator_type, accelerator_count):
    return "|".join([
      machine_type, zone, str(bool(preemptible)),
      accelerator_type or "-", str(int(accelerator_count or 0)),
    ])


def load_price_cache(path = None):
    path = PRICE_CACHE_PATH if path is None else path
    if not os.path.exists(path):
        return {}
    try:
        with open(path) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _save_price_cache(cache, path = None):
    path = PRICE_CACHE_PATH if path is None else path
    try:
        os.makedirs(os.path.dirname(path), exist_ok = True)
        with open(path, "w") as f:
            json.dump(cache, f)
    except OSError as e:
        canine_logging.warning("Could not persist price cache to {}: {}".format(path, e))


def _zone_to_region(zone):
    # e.g. "us-central1-a" -> "us-central1"
    return zone.rsplit("-", 1)[0]


def _machine_family(machine_type):
    # e.g. "n1-highcpu-8" -> "n1", "n2d-standard-4" -> "n2d"
    return machine_type.split("-", 1)[0]


_FAMILY_SKU_WORD = {"n1": "N1", "n2": "N2", "n2d": "N2D", "e2": "E2"}


def _fetch_compute_engine_service_name(client):
    global _COMPUTE_ENGINE_SERVICE_NAME
    if _COMPUTE_ENGINE_SERVICE_NAME is not None:
        return _COMPUTE_ENGINE_SERVICE_NAME
    request = client.services().list()
    while request is not None:
        response = request.execute()
        for service in response.get("services", []):
            if service.get("displayName") == "Compute Engine":
                _COMPUTE_ENGINE_SERVICE_NAME = service["name"]
                return _COMPUTE_ENGINE_SERVICE_NAME
        request = client.services().list_next(request, response)
    raise RuntimeError("Could not find Compute Engine in Cloud Billing Catalog services")


def _fetch_all_skus(client, service_name):
    skus = []
    request = client.services().skus().list(parent = service_name, currencyCode = "USD")
    while request is not None:
        response = request.execute()
        skus.extend(response.get("skus", []))
        request = client.services().skus().list_next(request, response)
    return skus


def _sku_unit_price_usd(sku):
    """
    $/unit from a SKU's pricingInfo. Assumes a single flat-rate tier, which holds
    for on-demand/preemptible Compute Engine core-hour and GB-hour SKUs (no
    graduated pricing tiers for these, unlike e.g. sustained-use discounts, which
    aren't modeled here at all -- see plan's discussion of what live pricing can't
    capture).
    """
    tiers = sku["pricingInfo"][0]["pricingExpression"]["tieredRates"]
    rate = tiers[-1]["unitPrice"]
    return float(rate.get("units", 0)) + rate.get("nanos", 0) / 1e9


def match_compute_engine_price(skus, machine_type, region, preemptible):
    """
    Given a list of Catalog API SKUs (as returned by _fetch_all_skus), find the
    per-core and per-GB hourly prices for a predefined machine type in a region.
    Returns (cpu_price_per_core_hour, ram_price_per_gb_hour), or None if no
    confident match is found.

    This is a best-effort match against SKU description text -- the only way the
    Catalog API exposes this, since it has no structured machine-type field.
    SKU wording is not a stable, versioned contract; this should be checked
    against a real API response during live validation, not trusted from static
    review alone.
    """
    family_word = _FAMILY_SKU_WORD.get(_machine_family(machine_type))
    if family_word is None:
        return None  # unrecognized family -- not enough info to match confidently

    def matches(sku, resource_word):
        if sku.get("category", {}).get("resourceFamily") != "Compute":
            return False
        if region not in sku.get("serviceRegions", []):
            return False
        desc = sku.get("description", "")
        is_preemptible_sku = "Preemptible" in desc or "Spot" in desc
        if is_preemptible_sku != bool(preemptible):
            return False
        return family_word in desc and "Predefined Instance" in desc and resource_word in desc

    cpu_skus = [s for s in skus if matches(s, "Core")]
    ram_skus = [s for s in skus if matches(s, "Ram")]
    if not cpu_skus or not ram_skus:
        return None
    return _sku_unit_price_usd(cpu_skus[0]), _sku_unit_price_usd(ram_skus[0])


def match_accelerator_price(skus, accelerator_type, region, preemptible):
    """Same best-effort description-matching approach as match_compute_engine_price,
    for GPU accelerator SKUs (priced per-GPU-hour, flat -- no core/RAM split)."""
    model = accelerator_type.rsplit("-", 1)[-1].upper()

    def matches(sku):
        if sku.get("category", {}).get("resourceFamily") != "Compute":
            return False
        if region not in sku.get("serviceRegions", []):
            return False
        desc = sku.get("description", "").upper()
        is_preemptible_sku = "PREEMPTIBLE" in desc or "SPOT" in desc
        if is_preemptible_sku != bool(preemptible):
            return False
        return "GPU" in desc and model in desc

    matched = [s for s in skus if matches(s)]
    if not matched:
        return None
    return _sku_unit_price_usd(matched[0])


def get_price(machine_type, zone, preemptible, accelerator_type = None, accelerator_count = 0, node_types = None):
    """
    Live $/hour for a (machine_type, zone, preemptible[, accelerator]) recipe, via
    the Cloud Billing Catalog API, cached on disk (PRICE_CACHE_PATH) keyed by
    recipe -- repeated lookups for the same recipe within a cluster's lifetime
    don't re-hit the API. Returns None (never a guess) if pricing can't be
    determined; callers should flag this as missing/provisional rather than
    silently treating a job as free.

    `node_types` (from load_node_types()) is required to convert per-core/per-GB
    SKU prices into a per-instance $/hour rate using that machine type's actual
    vCPU/RAM counts; loaded automatically if not given.
    """
    node_types = load_node_types() if node_types is None else node_types
    if machine_type not in node_types.index:
        return None

    # host_LuT.pickle stores accelerator_type/accelerator_count as NaN (not
    # None/0) for every non-GPU node -- provision_server.py builds it via a
    # regex .str.extract(), which leaves non-matching rows as NaN across all
    # captured columns. NaN is truthy in Python (`float("nan") or 0` evaluates
    # to the NaN, not 0), so a plain `or` fallback never actually catches it --
    # confirmed live: this crashed int(accelerator_count) with "cannot convert
    # float NaN to integer" for every non-accelerator node, i.e. nearly every
    # real job, well before this function's own try/except ever started.
    if accelerator_type is None or (isinstance(accelerator_type, float) and pd.isna(accelerator_type)):
        accelerator_type = None
    accelerator_count = (
      0 if accelerator_count is None or (isinstance(accelerator_count, float) and pd.isna(accelerator_count))
      else int(accelerator_count)
    )

    cache = load_price_cache()
    key = _price_cache_key(machine_type, zone, preemptible, accelerator_type, accelerator_count)
    if key in cache:
        return cache[key]

    try:
        client = get_billing_client()
        service_name = _fetch_compute_engine_service_name(client)
        skus = _fetch_all_skus(client, service_name)
        region = _zone_to_region(zone)
        matched = match_compute_engine_price(skus, machine_type, region, preemptible)
        if matched is None:
            return None
        cpu_price, ram_price = matched
        vcpus = int(node_types.loc[machine_type, "cpus"])
        mem_gb = float(node_types.loc[machine_type, "realmemory"]) / 1024
        price_per_hour = cpu_price * vcpus + ram_price * mem_gb

        if accelerator_type and accelerator_count:
            gpu_price = match_accelerator_price(skus, accelerator_type, region, preemptible)
            if gpu_price is not None:
                price_per_hour += gpu_price * accelerator_count
    except Exception as e:
        canine_logging.warning("Could not fetch live price for {}/{}/preemptible={}: {}".format(machine_type, zone, preemptible, e))
        return None

    cache[key] = price_per_hour
    _save_price_cache(cache)
    return price_per_hour


def make_live_price_source(zone, host_lut = None, node_types = None):
    """
    Builds a price_source callable (node_name -> $/hour or None) for the
    real-time path, backed by get_price(). `zone` is the cluster's single
    compute_zone (wolF's backend config) -- not a per-node value, since
    host_LuT.pickle has no zone column.
    """
    host_lut = load_host_lut() if host_lut is None else host_lut
    node_types = load_node_types() if node_types is None else node_types

    def _source(node_name):
        if node_name not in host_lut.index:
            return None
        row = host_lut.loc[node_name]
        return get_price(
          row["machine_type"], zone, bool(row["preemptible"]),
          row.get("accelerator_type"), row.get("accelerator_count") or 0,
          node_types = node_types,
        )
    return _source


############ per-task / per-node cost estimation ############

def estimate_task_cost(acct_df, price_source, host_lut = None, node_types = None):
    """
    acct_df: DataFrame shaped like Task.acct -- one row per shard, with an
      "attempts" column holding a list of per-attempt {NodeList, Start, End,
      AllocTRES, CPUTimeRAW} dicts (see Orchestrator.wait_for_jobs_to_finish's
      grouper()).
    price_source: callable(node_name) -> $/hour, or None if unavailable. Swap
      make_live_price_source(...) (real-time) for a reconciliation-side,
      billing-grounded per-node lookup to get identical-logic, differently-
      accurate numbers from the same function.

    Returns a DataFrame indexed the same as acct_df, with cost_usd (summed
    across every attempt/node the shard touched), missing_capacity_data, and
    is_provisional columns. Never guesses: a shard with any attempt on an
    unrecognized node, with unparseable AllocTRES, or missing a price, is
    flagged rather than silently under-costed.
    """
    host_lut = load_host_lut() if host_lut is None else host_lut
    node_types = load_node_types() if node_types is None else node_types

    rows = []
    for jid, row in acct_df.iterrows():
        attempts = row.get("attempts") or []
        total_cost = 0.0
        missing_capacity_data = len(attempts) == 0
        is_provisional = False

        for attempt in attempts:
            node_name = attempt.get("NodeList")
            vcpus, mem_mb = node_capacity(node_name, host_lut, node_types)
            if vcpus is None:
                missing_capacity_data = True
                continue

            alloc_cpus, alloc_mem_mb = parse_alloc_tres(attempt.get("AllocTRES"))
            if alloc_cpus is None or alloc_mem_mb is None:
                missing_capacity_data = True
                continue

            start = _parse_sacct_time(attempt.get("Start"))
            end = _parse_sacct_time(attempt.get("End"))
            if pd.isna(start) or pd.isna(end):
                missing_capacity_data = True
                continue

            price_per_hour = price_source(node_name)
            if price_per_hour is None:
                is_provisional = True
                continue

            elapsed_seconds = max(0.0, (end - start).total_seconds())
            cpu_frac = alloc_cpus / vcpus
            mem_frac = alloc_mem_mb / mem_mb
            total_cost += (price_per_hour / 3600) * elapsed_seconds * max(cpu_frac, mem_frac)

        rows.append({
          "JobID": jid, "cost_usd": total_cost,
          "missing_capacity_data": missing_capacity_data, "is_provisional": is_provisional,
        })

    return pd.DataFrame(rows).set_index("JobID")


def estimate_node_undersubscription(node_sacct_snapshot, price_source, host_lut = None, node_types = None):
    """
    node_sacct_snapshot: DataFrame as returned by
      Orchestrator.query_sacct_for_nodes -- one row per job attempt on some set
      of nodes, any account/tenant, indexed by JobID, with a "NodeList" column.
    price_source: callable(node_name) -> $/hour, or None if unavailable.

    For each node: reconstructs the allocated-vs-total-capacity curve over the
    window jobs were actually observed on it (union of every job's [Start, End]
    x its AllocCPUS/AllocMem share, against node capacity), and returns the
    integrated gap priced in dollars -- capacity billed but never allocated to
    any job while the node was actively hosting work. Does NOT cover time before
    a node's first observed job or after its last (that's only recoverable from
    real node lifetime data, i.e. the reconciliation tool's true_wasted_cost).

    Returns a DataFrame, one row per node: node_name, vcpus, mem_mb,
    observed_start, observed_end, node_cost_usd, wasted_cost_usd,
    missing_capacity_data, is_provisional.
    """
    host_lut = load_host_lut() if host_lut is None else host_lut
    node_types = load_node_types() if node_types is None else node_types

    results = []
    if node_sacct_snapshot is None or len(node_sacct_snapshot) == 0:
        return pd.DataFrame(results)

    for node_name, jobs in node_sacct_snapshot.groupby("NodeList"):
        vcpus, mem_mb = node_capacity(node_name, host_lut, node_types)
        price_per_hour = price_source(node_name)

        if vcpus is None or price_per_hour is None:
            results.append({
              "node_name": node_name, "vcpus": vcpus, "mem_mb": mem_mb,
              "observed_start": pd.NaT, "observed_end": pd.NaT,
              "node_cost_usd": None, "wasted_cost_usd": None,
              "missing_capacity_data": vcpus is None, "is_provisional": price_per_hour is None,
            })
            continue

        intervals = []  # [start, end, alloc_cpus, alloc_mem_mb]
        for _, job in jobs.iterrows():
            alloc_cpus, alloc_mem_mb = parse_alloc_tres(job.get("AllocTRES"))
            if alloc_cpus is None or alloc_mem_mb is None:
                continue
            start = _parse_sacct_time(job.get("Start"))
            if pd.isna(start):
                continue
            end = _parse_sacct_time(job.get("End"))
            intervals.append([start, end, alloc_cpus, alloc_mem_mb])

        if not intervals:
            results.append({
              "node_name": node_name, "vcpus": vcpus, "mem_mb": mem_mb,
              "observed_start": pd.NaT, "observed_end": pd.NaT,
              "node_cost_usd": 0.0, "wasted_cost_usd": 0.0,
              "missing_capacity_data": False, "is_provisional": False,
            })
            continue

        # still-running jobs (no End yet, known v1 limitation -- see plan) are
        # clipped to the node's overall observed window, once that's known
        observed_start = min(iv[0] for iv in intervals)
        known_ends = [iv[1] for iv in intervals if not pd.isna(iv[1])]
        observed_end = max(known_ends) if known_ends else observed_start
        for iv in intervals:
            if pd.isna(iv[1]) or iv[1] <= iv[0]:
                iv[1] = observed_end

        breakpoints = sorted({iv[0] for iv in intervals} | {iv[1] for iv in intervals})
        wasted_seconds_weighted = 0.0
        for t0, t1 in zip(breakpoints[:-1], breakpoints[1:]):
            duration = (t1 - t0).total_seconds()
            if duration <= 0:
                continue
            active = [iv for iv in intervals if iv[0] <= t0 < iv[1]]
            alloc_cpu_sum = sum(iv[2] for iv in active)
            alloc_mem_sum = sum(iv[3] for iv in active)
            utilization = max(alloc_cpu_sum / vcpus, alloc_mem_sum / mem_mb)
            waste_fraction = max(0.0, 1.0 - min(utilization, 1.0))
            wasted_seconds_weighted += waste_fraction * duration

        node_window_seconds = (observed_end - observed_start).total_seconds()
        results.append({
          "node_name": node_name, "vcpus": vcpus, "mem_mb": mem_mb,
          "observed_start": observed_start, "observed_end": observed_end,
          "node_cost_usd": (price_per_hour / 3600) * node_window_seconds,
          "wasted_cost_usd": (price_per_hour / 3600) * wasted_seconds_weighted,
          "missing_capacity_data": False, "is_provisional": False,
        })

    return pd.DataFrame(results)
