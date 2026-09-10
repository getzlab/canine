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

import http.client
import json
import os
import threading
import time

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


def resolve_job_resources(alloc_tres, ncpus = None, req_mem = None):
    """
    (alloc_cpus, alloc_mem_mb) for a job attempt, preferring AllocTRES's own
    cpu=/mem= (per-attempt granted allocation) and falling back, per-field, to
    NCPUS/ReqMem when AllocTRES doesn't report a given resource.

    Confirmed live: some SLURM/accounting configurations report AllocTRES as
    just "billing=1+" with no cpu=/mem= keys at all, even though the same
    sacct row's own NCPUS (allocated CPU count) and ReqMem are fully
    populated -- not a hypothetical edge case. ReqMem is used rather than
    ReqCPUS's memory analogue because SLURM has no separate "allocated mem"
    field outside AllocTRES; since wolF/canine only ever submit via --mem
    (never --mem-per-cpu), ReqMem is already a per-node total, matching
    AllocTRES's own mem= convention -- but older SLURM versions can still
    suffix ReqMem with a trailing "c"/"n" (per-cpu/per-node) marker, so that
    suffix is stripped defensively before parsing.
    """
    alloc_cpus, alloc_mem_mb = parse_alloc_tres(alloc_tres)

    if alloc_cpus is None and ncpus is not None:
        try:
            alloc_cpus = int(ncpus)
        except (TypeError, ValueError):
            pass

    if alloc_mem_mb is None and req_mem is not None:
        req_mem = str(req_mem).strip()
        if req_mem and req_mem[-1].upper() in ("C", "N") and len(req_mem) > 1 and req_mem[-2].upper() in _MEM_UNIT_MULTIPLIERS_TO_MB:
            req_mem = req_mem[:-1]
        alloc_mem_mb = _parse_mem_to_mb(req_mem)

    return alloc_cpus, alloc_mem_mb


def _parse_sacct_time(value):
    """sacct reports "Unknown" for jobs that haven't started/finished yet."""
    if value in (None, "Unknown", "-", ""):
        return pd.NaT
    try:
        return pd.Timestamp(value)
    except (ValueError, TypeError):
        return pd.NaT


_SACCT_TIME_PLACEHOLDERS = {"Unknown": "", "-": ""}


def _attempt_sort_key(start_col):
    """
    Sort key for ordering a job's preemption/requeue attempts chronologically
    by Start, not Submit (Submit is the original job submission time and
    stays constant across every requeue of the same JobID, so sorting on it
    doesn't reliably produce chronological order -- pandas' sort isn't
    guaranteed stable for tied keys). Mirrors
    Orchestrator.wait_for_jobs_to_finish's identically-named helper --
    duplicated here for the same reason group_sacct_by_job() itself is (see
    its docstring). Maps "Unknown"/"-" to "" so an attempt that hasn't
    started yet sorts before any real ISO timestamp string.
    """
    return start_col.replace(_SACCT_TIME_PLACEHOLDERS)


def group_sacct_by_job(raw_acct_df):
    """
    Collapses a raw, possibly-multi-row-per-JobID sacct DataFrame (e.g. from a
    query using sacct's "-D" duplicates flag, which shows every preemption/
    requeue attempt as a separate row -- as Orchestrator.query_sacct_for_nodes()
    returns) into one row per JobID, with a summed CPUTimeRAW, the last (by
    Start) attempt's other fields, and an "attempts" column holding a
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
        g = g.sort_values("Start", key = _attempt_sort_key)
        final = g.iloc[-1].copy()
        if "CPUTimeRAW" in g.columns:
            final.at["CPUTimeRAW"] = g["CPUTimeRAW"].sum()
        final.at["Submit"] = g["Submit"].iloc[0]
        final["n_preempted"] = len(g) - 1
        attempt_cols = [c for c in ["NodeList", "Start", "End", "AllocTRES", "CPUTimeRAW", "NCPUS", "ReqMem"] if c in g.columns]
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
    Lazy, per-forked-process client singleton -- mirrors
    canine.backends.imageTransient.get_gce_client()'s existing pattern, since
    wolF forks per flow run and a client built in the parent isn't safe to reuse
    in a child. Uses googleapiclient.discovery (already a canine dependency);
    no new dependency needed.

    Only *construction* of the singleton is thread-safe (guarded below).
    googleapiclient's discovery-based clients are backed by httplib2, which is
    documented as unsafe for concurrent .execute() calls from multiple threads
    on the same instance -- callers must serialize actual use of the returned
    client themselves (get_price() does this via _PRICE_FETCH_LOCK). Confirmed
    live: without that serialization, several wolF tasks finishing around the
    same time and racing into a price-cache miss concurrently crashed the
    whole process with "malloc(): unsorted double linked list corrupted" --
    silent heap corruption from concurrent use of the shared, non-thread-safe
    client, not a Python-level exception.
    """
    global _BILLING_CLIENT, _BILLING_CLIENT_BUILD_PID
    with _BILLING_CLIENT_LOCK:
        if _BILLING_CLIENT is None or os.getpid() != _BILLING_CLIENT_BUILD_PID:
            _BILLING_CLIENT_BUILD_PID = os.getpid()
            _BILLING_CLIENT = gd.build('cloudbilling', 'v1')
    return _BILLING_CLIENT


def _invalidate_billing_client():
    """
    Forces the next get_billing_client() call to build a brand new client
    (fresh httplib2 connections), rather than reusing one whose underlying
    connection may now be dead. Called by get_price() when a fetch fails with
    a transient network/SSL error.

    Confirmed live: a long-lived cluster can see its cached client's
    keep-alive connection go stale (server- or load-balancer-side idle
    timeout) between infrequent price lookups, surfacing as e.g.
    "[SSL: RECORD_LAYER_FAILURE] record layer failure". Since a failed fetch
    is never cached, simply retrying against the same broken connection would
    otherwise keep failing for the rest of the cluster's life -- silently
    degrading every future not-yet-cached recipe to is_provisional instead of
    just this one transient hiccup.
    """
    global _BILLING_CLIENT
    with _BILLING_CLIENT_LOCK:
        _BILLING_CLIENT = None


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


_PRICE_FETCH_LOCK = threading.Lock()


def _save_price_cache(cache, path = None):
    path = PRICE_CACHE_PATH if path is None else path
    try:
        os.makedirs(os.path.dirname(path), exist_ok = True)
        with open(path, "w") as f:
            json.dump(cache, f)
    except OSError as e:
        canine_logging.warning("Could not persist price cache to {}: {}".format(path, e))


_MAX_FETCH_ATTEMPTS = 3

# ssl.SSLError and ConnectionError are both OSError subclasses in Python 3;
# http.client.HTTPException (e.g. RemoteDisconnected) is a separate hierarchy
# httplib2 can also surface. Both are transient-connection symptoms worth a
# fresh-client retry, unlike e.g. a RuntimeError from a genuinely missing
# Catalog API service, which retrying can't fix.
_TRANSIENT_FETCH_ERRORS = (OSError, http.client.HTTPException)


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

    Confirmed live against a real Catalog API response: the "Predefined
    Instance" wording only holds for N1 ("N1 Predefined Instance Core
    running in Americas") -- N2/N2D/E2's real, plain on-demand SKUs read
    "N2 Instance Core running in Americas"/"N2D AMD Instance Core running in
    Americas" etc, with no "Predefined" at all. Requiring that literal
    phrase made every non-N1 family silently unmatchable (get_price()
    returning None with no error, since the API call itself succeeds --
    see get_price()'s own "no matching SKU found" warning, added
    specifically because this failure mode used to be invisible).
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
        # Custom machine types and Sole Tenancy nodes have their own,
        # differently-priced SKUs that also contain the plain family
        # word/"Instance" (e.g. "N2 Custom Instance Core running in
        # Americas", "N2 Sole Tenancy Instance Core running in Americas")
        # -- excluded explicitly, since canine only ever creates predefined
        # (non-custom, non-sole-tenancy) machine types.
        if "Custom" in desc or "Sole Tenancy" in desc:
            return False
        # family_word must match as a whole word, not a substring: "N2" is
        # a substring of "N2D", so a plain `"N2" in desc` check would let an
        # N2 query silently match (and mis-price against) an N2D SKU.
        return family_word in desc.split() and "Instance" in desc and resource_word in desc

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


def _resolve_machine_type_capacity(machine_type, zone, project, node_types):
    """
    (vcpus, mem_gb) for machine_type, or None if it can't be determined --
    node_types (nodetypes.json) first, since that's a free, already-loaded
    local lookup for every SLURM worker type; falling back to a live Compute
    Engine API lookup (get_machine_type_capacity()) when machine_type isn't a
    worker type at all -- e.g. wolF's controller VM -- and a project was given
    to look it up against. Logs a warning either way when capacity can't be
    found, rather than the silent `None` get_price() used to return here (a
    real bug: it meant a missing/unpriceable machine type -- like a controller
    VM whose type is never a SLURM partition type -- produced no cost lines
    and no warning at all).
    """
    if machine_type in node_types.index:
        row = node_types.loc[machine_type]
        return int(row["cpus"]), float(row["realmemory"]) / 1024

    if project is not None:
        vcpus, mem_mb = get_machine_type_capacity(machine_type, zone, project)
        if vcpus is not None:
            return vcpus, mem_mb / 1024

    canine_logging.warning(
      "Could not price {}/{}: not a known worker machine type (not in nodetypes.json){}".format(
        machine_type, zone,
        "" if project is None else ", and not found via the Compute Engine API either"))
    return None


def get_price(machine_type, zone, preemptible, accelerator_type = None, accelerator_count = 0, node_types = None, project = None):
    """
    Live $/hour for a (machine_type, zone, preemptible[, accelerator]) recipe, via
    the Cloud Billing Catalog API, cached on disk (PRICE_CACHE_PATH) keyed by
    recipe -- repeated lookups for the same recipe within a cluster's lifetime
    don't re-hit the API. Returns None (never a guess) if pricing can't be
    determined; callers should flag this as missing/provisional rather than
    silently treating a job as free.

    `node_types` (from load_node_types()) is consulted first to convert
    per-core/per-GB SKU prices into a per-instance $/hour rate using that
    machine type's actual vCPU/RAM counts; loaded automatically if not given.
    But `node_types` only lists *SLURM worker* machine types (nodetypes.json is
    written for the cluster's own elastic partitions) -- a machine type that's
    never a worker type, e.g. wolF's controller VM (see
    Workflow._controller_price_per_hour()), will never appear there. If
    `project` is given, a machine type missing from `node_types` falls back to
    a live Compute Engine API lookup (get_machine_type_capacity()) instead of
    silently failing.

    Everything past the first cache check is serialized behind
    _PRICE_FETCH_LOCK: get_billing_client()'s httplib2-backed client isn't safe
    for concurrent .execute() calls from multiple threads (see its docstring),
    and price_cache.json's own read-modify-write isn't safe to race either --
    both real bugs, not theoretical, since wolF calls this from per-task worker
    threads that can finish concurrently.
    """
    node_types = load_node_types() if node_types is None else node_types
    capacity = _resolve_machine_type_capacity(machine_type, zone, project, node_types)
    if capacity is None:
        return None
    vcpus, mem_gb = capacity

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

    key = _price_cache_key(machine_type, zone, preemptible, accelerator_type, accelerator_count)
    cache = load_price_cache()
    if key in cache:
        return cache[key]

    with _PRICE_FETCH_LOCK:
        # re-check: another thread may have populated this key while we were
        # waiting on the lock -- avoids a redundant fetch, not just a redundant
        # race.
        cache = load_price_cache()
        if key in cache:
            return cache[key]

        last_exc = None
        for attempt in range(1, _MAX_FETCH_ATTEMPTS + 1):
            try:
                client = get_billing_client()
                service_name = _fetch_compute_engine_service_name(client)
                skus = _fetch_all_skus(client, service_name)
                region = _zone_to_region(zone)
                matched = match_compute_engine_price(skus, machine_type, region, preemptible)
                if matched is None:
                    # Distinct from the API-call failures below: the request
                    # itself succeeded, but no SKU's description text matched
                    # this machine_type/region/preemptible combination.
                    # Confirmed live: this was previously silent (a bare
                    # `return None`), which is indistinguishable from
                    # "pricing succeeded and cost is legitimately $0" in the
                    # logs -- the very failure mode match_compute_engine_price()'s
                    # own docstring already warns is possible (SKU wording is
                    # not a stable, versioned contract).
                    canine_logging.warning(
                      "Could not fetch live price for {}/{}/preemptible={}: no matching SKU found (region={})".format(
                        machine_type, zone, preemptible, region))
                    return None
                cpu_price, ram_price = matched
                price_per_hour = cpu_price * vcpus + ram_price * mem_gb

                if accelerator_type and accelerator_count:
                    gpu_price = match_accelerator_price(skus, accelerator_type, region, preemptible)
                    if gpu_price is not None:
                        price_per_hour += gpu_price * accelerator_count
                break
            except _TRANSIENT_FETCH_ERRORS as e:
                last_exc = e
                canine_logging.warning(
                  "Transient error fetching live price for {}/{}/preemptible={} (attempt {}/{}): {} -- retrying with a fresh client".format(
                    machine_type, zone, preemptible, attempt, _MAX_FETCH_ATTEMPTS, e))
                _invalidate_billing_client()
                if attempt < _MAX_FETCH_ATTEMPTS:
                    time.sleep(attempt)
            except Exception as e:
                canine_logging.warning("Could not fetch live price for {}/{}/preemptible={}: {}".format(machine_type, zone, preemptible, e))
                return None
        else:
            canine_logging.warning(
              "Could not fetch live price for {}/{}/preemptible={} after {} attempts: {}".format(
                machine_type, zone, preemptible, _MAX_FETCH_ATTEMPTS, last_exc))
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


############ persistent-disk pricing (Cloud Billing Catalog API, cached on disk) ############

# GCP's own standard month-length convention for converting a $/GB-month PD
# price into a $/GB-hour rate -- introduced here, not something derived from
# any API response.
AVG_HOURS_PER_MONTH = 730.0

_DISK_TYPE_ALIASES = {
  "standard": "pd-standard", "pd-standard": "pd-standard",
  "ssd": "pd-ssd", "pd-ssd": "pd-ssd",
  "balanced": "pd-balanced", "pd-balanced": "pd-balanced",
}


def _normalize_disk_type(disk_type):
    """
    Canonicalizes the two disk-type vocabularies used across wolF/canine
    (nfs_disk_type's "pd-standard"/"pd-ssd"/"pd-balanced", scratch_disk_type's
    "standard"/"ssd"/"balanced") onto one "pd-<x>" form. Returns None for
    anything unrecognized -- callers should treat that as missing pricing
    data, not guess a fallback type.
    """
    if not disk_type:
        return None
    return _DISK_TYPE_ALIASES.get(str(disk_type).strip().lower())


_DISK_TYPE_SKU_WORD = {
  "pd-standard": "Storage PD Capacity",
  "pd-ssd": "SSD backed PD Capacity",
  "pd-balanced": "Balanced PD Capacity",
}


def match_disk_price(skus, disk_type, region):
    """
    Given Catalog API SKUs (as returned by _fetch_all_skus), find the
    $/GB-month price for a persistent-disk type in a region. Returns None if
    no confident match is found.

    Best-effort text match, same caveat as match_compute_engine_price: this
    needs checking against a real Catalog API response during live
    validation, not trusted from static review alone -- in particular,
    whether persistent-disk SKUs really report resourceFamily "Storage"
    (rather than "Compute", used by the CPU/RAM SKUs matched above) isn't
    asserted anywhere else in this codebase. Regional (replicated) PD
    variants are explicitly excluded: wolF/canine never creates those.
    """
    sku_word = _DISK_TYPE_SKU_WORD.get(disk_type)
    if sku_word is None:
        return None  # unrecognized disk type -- not enough info to match confidently

    def matches(sku):
        if sku.get("category", {}).get("resourceFamily") != "Storage":
            return False
        if region not in sku.get("serviceRegions", []):
            return False
        desc = sku.get("description", "")
        if "Regional" in desc:
            return False
        return sku_word in desc

    matched = [s for s in skus if matches(s)]
    if not matched:
        return None
    return _sku_unit_price_usd(matched[0])


def _disk_price_cache_key(disk_type, zone):
    # "disk|" prefix keeps this distinct from get_price()'s own cache keys in
    # the same on-disk cache file (PRICE_CACHE_PATH) -- disk pricing has no
    # preemptible/accelerator dimension, but a plain "<disk_type>|<zone>" key
    # could otherwise collide in principle with a machine-type string.
    return "disk|{}|{}".format(disk_type, zone)


def get_disk_price_per_gb_month(disk_type, zone):
    """
    Live $/GB-month for a persistent-disk type in a zone, via the Cloud
    Billing Catalog API, cached on disk (PRICE_CACHE_PATH -- same cache file
    get_price() uses, keyed distinctly via the "disk|" prefix so the two
    never collide). Returns None (never a guess) if pricing can't be
    determined or disk_type is unrecognized.

    Shares get_price()'s retry/locking machinery (_PRICE_FETCH_LOCK,
    _TRANSIENT_FETCH_ERRORS, _invalidate_billing_client) for the same reason:
    get_billing_client()'s underlying httplib2 client isn't safe for
    concurrent .execute() calls, and price_cache.json's read-modify-write
    isn't safe to race either.
    """
    disk_type = _normalize_disk_type(disk_type)
    if disk_type is None:
        return None

    key = _disk_price_cache_key(disk_type, zone)
    cache = load_price_cache()
    if key in cache:
        return cache[key]

    with _PRICE_FETCH_LOCK:
        # re-check: another thread may have populated this key while we were
        # waiting on the lock.
        cache = load_price_cache()
        if key in cache:
            return cache[key]

        last_exc = None
        for attempt in range(1, _MAX_FETCH_ATTEMPTS + 1):
            try:
                client = get_billing_client()
                service_name = _fetch_compute_engine_service_name(client)
                skus = _fetch_all_skus(client, service_name)
                region = _zone_to_region(zone)
                price_per_gb_month = match_disk_price(skus, disk_type, region)
                if price_per_gb_month is None:
                    # Same distinction as get_price()'s equivalent branch:
                    # the request succeeded, but no SKU matched -- log it
                    # rather than returning silently, so this isn't
                    # indistinguishable from "priced at $0".
                    canine_logging.warning(
                      "Could not fetch live disk price for {}/{}: no matching SKU found (region={})".format(
                        disk_type, zone, region))
                    return None
                break
            except _TRANSIENT_FETCH_ERRORS as e:
                last_exc = e
                canine_logging.warning(
                  "Transient error fetching live disk price for {}/{} (attempt {}/{}): {} -- retrying with a fresh client".format(
                    disk_type, zone, attempt, _MAX_FETCH_ATTEMPTS, e))
                _invalidate_billing_client()
                if attempt < _MAX_FETCH_ATTEMPTS:
                    time.sleep(attempt)
            except Exception as e:
                canine_logging.warning("Could not fetch live disk price for {}/{}: {}".format(disk_type, zone, e))
                return None
        else:
            canine_logging.warning(
              "Could not fetch live disk price for {}/{} after {} attempts: {}".format(
                disk_type, zone, _MAX_FETCH_ATTEMPTS, last_exc))
            return None

        cache[key] = price_per_gb_month
        _save_price_cache(cache)
        return price_per_gb_month


def get_disk_price_per_gb_hour(disk_type, zone):
    price_per_gb_month = get_disk_price_per_gb_month(disk_type, zone)
    if price_per_gb_month is None:
        return None
    return price_per_gb_month / AVG_HOURS_PER_MONTH


############ live disk size/type (Compute Engine API, short-lived in-process cache) ############

_GCE_DISK_CLIENT = None
_GCE_DISK_CLIENT_BUILD_PID = os.getpid()
_GCE_DISK_CLIENT_LOCK = threading.Lock()


def get_gce_disk_client():
    """
    Lazy, per-forked-process client singleton for the Compute Engine disks()
    API -- same rationale/pattern as get_billing_client() (wolF forks per
    flow run; a client built in the parent isn't safe to reuse in a child).
    Kept here (rather than imported from canine.backends.imageTransient's own
    GCE client helper) for consistency with this module's existing
    self-contained style; only construction is thread-safe, same caveat as
    get_billing_client().
    """
    global _GCE_DISK_CLIENT, _GCE_DISK_CLIENT_BUILD_PID
    with _GCE_DISK_CLIENT_LOCK:
        if _GCE_DISK_CLIENT is None or os.getpid() != _GCE_DISK_CLIENT_BUILD_PID:
            _GCE_DISK_CLIENT_BUILD_PID = os.getpid()
            _GCE_DISK_CLIENT = gd.build('compute', 'v1')
    return _GCE_DISK_CLIENT


def _invalidate_gce_disk_client():
    """Same rationale as _invalidate_billing_client(): forces the next
    get_gce_disk_client() call to build a fresh client, rather than retrying
    against a connection that may now be dead (e.g. after a read timeout)."""
    global _GCE_DISK_CLIENT
    with _GCE_DISK_CLIENT_LOCK:
        _GCE_DISK_CLIENT = None


_DISK_INFO_CACHE = {}
_DISK_INFO_CACHE_LOCK = threading.Lock()

# Serializes every actual GCE API call this module makes for disk info --
# same rationale as _PRICE_FETCH_LOCK: get_gce_disk_client()'s underlying
# discovery client is httplib2-backed, and concurrent .execute() calls from
# multiple threads on the same client instance are unsafe. Confirmed live:
# without this, many wolF tasks calling get_live_disk_info() concurrently
# (one call per node, per task -- wolF runs many tasks at once) corrupted
# the shared client's connection state, surfacing as
# "[SSL: RECORD_LAYER_FAILURE]"/read-timeout errors on nearly every call --
# not genuine network flakiness, and not fixable by _invalidate_gce_disk_client()
# alone, since a freshly-rebuilt client immediately gets corrupted again by
# whichever other thread is still mid-.execute() on it.
_GCE_DISK_FETCH_LOCK = threading.Lock()

_GCE_DISK_TYPE_URL_ALIASES = {"pd-standard": "pd-standard", "pd-ssd": "pd-ssd", "pd-balanced": "pd-balanced"}


def _parse_gce_disk_type(disk_type_url):
    """'https://www.googleapis.com/compute/v1/projects/.../diskTypes/pd-ssd' -> 'pd-ssd'."""
    if not disk_type_url:
        return None
    return _GCE_DISK_TYPE_URL_ALIASES.get(disk_type_url.rsplit("/", 1)[-1])


def get_live_disk_info(disk_name, zone, project, ttl_seconds = 300):
    """
    Live (size_gb, disk_type) for a disk resource, via the Compute Engine
    API -- used to get a worker node's *actual current* boot-disk size,
    since worker_boot_disk_resize.sh auto-grows it well past its
    provisioning-time default over a long-lived node's lifetime, and that
    growth is recorded nowhere static (not in host_LuT.pickle/nodetypes.json).

    Returns (None, None) on any failure (disk not found, API error, node
    already torn down) -- never guesses a fallback size.

    Cached in-process only (not persisted to PRICE_CACHE_PATH like
    get_price()/get_disk_price_per_gb_month(), since a disk's size can
    legitimately change again later in the same node's life -- persisting it
    would risk permanently undercounting a node that grows its disk after the
    first lookup), with a short TTL: this collapses redundant lookups when
    several shards on the same node finish near-simultaneously (a real, not
    hypothetical, pattern -- see get_price()'s own docstring on concurrent
    per-task cost-estimate calls) without letting a long-lived, still-growing
    node's cached size go stale for its whole remaining lifetime. The
    ttl_seconds default is a judgment call, not a measured constant.

    Retries on the same transient-error class get_price() does
    (_TRANSIENT_FETCH_ERRORS -- read timeouts confirmed live against real GCE
    API calls, not hypothetical), rebuilding the client between attempts via
    _invalidate_gce_disk_client() for the same reason get_price() does for
    the billing client: a read timeout can leave the underlying connection in
    a bad state that a bare retry against the same client wouldn't recover
    from.

    Everything past the first cache check is serialized behind
    _GCE_DISK_FETCH_LOCK -- see that lock's own comment for why (this is the
    same real constraint/incident class get_price() already guards against
    via _PRICE_FETCH_LOCK, not a new concern).
    """
    key = (project, zone, disk_name)
    now = time.monotonic()
    with _DISK_INFO_CACHE_LOCK:
        cached = _DISK_INFO_CACHE.get(key)
        if cached is not None and now - cached[0] < ttl_seconds:
            return cached[1]

    with _GCE_DISK_FETCH_LOCK:
        # re-check: another thread may have already fetched (and cached)
        # this exact disk while we were waiting on the lock.
        with _DISK_INFO_CACHE_LOCK:
            cached = _DISK_INFO_CACHE.get(key)
            if cached is not None and now - cached[0] < ttl_seconds:
                return cached[1]

        result = (None, None)
        last_exc = None
        for attempt in range(1, _MAX_FETCH_ATTEMPTS + 1):
            try:
                client = get_gce_disk_client()
                disk = client.disks().get(project = project, zone = zone, disk = disk_name).execute()
                result = (float(disk["sizeGb"]), _parse_gce_disk_type(disk.get("type")))
                break
            except _TRANSIENT_FETCH_ERRORS as e:
                last_exc = e
                canine_logging.warning(
                  "Transient error fetching live disk info for {}/{}/{} (attempt {}/{}): {} -- retrying with a fresh client".format(
                    project, zone, disk_name, attempt, _MAX_FETCH_ATTEMPTS, e))
                _invalidate_gce_disk_client()
                if attempt < _MAX_FETCH_ATTEMPTS:
                    time.sleep(attempt)
            except Exception as e:
                canine_logging.warning("Could not fetch live disk info for {}/{}/{}: {}".format(project, zone, disk_name, e))
                break
        else:
            canine_logging.warning(
              "Could not fetch live disk info for {}/{}/{} after {} attempts: {}".format(
                project, zone, disk_name, _MAX_FETCH_ATTEMPTS, last_exc))

        with _DISK_INFO_CACHE_LOCK:
            _DISK_INFO_CACHE[key] = (now, result)
        return result


_MACHINE_TYPE_CAPACITY_CACHE = {}
_MACHINE_TYPE_CAPACITY_CACHE_LOCK = threading.Lock()


def get_machine_type_capacity(machine_type, zone, project):
    """
    Live (vcpus, memory_mb) for an arbitrary GCE machine type, via the Compute
    Engine API's machineTypes().get() -- the fallback _resolve_machine_type_capacity()
    uses when a machine type isn't in this cluster's own nodetypes.json (e.g.
    wolF's controller VM, which is provisioned out-of-band and is never
    itself a SLURM worker partition type).

    Cached in-process indefinitely, unlike get_live_disk_info()'s short TTL:
    a machine type's core/memory spec in a given zone is a fixed catalog
    fact, not something that changes over a node's lifetime the way a disk's
    size can.

    Returns (None, None) on any failure -- never guesses a fallback capacity.
    Shares get_live_disk_info()'s client/retry/locking machinery
    (get_gce_disk_client() is the same 'compute' v1 client; machineTypes()
    and disks() are both methods on it, so reusing it needs no new client
    singleton) for the same underlying reason: concurrent .execute() calls on
    the shared httplib2-backed client corrupt its connection state.
    """
    key = (project, zone, machine_type)
    with _MACHINE_TYPE_CAPACITY_CACHE_LOCK:
        if key in _MACHINE_TYPE_CAPACITY_CACHE:
            return _MACHINE_TYPE_CAPACITY_CACHE[key]

    with _GCE_DISK_FETCH_LOCK:
        with _MACHINE_TYPE_CAPACITY_CACHE_LOCK:
            if key in _MACHINE_TYPE_CAPACITY_CACHE:
                return _MACHINE_TYPE_CAPACITY_CACHE[key]

        result = (None, None)
        last_exc = None
        for attempt in range(1, _MAX_FETCH_ATTEMPTS + 1):
            try:
                client = get_gce_disk_client()
                info = client.machineTypes().get(project = project, zone = zone, machineType = machine_type).execute()
                result = (int(info["guestCpus"]), float(info["memoryMb"]))
                break
            except _TRANSIENT_FETCH_ERRORS as e:
                last_exc = e
                canine_logging.warning(
                  "Transient error fetching machine type capacity for {}/{}/{} (attempt {}/{}): {} -- retrying with a fresh client".format(
                    project, zone, machine_type, attempt, _MAX_FETCH_ATTEMPTS, e))
                _invalidate_gce_disk_client()
                if attempt < _MAX_FETCH_ATTEMPTS:
                    time.sleep(attempt)
            except Exception as e:
                canine_logging.warning("Could not fetch machine type capacity for {}/{}/{}: {}".format(project, zone, machine_type, e))
                break
        else:
            canine_logging.warning(
              "Could not fetch machine type capacity for {}/{}/{} after {} attempts: {}".format(
                project, zone, machine_type, _MAX_FETCH_ATTEMPTS, last_exc))

        with _MACHINE_TYPE_CAPACITY_CACHE_LOCK:
            _MACHINE_TYPE_CAPACITY_CACHE[key] = result
        return result


# slurm_gcp_docker's slurm_resume.py's own hardcoded boot-disk provisioning
# defaults (disk_size = "25GB", bumped to "50GB" for a GPU node; no
# --boot-disk-type flag is passed, so the GCE image's own default type
# applies -- pd-standard is assumed here, matching this codebase's existing
# "flag text-matching/format assumptions for live verification" caveat
# style rather than something actually confirmed against a real image).
_STATIC_BOOT_DISK_GB_DEFAULT = 25.0
_STATIC_BOOT_DISK_GB_GPU_DEFAULT = 50.0
_STATIC_BOOT_DISK_TYPE_DEFAULT = "pd-standard"


def make_live_boot_disk_price_source(zone, project, host_lut = None, node_types = None):
    """
    Builds a disk_price_source callable (node_name -> (price_per_hour,
    is_approximate) or None) for a worker node's boot disk, for the
    real-time path -- mirrors make_live_price_source()'s shape (plus the
    is_approximate flag estimate_task_cost() folds into disk_cost_provisional),
    so a future GCSfuse-based cost model needs only a new price-source
    builder with this same signature, no changes to estimate_task_cost()
    itself.

    Assumes gcloud's default naming convention (the boot disk's resource
    name equals its instance's name) -- true for worker nodes as provisioned
    by slurm_gcp_docker's slurm_resume.py, which passes no explicit disk
    name/--create-disk flag, but this is a gcloud CLI convention being relied
    on, not something this codebase asserts anywhere else.

    Falls back to the static provisioning-time default (_STATIC_BOOT_DISK_GB_
    DEFAULT/_GPU_DEFAULT, from slurm_resume.py's own hardcoded values) when
    the live GCE lookup fails, rather than giving up entirely. Confirmed
    live: this is common, not rare -- a task's own cost estimate only runs
    once, after ALL of its shards finish, so an early-finishing shard's node
    can already be reclaimed by SLURM's elastic scaling by the time this
    runs, well before any other, still-running shard's own completion.
    Deliberately a reasonable approximation for exactly that case: a node
    reclaimed quickly is also the node least likely to have ever triggered
    worker_boot_disk_resize.sh's auto-grow (which only fires once free space
    drops below 30%), so its provisioning-time size is usually still
    accurate. Flagged via is_approximate=True either way, so this is never
    mistaken for a precise live measurement -- e.g. a genuinely long-lived,
    heavily-grown node that also happens to 404 for some *other* reason
    would still be marked approximate rather than silently trusted.
    """
    host_lut = load_host_lut() if host_lut is None else host_lut
    node_types = load_node_types() if node_types is None else node_types

    def _static_fallback(node_name):
        has_gpu = False
        if node_name in host_lut.index:
            accel_count = host_lut.loc[node_name].get("accelerator_count")
            has_gpu = bool(accel_count) and not (isinstance(accel_count, float) and pd.isna(accel_count))
        size_gb = _STATIC_BOOT_DISK_GB_GPU_DEFAULT if has_gpu else _STATIC_BOOT_DISK_GB_DEFAULT
        price_per_gb_hour = get_disk_price_per_gb_hour(_STATIC_BOOT_DISK_TYPE_DEFAULT, zone)
        if price_per_gb_hour is None:
            return None
        return price_per_gb_hour * size_gb, True

    def _source(node_name):
        size_gb, disk_type = get_live_disk_info(node_name, zone, project)
        if size_gb is None or disk_type is None:
            return _static_fallback(node_name)
        price_per_gb_hour = get_disk_price_per_gb_hour(disk_type, zone)
        if price_per_gb_hour is None:
            return None
        return price_per_gb_hour * size_gb, False
    return _source


def make_live_disk_gb_hour_price_source(zone):
    """
    disk_type -> $/GB-hour, for standalone per-task disk costs (scratch
    disks) that aren't tied to a node's own capacity/lifetime the way boot
    disks are -- see estimate_scratch_disk_cost().
    """
    def _source(disk_type):
        return get_disk_price_per_gb_hour(disk_type, zone)
    return _source


############ per-task / per-node cost estimation ############

def estimate_task_cost(acct_df, price_source, disk_price_source = None, host_lut = None, node_types = None):
    """
    acct_df: DataFrame shaped like Task.acct -- one row per shard, with an
      "attempts" column holding a list of per-attempt {NodeList, Start, End,
      AllocTRES, CPUTimeRAW, NCPUS, ReqMem} dicts (see
      Orchestrator.wait_for_jobs_to_finish's grouper()). NCPUS/ReqMem are used
      as a per-field fallback (via resolve_job_resources()) when AllocTRES
      doesn't report cpu=/mem= itself -- confirmed live as a real, not
      hypothetical, case on some clusters.
    price_source: callable(node_name) -> $/hour, or None if unavailable. Swap
      make_live_price_source(...) (real-time) for a reconciliation-side,
      billing-grounded per-node lookup to get identical-logic, differently-
      accurate numbers from the same function.
    disk_price_source: optional callable(node_name) -> (price_per_hour,
      is_approximate) or None, so a worker's boot-disk cost can be prorated
      using the same max(cpu_frac, mem_frac) share already computed for
      compute cost. `is_approximate` lets a source like
      make_live_boot_disk_price_source() report a usable (non-None) price
      that's known to be a rough stand-in -- e.g. a static provisioning-time
      default used because the node was already torn down before its live
      size could be read -- without that price being mistaken for a precise
      live measurement. Default None reproduces today's exact behavior/
      columns unchanged -- existing callers are unaffected. When given, adds
      disk_cost_usd/disk_cost_provisional columns, kept DELIBERATELY separate
      from cost_usd/is_provisional/missing_capacity_data so a disk-pricing
      failure can never corrupt or mask the existing, already-relied-upon
      compute-cost signal.

    Returns a DataFrame indexed the same as acct_df, with cost_usd (summed
    across every attempt/node the shard touched), total_running_seconds,
    missing_capacity_data, is_provisional, disk_cost_usd, and
    disk_cost_provisional columns. Never guesses: a shard with any attempt on
    an unrecognized node, with unparseable AllocTRES, or missing a price, is
    flagged rather than silently under-costed.

    total_running_seconds is the sum of every attempt's own (End - Start)
    duration -- i.e. actual time spent running, across every preemption/
    requeue attempt -- computed independently of whether pricing/capacity
    lookup succeeded for a given attempt (a job's real runtime is a fact
    about what sacct observed, not about whether canine.cost happens to know
    that node's price). This is deliberately distinct from acct_df's own
    "Elapsed" column, which (per grouper()) reflects only the single attempt
    picked as the group's "final" row -- for a heavily-preempted job, that
    can look deceptively small (e.g. the last, quick, successful attempt)
    next to a cost/n_preempted that correctly reflects every attempt.
    Confirmed live: a job preempted 47 times showed Elapsed=24s (the final
    attempt alone) sitting next to a correctly-computed but easy-to-doubt
    small cost_usd, with no way to see that ~497s across all 48 attempts is
    what actually produced it.
    """
    host_lut = load_host_lut() if host_lut is None else host_lut
    node_types = load_node_types() if node_types is None else node_types

    rows = []
    for jid, row in acct_df.iterrows():
        attempts = row.get("attempts") or []
        total_cost = 0.0
        total_disk_cost = 0.0
        total_running_seconds = 0.0
        missing_capacity_data = len(attempts) == 0
        is_provisional = False
        # No attempt ever gets a chance to clear this when disk pricing
        # wasn't requested at all -- $0 there means "not priced", not "free".
        disk_cost_provisional = disk_price_source is None

        for attempt in attempts:
            node_name = attempt.get("NodeList")

            start = _parse_sacct_time(attempt.get("Start"))
            end = _parse_sacct_time(attempt.get("End"))
            if not pd.isna(start) and not pd.isna(end):
                total_running_seconds += max(0.0, (end - start).total_seconds())

            vcpus, mem_mb = node_capacity(node_name, host_lut, node_types)
            if vcpus is None:
                missing_capacity_data = True
                continue

            alloc_cpus, alloc_mem_mb = resolve_job_resources(
              attempt.get("AllocTRES"), attempt.get("NCPUS"), attempt.get("ReqMem"),
            )
            if alloc_cpus is None or alloc_mem_mb is None:
                missing_capacity_data = True
                continue

            if pd.isna(start) or pd.isna(end):
                missing_capacity_data = True
                continue

            # Computed once, up front, so disk pricing (below) can reuse the
            # same proration share as compute pricing even on an attempt
            # where compute pricing itself is unavailable -- the two are
            # priced independently from this point on.
            elapsed_seconds = max(0.0, (end - start).total_seconds())
            cpu_frac = alloc_cpus / vcpus
            mem_frac = alloc_mem_mb / mem_mb
            resource_frac = max(cpu_frac, mem_frac)

            price_per_hour = price_source(node_name)
            if price_per_hour is None:
                is_provisional = True
            else:
                total_cost += (price_per_hour / 3600) * elapsed_seconds * resource_frac

            if disk_price_source is not None:
                disk_price = disk_price_source(node_name)
                if disk_price is None:
                    disk_cost_provisional = True
                else:
                    disk_price_per_hour, disk_price_is_approximate = disk_price
                    total_disk_cost += (disk_price_per_hour / 3600) * elapsed_seconds * resource_frac
                    if disk_price_is_approximate:
                        disk_cost_provisional = True

        rows.append({
          "JobID": jid, "cost_usd": total_cost, "total_running_seconds": total_running_seconds,
          "missing_capacity_data": missing_capacity_data, "is_provisional": is_provisional,
          "disk_cost_usd": total_disk_cost, "disk_cost_provisional": disk_cost_provisional,
        })

    return pd.DataFrame(rows).set_index("JobID")


############ standalone disk / infrastructure cost helpers (not derived from acct_df) ############

def estimate_scratch_disk_cost(size_gb, disk_type, duration_seconds, price_source):
    """
    Cost of a per-task scratch/persistent disk (Task's use_scratch_disk):
    unlike a worker's boot disk, this disk is dedicated to one shard, not
    shared with other jobs on the same node -- so it's priced for its full
    size over its full duration, with no cpu/mem-fraction proration.

    price_source: callable(disk_type) -> $/GB-hour, or None if unavailable
    (see make_live_disk_gb_hour_price_source) -- deliberately a
    disk_type-only interface (not node-based, like boot-disk pricing), and
    the intended seam for a future bucket-mount-backed scratch "disk": swap
    in a GCS-storage-equivalent $/GB-hour price_source with the same
    signature, no change needed here.

    Returns (cost_usd, is_provisional). Never guesses: returns (0.0, True)
    if size_gb/duration_seconds aren't known or pricing isn't available.
    """
    if not size_gb or duration_seconds is None or duration_seconds < 0:
        return 0.0, True

    normalized_type = _normalize_disk_type(disk_type)
    if normalized_type is None:
        return 0.0, True

    price_per_gb_hour = price_source(normalized_type)
    if price_per_gb_hour is None:
        return 0.0, True

    return price_per_gb_hour * size_gb * (duration_seconds / 3600), False


def estimate_window_cost_usd(price_per_hour, window_start, window_end):
    """
    Generic elapsed-hours x price helper for costs that aren't derived from
    acct_df at all -- used for controller-VM and shared-storage overhead,
    which are priced over a run's own wall-clock window rather than any
    job's own [Start, End]. Returns None (never a guess) if price_per_hour
    is None or either timestamp is missing.
    """
    if price_per_hour is None or window_start is None or window_end is None:
        return None
    window_start = pd.Timestamp(window_start)
    window_end = pd.Timestamp(window_end)
    elapsed_hours = max(0.0, (window_end - window_start).total_seconds() / 3600)
    return price_per_hour * elapsed_hours


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
            alloc_cpus, alloc_mem_mb = resolve_job_resources(
              job.get("AllocTRES"), job.get("NCPUS"), job.get("ReqMem"),
            )
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
