# Parallel chunked localization — operator's guide

Localization downloads used to be a single `curl` or `aws s3api get-object` per input.
They are now N simultaneous ranged GETs of even-sized chunks, reassembled byte-identically
and **resumable across VM preemption** (`parallel_download.py`).

This is the operational surface: how to turn it off, how to tune it, and how to tell what
it did. For the design and its rationale see `update_localization.md`; for measuring it,
`test/BENCHMARK_RUNBOOK.md`.

---

## Turning it off

Three levels, coarsest first. All of them fall back to the same single-stream command the
old code used, with the same resume semantics — so switching off is always safe, never a
correctness change.

### 1. Per node, at runtime — the kill switch

```bash
export CANINE_DISABLE_PARALLEL_DOWNLOAD=1
```

Set anywhere the job's environment reaches (the worker's shell profile, a SLURM
`--export`, the controller before submitting). Any non-empty value counts. Each download
logs why it declined:

```
falling back to a single stream: CANINE_DISABLE_PARALLEL_DOWNLOAD is set
```

This is the switch to reach for if parallel downloading is implicated in an incident. It
needs no redeploy and no code change, and it takes effect on the next job.

### 2. Per localizer, in Python

```python
canine.Orchestrator(..., localizer_args = {"parallel_download": False})
```

Emits the legacy single-stream commands directly, so the downloader is never invoked.

### 3. Per input

Any input whose size is unknown, whose URL is `ftp://`, or which resolves to
`connections <= 1` takes the single stream automatically. Nothing to configure.

---

## Tuning

| Knob | Where | Default | Effect |
|---|---|---|---|
| `download_connections` | `localizer_args` | `8` | ranged GETs in flight per input |
| `download_min_chunk` | `localizer_args` | 64 MiB | chunk size |
| `CANINE_DOWNLOAD_CONNECTIONS` | environment | — | overrides the connection count per node |

```python
canine.Orchestrator(..., localizer_args = {
    "download_connections": 12,
    "download_min_chunk": 128 * 1024 * 1024,
})
```

```bash
export CANINE_DOWNLOAD_CONNECTIONS=4      # per node, no redeploy
```

An unparseable value is ignored with a log line rather than failing the job:

```
ignoring unparseable CANINE_DOWNLOAD_CONNECTIONS='four'
```

**`download_min_chunk` is deliberately not overridable from the environment.** The chunk
size feeds `plan_id`, which identifies a partial download; changing it between attempts
would make a requeued task discard a good partial file and start over. `connections` is
safe to vary per node precisely because the chunk layout does *not* depend on it.

### Choosing a connection count

8 is one per vCPU on the `n1-standard-8` that `LocalizeToDisk` requests, but the streams
are IO-blocked rather than CPU-bound, so 12–16 may do better. `BENCHMARK_RUNBOOK.md` §6.1
finds the knee empirically; until that has been run against your sources, 8 is a
conservative default rather than a measured one.

Lower it if a source throttles per-connection, or to be a quieter neighbour when many
workers pull from the same endpoint at once.

---

## Reading the logs

Progress, roughly every few seconds:

```
42.1% (1288490188/3060000000 bytes, 1.2 GiB transferred)
```

`transferred` is what this attempt actually fetched, which after a preemption is less than
the total — that difference is the resume working.

Phase timings, machine-readable:

```
k9pdl-phase download 3612.4s, 83.1 MB/s
k9pdl-phase verify 612.8s, 489.9 MB/s
```

Localization is two costs: moving the bytes, then re-reading them to hash. On the in-place
route `verify` is a **full re-read of the object**, so a large verify share is expected and
worth watching — it decides whether the node's cores are busy and whether hashing during
the transfer is worth building.

Resume, on a second attempt:

```
resuming: 47/71 chunks already complete
```

Completion:

```
complete: 3060000000 bytes (verified)
```

`(verified)` means a hash was supplied and matched. Its absence means no hash was
available — not that verification failed, which is always a hard error.

---

## Exit codes

The contract canine's entrypoint reads. Unchanged from the rest of canine:

| Code | Meaning |
|---|---|
| `0` | complete, and verified if a hash was available |
| `5` | requeue and resume — a transient failure *after* bytes moved. Excluded from `CANINE_PREEMPT_LIMIT` |
| `1` | do not retry: a hash mismatch, a permanent HTTP error, or no forward progress at all |

The distinction between 5 and 1 is deliberate. Exit 5 is for "this will work if tried
again", and because those requeues do not count against the preemption limit, it must not
be returned when nothing was accomplished — a server that will never answer would
otherwise loop forever. No forward progress therefore exits 1.

---

## What it leaves behind

Beside each destination, while a download is in flight:

| File | Purpose |
|---|---|
| `.<name>.k9pdl.json` | chunk manifest — resume state |
| `.<name>.k9pdl.done` | completion marker, with size, plan id and digest |
| `<name>.k9pdl.gz` | compressed sidecar, only with `--gunzip` |

The manifest is removed on success; the done marker is kept, and is what makes a re-run of
`localization.sh` after a preemption a no-op instead of a re-download. On the bucket-compose route (a
bucket destination) the manifest is an object in the bucket rather than a file on the
mount, because a flat-namespace bucket has no atomic rename.

Stale sidecars from an abandoned attempt are harmless: a manifest whose `plan_id` does not
match the current layout is discarded and the download restarts.

---

## If something looks wrong

1. **Set `CANINE_DISABLE_PARALLEL_DOWNLOAD=1`.** Behaviour returns to the old
   single-stream path immediately, with the old resume semantics.
2. **Check for a hash mismatch** — `md5 mismatch` or `ETag mismatch` in the log, with
   exit 1. That is the downloader refusing to publish bytes it could not confirm, and it
   is working as intended; the question is why the source disagrees.
3. **Check whether it fell back on its own.** `falling back to a single stream: <reason>`
   names the reason — unknown size, ftp, no room to stage, or a destination filesystem
   that cannot support in-place writes.
4. **Check the frontier line.** `SEEK_HOLE unsupported here; checkpointing instead of
   frontier recovery` means resume granularity is the checkpoint interval rather than the
   exact byte, so a preemption costs a little more re-fetching. Correct, just less
   efficient.
