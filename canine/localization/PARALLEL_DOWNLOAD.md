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
| `download_connections` | `localizer_args` | `16` | ranged GETs in flight per input |
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

**The default is 16, and it is measured.** It was 8 — one per vCPU, which was never the
right unit, since the streams are IO-blocked rather than CPU-bound. Against the real GDC
source, throughput is linear in connections all the way to 16 with no knee at the source
at all:

| connections | 1 | 4 | 8 | 12 | 16 |
|---|---|---|---|---|---|
| MiB/s | 16.42 | 65.0 | 129.4 | 180.6 | **227.18** |

That is **13.85× over a single stream**, and the limit is per *connection* — not per
signed URL, not per endpoint. 16 is also `MAX_CONNECTIONS`, so the default is now the cap.

**Raising it above 16 is not the lever you want, and on most inputs neither is lowering
it.** What binds is almost always the destination:

* **`LocalizeToDisk` (the common case).** The localization disk is a `pd-standard` sized
  to the payload plus 5%, and `pd-standard` throughput is provisioned per gigabyte. For a
  300 GB object that is a 316 GB disk sustaining **43.9 MiB/s** — measured with plain `dd`
  and `O_DIRECT`, with the downloader coming within 0.8% of it. Eight connections already
  exceed that, so on this path the connection count is not what decides your runtime and
  tuning it will not move the number. See `BENCHMARK_RUNBOOK.md` §6.5i.
* **Fast destinations** — NFS, tmpfs, the bucket route — are where 16 earns its keep,
  because there the source is what you are asking for more.

**Beware short benchmarks when checking any of this.** That disk delivers 92.1 MB/s for
its first ~56 GiB and 46.0 MB/s thereafter — an exact 2× burst. Any test under ~64 GiB
reports roughly double the sustained rate, and four sections of the runbook concluded
there was a defect in this downloader on exactly that mistake. If you measure, measure
past 96 GiB, and treat two consecutive stages agreeing as the bar.

Lower it if a source throttles per-connection, or to be a quieter neighbour when many
workers pull from the same endpoint at once. Those are the two reasons left.

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

Concurrency and bookkeeping, at the end of the download phase:

```
k9pdl-streams mean 14.82 of 16 workers (3283 chunks, 5748.2s wall, 85189.4s streaming)
k9pdl-bookkeeping 2.1s over 3283 calls (mean 0.001s, 0.0% of 91971.2 worker-seconds)
k9pdl-commit 94.7s over 212 batches (3283 chunks, mean batch 15.5, 1.6% of 5748.2s wall)
```

* **streams** — how many requests were really receiving bytes at once. `mean` near 1 with
  many chunks left means the requests were not concurrent, which is a defect, not a slow
  source.
* **bookkeeping** — the share of the *worker pool* spent on the worker side of chunk
  completion. This should be near zero. It was 70% before the manifest commit moved to its
  own thread, and that alone held the full-size run to half the disk's floor.
* **commit** — the manifest writer, which runs *off* the worker pool, so its share is of
  the wall clock. **`mean batch` is the number to look at.** At 1.0 the writer is being
  drained as fast as it is filled, nothing was amortised, and the commits are still
  effectively per-chunk — a state that looks identical to a healthy run if you only read
  the throughput.

Decompression, when the source arrived `Content-Encoding: gzip`:

```
k9pdl-phase gunzip 291.7s, 287.4 MB/s
decompressed 3060000000 bytes into 9884127232 bytes
```

This runs on the in-place and bucket-compose routes, always **after** verification — the
advertised digest covers the *compressed* bytes, so they have to be checked before
anything is decoded, and the decompressed output consequently has no digest of its own.
stage-publish copies its staged bytes through unchanged and refuses `--gunzip` outright
rather than storing a gzip stream under a name promising plain content.

Two log lines mean the decode was deliberately skipped and the stored bytes kept, both
because the server's metadata was wrong rather than the data: `is not gzip` (the object is
not a gzip stream at all) and `singly-compressed` (the name promises `.gz` and decoding
would have produced something that is not). Neither is a failure.

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

The done marker records two lengths when `--gunzip` is in play. `size` is what was
transferred and is what identifies the plan; `stored_size` is what actually landed on the
destination. The marker is only believed if the destination still matches `stored_size`,
so without the second field the check would compare a decompressed file against a
compressed length, disagree every time, and re-download on every run.

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
