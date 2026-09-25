# Plan: Parallel chunked localization for curl-backed and S3-backed downloads

## Goal
Replace single-stream `curl` / `aws s3api get-object` localization with N simultaneous
ranged GETs of even-sized chunks, reassembled byte-identically into the original file,
with concurrency/chunking tuned for **GCP n1-standard-8** worker VMs.

**Hard requirement: downloads must remain resumable across VM preemption.** Workers are
preemptible by default (`backends/gcpTransient.py:51`, `imageTransient.py:77`), so any
download may be killed at any byte and re-run later — possibly on a different VM. Today
this is provided by `curl -C -` and by `HandleAWSURL`'s `SZ=$(stat ...)` append-resume.
The new implementation must be **at least as resumable**, never weaker. See §4.

---

## 1. Current state (canine/canine/localization/file_handlers.py)

All download commands are emitted as *bash strings* by `FileType.localization_command(dest)`.
They are (a) concatenated into the per-job `localization.sh` run on the worker VM
(`base.py:1141-1161`, `base.py:1357`), and (b) executed directly via `subprocess` on the
controller for deduplicated "common" inputs (`nfs.py:62-64`, `local.py`, `remote.py`).

| Handler | Line | Current download | In scope |
|---|---|---|---|
| `HandleAWSURL` | 402 | `aws s3api get-object --range "bytes=$SZ-"` (single stream, append-resume) | **yes** |
| `HandleGDCHTTPURL` | 532 | `curl -C - -o` (+ `X-Auth-Token` header) | **yes** |
| `HandleDRSURI` | 678 | `curl -C - -o "$signed_url"` | **yes** |
| `HandleGCSSignedURL` | 758 | `curl -C - -o` | **yes** |
| `HandleOtherURL` | 793 | `curl -C - -o` (http/https/ftp) | **yes** (https/http only; ftp falls back) |
| `HandleGSURL` | 254 | `gcloud storage cp` | no — already does sliced downloads; only tune (§6) |
| `*Stream` variants | 263/451/550/696 | pipe into FIFO | no — inherently sequential |
| `HandleRODISKURL`, `HandleRegularFile` | 873/805 | disk mount / local copy | no |

Key facts that constrain the design:
* Commands must remain **shell strings** — no Python objects cross to the VM.
* They are joined into one `localization.sh` that runs **on the remote SLURM compute node**
  as a subprocess of the entrypoint (`orchestrator.py:70`), under `set -e`
  (`base.py:1360`). Nothing about the host process is available there — only what is
  written into the string and what the entrypoint exports.
* Because of `set -e`, every emitted line must either succeed or be explicitly guarded
  with `|| :` (the existing style, e.g. `base.py:1157`). A bare failing `[ ... ]` test as
  the last command of a line aborts the whole localization.
* `CANINE_ROOT` **is** exported by the entrypoint before `localization.sh` runs
  (`orchestrator.py:50`), so a script staged there is addressable on the compute node —
  this is exactly how `delocalization.py` is invoked from the teardown script
  (`base.py:1385`).
* `python3` **is** available on cluster nodes — the existing commands already rely on it
  unconditionally (`file_handlers.py:425` runs a `python3` heredoc, `:686` a `python3 -c`).
  No interpreter-detection guard is needed.
* The existing `localization_command` implementations are **known-good on cluster nodes**.
  New versions must inherit their construction and idioms verbatim (§6.1) and change only
  the download line — this is a surgical swap, not a rewrite of the surrounding shell.
* `debug.sh` regenerates a runnable script with `grep -v '#DEBUG_OMIT'` and
  `sed -e '$s/ - <<.*$//'` (`localization/debug.sh:24,38`). So emitted lines must not
  contain the literal `#DEBUG_OMIT`, and **heredocs are fragile** — a staged file is
  strongly preferred over an inline `<<CODE` block. (Canine does already emit one heredoc in a
  localization script, `base.py:880-894`, so the constraint is narrower than "never" — see
  §12.5 for the precise rule.)
* Downloads within a job are **sequential** — each handler appends its own lines to the one
  script. So a single download owns the whole node budget (§2). Parallelising *across*
  files is out of scope: it would break `set -e` error propagation.
* The same command string is also executed **on the controller** via
  `subprocess.check_call` for deduplicated common inputs (`nfs.py:62-64`), where
  `CANINE_ROOT` may be unset or may point at a staging dir that has **not been populated
  yet** — `pick_common_inputs` runs *before* the `shutil.copyfile` staging step in
  `nfs.py`. The script path must therefore be resolved by "first path that exists"
  (§7), not by a bare `${CANINE_ROOT:-...}` default.
* Exit codes from `localization.sh` are interpreted by the entrypoint
  (`orchestrator.py:107-120`): **5** ⇒ log, increment `.localization_failure_count`,
  `scontrol requeue`; **15** ⇒ skip job; **anything else nonzero** ⇒
  `LOCALIZATION FAILURE! JOB CANNOT RUN!` and a **DNR** (do-not-retry) marker. A transient
  network error must therefore never escape as a generic nonzero code.
* Retries re-run `localization.sh` **in full**, so every emitted command must be idempotent.
* Every handler already knows `self.size` **host-side** before command generation, so the
  chunk plan is computed on the host and baked into the command — no extra HEAD request on
  the compute node, and the plan is identical across preemption retries.
* `HandleAWSURL` already extracts `PartsCount`/`PartLength` (line 380-395) for multipart
  ETag verification — chunk boundaries must respect this (see §5).
* The worker image is not guaranteed to have `aria2c`/`axel`; it *does* have `curl`, `aws`
  and `gcloud` (already relied on).
* Where the partially-downloaded bytes live on preemption, and whether they survive.
  **The common case is a GCP persistent disk, not NFS** — `wolF/wolf/localization.py`'s
  `LocalizeToDisk` (aliased `BatchLocalDisk`) passes
  `extra_localization_args = {"localize_to_persistent_disk": True}` and its `script` is a
  noop `":"`, i.e. the download *is* the entire job:
  * **Persistent localization disk** (`localize_to_persistent_disk`, the dominant path):
    survives preemption. Canine re-attaches and explicitly resumes it ("Resuming creation of
    persistent disk", `base.py:796`), and the `finished=yes` label — the real completion
    signal — is only applied after the whole localization script succeeds (`base.py:1365`),
    so a preempted disk correctly re-enters localization. The disk is attached read-write to
    **one** VM at a time; canine exits 5 if another instance holds it (`base.py:828-830`).
    Afterwards it is relabelled and mounted **read-only** as a RODISK by downstream tasks.
  * **NFS staging dir**: also supported (plain `url`-mode inputs without a disk), and
    likewise survives — a requeued task on any VM sees the partial file.
  * **Ephemeral local disk** (`CANINE_LOCAL_DISK_DIR`, `base.py:1065`): destroyed with the
    VM — resume is impossible and the downloader must cleanly restart from zero.
  * **A gcsfuse-mounted bucket standing in for the NFS share** (anticipated future change):
    a GCS object store presented as a POSIX filesystem. It survives preemption trivially, but
    it violates almost every filesystem assumption the chunked writer relies on, so it is
    handled by an explicit capability gate (§4.7). It is not merely a degraded case: because
    GCS `compose` concatenates server-side with **no data transfer**, and a bucket imposes no
    size constraint, the bucket route is in some respects *better* than the PD route — there
    is no sequential assembly step at all.
  The design must be correct on all four; it may not assume NFS semantics anywhere, and it
  must **not silently assume POSIX semantics either** — see §4.7.
* `exit 5` is canine's existing "requeue this task" signal (`base.py:830`).

---

## 2. n1-standard-8 tuning rationale

**The download owns the whole node — by explicit configuration.** `LocalizeToDisk`
(`wolF/wolf/localization.py:34-36`) hardcodes
`resources = {"partition": "n1-standard-8", "cpus-per-task": 8, "mem": "28200M"}` — an exact
match for the `n1-standard-8` entry in `slurm_gcp_docker/conf/nodetypes.json`
(`"cpus": "8", "realmemory": "28200"`) — *and* passes
`extra_slurm_args = {"exclusive": None}` (`--exclusive`). Its task `script` is a noop `":"`,
so localization is the entire workload. Nothing competes for NIC, PD, CPU or RAM, and the
full machine budget can be claimed unconditionally: no runtime negotiation, no per-node
semaphore, no scaling by tasks-per-node. The `--exclusive` flag also makes this independent
of the partition's `OverSubscribe=YES:10` setting (`slurm_gcp_docker/conf/slurm.conf:166`).

| Resource | n1-standard-8 | Implication |
|---|---|---|
| vCPUs | 8 (all ours) | TLS + checksumming never contend with the job |
| Memory | 28.2 GB declared (all ours) | buffers are a rounding error; sized for locality, not scarcity |
| Egress cap | 2 Gbps/vCPU, capped 16 Gbps (~2 GB/s) | a *single* TCP stream to S3/GDC realistically gets 50–200 MB/s ⇒ 8–16 streams needed to approach the cap. This is the whole reason for the change |
| Boot/PD disk | `pd-standard` created by `create_persistent_disk`; throughput scales with provisioned size | beyond ~16 streams the disk, not the network, becomes the bottleneck. For `LocalizeToDisk` the target is this PD, so **PD write throughput is the more likely limiter than the NIC** — the connection sweep in §8 must measure both |

**Derived defaults:**
* `PDL_CONNECTIONS = 8` (one per vCPU), hard max 16. Note the streams are **IO-blocked**,
  not CPU-bound, so 12–16 may saturate the NIC better than 8 — the benchmark (§8) picks the
  final default; the knob exists either way.
* `PDL_MIN_CHUNK = 64 MiB` — below this, per-request setup/TLS overhead dominates.
* `n_chunks = clamp(ceil(size / PDL_MIN_CHUNK), 1, connections)`; chunk size =
  `ceil(size / n_chunks)` so all chunks are **even-sized** except a possibly-shorter tail.
* Files `< 64 MiB` ⇒ 1 chunk ⇒ behaviour identical to today (no regression for small inputs).
* Read buffer 8 MiB/stream; write via `os.pwrite` into one **sparse** file (`ftruncate`,
  not `posix_fallocate` — see §4.1) ⇒ no `cat`-merge pass, no 2× disk space, no 2× disk I/O.
  This matters more than it looks: the localization disk is sized as `sum(input sizes)` with
  only a **5% margin** (`base.py:727-731`), so any design needing part files plus a merge
  (peak ≈ 2× size) simply would not fit. Writing in place is the only option that does.
* Multiple inputs in one job are downloaded **sequentially** (one `localization.sh`, §1), so
  each in turn gets the full connection budget — no need to divide it across inputs.

**Caveat worth knowing (not designed around):** the partition is configured
`OverSubscribe=YES:10` (`slurm_gcp_docker/conf/slurm.conf:166`) and wolF's default task
resources are `cpus-per-task: 1, mem: 1G` (`wolF/wolf/task.py:324`), so a task that does
*not* size itself to the node can still be co-scheduled. That is not how these pipelines are
run, so the design assumes exclusivity — but `download_connections` is exposed as a
per-task option (§6) so an intentionally-shared workload can dial it down, and the
`flock` used for concurrent-writer safety (§4.5) is on the *manifest*, which protects
correctness in that case regardless.

---

## 3. New component: `canine/localization/parallel_download.py`

A **stdlib-only** (`urllib`, `os`, `threading`, `hashlib`, `subprocess`) Python 3 script,
runnable both as `python3 <path>/parallel_download.py` and `python3 -m
canine.localization.parallel_download`. Stdlib-only so it works inside the slurm_gcp_docker
worker image without adding dependencies.

CLI:
```
parallel_download.py --url URL --dest PATH --size N
                     [--connections 8] [--min-chunk 67108864]
                     [--header 'K: V']...            # GDC token, etc.
                     [--s3-bucket B --s3-key K --s3-extra-args '...']
                     [--check-md5 HASH | --check-etag ETAG --part-length N]
                     [--url-refresh-cmd '<shell>']
                     [--checkpoint-interval 67108864] [--no-resume]
                     [--retries 5] [--timeout 60]
```

Algorithm:
1. **Probe** — `HEAD` (or the `--size` passed in from the host-side handler, which every
   handler already has), then a **mandatory `Range: bytes=0-0` probe requiring `206` + a
   matching `Content-Range`** before any parallel work (§12.6 — a host-side size is *not*
   sufficient evidence that ranges are honoured, and a server that ignores `Range` returns the
   whole object). If the probe returns `200` / `Accept-Ranges` is absent / size unknown /
   scheme is `ftp:` ⇒ abort the connection and **fall back to a single stream** (identical to
   today's behaviour).
2. **Gate on the destination filesystem** (§4.7) and pick a route. POSIX allowlist + probes
   pass ⇒ **the in-place route**, write chunks directly into `dest` (unchanged; the dominant PD path).
   A gcsfuse mount whose `gs://` URL resolves ⇒ **the bucket-compose route**, upload each chunk as its own
   object via a resumable upload session and `compose` them server-side — no local staging and
   no concatenation transfer. Any other non-POSIX destination ⇒ **the stage-publish route**,
   stage-then-publish via a block-device work directory.
3. **Create the working file sparse** (Routes A/C only) with `ftruncate` to the final size —
   deliberately **not** `posix_fallocate`, so that written and unwritten regions stay
   distinguishable (§4.1). Probe `SEEK_HOLE` support on that filesystem. The bucket-compose route has no local
   file: its durable frontier comes from the resumable upload sessions instead (§4.7).
4. **Plan** even chunks per §2; write the immutable plan once to a sidecar manifest
   `.<basename>.k9pdl.json` **next to the working file** (§4.6) — same filesystem as the data,
   whether that is the localization PD, the NFS share, the staging directory, or (the bucket-compose route) an
   object next to `dest` (§4.7). The manifest records the *plan*, not progress (§4.1) — plus,
   on the bucket-compose route, the per-chunk resumable-upload session URIs, which must be persisted **before**
   any bytes are sent.
5. **Download** with a `ThreadPoolExecutor(connections)`; each worker issues
   `Range: bytes=start-end`, verifies the response is `206` and that
   `Content-Range`/`Content-Length` match the request exactly, and streams 8 MiB buffers
   **strictly sequentially within its chunk** — via `os.pwrite(fd, buf, offset)` on Routes A/C,
   so the chunk's durable frontier is recoverable via `SEEK_HOLE` (§4.1), or into the chunk's
   resumable upload session on the bucket-compose route, whose durable frontier is queryable from GCS. No
   periodic fsync.
6. **Retry** per chunk with exponential backoff + jitter (5 attempts) on
   connection reset / 5xx / 429 / short read, resuming from the chunk's current frontier —
   never from the chunk start. On the bucket-compose route the frontier is re-read from the session
   (`PUT` + `Content-Range: bytes */SIZE` ⇒ `308` + `Range`) before each attempt. Signed URLs can expire mid-transfer ⇒ support
   `--url-refresh-cmd '<shell>'` so DRS/GDC re-mint a signed URL on 403 and resume in place.
7. **Verify** final size == expected; then, **if `check_hash` is set, verify the content hash
   unconditionally** by the best method available for the route (§4.7) — falling back to a full
   read of the file rather than skipping. On mismatch *or on inability to verify*, delete
   `dest` + manifest and exit 1 (matches current semantics at `file_handlers.py:444`).
8. **Assemble** (non-POSIX routes only): the bucket-compose route issues `compose` (≤32 sources per call,
   tree-composing beyond that) with `deleteSourceObjects: true` and checks the returned
   CRC32C against the combined part CRC32Cs; the stage-publish route makes a single sequential streaming copy
   to `dest` and re-verifies size after `close()` (§4.7).
9. **Finalize** by writing the `.k9pdl.done` marker (§4), then removing the manifest.
   Progress logged to stderr at ≤1 line/5 s.

Concurrency safety: single `fd` opened `O_RDWR`; `pwrite` is atomic w.r.t. offset and
requires no lock. Disjoint ranges ⇒ no interleaving.

---

## 4. Preemption resumability (hard requirement)

A GCE preemption may give an ACPI shutdown signal and ~30 s of warning — **or it may not**.
The VM can vanish between any two instructions, and data sitting in the page cache at that
moment is lost even though `pwrite` returned success.

**Design rule: correctness may never depend on catching a signal, running a cleanup path, or
flushing at shutdown.** The on-disk state must be recoverable after a hard kill at *any*
instant, including mid-`write`, mid-`fsync`, and mid-manifest-update. Signal handling is
included only as a best-effort latency optimisation (§4.2) and the implementation must be
correct if it never runs.

### 4.1 Crash-consistency by construction — zero discarded work

**Why `curl -C -` loses nothing today, and how to keep that.** `curl` never records progress
anywhere; it appends to the file and resumes from the file's own size. On ext4 with the
default `data=ordered` journaling, data blocks are written **before** the metadata (size)
update is committed, so after a crash *the file size never exceeds durable data*. The resume
point is therefore always valid, and the only bytes re-fetched are those the filesystem had
not yet committed. **No committed work is ever discarded.** An explicit checkpoint interval —
my earlier 64 MiB/chunk design — would throw away up to 512 MiB of *already-durable* data
across 8 streams. That is a regression and is rejected.

**Fix: derive durable progress from the file itself, exactly as `curl` does; never store a
progress counter.**

* `dest` is created **sparse** with `ftruncate` to the final size — **not**
  `posix_fallocate`, whose *unwritten extents* are indistinguishable from written data.
* Each chunk is written **strictly sequentially** within its own byte range, so every chunk
  region has a single contiguous frontier.
* On resume, that frontier is read back per chunk with
  `os.lseek(fd, chunk_start, os.SEEK_HOLE)`. Allocated extents imply journal-committed data
  under `data=ordered`, so this yields precisely the same guarantee `curl -C -` relies on —
  per chunk, in parallel. Explicitly-written zero bytes still allocate extents, so genuine
  zeros in the payload are not mistaken for holes.
* Delayed allocation (`delalloc`) can make `SEEK_HOLE` *over*-report while dirty pages are in
  cache, but that only affects the running process, which never consults it. After a crash
  those pages and their extents are both gone, so the post-crash read is accurate — the only
  time it is used.

Consequences: **no periodic checkpoints, no fsync in the hot path** (faster than the
checkpointing design as well as safer), and worst-case re-download equals the filesystem's
uncommitted tail — the same bound as today.

**The manifest is demoted from a progress record to a plan record.** It stores the immutable
plan (`plan_id`, chunk boundaries) written once at startup, plus one small record per
*chunk completion* — a rare event (8–16 times per file, not continuously). Those records
carry the per-part md5/CRC digests (§5) and are written with the strict ordering
`fsync(data)` → update → `tmp` + `fsync` → atomic `rename` → `fsync(dir)`. Because `rename`
is atomic, a crash leaves either the old manifest or the new one, never a torn one; and a
crash before the record is written merely costs a re-hash, never a re-download.

**`SEEK_HOLE` capability probe (required — silent failure would corrupt data).** On a
filesystem without `SEEK_HOLE` support the kernel reports the *entire file* as data, which
would make every chunk look complete and yield a silently corrupt output. So at startup,
probe: create a small sparse temp file next to `dest`, verify `SEEK_HOLE` actually reports
the hole, and delete it. Only use the frontier scheme if the probe passes (ext4 on the
localization PD does; NFSv4.2 does; NFSv3 does not). **If the probe fails, fall back to a
checkpointing manifest with a small 8 MiB interval** — more fsyncs, but bounded loss of
8 MiB/stream rather than 512 MiB, and only on filesystems that need it.

On resume, additionally sanity-check each derived frontier: monotonic, ≤ chunk end, and
consistent with the last-block CRC recorded at the previous chunk completion. Any
inconsistency rewinds that chunk to its last verified point rather than trusting the frontier.

### 4.2 Signal handling is an optimisation, not a mechanism
If a `SIGTERM` does arrive (and as a backstop, if polling GCE metadata
`instance/preempted` every 5 s reports true), stop issuing new range requests, fsync,
checkpoint, and exit. This merely shrinks the lost window from ≤64 MiB/chunk to ~0; it
changes nothing about correctness. The handler must itself be crash-safe — being killed
*during* the handler is just another crash, handled by §4.1 — and must never block shutdown
(no unbounded waits, no retries inside the handler).

**Do not rely on the handler for requeueing either.** On a hard preemption the process never
exits, `localization.sh` never returns, and the node disappears; SLURM requeues the job
because the node failed, not because of any exit code we produced. The exit-code contract
below therefore only governs the cases where the process actually gets to exit.

Exit-code contract (the entrypoint gives only three behaviours — see §1):
| Situation | Exit | Entrypoint behaviour |
|---|---|---|
| Success | 0 | continue to job |
| Caught preemption / transient failure after exhausting internal retries, **with forward progress** | 5 | requeue and resume |
| Same, but **no** forward progress this attempt | 1 | DNR — surfaces a genuinely stuck download instead of looping |
| Hash mismatch, 404, permission denied, invalid range support | 1 | DNR (a real error; matches today) |
| Hard preemption (no warning) | *n/a* | process never exits; SLURM requeues on node failure and §4.1 makes the partial file resumable |

The "forward progress" guard matters: `CANINE_PREEMPT_LIMIT` accounting at
`orchestrator.py:57` **subtracts** `.localization_failure_count` from
`SLURM_RESTART_COUNT`, so exit-5 requeues deliberately do not count toward the escalate-to-
non-preemptible limit. Returning 5 unconditionally would let a download that can never make
progress requeue forever. A transient failure must also never escape as an *arbitrary*
nonzero code, since anything other than 5/15 is treated as DNR.

### 4.3 Manifest validity and safe restart
The manifest records `plan_id = sha256(url identity, size, etag/md5, chunk_size,
schema_version)` plus the destination's `st_dev`/`st_ino`/`st_size`. On startup:
* **no manifest** ⇒ fresh download (correct on a wiped ephemeral local disk);
* **`plan_id` mismatch, or dest inode/size inconsistent** ⇒ discard the partial file and
  manifest and restart (protects against a changed URL/object, a changed chunk layout,
  or a stale manifest left on reused storage);
* **`plan_id` match** ⇒ resume, re-fetching only `[chunk_start + bytes_done, chunk_end]`
  for each incomplete chunk.

Changing `--connections` between attempts must **not** invalidate progress: `plan_id` is
derived from the *chunk size*, and the chunk size is computed from `size` and
`min_chunk` only. The connection count controls how many chunks are in flight, never the
chunk layout, so a requeued task on a differently-loaded node resumes cleanly.

### 4.4 The full-size-file hazard — completion must not be inferred from size
`ftruncate`-to-final-size makes an *incomplete* file already have its full apparent size
(`stat` reports the logical size of a sparse file, not its allocated blocks). Anything that
infers "done" from `stat` size would silently accept a truncated/corrupt input. So:
* completion is asserted **only** by the `.<basename>.k9pdl.done` marker, written after
  hash verification and containing `{size, mtime, hash, plan_id}`;
* the presence of a `.k9pdl.json` manifest means "in progress / incomplete";
* the emitted bash checks the marker (not `stat`) before deciding to skip the download, and
  a size-matching file with **no** marker and **no** manifest (i.e. a legacy or
  externally-placed file) is verified by hash before being accepted, else re-downloaded;
* `HandleAWSURL`'s current `SZ=$(stat --printf '%s' ...)` heuristic (`file_handlers.py:409`)
  is removed along with the append-resume it drives — note this is precisely the pattern that
  becomes unsafe once the file is created at full size upfront;
* nothing downstream may use `du`/allocated-block counts either: a sparse in-progress file
  under-reports there, just as it over-reports via `stat`.

### 4.5 Concurrent-writer safety (without depending on the lock)
For the dominant `LocalizeToDisk` path this is nearly moot: the localization PD is attached
read-write to a single VM, and canine already exits 5 when another instance holds it
(`base.py:828-830`). It matters for the NFS destination, where a requeued task can start
while the preempted VM is still being torn down.

Take a non-blocking `flock` (LOCK_EX) on the manifest for the whole download, recording
writer `hostname`/`pid`/`boot_id`; a second writer waits with backoff and re-reads the
manifest rather than interleaving manifest updates. On a local ext4 PD this is ordinary
POSIX `flock` and works.

**On NFS it does not work at all**: workers mount the share with `nolock`
(`worker_startup_script.sh:33`: `mount -o defaults,hard,intr,nolock ...`), which disables
NFS file locking, so `flock` is client-local and coordinates nothing between VMs. **On a
gcsfuse mount it is unsupported outright** and the `flock` call may fail with
`ENOTSUP`/`ENOSYS` rather than succeeding — so the call must be wrapped in `try/except
OSError` and treated as "no lock available", never as a download failure. This is not a
problem to fix — it is a reason the design must not depend on locking, and it does not.

Two writers derive the *same* chunk plan from the same `plan_id`, so they write
byte-identical data to the same offsets — overlapping `pwrite`s are benign. The only real
hazard is a manifest update that under- or over-reports another writer's progress, and that
is bounded by §4.1 (worst case a redundant re-download) and caught by end-to-end
verification. A stale writer record is simply overwritten.

### 4.6 Sidecar files land on the RODISK
For `LocalizeToDisk`, the destination directory becomes a read-only disk consumed by
downstream tasks, so the `.k9pdl.json` manifest and `.k9pdl.done` marker persist there.
This is acceptable and has direct precedent: `HandleGSURL` already writes
`{dest_dir}/.gcloud_tracker_dir` and `{dest_dir}/.gcloud_manifest` sidecars into the same
location for exactly the same purpose — resumable transfer state (`file_handlers.py:258`).

Rules that follow:
* dotfile names, so they stay out of glob/`find`-based output patterns;
* written **next to the working file**, on the same filesystem, so they are attached,
  detached, preempted and resumed atomically with the data they describe — never on `/tmp` or
  the boot disk, which do not survive. Under stage-then-publish (§4.7) the working file is in
  the staging directory, so the manifest goes there rather than onto the object store; the
  `.k9pdl.done` marker still lands next to `dest`, because that is what the emitted bash
  consults on the next attempt;
* the marker is **not** deleted after a successful download. It must outlive the download to
  cover a preemption between "download finished" and "disk labelled `finished=yes`"
  (`base.py:1365`); the label, not the marker, is the final completion signal, and once it is
  applied canine skips localization entirely and mounts the disk read-only;
* downstream RODISK consumption is unaffected — `rodisk_paths` is built from the declared
  input list, not by scanning the disk, so stray dotfiles are inert.

### 4.7 Destination filesystem capability gate (and the gcsfuse case)
The staging share is NFS today, but it may become a **gcsfuse-mounted GCS bucket**. gcsfuse
presents a POSIX-looking interface over an object store and breaks essentially every
assumption in §4.1:

* **No sparse files.** `SEEK_HOLE` reports the whole file as data, so every chunk would look
  complete on resume. The §4.1 probe catches this — but only as a *last* line of defence.
* **`ftruncate` to the final size is destructive, not free.** There are no holes to create, so
  a 50 GB `ftruncate` means materialising 50 GB of zeros.
* **Random writes are pathological.** Out-of-order `pwrite`s force gcsfuse out of its
  streaming-write path into read-modify-write against a local temp file, then a **full-object
  re-upload**. Eight concurrent writers at eight different offsets is the worst possible
  access pattern for it.
* **No durability before `close()`.** Bytes live in gcsfuse's local temp directory (on the
  boot/ephemeral disk) until the object is finalised. A preemption loses **all** of them —
  the exact opposite of the §4.1 guarantee, and it would not even be detectable, since the
  partially-written object simply never appears.
* **`fsync` is not a cheap durability barrier** — it implies a full object upload.
* **`rename` is not atomic** on flat-namespace buckets (server-side copy + delete), so the
  manifest's `tmp`+`rename` commit is not atomic there. (Hierarchical-namespace buckets do
  provide atomic rename, but the design must not depend on which kind of bucket is mounted.)
* **`flock` is unsupported** and may fail with `ENOTSUP`/`ENOSYS` rather than succeeding.
* **Metadata caching** (stat cache, default TTL) means a file written by one VM may not be
  visible, or may show a stale size, to another VM for the duration of the TTL.
* Allocated-block accounting (`du`, `st_blocks`) is synthetic and meaningless.

**Gate, don't adapt.** Rather than trying to make the chunked writer behave on a FUSE object
store, detect it up front and take a different route.

*Detection.* Resolve `dest`'s mount by longest-prefix match over `/proc/mounts` (after
`realpath`) and check the fstype against an **allowlist** — `ext4`, `xfs`, `btrfs`, `tmpfs`,
`nfs`/`nfs4` — not a denylist, so an unknown future filesystem fails safe rather than
silently taking the fast path. Any `fuse.*` fstype (including `fuse.gcsfuse`) is off-list.
Back the fstype check with two runtime probes on the target directory, since fstype strings
lie: the §4.1 `SEEK_HOLE` probe, and a **random-write probe** (write a small file, `pwrite` at
a non-zero offset, verify the readback and that `st_blocks` reflects sparseness).

*the in-place route — POSIX destination (allowlist + probes pass).* Unchanged: download chunks directly
into `dest` exactly as §4.1 describes. This covers the dominant `LocalizeToDisk` PD path and
today's NFS share, so **the common case is not affected at all**.

*the bucket-compose route — parts + server-side compose (bucket-backed destination, gs:// URL resolvable).*
**This is the preferred route for a gcsfuse-backed destination.** The disk-space argument that
ruled out part-files-and-merge on the localization PD (peak ≈ 2× size against a 5 % margin,
`base.py:727-731`) **does not apply to a bucket** — and, more importantly, on GCS the "merge"
transfers no data at all:

* `compose` creates an object from existing objects "without transferring additional object
  data" — it is a server-side metadata operation, billed as one Class A op. There is no
  download-and-re-upload step, so unlike the POSIX case there is no sequential tail to pay for
  and **the entire transfer stays parallel end to end.**
* Up to **32 source objects per compose request**; sources may themselves be composite, so
  >32 chunks tree-compose (e.g. 800 → 25 → 1). Total component count is effectively unbounded
  (`componentCount` saturates at INT32_MAX; the only real ceiling is the 5 TiB object limit).
* Sources must be in the **same bucket and the same storage class** — satisfied by writing
  parts into the destination bucket. `deleteSourceObjects: true` hard-deletes the parts as
  part of the compose, so no cleanup pass and no lingering storage charges.

Steps:
1. **Resolve the `gs://` URL** for `dest` from the gcsfuse mount entry in `/proc/mounts` (the
   device field is the bucket; account for `--only-dir`). If it cannot be resolved
   unambiguously, fall through to the stage-publish route.
2. **Bypass the mount for writes.** Each chunk is uploaded straight to the GCS JSON API as its
   own temp object `<dest>.k9pdl.parts/NNNN`, never through gcsfuse — which sidesteps
   staged-write re-uploads, the `close()`-only durability rule, `--implicit-dirs`, the metadata
   cache and non-atomic rename in one move.
3. **Upload each chunk through its own resumable upload session**, streaming the ranged GET
   response directly into it. This is the crux (see below).
4. **Compose** the parts into `dest` in chunk order with `deleteSourceObjects: true`.
5. **Verify**, then write the `.k9pdl.done` marker.

*Why this keeps the §4.1 zero-loss guarantee.* A resumable upload session is a durable frontier
oracle, exactly analogous to `SEEK_HOLE`: GCS persists bytes at **256 KiB** granularity, and the
committed offset is queryable with `PUT <session_uri>` + `Content-Length: 0` +
`Content-Range: bytes */SIZE`, which returns `308` with a `Range: bytes=0-N` header (absent if
nothing is persisted yet). So on resume the downloader **asks the storage where its durable
frontier is** rather than trusting a stored counter — the same principle as §4.1, different
oracle. Worst-case discarded work is **<256 KiB per in-flight chunk** (≈2 MiB across 8
connections), versus 512 MiB for the rejected checkpoint design. Also:
* sessions are valid for **7 days**, far longer than any localization;
* uploads within a session must be **sequential**, which is exactly how chunks are already
  streamed, and persisted bytes can never be overwritten — so a retry re-sending an
  already-committed range is harmless;
* **an incomplete resumable upload is invisible in the bucket**, so a preempted part leaves no
  partial object and no ambiguity about what exists.
The session URIs *are* the resume state and must be persisted in the manifest before any bytes
are sent; a lost URI costs only that one part.

*Verification (`check_hash`, alias `check_md5` — §6.4).* **Hard rule: if the flag is set, the
file's integrity is verified against the source's declared hash before the `.done` marker is
written, no matter what it costs — up to and including reading the entire file back.** The
fast methods below are optimisations layered *on top of* that guarantee, never a substitute
for it. There is no path in which the flag is set and the download is accepted unverified: an
inability to verify is a hard failure (`exit 1` + delete `dest`), never a pass. Composite
objects having no MD5 is a reason to *change the method*, never a reason to skip the check.

Method selection, fastest sound option first:
* **the in-place route / C (POSIX destination):** unchanged from today — a sequential md5 (or the
  multipart-ETag md5-of-md5s) over the finished local file. This is already a full read of the
  file in the current implementation, so there is no regression; the read is checkpointed by
  `hash_offset` so a preemption resumes the *hashing*, not the download.
* **the bucket-compose route (bucket), source exposes per-part digests** (S3 multipart ETag with chunk
  boundaries snapped to `PartLength`, §5): each uploaded part is a plain single-stream upload,
  so GCS reports its `md5Hash` — compare directly against the source's per-part ETag. Every
  byte is checked against the source's own digests, server-side, with **no read-back**.
* **the bucket-compose route, source exposes only a whole-file hash:** combine the per-chunk CRC32Cs computed
  in-flight (CRC32C *is* algebraically combinable, unlike MD5, so out-of-order chunk
  completion is fine) and compare against the CRC32C `compose` returns — Google explicitly
  endorses this. That proves *`dest` == the bytes we received*. If the source's declared hash
  is an MD5, that alone does not prove *the bytes we received == the source*, so the md5 is
  additionally computed **in-flight in chunk order** where the chunk completion order permits,
  and otherwise by a **full parallel ranged read-back of the composed object** — 8 connections,
  same-region GCS→GCE, and the pass is checkpointed so it survives preemption. This is the
  expensive case, and it is accepted rather than avoided.
* **CRC32C availability:** combining requires the `google-crc32c` C extension (`zlib.crc32` is
  CRC32, not CRC32C, and a pure-Python CRC32C over tens of GB is far too slow). If it is not
  importable, do **not** silently drop the check — fall back to the per-part `md5Hash`
  comparison, and to the full read-back when that is not available either.

Net effect: in the common S3-multipart case coverage is *stronger* than today's single
whole-file md5 and strictly cheaper; in the worst case it costs one extra full read, which is
the price of the guarantee. Optionally record the source-declared md5 in the object's custom
metadata for downstream consumers that expect one — informational only, never the gate.

*Cost note.* The VM is a relay: bytes arrive from the source and leave again to GCS, so the
transfer is bounded by both directions rather than by PD write throughput. Same-region GCS
egress is free and fast, but the `connections` sweep (§8) must be re-run for this route rather
than reusing the PD-tuned default.

*the stage-publish route — stage-then-publish (non-POSIX destination that is **not** a resolvable GCS bucket).*
The generic fallback. Chunk-download into a work directory on a real block device, then publish
with a single sequential copy:
1. **Choose the work directory**, first match wins: the localization persistent disk (when
   `localize_to_persistent_disk` is set — the only one where the staged file survives
   preemption), then `CANINE_LOCAL_DISK_DIR` (`base.py:1065`), then `$TMPDIR`. Require
   `free space ≥ size × 1.05`; if nothing qualifies, degrade to a **single sequential
   `curl -C -` writing straight through the mount** — for gcsfuse this is also the only pattern
   that hits its streaming-write path (sequential writes, new file, one handle; anything else
   reverts to staging the whole file locally and re-uploading it). Never silently proceed with
   a work directory that cannot hold the file.
2. **Download** into the work directory with the full §4.1 frontier scheme, valid there because
   it is a real POSIX filesystem.
3. **Verify** the hash on the staged file *before* publishing, so a corrupt download never costs
   an upload.
4. **Publish** with one sequential streaming copy to `dest`, then `close()` — note that with
   gcsfuse **`fsync` does not finalize the object**; only `close()` does, and nothing is durable
   before it. Do **not** publish to a temp name and rename: rename is not atomic on
   flat-namespace buckets.
5. **Re-verify** the published object's size after `close()`, then write the `.k9pdl.done` marker.

*Resumability under the stage-publish route.* The staged file carries the §4.1 guarantee, so preemption during
the download costs nothing when the work directory is on the localization PD, and falls back to
the documented ephemeral-disk behaviour (clean restart from zero) when it is not — which is why
the PD is preferred first. Preemption during the *publish* re-publishes from the still-staged,
already-verified file without re-downloading anything. This route is strictly worse than the bucket-compose route
(a full sequential publish, and up to a whole file of staged work at risk), which is why the bucket-compose route
is preferred whenever the destination is a resolvable bucket.

*Sidecars under Routes B/C.* Under the bucket-compose route the manifest is written as an object next to `dest`
(object writes are atomic, so no dependence on rename semantics) and the parts live under a
dotted `.k9pdl.parts/` prefix that is deleted by the compose itself. Under the stage-publish route the manifest
follows the **working** file into the staging directory rather than onto the mount. In both
cases the `.k9pdl.done` marker stays next to `dest`, because that is what the emitted bash
consults on the next attempt (§4.4).

*Locking.* `flock` must already be treated as advisory-at-best (§4.5); on gcsfuse it may raise
`ENOTSUP`/`ENOSYS`. Catch `OSError` around the `flock` call and continue without it — never
fail the download because a lock could not be taken.

*Deployment implications.* If `CANINE_ROOT` itself becomes gcsfuse-backed, see §6.3: reads are
fine, but metadata caching can delay cross-VM visibility of a freshly staged
`parallel_download.py`, which changes the script-resolution order.

### 4.8 Fallback path keeps its resumability
The single-stream fallback (no `Accept-Ranges`, unknown size, `ftp://`, or
`parallel_download=False`) continues to use `curl -C -`, so preemption resume is preserved
exactly as today for every case the chunked path declines to handle. The
`aws s3api` chunk fallback (§5) uses the same sparse-file frontier recovery as the HTTP path.

---

## 5. S3 specifics

* **Primary path:** mint a presigned URL host-side is *not* possible (creds live on the
  VM), so the emitted script runs `aws s3 presign` (honouring `--endpoint-url`) on the VM
  and feeds the resulting URL into the generic ranged-HTTP downloader — one code path
  for curl-backed and S3-backed downloads, and no `aws` process per chunk.
  For public buckets (`--no-sign-request`, already detected at `file_handlers.py:326`)
  the plain `https://` object URL is used directly.
* **Fallback:** if `presign` fails (e.g. session-token-only creds, exotic endpoint), each
  chunk is fetched via `aws s3api get-object --range "bytes=A-B" /dev/stdout`, read from
  the subprocess pipe and `pwrite`n at the offset. Same manifest/resume logic.
* **Multipart ETag alignment:** when `check_md5` is set and `PartsCount > 1`, snap chunk
  boundaries to multiples of `PartLength` (`self.headers["PartLength"]`). Each chunk then
  spans whole S3 parts, so the md5-of-md5s ETag is computed **incrementally during the
  download** and the existing post-hoc `multiprocessing` md5 heredoc
  (`file_handlers.py:424-441`) is deleted — saving a full extra read of the file.
  If `PartLength` ≥ the ideal chunk size, use `PartLength` as the chunk size directly.
  **Resume interaction:** per-part md5 digests are recorded in the manifest at *chunk
  completion* (§4.1) — rare, cheap (16 bytes/part) and fsync-ordered after the data. A crash
  mid-chunk costs only a re-hash of that chunk's parts from disk on resume, never a
  re-download. If the digests are missing or inconsistent, fall back to a full verification
  pass — correct, just slower.
* **Presigned-URL expiry:** the presign is minted on the VM at run time, so a requeued task
  always gets a fresh URL. For expiry *mid-download*, the presign command is passed as
  `--url-refresh-cmd` and re-run on 403, resuming from the checkpoint (§4).
* Single-part objects keep plain `md5sum`-equivalent streaming md5; because chunks complete
  out of order, the md5 is computed in one sequential verification pass at the end. That pass
  records its `hash_offset` at chunk-completion granularity, so a preemption during
  verification resumes the *hashing*, never the download.
* **Bucket destination (the bucket-compose route, §4.7):** the multipart-ETag alignment pays off twice here.
  Snapping chunk boundaries to `PartLength` means each uploaded GCS part corresponds to whole
  S3 parts, so S3's per-part ETag md5 can be compared directly against the `md5Hash` GCS
  reports for the matching uploaded part — end-to-end verification of every byte against the
  source's own digests with **no** local hashing and **no** read-back on either side. This is
  what makes the alignment worth keeping: it is the difference between a free verification and
  a full read-back. It replaces (does not suppress) the whole-file `md5sum` gate, which is
  unavailable because composite objects have no MD5. For a **single-part** S3 object the ETag
  *is* the whole-file md5 and there are no per-part digests to exploit, so verification falls
  back to the in-flight/read-back path of §4.7 — slower, but never skipped.

---

## 6. Handler changes (file_handlers.py)

### 6.1 Derive from the existing commands, don't replace them
The current `localization_command` bodies are known to work on cluster nodes, so they are
the template. Each rewrite keeps the surrounding shell **character-for-character** and
swaps only the download line. Concretely, the idioms to preserve:

* the quoting/assignment preamble used by every handler —
  ```python
  dest_dir  = shlex.quote(os.path.dirname(dest))
  dest_file = shlex.quote(os.path.basename(dest))
  self.localized_path = os.path.join(dest_dir, dest_file)
  ```
  (note it quotes *then* joins; `localized_path` therefore embeds quote characters.
  Downstream code depends on this exact string, so do **not** "fix" it here);
* the directory guard `"[ ! -d {dest_dir} ] && mkdir -p {dest_dir} || :; "`;
* building a `cmd = []` list and returning `"\n".join(cmd)`;
* the md5 gate verbatim, including its `exit 1` on corruption
  (`file_handlers.py:546`, `:692`, `:444`);
* AWS's inline credential prefix `"{env} aws s3api {extra_args} ..."` (`:411`);
* DRS's `signed_url=$(curl ... | python3 -c ...)` resolution step (`:683-686`), reused
  unchanged as the `--url-refresh-cmd`.

Illustrative before/after for `HandleOtherURL` (line 793) — the whole change is one line:
```python
# before
cmd += ["[ ! -d {dest_dir} ] && mkdir -p {dest_dir} || :; curl -C - -o {path} '{url}'".format(...)]
# after
cmd += ["[ ! -d {dest_dir} ] && mkdir -p {dest_dir} || :; " + _pdl_command(
    url = self.url, dest = self.localized_path, size = self.size, legacy_cmd = <the curl string>
)]
```

### 6.2 Helpers
* `_pdl_path()` → the script path, resolved for **both** execution contexts (§1) by picking
  the first *usable* path rather than defaulting on `CANINE_ROOT`. The installed package copy
  is tried **first**, so the node depends on the shared mount only when the package is absent
  — which matters if `CANINE_ROOT` ever becomes a gcsfuse mount whose metadata cache can lag
  (§6.3). "Usable" means present *and* complete, verified by the trailing `# k9pdl-eof`
  sentinel, so a truncated or partially-visible copy is skipped rather than executed:
  ```bash
  K9_PDL=; for p in "<installed pkg dir>/parallel_download.py" "${CANINE_ROOT:-}/parallel_download.py"; do [ -f "$p" ] && tail -n1 "$p" | grep -q '^# k9pdl-eof$' && { K9_PDL="$p"; break; } || :; done
  K9_PDL_RUN="$K9_PDL"; [ -x "$K9_PDL" ] || K9_PDL_RUN="python3 $K9_PDL"
  ```
  The script carries `#!/usr/bin/env python3` and is committed executable (§6.5), so it is
  normally invoked **directly** as `$K9_PDL_RUN`. The `python3` fallback exists because the
  exec bit cannot be relied on everywhere: gcsfuse cannot represent one at all (§4.7), a mount
  may be `noexec`, and `chmod` can fail under NFS `root_squash`. Correctness therefore never
  depends on the bit — only convenience does.
  On the controller during `pick_common_inputs` the staging dir is not populated yet, so the
  installed package path is the only candidate anyway. `parallel_download.py` must therefore
  end with the literal `# k9pdl-eof` line, asserted by a unit test.
  `<installed pkg dir>` is interpolated host-side via `os.path.dirname(__file__)`.
  `python3` is invoked directly — it is present on cluster nodes (§1), exactly as the
  existing `python3` heredoc and `python3 -c` call sites already assume.
* `_pdl_command(url, dest, size, headers=[], md5=None, etag=None, part_length=None,
  s3=None, url_refresh_cmd=None, legacy_cmd=..., **opts)` → emits, in order:
  1. the `.k9pdl.done` marker check that short-circuits an already-complete download (§4.4);
  2. a fixed connection count (§2 — the node is ours, so no runtime negotiation), overridable
     by `download_connections` / `CANINE_DOWNLOAD_CONNECTIONS`;
  3. `python3 "$K9_PDL" ...` — with `legacy_cmd` retained as the `parallel_download=False`
     opt-out and as the in-script fallback when the server turns out not to honour ranges
     (§4.8), so the degraded path is byte-for-byte today's working command.

  No heredocs in the emitted download commands (§12.5) — the staged-file approach makes them
  unnecessary — and no emitted line may contain the literal `#DEBUG_OMIT`.

Apply to `HandleAWSURL`, `HandleGDCHTTPURL`, `HandleDRSURI`, `HandleGCSSignedURL`,
`HandleOtherURL`. `HandleGDCHTTPURL` passes its `X-Auth-Token` via `--header`;
`HandleDRSURI` passes its existing signed-URL snippet as `--url-refresh-cmd`.

Secrets in the emitted script: GDC tokens and AWS keys are already interpolated into these
command strings today (`file_handlers.py:319,479`). Pass them to the downloader the same way
(`--header`, `AWS_*` env prefix) — **do not** widen exposure by writing them into the
manifest or into log lines; the downloader redacts query strings and headers from stderr.

Because the whole `localization.sh` re-runs after a preemption, every emitted command
stays idempotent: `mkdir -p` guards are kept, the download is skipped only on a valid
`.k9pdl.done` marker (§4.4), and the `ln -s` symlink guard at `base.py:1157` already
tolerates re-runs.

### 6.3 Deployment: how `parallel_download.py` reaches the node
It is **not** installed on the node and requires no image rebuild. It rides the
NFS-shared staging directory, exactly like `delocalization.py`:

1. wolF always uses `NFSLocalizer` (`wolf/task.py:1381`) with
   `staging_dir = output_dir` (`wolf/task.py:702`), which lives under the shared
   `/mnt/nfs` workspace. For this localizer `local_dir == staging_dir` (`nfs.py:47-48`), so
   `environment('local')['CANINE_ROOT']` and `environment('remote')['CANINE_ROOT']`
   (`base.py:224-231`) are the **same absolute path**.
2. At the end of `localize()`, the localizer copies the script into `CANINE_ROOT` —
   add `parallel_download.py` beside the existing `delocalization.py` copy at
   `nfs.py:170` (and `local.py:141`, `remote.py:121` for the other localizers).
3. Every worker VM mounts the controller's NFS at the identical path
   (`worker_startup_script.sh:33`: `mount ... ${CONTROLLER_NAME}:/mnt/nfs /mnt/nfs`) and
   bind-mounts it into the slurmd container unchanged
   (`worker_startup_script.sh:61`: `-v /mnt/nfs:/mnt/nfs`). So the path resolves identically
   on the controller, on the worker VM, and inside the container where `localization.sh`
   actually runs.
4. The entrypoint exports `CANINE_ROOT` (`orchestrator.py:50`) before invoking
   `localization.sh`, so the emitted command just references `$CANINE_ROOT/parallel_download.py`.

Ordering is safe: the copy happens during `localize()`, which completes before
`submit_batch_job` (`wolf/task.py:1389` → `:1401`). The one exception is controller-side
common-input localization, which runs *earlier* in `localize()` than the copy
(`nfs.py:63` vs `nfs.py:170`) — handled by the "first existing path" resolution in §6.2,
which falls through to the installed package directory there.

**Staging must preserve the exec bit.** `shutil.copyfile` — used for `delocalization.py` at
`nfs.py:167` — does *not* copy permissions, so the staged copy would land non-executable. Use
`shutil.copy` (which copies mode) followed by an explicit
`os.chmod(dst, 0o755)`, wrapped in `try/except OSError` so a filesystem that cannot honour it
(gcsfuse, `root_squash`) degrades to the `python3` fallback rather than failing localization.
Do **not** rely on the installed package copy being executable either: `parallel_download.py`
lives inside the `canine.localization` package, so it is picked up automatically as a module
(`pyproject.toml:57-59`; note `package-data` at `:61-69` lists `localization/debug.sh`
explicitly precisely because it is *not* a `.py` file), and wheel/pip installation does not
reliably preserve mode bits for package modules. Ship the shebang and the committed exec bit,
chmod at staging, and keep the fallback.

**If `CANINE_ROOT` becomes a gcsfuse mount.** Staging still works — the controller writes the
file, the workers read it — and GCS is strongly read-after-write consistent, but *gcsfuse's
metadata cache is not*: with a nonzero stat-cache TTL a worker that has already looked in
that directory can serve a stale negative or stale-size entry for the length of the TTL, so a
freshly staged `parallel_download.py` may briefly appear missing or truncated. Three
consequences, all cheap:
* the §6.2 "first path that exists" resolution must prefer the **installed package copy**
  when present and fall back to `$CANINE_ROOT` — not the reverse — so the node normally never
  depends on mount freshness at all;
* the resolver must reject a zero-length or truncated candidate (check for the trailing
  `# k9pdl-eof` sentinel line) and continue to the next path, rather than running a partial
  script;
* gcsfuse must be mounted with `--implicit-dirs` for the staging directory tree to be visible
  at all on a **flat-namespace** bucket (not needed on a hierarchical-namespace bucket, where
  directories are first-class), and the staging path wants a short metadata-cache TTL.
  Document both as requirements rather than discovering them at runtime. The default TTL is
  1 minute, **but gcsfuse raises it to *infinite* by default on high-spec machines** — which
  an n1-standard-8 may well qualify as. An infinite stat-cache TTL would make a stale view of
  the staging directory permanent for the life of the mount, so `metadata-cache: ttl-secs`
  must be set explicitly rather than left to the default. This is the single most likely way a
  gcsfuse migration would break script staging, and it is why the installed-package copy is
  preferred first.
This is exactly why the delocalization/localization scripts should keep riding the same
mechanism: whatever makes `delocalization.py` reachable makes `parallel_download.py`
reachable, so there is one thing to get right, not two.

Alternatives considered and rejected: baking it into the worker image
(`slurm_gcp_docker/.../Dockerfile`) would need a fleet-wide rebuild and would version-skew
against the installed canine; an inline heredoc would be mangled by `debug.sh`'s
`sed -e '$s/ - <<.*$//'` (§1).

Add the staging assertion to the four existing tests that already check for
`delocalization.py` (`test_localizer_nfs.py:139`, `test_localizer_remote.py:107`,
`test_localizer_local.py:105`, `test_localizer_batched.py:372`).

New per-handler kwargs (arrive through `extra_args`, already plumbed from
wolF `task.conf["localization"]` → `get_file_handler(**extra_args)` at `wolf/task.py:1565`):
`parallel_download` (bool, default `True`), `download_connections` (int, default `8`;
`0`/`1` ⇒ legacy single stream), `download_min_chunk` (bytes, default `64 MiB`), and
`check_hash` (aliasing the existing `check_md5`, §6.4).
Mirror them as `Localizer.__init__` kwargs in `base.py:61-99` for a global default and
document in `canine/pipeline_options.md`.


### 6.4 `check_hash` / `check_md5` alias (backwards compatibility)
The flag now means "verify integrity by the best method the destination supports" (§4.7), so
`check_md5` is a misleading name — but it is used throughout wolF pipelines
(`wgs_pipeline.py:249,260,761,771`, `wrapped_pipeline_workflow.py`, `char_sv_workflow.py`,
`run_mutect1.py`, `run_skcm.py`, …), so it **must keep working indefinitely**. Rename, alias,
do not break:

* **Resolve once in the base class.** `FileHandler.__init__` (`file_handlers.py:38`, where
  `self.extra_args = kwargs` is set) gains:
  ```python
  self.check_hash = self._resolve_check_hash(kwargs)
  ```
  and the four per-handler reads of `self.extra_args.get("check_md5", False)`
  (`:310`, `:480`, `:586`, plus the gate at `:691`) are deleted in favour of the inherited
  attribute. Today those reads are duplicated and can drift; consolidating them is a
  precondition for the route-dependent behaviour in §4.7, which must be decided in exactly one
  place.
* **Precedence:** explicit `check_hash` wins; otherwise `check_md5`; otherwise `False`. If
  **both** are supplied and disagree, raise `ValueError` rather than silently picking one —
  that is a caller bug, and guessing could disable integrity checking without anyone noticing.
  Both supplied and equal is fine (a caller mid-migration).
* **`check_md5` stays readable as an attribute.** Expose it as a property aliasing
  `check_hash` (getter *and* setter), so any subclass, test or downstream consumer that reads
  or assigns `handler.check_md5` behaves exactly as before.
* **Normalise `extra_args`** so it carries *both* keys with the resolved value. `extra_args` is
  passed around as a plain dict (e.g. `wolf/task.py:1565`'s `get_file_handler(**extra_args)`),
  and code elsewhere may index either spelling; making them consistent removes the chance of
  two call sites disagreeing.
* **No runtime `DeprecationWarning`.** Every in-repo caller currently uses `check_md5`, so
  warning on it would emit noise on essentially every localization for no benefit. Document the
  preferred spelling in the docstring and leave the alias silent. (If a warning is ever wanted,
  emit it once at construction — never on attribute access, which is in the hot path.)
* **Tests:** both spellings produce byte-identical emitted commands; conflicting values raise
  `ValueError`; neither key given ⇒ `False`; `handler.check_md5` reads back equal to
  `check_hash`, and assignment through either name is visible in both. Add to the existing
  `test_file_handlers.py`.

No wolF-side change is required: `get_file_handler(**extra_args)` forwards kwargs verbatim, so
both spellings reach the handler unchanged.

### 6.5 Convention: new CLI scripts are self-executable
Any new script added by this work is a first-class CLI tool, not just an importable file:

* **Shebang on line 1** — `#!/usr/bin/env python3` for Python, `#!/bin/bash` for shell. `env`
  resolution matches how the rest of the stack finds `python3` (`base.py:1385` and the existing
  `python3` call sites in `file_handlers.py`), so no interpreter path is hard-coded.
* **Committed executable** (`git update-index --chmod=+x`, mode `0755`), following the existing
  `localization/debug.sh` precedent — the one file in `localization/` that is already `0755`,
  whereas `delocalization.py` is `0644` with **no shebang** and is therefore *only* runnable as
  `python3 delocalization.py`. New scripts should not inherit that limitation.
* **Runnable standalone for debugging** — a real `argparse` interface with `--help`, sane
  defaults, no dependency on canine-specific environment variables beyond documented options,
  and no import of `canine` itself. This matters directly for the emitted-script contract
  (§1): an operator debugging a failed localization can re-run the exact download by hand.
* **Invocation stays fallback-tolerant** (§6.2): prefer direct execution, fall back to
  `python3 <path>` where the exec bit cannot survive. The shebang is what makes the direct form
  work; the fallback is what keeps it correct on gcsfuse and `noexec` mounts.
* **Ends with the `# k9pdl-eof` sentinel** (§6.3) so a truncated or partially-visible staged
  copy is detected rather than executed.
* If it is ever wanted as a user-facing command, it can additionally be exposed via
  `[project.scripts]` in `pyproject.toml`; that is independent of the staging mechanism and not
  required here.

The same convention applies to any shell helper this work adds (e.g. if the localization-disk
resize daemon in §12.2 is extracted from its heredoc into a real file, it gets `#!/bin/bash`,
mode `0755`, and an entry in `[tool.setuptools.package-data]` — `.sh` files are *not* picked up
automatically the way `.py` modules are).

## 7. Adjacent tuning
* While in `HandleGSURL` (line 258): keep `gcloud storage cp` but export
  `CLOUDSDK_STORAGE_SLICED_OBJECT_DOWNLOAD_THRESHOLD=64MiB`,
  `..._MAX_COMPONENTS=8` and `CLOUDSDK_STORAGE_PROCESS_COUNT`/`THREAD_COUNT` sized to the
  same node budget, so GS transfers match the n1-standard-8 tuning. (Config only —
  `gcloud storage cp` already resumes via its `.gcloud_tracker_dir`/manifest.)

## 8. Testing
1. **Unit** (`canine/test/test_file_handlers.py`, existing pytest style): chunk-plan math
   (even sizing, tail chunk, <min-chunk ⇒ 1 chunk, clamp at 16, `PartLength` snapping);
   command-string generation for each of the 5 handlers with network calls mocked, incl.
   `parallel_download=False` ⇒ legacy string.
2. **Emitted-script contract tests** (these guard the remote-execution assumptions in §1,
   which unit-testing the Python alone would miss):
   (a) every generated `localization_command` passes `bash -n` (syntax) **and** still passes
       `bash -n` after `grep -v '#DEBUG_OMIT'` and `debug.sh`'s `sed` transform;
   (a2) `parallel_download.py` starts with `#!/usr/bin/env python3`, is mode `0755` in the repo,
       ends with the `# k9pdl-eof` sentinel, runs standalone under `--help` without importing
       `canine`, and the staged copy is still executable (with the test asserting the emitted
       command falls back to `python3` when the bit is cleared);
   (b) no emitted download-command line contains the literal `#DEBUG_OMIT` or a heredoc, and
       any pre-existing heredoc elsewhere in the script (e.g. the resize daemon,
       `base.py:880-894`) survives the `debug.sh` transform intact (§12.5);
   (c) diff each generated command against the current one for the same inputs ⇒ asserts the
       surrounding shell (quoting preamble, `mkdir -p` guard, md5 gate, AWS env prefix) is
       unchanged and only the download line differs (§6.1);
   (d) run one with `CANINE_ROOT` unset (controller context) and one with `CANINE_ROOT` set to
       an empty dir (staging not yet populated) ⇒ asserts script resolution still finds the
       installed package copy;
   (e) run one with `CANINE_ROOT` set to a populated staging dir ⇒ asserts the staged copy is
       used;
   (f) a full generated script with several inputs runs end-to-end under `set -e` against a
       local server without aborting early.
3. **New `canine/test/test_parallel_download.py`**: spin up a local
   `http.server` serving a random multi-hundred-MiB file
   (a) with Range support ⇒ assert byte-identical output and md5 match for
       1/2/8/16 connections and for a size that is not a multiple of the chunk count;
   (b) with Range disabled ⇒ assert single-stream `curl -C -` fallback;
   (c) a server that drops the connection mid-chunk ⇒ assert retry + resume produces a
       correct file.
4. **New `canine/test/test_parallel_download_resume.py`** — preemption-specific, the
   critical suite. `SIGKILL` (never `SIGTERM`) is the primary instrument, because the design
   may not assume a catchable signal (§4):
   (a) **randomised kill-storm**: `SIGKILL` at N random offsets/times, re-running after each,
       repeated over many seeds ⇒ the final file is always byte-identical and hash-correct,
       and each re-run transfers strictly fewer bytes (instrumented via a counting server).
       This is the headline test — it must pass with the signal handler **disabled** to prove
       correctness does not depend on it;
   (b) **no committed work is discarded**: instrument the counting server to record bytes
       transferred per attempt and assert `sum(bytes transferred) - file size` stays within
       the filesystem's uncommitted tail (a few MiB), *not* a checkpoint interval. This is the
       test that would have caught the rejected 64 MiB-checkpoint design;
   (c) targeted kills in each crash window: mid-`pwrite`; between a chunk completing and its
       manifest record; between the manifest `fsync` and the `rename`; during the `rename`
       ⇒ assert the manifest is always either the old or the new version, never torn, and
       resume always succeeds (§4.1);
   (d) `SEEK_HOLE` frontier recovery: kill mid-chunk, then assert the recovered frontier
       equals the byte count the server actually sent (minus the uncommitted tail), that
       payloads containing long runs of genuine zero bytes are **not** mistaken for holes, and
       that a filesystem failing the capability probe takes the 8 MiB-checkpoint fallback
       instead of silently treating the whole file as complete;
   (e) `SIGTERM` ⇒ assert the *optimisation* path exits 5 with a consistent manifest, and
       assert the same input killed by `SIGKILL` instead still resumes correctly;
   (f) simulated page-cache loss: after a kill, punch a hole in the tail of a chunk's written
       region ⇒ assert the resumed download detects the shorter frontier, refetches exactly
       that tail, and the hash still matches;
   (g) truncate/corrupt/empty the manifest ⇒ assert clean restart from zero, not a corrupt file;
   (h) mutate the served object (different size/etag) between attempts ⇒ assert `plan_id`
       mismatch discards the partial file;
   (i) resume with a *different* `--connections` value ⇒ assert progress is retained;
   (j) full-size-but-incomplete file with no `.done` marker ⇒ assert it is **not** accepted
       as complete (the preallocation hazard, §4.4);
   (k) two concurrent downloaders on one destination, and the same with the `flock`
       **forcibly disabled** ⇒ assert correct output either way (§4.5);
   (l) kill during the final hash-verification pass ⇒ assert verification resumes rather
       than re-downloading;
   (m) for the no-content-hash handlers, corrupt the tail block of a completed chunk ⇒ assert
       the per-chunk CRC check catches it on resume (§4.1);
   (n) **filesystem gate / route selection (§4.7)**, against a fake GCS server plus a fake
       `/proc/mounts`: `fuse.gcsfuse` with a resolvable bucket ⇒ the bucket-compose route; `fuse.gcsfuse` with
       an ambiguous/unresolvable mount ⇒ the stage-publish route; an unrecognised fstype ⇒ the safe route
       (allowlist, not denylist); a filesystem passing the fstype check but failing the
       random-write or `SEEK_HOLE` probe ⇒ still off the in-place route. Assert that on Routes B/C the
       downloader never issues an out-of-order `pwrite` or a full-size `ftruncate` against the
       mount, and that `flock` raising `ENOTSUP` is tolerated rather than fatal;
   (o) **the bucket-compose route resumability**: `SIGKILL` mid-upload ⇒ assert the re-run queries each session
       (`PUT` + `Content-Range: bytes */SIZE`) and re-downloads only the bytes past the
       reported `Range`, never a whole part; assert total refetched bytes ≤ 256 KiB ×
       `connections`; assert session URIs are persisted **before** the first byte is sent;
       assert a lost/expired session URI (`410`/`404`) restarts only that part; assert no
       partial object is ever visible for an incomplete part; assert `compose` is called with
       the parts in order with `deleteSourceObjects: true`, that >32 parts tree-compose
       correctly, and that a CRC32C mismatch between the combined part CRCs and the composed
       object's CRC32C fails the download rather than writing the marker;
   (p) **the stage-publish route**: when no work directory has `size × 1.05` free the downloader degrades to
       single-stream `curl -C -` instead of proceeding; kill during publish ⇒ the re-run
       re-publishes from the staged file and re-downloads **nothing**; kill after publish but
       before the marker ⇒ the re-run re-verifies rather than re-downloading; the marker is
       only written after post-`close()` re-verification;
   (q) **`check_hash` is never skipped**: for each route, assert the `.done` marker is written
       only after verification passes; assert a corrupted byte is caught on every route
       (POSIX md5, bucket per-part `md5Hash`, bucket combined-CRC32C); assert that with
       `google-crc32c` unimportable the downloader falls back to per-part md5 or a full
       read-back rather than passing; assert a single-part S3 object on a bucket destination
       (no per-part digests available) still verifies, via read-back if necessary; assert an
       inability to verify exits 1 and deletes `dest` rather than exiting 0; assert a kill
       during a read-back verification resumes the hashing and does not re-download;
   (r) sidecars: assert both are dotfiles written next to the working file on the same
       filesystem, that
       the `.done` marker **survives** a successful download (needed to cover a preemption
       between download completion and the `finished=yes` disk label), and that their
       presence does not perturb `rodisk_paths` or the delocalization output globs (§4.6).
5. **Integration**: on a real n1-standard-8, localize a ~50 GB S3 object and a large GDC
   object; record wall-clock and md5 vs. the legacy path; confirm ≥4× speedup,
   `dstat`-observed NIC utilisation approaching the 16 Gbps cap, and bounded memory.
   **Sweep `connections` ∈ {4, 8, 12, 16}** to fix the default empirically (the streams are
   IO-blocked, so the optimum may exceed one per vCPU) and to find where PD write
   throughput, not the NIC, becomes the limit. Then **force a real preemption** mid-localization
   (`gcloud compute instances simulate-maintenance-event` / delete the preemptible worker)
   and confirm the requeued task resumes and produces a hash-correct file — repeat for
   the `LocalizeToDisk` / `localize_to_persistent_disk` destination (the dominant path —
   verify the disk is re-attached, resumed, and only then labelled `finished=yes`) **and**
   the NFS destination. If a gcsfuse-backed staging share is being evaluated, repeat the
   forced-preemption run against it to confirm the stage-then-publish route (§4.7) and to
   measure the publish-copy cost, which is the only part of the transfer that is not
   parallelised.
6. Run existing suites: `pytest canine/test/test_file_handlers.py
   canine/test/test_localizer_batched.py`.

## 9. Rollout
* Default `parallel_download=True`; a single env var `CANINE_DISABLE_PARALLEL_DOWNLOAD=1`
  honoured inside the emitted script gives an instant kill-switch on the VM without
  re-deploying wolF.
* Update `canine/pipeline_options.md`, `canine/README.md`, and `canine/CLAUDE.md`.

## 10. Task order
1. `parallel_download.py` core: chunk planner, preallocation, pwrite workers.
2. Crash-consistency layer (§4.1): sparse `ftruncate`, sequential-within-chunk writes,
   `SEEK_HOLE` frontier recovery, the required capability probe and its 8 MiB-interval
   fallback, and the plan-record manifest — built *with* the core, not bolted on afterwards —
   plus the `test_parallel_download_resume.py` SIGKILL suite. Signal handling
   is added **last**, as an optimisation, and the suite must pass with it disabled.
3. Destination filesystem gate + route selection (§4.7): mount resolution, fstype allowlist,
   random-write probe, `gs://` URL resolution, and `flock` `ENOTSUP` tolerance. Do this
   **before** wiring up handlers, so no handler is written against the assumption that `dest`
   is POSIX.
4. The bucket-compose route (bucket destination): per-chunk resumable upload sessions with session URIs
   persisted in the manifest, frontier recovery via the `308`/`Range` status query, per-part
   `md5Hash`/`crc32c` verification, ordered `compose` with `deleteSourceObjects: true` and
   tree-compose beyond 32 parts, combined-CRC32C check. Schedule **before** the stage-publish route — it is
   the route a gcsfuse migration would actually take.
5. The stage-publish route (generic non-POSIX): work-directory selection with the free-space check, idempotent
   publish + post-`close()` re-verification, and the single-stream degradation path.
6. Remaining downloader unit tests (standalone, no canine coupling).
7. Staging in `local.py` / `nfs.py` / `remote.py` (+ extend the existing
   `delocalization.py` staging assertions in the four localizer test files).
8. `check_hash`/`check_md5` alias + base-class consolidation (§6.4) — before any handler
   conversion, so the four duplicated `check_md5` reads are gone and the route-dependent
   verification of §4.7 has a single decision point.
9. `_pdl_path` / `_pdl_command` helpers, plus the emitted-script contract tests
   (`bash -n`, `#DEBUG_OMIT`/`debug.sh` survival, controller vs. compute-node resolution,
   diff-against-current-command).
10. `HandleOtherURL` + `HandleGCSSignedURL` (simplest).
11. `HandleGDCHTTPURL` (headers) and `HandleDRSURI` (URL refresh).
12. `HandleAWSURL` (presign, aws fallback, ETag alignment + per-part digests in the
   manifest, delete old md5 heredoc and its `stat`-based resume).
13. `Localizer.__init__` options + `HandleGSURL` env tuning.
14. Localization-disk resize daemon (§12.2): hoist `base.py:876-898` out of the
   `is_scratch_disk` branch, retune poll/threshold for the download write rate, plus the
   `ENOSPC`-is-retryable handling in the downloader (§12.3). These are independent of the
   chunking work and could land first — the disk-sizing bug exists today.
15. Transcoding-aware size estimation (§12.3) and the `Content-Encoding: gzip` ⇒ decline-chunked
   guard in `HandleGCSSignedURL`/`HandleOtherURL`.
16. Docs + integration benchmark **and forced-preemption test** on n1-standard-8.

## 11. Resumability invariants (acceptance criteria)
Every change must preserve all of these:
* **A hard `SIGKILL` at any instant — with signal handling disabled — leaves a state that
  resumes to a byte-identical, hash-correct file.** Nothing about correctness may depend on
  catching a signal, running a cleanup path, or flushing at shutdown.
* **No committed work is ever discarded.** Progress is derived from the file's own durable
  extents, never from a stored counter, so a re-run refetches only the filesystem's
  uncommitted tail — matching `curl -C -`. Any design with a periodic checkpoint interval is
  rejected.
* Killing the download at *any* byte and re-running produces a byte-identical, hash-correct
  file, on the localization persistent disk (the dominant `LocalizeToDisk` path) and on NFS.
* A re-run never re-downloads more than the uncommitted tail per in-flight chunk (and, only
  on a filesystem failing the `SEEK_HOLE` probe, at most the 8 MiB fallback interval).
* `SEEK_HOLE` is never trusted without the startup capability probe passing.
* An incomplete file is never mistaken for a complete one, regardless of its size.
* **`check_hash` (alias `check_md5`, §6.4) is an absolute guarantee, not a best-effort
  optimisation.** When it is set, the file is verified against the source's declared hash
  before the `.done` marker is written — **even if that requires reading the entire file
  back**. Being unable to verify (missing digests, no `google-crc32c`, composite object with no
  MD5, lost manifest) forces the expensive path or a hard failure; it never yields a silent
  pass. Every route honours it by the fastest *sound* method that medium supports —
  md5/multipart-ETag on a POSIX destination, per-part `md5Hash` against the source's own
  per-part digests on a bucket, combined CRC32C to prove the destination matches the received
  bytes — and falls back to a full read-back when nothing cheaper is sound.
* Verification is checkpointed, so a preemption during it resumes the *hashing*, never the
  download; and the `.done` marker is written only after it passes.
* A corrupt/absent/stale manifest degrades to a correct full re-download, never to a
  corrupt output.
* Correctness holds even if the `flock` is never honoured — including when the `flock` call
  itself fails with `ENOTSUP`/`ENOSYS`, as it does on gcsfuse.
* **No POSIX semantics are assumed without being verified.** The chunked writer runs only on
  a destination whose filesystem is on the allowlist *and* passes the `SEEK_HOLE` and
  random-write probes; anything else — notably a gcsfuse-mounted bucket — is routed to
  stage-then-publish or to single-stream `curl -C -`. An unrecognised filesystem fails safe.
* **The zero-discarded-work guarantee holds on every route, by asking the storage where its
  durable frontier is** — `SEEK_HOLE` on a POSIX destination, the resumable-upload session's
  committed offset on a bucket. Worst case is the filesystem's uncommitted tail (the in-place route) or
  <256 KiB per in-flight chunk (the bucket-compose route). No route uses a periodic checkpoint interval.
* On a bucket destination, an incomplete part is never visible as an object, the composed
  object is never written unless every part verified, and `compose` transfers no data — so
  assembly cannot itself fail partway and leave a corrupt destination.
* Under stage-then-publish (the stage-publish route), a preemption during the publish step re-publishes from
  the verified staged file and re-downloads **nothing**; the destination is never left in a
  state that a later run mistakes for complete, because the marker is written only after the
  post-`close()` re-verification.
* When the process does get to exit, it exits 5 (requeue+resume) only if the attempt made
  forward progress; otherwise it fails loudly rather than requeueing forever (§4.2).
* Any case the chunked path cannot handle — no `Accept-Ranges`, unknown size, `ftp://`,
  `parallel_download=False` — falls back to the **existing, unmodified** `curl -C -`
  command, which is exactly today's resume behaviour.

---

## 12. Gzip-transcoded objects and localization disk sizing

### 12.1 The bug (pre-existing, independent of this work)
GCS objects stored with `Content-Encoding: gzip` are served **decompressed** to clients that do
not ask for gzip ("decompressive transcoding"), but their `size` metadata is the **stored,
compressed** byte count. Canine sizes the localization disk from exactly that number:

* `HandleGSURL._get_size()` (`file_handlers.py:232-236`) returns `sum(b.size)` — compressed.
* `base.py:727-731` computes `disk_size = F.loc[F["localize"], "size"].sum()` and then
  `max(10, 1 + int(disk_size / (0.95*10**9)))` — a **5 % margin**.
* `HandleGSURL.localization_command()` (`:258`) runs `gcloud storage cp`, which writes the
  object **decompressed**.

So the disk is provisioned against the compressed size while the decompressed bytes land on it.
Genomics text (VCF/BED/GTF/FASTA) routinely compresses 4–10×, so the 5 % margin is not close;
the failure mode is `ENOSPC` partway through localization. This exists today and is unrelated
to parallel downloading — but it must be fixed, because the parallel downloader will hit it
faster and harder.

Not affected: `_get_hash()` (`:238-252`) uses `b.crc32c`, which also covers the stored bytes,
but it is only ever used as a disk-name identity key (`base.py:715-717`), never to verify a
localized file. It is self-consistent and needs no change.

### 12.2 Don't predict the size — react to it (preferred fix)
Predicting the decompressed size is unreliable in general: GCS exposes no decompressed-size
field; the gzip trailer's `ISIZE` is only obtainable by a ranged read of the last 4 bytes and is
mod 2³², so it is untrustworthy at ≥4 GiB or for multi-member streams; and a fixed multiplier is
a guess. **Growing the disk on demand is always correct and needs no prediction.**

The machinery already exists in three places, so this is enabling a proven pattern, not building
one. **No new daemon is written and no `slurm_gcp_docker` change is required** — the first two
entries below are cited only as evidence that the pattern works and that workers already hold the
necessary permissions; the actual fix is a one-branch change in canine (the third entry):
* `slurm_gcp_docker/controller_disk_resize.sh` — NFS disk, `+200 GB` when <10 % free, 30 s poll,
  launched from `docker_entrypoint_controller.sh:12`.
* `slurm_gcp_docker/worker_boot_disk_resize.sh` — **worker** boot disk, `×1.6` when <30 % free,
  10 s poll, launched from `container_heartbeat.sh:24`.
* **`canine/localization/base.py:876-898` — a per-disk resize daemon canine already emits**,
  `×1.6` when <30 % free, 10 s poll, started in the localization script and killed in teardown
  (`:928`).

The third is the important one: **canine already writes exactly the daemon we need — but only
for scratch disks.** `is_scratch_disk = len(file_paths_arrays) == 0` (`:682`), so a disk that
has inputs — i.e. the *localization* disk — takes the `if is_scratch_disk:` branch at `:878` and
gets **no daemon at all**, with its size frozen at the `:727-731` estimate. That is precisely
the gap.

**Fix: emit the same daemon for the localization disk.** Concretely, hoist the `:876-898` block
out of `if is_scratch_disk:` so it also runs when `localize_to_persistent_disk` is set, and
correspondingly hoist the `kill $(cat .diskresizedaemon_pid)` teardown at `:928` (already
outside the scratch-only branch, so it needs no change). Permissions are already proven: workers
run `gcloud compute disks resize` today in both the scratch daemon and
`worker_boot_disk_resize.sh`, under `CLOUDSDK_CONFIG=/slurm_gcloud_config` with
`gcloud_exp_backoff` on `PATH`.

Repurposing `controller_disk_resize.sh` itself is *not* recommended: it keys on
`df $DISK_DEV | grep -q /mnt/nfs`, runs on the VM outside the job's lifecycle, and would have to
discover which of several attached disks belongs to which job. Canine's own daemon already knows
`$GCP_DISK_NAME` and `$GCP_TSNT_DISKS_DIR`, is started and stopped with the job, and is killed
on teardown — strictly better fit.

### 12.3 Consequences that must be handled
* **The downloader must treat `ENOSPC` as retryable, not fatal.** This is the single most
  important interaction with the rest of the plan. Sparse `ftruncate` (§4.1) does **not** reserve
  blocks, so an undersized disk does not fail at creation — it fails on a `pwrite` deep into the
  transfer. With the resize daemon running, that condition is *temporary*. `ENOSPC` must
  therefore pause the affected chunk and retry with backoff (bounded, e.g. 10 min) rather than
  abort; the frontier scheme means a paused chunk resumes exactly where it stopped and loses
  nothing.
* **The daemon can lose the race.** Eight streams saturating a 16 Gbps NIC write ~2 GB/s, which
  consumes 30 % of a 100 GB disk in ~15 s — comparable to one poll interval plus a control-plane
  resize. So: shorten the poll to 5 s for the localization disk, and trigger on *estimated time
  to full* (free bytes ÷ observed write rate) rather than a fixed percentage, growing by at least
  enough to cover several minutes of writing. The `ENOSPC` retry above is the backstop that makes
  a lost race harmless rather than fatal.
* **Disks only grow.** The localization disk becomes a RODISK consumed by downstream tasks and
  persists (optionally `protect=yes`, `:936`), so overshoot is a permanent storage cost. Prefer
  a smaller multiplier with a faster poll over the scratch disk's ×1.6, and log every resize.
* **Preemption is fine.** The daemon is stateless polling; if the VM dies it dies with it, the
  requeued task re-runs the localization script and restarts it, and the disk retains its grown
  size because `disks resize` is persistent control-plane state.
* **Still fix the estimate.** The daemon removes the failure mode but a wildly low initial size
  means many resizes and a slow start, so `_get_size()` should additionally report a
  transcoding-aware estimate: detect `contentEncoding == "gzip"` on the blob, and apply, in
  order of preference, (1) an explicit `uncompressed_size` extra-arg or a custom object-metadata
  field on the reference bucket, (2) the gzip `ISIZE` trailer when the object is <4 GiB, or
  (3) a conservative default multiplier — logging loudly which was used. This is an optimisation
  of the starting point only; correctness rests on the daemon.
* **`HandleGCSSignedURL` / `HandleOtherURL` need the transcoding check too**, because they *do*
  go through the new parallel downloader. The exact semantics are now confirmed — see §12.6,
  which supersedes the "decline on `Content-Encoding: gzip`" rule originally written here
  (that rule does not work, because a transcoded response does **not** carry the header).
* `gcloud storage cp` almost certainly cannot slice a transcoded object (there is nothing to
  range-read server-side), so those `HandleGSURL` downloads stay single-stream regardless. A
  performance footnote, not a correctness issue.

### 12.4 Testing
* Unit: a blob with `contentEncoding == "gzip"` ⇒ assert the size estimate exceeds `b.size` and
  that the chosen estimation method is logged; assert the explicit override wins.
* Emitted-script: assert the resize daemon block is present for a `localize_to_persistent_disk`
  disk (it currently is not), that its PID file is written, and that teardown kills it.
* Integration: localize a deliberately under-provisioned disk with a highly compressible input
  ⇒ assert the daemon grows it, the download completes, and the hash verifies; then repeat with
  the daemon disabled ⇒ assert the downloader blocks on `ENOSPC` and retries rather than
  failing.
* Preemption: kill the VM mid-resize ⇒ assert the requeued task restarts the daemon and the
  already-grown size is retained.

### 12.5 Note on heredocs in emitted scripts
§1 states that `debug.sh` rewriting makes heredocs unsafe. `base.py:880-894` shows canine
already emits a `cat <<EOF` heredoc in a localization script, so the constraint is narrower than
stated: `sed -e '$s/ - <<.*$//'` only rewrites the **last line** and only matches ` - <<`
(the `delocalization.py` invocation), while `grep -v '#DEBUG_OMIT'` can corrupt a heredoc **body**
if a line inside it carries that marker. Heredocs are therefore permissible if they are not the
last line and contain no `#DEBUG_OMIT`; the `parallel_download.py` command should still avoid
them, since it is emitted per input and the staged-file approach (§6.3) makes them unnecessary.

### 12.6 Confirmed transcoding semantics (and why the obvious guard fails)
Verified against `cloud.google.com/storage/docs/transcoding`,
`/storage/docs/data-validation`, RFC 9110 §14.1.2/§14.3 and the curl manual:

1. **Transcoding is triggered by the object's metadata, not the request:** it occurs when the
   stored bytes are gzip and the object metadata carries `Content-Encoding: gzip`. It is
   suppressed for a given request by sending `Accept-Encoding: gzip` (or permanently by setting
   `Cache-Control: no-transform` on the object).
2. **A transcoded response omits *both* `Content-Encoding` and `Content-Length`.** This kills
   the guard drafted in §12.3: you cannot detect transcoding by looking for
   `Content-Encoding: gzip`, because the header is *absent* precisely when transcoding is
   happening. The detectable signals are the **missing `Content-Length`** and a `200` (not
   `206`) response to a ranged request.
3. **`Range` is silently ignored on a transcoded object and the whole object is returned** —
   and Google explicitly warns that *"charges are incurred for the transmission of the entire
   object and not just the range requested."* With 8 workers issuing ranged GETs, a
   transcoded object would be downloaded and billed **8 times over**, with no error raised.
   This is the single largest new hazard in the plan and the reason for the mandatory probe
   below.
4. **With `Accept-Encoding: gzip`, everything works properly:** the raw stored bytes are
   served, `Content-Length` is the compressed size, range requests behave normally, and — the
   important part — **the stored `md5Hash`/`crc32c` are valid**, because they cover exactly
   those compressed bytes.
5. **Conversely, transcoded downloads cannot be integrity-checked at all**: *"Decompressive
   transcoding invalidates integrity checking."* The Python client silently skips validation
   with only an INFO-level log. So the way canine localizes such objects today is
   **unverified**, and `check_hash` (§4.7) cannot be honoured on that path at all.
6. **RFC 9110 §14.1.2:** for any server, byte ranges are computed over the *encoded* bytes, so a
   ranged fragment of a `Content-Encoding: gzip` response is a fragment of the compressed
   stream and is not independently decompressible.
7. **`curl --compressed` + `-C -` is a silent-corruption hazard**: the resume offset is taken
   from the local *decompressed* file size but interpreted by the server as an offset into the
   *compressed* stream. Canine does not currently pass `--compressed` anywhere (checked), and
   it **must not be added** to any emitted command.

**Resulting design.**
* **Mandatory range probe before any parallel work.** Never infer range support from the
  host-side `self.size`, which for a gzip object is the compressed size and would happily
  produce a plausible-looking chunk plan. Instead issue a single `Range: bytes=0-0` probe,
  read only the headers, and require `206` + a matching `Content-Range`. On `200`, abort the
  connection immediately and fall back to single-stream (§4.8). This bounds the billing
  exposure to one aborted request instead of 8 full objects, and it also catches any other
  server that ignores `Range` — a strictly more robust rule than sniffing headers.
* **Prefer the raw path for known-gzip objects.** When the handler knows from object metadata
  that `contentEncoding == "gzip"`, send `Accept-Encoding: gzip` and download the **compressed**
  bytes in parallel. That path is fully rangeable, its `Content-Length` matches `blob.size`, and
  the stored checksums verify **exactly** — so it is the only way to satisfy `check_hash` for
  these objects, and it is strictly better than today's unverified behaviour. Then run a single
  sequential `gunzip` into the final file. Peak disk is compressed + decompressed, which is what
  the resize daemon (§12.2) exists to absorb; delete the compressed file immediately after.
  The decompress pass must be resumable-by-restart (write to a temp name, `fsync`, atomic
  rename) — a preemption there re-runs it from the verified compressed file, never re-downloads.
* **Fall back to single-stream transcoded download** when the object's encoding cannot be
  determined (e.g. a signed URL with no metadata access) *and* the probe reports no range
  support. This is exactly today's behaviour, and `check_hash` must then fail loudly rather
  than silently skip (§4.7) — matching the user requirement that a requested integrity check
  always happens.
* **Add the probe result to the emitted-script contract tests** (§8) and add a fake server that
  mimics GCS transcoding — ignores `Range`, returns `200` with no `Content-Length` — asserting
  the downloader never issues more than the one probe request against it.

---

## 13. Corrections found during implementation

Recorded here because the claims above are cited as verified, and three of them are not.
Each was checked against source or against the running tool rather than against docs.

### 13.1 `gcloud storage cp` DOES integrity-check gzip-encoded objects (corrects §12.6, §12.3)

§12.6 point 5 says "transcoded downloads cannot be integrity-checked at all", and §12.3
concludes from that (plus "gcloud almost certainly cannot slice a transcoded object") that
`HandleGSURL` downloads of such objects are unverified and stay single-stream. Verified
against the Google Cloud SDK's own source (578.0.0), the opposite is true:

* `command_lib/storage/tasks/cp/file_part_download_task.py`,
  `_disable_in_flight_decompression()` returns True for a gzip-encoded object on a
  resumable-or-sliced download, with the comment *"Decompressing in flight changes file
  size, making resumable and sliced downloads impossible."*
* That sets `do_not_decompress=True`, which in `api_lib/storage/gcs_json/client.py`
  sets `additional_headers['accept-encoding'] = 'gzip'`.
* So GCS serves the raw **stored** bytes -- no transcoding -- and the digesters hash
  exactly the bytes the stored md5/crc32c cover, so validation succeeds. md5 is used when
  present; sliced downloads use per-component crc32c with a combined check in
  `FinalizeSlicedDownloadTask`.
* `command_lib/storage/gzip_util.decompress_gzip_if_necessary()` then gunzips the
  temporary file locally into the final destination.

In other words gcloud already implements exactly the "raw path" §12.6 recommends building,
and it *can* slice a gzip object, precisely because ranges work normally once
`accept-encoding: gzip` suppresses transcoding.

What survives from §12.6 is the raw-HTTP fact (a client that receives decompressed bytes
cannot check them against the stored hash) and the note about the `google-cloud-storage`
Python client, which is a different code path from the CLI.

Consequences:
* §12.3's disk-sizing problem is unaffected and still real: gcloud writes the
  *decompressed* bytes, so sizing from `blob.size` under-provisions. That is what the
  transcoding-aware estimate and the resize daemon address.
* The "prefer the raw path" work is **not** needed for `gs://` inputs. It would only matter
  for handlers that do not go through gcloud, and those cannot determine an object's
  encoding anyway (a signed URL exposes no metadata).
* Unverified: for a **one-shot** (non-sliced) download `_disable_in_flight_decompression`
  returns False, so `accept-encoding: gzip` is not set and the server may transcode. Since
  the sliced threshold is now pinned to 64 MiB (§7 tuning), objects at or above that take
  the safe path. Whether gcloud suppresses validation for a transcoded one-shot download
  was not traced.

### 13.2 A single-stream fallback must not inherit a preallocated working file

Not covered above, and a silent-corruption bug rather than a design nicety.

The legacy commands resume from the destination's own size (`curl -C -`, and the S3
`--range "bytes=$SZ-"` form). The chunked path creates its working file at full apparent
size upfront, so handing that to `curl -C -` makes it report "already fully downloaded",
**exit 0**, and accept a file of zeros -- verified directly against curl. Nothing
downstream notices unless `check_hash` happens to be set.

This is reachable: the downloader creates the sparse file, then discovers mid-download that
the server does not honour Range, and hands over to the fallback. The fix is to discard the
working file before the handover, keyed on the manifest's presence -- a genuine partial from
a previous single-stream attempt has no manifest, and its progress must survive, which is
the case `curl -C -` exists to serve.

Note the consequence for §4.8 ("the fallback path keeps its resumability"): *with* that
guard in place, size-based resume in the fallback is sound, so §4.4's instruction to remove
`HandleAWSURL`'s `stat`-based append-resume applies only to the chunked path. Removing it
from the fallback as well would make that path non-resumable for no benefit.

### 13.3 Emitted commands are bash, and must be run as bash

Emitted commands use `[[ ... ]]` (the md5 gates) and process substitution (`>(cat >> dest)`
in the S3 fallback). Python's `subprocess(shell=True)` uses `/bin/sh`, which rejects both.
Verified: `sh -n` cannot even parse the emitted AWS command -- *"syntax error near
unexpected token `('"* -- where `bash -n` accepts it. So anywhere an emitted command is
executed, the interpreter has to be named explicitly.

Two cases, with different weight:

* **`parallel_download.py` -- necessary.** It runs on the compute node, and jobs there
  execute *inside* the slurm_gcp_docker worker container: `worker_startup_script.sh` runs
  slurmd via `docker run ... -v /mnt/nfs:/mnt/nfs`, and `localization.sh` is a subprocess of
  that entrypoint. The image is `ubuntu:22.04`, so `/bin/sh` is dash. This process newly
  shells out to handler-authored strings (`--legacy-cmd`, the DRS URL refresh, the per-chunk
  `aws s3api` call), which nothing did before.

* **`nfs.py` / `local.py` -- hardening, not a bug fix.** These execute an emitted command
  directly via `subprocess.check_call(shell=True)` for URL inputs. Two things bound the
  exposure. It is not the usual path: `localize_file` is reached for a URL input only from
  `pick_common_inputs`, i.e. an input duplicated across jobs, while a single-use URL input
  goes into `localization.sh`, which starts `#!/bin/bash`. And it does not run in the
  worker image: canine *starts* the container (`dockerTransient.py`, `dkr.containers.run`)
  and reaches into it with `docker exec`, so the localizer's own Python runs on the
  controller VM host. That host comes from the private `wolf-template` image, whose
  `/bin/sh` is not determinable from these repos -- **open question, settled by `ls -l
  /bin/sh` on a live controller.** Naming bash removes the dependency either way, but it is
  not repairing an observed failure: S3 localization has worked in practice, which both of
  the above facts independently explain.

The general lesson is worth recording separately: a claim about *where* code runs is not
testable by the test suite, and two of the corrections in this section were initially
misattributed to the wrong side of the container boundary.

### 13.4 Smaller corrections

* The base file-handler class is `FileType`, not `FileHandler` (§6.4).
* `objects.compose` has no `deleteSourceObjects` parameter in the GCS JSON API (§4.7);
  parts must be deleted explicitly after a successful compose.
* Combining per-chunk CRC32Cs (§4.7) needs `crc32c_combine`, which `google-crc32c` does not
  expose -- and is unnecessary, since per-part `md5Hash` comparison plus a metadata-only
  compose already establishes that the destination matches the received bytes.
* §2's `n_chunks = clamp(ceil(size / min_chunk), 1, connections)` makes the chunk layout
  depend on the connection count, and therefore `plan_id` too -- which contradicts §4.3's
  requirement that changing `--connections` not invalidate progress. Chunk size is fixed at
  `min_chunk` instead.
* There is no ` - <<` heredoc at the `delocalization.py` invocation (`base.py`), so §1's
  heredoc hazard is narrower than stated; §12.5's account is the accurate one.
* `shutil.copy` cannot be used to stage scripts (§6.3): it calls `copymode` internally, so
  on a filesystem that refuses chmod the copy itself raises, defeating the very
  `try/except` the instruction pairs it with. `copyfile` plus an explicit tolerant chmod is
  required.

### 13.5 Measured header behavior of a gzip-stored GCS object

Everything above about transcoding was reasoned from documentation. Here is what a real
gzip-stored object in a Getz Lab reference bucket actually returns, via a signed URL:

```
content-encoding: gzip
cache-control: no-transform
etag: "853f4cd545dcefd9a537546f82bd6d2a"
x-goog-stored-content-encoding: gzip
x-goog-stored-content-length: 6266373
x-goog-hash: crc32c=cYnNEg==
x-goog-hash: md5=hT9M1UXc79mlN1Rvgr1tKg==
accept-ranges: bytes
```

`Accept-Encoding: gzip` made **no difference** -- both responses were byte-identical.

Four facts, three of which contradict assumptions made earlier in this plan and in §13.1:

1. **There is no `Content-Length` at all**, on either request. The size is in
   `x-goog-stored-content-length`. §13.1 assumed the raw path would supply a
   Content-Length; it does not. Requiring one made such objects raise
   *"Could not get file header size"* -- a real, **pre-existing** failure, since the
   original code's `grep -i Content-Length` did match the `x-goog-` header but the regex
   it then applied was anchored at position 0, so the match failed and the bare `except`
   produced the same error.
2. **`Content-Encoding: gzip` IS present**, so §12.6's "a transcoded response omits both
   Content-Encoding and Content-Length" is not what a compressed body looks like here.
3. **`cache-control: no-transform` is set on the object**, which per the GCS docs
   *suppresses* transcoding. So both responses are the raw/stored path, and this data says
   nothing about a transcoding-*eligible* object. That case remains unverified -- do not
   treat §12.6's account of it as established.
4. **The `x-goog-hash` md5 decodes to exactly the ETag**, and both cover the stored
   (compressed) bytes. `accept-ranges: bytes` is present, so ranges work on the compressed
   representation.

Implemented accordingly, following gcloud's pattern (§13.1): request the stored bytes,
download them in parallel, verify against the stored digest, then decompress into the
destination with an atomic rename so the step is resumable-by-restart. The compressed
sidecar is deleted immediately, since peak disk is compressed + decompressed.

Note the two sizes are now genuinely distinct and must not be conflated: the *compressed*
length drives the chunk plan and the range requests, while the *decompressed* length is
what the localization disk must be sized for (§12.3's estimate, plus the resize daemon).

### 13.6 The missing Content-Length is Google-specific; do not generalize from it

§13.5 is measured fact about GCS, and it is tempting to turn it into a general rule about
gzip responses. That would be wrong twice over.

**Content-Length is the standard header and most servers do send it**, gzip or not. Per
RFC 9110 it describes the *encoded* body, i.e. exactly what crosses the wire, which is what
the chunk plan needs. So it must be preferred, with `x-goog-stored-content-length` only as
a fallback for the GCS case that omits it. Notably S3 always sends Content-Length,
including for a gzip-stored object.

**`Content-Encoding: gzip` alone must NOT trigger decompression.** S3 does not transcode:
it serves a gzip-stored object as stored, with that header and an ETag over those same
bytes. The historical behavior for any such URL is that `curl -C - -o` (no `--compressed`)
writes the bytes as received, so the localized file has always been the *compressed* one.
Decompressing on the strength of the header would silently change what lands on disk for
those inputs -- the kind of change that breaks a pipeline for no visible reason. So
decompression is keyed on GCS-specific positive evidence
(`x-goog-stored-content-encoding: gzip` *and* the body actually arriving encoded), which
is the only combination whose semantics have been measured.

**A digest, separately, is usable more often than an earlier rule allowed.** The predicate
is whether the bytes landing on disk are the bytes the digest covers. Nothing in this stack
decompresses in flight (urllib does not; curl without `--compressed` does not), so what is
received is what is stored and the digest normally applies -- including for a gzip-stored
S3 object, whose ETag was being refused for no reason. The single exception is transcoding:
stored compressed but served decompressed, so the digest covers bytes never seen. That is
detected by the stored-encoding header being present while `Content-Encoding` is absent --
which also happens to be the only reliable signal for transcoding, since `Content-Encoding`
is absent precisely when it occurs.

**What about a standard server's transport compression?** That is the case an earlier
version of this section omitted, and it needs its own treatment rather than being folded in
with S3. Per RFC 9110 a `Content-Encoding` is a property of the representation and the
recipient is expected to decode it, so the logical entity there IS the decompressed
content -- the opposite of the S3 conclusion above.

It largely does not arise, for a measurable reason: **we never opt into transport
compression.** Verified -- urllib sets `Accept-Encoding: identity` on every request, and
curl sends no such header at all. A server doing on-the-fly compression (`nginx gzip on`)
only compresses for clients that ask, so it will serve us identity. The probe is now sent
with an explicit `Accept-Encoding: identity` so that it asks the same thing the download
does; those two are *not* equivalent under RFC 9110 (omitting the header means any coding
is acceptable and permits the server to compress, while `identity` asks it not to), and
leaving them unmatched would let the probe describe a representation we never download.

So a `Content-Encoding: gzip` arriving *despite* an identity request means the server is
handing over a stored-compressed representation it will not decode for us -- which is the
S3/GCS situation, not on-the-fly compression. Two sub-cases, indistinguishable on the wire
and treated alike:

* **pre-compressed static** (e.g. nginx `gzip_static`, or S3 with gzip content metadata):
  `Content-Encoding: gzip` plus a normal Content-Length, digest over the stored bytes.
  Left compressed, matching the historical `curl -C -` result, and verifiable.
* **on-the-fly compression that ignored our identity request**: typically chunked with no
  Content-Length, and no precomputable digest. No Content-Length means this raises
  *"Could not get file header size"* -- the same outcome as before this work, since the
  original code required Content-Length too. Failing is the right outcome here; the
  concerning alternative would be proceeding with a size that describes other bytes.

The residual known gap is that for a pre-compressed-static URL the localized file is the
compressed one, whereas strict HTTP semantics say the entity is the decompressed content.
That is a deliberate choice to preserve existing behavior, not an oversight -- decompressing
on the strength of the header would silently change what lands on disk for URLs that
pipelines already consume. Revisiting it should be a explicit, announced change.

Summary of the three independent questions, which an earlier implementation had conflated
into one flag:

| | what it is served as | decompress? | digest usable? |
|---|---|---|---|
| plain object | as stored | no | yes |
| standard server, on-the-fly gzip | never happens: we request `identity` | n/a | n/a |
| standard server, pre-compressed static | as stored, `Content-Encoding: gzip` + C-L | no (behavior preserved) | **yes** |
| S3 gzip-stored | identical on the wire to the row above | no (behavior preserved) | **yes** |
| GCS gzip-stored, `no-transform` | as stored, both headers, **no C-L** | yes | yes (before decompressing) |
| GCS transcoded | decompressed, no encoding header | n/a | **no** |
| compressed, no C-L at all | -- | raises, as it did before | -- |

Note the S3 row and the pre-compressed-static row are indistinguishable from the response
alone, which is why the rule is expressed in terms of headers rather than provider.

### 13.7 Signed URLs had their query string in the localized filename

Pre-existing in `HandleOtherURL`, whose filename regex took everything after the last `/`.
An S3 SigV4 URL therefore localized to a file named
`sample.bam?X-Amz-Algorithm=...&X-Amz-Signature=...`. Three distinct consequences: such a
name typically exceeds the 255-byte filename limit; the signature changes on every attempt,
so the name is unstable; and because the localization disk's name hash is built from input
basenames, an unstable basename also defeats disk reuse and job avoidance.
`HandleGCSSignedURL` already stripped the query, so the generic handler was the outlier.

Incidentally the URL used to measure §13.5 was a **GCS** URL signed with AWS SigV4 (the S3
interoperability API) -- the `X-Amz-*` parameters and `x-amz-checksum-crc32c` response
header do not imply the object is in S3.

### 13.8 Decision: a gzip-encoded body is decompressed locally

Decided by the pipeline owner, overriding the "preserve existing behavior" option in
§13.6: `Content-Encoding: gzip` means the localized file is the DECODED content, because
the task reading it expects the data rather than a gzip stream. This is RFC 9110's reading,
and it also removes the inconsistency where the same object arrived decompressed over
`gs://` (gcloud decompresses locally) but compressed via a signed URL.

This was initially flagged as a behavior change needing an announcement. Measuring it
against the old code says that was overcautious:

| name | metadata | OLD | NEW | |
|---|---|---|---|---|
| `d.vcf` | consistent (gzip-encoded) | gzip | plain | changed |
| `d.vcf.gz` | **inconsistent** (single, C-E by mistake) | gzip | gzip | **unchanged** |
| `d.vcf.gz` | consistent (doubly wrapped) | gzip(gzip) | gzip | changed |
| `d.dict` | GCS stored-gzip (no C-L) | **raised** | plain | was broken |

The metadata-inconsistency case -- an already-compressed file whose content-encoding was
set by mistake, and the one most likely to exist in the wild -- **does not change at all**,
because the double-compression rule keeps those bytes as-is, which is what the old code did
by accident. The cases that do change require a pipeline to have depended on canine's old
*inconsistent* output, i.e. gunzipping a file named `.vcf`. And the GCS stored-gzip case
previously raised outright, so there is no prior behavior to preserve -- that is the case a
real reference object in the bucket falls into, making it a fix rather than a change.

The property that matters to consumers is that several pipelines dispatch on the file
extension, which needs exactly one thing: **the localized file is what its name says.**
That is what the double-compression rule produces, asserted across eight name/content
combinations on both routes. So for extension-dispatching callers the change is not merely
neutral but strictly better -- the single case that moved was a name lying about its
content, which is precisely what extension dispatch cannot survive.

Supporting measurement: the consumer repo references `gs://` 48 times against `https://`
five times, none of the latter being data inputs, and `gs://` goes through `HandleGSURL`
and gcloud, which this does not touch.

Two consequences that are easy to miss:

**Every route has to decompress, not just the parallel one.** Which route runs depends on
configuration and on whether the server honors Range, so if the primary and the fallback
disagreed the localized file would differ for reasons invisible to the pipeline. The
fallback is therefore a pipeline: download to a compressed sidecar, verify *those* bytes,
decompress, drop the sidecar. Verification has to happen on the sidecar -- the advertised
digest covers the compressed bytes, so a gate applied after decompression would compare the
decoded file against a digest of the encoded one and fail every correct download. And
deliberately not `curl --compressed`, which with `-C -` is the silent-corruption hazard
§12.6 describes.

**A name that already promises gzip is ambiguous, and the extension alone cannot resolve
it.** Two different situations produce `Content-Encoding: gzip` on a `.gz` object:

* it was gzipped **twice** -- a `.gz` additionally encoded for transport -- so removing one
  layer yields the original uploaded `.gz`;
* it was gzipped **once** and the content-encoding metadata was set by mistake, a common
  slip when uploading an already-compressed file, so the stored bytes ALREADY are that `.gz`
  and decompressing would leave plain data in a file called `.gz` -- which a downstream
  `gzip -d` would then choke on.

They are distinguished by what one layer of decoding actually yields: if it is itself gzip
(magic `1f 8b`), the object was doubly compressed and the decoded copy is kept; otherwise
the stored bytes are kept unchanged. The invariant either way is that **the localized file
matches what its name says**, which is the property worth stating, since it is what a
downstream tool relies on.

Note this made an earlier warning-only approach obsolete: logging "this may not be what you
expect" put the burden on the operator for a case that is decidable from the bytes.

**`.gz` is not the only extension that implies gzip, and that mattered.** BGZF is a gzip
container by design so that standard readers work, which means `.bam`, `.bcf` and the
`.bai`/`.tbi`/`.csi` indices are all gzip-magic -- verified locally against bcftools
output, all beginning `1f 8b`. A gzip-only extension list therefore had a real bug: a
`.bam` served with `Content-Encoding: gzip` would be decompressed into a raw BAM stream,
which is not a valid `.bam` and which samtools cannot read. Given the volume of BAMs this
stack moves, that was the most likely case to actually bite.

The fix is not a longer gzip list but the right generalization: **the name says what the
DECODED bytes should be, and gzip is only one possibility.** A table maps extensions to
expected magic -- gzip family (including the BGZF names above), `BZh` for bzip2,
`fd 37 7a 58 5a 00` for xz, `28 b5 2f fd` for zstd, `PK` for zip, `CRAM` -- and anything
unlisted is treated as promising plain content. A gzip-only magic check could not express
the bzip2 case at all: a `.bz2` wrapped in transport gzip decodes to bzip2, not gzip, yet
that is exactly what the name promises.

The table is necessarily incomplete, which is stated in the code rather than implied away.
Both error directions produce a file a downstream tool rejects outright, not silently wrong
data, so an unlisted format fails loudly rather than corrupting quietly.

**A server can also lie about the encoding.** `Content-Encoding: gzip` over bytes that are
not gzip is the server's metadata being wrong, not a reason to fail the localization, so
the stored bytes are kept. This is distinguished from a broken transfer, which still
fails: a bad gzip *header* raises BadGzipFile (mislabelled metadata), whereas a valid
header with a truncated body raises EOFError (incomplete download). Verified both.

**The standalone-script constraint is one-directional, which is easy to overstate.**
`parallel_download.py` must not import canine, so it stays runnable by hand on a node where
canine is absent -- but it is a module inside the package, so `file_handlers.py` importing
FROM it is an ordinary intra-package import. Verified: the downloader imports only stdlib
and nothing relative, so there is no cycle, and it costs ~31 ms against a canine chain that
already takes ~2.5 s pulling in google.cloud.storage and pandas.

So the format table, the `expected_magic` helper and the connection/chunk defaults are
single-sourced in the downloader and imported, not duplicated. This matters beyond tidiness
for the table: the emitted fallback and the downloader have to agree about which files are
ambiguous, or the localized file would depend on which route ran -- and that is now true by
construction rather than being a property a test has to check. The test that remains
asserts *identity* rather than equality, since equality would still pass if a copy were
reintroduced.

### 13.9 §4.7's per-part digest comparison had never executed

The bucket-compose route compares each part against the md5 GCS reports for it before composing, so a
corrupt part is caught without reading the composed object back. That comparison read
`None` on every part from the day it was written.

`BucketChunkSink.chunk_done` calls `record_part_digest` and then `record_chunk_done`, and
the latter *replaced* the chunk's record rather than merging into it:

```python
record = {"done": True}
self.state["chunks"][str(index)] = record   # wipes the md5 just recorded
```

The fix is a merge. What makes this worth recording is not the bug but why it survived:
every the bucket-compose route test passed with the comparison dead, because the full read-back
verification still caught corruption. The optimization was skipped, the outcome was
unchanged, and nothing failed. It surfaced only from a test asserting that manifest
records survive a round trip — i.e. asserting the mechanism ran, not that the result was
right.

**Rule.** A verification step that exists to make another check cheaper needs a test that
it *ran*. Correct output does not distinguish "the cheap check passed" from "the cheap
check never happened".

### 13.10 Reading resume state must not be fatal when writing it is not

`GcsManifest.flush` deliberately swallows failures: losing a manifest update costs
re-uploading parts, never correctness. `load` was left raising, and that asymmetry was a
real defect, because `load` runs *before* the downloader's own error handling. An
exhausted `TransientError` there escaped to `main`'s catch-all, was logged as an
"unexpected failure", and returned exit 1 — canine's do-not-retry. A transient GCS blip
would have permanently failed the job, which is the exact opposite of the design's
premise.

Tolerance costs nothing here: if the service is genuinely unreachable, the first upload
fails moments later and reports itself correctly, with progress accounted for. Failing to
read resume state means starting fresh, which is wasteful and safe.

### 13.11 A unit test was reaching real GCS with real credentials

The Route-B-writes-nothing-in-place test forced the route with no fake behind it and
asserted the run failed without writing locally. Once the manifest moved into the bucket,
that test began issuing live requests to `storage.googleapis.com` — `GcsClient.token`
falls back to `gcloud auth print-access-token`, which succeeds on any developer machine.

Two lessons, the second more important than the first:

1. Anything touching `GcsClient` in a test must patch the endpoints *and* the token. The
   fallback chain is designed to find credentials, and it will.
2. Asserting a safety property on a run that *fails* proves much less than asserting it on
   one that succeeds. A run that dies early has not yet had the opportunity to write in
   place. The property now runs against the fake on a completed transfer.

### 13.12 What the integration benchmark exists to settle

`canine/test/benchmark_localization.py` covers §8.5. Recording the open questions plainly,
since none of them are answered by the 794-test suite:

| Question | Status |
|---|---|
| ≥4× speedup; the right `connections` default | unmeasured |
| NIC vs. persistent disk as the limit | §2 reasoned it, never measured |
| Memory bounded at ~1 MiB per connection | unmeasured at scale |
| `SEEK_HOLE` on the localization disk | **fails on APFS** — every local run used the 8 MiB checkpoint fallback, so the frontier path has never run |
| `FALLOC_FL_PUNCH_HOLE` page-cache loss | Linux-only, skipped locally |
| the bucket-compose route against real GCS (auth, sessions, compose) | never touched real infrastructure |
| the stage-publish route against a real gcsfuse mount | never touched real infrastructure |
| `/bin/sh` on the controller image | unknown; §13.3 is why it matters |

The `probe` subcommand answers the last five rows' preconditions in seconds at no cost,
and should be run before anything else.

Forcing a real preemption mid-localization cannot be driven from the VM being preempted,
and needs the SLURM requeue path to observe the resume, so it stays a manual procedure —
`benchmark_localization.py preempt` prints it. The case worth engineering deliberately is
a kill *between* the download finishing and the disk being labelled `finished=yes`: the
`.k9pdl.done` marker should make that resume without re-downloading.

### 13.13 Where the §8.5 benchmark must actually run

On a mock-up node built from the `slurm_gcp_docker` image — but the details decide whether
the numbers mean anything.

**Inside the container, not on the host.** `worker_startup_script.sh:60` bind-mounts
`/mnt/nfs` and *not* `/mnt/rwdisks`. The localization script runs as a SLURM job step and
`slurmd` runs inside the container, so the localization PD is mounted in the container's
mount namespace and does not exist on the host. Probing the host reports the host's
`/bin/sh`, the host's tool inventory, and no localization disk:

```
docker exec slurm <path>/benchmark_localization.py probe
```

This also settles §13.3 empirically rather than by argument: the image is `ubuntu:22.04`,
so `/bin/sh` inside the container is **dash**, which is exactly the shell that rejects the
`[[ ]]` and process substitution the emitted commands use.

**The node must be created as `n1-standard-8` explicitly.** That shape does not come from
the worker default — `gcpTransient.py:52` defaults `worker_type` to `n1-highcpu-2`, which
is 2 vCPU and roughly 4 Gbps, a quarter of the egress ceiling §2 reasons from. The
`n1-standard-8` in §2/§8.5 comes from `LocalizeToDisk` pinning
`partition: n1-standard-8` with `--exclusive` (`wolF/wolF/localization.py:35`). Benchmarking
a default worker would tune `connections` against the wrong NIC.

**The disk type is likely the headline result, not `connections`.** `base.py:1002` creates
the localization disk as `--type pd-standard`, sized at `1 + size/0.95e9` GB — about 54 GB
for a 50 GB object. pd-standard sustained write throughput is provisioned *per gigabyte*
(GCP documents ~0.12 MB/s/GB), so a disk that size sits in the tens of MB/s against a
~2 GB/s NIC.

If that holds, the sweep on the `LocalizeToDisk` path is a **flat line** and the ≥4× target
is unreachable there — because of the disk, not the downloader. §2 anticipated the PD as
the limiter "beyond ~16 streams"; the provisioned-size arithmetic suggests it binds far
earlier. Two consequences:

* Benchmark the destinations **separately**: an NFS destination (where ≥4× is plausible)
  and the pd-standard localization disk (where it may be disk-bound). They have different
  ceilings and averaging them hides both.
* If the disk is confirmed as the limit, the highest-value change for `LocalizeToDisk` is
  not tuning `connections` but the disk `--type`/size — a one-line change in
  `create_persistent_disk` with a real cost tradeoff, and a decision that belongs to
  whoever owns the billing.

Note also that the dynamic resize daemon (§12.2) grows the disk during localization, so on
pd-standard the write ceiling *rises mid-run* and a single average throughput figure is
misleading. Use the sampled peaks.

**It can be benchmarked before the Phase 1 Python upgrade.** The image currently ships
Python 3.8, and `parallel_download.py` is stdlib-only and parses cleanly under 3.8 (checked
with `ast.parse(..., feature_version=(3, 8))`). So §8.5 is not blocked on the image's
3.8 → 3.14 migration, which is useful because it means the measurement can inform the
`connections` default before the rest of the stack moves.

### 13.14 The real workload is 300 GB, and the disk is the ceiling

The impetus for this work is **~300 GB BAMs from non-GCS sources taking upwards of 4
hours** — about 21 MB/s. §2 and §8.5 were written around ~50 GB, which understates the
problem and, more importantly, hides where the limit actually is.

`create_persistent_disk` sizes the localization disk at `1 + size/0.95e9` GB, so 300 GB
gets a **316 GB** disk, and `base.py:1002` creates it as `pd-standard`, whose throughput
GCP provisions per gigabyte at ~0.12 MB/s/GB:

| Disk type at 316 GB | Write ceiling | Best case for 300 GB | RO fan-out |
|---|---|---|---|
| `pd-standard` (today) | ~38 MB/s | 2.2 h | **unlimited** |
| `pd-balanced` | ~89 MB/s | 0.94 h | max 10 VMs |
| `pd-ssd` | ~152 MB/s | 0.55 h | max 10 VMs |

Today's 21 MB/s is already **55% of the pd-standard ceiling**. So the parallel downloader,
however well it works, wins at most **1.8×** on this path — 4 h to ~2.2 h. **§8.5's ≥4×
target is unreachable on `pd-standard` at this object size, and no `connections` value
changes that.** The connection sweep should be expected to plateau, and that plateau is a
property of the disk rather than a defect in the downloader.

**`pd-standard` is not negotiable, and this corrects an earlier recommendation of mine.**
I proposed switching to `pd-balanced` on cost-per-hour grounds; that was wrong. Only
`pd-standard` attaches read-only to an unlimited number of VMs — the others cap at 10 — and
unlimited fan-out is precisely what the rodisk exists to provide. Content-addressed rodisks
are also reused across runs, so fan-out is a property of a disk's whole lifetime and cannot
be predicted from the shard count of the job that created it; choosing the type per-task
therefore does not work either.

The downloader is still worth shipping on its own merits, and for a reason beyond the 1.8×:
it **changes which resource is scarce.** Today the network binds at 21 MB/s; afterwards the
disk binds at 38. That reordering is what makes the disk the next thing worth attacking, and
it is not observable until the downloader lands.

Two ways past the ceiling, neither of them a disk-type change:

1. **Snapshot conversion** — download to a fast single-attach disk, snapshot it, restore as
   `pd-standard`, delete the download disk. Only the published disk needs fan-out, so the
   writer's type is unconstrained. Note the arithmetic first, because it rules out the naive
   version immediately: any path ending in a *normal write* of 300 GB to pd-standard is
   bounded by 38 MB/s, so download-to-fast-disk plus a copy is 3.1 h — **worse than the 2.2 h
   baseline**. Conversion can only win if snapshot creation and restore happen inside GCP
   rather than through the guest, and are not subject to the guest-visible ceiling. Since
   the download already spends 0.94 h, snapshot plus hydration must finish inside ~1.26 h
   merely to break even. Both rates are unmeasured; the runbook's §4.2 settles it in about
   half an hour. There is also a second-order risk the timings hide: a snapshot-restored
   disk hydrates lazily, so it may read *slower* for every downstream consumer than a
   natively-written one — which would count against the approach even if the conversion
   itself is fast.
2. **A GCS bucket with read caching** (`fuse-localize`), which sidesteps persistent disks
   and has no fan-out limit at all. Being benchmarked separately.

Full procedure, including the two disk experiments that bound everything else:
`canine/test/BENCHMARK_RUNBOOK.md`.

### 13.15 Disk sizing beats disk conversion, and snapshots are not images

Two corrections to §13.14, both from operational facts I did not have.

**Retention is 24-48 hours.** That is long enough to change the arithmetic: the download is
a one-off but the disk bill runs for two days, so nothing that costs storage can be
justified by localization time alone.

**pd-standard throughput is provisioned per gigabyte, so speed does not require a type
change.** A *bigger* pd-standard is faster and keeps unlimited read-only fan-out. For a
300 GB object (natural size 316 GB):

| Disk | Write | 300 GB | VM saved | extra disk @24h | net @24h | @48h | net @48h |
|---|---|---|---|---|---|---|---|
| 316 GB (today) | ~38 MB/s | 2.20 h | — | — | — | — | — |
| 742 GB | ~89 MB/s | 0.94 h | $0.48 | +$0.56 | −$0.08 | +$1.12 | −$0.64 |
| 2000 GB | ~240 MB/s | 0.35 h | $0.70 | +$2.21 | −$1.51 | +$4.43 | −$3.73 |

On localization alone every row loses money. What pays for it is **consumer reads**, at the
same per-GB rate: each task reading the whole 300 GB saves 1.26 h on a 742 GB disk. Break-even
is **1.2 consumers at 24 h, 2.3 at 48 h** — and retention is itself evidence of reuse, since
a disk kept for two days is being read by something. The two variables are correlated.

**Recommendation: ~2.35× oversizing, i.e. 742 GB for a 300 GB object.** That gives 0.94 h,
a **4.3× improvement on today's 4 hours** — clearing the §8.5 target that 316 GB
pd-standard cannot reach at all — for $29.68/month, within 6% of what a 316 GB `pd-balanced`
would have cost, and without pd-balanced's 10-VM cap. If it becomes a code change, note
that sizing for a throughput target rather than for the data over-provisions absurdly for
small inputs (a 10 GB object wanting 89 MB/s would get 742 GB), so it must be a multiplier
with a size threshold, not a target-throughput formula.

**Conversion is now clearly the wrong tool**, on three counts rather than one:

1. It cannot shrink — a disk created from a snapshot must be at least the source disk's
   size — so it cannot save the storage cost that is the only argument against oversizing.
2. It delays first-availability under contention. `finished=yes` currently lands the moment
   the download completes; conversion makes every parked workflow wait for download plus
   snapshot plus hydration, and the fast intermediate cannot serve them because its type is
   fan-out-capped. It is least helpful in exactly the many-workflows-one-input case the
   disk-tagging protocol exists for.
3. It splits identity from commit. Today the disk *is* both the work-in-progress and the
   artifact: content-addressed name, `users` meaning "being built", `finished=yes` as a
   single atomic commit on that same object. Conversion introduces a temp disk, a snapshot
   and a final disk, makes "resume building it" ambiguous between a partially-written disk
   and a lazily-hydrating restored one, and leaves cleanup ownership undecided while other
   workers are parked waiting.

**Snapshots and images are different, and I had conflated them.** A snapshot captures the
data on the disk and is billed on used data; an image captures the entire disk and is billed
on the whole provisioned size. This bit the analysis twice:

* the §4.2 extrapolation justified a claim about snapshot *time* with a fact about snapshot
  *billing*. Related, but not the same thing — hence measuring it rather than reasoning
  about it. The extrapolation does not hold for images at all.
* §10's cost model priced disks and VM time but never the intermediate artifact. With an
  oversized source that omission is large: a snapshot of a 2000 GB disk holding 300 GB bills
  ~300 GB, an image bills 2000 GB.

A useful corollary: **snapshot storage is cheaper than disk storage for a retained
artifact**, since one is billed on data and the other on provisioned size. Not a proposal —
the shrink floor and lazy hydration still apply — but it means the retention cost driving
the tables above is a consequence of retaining a *disk* specifically.

### 13.16 Corrections from operational facts: sizing, not converting

Five things the Getz Lab established that I did not have, each of which moved the
conclusion. Recorded together because the sequence matters — the final answer is much
simpler than the intermediate ones.

**1. Retention is 24-48 h for localization disks; a week or more for reference disks.**
Long enough that nothing costing storage is justified by localization time alone.

**2. Read-only fan-out throughput is per-attachment, not shared** (measured: speed does
not degrade as attachments increase). This closes an open question in the runbook and is
good news about the existing design — there was never a scenario where one disk divided
38 MB/s among fifty readers.

**3. A snapshot CAN restore into a smaller disk.** I claimed the opposite and used it to
reach a conclusion. Both wrong: the claim is false, and the runbook already contained a
two-minute test I should have run before relying on it. Snapshots capture the data and bill
on used bytes; images capture the whole provisioned disk. The restore-floor behaviour I was
describing belongs to images, and I had conflated the two.

**4. Read speed only helps IO-bound consumers.** A process-bound task computes while it
reads and does not care what the disk can do, so the per-consumer read saving is zero for
those. The number that decides whether oversizing is cost-positive is the **IO-bound**
consumer count, not the consumer count.

**5. There is no snapshot-and-rehydrate step**, so cost is simply provisioned size ×
lifetime.

#### The conclusion: oversize the disk, do not convert it

pd-standard is required for unlimited fan-out, but its throughput is provisioned per
gigabyte — so the speed is available from a *bigger pd-standard*, with no type change, no
snapshot, and no new failure modes.

| Disk | Write | 300 GB | saved | extra @48h | $/hour saved |
|---|---|---|---|---|---|
| 316 GB (today) | ~38 MB/s | 2.20 h | — | — | — |
| **742 GB** | ~89 MB/s | **0.94 h** | 1.26 h | $1.12 | **$0.89/h** |
| 2000 GB | ~240 MB/s | 0.35 h | 1.85 h | $4.43 | $2.39/h |

**Worst case for 742 GB — 48 h retention, not one IO-bound consumer — is $0.64 per disk for
1.26 hours off a blocking step.** That is the honest framing: not a cost saving, but buying
pipeline latency for under a dollar. 0.94 h against today's 4 h is **4.3×**, which clears
§8.5's target that 316 GB pd-standard cannot reach at all. With two or more IO-bound
consumers it pays for itself outright.

**Conversion is dead on arithmetic, and the argument no longer depends on anything I might
have wrong.** Because the published disk should be oversized anyway, and an oversized disk
already writes fast, conversion can only buy the gap between downloading on the disk you
will publish (742 GB, 0.94 h) and downloading on a bigger one (2000 GB, 0.35 h): **$0.22**,
less the big disk's own cost and less snapshot plus hydration time. A ceiling of about
eleven cents does not buy a three-object state machine — one that would also replace a
single-object commit protocol (content-addressed name, `users` meaning "being built",
`finished=yes` as an atomic commit) with ambiguous resume rules, and delay
first-availability for every workflow parked under contention.

#### Reference disks may be the bigger problem

The per-GB model predicts a 10 GB reference disk runs at **1.2 MB/s and 7.5 IOPS** — 42
minutes to read a 3 GB reference, on every task that mounts it.

**I doubt this, on their evidence rather than mine:** if reference loading cost 40 minutes
per task it would be the loudest complaint in the pipeline, and it is not. That is real
evidence the linear model breaks down at small sizes. So it is a hypothesis, and the
runbook's §4.1b now sweeps 10-2000 GB to settle it cheaply.

If it *does* hold, it is a larger and far cheaper problem than the BAM download: 10 GB →
200 GB is a 20× speedup for $1.75/week per disk. The catch is that a floor is a standing
cost on every disk — $175/week at 100 concurrent reference disks — so it needs the
concurrent disk count, which I do not have.

Consequently the sizing rule needs a **floor**, not the size threshold I first proposed
(which would leave small disks at exactly the worst size):

```python
disk_gb = max(int(natural_size * 2.35), FLOOR_GB)
```

The multiplier governs the large case, the floor the small one, and they tune
independently.

### 13.17 Retraction: the disk ceiling was probably never real

§13.14-13.16 built an increasingly elaborate analysis — sizing tables, cost models,
break-even-by-consumer-count — on one unverified premise: that GCP's per-gigabyte
pd-standard throughput figures (~0.12 MB/s/GB) govern the localization write. From that
came the claim that a 316 GB disk caps at ~38 MB/s, that today's 21 MB/s is 55% of a hard
ceiling, and therefore that **the parallel downloader can win at most 1.8×**.

**That claim is retracted.** The Getz Lab's evidence is that the throughput limit which
actually bites is on the *transfer*, not on read/write to a disk attached to a VM.

The tell was available the whole time and I reasoned past it. Reference disks are 10 GB and
the same model predicts **1.2 MB/s and 7.5 IOPS** for them — 42 minutes to read a 3 GB
reference, on every task that mounts one. That plainly does not happen; if it did it would
be the loudest complaint in the pipeline rather than the BAM download. I noted the
contradiction in §13.16 and treated it as a curiosity about small disks, when it was
evidence against the model as a whole.

If the limit is on the transfer, then:

* today's 21 MB/s is **source-bound** — a single-stream limitation, which is exactly what
  N parallel ranged GETs address;
* the downloader has **full headroom**, and §8.5's ≥4× target is available rather than
  arithmetically impossible;
* the disk-sizing analysis in §13.16 is **moot**, because the disk was never the constraint;
* §13.15's conclusion survives unchanged but for a simpler reason: conversion was already
  dead on arithmetic, and if the disk is not a bottleneck there is nothing left for it to
  optimise at all.

What remains true regardless: `pd-standard` is required for unlimited read-only fan-out;
read throughput is per-attachment; snapshots restore to smaller disks and bill on used data
while images bill on the provisioned disk; retention is 24-48 h for localization disks and
a week or more for references.

**The gate is §4.1's `dd`**, which is ten minutes and no egress: write and read a 316 GB
pd-standard, and a 10 GB one. Well above 38 MB/s means the per-GB model does not describe
this path and §13.16 can be discarded; close to 38 MB/s means it holds and the sizing
analysis applies. The runbook now leads with that test and marks §10 conditional on it.

**The methodological lesson, which is the durable part.** A documented ceiling is not a
measurement. I treated a published per-GB figure as ground truth, derived a headline
conclusion from it, propagated that conclusion into three sections of cost modelling, and
only abandoned it when told directly — despite having written the ten-minute test that
would have settled it in the same document. §13.9's rule was that a verification which
exists to make another check cheaper needs a test that it *ran*; the analogue here is that
a model load-bearing for a conclusion needs a measurement, and one already in reach is no
excuse for not taking it.

### 13.18 The objective is total cost, and it inverts the sizing recommendation

Two facts: workflows run at scale (**hundreds of unique BAM disks concurrently**), and the
goal is a **decrease in the total cost of localizing**, not merely a faster localization.

Pricing the whole thing — VM-hours plus disk-hours, preemptible workers
(`gcpTransient.py` defaults `preemptible=True`), 48 h retention, 300 disks:

| Scenario | VM | disk | total | ×300 batch |
|---|---|---|---|---|
| today: 4.0 h, n1-standard-8 | $0.32 | $0.83 | $1.15 | $345 |
| downloader 4×: 1.0 h | $0.08 | $0.83 | $0.91 | **$273** |
| …plus 742 GB disk | $0.08 | $1.95 | $2.03 | **$608** |
| …instead, 2-vCPU node | $0.01 | $0.83 | $0.85 | **$254** |

**The disk is the larger line item and the downloader cannot touch it.** At 48 h retention
the disk costs $0.83 against $0.32 of VM time, and retention is set by reuse rather than by
how fast the disk was filled. The downloader's cost ceiling is therefore the VM share —
about 21% of total. It is still worth shipping; it is just not where most of the money is.

**§13.16's 742 GB recommendation is withdrawn.** It rested on two errors pushing the same
way: on-demand VM pricing (~$0.38/h where preemptible is ~$0.08/h, overstating the time
saving nearly 5×) and a single-disk view. The structural reason it can never work: a disk
exists for 48 h but is *written* for a few of them, so buying gigabytes across the whole
lifetime to save time in a small fraction of it does not recover. +$1.12/disk of storage to
save $0.10/disk of VM time is **−$1.02 per disk, −$306 per batch**.

**The largest untapped lever is one nothing in this plan had questioned: the machine type.**
`LocalizeToDisk` pins `n1-standard-8` with `--exclusive`. §2 justified that as "the download
owns the whole node" — which argues that nothing should *compete* with localization, not
that it needs 8 vCPUs and 28 GB of RAM. Localization is pure IO: ~1 MiB buffered per
connection, and md5 over 300 GB is a few minutes of one core. Even a 2-vCPU n1 has a 4 Gbps
(~500 MB/s) egress cap, orders of magnitude above the rates in play. Localizing on a small
node would cost **$0.01 against $0.32** — a bigger saving than the speedup, and it composes
with it. `nodetypes.json` has no 2-vCPU entry, so this needs a partition added before it can
be tested.

**Ranked by effect on total cost:** retention (dominant, but a workflow decision), then
machine type, then localization speed. Oversizing moves it backwards.

#### A process note, which prompted a correction

Having concluded oversizing was uneconomic, I had written "so do not measure it" over §4.2
and marked §4.1b/§4.1c skippable. That was wrong, and the user pushed back: a benchmark
exists to find out what is true, not to confirm a decision already taken. Worse, the
measurement I was proposing to skip is precisely the one that *validates the per-GB
throughput model every cost estimate here depends on* — the model already retracted once in
§13.17.

The runbook now separates the two explicitly: **§4 is measurement, run all of it; §10 is the
decision that consumes it.** §4.2 is marked optional with its rationale (no result can change
the conversion decision, but snapshot and hydration rates are unmeasured and worth knowing
for their own sake) rather than discouraged.

### 13.19 Verification is a full read-back, and multipart hashing is parallel

Prompted by a correction: ETag/md5 for S3 files uses more than one core. Chasing that found
two real defects, and one gap between this plan's stated intent and the implementation.

**§5's claim that snapping to part boundaries avoids the read-back is only true on the bucket-compose route.**
The per-part digest machinery (`_digests`, `part_digest`, `record_part_digest`) lives on
`BucketChunkSink`. On the in-place route — the in-place POSIX write, which is what `LocalizeToDisk`
actually uses — `verify()` unconditionally re-reads the whole object. So a 300 GB BAM is
downloaded, then **read again in full** to hash it. `HandleAWSURL`'s comment claims this
"replaces the post-hoc multiprocessing md5 pass, saving a full extra read of the object";
that saving is not realized on the route that matters. Documented rather than fixed for now,
since threading in-transfer digests through the POSIX sink is a larger change.

**`multipart_etag` buffered an entire S3 part in memory.** It called `_read_exactly` into a
bytearray, so peak memory was a property of how the uploader happened to chunk the object —
S3 parts run from 8 MB to several GB. Now each worker holds `READ_BUFFER` bytes and hashes
incrementally, so memory is `workers × 8 MiB` regardless of part size. `_read_exactly` had
no other caller and is gone.

**Multipart hashing is now parallel, which is the substance of the correction.** Each part's
md5 is independent, so the read-back is embarrassingly parallel; `hashlib` releases the GIL
for buffers this size, so threads genuinely overlap rather than interleave. `verify()`
passes `connections` as the worker count. A **whole-file** md5 (single-part object, or a
plain URL with a supplied md5) is inherently sequential over the byte stream and cannot be
parallelized at all — so those two cases have very different CPU profiles, which is worth
knowing when sizing the node.

Fourteen tests: agreement with a straightforward implementation across five part/size
alignments including ragged tails, invariance to worker count from 1 to 64, reads bounded by
`block` rather than `part_length`, empty-file and short-file behaviour, and that `verify()`
threads the connection count through.

**Consequence for §13.18's machine-type argument.** I had dismissed localization as pure IO
needing "a few minutes of one core". That was wrong: verification is a full 300 GB re-read,
multi-core for multipart ETags and single-core-but-long for whole-file md5. The runbook now
asks §6.3 to record the **download/verify split**, because that ratio is what decides whether
a smaller node is viable — a large verify share means the cores are doing real work.

### 13.20 Scope: the scale figure is arithmetic, not a test

Hundreds of concurrent unique BAM disks is why per-disk cost differences matter (§13.18), but
nothing in the benchmark should try to reproduce that scale. Single-worker measurement is
sufficient and is what the runbook asks for.

The standalone contention procedure has been dropped and its one worthwhile check folded
into the forced-preemption section, where a real cluster is needed anyway: resume state
lives in `.k9pdl.json` **on the disk being built**, so a successor worker taking over a
dead builder's disk should inherit it and continue rather than restart. That is a property
of this change and is unverified; a wider contention test is not.

### 13.21 Phase timing, and the §9 operator docs

Deferring in-transfer hashing until measured (§13.19) exposed a gap: the runbook asked for
the download/verify split, and **the downloader logged no timings at all**. The measurement
the decision depends on was not obtainable.

Added a `phase` context manager emitting `k9pdl-phase <name> <secs>s[, <rate>]`, applied to
download/verify/gunzip on the in-place route, relay/compose on the bucket-compose route, and download/verify/publish on
The stage-publish route. `benchmark_localization.py` parses those lines into its JSON and prints a
`download vs verify` summary that says which way the decision falls:

* verify > 25% of wall clock → in-transfer hashing is worth building, and the node's cores
  are doing real work so do not size the instance down;
* verify small → hashing during transfer buys little, and a smaller instance is worth
  investigating.

Six tests pin the format, including one that checks the downloader's output against the
benchmark's own regex rather than a hand-written sample, so a format change on either side
cannot pass silently. A bug caught in the process: the summary gated on
`phases.get("verify")`, and `0.0` is falsy — a *fast* verify, precisely the result arguing
for a smaller node, would have been dropped.

Also written: **`canine/localization/PARALLEL_DOWNLOAD.md`**, the §9 operator guide that
was the last open piece of this plan. Covers the kill switch
(`CANINE_DISABLE_PARALLEL_DOWNLOAD`, per node, no redeploy), the three levels of turning it
off, tuning (`download_connections`, `download_min_chunk`,
`CANINE_DOWNLOAD_CONNECTIONS`), how to read the logs, the exit-code contract, the sidecar
files, and a short triage list.

One thing that guide documents which had not been written down anywhere: **`min_chunk` is
deliberately not environment-overridable** while `connections` is. The chunk size feeds
`plan_id`, so changing it between attempts would make a requeued task discard a good
partial file; `connections` is safe to vary per node precisely because the chunk layout
does not depend on it. That asymmetry is load-bearing and was previously implicit.

### 13.22 The routes are named, not lettered

`ROUTE_POSIX`/`ROUTE_BUCKET`/`ROUTE_STAGED` had the values `"A"`, `"B"`, `"C"`. The
constants were meaningful; the values were not — and it is the values that reach logs,
manifests and the emitted scripts, where `route B` told whoever was diagnosing a
localization nothing about what the code was doing with their bytes.

| Constant | Was | Now | What it does |
|---|---|---|---|
| `ROUTE_POSIX` | `A` | **`in-place`** | writes chunks straight into the destination |
| `ROUTE_BUCKET` | `B` | **`bucket-compose`** | uploads parts, composes server-side |
| `ROUTE_STAGED` | `C` | **`stage-publish`** | stages on a block device, then publishes |

So `route bucket-compose: gcsfuse on /mnt/bucket` instead of `route B: gcsfuse on
/mnt/bucket`.

Schema-safe: `"route"` is written into the manifest but never read back for comparison
(`matches()` uses `plan_id` and size), so an in-flight manifest carrying `"B"` still loads
and `SCHEMA_VERSION` does not move. No test asserted on the letters — they all used the
constants — so nothing needed rewriting to accommodate it.

93 prose references renamed across the code, the two operator/benchmark documents and this
plan. Three tests pin the values, reject any single-character route, and check the chosen
route is named in the log line. Note this plan's older sections now read "the
bucket-compose route" where they once read "Route B"; the letters survive only in commit
messages predating this change.

### 13.23 First measurement from the VM, and why it is not yet interpretable

Uploading the §2 test objects from the node to a same-region bucket in another project:

| Object | Rate | Elapsed |
|---|---|---|
| 12 GiB | 59.4 MiB/s | ~3.5 min |
| 300 GiB | 57.1 MiB/s | ~90 min |

Two observations that are solid, and one temptation that is not.

**Solid: it is a stable plateau.** 4% apart across a 25× size difference, so not a warm-up
artifact. And it is **3% of the ~2 GB/s the NIC should do**, about 2.9× today's 21 MB/s BAM
download.

**Solid: the runbook's estimate was badly wrong.** It said "~10 min at GB/s" for the 300 GB
object; the real figure is ~90 minutes, off by 9×. Corrected, and it reinforces the tmux
step added the same day.

**Not solid: reading 60 MB/s as a single-stream network rate.** There are three candidate
limiters, and the third only surfaced because the lab pointed it out:

1. the generating pipeline (openssl / tee / md5sum);
2. **the node's own boot disk** — §1 created it as 50 GB `pd-standard`, and
   `gcloud storage cp -` may stage a non-seekable input through local disk, since a pipe
   cannot be chunked for a resumable upload without staging;
3. the single-stream rate to GCS.

The boot disk deserves attention in *both* directions, because the arithmetic is striking
either way. The per-GB model predicts a 50 GB `pd-standard` sustains **6 MB/s**. We measured
**60 MB/s**. So either the data never touched the disk — in which case 60 MB/s is a network
figure — or it did, and **a pd-standard disk beat the per-GB model by 10×**, which is direct
evidence for what §4.1 exists to establish, arriving early and by accident.

One minute of measurement distinguishes them: time the pipeline with the upload removed,
and compare `/proc/diskstats` field 10 (sectors written) on the boot device across a 4 GiB
upload. ~4 GiB written means gcloud stages through disk and the number is a disk
measurement; near zero means it streams. Both tests are now in §2, and the parsing was
checked against synthetic `/proc/diskstats` output.

Two changes followed, the first of which §13.24 then reverted: §1's boot disk was raised to
**200 GB** as a precaution (about three cents for a three-hour session, cheaper than
discovering a measurement was disk-bound), and §2 offers building the
300 GB object with server-side `compose` from 25 copies of the 12 GiB one — minutes instead
of 90, at the cost of the composite having no md5, so correctness rides on §6.4 and the
real BAM instead.

**Worth stating plainly: none of this touches the localization measurements.** The
downloader `pwrite`s straight into the destination on the localization disk, `verify()`
reads back from the same place, and §6.1 writes to tmpfs. The boot disk is in the path only
for *generating* the test objects. The lesson is narrower and about method: I had already
written "consistent with the single-stream hypothesis" about a number with three unexcluded
explanations, which is the same eagerness that produced §13.17's retraction.

### 13.24 Measured: the single stream is the limit, and the boot disk is not

The three candidates from §13.23, resolved on the node:

| Candidate | Measurement | Verdict |
|---|---|---|
| generating pipeline | `openssl \| head \| tee >(md5sum)` → 4 GiB in 10.04 s = **408 MiB/s** | not the limit; 6.2× headroom |
| 50 GB `pd-standard` boot disk | **0 MiB written** during a 4096 MiB upload | not in the path at all |
| NIC | 69 MB/s is **3.4%** of ~2 GB/s | not the limit |
| **single stream to GCS** | 57.1 / 59.4 / 65.8 MiB/s across 300 GiB / 12 GiB / 4 GiB | **what remains** |

The elimination is stronger than it looks, because the control pipeline included a full
`md5sum` pass that the upload path does *not* have and still ran 6.2× faster — so 408 MiB/s
is a lower bound on the non-network headroom.

**Two things this settles.** The boot-disk speculation in §13.23 is dead: `gcloud storage
cp -` does not stage a pipe through local disk, so §1 reverts to a 50 GB boot disk and the
precautionary 200 GB bump is withdrawn — never having been applied, since the diagnostic
was run on the original 50 GB disk. That matters for the strength of the result: the
configuration measured is exactly the configuration the runbook specifies, so the
conclusion needs no extrapolation. And the per-GB throughput model gets no evidence
either way from this measurement, since the disk was never involved — §4.1's `dd` remains
the only thing that settles §13.17.

**What it establishes positively:** a single stream between an n1-standard-8 and same-region
GCS moves ~60 MB/s while the NIC can do ~2000. That is the premise this entire project
rests on, measured on the lab's own infrastructure for the first time. Today's 21 MB/s BAM
download is a *third* of even that single-stream figure, which points at source-side
per-connection throttling and means parallelism has more headroom against the real source
than against GCS.

**One confound survives, and §6.1 resolves it for free.** `gcloud storage` is Python and
hashes crc32c inline, so part of the 60 MB/s could be its own overhead rather than the
stream. §6.1's `connections 1` row uses `curl`: if curl also lands near 60 MB/s the stream
is genuinely the limit; if curl reaches 200+ MB/s then gcloud's Python was the limit, the
real single-stream baseline is higher, and the available speedup is correspondingly
smaller. That number is the denominator of every speedup this benchmark reports, so it
wants recording explicitly rather than inferring from the sweep's own ratio.

### 13.25 §13.3 confirmed: /bin/sh in the container is dash

`probe`, run inside the slurm_gcp_docker container on a real n1-standard-8:

```
/bin/sh -> /usr/bin/dash   BASH_VERSION=<not bash>
```

**§13.3 was right, and pinning `executable=/bin/bash` was load-bearing rather than
defensive.** Every emitted command uses `[[ ]]` or process substitution, and `shell=True`
alone would have run them under dash — which rejects both. The bug that prompted §13.3 was
found by reasoning about the image; this is the measurement.

Three other things the probe settled:

* **`python3` is 3.14.6.** The image has already been through its 3.8 → 3.14 upgrade, so
  §13.16's observation that `parallel_download.py` parses under 3.8 is moot — still true,
  no longer load-bearing.
* **8 cpus / 29.38 GiB**, matching `nodetypes.json`'s `n1-standard-8` entry, so the machine
  shape §2 reasons from is the machine shape in play.
* **`gcsfuse` was not in the probed tool list at all**, which is a gap given §6.6 cannot
  mount a bucket without it. Added, reported separately as needed only by the
  bucket-compose and stage-publish routes.

#### A flaw in the probe, found by running it

Both candidate destinations came back as **`overlay`** — `/mnt/nfs` and `/tmp`, each with
the same 38.69 GiB free, because both are the container's own writable layer on the boot
disk. `/mnt/nfs` is not the NFS mount; nothing was mounted there, since this node has no
controller.

The probe nevertheless announced *"the frontier path is live here"* on the strength of
SEEK_HOLE passing on overlayfs. That conclusion is about the image's writable layer, not
about the persistent disk a job localizes to — and a conclusion drawn from the wrong
filesystem is worse than no conclusion, because it reads like an answer.

Fixed by classifying each candidate's backing (`ext4`/`xfs`/`btrfs` → block device;
`overlay` → container-internal; `tmpfs` → memory; `nfs*`/`fuse*` → network) and drawing the
frontier conclusion **only** from block-device-backed candidates. Before §4 creates the
localization disk there are none, and the probe now says exactly that and tells the reader
to re-run afterwards.

Related: `punch-hole : no` on overlay. `FALLOC_FL_PUNCH_HOLE` is unsupported there, so
§6.5's page-cache-loss test — the one case SIGKILL cannot simulate — needs the ext4
localization disk. Another reason the post-§4 probe is the meaningful one.

**The pattern worth noticing:** this is the third time in this effort that a check has
reported on something adjacent to what it claimed to measure. §13.9's per-part digest
comparison ran on `None`; §13.23's upload rate had three unexcluded explanations; here a
capability probe answered for the wrong filesystem. In each case the output looked healthy.

### 13.26 Where a FUSE mount has to happen, and why

Two related questions came up: is `gcsfuse` available on a GCP VM, and would the container
see a host-side gcsfuse mount bind-mounted into it?

**Availability.** Not on this image. `slurm_gcp_docker/Dockerfile` on `wolf-2.0-update`
installs `fuse-overlayfs` only, for podman. `rclone`'s install is present but **commented
out** (lines 93-95), even though `conf/rclone.conf` ships. Note also that the probe runs
*inside the container*, which has its own filesystem — a `gcsfuse` binary on the VM host
would not be visible to it regardless.

**Visibility of a host mount.** Yes, but conditionally, and the conditions are the
interesting part:

1. **Mount propagation.** A plain bind mount is point-in-time: the container sees whatever
   was at the path when it started, so a mount made on the host *afterwards* is invisible.
   Making it visible needs `rshared` propagation on the bind.
2. **FUSE permissions.** FUSE mounts are private to the mounting user, so a container
   process running as a different uid gets `EACCES` without `allow_other`.

canine already does both, on the controller: `dockerTransient.py:151` bind-mounts `/mnt`
and `/dev` with `propagation="rshared"`, and its `rclone mount` passes `--allow-other`
together with `--uid $HOST_UID --gid $HOST_GID`. It also documents a third constraint worth
knowing — "NFS cannot export a nested FUSE filesystem" — which is why the mount is created
at `/mnt/rclone/<ns>` and bind-mounted to `/mnt/nfs/<ns>` rather than made there directly.

**But the worker does none of that.** `worker_startup_script.sh:60` binds only `/mnt/nfs`,
with no propagation flag and no `/mnt`. And canine creates its FUSE mounts *inside* the
container in any case — `rclone mount` is issued through `self.invoke(...)`, not on the
host. So mounting inside the container matches production on both counts, and the runbook's
§6.6 already did that; what it lacked was the reason, and the fact that neither FUSE tool is
installed to do it with.

This also confirms, independently, §13.3's finding about the container boundary being where
these questions get decided: the host's tools, mounts and shell are not the ones in play.

### 13.27 The real source, probed: every precondition holds

`probe` against the 300 GB BAM, on a non-Amazon S3-compatible store (a Gen3/datacommons
endpoint), from inside the container:

| Reported | Value | Consequence |
|---|---|---|
| endpoint source | `aws config` | neither endpoint nor key on any command line (§13.?) |
| credentials | `/root/.aws/credentials [default]` | §3's `~/.aws:ro` mount works |
| size | **278.91 GiB** (299.5 GB) | `create_persistent_disk` provisions 316 GB, as assumed |
| ETag | `2872df08…-9849` | **AWS-style multipart** |
| ranged GET | yes, 1024 bytes for a 1024-byte request | the assumption the design rests on |
| accept-ranges | `bytes` | advertised as well as honoured |
| presign | **yes** | the plain-HTTP path, no `aws` process per chunk |

**The two that were genuinely open are both favourable.** A store that is not Amazon's had
no obligation to follow AWS ETag semantics — an opaque ETag would have meant no hash
verification at all on the primary object (§13.19's opaque branch). It follows them, so
`--check-etag` works and neither `--size` nor `--md5` need supplying anywhere. And presign
works, so every chunk goes through `HttpSource` rather than spawning an `aws` process.

Derived layout: 9849 parts means **~29.0 MiB per part**, so at `min_chunk` 64 MiB
`plan_chunks` snaps to **87 MiB chunks (3 whole parts each)** and the object becomes ~3283
chunks. Manifest overhead at that count is negligible per §13.19's measurement.

#### A composition worth documenting

Because presign works, the production path is a presigned URL — but a URL alone carries no
metadata, so the size and ETag would have to be supplied by hand. Passing **both** solves
it: `--url` carries the transfer while `--s3-bucket/--s3-key` are used for `head-object`
only. Verified: `HttpSource` is selected, and `--check-etag`/`--part-length` are still
derived automatically. The runbook now tabulates which argument combination selects which
source, since picking the wrong one by accident is easy and the difference is one `aws`
process per chunk.

#### And the bug it took a real object to find

The range check crashed on first contact: `UnicodeDecodeError: 'utf-8' codec can't decode
byte 0x8b in position 1`. A BAM is BGZF, so byte 1 is gzip magic, and the check was
capturing the body with `text=True`. Every object this tool exists for is binary; none of
the fakes caught it because their payloads never went through that path.

The same call had a worse latent bug: capturing stdout meant that a store which *ignored*
`Range` — exactly what the check detects — would have had its whole object pulled into
memory. 279 GiB, here. Both fixed by writing the body to a temp file, and the verdict now
requires the right byte count rather than merely exit 0, so "returned everything" reads as
NOT HONOURED instead of success.

### 13.28 Part length: verify the stride rather than assume it

The md5-of-md5s only reproduces the ETag if every non-final part is striden at its **true**
length. `head-object --part-number 1` gives one part's length, and both `HandleAWSURL`
(`file_handlers.py:1189-1205`) and the benchmark take it as the stride for the whole
object.

**S3 does not require parts to be uniform** — only that non-final parts are ≥5 MiB. A
non-uniform upload would therefore produce a wrong md5-of-md5s, and the consequence is not
a missed optimization: `verify()` raises `PermanentError` on an ETag mismatch, `discard()`
deletes the file, and the job exits do-not-retry. A byte-perfect 279 GB download would be
thrown away.

**In practice the constraint holds here** — the endpoint under test uploads uniform parts
except the final one, and the existing code has been tested against it. So this is a guard,
not a fix, and production code is unchanged.

The benchmark now confirms rather than assumes, for two extra HEADs:

* `(count - 1) x first + last == size`. A differing interior part breaks this identity
  unless another compensates exactly.
* `part 2 == part 1`, which closes the compensating cases the sum alone would miss
  (10 + 5 + 15 + 5 sums the same as 4 x 10 + 5).
* `last <= first`, since the short part is the final one.

If any check fails the part length is withheld and ETag verification is **declined** rather
than performed wrongly — better no verification than one that fails on correct data.

Two things this exercise got right by accident and one it nearly got wrong:

* I initially read `localization_command`'s `else` branch as a bug, since
  `PartsCount > 1` with no `PartLength` would verify a multipart ETag as a whole-file md5.
  It is unreachable: the constructor raises if the part-1 HEAD fails, so `PartsCount > 1`
  implies `PartLength` is set. The branch is exactly the single-part case and is correct.
* The confirmation HEADs are **tolerant**. `HandleAWSURL` only ever requests part 1, so an
  endpoint could support that and reject other part numbers; treating that as fatal would
  regress a working configuration in order to run a check. An unavailable confirmation
  downgrades to "unconfirmed" and the part-1 stride is used exactly as before.
* Refactoring `probe` onto the shared metadata helper silently dropped `AcceptRanges`,
  which a test caught. The probe reported `None` where it had reported `bytes`.

The `probe` output now shows the measured stride and the layout it implies, rather than
leaving it to be inferred by dividing size by part count — which understates the stride,
since the final part is short:

```
parts       : 9849 x 29.00 MiB (from --part-number 1; the last is shorter)
chunk plan  : 87.00 MiB (3 whole parts) -> 3283 chunks at --min-chunk 64.00 MiB
```

### 13.29 "Uniform parts" was the wrong name for the right check

For a two-part object, part 1 ≠ part 2 is the **normal** case, not a warning sign: with a
stride `P` and size `S` where `P < S <= 2P`, part 2 is the remainder `S - P`, which equals
`P` only when `S` is an exact multiple. §13.28 nevertheless reported such an object as
`parts_uniform = True`, which is correct but confusingly labelled — and the label invites
exactly the wrong conclusion.

The property being checked is narrower and is the one that matters: **does striding the
file by part 1's length reproduce the real part boundaries**, so that md5-of-md5s
reproduces the ETag? Renamed `stride_verified` / `stride_check` accordingly, and the output
now separates the two numbers instead of implying they are the same:

```
parts       : 6 x 1.00 MiB stride, last part 12.06 KiB (confirmed)
```

Checked the logic against ground truth — computing the real boundaries and comparing them
to the strided ones — across six layouts, and the check agrees in all of them:

| Layout | Stride reproduces boundaries | Check says |
|---|---|---|
| `10 + 5` (typical two-part) | yes | yes |
| `10 + 10` (exact multiple) | yes | yes |
| `5 + 100` (legal, pathological) | **no** | **no** |
| `10, 10, 5` (typical) | yes | yes |
| `10, 20, 5` | **no** | **no** |
| `10, 5, 15, 5` (sum-compensating) | **no** | **no** |

Two observations worth keeping:

* **For exactly two parts the sum identity is vacuous** — `first + last == size` holds by
  definition — so `last <= first` is the only informative check there. It is also
  *sufficient*: with a single non-final part, striding by it gives `[0, first)` and
  `[first, size)`, which are the real boundaries.
* **`5 + 100` is legal S3** and is why that check is not merely defensive. Only non-final
  parts must be ≥5 MiB, so a small first part followed by a large one is permitted, and
  there part 1 is emphatically not the stride.

Also fixed the local fake `aws`, which returned `min(PART, size)` for every part number
rather than the real length of part *n*. That made a uniform object report as non-uniform —
a false signal from test tooling, which is worse than no tooling.

> **Where this landed in git.** The rename has no commit of its own in canine: a working
> directory slip put its intended commit in the *parent* repo as a submodule-pointer bump
> (since removed), and the code was swept into canine's `0076014`, whose message describes
> only the `disk_details` fix it also contains. Not rewritten — an eight-deep reword among
> 46 unpushed commits is disproportionate for a message — so this section is the record of
> the reasoning, and `git log -S stride_verified` finds the code.

### 13.30 §3 complete: every S3-side precondition measured and favourable

Final `probe` against the 279 GB BAM on the Gen3 endpoint. Everything that can be
established without the localization disk now is:

| Property | Measured | Why it mattered |
|---|---|---|
| credentials | `/root/.aws/credentials [default]` | canonical file, nothing on a command line |
| endpoint | via `aws config` | likewise off the command line |
| size | 278.91 GiB (299.5 GB) | `create_persistent_disk` gives 316 GB, as §4 assumes |
| ETag | `2872df08…-9849`, AWS-style multipart | opaque would have meant **no verification at all** |
| **stride** | **9849 x 29.00 MiB, last part 15.40 MiB, confirmed** | a wrong stride discards a byte-perfect download |
| ranged GET | yes, 1024 bytes for a 1024-byte request | the assumption the design rests on |
| accept-ranges | `bytes` | advertised as well as honoured |
| presign | **yes** | plain HTTP path, no `aws` process per chunk |

The stride closes arithmetically: `9848 x 30,408,704 + 16,148,070` reconstructs 278.9135
GiB against 278.91 reported, so the uniformity identity from §13.28 holds on real data
rather than only on fakes.

Derived layout, now measured rather than inferred:

* **chunk = 87.00 MiB**, exactly 3 whole parts, from `align_up(64 MiB, 29 MiB)`
* **3283 chunks**
* manifest ~71 KiB at completion, and ~0.04% of the payload in total manifest writes —
  consistent with §13.19's estimate that chunk-count overhead is negligible at this scale

Two notes for the record. `gcsfuse` is **MISSING** in the image, confirming §13.26 — so the
bucket-compose and stage-publish routes stay unverified here and ride on the separately
benchmarked `fuse-localize` work. And every candidate destination is still `overlay`,
which is correct before §4: the frontier and punch-hole answers are not yet available and
the probe declines to give them.

What remains is entirely disk-side: §4.1's `dd` (the gate on §13.17's retraction), the
post-§4 probe for SEEK_HOLE and punch-hole on real ext4, and then §6's sweeps.

### 13.31 §4.1 measured: 92 MB/s, and verification is now the dominant cost

`dd` on the 316 GB pd-standard localization disk, `oflag=direct`/`iflag=direct`, inside the
container:

| | Measured | Per-GB model | Ratio |
|---|---|---|---|
| write | **92.3 MB/s** | 37.9 MB/s | **2.43x** |
| read | **91.6 MB/s** | 37.9 MB/s | 2.42x |

**§13.17's retraction is confirmed.** The per-gigabyte model does not describe this path;
deriving a 38 MB/s ceiling from it — and the 1.8x cap, and all of §10's sizing arithmetic —
was wrong by a factor of 2.4. §10 is moot, as §13.24 anticipated.

A first attempt read 69.5 MB/s, understated 25% by a straggler `dd` from an interrupted
`docker exec` (which does not forward SIGINT). Worth recording because the contaminated
figure was plausible enough to have been believed.

#### The good news: >=4x is reachable

Today's 21 MB/s is **23% of the disk**, so there is **4.4x headroom**. At 92.3 MB/s the
279 GiB BAM downloads in **0.90 h**, against 4 h today — **4.44x**, which clears §8.5's
target. The disk binds there, so 4.4x is the ceiling for the in-place route rather than a
milestone on the way to more.

#### The bad news: verify() re-reads the object, and that costs as much again

Read and write are symmetric, so the full read-back `verify()` performs on this route takes
**0.91 h** — as long as the download itself:

| | Hours | Speedup vs 4 h |
|---|---|---|
| download at disk speed | 0.90 | 4.44x |
| + verify read-back, as it stands | **1.81** | **2.21x** |

So **verification is half the wall clock**, and the achievable speedup today is 2.2x rather
than 4.4x. §13.19 documented that the read-back is only avoidable on the bucket-compose
route, because the per-part digests live on `BucketChunkSink`; §13.19's decision was to
measure the split before building the in-place equivalent. The split is now measured, and
it is 50%.

**That makes in-transfer hashing the highest-value remaining change** — it is worth exactly
a doubling, and everything it needs is already verified: chunk starts are part-aligned for
every size and part-length combination tested, and this object's stride is confirmed at
9849 x 29.00 MiB with 87 MiB chunks of exactly 3 whole parts.

One honest consequence for §13.19's own work: **parallelising `multipart_etag` buys nothing
here.** md5 runs at several hundred MB/s per core while the disk reads at 92, so the read is
the bottleneck and adding hashing threads cannot help. The parallelisation is still correct
and helps on a faster destination, but the win on this path comes only from not reading the
object a second time.

### 13.32 Correction: 92 MB/s is the single-stream rate, not the disk's ceiling

§13.31 concluded that the disk binds at 4.44x and that parallelising `multipart_etag` buys
nothing. Both are wrong, and the evidence was already in hand — in the figure I had
dismissed as contamination.

When a straggler `dd` overlapped a fresh one, the fresh one still achieved **69.5 MB/s,
which is 75% of solo, not 50%**. A fixed-bandwidth device sharing 92.3 MB/s between two
writers would have given each 46. Aggregate was therefore around **139 MB/s — roughly 1.5x
solo from two writers**, so throughput is non-linear in concurrency and a single sequential
stream does not saturate the disk.

I had treated that 69.5 purely as a measurement to discard, when it was the only
concurrency data point available.

**Why it matters: nothing in this system writes or reads with one stream.** The downloader
issues `pwrite` from N threads and `multipart_etag` reads from N threads. Both see the
aggregate, not the single-stream rate. So:

* the download's disk-side ceiling is **above 92 MB/s**, by an unmeasured factor, and the
  4.44x figure in §13.31 is a floor rather than a cap;
* `verify()`'s read-back is **not** fixed at 0.91 h — parallel reads may cut it materially,
  which means §13.19's thread pool was not wasted effort after all. I withdraw that
  assessment: it was reasoned from a single-stream read rate as though it were the disk's
  limit.

In-transfer hashing (#19) remains strictly better than a faster read-back, because it reads
the object zero times rather than quickly. But its margin over a parallel read-back is
smaller than §13.31 implied, and the honest ordering needs the concurrency numbers first.

Added §4.1a: a write and read sweep at N = 1, 2, 4, 8 on the localization disk, reporting
aggregate MiB/s at each. The read sweep reuses the files the write sweep leaves, so N
readers read N distinct files — matching what the downloader and verifier do, rather than
several threads contending on one file. Three numbers to record: single-stream, the
plateau, and the N at which it plateaus.

**The pattern, again.** §13.9's dead comparison, §13.23's three-explanation upload rate,
§13.25's probe answering for the wrong filesystem, and now a discarded data point that
contradicted the conclusion drawn without it. Each time the mistake was treating a number
as noise, or as confirmation, rather than asking what else it was consistent with.

### 13.33 Measured: concurrency does not help the disk, and my parallel read-back hurt it

§4.1a, on the 316 GB pd-standard, aggregate MiB/s:

| readers/writers | 1 | 2 | 4 | 8 |
|---|---|---|---|---|
| write | 90 | 86 | 81 | 81 |
| read | 86 | 85 | 76 | **62** |

**The disk is a fixed-bandwidth resource at ~85-90 MiB/s.** Write aggregate is flat to
-10%; read aggregate *declines* 28% from one reader to eight. Extra streams buy nothing and
add interleaving cost.

So §13.32 was wrong and §13.31 was right: **92 MB/s is the ceiling, not the single-stream
rate, and 4.44x is a cap.** §13.32's inference came from the straggler measurement — one
`dd` reading 69.5 MB/s while sharing, which I read as "75% of solo, so aggregate must be
1.5x". The flaw was assuming the two overlapped for the whole 120 s. They did not: the
straggler finished partway through, so 69.5 is an average of a shared period and a solo
one, and carries no information about aggregate. Forty seconds shared at ~45 plus eighty
solo at ~92 averages 76 — near enough to what was seen.

**A code defect fell out of it.** `verify()` passed `workers=connections`, so at the default
8 the read-back ran at 62 MiB/s instead of 86: for the 279 GiB BAM, **21 minutes slower than
single-threaded**. My parallelisation of `multipart_etag` was not merely useless on this
path, as §13.31 said and §13.32 withdrew — it was an active regression.

Fixed: `VERIFY_READ_WORKERS = 2`, not the connection count. Two rather than one because it
costs ~1% here (85 against 86) and leaves headroom for a destination fast enough that
hashing rather than IO is the limit — tmpfs or local SSD, where md5's few-hundred-MB/s per
core would bind. On any persistent disk one reader already outruns the device.

Consequences for the plan:

* the in-place route's ceiling is **~90 MiB/s**, so the download is 0.90 h and **4.44x is
  the cap** for §8.5's target — reachable, with no margin;
* `verify()`'s read-back is **0.92 h and cannot be parallelised away**, so it really does
  double the wall clock to 1.81 h and 2.21x;
* which makes **#19, in-transfer hashing, the only way to recover it** — not one option
  among several. There is no faster read; there is only not reading.

**On the reasoning, since this is the third reversal on the same question.** §13.31 asserted
a cap from a single-stream measurement. §13.32 retracted it from a hypothesis about
non-linear scaling that fitted a contaminated data point. §13.33 settles it with a direct
sweep. The first two were both premature in the same way: a claim about how the disk behaves
under concurrency, made without measuring concurrency, when the measurement was four
minutes of `dd`.

### 13.34 Correction: parallel hashing does help, because read and hash serialise

§13.33 justified `VERIFY_READ_WORKERS = 2` with "one reader already outruns the device", and
that reasoning is wrong even though the value is right.

Each worker in `multipart_etag` **reads and hashes serially** — read a block, hash it, read
the next. So a lone worker leaves the disk idle for the duration of every hash. With md5
measured at **700 MiB/s** on one core and the disk reading at 86, one worker achieves
`1/(1/86 + 1/700) = 77 MiB/s`, not 86. Overlapping the phases across two workers recovers
most of that gap.

Combining the two measured effects — aggregate read falling with concurrency, and
read/hash overlap rising with it:

| workers | read aggregate | per-worker read | modelled effective |
|---|---|---|---|
| 1 | 86 | 86.0 | 76.6 MiB/s |
| **2** | 85 | 42.5 | **80.1 MiB/s** |
| 4 | 76 | 19.0 | 74.0 MiB/s |
| 8 | 62 | 7.8 | 61.3 MiB/s |

So N=2 is the peak, ~5% above a single worker, and the parallelisation added in §13.19 is
not useless after all — it is worth a few percent here, and would be worth far more on a
destination where hashing rather than IO binds (tmpfs, local SSD), since md5 at 700 MiB/s is
then the constraint and threads scale nearly linearly.

**What I keep getting wrong on this question.** §13.31: the disk is the ceiling and
parallel hashing is pointless — from a single-stream number. §13.32: neither, because
throughput is superlinear — from a contaminated number. §13.33: the disk is the ceiling and
parallel hashing is harmful — from a concurrency sweep that measured raw IO but not the
pipeline. Each was a claim about a two-stage pipeline argued from one stage of it.

The table above is still a model. Added a direct measurement of `multipart_etag` at
1/2/3/4/8 workers on the real disk, with cache-dropping between runs and an explicit
"CACHED -- meaningless" marker if `drop_caches` is unavailable, since without it every run
after the first reads from RAM. Set the constant from that, not from the model.

### 13.35 In-transfer part hashing on the in-place route (#19)

Built, because §4.1 measured the thing §13.19 deferred on: `verify()` re-reads the whole
object on this route, and at the disk's ~86 MiB/s that read-back costs 0.91 h against a
0.90 h download. Half the wall clock, and no amount of read concurrency removes it
(§13.33). The only way past it is not to read.

**Design.** Each S3 part's md5 is folded in as its bytes pass through
`PosixChunkSink.write`, keyed on having seen that part contiguously from its first byte.
The invariant it rests on was verified before any code was written: `plan_chunks` makes
every chunk start a multiple of `part_length` — checked across part sizes 5 MiB-512 MiB,
object sizes 1 B-300 GB and three `min_chunk` values, with zero misalignments — so a chunk
covers a whole run of parts.

Three decisions worth recording:

* **Its own manifest keyspace, `part_md5`.** On the bucket-compose route one chunk *is* one
  uploaded part, so `record_part_digest` keys by chunk index. Here a chunk spans several
  parts (87 MiB of 29 MiB parts for the object in question), so sharing the `chunks` dict
  would conflate two different things both called "part".
* **Committed at chunk completion, not per part.** One manifest write per chunk — the
  cadence that already existed — rather than one per 29 MiB. And it goes through `flush()`,
  which fsyncs the data first, so a digest can never be durable while the bytes it
  describes are not.
* **Per-part fallback, not all-or-nothing.** After one preemption only the parts straddling
  the resume point are unrecorded, so a handful of 29 MiB reads stands in for a 279 GiB
  one. `verify()` logs the split: `etag from 3281 recorded part digests, 2 re-read`.

**Thirteen tests.** The ETag correct with `0 re-read` across four alignments including
ragged tails; a wrong ETag still failing and discarding the file, since the digests must be
able to *reject* and not merely agree with themselves; a stale digest producing a mismatch
rather than a pass; and three real SIGKILL tests — correct ETag after kill-and-resume, a
substantial fraction of digests carried across rather than everything re-read, and a
manifest from a different plan not being trusted.

**And a failure in my own test, of exactly the kind this plan keeps cataloguing.** The
"re-reads only what it must" test was flaky and passed *vacuously*: 600 KiB over loopback
often finished before the kill landed, so the second run read the done marker and verified
nothing, while `reread < n_parts` passed anyway. Noticed only because a `-s` run failed
where a supposedly stronger version had passed. Fixed by throttling the server — the
harness already supported it — and by asserting the done marker is *absent* before the
second run, so a kill that arrives too late fails loudly instead of quietly proving
nothing. Stable across five consecutive repeats.

That is the §13.9 pattern again: a check that looks healthy while measuring nothing. It
nearly shipped inside the tests for the fix whose entire purpose is to avoid a redundant
read.

872 passed, 1 skipped.

### 13.36 The page-cache-loss test never ran, anywhere

`probe` on the mounted ext4 localization disk reported:

```
/mnt/rwdisks/canine-bench-...
    fstype     : ext4
    SEEK_HOLE  : yes        <- the frontier path is live, confirmed for the first time
    punch-hole : no         <- wrong
```

**SEEK_HOLE passing on ext4 is the good half.** The frontier scheme — progress re-derived
from the file's own extents rather than a stored counter — has never executed anywhere,
because APFS fails the probe and every local test therefore used the checkpoint fallback.
§6 will be its first exposure, and it is the mechanism the whole resumability claim rests
on.

**`punch-hole : no` on ext4 is a bug of mine, and a consequential one.** The probe gated
on:

```python
if not (hasattr(os, "fallocate") and hasattr(os, "FALLOC_FL_PUNCH_HOLE")):
```

**Python exposes neither** — no `fallocate()` taking a mode, no `FALLOC_FL_*` constants,
not even `os.posix_fallocate` on this build. So that predicate is False on every platform
and every filesystem, and always was.

Which means the "1 skipped" reported in every suite run through the whole of this
work — twenty-odd times, including in commit messages — is `TestSimulatedPageCacheLoss`:
the scenario §8.4 identifies as the one SIGKILL cannot produce, since SIGKILL does not
discard dirty pages while a vanished VM loses them. It has never run. I read its skip
message, "no punch-hole support on this platform/filesystem", as a macOS limitation and
never questioned it; it would have skipped identically on the production filesystem.

Fixed by calling `fallocate(2)` through ctypes in both the probe and the resume-test
helper. macOS now reports `[Errno 78] libc has no fallocate(2)` — unsupported for the real
reason — and on ext4 the test will run.

Three guards against the same trap:

* the skip message names the cause and states that a skip must mean fallocate is absent,
  never that a predicate is broken;
* a test asserts `os.fallocate` and `os.FALLOC_FL_PUNCH_HOLE` do **not** exist, so
  reintroducing that gate fails loudly — and if a future Python adds them, it fails as a
  prompt to simplify rather than sitting there misleading;
* the runbook's claim that overlay lacks hole punching is withdrawn. That came from the
  same broken probe, so whether overlay supports it is unknown until a fixed probe says.

**Fifth instance of one shape.** §13.9's per-part comparison reading `None`; §13.23's
upload rate with three unexcluded causes; §13.25's capability probe answering for the
container overlay instead of the disk; §13.35's kill test passing while verifying nothing;
and now a capability check reporting on its own dead predicate. Every one produced
plausible, healthy-looking output. The common defence is the same in each case and I keep
having to relearn it: assert that the mechanism *ran*, not merely that the result looked
right.

873 passed, 1 skipped — and the 1 becomes 0 on ext4.

**Confirmed after the fix:** `punch-hole : yes` on all three candidates, ext4 and overlay
alike. So overlayfs does support hole punching — the earlier "no" was entirely the dead
predicate, on every filesystem, and the runbook's speculation about overlay was wrong in
the other direction too.

### 13.37 The benchmark fabricated a measurement and declared success

§6.1's first run against the 12 GiB object:

```
conns    seconds     throughput    ...  hash
    1       1.75     6.86 GiB/s         BAD
    8       0.25    48.00 GiB/s         BAD
speedup: 7.0x vs a single stream (§8.5 wants >=4x)
       : MEETS the target
```

48 GiB/s over a 2 GB/s NIC, from a 0.25 s run, with `BAD` on every row. **Nothing was
downloaded.** The elapsed time was how long each attempt took to *fail*, and the tool
divided the object size by it, reported the quotient as throughput, and concluded the
target was met.

Three separate defects, all in the measuring tool:

1. **Throughput computed regardless of exit code.** A run must now finish *and* verify to
   count; otherwise the row prints `FAILED (rc=…)` and is excluded. If none survive the
   output is `NO USABLE RESULT` with "nothing here should be recorded as a benchmark
   result", and no speedup is stated.
2. **The failure reason was captured and discarded.** `stderr_tail` had held the 403 since
   the beginning and was only ever printed by `routeb`. Failed rows now show it.
3. **`--header` did not exist.** The actual cause was that
   `https://storage.googleapis.com/BUCKET/OBJECT` is anonymous and the bucket is private,
   so every setting 403'd. The fix is an `Authorization: Bearer` header — the mechanism the
   GCS handlers already use — and the benchmark had no way to pass one. Added, forwarded to
   both the ranged-GET path and the `connections=1` curl fallback, and checked against the
   downloader's real parser rather than a hand-written expectation.

Nine tests, including that a rate is never printed for a failed run, that the reason is
shown, that `rc=0` with a hash mismatch is also excluded, and that a partial failure still
reports the surviving settings.

**Sixth instance, and the worst placed.** §13.9's dead comparison, §13.23's ambiguous
rate, §13.25's probe answering for the container overlay, §13.35's vacuously-passing kill
test, §13.36's capability check reporting its own broken predicate — and now the instrument
itself. Everything else in §8.5 is measured *by* this tool, so a fabricated number here
propagates into every conclusion drawn from it. Had `48.00 GiB/s` been merely implausible
rather than absurd, it could have been recorded as a result.

The rule that would have caught all six, stated once more: **assert that the mechanism ran,
not that the output looked reasonable.** For a measuring tool specifically, that means a
measurement must be able to say "I did not measure anything."

882 passed, 1 skipped.

### 13.38 §6.1 measured, and three of its own reported figures were wrong

First real sweep, 12 GiB from same-region GCS into tmpfs, with an auth header:

| conns | total | download | verify | reported |
|---|---|---|---|---|
| 1 | 118.7 s | — (legacy curl) | — | 103.52 MiB/s |
| 4 | 91.1 s | 63.0 s | 27.7 s | 134.83 MiB/s |
| 8 | 96.4 s | 68.3 s | 27.7 s | 127.48 MiB/s |
| 12 | 91.1 s | 62.9 s | 27.8 s | 134.83 MiB/s |
| 16 | 110.2 s | 82.1 s | 27.8 s | 111.54 MiB/s |

**What it established.** §13.24's surviving confound is resolved: a single stream through
`curl` does **103.52 MiB/s**, not the ~60 MB/s §13.23 measured through `gcloud storage`. So
that figure was Python overhead in the CLI, not a network or per-stream limit — and §13.24's
"the single stream is the limit" framing was right about the *shape* and wrong about the
*number*. Memory is bounded as designed: 27.7 MiB at one connection to 64.1 MiB at sixteen.
And the knee is **4**, not the 8 currently shipped; 16 is measurably worse than 4.

**Three reported figures were wrong, all in the verdict.**

*The speedup said 1.3x; like for like it is 1.89x.* `connections=1` takes
`single_stream_fallback`, which never calls `verify()`, so its total is download-only while
every parallel row's total includes 27.7 s of read-back. Comparing totals across routes
that verify differently is not a comparison. Fixed at the source — the fallback now emits a
`download` phase like every other route — and the verdict compares download phases and
names its basis.

*"the DISK looks like the limit" fired on a tmpfs destination.* `/proc/diskstats` sees only
block devices, so disk writes read as ~0, which the heuristic took for saturation in a run
with no disk in the path. It now checks the destination's backing and says the run says
nothing about the disk.

*The verify advice recommended building something already built, for a case where it cannot
help.* In-transfer hashing (#19) applies to a multipart ETag. This object is verified by a
whole-file md5, which is sequential over the byte stream and cannot be assembled from
parts, so its read-back is unavoidable. Now distinguishes the two and, for multipart,
points at the `etag from N recorded part digests, M re-read` line.

**The result that matters is still unmeasured.** 1.89x is a property of *this source*: GCS
in-region already gives 103 MiB/s on one stream, leaving little for parallelism. The real
source gives 21 MB/s single-stream, and that is where a 4x would come from. The sweep
against a presigned URL of the real BAM — passing `--s3-bucket/--s3-key` too, so the ETag
and part length come from `head-object` and #19 is exercised — is the outstanding
measurement.

Seventh, eighth and ninth defects of the same shape, all in the instrument's verdict rather
than its data collection. The data was fine every time; the summary drawn from it was not.

887 passed, 1 skipped.

### 13.39 The baseline curl fetched 279 GiB when asked for 12

Setting up §13.38's outstanding measurement — the knee against the real BAM — the sweep's
`connections=1` row filled a 16 GiB tmpfs and died. `df` showed 8.4 GiB used with no file
to account for it; the space came back only after finding the holding process in `/proc`,
which was still writing `bench.1.bin (deleted)`.

Cause: the legacy row does not run the downloader's chunk planner. The downloader declines
at `connections <= 1` and synthesizes `curl -C - -sSL -o dest url`, which **has no range**.
That is correct in production, where `--size` is always the object's real size and the
whole object is what is wanted. It is wrong in a sweep, where `--size` deliberately names
a 12 GiB slice of a 279 GiB object: the parallel rows honored it, the baseline ignored it,
and the two rows being compared were not fetching the same bytes at all.

So the benchmark grew a `--prefix` flag emitting `curl --fail -sSL -r 0-<last>`, and the
sweep header now states which baseline it used rather than leaving it to be inferred.

**The same audit found `--fail` missing from production's fallback**, which is the more
serious of the two. Without it `curl` writes an HTTP error body — a 403 from an expired
signature, a 404 from a wrong key — *into the destination file* and exits **0**. The
localization step then reports success with an XML error document standing in for a BAM.
Whether that gets caught depends entirely on whether a hash was supplied, and for
`HandleOtherURL` nothing is. Added, with a test that forces a 403 and asserts the failure
propagates; the fake server needed a `force_status` knob, having previously been able to
fail a request only transiently and never with a chosen status.

**Correcting §13.38's closing paragraph.** It said to pass `--s3-bucket/--s3-key` on the
real-source sweep so the ETag and part length come from `head-object`. That is wrong for a
prefix run, and commit `806741a` had already fixed the code: those flags derive the ETag of
the *whole* object, which a 12 GiB slice cannot match, so verification fails and every row
exits 1. The prefix knee sweep is throughput-only, and #19's payoff is verified at full
size in §6.3 / §6.4 instead. §6.1 now carries the command with both constraints and the
reason for each.

Tenth instance of the shape, but with a new wrinkle: the defect was not a check that
measured nothing, it was two commands that *looked identical* and differed in the one
respect that decided what each fetched.

889 passed, 1 skipped.

### 13.40 The baseline was the wrong command, not just the wrong range

Asked why the prefix baseline curls rather than using `aws s3api`. Two answers, and the
second matters more than the fix in §13.39.

**The narrow one.** `s3_legacy_command` had §13.39's defect verbatim. Its range is
`bytes=$SZ-`, open-ended, which is right in production — `self.size` there is always the
object's real size — and wrong under a truncated `--size`, where it pulls all 279 GiB
while the parallel rows pull 12. So `--prefix` now bounds it to `bytes=$SZ-<last>`, and
without the flag it stays open-ended, because the command exists to reproduce what
`HandleAWSURL` emits. A test asserts production's range stays unbounded: bounding it there
would break resume for any object whose size the handler got wrong, which is the one case
the append-resume exists to survive.

**The one that changes a number.** Production's legacy path for an `s3://` input is
`aws s3api get-object --range` (`file_handlers.py:1239`) — not `curl`. The presigned-URL
path through `HttpSource` is what the *parallel* transfer uses, and it is preferred for
good reasons (one code path per source, no `aws` process per chunk — 9849 Python
interpreter startups for this BAM at 29 MiB parts). But the *baseline* has to be what was
actually running when localization took four hours, and that is the `aws` command.

§13.38 already measured what the distinction costs: `gcloud storage` did ~60 MB/s where
`curl` did 103 MB/s on the same object, a 1.7× penalty for being a Python CLI. `aws` is
also a Python CLI. So a `curl` baseline probably **understates** the speedup, by a factor
nobody has measured — the opposite direction from every error so far, all of which
flattered the result.

§6.1 now runs both baselines and names the honest comparison: the `aws s3api` single
stream against the presigned parallel rows. The S3ApiSource parallel rows are recorded as
a floor rather than a result, since they pay the per-chunk process cost.

Different shape from §13.39 and the nine before it. Those were instruments that measured
nothing while looking healthy. This one measured something real, correctly, and compared
it against the wrong thing — which no amount of asserting that the mechanism ran would
have caught. The check that would have caught it is asking what command production
actually replaced.

905 passed, 1 skipped.

### 13.41 The presigned URL expired before the download finished

Three challenges to §13.40's reasoning: shouldn't the number of concurrent `aws`
processes be the thread count rather than the chunk count; don't those processes get TCP
connection reuse; and don't they avoid worrying about token expiration. All three correct,
and the third was a live bug in the path this whole effort exists to fix.

**The bug.** `HandleAWSURL` minted its presigned URL with `aws s3 presign` and no
`--expires-in`, so the AWS CLI default of **one hour** applied. It also passed no
`--url-refresh-cmd` — `HandleDRSURI` does, `HandleAWSURL` did not, and I had not noticed
the asymmetry while converting either. A 279 GiB object at the ~60 MB/s measured against
the GDC endpoint takes about **80 minutes**. Past the hour:

`open_range` → HTTP 403 → `refresh_url()` returns False (no command) → `PermanentError`
→ `main` returns `EXIT_FAIL` → **do-not-retry**, with most of the object on disk.

Worse than a slow download: a guaranteed failure at the one-hour mark for exactly the
objects the parallel path was built for, and no retry. I had already hit this in the
*benchmark* and put `--expires-in 43200` in the runbook (§6.4's comment even says "the
default is one hour, which a multi-setting sweep of a large object can outlast") without
asking whether production had the same problem. It did.

Fixed both ways, because they fail differently: `--expires-in` defaulting to 12 hours
(long enough with margin; short of SigV4's 7 days, because the URL is a credential sitting
in the emitted script), and the presigner passed as `--url-refresh-cmd` so clock skew, a
requeued task resuming near the end of the window, or an object slower than 12 hours
re-mint and resume in place. `presign_expiry` is overridable per file and `int()`-coerced
host-side, since it is interpolated into a shell command.

**Correcting §13.40 on the other two.** "One `aws` process per chunk" was true; the number
I attached to it was not. Chunks are `align_up(64 MiB, 29 MiB)` = 87 MiB — three parts
each — so the object is **3283 chunks**, and 9849 is its *part* count. I reached for the
part count because it was the number in front of me. With `connections` processes at a
time, each overlapping a full chunk's transfer, ~1 s of Python startup at 8 connections is
about **7 minutes** across the run: worth avoiding, not decisive. §6.1 now states this as
a prediction the sweep can contradict rather than as a settled cost.

And connection reuse is **not a difference between the paths**. `HttpSource` calls
`urllib.request.urlopen` per chunk; urllib neither pools nor keeps alive, sending
`Connection: close`. Both paths pay a TCP and TLS handshake per chunk, 3283 apiece. So the
presigned path's advantage is narrower than the docstring claimed — one code path, no
process startup — and pooling per worker is an available optimization nobody has measured.
Recorded, not built, per the standing scope constraint.

**A different failure mode in the tests, worth naming.** Two modules build `HandleAWSURL`
via `__new__` and hand-set the fields `__init__` sets, because `__init__` shells out to
`head-object`. Adding one field broke 28 tests at once — loud, and the benign direction.
The dangerous direction is a fixture that keeps setting a field `__init__` has dropped, or
sets a different value: then every assertion made through it is about a handler that does
not exist. `TestTheRealConstructorAgreesWithTheFixtures` now emits one command through the
real constructor with `head-object` mocked and asserts it equals the fixture's byte for
byte, verified to fail when only the fixture's value is changed.

Two lessons, and the first is not the ninth instance of "the check measured nothing":

* **A finding in the instrument is a question about production.** The one-hour presign
  window was diagnosed, written down, and worked around in the runbook. Nothing prompted
  me to ask whether the same command in `file_handlers.py` had the same defect.
* **Asymmetry between two handlers doing the same job is a defect until shown otherwise.**
  `HandleDRSURI` refreshes its signed URL and `HandleAWSURL` did not, with no comment
  anywhere explaining why. That difference was visible in every reading of the conversion.

919 passed, 1 skipped.

### 13.42 The flat source was never a source: 13.85x, and the disk is now the limit

`probe_range` compares the server's declared total against the size it was given. In
`--prefix` mode those differ legitimately — the server reports the whole 278.91 GiB
object, the sweep asked for a 4 GiB slice — so it concluded Range was not honored, raised
`RangeNotSupported`, and the downloader fell back to a single stream. On **every** row,
while the table went on printing connections 1, 4, 8, 12 and 16.

Nothing else looked wrong. Right byte count. `wire: 1.01x`, so no duplicate fetching. Real
throughput: 16.62, 16.69, 16.51, 15.81, 16.73 MiB/s. That reads as a flat source and was
in fact a constant experiment — one curl, measured five times. Every conclusion drawn from
those two runs was about nothing at all, including the one I was most confident in.

With `--object-size` supplied, same source, same node, same 4 GiB:

| | throughput | streams | wire |
|---|---|---|---|
| 1 connection (ranged curl) | 16.62 MiB/s | — | — |
| 16 connections | **230.11 MiB/s** | 15.05 of 16 | 1.01x |

**13.85x** against a ≥4x target. And 230.11 / 15.05 = 15.29 MiB/s per stream, which is the
single-stream rate — so throughput is linear in connections and the cap is **per
connection**. §6.1a's four-hypothesis experiment is answered without running it, and
§13.41's speculation about per-signed-URL throttling is moot.

**The binding limit is now the disk.** pd-standard writes 92.3 MB/s (§13.33), saturated at
about 5.3 connections. For the 279 GiB BAM: **4.77 h** single-stream — which is exactly
the "upwards of 4 hours" that motivated this work, so the benchmark reproduces production
— **54 min** disk-bound, **21 min** if the destination could keep up. §6.2 is now the
measurement that decides the project, and §10's cost model needs rebuilding around a disk
ceiling rather than a source ceiling. `MAX_CONNECTIONS` is 16 and 16 was still scaling,
which matters only for destinations faster than pd-standard.

**On the diagnosis.** When the streams line was missing I blamed a stale downloader in the
container, wrote a guard whose message names that cause, and told the user my earlier
instruction had caused it. `grep -c k9pdl-streams` returned 1 and disproved it in one
command. The actual evidence was three lines away the whole time, in `stderr_tail`, which
the benchmark already recorded and printed on failure — but this run had not failed, so
nothing showed it.

Eleventh instance, and the most expensive: ~76 GiB of egress and two hours of wall clock
spent measuring a constant, plus a wrong conclusion stated confidently enough that the
next step was going to be a three-arm experiment to explain it.

What was missing was not a check on the *result* but a check that the *configuration under
test was the configuration requested*. The sweep asked for 16 connections; the downloader
said, in plain language, that it was using one; nothing compared the two. So:

* `--prefix` now learns the total and forwards `--object-size`, once per sweep.
* The header prints the object size, or `UNKNOWN` plus what will happen.
* Any row that fell back is marked `NOT PARALLEL` with the server's own reason.
* The verdict gains `NOT A PARALLEL MEASUREMENT`, and suppresses the
  `NO CONCURRENCY FIGURE` warning in that case so the real cause is not buried under a
  guess about a stale script.
* `connections=1` is exempt, being the baseline.

The general rule this session keeps re-deriving, now in its strongest form: **an
instrument must assert that it ran the experiment it was asked to run.** Every other guard
added here — `ok`, `NO USABLE RESULT`, the wire ratio, the streams figure — checked the
output. This one checks the input, which is where the loss was.

955 passed, 1 skipped.

### 13.43 The disk sweep: 4.5x, and the ceiling is the disk absorbing bytes

With the prefix bug fixed, the same 4 GiB to a pd-standard destination:

| conns | throughput | peak disk | streams | per-stream |
|---|---|---|---|---|
| 1 | 15.96 | 87.65 | — | — |
| 4 | 37.61 | 93.24 | 2.36/4 | 15.94 |
| 8 | 54.90 | 96.86 | 3.37/8 | 16.29 |
| 12 | 59.48 | 95.23 | 3.64/12 | 16.34 |
| 16 | 66.49 | 87.74 | 4.12/16 | 16.14 |

**Per-stream throughput is identical to tmpfs** (16.14–16.34 against 15.80–16.23). Each
active stream runs at the full source rate; what collapses is how many are active —
14.38/16 on tmpfs, 4.12/16 on disk. Throughput is `active_streams × 16 MiB/s` in both
cases, so nothing is slowing the streams down, they are merely not overlapping.

**`peak disk` is worthless as a saturation signal.** It reads 87–97 MiB/s at *every*
setting, including `connections=1`, which moved 15.96 MiB/s — a 5.5× gap. Writes land in
page cache and the kernel flushes at device speed regardless of the arrival rate. The
verdict's disk heuristic compared peak disk against peak NIC, so "the DISK looks like the
limit; raising connections further will not help" had been firing on *every* disk-destined
run ever made, and it fired here on a table showing throughput still rising from 12 to 16
connections. Now computed from `disk_bytes / seconds`.

**My hypothesis for the missing overlap was wrong, and the correction is the finding.** I
blamed the per-chunk `fsync` barrier in `chunk_done` — `record_chunk_done` fsyncs the
shared fd and rewrites the manifest under `Manifest._lock`, so 64 chunks meant 64
serialized flushes. The test was to cut the chunk count 4× and watch throughput climb
toward 88. It went 66.49 → 72.06 MiB/s, +8%, and streams 4.12 → 4.60.

Because **the flush work is invariant to chunk count.** The same 4 GiB has to reach the
platter at ~87 MiB/s ≈ 46.8 s however it is divided. Cutting chunks 4× removed 48 fsync
*calls* — metadata write, rename, directory fsync — not three-quarters of the flushing.
Total stream-seconds confirms it: 251.3 at 64 MiB chunks, 259.0 at 256 MiB. The same work
at the same rate; only the per-call overhead changed.

So the right frame is efficiency against the disk-bound floor:

| | throughput | 279 GiB | vs today |
|---|---|---|---|
| today, single stream | 15.96 MiB/s | 4.97 h | 1.00× |
| 64 MiB chunks, 16 conns | 66.49 MiB/s | 72 min | 4.17× |
| 256 MiB chunks, 16 conns | 72.06 MiB/s | 66 min | 4.52× |
| disk-bound floor | 87.57 MiB/s | 54 min | 5.49× |

77% of the floor at 64 MiB chunks, 83% at 256 MiB. The disk essentially *is* the ceiling,
and "sustained writes are 18% below the device's peak, so the disk is not saturated" —
which is what the new heuristic first said — undersells that: at 82% of what the device
demonstrably absorbs, the remaining gap is overlap loss between the write path and the
network, not spare bandwidth. More connections cannot recover it, because each stream is
already at its source rate. Added a MOSTLY disk-bound band saying so.

**What this settles and what it does not.** The ≥4× target is met on the real source and
the real destination: 4.52×, 4.97 h → 66 min. The remaining 12 minutes need write/read
overlap work, not tuning. A larger win would need the disk out of the path entirely, which
is what the bucket-compose route and the gcsfuse experiments are for — tmpfs reached
227 MiB/s on the same source, so there is 3× more available to a destination that can take
it.

The 256 MiB chunk size is a cheap +8% but it is not free: resume granularity goes from
64 MiB to 256 MiB, so a preemption discards up to 4× more in-flight work. At 66 minutes
per BAM on preemptible nodes that trade needs its own measurement (§7) before becoming a
default.

Two process notes. My hypothesis was falsifiable, cheap to test, and wrong — and the test
cost one 4 GiB run because it was designed to discriminate rather than confirm. That is
the first time this session a wrong idea of mine was caught by the experiment I proposed
for it rather than by the user. And the thing that made the diagnosis possible at all was
`streams:`, added two days ago for a different purpose: without it, 66 MiB/s against a
disk doing 88 would have looked like a disk limit and the per-stream invariance would have
gone unnoticed.

960 passed, 1 skipped.

### 13.44 Measured: 70% of the worker pool is the per-chunk fsync, not the transfer

The full-size run gave `hash ok` against the multipart ETag `2872df08...-9849` — first
correctness verification against the real BAM — and `verify 0.0s`, which is #19 working
exactly as designed: the digest was assembled from part md5s recorded during the transfer,
so a 279 GiB read-back never happened (~52 min at §4.1's 91.6 MiB/s read rate). The report
then read that as "verification is a small share, so hashing during the transfer would buy
little" — the feature's success as an argument for deleting it. Fixed.

But throughput degrades with object size, and at full scale the target is missed:

| | throughput | time | vs today |
|---|---|---|---|
| today, single stream | 15.96 MiB/s | 4.97 h | 1.00× |
| 4 GiB, extrapolated | 72.06 MiB/s | 66 min | 4.52× |
| **279 GiB, measured** | **48.50 MiB/s** | **98 min** | **3.04×** |

The extrapolation was 49% optimistic and 3.04× is below §8.5's ≥4×. Efficiency against the
disk floor fell from 83% to 50%, with per-stream throughput unchanged at the source rate
(16.17 vs 15.82 MiB/s) and streams down from 4.60 to 3.00.

**So I instrumented `chunk_done` instead of theorising, having already been wrong once
(§13.43's fsync-barrier hypothesis predicted a large win from fewer chunks and delivered
8%).** At 4 GiB, 16 connections, 64 chunks:

```
streams:    4.34 of 16 concurrent on average
chunk_done: 683.4s over 64 calls (mean 10.679s, 70% of the worker pool)
```

70% in `chunk_done`, 27% in the read loop, 97% of 982 worker-seconds accounted for. And
the mean is the fingerprint: 16 workers × 64 MiB dirty = 1 GiB, flushed at the measured
96.03 MiB/s, is **10.7s** — matching 10.678s. Each `fsync(data_fd)` flushes what *every*
worker wrote, not just its own chunk, and `Manifest._lock` serialises them. Actual flush
work is 42.7s of the 61.4s wall; the other ~641 worker-seconds is lock wait.

This also explains §13.43's failed prediction properly. Fewer, larger chunks cut the
*number* of flushes but each flushes proportionally more, so total flush work is invariant
— but the *lock wait* falls, which is the 8% that did materialise.

**Why it is avoidable.** `resume_offset` checks `is_complete(index)` first and returns
`end`, so `done` genuinely gates whether a chunk is re-fetched, and the fsync before it is
required — on the checkpoint path, where progress comes from a stored counter that must
never outrun durable data. On the **frontier** path it is redundant: `chunk_frontier()`
derives progress from the file's extents, a fully-written chunk has no holes so the
frontier already returns `end`, and after a crash unflushed bytes have no allocated
extents and read back as a hole, so the chunk is re-fetched. Conservative in the safe
direction, which is the property the frontier design was chosen for in the first place.

The subtlety needing a test rather than an argument: recorded part digests (#19) currently
become durable together with `done` by construction. Decoupling them means a crash can
leave a digest for bytes that were lost — harmless, because the re-fetched bytes are the
same object's bytes, and `_hash` only overwrites a digest when a part is re-read from its
first byte. Sound, but asserted by reasoning, which is what the SIGKILL suite exists for.

Three candidate fixes, in increasing order of what they preserve:

1. On the frontier path, stop recording `done` and stop fsyncing data per chunk. Biggest
   win, and rests entirely on the frontier argument above.
2. Keep `done`, but move the fsync and manifest write to a single background writer thread
   so workers never block. `done` is still only ever set after its own fsync, just later.
3. Accept 3.04× and spend the effort on getting the disk out of the path instead — tmpfs
   reached 227 MiB/s on the same source, so the ceiling here is the destination.

The instrumentation is the durable result either way: `k9pdl-bookkeeping` is now emitted
every run, with a test that injects a slow `chunk_done` and asserts the number moves, and
another asserting it stays out of the `streams` figure — otherwise a bookkeeping-bound run
reports healthy concurrency, which is the confusion these two numbers exist to resolve.

973 passed, 1 skipped.

### 13.45 The resume baseline: refetch is ~0, and the guard cried wolf on it

Three false starts before this measured anything, all in the harness:

1. The three SIGKILLs aimed at 25/50/75% all landed after **132 bytes** — the range
   probe. The trigger read `os.path.getsize(dest)`, and the downloader creates the
   destination sparse with `ftruncate` at the full object size, so the apparent size was
   already final on the first tick. Attempt 4 then did a complete fresh download and the
   run reported "refetched 31.79 MiB (0.8% overhead)" against a stated expectation of
   ~3 GiB for a broken frontier. It read as a strong pass. Fixed to `st_blocks * 512`.
   *The benchmark made exactly the mistake `clear_preallocated_working_file` exists to
   prevent in production.*
2. Next run exited 0 after **108 bytes** with `final hash WRONG`, then died in a
   `FileNotFoundError` traceback. Cause: `command_resume`'s stale sweep used
   `glob("*bench.resume.bin*")`, and glob's leading `*` does not match a leading dot — so
   it deleted the payload and left `.bench.resume.bin.k9pdl.done`. **And `run()` honoured
   that marker without checking the file existed**, which is a production bug: anything
   removing a payload without sweeping dotfiles makes localization report success with no
   destination. The stage-publish route had always checked `os.path.exists(staged) and
   os.path.getsize(staged) == size`; the primary route was the outlier, three functions
   away. Fixing it then broke bucket-compose — that route leaves no local file by design,
   so the check failed and it re-uploaded the whole object, showing up as exactly 2× the
   bytes. The marker now records its route.
3. Third run landed the kills correctly and **my own guard flagged it as invalid.**
   `kill_after_bytes` is an absolute file-progress threshold; I compared it against each
   attempt's *own* NIC bytes. On a resume the file already holds the earlier attempts'
   work, so attempt 3 needed only ~1 GiB of its own to cross the 3 GiB mark. Every
   attempt moving ~1.01 GiB **is** correct resume. Now compared against file progress at
   kill time, recorded when the signal fires.

**The baseline, finally.** Three kills, four attempts, each transferring ~1.01 GiB:

```
total received : 4.03 GiB for a 4.00 GiB object
refetched      : 32.98 MiB (0.8% overhead)
final hash     : CORRECT       SEEK_HOLE: yes -- frontier recovery
punch-hole     : refetched 111.80 MiB, hash CORRECT
```

The four transfers tile the object exactly once. 0.8% is the same TLS/HTTP framing every
run shows as `wire: 1.01x`, so **actual refetched payload is ≈ 0** against ~3 GiB for a
broken frontier. Frontier recovery is essentially lossless, and this is the first time
that path has run anywhere — it fails the SEEK_HOLE probe on APFS, so every local test
used the checkpoint fallback.

Punch-hole also ran for the first time: 111.80 MiB refetched after a 64 MiB hole was
punched inside completed regions, hash CORRECT. That is the case SIGKILL cannot produce —
bytes the process wrote being gone because the VM vanished — and the one the deferred-`done`
design has to survive. Encouraging for it.

**Thirteenth instrument defect, and the first of the opposite polarity.** Twelve were
checks that looked healthy while measuring nothing. This one declared a *good* result
invalid. Both come from the same root: the number being compared was not in the units the
threshold was expressed in. And the fix generalises — a guard needs a test that it does
not fire on the correct case, not only that it fires on the broken one. Two of the three
mutation checks I have been running per change already do this; the third (assert the
guard stays quiet on a healthy run) is the one I had been skipping.

985 passed, 1 skipped.

### 13.46 Measured: the disk bursts at 2× for 56 GiB, and the downloader is at its ceiling

§13.14–§13.16 reasoned about the pd-standard ceiling from GCP's published
**0.12 MB/s per provisioned GB**. That constant is now measured, along with something the
tables have no term for at all.

Plain `dd`, `O_DIRECT`, single stream, twelve timed 8 GiB stages on the 316 GB disk:

```
stages 0-6  (0-56 GiB) : 92.1 MB/s   flat to 0.2%
stage  7   (56-64 GiB) : 61.1 MB/s   <- the knee
stages 8-11 (64-96 GiB): 46.0 MB/s   flat
```

**Two corrections to the model, both in our favour.** The per-GB constant is
`46.0 / 316 = 0.1456 MB/s/GB`, not 0.12 — every row of §13.15's and §13.16's tables
understates throughput by **21%**. And there is a **burst**: the disk delivers exactly
`92.1 / 46.0 = 2.002×` its sustained rate for the first ~56 GiB, which nothing in §13
accounts for.

**Only the 316 GB row is measured, and it is the only row that can be.** One disk size was
tested, so nothing here establishes how either correction behaves at another. The knee
might sit at 56 GiB on every pd-standard, or scale with provisioned size, or not exist
above some size; the 2.002× multiplier might be constant or not; and the 0.1456 MB/s/GB
constant is a single point, which cannot confirm that sustained rate is linear in size at
all — §13.15's tables took linearity from GCP's documented model, and this measurement does
not test it.

| Disk | sustained | 300 GB, flat | 300 GB, with the 316 GB burst | §13.15 said |
|---|---|---|---|---|
| **316 GB (today)** | **46.0 MB/s measured** | 1.81 h | **1.63 h — matches the 1.62 h measured in §6.3** | 2.20 h |
| 460 GB | 67.0 MB/s *if linear* | 1.24 h | *unknown — burst unmeasured* | — |
| 742 GB (§13.15's recommendation) | 108.0 MB/s *if linear* | 0.77 h | *unknown — burst unmeasured* | 0.94 h |
| 2000 GB | 291.1 MB/s *if linear* | 0.29 h | *unknown — burst unmeasured* | 0.35 h |

Read the non-316 rows as "§13.15's arithmetic with a better constant", not as predictions.
How much the burst is worth on them depends entirely on an assumption nobody has tested —
whether the knee travels with provisioned size:

| Disk | flat | if the knee stays at 56 GiB | if the knee scales with size |
|---|---|---|---|
| 460 GB | 1.24 h | 1.12 h (−10%) | 1.06 h (−15%) |
| 742 GB | 0.77 h | 0.69 h (−10%) | 0.59 h (−24%) |
| 2000 GB | 0.29 h | 0.26 h (−10%) | 0.14 h (−50%) |

A 10% effect and a 50% effect are different decisions, and on the 2000 GB row they are the
difference between oversizing being marginal and being transformative. Guessing which
column applies is exactly how this section's parent error happened.

**The recommendation does not change — 742 GB is still right.** What changes is that the
316 GB baseline it is measured against is faster than §13.15 assumed (1.62 h, not 2.20 h),
so the *margin* oversizing buys is smaller than the tables claim, and the cost case in
§13.15/§13.16 should be re-derived against 1.62 h before anyone spends money on it. That
re-derivation needs the 742 GB row measured, not modelled: **§6.5j in the runbook is the
same 25-minute `dd` ladder at 460 and 640 GB**, and it is the cheapest way to settle both
unknowns at once — whether sustained scales linearly, and whether the burst comes with it.
Its instructions already require extending the ladder until two consecutive stages agree,
precisely because a knee that scales with size would otherwise let a larger disk report its
burst as sustained.

**The downloader is at the device's ceiling and there is nothing left to win in code.**
It settles at 44.2 MiB/s where single-stream sequential `O_DIRECT` `dd` sustains 43.9:
sixteen concurrent workers issuing sparse out-of-order 1 MiB `pwrite`s, with `fdatasync`
and an in-transfer md5 on top, run **0.8% faster than `dd`**. A three-parameter model of
the device — burst rate, knee position, sustained rate, fitted only to `dd` — predicts the
279 GiB run's wall clock to **8.3 s out of 5818, or 0.14%**.

This also revises §13.14's estimate that the downloader "wins at most 1.8× on this path".
Measured: **3.07×** (4.97 h → 1.62 h). §13.14 was reasoning from a flat 38 MB/s; the real
device is faster and bursts.

**The resize daemon now cuts the other way.** §13.14 notes that §12.2's daemon grows the
disk mid-run, so "on pd-standard the write ceiling *rises* mid-run". True, but it is not
the dominant term: the measured rate *fell*, 92.1 → 46.0, as the burst exhausted at 56 GiB.
In production both effects run at once and oppose each other. A localization that starts on
a small disk is starting with a small burst allowance as well as a low sustained rate, and
the daemon's growth has to outrun the burst's exhaustion to be visible at all. Nothing here
measures that interaction — the benchmark used a fixed 316 GB disk — and §13.14's advice to
"use the sampled peaks" is now actively wrong, because the peak is the burst and the burst
is 20% of the transfer.

**And a second-order warning for every future measurement on this hardware.** Four
subsections of the runbook (§6.5c–§6.5f) concluded there was a 1.70× defect "inside our own
code". There was no defect. Every arm that produced that conclusion ran for ≤16 GiB and so
measured the burst, and the downloader's full-size figure was compared against it. The
error is cheap to repeat and expensive to find: **on this disk, any measurement shorter
than ~64 GiB reports roughly double the sustained rate.** The rule the runbook now carries
is to extend a ladder until two consecutive stages agree, and it applies with equal force
to the `fuse-localize` path — a Rapid Cache bucket has every reason to show its own
burst-then-settle curve, and a 4 GiB `gcsfuse` benchmark would reproduce this mistake in a
new medium.

**Fourteenth instrument defect, and the most expensive so far**, because it was not in an
instrument. The harness worked; the *comparison* did not. Both numbers were correct and
they measured different regimes of the same device. Twelve of the previous thirteen were
checks that looked healthy while measuring nothing, and one declared a good result bad;
this one measured two real things and drew a false conclusion from putting them side by
side. A guard cannot catch it — the defense is that a comparison between two measurements
has to state the conditions both were taken under, which the runbook's tables now do with
an explicit `extent` column.

One further note on process: **I re-recommended `pd-balanced` in the runbook**, which
§13.14 already records me proposing and already records as wrong for the same reason — only
pd-standard attaches read-only to an unlimited number of VMs. Corrected in the runbook, and
noted here because the correction had already been written down once.

1058 passed, 1 skipped.

#### The cost case, re-derived: download speed alone never pays for the disk

§13.15 recommended 742 GB at 0.94 h against a 2.20 h baseline. Both numbers were built on
0.12 MB/s/GB. With the measured constant the baseline is **1.62 h**, so oversizing saves
21% less time while costing exactly the same, and the case has to be re-checked rather
than inherited. Prices are §13.15's own — $0.04/GB/month from its $29.68/mo for 742 GB,
$0.38/h from its $0.48 saved over 1.26 h — not fresh recollections.

Conservatively throughout: the 316 GB baseline is given its **measured** burst, making it
look as fast as possible, while candidates get the **flat** model, making them look as slow
as possible. The bias is deliberately against spending money.

| disk | 300 GB | download saved | worth once | extra disk @24h | @48h | IO-bound consumers to break even @24h / @48h |
|---|---|---|---|---|---|---|
| 316 GB (today) | 1.63 h | — | — | — | — | — |
| 460 GB | 1.24 h | 0.39 h | $0.15 | $0.19 | $0.38 | **0.3 / 1.6** |
| 742 GB (§13.15) | 0.77 h | 0.86 h | $0.33 | $0.56 | $1.12 | **0.7 / 2.4** |
| 1000 GB | 0.57 h | 1.06 h | $0.40 | $0.90 | $1.80 | 1.2 / 3.5 |
| 2000 GB | 0.29 h | 1.34 h | $0.51 | $2.21 | $4.43 | 3.3 / 7.7 |

**The answer is no, and the reason is structural rather than a matter of degree: the
download saving is paid once, and the disk is billed per hour of retention.** A faster
download can therefore never pay for a bigger disk on a long enough retention, whatever the
speedup. The crossover is where retention stops being short:

| disk | retention at which the download saving alone covers the extra disk |
|---|---|
| 460 GB | 18.6 h |
| 742 GB | 14.0 h |
| 1000 GB | 10.7 h |
| 2000 GB | 5.5 h |

Retention is 24–48 h. **Every candidate is under water on download speed alone**, and the
bigger the disk the worse it gets — at 2000 GB the disk has to disappear within 5.5 hours
to justify itself, which is not what a localization disk is for.

So §13.15's conclusion survives only via the mechanism §13.15 itself identified: **consumer
reads**, with §13.16's correction that only **IO-bound** consumers count. At 742 GB each
IO-bound consumer reading the full 300 GB saves 0.86 h — the same $0.33 — so break-even at
48 h retention is **2.4 such consumers**. The whole recommendation rests on that number
being exceeded, and it is a fact about the lab's DAGs that no benchmark on this node can
supply. §13.15's own figures were 1.2 / 2.3; the corrected ones are 0.7 / 2.4, so the
24-hour case improved and the 48-hour case did not.

Three consequences worth acting on before buying disk:

1. **Retention is the cheaper lever, and it is the one nobody has priced.** Dropping a
   large localization disk from 48 h to 12 h makes 742 GB profitable with zero consumers,
   and costs nothing to implement beyond a policy. Oversizing buys speed by spending on the
   same axis retention wastes it on; shortening retention buys the speed for free. If both
   are available, do retention first.
2. **460 GB is the better risk-adjusted buy than 742 GB** wherever the consumer count is
   uncertain. It needs 1.6 IO-bound consumers at 48 h against 742 GB's 2.4, still reaches
   1.24 h, and exposes $0.38 rather than $1.12 to the assumption being wrong.
3. **Do not size by throughput target.** Reaching a fixed rate demands a fixed disk size
   regardless of payload, so a 10 GB input would get the same 742 GB. §13.15 reached this
   too: it has to be a multiplier with a size threshold.

All of which is conditional on scaling that remains unmeasured — §6.5j at 460 and 742 GB
settles whether sustained rate is linear and whether the burst travels with size. If the
burst does travel, candidates get 10–24% faster and every break-even above improves
proportionally; if sustained rate is not linear, the table is void. **The 25-minute ladder
costs less than the first hour of the disk it would justify buying.**

#### Correction: the localization VM is preemptible, which collapses the case entirely

The table above priced saved time at **$0.38/h**, taken from §13.15. That is the on-demand
`n1-standard-8` rate, and the localization VM is **preemptible** — so the saving is a
preemptible VM-hour, worth a fraction of that. §13.15's cost model was built on the wrong
side of the trade, and so was the re-derivation above until this correction.

The structure is now the whole story. **A preemptible VM-hour is the cheapest hour in the
system; a retained persistent disk-hour is among the most expensive.** Oversizing spends
the expensive one to save the cheap one, for 24–48 hours, to shorten a job that runs once.
Parametric in the spot rate, since the exact figure depends on zone and machine type:

| VM $/h | IO-bound consumers to break even at 48 h — 460 GB | 742 GB |
|---|---|---|
| $0.38 (on-demand — what §13.15 assumed) | 1.6 | 2.4 |
| $0.20 | 3.9 | 5.5 |
| $0.12 | 7.2 | 9.9 |
| $0.10 | 8.8 | 12.1 |
| $0.08 | 11.3 | 15.3 |
| $0.05 | 18.7 | 25.1 |

| VM $/h | retention at which download speed alone pays — 460 GB | 742 GB |
|---|---|---|
| $0.38 | 18.6 h | 14.0 h |
| $0.20 | 9.8 h | 7.4 h |
| $0.10 | 4.9 h | 3.7 h |
| $0.08 | 3.9 h | 2.9 h |

At anything resembling a spot rate the disk would have to be deleted within a few hours of
the download finishing, against an actual retention of 24–48. For zero-consumer break-even
at 48 h the VM would need to cost **$0.98/h at 460 GB or $1.31/h at 742 GB** — three to
four times the *on-demand* price of the machine.

**So: oversizing the localization disk is not a cost saving, and the faster the download
gets the less true it becomes.** §13.15's recommendation of 742 GB does not survive
preemptible pricing. It survives only if the lab has ~10–15 IO-bound consumers per
localized object, and that is a much stronger claim than the 2.3 it was accepted on. The
question was never really about throughput; it is about how many downstream tasks read the
whole object and are IO-bound, and the answer is a property of the DAGs.

**What a faster download on a preemptible VM is actually worth is preemption exposure, not
dollars.** 1.62 h to 0.77 h halves the window in which the node can vanish, and thus the
expected number of restarts. This project already makes those restarts cheap and correct —
frontier recovery refetches ≈ 0 (§13.45) — so the value is bounded and is about latency
variance rather than cost. It is a real argument and a smaller one than the cost tables
implied; it should not be dressed up as savings.

**Two levers remain, and neither is disk size.** Shorter retention for large localization
disks, which spends nothing and directly attacks the term that dominates every table here.
And `fuse-localize`, which removes the provisioned-disk-hour from the model altogether —
a Rapid Cache bucket is billed on stored bytes and cache capacity rather than on a
provisioned size chosen for throughput, so it does not force the trade this section has
been trying and failing to make pay.

**Sixth correction in this section's own lineage, and worth naming as a pattern.** The
pd-standard ceiling was reasoned from a spec constant (wrong by 21%), the write path was
compared against a burst rate (no defect existed), `pd-balanced` was recommended twice
against a fan-out constraint already recorded here, the burst was extrapolated to disk
sizes never measured, and the saving was priced at on-demand rates for a preemptible VM.
Every one of them was an input taken from a plausible-looking source instead of measured or
asked about, in a document whose entire method is to measure. **The measurements in this
effort have been reliable; the premises fed into them have not.** Worth a standing check:
before a number decides something, establish whether it was measured here, measured
elsewhere, or assumed — and label it in the table.

### 13.47 Planned: evaluate the cached bucket as the localization destination

**Not a redirection of current work.** The parallel downloader's remaining tasks stand as
they are; this is added to the plan because §13.46 removed the alternative. The disk path
is measured and closed — the downloader runs 0.8% faster than `dd` on a device that
sustains 43.9 MiB/s, `pd-standard` is fixed by read-only fan-out, and oversizing does not
pay against a preemptible VM. There is no remaining lever on that path, so the cached
bucket is where the ≥4× target has to come from if it comes from anywhere. The concurrent
`fuse-localize` effort reports going well, which is the other reason to plan it in rather
than keep it as a completeness item.

**Scope.** Four questions, in dependency order:

1. **Write throughput to a Rapid Cache bucket at full size.** The number that replaces
   43.9 MiB/s. Everything else is contingent on it.
2. **Read throughput for consumers** through `gcsfuse` against the same cache. §13.46's
   cost model turns entirely on IO-bound consumer savings, and a cached bucket changes both
   sides of it — reads may be faster *and* the provisioned-disk-hour disappears from the
   billing, which is the term that sank oversizing.
3. **The bucket-compose route's correctness against real GCS**, which has still never run
   outside a mock: auth path, resumable sessions, compose.
4. **The cost model**, which is structurally different — stored bytes and cache capacity
   rather than a provisioned size chosen for throughput. §10 needs a second table, not an
   edited one.

**The one rule carried over from §13.46: measure over ≥96 GiB.** A Rapid Cache bucket has
every reason to show its own burst-then-settle curve — cache fill at zonal SSD rate, then
origin rate on a miss — and a 4 GiB `gcsfuse` benchmark would reproduce this effort's most
expensive error in a new medium. The bar is two consecutive stages of the §6.5i ladder
agreeing. This applies to the read arm as much as the write arm, and a cache makes it worse
than a disk does: a small test may never miss at all.

**Known prerequisites and gaps, so none of them is discovered mid-run:**

* `gcsfuse` is absent from the `wolf-2.0-update` image (so is `rclone`). The
  `fuse-localize` image has it, and is where this is being evaluated anyway.
* The bucket needs an Anywhere Cache instance **in the node's zone**, created with
  `--enable-ingest-on-write` to match `get_or_create_rapid_cache()`. Without
  ingest-on-write the write arm measures the origin bucket and the read arm measures a cold
  cache — two wrong numbers that will look like one right one.
* ADC must be explicit: `gcsfuse` does not read `CLOUDSDK_CONFIG`. Confirm the token source
  is `gcloud` and not the metadata server, or the run exercises the compute service account
  rather than the identity production uses.
* **`compose_tree` has no direct test coverage.** It implements the 32-source limit via
  intermediates, and 279 GiB at 87 MiB chunks is 3283 sources — a **three-level** tree
  (3283 → 103 → 4 → 1). A test at two levels would pass where three fails, and the failure
  would surface hours into a full-size run. Worth a unit test before the run, not after.

Procedure lives in `BENCHMARK_RUNBOOK.md` §6.6, which §6.5i already re-scoped from fallback
to deciding measurement.

### 13.48 Reconciling §13.47 against `LOCALIZATION.md`: three corrections and a new risk

`fuse-localize` is merged (`parallel-localization-fuse`) and ships `LOCALIZATION.md`, which
documents the bucket-mounted path end to end. Reading it changes §13.47 in four ways.

#### 1. The hazard it warns about is real, and our gate is what avoids it

`LOCALIZATION.md` §4c: the `mount` upload kind mounts the bucket **read-write** at
`/mnt/localize/<bucket>` and runs the handler's own `localization_command` straight into it,
relying on gcsfuse streaming writes (image pins 3.11.2). Those are **append-only from offset
0**, so — its words — *"a multipart/parallel writer would silently fall back to staging"*,
buffering the whole object under `--temp-dir` on the 25 GB boot disk. On a 279 GiB BAM that
is an ENOSPC, arrived at silently.

Our downloader is exactly that multipart/parallel writer, and `s3://`/GDC inputs are exactly
`kind == "mount"`. So the 279 GiB object is precisely the case that would hit it.

It does not, because `select_route` fires first: `fuse.gcsfuse` is off
`POSIX_FSTYPE_ALLOWLIST`, `gs_url_for` resolves the bucket from `/proc/mounts`, and the
result is `ROUTE_BUCKET` — parts uploaded through the JSON API and composed server-side,
never a byte through the mount. That is §4.7's "gate, don't adapt" doing the job it was
built for, against a hazard documented independently by another branch.

**But it is now a correctness interaction between two subsystems, not a fallback route, and
nothing has ever run it against real infrastructure.** It becomes §6.6's first measurement
and it gates everything after it:

* Does `select_route` actually reach `ROUTE_BUCKET` inside `/mnt/localize/<bucket>`, or does
  something about that mount's options resolve no `gs://` URL and drop it to `stage-publish`
  — which stages to the same 25 GB boot disk and ENOSPCs just as surely?
* Does the composed object land at `<input_name>/<basename>`, where the post-unmount
  `gcloud storage ls` check expects it? Our route writes it before the unmount, so
  "the unmount is what finalizes the objects" no longer describes how it got there.
* Does `gcloud storage objects update --custom-time` then stamp it? An object the
  lifecycle rule cannot see never expires (`LOCALIZATION.md` §3).

None of this needs a 279 GiB transfer. A small input exercises every one of them.

#### 2. Rapid Cache is off by default, and wired to the wrong bucket

§13.47 was written as "measure a Rapid Cache bucket". Per `LOCALIZATION.md` §5b,
`rapid_cache` defaults to **`False`**, and its own caveat records that
`get_or_create_rapid_cache()` is called on `config["storage_bucket"]` — the *workflow*
bucket `canine-<project>-<workflow_name>` — while localization reads per-localization
`wolf-<project_number>-<region>-<hash>` buckets that are never passed to it. As wired,
`rapid_cache=True` caches a bucket this path does not read.

So the measurement has to provision the cache **by hand on the `wolf-...` bucket**, and
report the uncached bucket as the baseline rather than assuming the cache is in play. Also
note the cache is **zonal** while the bucket is **regional**: a worker in another zone of the
same region gets a silent miss, so a multi-zone cluster does not see one number.

#### 3. The cache re-introduces the per-GiB-hour bill the bucket was supposed to remove

§13.46 concluded that oversizing the disk fails because a preemptible VM-hour is cheap and a
retained provisioned disk-hour is expensive, and that `fuse-localize` helps by removing the
provisioned disk from the model. Half of that survives. `LOCALIZATION.md` §5b prices Rapid
Cache at roughly **4× standard storage (~$0.089/GiB-month)** while in-region transfer is
**$0/GiB** — so for a 279 GiB input the cache is on the order of **$25/month of storage to
save transfer that is already free**, and it buys read *latency* only.

That is the same trade §13.46 rejected, in a new medium: pay per-GiB-hour to speed something
up. It is a good trade for an input many shards read and a bad one for read-once work, which
is what a one-off dbGaP BAM is. **The honest default for §6.6 is therefore the *uncached*
bucket**, with the cache measured as a separate arm and justified only by a consumer count —
the same number §13.46 found the disk question turning on.

#### 4. New risk: our own runtime exceeds the sibling-wait ceiling

`bucket_upload_wait_tries` defaults to 60 polls at 60 s — a **one hour** ceiling
(`LOCALIZATION.md` §3). The measured full-size localization is **1.62 h**.

So a sibling shard waiting on our upload times out *before it can finish*, sets
`wolf=stale`, and exits 5. A later worker reads `stale`, concludes the previous uploader
died, and takes over — while the original is still running. For the `server_side` and `copy`
kinds `-n` makes a double take-over harmless, and the doc says so. For `kind == "mount"`,
which is ours, the only thing standing between that and two concurrent writers is the
downloader's own resume state in the bucket, and **two concurrent writers against one
plan_id has never been tested** — the resume suite kills and restarts one writer, it does
not run two.

This is not hypothetical and it is not new to the merge: any localization over an hour
reaches it, which is the entire workload this effort exists for. The mitigation is probably
one line — `bucket_upload_wait_tries` scaled to the expected transfer time rather than a
flat 60 — but the failure mode should be measured before the constant is picked, and the
two-writer case tested regardless, because a node dying mid-upload produces it too.

Also from §5a: `localization_expiry_days` defaults to **1**, so a localized artifact is gone
the next day. Any follow-up measurement against it must happen the same day or pay to
re-localize — the same trap as the sweep's missing `--keep` (§6.3).

#### Revised order for §6.6

1. **Route correctness on a real gcsfuse RW mount**, small input, no benchmark. Gates the rest.
2. **Object path, `ls` check, and customTime stamping** — same run.
3. **Two concurrent writers against one `plan_id`**, and the `bucket_upload_wait_tries`
   timeout that produces it.
4. **Throughput, uncached bucket, ≥96 GiB** — the number that replaces 43.9 MiB/s,
   and the number that sizes `bucket_upload_wait_tries` (§13.49, a placeholder until then).
5. **Throughput with a hand-provisioned Rapid Cache on the `wolf-...` bucket**, as a separate
   arm with its storage cost stated.

Steps 1–3 need no large transfer and no cache. Only step 4 needs the bench node back.

### 13.49 `bucket_upload_wait_tries`: raised to 180, and the constant is the wrong instrument

**First, a correction to §13.48.** That section flagged the 1 h upload-wait ceiling as a
risk *"because the measured localization is 1.62 h"*. That reasoning is wrong. 1.62 h is
the **pd-standard download** time from §6.3; the bucket route is a relay — source straight
to the JSON API, with **no disk anywhere in it**. Sizing a bucket-path constant off a
disk-path measurement is precisely the category error §6.5c–§6.5i spent four subsections
unwinding, committed again three sections later, in the middle of writing up the lesson.

What is actually known about the relay:

| bound | figure | 279 GiB |
|---|---|---|
| source → `/dev/null`, 16 conns (§6.5c) | 240 MiB/s | 0.33 h |
| source → tmpfs, 16 conns (§6.1) | 227 MiB/s | 0.35 h |
| GCS ingest from an `n1-standard-8` | **unmeasured** | — |
| ~~pd-standard sustained (§6.5i)~~ | ~~43.9 MiB/s~~ | *wrong path* |

So there is a **floor of ~0.35 h** and no upper bound. Against the floor alone the old 1 h
default may have been perfectly adequate, and the "it fires on every large input" claim in
§13.48 is unsupported. It might; nobody knows.

**Raised to 180 anyway (3 h), on risk asymmetry rather than arithmetic.** The two failure
directions are not symmetric:

* **Too short** declares a healthy upload dead, hands the object to a take-over worker
  mid-flight, and costs a duplicate transfer — reported as a warning, on every affected
  input, indefinitely. Silent and recurring.
* **Too long** only delays recovery from an uploader that genuinely died. Visible in the
  logs, bounded, and the transfer itself survives: the manifest is in the bucket and the
  take-over resumes.

Under an unmeasured distribution, pay the visible cost rather than the silent one. This is
explicitly a placeholder: **§6.6 step 4 measures the relay and supplies the real number**,
and lowering it again with data would be a good outcome, not a regression.

**But a constant cannot be right here, whatever value it takes**, because it is doing two
unrelated jobs at once:

1. *How long can a legitimate upload take?* — a function of object size and relay
   throughput, so it scales with the workload.
2. *How long until we conclude the uploader died?* — a function of failure detection, and
   wanting to be as short as possible. On preemptible workers a dead uploader is routine,
   not exceptional: it leaves `wolf=working` behind, and every sibling — including its own
   requeued incarnation — now waits the full ceiling before anyone takes over. Raising the
   constant for job 1 makes job 2 strictly worse, and 3 h of stall per preemption is a real
   cost on exactly the machines this runs on.

**The fix is a liveness signal on the claim, not a longer wait.** canine already has the
pattern: `bucketmount_heartbeat_start()` (`LOCALIZATION.md` §7) re-stamps customTime on a
timer for precisely this reason — *"self-healing by construction. If the worker dies, the
loop dies with it."* The same shape applies to the upload claim: the uploader periodically
re-stamps `wolf=working` (or a companion object carrying a timestamp), and a waiter blocks
while the stamp is fresh and gives up in **minutes** when it goes stale. That decouples the
two jobs — unbounded patience for a live uploader, fast recovery from a dead one — and
removes the need to predict transfer times at all.

Not built here: it changes a subsystem `fuse-localize` owns, and the constant is adequate
in the meantime. Worth raising with whoever owns that path.

**Recorded because the estimate was wrong in an instructive way.** The number was not
plucked from nowhere — it was a real measurement, correctly obtained, of a different
system. That is the failure mode this whole effort keeps producing, and the reason the
runbook's tables now carry an explicit `extent` column: **a figure is only usable together
with the conditions it was measured under, and "measured" is not the same as "relevant".**

### 13.50 The bucket route clears the target, and the cap was one of our constants

§6.6 ran. The measurements, and what they overturn.

#### The gate holds

`select_route` reaches `bucket-compose` on a real read-write gcsfuse mount: `rc=0`,
**192 parts composed** for a 12 GiB object, and parts exist only on that route. §13.48's
first step — the one everything else was conditional on — passes. `LOCALIZATION.md` §4c's
hazard, a parallel writer silently staging onto the 25 GB boot disk, is gated in practice
and not just in principle.

`gcloud storage ls -L` also settles the crc32c question empirically: `Component-Count: 192`,
`Hash (CRC32C)` present, **no `Hash (MD5)` line at all**.

#### Two constants of ours, not the infrastructure

The first bucket measurements were disappointing — 40.8 MiB/s at 8 connections, then
70.1 with a GCS source, then 64.4 with the real GDC source. Against the pd-standard's
43.9 MiB/s that is 1.47x: real, but nowhere near the ≥4x the disk path had failed to
reach, and it looked like the bucket was simply not the answer.

It was not the bucket. **Per-worker upload was 4.71 MiB/s from the GDC S3 endpoint and
4.76 MiB/s from a GCS object** — two unrelated networks, 1% apart. Agreement like that
cannot come from either end; it comes from something we control. A resumable PUT costs
~53 ms round-trip whatever it carries, and the route sent one per 256 KiB
(`GCS_UPLOAD_GRANULARITY`), so the rate was arithmetic and the ceiling was ~72 MiB/s at 16
connections regardless of what anything else could do.

That block was deliberate, bounding bytes-read-but-not-yet-durable to <256 KiB per
in-flight chunk. The guarantee was real and **mispriced**: at 8 MiB the worst case is
128 MiB across 16 chunks — 0.045% of a 279 GiB object, on a preemption that already costs
minutes of restart — against a 5x throughput cap paid on every transfer, preempted or not.

| 96 GiB, GDC S3, 16 conns | 256 KiB | 8 MiB |
|---|---|---|
| relay | 64.4 MiB/s | **146.4** |
| per-worker upload | 4.71 | **39.2** |
| workers inside upload | 13.67 of 16 | **3.73** |
| `io` read share | 12% | **76%** |

The bottleneck flipped to the source, which is where it belongs.

#### The answer to the question this effort started with

| | pd-standard | bucket |
|---|---|---|
| 278.91 GiB | 1.62 h | **~0.63 h** |
| vs today's 4.97 h | 3.07x | **7.9x** |
| storage 24–48 h | $0.42–$0.83 | **$0.20–$0.39** |

**≥4x is cleared.** And the storage is 2.1x cheaper as well, because GCS bills stored
bytes while a disk bills provisioned size — 316 GB provisioned to hold 279 GiB, the
capacity headroom §13.15 spent a table on.

#### What this overturns

* **§13.46's "the disk is the answer" stands as a fact about the disk and is no longer
  the conclusion.** 43.9 MiB/s sustained, the downloader within 1% of `dd` — all still
  true. It simply stopped being the ceiling that matters, because the workload does not
  have to go through a disk.
* **§13.46's disk-sizing economics are moot on this path.** Oversizing needed ~12 IO-bound
  consumers to break even at a spot VM rate; the bucket needs none, is faster, and costs
  less to store. The analysis was correct and is now about a road not taken.
* **§6.5g's "do not build the reader/writer split" was route-specific and the other route
  is different.** On the pd-standard the overlap ceiling was 1.05x. Here it is 1.32x, and
  against a source measured at ~240 MiB/s a decoupled loop could take 146 toward ~193.
  Worth building now; it was not then.
* **§13.49's `bucket_upload_wait_tries` placeholder can be re-derived.** At ~0.63 h for the
  largest realistic input, the 1 h ceiling it was raised from was probably adequate after
  all, and 180 is over-provisioned. §6.7's `pdl claim` supplies the number — and the input
  count question still needs answering, since the timeout covers a whole localization.

#### What is not yet established

The full-size run with verification is in flight and is the first exercise of the ETag
path against real GCS; `etag from 9849 recorded part digests, 0 re-read` is the claim.
crc32c remains unimplemented — it is the only digest a composite carries and the only one
that combines across out-of-order parallel chunks, so it would make verification
metadata-only rather than a read-back on the md5 path. And peak RSS went 61 → 397 MiB:
accumulation is ruled out (flat at ~32 MiB across a 32x size range, in isolation) but the
397 itself was not reproduced, so that risk is bounded rather than explained.

#### The pattern, one more time

Three months of this effort have produced the same lesson in four different places: §6.5c–
§6.5i compared a sustained rate against a burst rate, §13.49 sized a relay constant from a
disk measurement, §13.46 priced VM time at on-demand rates for a preemptible VM, and this
section found a 5x throughput cap sitting in a constant that had been chosen for a reason
nobody re-examined. **Every one was a correct number applied to the wrong system.** The
defense that actually worked, each time, was a second measurement taken under deliberately
different conditions — two disk sizes, two sources, two block sizes — because a wrong
premise survives repetition and dies on contrast.

### 13.51 Measured at full size: 4.97 h → 0.56 h, verified, no read-back

The full 278.91 GiB BAM, GDC S3 → bucket, 16 connections, 8 MiB block:

```
2031.41 s   140.60 MiB/s   relay 1802.4s   compose 36.5s   hash ok
etag from 9849 recorded part digests, 0 re-read
```

| | today | pd-standard | **bucket** |
|---|---|---|---|
| 278.91 GiB | 4.97 h | 1.62 h | **0.564 h** |
| speedup | — | 3.07x | **8.8x** |

**§8.5's ≥4x target is met**, on the path the disk could not reach, with the object
verified against its real multipart ETag.

#### What the run establishes beyond the headline

* **`0 re-read`.** The ETag came entirely from digests recorded as the bytes were relayed.
  This route returned `EXIT_FAIL` on any ETag source until §6.6 — it could not localize the
  actual workload at all — and it now verifies a 279 GiB object without fetching a byte
  back. #19's guarantee, extended to the relay.
* **Nothing degraded at scale.** 3283 chunks, 9849 parts, a three-level compose tree in
  36.5 s, `wire 1.01x` (no duplicate fetching), `streams 15.47 of 16`, peak RSS 402 MiB
  inside its budget. The 96 GiB prefix predicted 146.4 MiB/s relay and the full object
  delivered 158.5.
* **Consumers gain too: 106.63 MiB/s through gcsfuse**, from the benchmark's own re-read
  of the finished object. That is 2.4x the pd-standard's sustained read, and it is the
  number §13.46's break-even analysis needed and did not have. Both sides of that cost
  model now favour the bucket.

#### One thing got worse, and it is mine

`commit` was 8% of wall at 12 GiB, 16% at 96 GiB, and **43% at full size** — 768.3 s over
3221 batches, `mean batch 1.0`. Scaling is superlinear: 2.14x the chunks between the 96 GiB
and full-size runs, 7.2x the commit time.

The cause is the ETag work. The manifest is rewritten **in full on every chunk completion**,
and it now carries 9849 part digests alongside 3283 chunk records — so each write
re-uploads a larger document, and the cost goes as chunks x manifest size. At ~0.55 s
between completions the deferred writer drains each one before the next arrives, so the
batching meant to amortise this never engages.

It is off the worker pool and not in the critical path (`relay 1802 + compose 36` accounts
for the 2031 s wall), so it cost nothing measurable here. It is still ~1.6 GB of redundant
uploads running alongside the transfer, and it grows quadratically — at 1 TB it would stop
being free. A short accumulation window in the writer is the fix.

**The `NOT BATCHED` guard fired on all three runs and I explained it away twice** — once as
"nothing to batch at 4.7 MiB/s", once as "it will fix itself when the upload block goes
up". Both were plausible and neither was checked against the next data point. The guard was
measuring a real property the whole time.

#### Where this leaves the effort

The original question — 300 GB BAMs taking upwards of 4 hours — is answered: **0.56 h,
verified, on infrastructure that also stores the result 2.1x cheaper**. What remains is
tidying rather than discovery:

* the commit scaling above;
* **crc32c**, still unimplemented, which would make md5-sourced objects verify from
  metadata instead of a 62%-of-wall read-back;
* **`bucket_upload_wait_tries`**, raised to 180 against a 1.62 h worst case (§13.49) and
  now facing a 0.56 h one — `pdl claim` can re-derive it, and the answer is probably that
  the original 60 was fine;
* the **reader/writer split**, which §6.5g rejected at a 1.05x ceiling on the disk and
  which is 1.41x here — worth ~40% on a route that is now 71% read-bound.

#### 13.51a Confirmed: the commit fix, and what "off the critical path" means

Re-ran the full 278.91 GiB with the linger (§13.51's open item, `969c9cb`):

| | before | after |
|---|---|---|
| `commit` | 768.3 s, 43% of wall | **138.4 s, 8%** |
| batches | 3221, mean 1.0 | 685, mean **4.8** |
| relay | 1802.4 s | 1787.6 s |
| total | 2031.4 s | 2045.8 s |
| hash | ok | ok |

**5.55x less commit work; the headline did not move.** That is the confirmation, not a
disappointment: `commit` was always off the worker pool, so the prediction was that fixing
it would change the concurrent load and nothing else. ±1% on both phases is noise. Had the
total dropped, it would have meant the manifest traffic was contending with the transfer
after all — which nothing had shown, and which this rules out.

Per-batch cost is unchanged at ~0.2 s. A manifest rewrite costs what it costs; the fix was
never about making it cheaper, only about doing it 4.8x less often.

Consumer read through gcsfuse now has two independent measurements — **106.63 and
111.09 MiB/s** — so ~109 MiB/s, 2.5x the pd-standard's sustained read. §13.46's break-even
analysis can use that with more confidence than a single figure.

**The final numbers for the effort, verified twice:**

| | today | pd-standard | bucket |
|---|---|---|---|
| 278.91 GiB | 4.97 h | 1.62 h | **0.57 h** |
| speedup | — | 3.07x | **8.7x** |
| consumer read | — | 43.9 MiB/s | **109 MiB/s** |
| storage 24–48 h | — | $0.42–$0.83 | **$0.20–$0.39** |

### 13.52 `bucket_upload_wait_tries` settled at 90, and the unit was wrong

§13.49 raised this to 180 as an explicit placeholder, chosen on risk asymmetry because
the relay's throughput was unknown. It is now known, so the placeholder can go.

**No new benchmark was needed.** §6.6's two full-size runs — 2031.4 s and 2045.8 s, 0.7%
apart — are exactly what `pdl claim` exists to produce. Running it at `--repeat 3` would
have spent ~1.7 h of transfers adding a third sample to a number two runs already
bracket. Applying its arithmetic directly: 0.57 h + the 60 s bucket-create ceiling,
doubled for safety, is **71 polls**. The default is now **90**.

| | ceiling | verdict |
|---|---|---|
| 60 (original) | 1.00 h | would fire occasionally on a healthy upload |
| **90** | **1.50 h** | clears the measurement at 2x safety, 1.3x the need |
| 180 (§13.49) | 3.00 h | 2.5x over — three hours of stall per preemption |

The constant is bounded on **both** sides and §13.49 only argued one. Below the
measurement, healthy uploads are declared dead and taken over mid-flight, charging a
duplicate transfer on every large input. Above it, the same number is how long every
sibling waits after an uploader genuinely dies — routine on preemptible workers. The
tests now pin both, and mutating the default to 60, 180 or 300 each fails one of them.

#### The parameter was measuring the wrong thing

`pdl claim` took `--inputs-per-localization`, a **count**, and multiplied the transfer
time by it. That silently assumes every input is the same size. The real shape of this
workload is one BAM plus one index: **four inputs would be ~1.0x the time, and a
count-based scaler would have said 4x.** Two BAMs is two inputs and genuinely 2.0x.

So the unit is bytes — `--localization-bytes`, the total the largest real localization
relays. And only some inputs count at all: `gs://` sources take canine's `server_side`
path, a GCS-to-GCS rewrite that moves nothing through the VM, so a set of one S3 BAM and
three reference genomes is still a multiplier of 1.

This is the same error as §13.49's, one level down. That one sized a relay constant from
a disk measurement; this one sized it in the wrong unit. Both were plausible quantities
standing in for the quantity that mattered, and both survived review until something
forced the arithmetic to be written out.

### 13.53 The reader/writer split: +6%, reverted, and the ceiling was loose

§13.51 listed this as the last optimisation worth having: §6.5g had rejected it for the
pd-standard at a 1.05x overlap ceiling, the bucket route showed 1.38x and 73% read, and
the rejection was explicitly route-specific.

Built as a single-slot prefetch per chunk — not a reader/writer pool, because a pool
reorders writes and two invariants forbid that (GCS resumable sessions must be written
sequentially, and `_hash` only records an S3 part seen contiguously from its first byte).
Measured against two baseline runs 0.7% apart: **1911.6 s against 2038.6 s, −6.2%.**

Real, and a quarter of what was advertised. Perfect overlap would have been ~1297 s of
relay; this reached 1679.9 s, **22% of the available gain**. The reason is in the same
`io` line: **write time rose 9.2%** while read fell 12.6%. Once the two halves genuinely
run at once they contend — for the NIC (peak 223 → 247 MiB/s) and for the interpreter —
so part of the saving came straight back. The ceiling *rose* to 1.47x, which says a
deeper queue would not help either.

**Reverted.** 6% on a target already met at 8.7x, against a thread pool and a
drain-on-rewind path in the hot loop, and +30% memory that pushed peak RSS past the
guard's budget. The code is gone; the number stays.

#### The transferable finding

`overlap ceiling` is `(read + write) / max(read, write)` — an upper bound on decoupling
*if the halves are independent*. On the pd-standard they were, and it correctly said
there was nothing to get. Here they are not, and it overstated the gain by a factor of
four.

So it is a screening statistic, not a forecast: **a low ceiling reliably says "don't
bother", a high one only says "worth measuring".** §6.5g's conclusion was right for the
right reason and this one was right for a reason I had to build the thing to discover.

#### And the tests were wrong four times

Worth recording because the failure wore a different disguise each time, and three of
them passed:

1. Counted reads *executing* at `connections=1` — the pool is single-threaded there, so
   two submitted reads simply queue.
2. Counted globally across streams — would have flagged correct code, since N connections
   legitimately read from N streams at once.
3. Keyed on `id(getattr(fn, "__self__", None))` — every non-bound-method submission,
   including the verification pool's, collapsed into one bucket and reported 7
   outstanding on correct code.
4. The real one: with the default 8 MiB block against a 1 MiB chunk there is exactly one
   block per chunk, so the read-ahead never fires. **Every test in the class was passing
   against an unreachable feature.**

The first three are variations on measuring the wrong quantity. The fourth is this
effort's signature defect — a test that exercises nothing and reports success — and it
survived three rounds of me specifically looking for that.

### 13.54 Declined: deriving the S3 part length for DRS-supplied signed URLs

A DRS input goes through the parallel downloader already (`HandleDRSURI`, mode `"url"`,
with `url_refresh_cmd = resolver` so an expired signature re-resolves through drshub), but
it verifies with drshub's **whole-file md5**. That cannot be assembled from out-of-order
parallel chunks, so it forces the read-back the multipart-ETag path avoids — 62% of wall
on the bucket route, ~52 min on the disk.

The ETag itself is available: S3 returns `ETag: "<md5>-<N>"` on a GET, including through a
presigned URL, and the downloader's probe already makes that request and discards the
header. What is *not* available is the part length. `?partNumber=1` cannot be appended to
a presigned URL, because the signature covers the query string.

It is derivable. `P` must satisfy `P ∈ [ceil(size/N), floor((size-1)/(N-1))]`, and for the
279 GiB object that window is 3087 bytes wide and contains exactly one multiple of 1 MiB —
30408704, the true 29 MiB. Uniqueness holds whenever `P < N MiB`, i.e. for the large
many-part objects where the saving matters.

**Declined anyway, and the reasoning is worth keeping.** §13.28 established that a wrong
stride is not a missed optimization: `verify()` raises `PermanentError`, `discard()`
deletes the file, and the job exits do-not-retry — a byte-perfect 279 GiB download thrown
away. Today the stride is authoritative (`head-object --part-number 1` and `2`, plus the
`(count-1) x first + last == size` identity). A derived stride has no such confirmation, so
a mismatch is ambiguous between bad bytes and bad arithmetic.

The proposed resolution was to demote on mismatch — treat it as derivation failure and
fall back to drshub's md5 rather than discarding, which DRS uniquely permits because it
supplies an independent whole-file digest. That is sound on paper and wrong in practice:
**the fallback runs only on mismatch, so it is the least-exercised path in the system, and
its job is to prevent destroying a 279 GiB download.** Code with that duty cycle and those
stakes is the wrong place to put confidence. The uniqueness test has the same problem — it
must be decided at runtime, and the moderate-`N` boundary is simultaneously where a wrong
answer is likeliest and where it gets least testing.

So: **no part length from metadata, no ETag path.** Same conclusion as §13.28 —
*better no verification than one that fails on correct data* — extended to the optimization
it would have enabled.

**There is no safe variant here, and two earlier drafts of this section got the reason
wrong.** The first claimed one existed: when a DRS URI resolves into a bucket we hold
credentials for — "the GDC case" — `head-object --part-number 1` would give an
authoritative part length. The second corrected that to "GDC has no credentialed
endpoint", which is also false. The accurate picture:

`HandleGDCHTTPURL` tries DRS **first** and falls back to the GDC API:

```python
try:
    self.uri = gdc_drs_root + self.uuid
    self.drs_obj = HandleDRSURI(self.uri, **self.extra_args)
except:
    canine_logging.warning("Re-attempting with GDC API")   # X-Auth-Token
```

The GDC API **is** credentialed — an `X-Auth-Token` header for controlled data. It is
tried second only because it is historically slower than a signed URL. What it lacks is
not credentials but an **S3 API surface**: it is an HTTP file endpoint, so there is no
bucket, no key and no `head-object` to ask. Same for the DRSHub signed URL, whose
credential is embedded in the signature.

**And the decisive objection, which neither draft reached: a multipart ETag is a property
of an upload, not of the content.** The DRSHub-supplied object and the GDC-API-served
object may live in *different object stores*, uploaded with different part sizes — so the
same bytes can carry two different ETags and two different strides. There is no canonical
part length for a GDC file at all. That is precisely why drshub publishes `hashes`
(md5, a property of the content) and not an ETag.

Which makes the failure mode concrete rather than theoretical. `HandleDRSURI` sets
`url_refresh_cmd = resolver`, so an expired signature **re-resolves through drshub
mid-transfer** — the mechanism that makes DRS usable at all. If that resolution lands on a
different store, the part digests already computed against the first layout are compared
against the second's ETag, mismatch, `PermanentError`, `discard()`, do-not-retry. A
byte-perfect 279 GiB download destroyed by the code path that exists to keep the transfer
alive.

So the rule is not "decline until we find a safer variant" but something sharper: **an
ETag may only ever be trusted from the same response whose bytes are being hashed, and
never as an attribute of the file.** On the S3 source that condition holds — bucket, key,
endpoint and credentials are fixed for the whole transfer and `head-object` answers
authoritatively about that object in that store. On DRS it cannot be made to hold, because
the source may legitimately change underneath. The `0 re-read` result belongs to sources
that pin bucket, key and credentials; DRS by construction does not pin any of them.

**Cost of declining, so it is priced rather than forgotten:** a large DRS input keeps its
whole-file md5 read-back. To a bucket destination that measured 43 min for 279 GiB against
~35 min of transfer, so verification is the larger half. Accepted; correctness is not worth
trading for it.

### 13.55 `--gunzip` on the bucket route: why it was refused, and the design (built in §13.56)

`run()` dispatches to `run_bucket_route` / `run_staged_route` and returns **before** the
gunzip block, which lives on the POSIX path only. So a `Content-Encoding: gzip` source
localized to a gcsfuse mount was relayed to GCS still compressed and composed, and since
`compose` sets no `contentEncoding`, GCS will not transcode it on read and gcsfuse serves
the gzip stream verbatim under a name promising plain content. Silent wrong output,
surfacing only in whatever tool reads the mount.

Reachable from exactly two handlers — `HandleGCSSignedURL` and `HandleOtherURL`, the only
callers of `_probe_http_metadata` and therefore the only ones that set
`body_is_compressed`. **`gs://` inputs are unaffected**: they take the `server_side` path,
where `gcloud storage cp` decompresses. `.bam`/`.bai`/`.bcf`/`.csi`/`.tbi` are unaffected
because `name_implies_gzip` suppresses the flag.

Which is what makes it worth stopping for: `file_handlers.py:509` records removing exactly
this inconsistency — *"previously the same object arrived decompressed via gs:// but
compressed via a signed URL"* — and the bucket route reintroduced it, for the same
objects, on the destination `create_bucket_mount()` is making the default.

Refused for now (`8fbc620`): `EXIT_FAIL` when a route cannot honour the flag.

#### Two constraints, and the plan they killed

**Gzip is sequential.** Chunk N cannot be decoded without N−1, so a parallel chunked
transfer can never decompress *in flight* on any route. **And the advertised digest covers
the compressed bytes**, so verification must precede decompression — which also means the
decompressed output has no digest to check against. "Received bytes verified, then
transformed" is the strongest guarantee available anywhere.

My first reading of the sequentiality constraint was that it *forced* routing gunzip
inputs to stage-publish, since that route has a disk to materialize onto. That was wrong,
and wrong in an expensive direction: stage-publish downloads onto the pd-standard at
44 MiB/s, so a 279 GiB input would take ~1.8 h against ~34 min on the relay path, plus a
staging disk sized for the compressed object.

The constraint says the compressed bytes must exist somewhere before decompression. It
does not say they must exist on a local disk — **after compose they exist in the bucket.**

#### The design

Keep the relay untouched. After compose, and after the existing hash validation of the
composed compressed object, run a streaming second pass: read it back with sequential
`download_range` calls, through `zlib.decompressobj`, into a fresh resumable upload, then
delete the intermediate. Every primitive already exists on `GcsClient`, and it needs **no
local disk** — both halves stream, memory stays at one buffer.

GCS has no server-side decompression, so this is a genuine full read+write: roughly
45–90 min for 279 GiB. But the exposed sources are signed GCS URLs and plain-http
`.vcf`/`.tsv`, typically orders of magnitude smaller than a BAM, and the alternative was
paying the slow disk for the *entire* transfer rather than one pass over a small file.

What it has to get right is crash consistency across two objects: the done marker must not
claim completion until the decompressed object exists, a preemption mid-pass must leave a
resumable state rather than the compressed object sitting under the final name, and the
intermediate must be deleted with the same care — it is a full second copy of the data.

#### Measured: `contentEncoding` does not help, and would make it worse

The one thing that could have removed the pass entirely was setting `contentEncoding:
gzip` on the composed object and letting GCS transcode on read. Measured on a throwaway
VM with **gcsfuse 3.11.2**, the version the worker image pins — three objects, the same
938-byte gzip payload, differing only in metadata:

```
                 gcsfuse ls   bytes read   md5
  plain.txt       360000       360000      5e4f4051…   plaintext
  gz-noenc.txt       938          938      40f322be…   gzip
  gz-enc.txt         938          938      40f322be…   gzip   <- Content-Encoding: gzip
  head -c 2 gz-enc.txt -> 1f8b                                    still a gzip stream
```

**gcsfuse ignores `contentEncoding` entirely.** The encoded object is byte-identical to
the unencoded one for a reader, so the metadata buys nothing. It is at least
self-consistent — 938 reported, 938 delivered — so this is *not* §4.7's
metadata-disagrees-with-bytes trap. It is simpler: stored bytes, served verbatim.

The same objects through `gcloud storage cp`, which is what the `server_side` path uses:

```
  gz-enc.txt      360000 bytes  md5=5e4f4051…   decompressed
  gz-noenc.txt       938 bytes  md5=40f322be…   gzip
```

So `gcloud` **does** honour it and gcsfuse does not. Which turns a would-be fix into a
new defect: setting `contentEncoding` makes the object **ambiguous** — decoded for a
consumer using `gcloud storage cp`, still gzipped for one reading the mount. Two answers
for one file. That is worse than today's state, which is at least consistently wrong.

**Do not set `contentEncoding` on the composed object.** The post-compose decompress pass
is required, and this measurement is why.

### 13.56 The decompress pass, built

Implements the design in §13.55. `decompress_object(client, bucket, source, dest)` reads
the composed object back in `READ_BUFFER` ranges, feeds them through zlib, and writes the
output into a second resumable upload. No local disk; memory is one block plus at most one
granule of buffered output.

The bucket route now mirrors the in-place route exactly. Parts compose into
`<object>.k9pdl.gz` rather than into the destination, that sidecar is what the digest is
checked against, the decode publishes the destination, and only then are the sidecar and
the parts deleted. The refusal in `run()` stays for stage-publish, which still has no
transform step to hang this on.

Four things this surfaced that the design note did not anticipate.

**The unknown total.** The decompressed length cannot be known in advance — gzip's ISIZE
is modulo 2**32, so it is unusable above 4 GiB, and measuring it honestly means
decompressing the object twice. So `upload_range` grew `total=None`, which sends
`Content-Range: bytes X-Y/*`. GCS accepts such a PUT only when its length is a multiple of
the 256 KiB commit granularity, which is why output is buffered to a granule before it is
sent; the final PUT carries the real total and is what completes the session. When the
decoded length lands exactly on a granule boundary there is nothing left to send, and the
session is finalized by a bodiless `bytes */TOTAL` — which is what `session_offset`
already sends.

**Multi-member gzip.** `zlib.decompressobj` decodes exactly one gzip member and then sets
`eof`, leaving the rest in `unused_data`. Concatenated members are valid gzip and are how
bgzip writes, so the obvious single-decompressobj implementation would truncate every
bgzipped input to its first block **and report success** — readable output, wrong content,
no error anywhere. `gzip.open`, which the in-place route uses, handles this for free.
`GzipStreamDecoder` does it by hand, and ignores trailing NUL padding for the same reason
the reference implementation does.

**The marker could never be believed.** The done marker's falsifiability check compares the
marker's `size` against what is actually on the destination. With `--gunzip` those are
different numbers by definition, so the check disagreed on every run and the marker was
worthless — a re-run re-downloaded the whole object. This was **already true of the
in-place route**, and had been since `--gunzip` was added; it went unnoticed because
nothing measured a second run of a gzip-encoded input. Fixed by recording `stored_size`
alongside `size`: `size` still identifies the plan, `stored_size` is what the presence
check compares.

**Exit codes.** Found by mutation-testing the new tests: a `TransientError` raised inside
the decode pass escaped `run()` as an uncaught traceback, which SLURM reads as
do-not-retry. The pass now distinguishes the two cases properly. A transient failure
requeues (5) and deliberately leaves the parts, the sidecar and the manifest in place, so
the retry pays for the decode and not the download. A `PermanentError` means a mid-stream
decode failure on bytes that already matched the advertised digest — the source object
itself is broken, no retry can help — so it cleans everything up rather than leaking a
full second copy of the data that nothing will ever collect.

The "server's metadata is wrong" cases are kept, not failed, matching `gunzip_to`: bytes
that are not gzip at all, and a `.gz`-named destination where decoding would not produce
gzip (the metadata was set on a singly-compressed object). Both are decided from the first
decoded bytes, before any upload starts, so the fallback costs one single-source compose —
a server-side copy — rather than a discarded upload.

126 tests on the bucket route, 773 across the parallel-download suites. Mutation-checked
against five mutations: a single-member decoder, unbuffered output, a skipped sidecar
delete, a dropped `stored_size`, and ignoring the flag entirely — the last of which is the
original bug, and fails nine ways.

### 13.57 §6.6b run: the pass works on real GCS, and one header does more than documented

Ran §6.6b end to end on a fresh `pdl-bench` (`n1-standard-8`, `us-east1-b`, gcsfuse 3.11.2
on `slurm_gcp_docker:v0.18.3`), 2026-09-23. Source 170401724 bytes gzip → 328888890 plain,
1.93:1. Node and bucket torn down; raw JSON in `benchmark-results/gunzip-*.json`.

**It works.** The unknown-total PUT sequence — the one request shape nothing else in
canine sends, and the one thing the fake could have been wrong about — is accepted by real
GCS. Destination md5 equals the original plaintext's and `cmp` is byte-identical. The
marker is believed on a second run (**1.25 s** against 19.78 s), so `stored_size` is
confirmed outside the fake. The keep-as-is branch fires on a `.gz` name in 0.3 s, and the
kept bytes are byte-identical to what was uploaded.

Three things the run established that the design did not.

#### `Accept-Encoding: gzip` is what makes ranges work at all

Measured both ways against the same object:

| | status | body |
|---|---|---|
| with `Accept-Encoding: gzip` | **206**, `content-range: bytes 0-99/170401724` | 100 bytes, `1f8b` |
| without | **200**, Range *ignored* | 328888890 bytes, fully decompressed |

The header was documented as "ask for the encoded body". It is stronger than that: without
it GCS ignores `Range` outright, so **no ranged transfer of a gzip-encoded object is
possible without it**, and the `Content-Range` total is the compressed length that `--size`
must match. `_pdl_command` emits it (`file_handlers.py:290`), so production is correct;
anything driving `parallel_download.py` directly has to supply it, and the runbook's first
draft of §6.6b did not.

#### A fallback in a benchmark is not a pass

Omitting it does not fail loudly. The downloader correctly detects the 200 and calls
`single_stream_fallback`, which runs `options.legacy_cmd` — and in production that is the
`file_handlers` pipeline carrying its own hash check and gunzip stages. **The benchmark
passes no `--legacy-cmd`**, so the fallback is a bare `curl`: no verification, no decode,
**rc=0 in 8.76 s**. The file it left was correct only because GCS transcoded it on the way
out, and the sole signal was `gunzip phase : did not decode`.

Not a product defect — the production path is covered — but it means a `routeb` run that
falls back has measured `curl`, and the runbook now says so in those words.

#### The decode is 5× the relay, not a cheap second pass

§13.55 justified the post-compose design partly on the decode being one cheap extra pass.
The phase split says otherwise:

```
relay   2.3s   (170 MB in, 16 connections, 74.1 MB/s)
compose 0.1s
gunzip 11.6s   (170 MB in, 329 MB out, 28.4 MB/s written)
```

**11.6 s against 2.3 s.** The gap is structural rather than incidental: the relay is
16-way parallel and the decode is a single sequential read-plus-write that cannot be
parallelised, because gzip is sequential — the same property that forced the post-compose
design in the first place. It also widens with the compression ratio, since every point of
ratio is another byte the decode writes and the relay never moved.

That does not change the decision — the alternative was stage-publish at 44 MiB/s for the
*entire* transfer, and refusing outright was worse than both — but "one cheap pass" was
wrong and the sizing guidance in §6.6b now reads **5× the relay for a 2:1 source**. It also
sharpens the resumability gap recorded in §6.6b step 6: a preemption costs the expensive
half of the run, not the cheap one. If gzip-encoded inputs turn out to be BAM-sized rather
than `.vcf`-sized, the decode needs a manifest of its own.

#### Runbook errors this shook out

All fixed in the same commit. Worth listing because five of the six were mine, written
into §6.6b without being run:

* §6.6a says to create the bucket **on the node**; the `v0.18.3` image ships gcloud
  **406.0.0**, which has neither `--lifecycle-file` on `buckets create` nor
  `--soft-delete-duration` at all. Create it from the workstation.
* `objectAdmin` does not grant `storage.buckets.get`, so any `gcloud storage` call as the
  node SA fails with *"or it may not exist"*, which reads like a missing bucket.
  `legacyBucketReader` is the narrow fix. The downloader's own calls never need it.
* `gcloud storage cp` parallel-composite-uploads at 150 MB, and a composite has **no
  md5Hash** — so §6.6b's "confirm the stored digest is the compressed one" step silently
  became impossible. Raise `storage/parallel_composite_upload_threshold` first.
* §6.6b step 3 omitted `--header "Accept-Encoding: gzip"`, per above.
* §6.6b step 4 checked `customTime`. `pdl routeb` never stamps it — that is canine's
  produce path — so the check fails by construction. Removed.
* The generator measured 1.93:1, not the 1.88 recorded from a local 1/20-scale probe.
  Close enough that the PUT estimate held, and the runbook already said to check it.

One asymmetry left as-is: the keep-as-is branch publishes by composing the sidecar, so the
destination comes back `componentCount: 3` with no `md5Hash`, where the in-place route
renames and preserves the file. Bytes identical either way, and every bucket-route object
already lacks an md5 (§6.6), so this is the route's existing property rather than a new one.

### 13.58 Instrumenting the decode, and why not a resume manifest

§13.57 measured the decode at 5.0× the relay. Two fixes suggest themselves — pipeline it,
or make it resumable so a preemption does not redo it — and they point in different
directions depending on which stage dominates. Nothing in the logs said which, so the
decode now reports its own split:

```
k9pdl-gunzip read 4.102s inflate 1.088s write 6.410s over 21 blocks
              (170401724 -> 328888890 bytes, ceiling 1.81x, pipe2 1.81x)
```

Same shape and same arithmetic as `k9pdl-io`: `ceiling` is `total / max(stage)`, bounded
by 3.0 here rather than 2.0 because there are three stages. Deliberately not
`1/(1 - share)` — that form assumes the hidden stages become free, and it reported
infinity the first time a relay stage rounded to zero. `pipe2` is the ceiling for the
cheap version, one thread reading and inflating against another uploading, which matters
because the inflate is CPU and the other two are network; if `pipe2` and `ceiling` are
close, a third thread buys nothing.

**This is instrumentation, not a fix, and the distinction is the point.** The relay looked
pipelineable by exactly the reasoning that makes the decode look pipelineable now. It
measured 95% write-bound with a 1.05× ceiling, the prefetch got built anyway, and it came
out **−6.2%** and was reverted (§13.53). The numbers in the sample above are illustrative;
the next §6.6b run fills them in, and a ceiling near 1.0 is an instruction to stop.

#### The manifest: no, and for a harder reason than cost

**`zlib` cannot be checkpointed.** Python's stdlib exposes no `inflatePrime`, and
`decompressobj.copy()` is in-memory only — it does not survive the process. The
random-access gzip technique (`zran`, as used by `indexed_gzip`) needs exactly that
primitive plus 32 KiB of history per checkpoint, and `parallel_download.py` is staged
standalone onto nodes and must not take dependencies, so that route is closed by
construction rather than by preference.

So "resume the decode" can only mean: re-read the compressed prefix, re-inflate it,
discard the output, and resume uploading at the frontier the resumable session already
knows. **A manifest buys back the upload third only** — and the session frontier is
already queryable via `session_offset`, so even that would not need a manifest, just a
persisted session URI.

Priced: inflate is ~1.1 s of the measured 11.6 s (329 MB at roughly 300 MB/s), so ~90% is
serialized network across a 170 MB read and a 329 MB write. A resume saves at most ~60% of
the decode, ~30% averaged over where a preemption lands. At ~5%/hour, expected loss
without any of this is about `4×10⁻⁶·D²` seconds:

| decode | implied gzip source | expected loss |
|---|---|---|
| 12 s | 170 MB | 0.0006 s |
| 10 min | ~10 GB | 1.5 s |
| 1 h | ~60 GB | 54 s |
| 4 h | ~250 GB | ~15 min |

It does not pay until the decode runs for hours, which needs a ~250 GB source that is
**doubly** compressed. The realistic large-file case — a BGZF `.bam` mislabelled as
gzip-encoded, singly compressed — takes the keep-as-is branch and aborts in **0.3 s**
measured, because the decision comes from the first decoded bytes. `--gunzip` is never
suppressed by extension (`file_handlers.py:598` sets it from `body_is_compressed` alone),
so those files do reach the pass; they just leave it immediately.

Deferred, with the trigger written down: revisit if a real input is ever observed decoding
for hours. Pipelining is the better first move if the split says so, because it helps every
run rather than the rare preempted one, and shrinks the preemption window as a side effect.

Noted in passing: `name_implies_gzip` now has **no callers** — dead since the decision
moved to `expected_magic`. Left alone here rather than mixed into an instrumentation
change.

### 13.59 The split, measured: this loop is not the relay, and 5× was a point on a curve

Re-ran §6.6b on a fresh node with the §13.58 instrumentation, at two sizes. Raw JSON in
`benchmark-results/gunzip-split-*.json`; node and bucket torn down.

| | read | inflate | write | ceiling | pipe2 | decode/relay |
|---|---|---|---|---|---|---|
| 170402482 → 328888890, 1 member | 2.420 s (23%) | 2.047 s (19%) | 6.145 s (58%) | 1.73× | 1.73× | **4.2×** |
| 852012410 → 1644444450, **5 members** | 26.080 s (40%) | 10.135 s (16%) | 29.077 s (45%) | **2.25×** | 1.80× | **7.9×** |

Both byte-exact against the originals. The 852 MB source is five concatenated members, so
`GzipStreamDecoder` is now confirmed on real GCS at 102 blocks and 1.6 GB of output, not
only in the fake — which matters, because the single-member version of that decoder would
have silently produced one fifth of the file and reported success.

#### The answer is the opposite of the relay's

§13.58 was written expecting this might look like the relay: one stage dominant, ceiling
near 1.0, don't build it. It does not. At 852 MB the split is **write 45% / read 40% /
inflate 16%** — balanced, with a **2.25×** ceiling against the relay's 1.05×. Pipelining
has something real to work with here.

`pipe2` — one thread reading and inflating, another uploading — is **1.80×**, so the cheap
two-thread version captures **80%** of the available win (65.3 s → 36.3 s, against 29.0 s
for a full three-way split). At 170 MB `pipe2 == ceiling`, because `read + inflate` (4.5 s)
is still under `write` (6.1 s) and a third thread would simply idle.

**The tempting reading of those two rows is wrong.** "The third thread pays more on bigger
files" is what they look like, and it is not what they measure. Per byte:

| stage | 170 MB | 852 MB | change |
|---|---|---|---|
| inflate | 160.7 MB/s | 162.3 MB/s | ×1.01 |
| write | 53.5 MB/s | 56.6 MB/s | ×1.06 |
| **read** | **70.4 MB/s** | **32.7 MB/s** | **×0.46** |

Inflate and write scale essentially perfectly; **read halved**, and that alone pushed
`read + inflate` from 0.73× `write` to 1.25×. The third thread did not become more useful
because the file got bigger — it became useful because read got slow. Three candidates,
inseparable at n=1 per size: run variance (a single-stream GCS read swinging 2× is
ordinary, and both numbers are plausible for one stream); a real size effect; or
**component count**, since the small object composed from 3 parts and the big from 13, and
part count scales with size. The third is the most testable and the only one that would be
fixable without touching threading.

This is §6.5c–§6.5f again, and I walked into it the same way: two measurements at
different sizes, the difference attributed to the size, when the variable that actually
moved was something else. There it was the disk's burst-versus-sustained behaviour. Here
it is whatever slowed the read, and the discriminator is cheap — repeat each size, then
hold size fixed and vary the part count via `--connections`/`--min-chunk`.

**So: build the two-thread split first, re-measure, and only add the third if the table
still says so at the size that matters.** And treat 2.25× as an experiment worth running,
not a forecast — §13.53 records the ceiling overstating when the stages are not
independent, and read and write here are both network on one NIC, contending in a way the
arithmetic does not model. That is precisely how the relay's predicted win became −6.2%.

#### 5.0× was one point on a rising curve

§13.57 reported the decode at "5.0× the relay" from a single measurement and I wrote it
into the runbook as a sizing rule. It is **4.2× at 170 MB and 7.9× at 852 MB**, and it
will keep climbing: the relay is 16-way and scales with the object, while the decode is
one sequential stream that cannot. Quoting a single ratio as guidance was the same error
as §6.5c–§6.5f — a number measured at one size applied to another.

The decode itself scales close to linearly: 5.0× the bytes for 6.2× the time.

This also sharpens §13.58's deferral of the resume manifest rather than reversing it. The
manifest still buys only the upload third, which is **45%** of the decode at the larger
size — but pipelining attacks the whole 65 s for every run, while a manifest attacks a
fraction of it only when a preemption lands. Pipelining first remains right, and by a
wider margin than the earlier arithmetic suggested.

### 13.60 The discriminator: it was none of the three candidates

§13.59 recorded the decode's read stage halving between 170 MB and 852 MB, with three
candidate causes. Ran the experiment (2026-09-23; `benchmark-results/gunzip-readbench-grid.json`,
`gunzip-warm-*.json`). Design: a size x component-count grid read with the real
`GcsClient.download_range` at 8 MiB blocks, 3 reps interleaved across objects so drift
hits everything equally; plus the decode run with and without `--md5` to test whether the
verify read-back warms the sidecar for the decode that follows.

| hypothesis | verdict |
|---|---|
| composite **component count** | **exonerated** — 88–96 MB/s steady at 1/3/4/11/13/51 components |
| a real **size** effect | **exonerated** — isolated reads 87 MB/s at 852 MB, 90 at 170 MB |
| the md5 read-back **warms** it | **refuted** — 3.306 s vs 3.302 s, identical |

**What it is: the first read of a freshly-written object runs at 33–40 MB/s against
88–96 MB/s steady.** A ~2.4× penalty, flat in component count, applying to all six
freshly-composed objects and to neither gcloud-uploaded source (spread across 3 reps:
2.2–2.7× for composed, 1.02–1.07× for uploaded). The decode always reads a sidecar it
composed moments earlier, so it always pays it.

Two earlier claims fall:

* The **70.4 MB/s** small-object read in §13.59 was an outlier. Repeats give 51.5 and
  51.6, so the small/big read gap is 1.7×, not 2.15×.
* **Read/write interference**, which §13.59 offered as the reason to distrust the
  ceiling, is **not established**. Against the cold isolated baseline the decode's
  interleaved read is 19% slower at 852 MB and 30% *faster* at 170 MB — inconsistent in
  sign. The ceiling's independence assumption remains untested rather than disproven, and
  I should not have asserted it as a caveat with evidence behind it.

Inflate (156–160 MB/s) and write (48–55 MB/s) are now flat across a 5× size range at n=4,
so the decode's only size-sensitive stage is the read, and only inside the decode loop.

#### This changes what to build first

The pipelining numbers are unchanged and reproduce well (`pipe2` 1.74–1.90 at 170 MB,
3-stage 2.23–2.35 at 852 MB, across four further runs). But the read stage is running at
**roughly a third of what the same object yields once warm**, and pipelining does not make
a read faster — it only hides it behind the write.

A **parallel read-ahead** does attack it: several ranged GETs in flight feeding the
sequential inflater. This is compatible with gzip's sequentiality, which constrains only
the *inflate* — the same observation that made the post-compose design possible at all
(§13.55). It is also the fix the relay already has, and the relay reads at 74–103 MB/s
where this reads at 30–51.

So the order is: measure a read-ahead first, pipeline second, resume manifest not at all
(§13.58 unchanged). And the cold-read penalty is worth understanding on its own — it
applies equally to the md5 verify read-back, which is the other full-object sequential
read on this route and ~5.8 s of unattributed time in the original §6.6b run.

### 13.61 The read-ahead, built and measured: 1.41x whole-run, and it supersedes the pipeline

§13.60 found the decode's read stage running at a third of the same object's warm
throughput and suggested a parallel read-ahead. Prototyped, measured, then built
(`--decode-readahead`, default 4).

#### Choosing the depth took three attempts, and the first two were invalid

**v1** minted fresh objects with `compose` and its depth-1 validity arm came out at
**88.6 MB/s** instead of the 33-40 cold figure. Compose moves no bytes, so the composed
copy references the same backing storage and inherits its cache state -- the objects were
never cold and the sweep measured the warm path. The arm existed precisely to catch this,
and did.

**v2** used separate uploads, which do get their own storage, but ran the depths in the
order `[1,2,4,8,16]` within every rep. Depth 1 was therefore the first read of its burst
three times out of three, absorbing whatever penalty running first carries. The 1.9x it
reported was partly an artifact of the schedule.

**v3** rotated the depth order across five reps. By-slot medians came out flat (99.4,
107.7, 105.9, 107.0, 103.3), which is what says the effect is depth and not order:

| depth | median | range | vs depth 1 |
|---|---|---|---|
| 1 | 59.3 | 42.9-72.2 | 1.00x |
| 2 | 105.0 | 93.0-119.1 | 1.77x |
| **4** | **140.3** | **129.3-147.7** | **2.37x** |
| 8 | 105.2 | 98.8-107.7 | 1.78x |
| 16 | 110.6 | 99.4-112.8 | 1.87x |

Ranges do not overlap between depth 1 and anything else, nor between depth 4 and the
rest. The curve is not monotonic -- 8 and 16 are both worse than 4, in two independent
sweeps -- so "more in flight" is not the mechanism and 4 is not a floor to raise later.

#### End to end

Two reps at each depth, alternated so ordering is not a confound, 852013000 ->
1644444450 bytes, all four outputs byte-identical:

| depth | routeb total | decode | blocked on read | write share of decode |
|---|---|---|---|---|
| 1 | 102.94 / 101.17 s | 67.8 / 68.8 s | 26.80 / 28.95 s | 45% / 43% |
| **4** | **72.12 / 72.37 s** | **39.8 / 40.7 s** | **0.273 / 0.241 s** | 72% / 73% |

**Whole-run 1.41x, decode phase 1.70x, and time blocked on reads falls 108x.** At depth 4
the fetch vanishes entirely behind the inflate and the upload; `stage["read"]` stops
measuring fetch time and starts measuring how often the consumer outruns the queue, which
is close to never.

#### It supersedes the pipelining recommendation

§13.59 and §13.60 both ended with "build the two-thread split". With the read gone that is
no longer the right next move: write is now 72% of the decode and the 3-stage ceiling has
fallen from **2.23-2.35x to 1.36-1.39x**. The read-ahead captured most of what the
pipeline was going to capture, for one bounded queue rather than a threaded pipeline with
a shutdown protocol.

What remains is hiding the 10.6 s inflate behind the 29 s write -- roughly 72 s -> 53 s,
and only if the two do not interfere, which is untested. The real bottleneck is now the
**single sequential resumable upload at 55-57 MB/s**. Parallelising that is not possible
within one session (offsets must advance in order), but it is exactly what the relay
already does with N sessions and a compose. That is the next thing worth pricing, not a
pipeline.

#### A test that measured nothing, again

The bound on in-flight requests is what keeps peak memory at `depth * READ_BUFFER`, and
the first version of that test asserted on *concurrent* calls. It passed against a
mutation that submits every block up front, because `ThreadPoolExecutor(max_workers=depth)`
caps concurrency by itself while completed payloads pile up in futures until the whole
object is resident. Same error as the relay's prefetch tests (§13.53) and the same shape
as counting executing reads at `connections=1`. Rewritten to measure how far the fetches
run ahead of the consumer: it now fails with "966 of 1000 fetches started while the
consumer took one block".

### 13.62 Sliced upload, and a do-not-retry bug found hiding as a flaky test

#### The sliced upload

A resumable session requires strictly advancing offsets, so one session cannot be
parallelised. Measured standalone on 1.64 GB: one session **81.3 MB/s**, width 2 159.4,
width 4 301.6, width 8 541.6 -- near-linear, widths rotated, non-overlapping ranges.

Built as `DecodeSliceUploader`: cut the inflater's output into 32 MiB slices, give each
its own session, compose at the end. Because a slice's length is known the moment it is
cut, every PUT carries a real total and the `total=None` protocol is not needed at all on
this path -- it now lives only at width 1.

End to end, 852013000 -> 1644444450, widths alternated:

| width | total | write stage | output |
|---|---|---|---|
| 1 | 73.63 / 74.38 s | 31.08 / 30.60 s | plain object, **md5Hash present** |
| **4** | **56.10 / 54.35 s** | **9.99 / 9.11 s** | 50 components, no md5Hash |
| 8 | 53.85 s | 9.48 s | 50 components, no md5Hash |

**Default 4, not 8.** At width 4 the upload (301 MB/s standalone) is already under the
inflater's 155 MB/s, and gzip inflation cannot be parallelised, so 8 measures 1.5% better
and doubles peak memory. The number to pick from is which stage is slowest, not which is
biggest.

One prediction missed: from the standalone 301 MB/s I expected ~5.5 s for the width-4
upload; it measured 9.1-10.0 s, about 170 MB/s. Interleaving with the inflater costs ~40%
that an isolated benchmark does not show -- the same lesson as §13.60, where isolated
reads ran at 87 MB/s and in-loop reads at 30-51.

**Cost:** the output is a composite and carries no `md5Hash`, where the single-session
path produced a plain object that did. Nothing on this route verifies that digest (the
advertised one covers the compressed bytes), but it is a real loss of observability, and
`--decode-upload-width 1` is the documented way back.

Cumulative for the decode work: `routeb` on this object went **102.9 s -> 54.4 s, 1.89x**,
across the read-ahead (§13.61) and this.

#### The bug the flaky test was reporting

`TestTwoWritersOnOneObject` had been failing about one full-suite run in four. I recorded
it as a rare pre-existing flake and moved on. That was wrong, and the reason it was wrong
is that a test named "two writers on one object" failing intermittently is a description
of the defect, not noise around it.

Two writers share deterministic part names, and each deletes the parts after composing.
So the loser can reach `compose` after the winner has consumed and removed its sources.
`compose_tree` had **no exception handling in `run_bucket_route`**, so the 404 surfaced as
`PermanentError`, which `main` turns into **EXIT_FAIL -- do not retry**.

The loser of the race therefore killed the job while the object it wanted sat complete and
correct in the bucket, where a retry would have returned EXIT_OK in milliseconds off the
marker check. Two workers on one object is a designed-for state (`LOCALIZATION.md` §3),
and a node dying mid-upload produces the same overlap with no timeout involved, so this is
not rare at scale.

The part-existence check immediately above compose already requeues for exactly this
reason. compose was simply never given the same treatment.

Fixed: a vanished-source compose now checks whether the destination landed. Present and
the right size -> EXIT_OK with a log line naming the other writer; otherwise EXIT_REQUEUE.
A wrong-sized object is never accepted as a winner's work, because that is the case where
declaring success loses data silently. A `TransientError` from compose requeues too -- it
was previously falling to `main`'s generic handler, which logs a traceback and returns 1,
the same do-not-retry outcome from a GCS hiccup.

Four tests force the interleaving rather than waiting for it, and all four fail against
the shipped code with the escaping exception. Ten consecutive full-suite runs clean
afterwards, against roughly one in four before.

**The lesson is about triage, not concurrency.** "Rare, pre-existing, not reproducible in
isolation" described the symptom accurately and was still the wrong conclusion: the test
only fails under load because that is when the interleaving happens, which is also the
condition production runs in.

### 13.63 The verify read-back, and where the time finally went

§13.62 left the md5 read-back as the biggest single cost in a `--gunzip` run and noted
the read-ahead had not been applied to it. Applied, and it was also not wrapped in a
`phase()`, which is why it had been sitting in "unattributed" for three rounds.

`verify_bucket_object` now reads through the same `ranged_blocks` the decode uses. md5 is
order-dependent, which is precisely why this needed a test rather than an assumption: a
read-ahead that delivered out of order would still produce a digest, just the wrong one,
and the failure would read as "the source is corrupt" rather than "the reader is broken".

| run | readahead | total | relay | verify | gunzip | other |
|---|---|---|---|---|---|---|
| ra1 | 1 | 76.38 s | 6.5 | 22.0 | 44.4 | 3.4 |
| **ra4** | **4** | **35.81 s** | 5.4 | **6.1** | 21.4 | 2.8 |
| ra4b | 4 | 36.31 s | 5.6 | 6.7 | 21.2 | 2.7 |
| ra1b | 1 | 81.90 s | 6.7 | 26.6 | 45.4 | 3.1 |

**Verify 22.0/26.6 s → 6.1/6.7 s, 3.8x** -- 852 MB at ~133 MB/s against ~35, which is
essentially the 140.3 MB/s that the rotated depth sweep measured as the depth-4 ceiling.
There is nothing further to get from that read.

Against the previous best (read-ahead on the decode only, sequential verify, width 4):
**55.2 s → 36.1 s, 1.53x.** Cumulative across the read-ahead, the sliced upload and this:
**102.9 s → 36.1 s, 2.85x.**

#### Where the remaining 36 s is

relay 5.4, verify 6.1, decode 21.4, other 2.8. The decode is inflate 11.4 + upload 9.7,
and both are close to their floors: gzip inflation cannot be parallelised at all, and
widening the upload past 4 measured 1.5% (§13.62). The only structural win left is a
2-thread inflate-against-upload split, worth roughly 21 s → 12 s if the two do not
interfere -- and the interference question is exactly the one that turned the relay's
prefetch into a -6.2% regression, so it needs measuring, not arguing.

Worth noting what the three changes have in common: none of them made a request faster.
They removed serialisation -- a read waiting on the previous read, a write waiting on the
previous write, a hash waiting on a fetch. The transport was never the problem, which is
the same conclusion §6.5i reached about the relay by a different route.

### 13.64 The inflate/write split: built, measured, reverted

The last structural win §13.63 identified was a 2-thread split of the decode's inflater
from its uploader -- the shape proposed as "thread 2 decompresses, thread 3 writes".
Built behind `--decode-queue` (a bounded queue and a writer thread), measured against the
ra4 baseline, and **reverted**.

| queue | total | decode | inflate | write |
|---|---|---|---|---|
| 0 | 36.31 / 36.57 s | 21.3 / 21.2 s | 11.3 / 11.9 | 9.7 / 9.0 |
| 2 | 33.81 / 36.57 s | 20.2 / 20.6 s | 16.2 / 16.5 | 3.7 / 3.7 |
| 4 | 35.31 s | 20.2 s | 16.2 | 3.8 |

Predicted 21.4 s -> ~12 s from the 1.8x ceiling. Measured **21.25 -> 20.4 s, ~4%** on the
decode phase and nothing at all in total: queue 2's two reps were 33.81 and 36.57, a
2.76 s spread that swamps the effect, and the second tied the queue-0 rep exactly.

**The mechanism is in the stage split.** Write collapsed 9.7 -> 3.7 s exactly as designed
-- the inflater really did stop waiting on upload slots. But inflate rose **11.3 -> 16.2 s,
43% slower**. Once the uploads genuinely overlap inflation they contend: `zlib` releases
the GIL, but the upload threads do their HTTP and TLS work in Python and hold it. The work
moved instead of disappearing.

This is the third time on this route that the overlap ceiling has overestimated, and the
reason is the same each time: `total / max(stage)` assumes the stages are independent, and
on a single interpreter driving a single NIC they are not. §13.53 recorded it for the
relay's prefetch (-6.2%, reverted), §13.60 for isolated reads measuring 87 MB/s against
30-51 in the loop, and §13.62 for a width-4 upload measuring 301 MB/s standalone and
~170 in situ. **The ceiling is a bound on the prize, never an estimate of it.**

Reverted rather than shipped off-by-default, on the §13.53 precedent: a thread, a bounded
queue, cross-thread error propagation and a deadlock hazard is real complexity in code
that runs at scale, and it buys nothing measurable. The measurement is kept
(`benchmark-results/gunzip-split-attempt-q*.json`) so nobody has to build it twice to find
that out.

**What this settles.** The decode is inflate-bound at ~11.4 s for 1.64 GB, and single-
threaded `zlib` is the floor. Nothing in the remaining ~36 s of a `--gunzip` run is worth
attacking with more concurrency: relay 5.4, verify 6.1, decode 21.4, other 2.8, and the
two big ones are already at their measured ceilings. Further gains would have to come from
doing less work -- a faster inflater, or not decompressing at all -- not from doing the
same work in more places at once.

### 13.65 The GIL claim in §13.64 was wrong, and multiprocessing is not the answer

§13.64 blamed the split's inflate slowdown on the upload threads holding the GIL. That was
an inference, not a measurement, and it is **wrong**. Tested directly
(`benchmark-results/gilprobe.py`): inflate the same 852 MB object three ways -- alone,
with 4 upload workers in THREADS, and with the identical load in 4 PROCESSES.

| arm | inflate | vs solo |
|---|---|---|
| solo | 12.21 s | 1.00x |
| threads | 12.74 s | **1.04x** |
| procs | 12.67 s | **1.04x** |

Threads and processes are indistinguishable, so the GIL is not the mechanism and
**multiprocessing would buy nothing** -- the question that prompted this. It would also
have to move 1.64 GB across a process boundary to do it.

The arithmetic should have warned me off the GIL claim before I wrote it: the upload side
is ~50 HTTP requests for 1.64 GB, and both `sendall` and OpenSSL's AES release the GIL, so
there was never five seconds of held-GIL work to find.

#### What it means for the split

Concurrent uploads cost inflate **4%**, not the 43% §13.64 measured. So that 43% was not
caused by overlap at all -- it came from inside the implementation. The likely cause is
allocation and cache pressure: `PipelinedFeeder` keeps several 15-32 MiB buffers alive in
a queue and copies every byte through `bytes(pending)` -> queue -> `_buffer +=` ->
`bytes(_buffer[:n])`, where the probe's inflate allocates each decoded chunk and frees it
immediately.

**So the split is not disproven, only my version of it.** If inflate held near 11.4 s
while write stayed at the 3.7 s the split achieved, the decode would be ~15 s against
today's 21.4 -- about 1.4x, and worth roughly 6 s of a 36 s run. A second attempt would
have to be genuinely zero-copy: hand the uploader a `memoryview` of the slice rather than
re-materialising it, and size the queue in bytes rather than chunks.

Recorded as open rather than attempted. The prize is smaller than anything already taken,
the first attempt cost real effort for nothing, and the cause above is still a hypothesis
-- it has not been measured, and this section exists precisely because I stated an
unmeasured mechanism as fact one round ago.

#### A note on the probe itself

The first run died with **HTTP 429**: the load generator rewrote one object name per
worker in a loop, and GCS rate-limits writes to a single object at roughly one per second.
The production uploader never does this -- every slice already gets its own name -- so it
was purely a probe bug. Worth recording anyway, because it is a limit that would bite hard
if the slice naming were ever made non-unique.

### 13.66 At 10x: total is linear, but the relay and verify degrade per byte

Every decode number to this point came from one 852 MB -> 1.64 GB object and a ~36 s run.
That is exactly the length `CLAUDE.md` warns about -- "when benchmarking this, measure past
96 GiB" exists because four runbook sections concluded there was a defect by comparing a
short measurement against a long one. Re-ran at **10x** (8520130000 -> 16444444500, a
50-member gzip built by concatenation), two reps.

| stage | 1x | 10x | scaling for 10x the bytes |
|---|---|---|---|
| total | 36.1 s | 361.1 s | **10.01x** |
| relay | 5.5 s | 78.0 s | **14.17x** |
| verify | 6.4 s | 75.8 s | **11.84x** |
| inflate | 11.4 s | 116.4 s | 10.19x |
| write | 9.5 s | 77.8 s | **8.20x** |
| read | 0.30 s | 0.34 s | 1.17x |

**The total is linear and the composition is not.** Three things follow.

**The relay degrades 29% per byte** -- 155 MB/s at 852 MB against 110 MB/s at 8.5 GB --
and goes from 15% of the run to 21%. The verify loses 15% the same way (133 -> 113 MB/s).
Both are sustained-throughput effects invisible in a 36 s run, which is structurally the
same trap as §6.5i's burst-versus-sustained disk: a short measurement reports the fast
part of a curve and nothing warns you it is a curve.

**The write improves 18% per byte** (8.20x), as per-slice session setup amortizes across
490 slices instead of 50.

**The read-ahead is a fixed cost**, 0.29-0.39 s at either size. It scales perfectly
because it never becomes the constraint.

#### What it means for the split

The prize is **~78 s of 361 s, 21.6%** -- proportionally a little under the 26% at 1x, but
**nine times larger in absolute terms**. "~6 s of a 36 s run" was a bad way to price it,
and that framing is what made the split look not worth retrying. At BAM scale it would be
minutes. The zero-copy retry proposed in §13.65 is worth more than §13.64 concluded.

#### Correctness at scale

491 components, both reps identical by crc32c, no leftover slices, `compose_tree` handling
490 sources through its tree path without incident. The multi-member decoder handled 50
concatenated members.

#### Still unmeasured

10x is 8.5 GB. A transport-gzipped BAM would be ~33x beyond that again, and the two
degrading stages are precisely the ones that would dominate there. Nothing here says the
relay's 110 MB/s is a floor rather than another point on a descending curve -- the honest
statement is that per-byte cost is **not** constant on this route, and any figure quoted
from a single size should say which size.

### 13.67 At a VCF compression ratio the decode is 91% of the run, and the split is worth 1.78x

§13.66 chased object size. The variable that actually matters is the **compression
ratio**, because relay and verify scale with the compressed size while inflate and write
scale with the decompressed size. Every measurement to this point used a fixture that
compresses **1.93:1**, which is nothing like the realistic input.

A transport-gzipped BAM -- what §13.66 worried about -- is close to impossible: BAM is
already BGZF, nobody re-encodes it, and a mislabelled one aborts in 0.3 s through the
keep-as-is branch. The real case is a large VCF, gnomAD-shaped.

Re-measured with VCF-like text, ratio **16.49:1**, 552331810 -> 9110203400, two reps that
came out identical to 0.05 s:

| | 1.93:1 (10x, §13.66) | **16.49:1 (VCF-like)** |
|---|---|---|
| total | 361.1 s | 126.0 s |
| relay | 78.0 s | 5.3 s |
| verify | 75.8 s | 3.1 s |
| **decode** | 194 s (54%) | **114.1 s (91%)** |
| inflate | 116.4 s | 58.8 s |
| write | 77.8 s | 55.1 s |
| ceiling / pipe2 | 1.81x / 1.77x | **1.94x / 1.94x** |

**The decode is 91% of the run**, and the relay and verify work -- the read-ahead, the
sliced upload, most of §13.61 through §13.63 -- barely registers, because at 16:1 there
are almost no compressed bytes to move. Those changes still stand on their own
measurements; they are simply not what decides a VCF localization.

**Inflate and write are within 6% of each other**, which is the best possible shape for a
two-stage split: ceiling 1.94x against a theoretical maximum of 2.0. A working split puts
the decode at ~58.5 s and the whole run at **~71 s, 1.78x**.

#### This reopens §13.64

The split was shelved after measuring ~4% on the 1.93:1 fixture, where write was a
smaller share and the ceiling was 1.81x. On the workload that actually reaches this code
the ceiling is 1.94x and the prize is 55 seconds of a 126 second run. Two other things
now point the same way: §13.65 measured concurrent uploads costing inflate only **4%**,
not the 43% the split reported, and processes matched threads exactly -- so the slowdown
was the implementation's copying, not contention.

The retry has to be genuinely zero-copy: hand the uploader a `memoryview` of the decoded
buffer rather than re-materialising it through `bytes(pending)` -> queue -> `_buffer +=`
-> `bytes(_buffer[:n])`, and bound the queue in bytes rather than chunks.

#### The lesson, which is the same one twice

§13.66 tested 10x the size and found the total linear. It was the wrong axis. Size was
easy to vary and the ratio was baked into a fixture I had chosen for an unrelated reason
-- it was built in §6.6b to produce enough granule-aligned PUTs, and 1.93:1 was a
side effect of making the data incompressible enough to control PUT count. That choice
then silently set the relay/decode balance for every decode measurement that followed.

### 13.68 The zero-copy retry: 1.28x, and the split was never the point

§13.67 reopened the split on the grounds that at a VCF ratio inflate and write are within
6% of each other and the overlap ceiling is 1.94x. Rebuilt, measured on the VCF fixture
(552331810 -> 9110203400, 16.49:1), all outputs identical by crc32c.

Two changes, measured separately:

**Zero-copy slicing.** The uploader now holds a list of decoded chunks and joins once per
slice, and `decompress_object` hands the decoder's output straight over instead of
round-tripping it through `bytes(pending)`. Three copies per byte became one -- at this
ratio, 27 GB of memcpy became 9 GB, on the inflater's own thread.

**Extra queue slots.** `queue_slices` lets assembled slices wait in the upload pool's
queue beyond the `width` actively uploading. No feeder thread: the pool already has a
queue, which is what §13.64's `PipelinedFeeder` was reinventing.

| configuration | total | inflate | write |
|---|---|---|---|
| baseline (§13.67) | 126.0 / 126.0 s | 58.4 / 59.2 | 55.1 / 55.1 |
| zero-copy, queue 0 | 102.2 / 102.9 s | 49.8 / 49.8 | 40.0 / 40.7 |
| zero-copy, queue 8 | 99.2 / 97.2 s | 49.7 / 49.3 | 38.0 / 36.8 |
| zero-copy, queue 16 | 98.7 s | 49.8 | 37.0 |
| queue 16, width 8 | 94.2 / 98.4 s | 48.9 / 50.1 | 33.8 / 36.5 |
| queue 16, width 16 | 100.2 s | 49.9 | 38.3 |

**126.0 -> ~98 s, 1.28x.** The zero-copy rewrite is almost all of it (1.23x on its own)
and it made *both* stages faster -- inflate 58.8 -> 49.8 (15%) and write 55.1 -> 40.3
(27%) -- which is the clearest possible confirmation that the copies, not contention,
were what §13.64 measured. Extra queue slots add a consistent 4% (the three groups do not
overlap). Width 8 is inside the noise (94.2 and 98.4 straddle width 4's 98.7-101.4) and
width 16 is worse; width stays 4.

Defaults: `queue_slices` 8 rather than 16, because memory is
`(width + queue_slices) * 32 MiB` -- 384 MiB against 640 MiB for the same 4%. That is
arithmetic on buffer sizes, not a measured RSS.

#### The ceiling was wrong again, and this time the reason is clean

1.94x predicted 71 s. We got 98 s, and inflate + write still sums to the decode total, so
there is *still* no overlap -- but it is no longer serialisation. Uploads run at
**~104 MB/s in situ against inflation's 183 MB/s**, so the queue drains slower than it
fills and the inflater is genuinely upload-bound. Adding slots cannot fix a rate mismatch,
and adding width does not raise the rate.

That is now four times the overlap ceiling has overestimated on this route (§13.53,
§13.60, §13.62, and here). The pattern is consistent enough to state as a rule: **on this
route the ceiling has never once predicted the outcome, and the stage that looks hideable
has always turned out to be rate-limited by something the arithmetic does not model.**

#### Two test defects found while doing it

A `replace()` without an assert silently no-opped, so `--decode-queue-slices` never
reached the downloader's parser and five runs died at argparse. I had written the
assert-every-anchor lesson into this file earlier in the same session.

`test_slicing_is_single_copy` asserted on a list appended from the upload threads, so it
captured *completion* order rather than slice order. It passed by luck at first and
failed once timing shifted. Rewritten to key by slice name.

### 13.69 Exit-code audit of the bucket route: an infinite requeue, a truncation, and my own race fix

Prompted by the compose race (§13.62): if one GCS call on the two-writer path turned a
lost race into do-not-retry, the others probably did too. They did, and the audit found
worse things than exit codes.

Two facts set the terms. **`GcsClient.request` never retries** -- one 5xx, 429 or reset
raises on the spot -- and **`main` catches `PermanentError` but not `TransientError`**, so
anything transient that escapes lands in the generic handler and exits 1. And **canine
requeues a localization exit 5 with no cap**: `orchestrator.py` increments
`.localization_failure_count` only to subtract it from the preemption limit. So the fix
could not be a blanket "requeue on any transient" in `main` -- that trades do-not-retry
for retry-forever. Each site had to be fixed on its own terms.

#### What was wrong, in order of how bad

1. **A deterministic infinite requeue.** A crash after the parts are deleted but before the
   done marker leaves a manifest claiming every chunk complete. The next attempt skips the
   relay, finds a part missing, and requeued -- manifest untouched -- so the attempt after
   saw the identical state, forever, with no cap to stop it. The compose race can produce
   the same state, since a loser's manifest writer can re-create the manifest after the
   winner unlinked it. Now the manifest is dropped before that requeue, which converges in
   one re-download.
2. **The decoder silently accepted truncated gzip.** `decompressobj` returns what it has
   and leaves `eof` false rather than raising; `GzipStreamDecoder` never checked. Measured:
   2615 of 3923 compressed bytes decoded to 1.33 MB of 2 MB with no error, where stdlib
   `gzip` raises `EOFError`. With an advertised digest the md5 on the compressed sidecar
   catches it first, which is why nothing noticed; without one, an upstream file cut short
   by an interrupted upload was published short and exited 0. The in-place route was never
   affected -- `gzip.open` raises. Now an `IntegrityError`, and a truncated *second*
   member (a cut-off bgzip file) is caught too.
3. **My compose-race fix accepted stale objects.** It took any object of the right size as
   the winner's. An object that predates the attempt is not a race winner's -- a writer
   finishing earlier would also have deleted the manifest -- so accepting it could report
   success over whatever an earlier localization left there. Now the object must have been
   created during this attempt (`timeCreated`, 5 s skew allowance).
4. **And, found while fixing 3, presence after one's own compose proves nothing.** Without
   `--gunzip` the compose target is the destination, so post-compose the object there may
   be this attempt's own unverified compose. Accepting it skipped verification; in the
   interleaving where a winner's integrity cleanup deletes the object, this attempt
   re-composes it from still-present parts, and the parts then vanish, it would have
   reported EXIT_OK on bytes already known to be corrupt. Post-compose lost races now
   requeue unless `--gunzip`, where the destination is only written after a passing verify.
5. **Integrity cleanup deleted parts before the corrupt object**, opening a window where a
   racing writer's compose fails, finds the fresh right-sized corrupt object, and exits 0
   for it. Reversed. ETag mismatch previously left the corrupt object readable at the
   destination; it is now cleaned up the same way.
6. **Verify and decode conflated "wrong bytes" with "gone".** A 404 on a sidecar removed by
   the winner was reported as corruption and failed the job. `IntegrityError` now carries
   the distinction: wrong bytes fail, a vanished object goes through the lost-race check.
7. **Every cleanup delete after success.** A single 503 on deleting part 17 of a composed,
   verified object exited 1 with no marker. Now best-effort with one retry, orphans logged.
   A done-marker `OSError` (it is on the gcsfuse mount) likewise no longer fails a finished
   object.
8. **A failing writer's `abandon()` deleted the shared slice names** out from under a live
   writer's compose -- one worker's transient error failing two decodes. It now deletes
   only on integrity failures; on a retry the next attempt rewrites the same names and
   removes them itself.

#### Tests

Fifteen new tests, each pinning one site. All ten decisions were mutation-checked, and the
first pass found one untested: flipping `abandon(delete=False)` at the call site survived
every test, because the only coverage was a unit test of `abandon` itself. Added an
end-to-end test; it now fails.

Two of my own test mistakes, recorded because they are the same mistake as §13.61 and
§13.68: a helper forwarded `check_md5=None`, which `options_for` stringified to
`--check-md5 None`, so both ETag tests ran the md5 path and one passed for the wrong
reason. And the fake GCS had no `timeCreated`, so it could not have caught item 3 -- it
now stamps every write, and an injected object without a stamp reads as stale by default,
so a test has to opt in to "this just landed".

#### Orphans: first "accepted", then fixed properly

The first draft of this section closed with orphaned slices as "known and accepted, bounded
by `(width + queue_slices) * 32 MiB`": in the race-won-by-another-writer path,
`decompress_object` has already given up before the caller learns the race was lost, so the
caller never sees the slice names. "Bounded" was the wrong test. It is bounded per event and
unbounded in aggregate -- and `LOCALIZATION.md` makes it worse: the bucket's only deletion
mechanism is a `daysSinceCustomTime` lifecycle rule, an object with no customTime is
"invisible to this rule and would never expire" (§3), and "nothing deletes the bucket" (§8).
**`parallel_download.py` set customTime on nothing it wrote.** Every orphan from any path --
this one, a cleanup delete that 503'd twice, parts left by a job that hit its retry limit
mid-relay, extra part indices after a layout change -- was stored and billed forever, removed
only if some later heartbeat's bucket-wide `objects update` happened to stamp it.

Two fixes:

- **Structural: every object this module creates now carries customTime** -- resumable
  uploads (parts, slices) in the session-initiation metadata, compose destinations (sidecar,
  tree intermediates, the final object) in `destination`, and the manifest, which had to move
  from `uploadType=media` (which cannot carry metadata) to `uploadType=multipart` (still one
  request, still atomic). The lifecycle rule that already exists is now the backstop for every
  orphan path, including ones not enumerated here. The final object also no longer depends
  solely on canine's post-unmount stamp.
- **Narrow: the slice names travel with the error** (`exc.k9pdl_orphans`), so the lost-race
  path deletes them immediately instead of leaving them for the lifecycle rule.

Verified against **real GCS**, not just the fake -- the fake only proves the field is sent:
customTime is present as stored on a resumable-uploaded part, a composed object, and a
multipart-written manifest, and the manifest body round-trips. Multipart costs **208.6 ms
median against 205.9 ms** for media on a 22 KB manifest (40 each, interleaved, p90 253 vs
246 ms) -- inside the noise, and manifest writes are batched on a 2 s linger anyway.

One deliberate behavior change, accepted: a job requeued more than
`localization_expiry_days` after its last write now finds its parts expired and re-downloads
(the stale-manifest handling above drops the manifest when parts are missing), where it used
to reuse them. That is the bound the lifecycle rule already puts on idle storage for
everything else in the bucket.

Mutation-checked: removing the stamp from any one of the three write paths fails a test, as
does dropping the orphan hand-off. The multipart boundary's collision check initially
survived, because a random boundary never collides; a test now forces the first draw to
equal a boundary present in the body.

### 13.70 The gzip probe missed every ordinary GCS object, and what GCS actually serves

Found while reviewing the fuse-localize merge. `_probe_http_metadata` decided "the body is
gzip" from `Content-Encoding` on a HEAD. **A HEAD for an ordinary gzip-encoded GCS object
returns no `Content-Encoding` at all**, whatever `Accept-Encoding` it sends -- only
`x-goog-stored-content-encoding: gzip`. §13.5 had measured a `Cache-Control: no-transform`
object, which does send it, and explicitly left the transcoding-eligible case unverified; the
code then keyed on `Content-Encoding` alone, and a stale comment above it still said to key on
the stored header. So for signed URLs the whole decode of §13.55-§13.68 **never ran in
production**: those objects reached the downloader without `--gunzip`, GCS decoded and ignored
the Range, and they landed via one unverified single-stream curl. The §6.6b measurements only
exercised it because the flags were passed by hand.

#### Measured on real signed URLs

Minted V4 through GCS's S3-compatible endpoint with an HMAC pair read from `~/.boto` at call
time (never written anywhere; URLs kept in the session scratchpad, 15-minute expiry). Objects
uploaded the ordinary way with an explicit Content-Type. Every cell repeated 3x; all
deterministic:

| object | HEAD sees | GET, no Accept-Encoding | GET, `Accept-Encoding: gzip` |
|---|---|---|---|
| `no-transform` | `Content-Encoding: gzip` | raw, ranges honoured | raw, ranges honoured |
| ordinary, non-gzip Content-Type | **stored header only** | decoded, Range ignored | **raw, ranges honoured (206)** |
| ordinary, `Content-Type: application/gzip` | stored header only | decoded, Range ignored | raw only **with a Range** (200, Range ignored); decoded without one |

Two corrections to earlier sections follow. §13.57's "`Accept-Encoding: gzip` is what makes
ranges work at all" holds for the ordinary case but not for `application/gzip`-typed objects,
which never range. And the first round of these measurements, on **anonymous reads of a
public object**, was not deterministic -- the same request changed answers minutes apart and
decoded bodies arrived despite `Accept-Encoding: gzip`. That is Google's edge-cache path
(`cache-control: public`, `age:` set). Signed URLs never touch it, but the handler's regex
matches any `storage.googleapis.com` URL, including unsigned public ones.

#### The fix

* The probe keys on **either** header, and for a stored-gzip object takes its size from
  `x-goog-stored-content-length` (a Content-Length on a transcoded response would describe the
  decoded bytes). The stored digest then covers exactly what is fetched.
* Every fetch asks for the stored bytes: `_pdl_command` already sent `Accept-Encoding: gzip`;
  the legacy curl now does too.
* A **fresh** legacy fetch also sends `Range: bytes=0-`, the only way to get raw bytes for an
  `application/gzip`-typed object -- which is what a mislabelled `.gz` upload usually is. Never
  on a resume: a custom Range header replaces the one `curl -C -` computes (verified), so it
  would re-request from zero and append, duplicating every byte on disk.
* **Detect, don't predict.** After the fetch, the pipeline checks what arrived. Stored bytes
  are exactly the stored length; anything else was decoded server-side, cannot be verified
  (the digest covers bytes that never arrived), and is placed with a loud warning -- where
  the old pipeline deleted it as "corrupted". Size, not gzip magic: a doubly compressed object
  decoded once still starts `1f 8b`.
* `curl -C -` exit 33 (server ignores Range, so a surviving partial cannot resume) now
  restarts from zero. The partial survives by design, so every retry used to fail identically.

#### Acceptance, on the real signed URLs

The handler's emitted command, both paths (downloader and legacy), six object shapes:

| | before | after |
|---|---|---|
| ordinary / parallel-sized | correct, **unverified**, single stream | correct, **verified**, parallel |
| `no-transform` | correct, verified | correct, verified |
| `application/gzip`-typed | correct, **unverified** | correct, **verified** |
| mislabelled `.vcf.gz` (singly compressed, both types) | **wrong** -- plain text under a `.gz` name | correct -- stored bytes kept |

12/12 correct after, against 10/12 correct and 8 of those unverified before.

#### Known limitation, by decision

A mislabelled, singly compressed file with **no gzip-family name** -- a DRS object named by
UUID, say -- is still decoded into plain bytes. Measured: `0f3c9a2e-sample`, typed
`application/gzip`, came out decoded on both paths. Using the declared Content-Type as a
second "what must the decoded file be" signal would catch it; that was built and deliberately
backed out as going too far for now. It is no worse than before this work.

Still open: `HandleDRSURI` and `HandleGDCHTTPURL` never probe at all, so a DRS input
resolving to a gzip-encoded GCS object gets none of this. (Resolved in §13.72: not needed, since drshub's md5 would catch it.)

### 13.71 Gzip-encoded gs:// objects go through the downloader, not a rewrite

The fuse-localize merge brought a fix for gzip-encoded `gs://` sources on the bucket route.
A server-side `gcloud storage cp` copies the stored bytes and metadata verbatim, and gcsfuse
does not decode, so a reference `.dict` reached GATK as `1f 8b`. That fix was a runtime branch
in the `server_side` upload: `objects describe`, then `gcloud storage cat | gunzip > $(mktemp)`,
then a re-upload. It had four problems:

- It staged the whole decompressed object on the boot disk. A gnomAD-scale VCF runs that out
  of space.
- It had no keep-as-is check, so it decoded mislabelled `.vcf.gz` files into plain text under
  a `.gz` name (§13.70).
- It failed open. If `describe` failed, the branch fell through to the plain copy, which is
  the original bug.
- It depended on `gcloud storage cat` never transcoding, and any hiccup exited 1 rather than
  requeueing.

Decoding needs the bytes on a node, so these objects already paid for a node transfer. The
change gives them the downloader's version of that transfer.

#### The routing

* **Classified at plan time, at no cost.** `HandleGSURL._get_size` already fetches the blob
  to size the disk (`_blob_localized_size` reads `content_encoding` from it). It now keeps the
  encoding, the md5 and the stored size, and `transport_gzip` reads them back without a
  second request. A size that was cached without the metadata triggers a refetch rather than
  reading as "not encoded", because that reading would mean the plain copy, and the bug.
* **Routed to `mount`.** An encoded single object is planned onto the read-write gcsfuse
  mount, and the mount loop calls `HandleGSURL.downloader_command`. It does not call
  `localization_command`, whose sliced `gcloud storage cp` writes out of order. Every
  unencoded object keeps the free server-side copy, and the `server_side` branch is a plain
  copy again.
* **Read over the JSON API**, through a new `--gs-source` source (`GcsObjectSource`). There is
  no signed URL on this path, and a bearer token pasted into a header would expire mid-transfer
  on a large file, so the source reads through `GcsClient`, which refreshes its token.
  Requester-pays sources get `userProject` (`--user-project`), and only when the bucket asks
  for it.
* **Verified whenever the object has an md5, whatever `check_hash` says.** `gcloud storage
  cp`, the copy this replaces, always validates, so gating verification on an off-by-default
  flag would have been a silent regression. The downloader is given the **stored** length.
  `self.size` is the decoded estimate the disk is sized from, and a plan built over that length
  never matches what the server sends. The first draft had exactly that bug, and the tests
  caught it.
* **Keep-as-is is unchanged.** A name promising gzip content whose decoded bytes are not gzip
  keeps its stored bytes.
* **The fallback is the handler's `gcloud storage cp`**, used when the downloader is not
  found. It also decodes. It leaves `.gcloud_tracker_dir/` and `.gcloud_manifest` beside the
  object on the mount, and therefore in the bucket. That is untidy but harmless, and only on
  the fallback.

#### The JSON-API measurement

All earlier measurements (§13.70) used the XML API that signed URLs take. `GcsClient` uses
`/storage/v1/b/B/o/N?alt=media`, so the same objects were re-measured there. Every cell was
run three times, and all three agreed every time. Stored/decoded sizes are 98104/528901 bytes.
`urllib` sends `Accept-Encoding: identity` by default, so that column is what `GcsClient`
sent before this change.

| object | AE none / identity, no Range | AE gzip, no Range | AE none / identity, Range 0-99 | AE gzip, Range 0-99 |
|---|---|---|---|---|
| ordinary (text type) | 200 decoded | 200 raw | 200 decoded, Range ignored | **206 raw** |
| `no-transform` | 200 raw | 200 raw | 206 raw | 206 raw |
| `application/gzip`-typed | 200 decoded | **200 decoded** | 200 decoded | **200 raw, whole object** |
| `application/octet-stream` | 200 decoded | 200 raw | 200 decoded | 206 raw |

This is the same matrix as the XML API. `GcsObjectSource` therefore always sends
`Accept-Encoding: gzip` and a Range (`bytes=start-`, even for the whole object), and decides
what arrived from the response rather than predicting it:

* **206** is checked against the `Content-Range` total.
* **200 carrying the stored bytes** (`Content-Encoding: gzip`, and a length that is absent or
  equal to the stored size) means ranges are not honored for this object. The source switches
  to `whole_object_only`, the plan collapses to a single chunk, and a resume skips the
  already-held prefix of a full-object stream (`_SkippingStream`). This is the
  `application/gzip` case, and it is logged as `only whole`.
* **Anything else was decoded server-side** and raises `RangeNotSupported`, which falls back
  to the legacy command.

#### Acceptance, real GCS, the downloader's `--gs-source`

| object | chunks | outcome |
|---|---|---|
| ordinary `.dict` | 1 | decoded 98104 → 528901, verified |
| `no-transform` `.dict` | 1 | decoded, verified |
| `application/gzip` `.dict` | 1 (`only whole`) | decoded, verified |
| ordinary `variants.vcf` | **6, parallel** | decoded 5730289 → 37033840, verified |
| mislabelled `.vcf.gz` | 1 | kept as-is, verified |
| mislabelled `application/gzip` `.vcf.gz` | 1 (`only whole`) | kept as-is, verified |

All six were correct and verified. The emitted `downloader_command` has not yet been run end to
end on a node. The bucket-compose half of it is covered by the fake-GCS test `into a bucket
through compose`, and by the §13.63 node runs of that route.

#### Still open

* **Directories** are not classified, and still take the plain `cp -r`. Classifying one means
  inspecting every object under the prefix, and no gzip-encoded directory tree has turned up.
* **DRS/GDC** (§13.70): no probe needed; see §13.72.

### 13.72 DRS and GDC need no encoding probe, but the GDC API was never parallel

Picked up from §13.70's open item: `HandleDRSURI` and `HandleGDCHTTPURL` never probe for a
transport encoding. The conclusion is that they don't need to. Investigating it turned up an
unrelated defect that does matter.

#### Measured

**A real GDC-backed DRS URI**, a 348.7 GB TCGA WGS BAM (controlled access). drshub resolves
its metadata (size, md5, `bondProvider: dcf_fence`, no `gsUri`, `fileName` = the bare UUID).
The access step goes to CRDC DCF's `access/s3` endpoint, so the storage is AWS S3 and the
`accessUrl` is a presigned URL. At first it came back 401 (no read-storage on
`phs000178.c1`), even though the Terra `dcf-fence` link was valid. That turned out to be a
transient Terra-side problem, and a retry later the same day returned the URL.

**The presigned S3 URL** (`tcga-2-controlled`):

| request | response |
|---|---|
| HEAD | **403**: the URL is signed for GET only |
| GET, `Range: bytes=0-0`, AE identity or gzip | 206, `Content-Range: bytes 0-0/348693812393`, **no Content-Encoding**, multipart ETag (`…-5196`, so not an md5), SSE AES256 |

The downloader's real `probe_range`, and a 16-byte read at 1 GiB, both succeed, so the DRS
route runs in parallel. The real `HandleDRSURI` plan step resolves size and md5, and recovers
the real filename from the `accessUrl` path (drshub's `fileName` is the bare UUID). It emits the
downloader with `--url "$signed_url"`, `--url-refresh-cmd`, the drshub md5 and 16 connections;
the host-side URL never appears in the script.

**The GDC API, with a user token**, on the same object:

| request | response |
|---|---|
| HEAD | **400 BAD REQUEST** |
| GET, `Range: bytes=0-0`, AE identity or gzip | 206, `Content-Range: 0-0/348693812393` -- **no `bytes ` unit** -- `Content-MD5` in hex (equal to the DRS md5), no Content-Encoding |
| GET, no Range (the handler's fallback request) | 200, full Content-Length, Content-Disposition carrying the real filename |

The GDC API streams the bytes itself, with no redirect to storage.

**GCS signed URLs, a GET aborted at the first body byte** (`-o /dev/full`) against a HEAD,
for all six shapes of §13.70. The stored-encoding signals are identical on both, for every
shape. That includes the decoded 200 an identity request gets, which carries no
`Content-Encoding` but does carry `x-goog-stored-content-encoding` and
`x-goog-stored-content-length`. `pdl_server`'s decoded responses now do the same.

#### Decided: no encoding probe for DRS or GDC

drshub supplies the md5, and in practice it matches the localized bytes. A transport-encoded
object could not do that. The md5 covers the stored gzip bytes, and a download that doesn't
ask for them gets decoded ones. So an encoded DRS object would **fail verification loudly**
(`deleting corrupted file`, exit 1); it would not be silently wrong. The GDC API sent no
encoding on any request. A probe was built for this (an aborted GET, since a HEAD fails on
both the GDC API and a GET-presigned S3 URL), and it worked live against GCS and the GDC API.
It was backed out: it costs one request per DRS input and a fail-open path, and it guards
against a case that verification already catches. If an encoded DRS object ever turns up,
the failure will say so, and the design above is the starting point.

#### Fixed: the downloader could not parallelize the GDC API

`HttpSource` matched `Content-Range` against `^bytes 0-0/…` in the probe and
`^bytes (\d+)-(\d+)/` per chunk. Run against the real endpoint, the probe raised
`RangeNotSupported: unexpected Content-Range '0-0/348693812393'`, and a mid-object chunk raised
`TransientError`. The server had honored the range exactly; only the unit was missing. Nothing
failed, which is why it went unnoticed: every GDC-API download silently took the single-stream
fallback. The runbook's GDC-API sweep had never been run. The GDC numbers there (13.85×) come
from the GDC S3 endpoint through presigned URLs, which sends the unit.

The fix is `parse_content_range`, one parser for every server-sent `Content-Range`, with the
unit optional (`bytes `, `bytes=`, `bytes:` or none). It is lenient about the unit only; the
position must still match exactly. Nothing had tested that before, and mutating either
position check out survived the whole downloader suite. A fake-server knob (`shift_range`, in
both `pdl_server` and `pdl_gcs`) now serves a shifted range with a consistent header, and every
source must refuse it. `GcsObjectSource.probe_range` had checked only the total on a 206, never
the position; it now checks both, like `HttpSource`. Against the real GDC API afterwards, the
probe passes, a 16-byte mid-object range reads correctly, and a wrong expected size is still
rejected. A parallel throughput run is left for a node: the data is controlled access, and a
laptop is not the place for it.

### 13.73 Throughput from the GDC API and from DRS, measured

§13.72 fixed the GDC API's unit-less `Content-Range`, and checked the DRS route only at plan
time. Both sources were then swept on a node. The object is the same 324.75 GiB TCGA WGS BAM,
the node is n1-standard-8 in us-east1-b, and each run fetches a 12 GiB prefix to a tmpfs, so
no disk is in the path (the runbook's §6.1 method). A throughput-only run, so no md5.

| connections | GDC API | streams | DRS → AWS S3 (presigned) | streams |
|---|---|---|---|---|
| 1 (curl) | 16.01 MiB/s | — | 67.70 MiB/s | — |
| 4 | 38.23 | 2.75 of 4 | 206.21 | 3.24 of 4 |
| 8 | 72.72 | 5.27 of 8 | 415.98 | 6.45 of 8 |
| 12 | 111.81 | 8.03 of 12 | 467.40 | 10.12 of 12 |
| 16 | **145.04** | 11.40 of 16 | **490.73** | 14.06 of 16 |
| speedup | **9.24×**, no knee | | **7.43×**, knee at 12 | |

`wire` was 1.00-1.05× on every row, so nothing was fetched twice.

Time to first byte, from eight 1-byte ranged GETs at spread offsets:

| | connect | TLS done | first byte |
|---|---|---|---|
| GDC API | 0.027 s | 0.14 s | **1.1-1.7 s** |
| S3 presigned | 0.024 s | 0.09 s | 0.27-0.38 s |

**Why the GDC API is known as the slow source.** Its per-stream rate is not the cause: 16 MiB/s
is what GDC's own S3 endpoint gave per stream as well (the runbook's 16.42 MiB/s).

* **The per-request wait.** The API spends 1.2-1.5 s on each request before sending anything,
  presumably authorization. At ~14 MiB/s a 64 MiB chunk streams in ~4.6 s, so ~23% of every
  request is dead time. That predicts an effective concurrency of ~0.77; the sweep measured
  0.71. S3's ~0.2 s wait costs it far less (0.88).
* **Every download used to be a single stream.** Before §13.72 the downloader fell back to one
  stream for every GDC API download. That meant 16 MiB/s against AWS S3's 68 MiB/s, and
  against sources that were getting their parallel speedup.

Fixed, the GDC API reaches 145 MiB/s, and was still climbing at `MAX_CONNECTIONS`.

**Larger chunks would help it most, not tested.** At 256 MiB the dead time would fall to ~7%.
`download_min_chunk` is global and feeds `plan_id`, so this is not a per-source switch. It
would need measuring before any default changed.

**DRS through DCF is AWS S3, and it is fast.** 67.7 MiB/s on a single stream already exceeds
the pd-standard localization disk's sustained 44-88 MiB/s. For `LocalizeToDisk` from this
source, then, parallelism buys little. For fast destinations (the bucket route, NFS) it rises
to ~490 MiB/s, flattening past 12 connections at an aggregate ceiling not investigated here.

**Credentials.** The GDC token and the presigned URL lived on the node only as 0600 files, read
at run time and deleted afterwards. Every result file was checked for them before leaving the
node.

### 13.74 Legacy curl saved HTTP error bodies as the download

Reported from production: GDC API error output sometimes ended up *inside* the downloaded
file. Reproduced in the worker image (curl 7.81) against the real GDC API with a bogus token.
`curl -C - -o f URL` exits **0** and `f` contains `{"message":"Not authorized to download:
…"}`, 80 bytes. The same happens with a GCS 403 (757 bytes of XML), and two 500s left an empty
file, also exit 0. None of the eight handler legacy curls (GDC, DRS, signed-GCS, other-URL,
and their streaming variants) passed `--fail`. The downloader's own synthesized fallback did,
with a comment explaining why. But a handler's `--legacy-cmd` takes precedence over it, so the
unprotected command is what ran. That includes every GDC API download before §13.72, all of
which fell back.

What an error body did next:

* **No hash check:** it was accepted as the file.
* **Hash check:** md5 mismatch; the file was deleted, exit 1.
* **The compressed pipeline:** the wrong size read as "decoded by the server", and the error
  body was moved into place with a warning.
* **Stream handlers:** the error text was fed into the FIFO as data.

A partial file was never at risk, because `-C -` refuses to append a non-206.

**Fix: `CURL_FETCH = "curl --fail --retry 5"`**, the start of every curl that writes a localized
file, and the same flags on the downloader's fallback. Measured on curl 7.81, against the fake
server:

| case | before | after |
|---|---|---|
| fresh fetch, 403 | exit 0, error body saved as the file | exit 22/56, no file |
| fresh fetch, two 500s | exit 0, empty file | retried, exit 0, byte-identical |
| complete file, re-run (`-C -` → 416) | exit 0 | exit 0, unchanged |
| partial file, 403, then success | — | partial untouched, then resumed to byte-identical |
| connection dropped mid-body | exit 18, valid partial | same: plain `--retry` doesn't retry a drop |

The last row explains why it is **not `--retry-all-errors`**. With `-C -`, curl 7.81 re-requests
a retry from the *original* offset and rewinds the file to it. That is safe (no duplicated
bytes), but it makes no progress past a drop, and it would spend the retries on a 403. A drop
exits nonzero with a valid partial, which the next attempt resumes.

Tests run each handler's real emitted legacy command with bash against the fake server
(`TestLegacyCurlRefusesErrorBodies`). Removing `--fail`, removing `--retry`, and reverting to
the old command are each caught; the old command fails 7 of them.

#### A transient drop fails the attempt, and wolF's retry resumes it

A mid-body drop (exit 18/56; the GDC API resets connections routinely) could have been sent
to canine as exit 5, requeue-and-resume. It deliberately is not. canine requeues 5 **with no
cap**, so it is only safe behind a forward-progress gate, and a gate only mitigates the loop
risk. An ordinary failure is already the right retry: wolF retries a failed task (`retry`, 3
by default, `retry_delay` 1 minute), and the retry resumes from the partial file with `-C -`.
That is bounded and loses nothing; a test runs the same legacy command over its own partial
until it finishes byte-identical. The downloader's own chunk path still exits 5 on transient
errors after progress, as before.

What does matter is that a failure *reaches* canine as a failure. Wiring this up turned up
three ways it did not, all fixed by `fetch_or_exit`, which wraps every legacy fetch and the
downloader's fallback:

* **`set -e` does not apply inside an `&&` list.** In the compressed pipeline
  (`FETCH && verify && decode`), a failed fetch *or a failed gunzip* let localization.sh carry
  on to the next input. The task then ran without its input (measured: `set -e; false &&
  decode; echo next` prints `next`). Every failure now exits explicitly, and the compressed
  pipeline ends `|| exit 1`.
* **curl's exit codes collide with canine's.** curl exits 5 for "couldn't resolve proxy" and 15
  for an FTP host failure; canine reads 5 as requeue (uncapped) and 15 as **skip the job**,
  i.e. done. A raw pass-through could loop forever, or report a task that never ran as
  finished. Both now exit 1.
* **The exit-33 handler discarded the real code.** `{ rc=$?; [ $rc -eq 33 ] && … }` turned
  every other failure into a plain 1; it now passes it through.

#### Found by the tests: a truncated compressed body was placed as the file

For a compressed object without Content-Length (a body delimited by the connection closing),
a dropped connection looks complete to curl: exit 0, and a short body. The detect-don't-
predict check (§13.70) saw a size mismatch, called it "decoded by the server", and moved the
**truncated gzip stream into place** with a warning. That is the same failure as an error body
saved as the file.

Size cannot separate the two cases: a server-decoded mislabelled `.gz` is *also* shorter
than the stored bytes. Content can. A truncated stored body still starts `1f 8b` and fails
`gzip -t`; a decoded one is either not gzip at all or a complete inner stream, which passes.
A truncated stream now fails, and the sidecar is kept for the retry to resume. On real GCS
this needs the response to lack Content-Length, which the 206s the pipeline requests carry
(measured). So it is a guard for other servers, not a known GCS failure.

All six pieces are mutation-checked against the behavioral tests
(`TestLegacyFailuresExitExplicitly`).

### 13.75 Chunk size: measured on the GDC API, and a size-scaled rule

12 GiB prefix of the same 324.75 GiB BAM from the GDC API to tmpfs, n1-standard-8 in us-east1-b.
16 connections ran twice at each size:

| chunk | run 1 | run 2 | mean | vs 64 MiB | streams (of 16) |
|---|---|---|---|---|---|
| 64 MiB | 156.32 | 156.30 | 156.3 MiB/s | — | 11.2 |
| 128 MiB | 181.80 | 191.70 | 186.8 | +19% | 12.3-13.1 |
| 256 MiB | 201.15 | 198.64 | 199.9 | **+28%** | 13.2-13.4 |
| 512 MiB | 167.53 | 170.98 | 169.3 | +8% | 11.0-11.4 |

At 8 connections: 70.20 MiB/s at 64 MiB, 110.03 at 256 MiB (**+57%**), with 7.21 of 8 streams
active against 5.10. One first attempt at that row failed on a GDC connection reset (the
benchmark's size probe and then the curl baseline both got `Connection reset by peer`); the
re-run was clean. 512 MiB dips only because of this test's size: 12 GiB is 24 chunks, or 1.5
rounds of 16, so the last round leaves most connections idle.

**A queue model fits these to 4.2% RMS across five configurations.** Each chunk costs a
per-request overhead t₀ plus chunk/b of streaming; k workers pull from a queue, with per-stream
rate jitter of ±30%. The fit is b = 15 MiB/s and **t₀ ≈ 2.6 s**, higher than the 1.1-1.7 s an
idle 1-byte probe showed, so the GDC API's per-request wait grows under load. Extrapolated to
the full 325 GiB object: 149 MiB/s at 64 MiB, 208 at 256 MiB, 218 at 1 GiB (**+46%**). This is
untested at full size.

**No fixed size is right, because the best one grows with the object.** In the model the GDC
API's best chunk is 128 MiB at 2 GiB, 256 MiB at 8 GiB, and 1 GiB at 325 GiB; today's 64 MiB
reaches 67-88% of the best. An S3-like source (t₀ 0.35 s, ~35 MiB/s per stream, the ~500 MiB/s
aggregate ceiling measured in §13.73) is far less sensitive: 64 MiB is within 91%. Candidate
rules, each scored by its worst case across object sizes from 1 to 325 GiB:

| rule | GDC worst | S3 worst |
|---|---|---|
| fixed 64 MiB (today) | 67% | 91% |
| size/16, clamped to [64 MiB, 1 GiB] | 96% | 88% (one round of big chunks; stragglers) |
| **size/32, clamped to [64 MiB, 1 GiB]** | **88%** | **91%** |
| size/64 | 82% | 93% |
| size/128 / size/256 | 81% / 79% | 97% / 95% |

`size/32` is nearly gcloud's *upload* rule (`parallel_composite_upload_component_size` 50M,
at most 32 components). The 1 GiB cap is the difference that matters: a large object stays a
queue of many chunks (325 for this BAM, rather than 32 of 10 GiB), and the queue is what
protects against stragglers. gcloud's *download* rule is the other shape: a 5 MiB target, but
at most 8 or 16 slices, so one slice per thread, which the model penalizes at large sizes (176
against 218 MiB/s at 325 GiB).

**Pros of a size-scaled chunk:**

* **Fewer per-request waits,** the dominant cost on the GDC API. S3 and GCS gain a little, and
  lose nothing measurable.
* **Nothing lost on preemption or retry.** Both routes resume inside a chunk: the in-place route
  byte-exactly from the frontier, the bucket route from the upload session's 256 KiB-granular
  Range.
* **Memory unchanged.** The read and upload buffers are 8 MiB per connection whatever the chunk.
* **Fewer objects on the bucket route:** fewer parts, compose calls and deletes.
* **Fewer processes on the S3 API route,** one `aws` process per chunk.
* **The plan stays stable.** Chunk size depends on object size alone, so `plan_id` stays stable
  across requeues onto differently-configured nodes, which is the hard requirement in
  `plan_chunks`.

**Cons, and what bounds them:**

* **Fewer chunks means less parallelism on small objects, and longer tails.** The floor keeps
  every object under 2 GiB exactly as today, and the cap bounds any single straggler at 1 GiB.
* **Rollout restarts large downloads once.** `plan_id` changes for objects over 2 GiB, so a
  partial download in flight across the upgrade restarts once.
* **Little gain on the common destination.** `LocalizeToDisk` to pd-standard (44-88 MiB/s) is
  disk-bound either way. The gain is on fast destinations (the bucket route, NFS), and in
  reaching the disk's ceiling with fewer connections.
* **S3 multipart objects are unaffected in principle.** Chunks still snap up to a whole number
  of parts.
* **The model has not been checked past 12 GiB,** and ±30% jitter is an assumption about
  stragglers. A full-size run on a fast destination should come before a default change.

### 13.76 Size-scaled chunks at full size: +38.5% on the GDC API

§13.75's rule (size/32, clamped to [64 MiB, 1 GiB], committed as `7ad0eb8`) was checked on the
whole 324.75 GiB BAM from the GDC API. Both runs used 16 connections on n1-standard-8 in
us-east1-b, back to back, and both were md5-verified against the drshub digest. The baseline
pins the old layout with `--max-chunk 67108864`.

| | fixed 64 MiB (old) | size-scaled, 1 GiB (new) |
|---|---|---|
| chunks | 5,196 | 325 |
| download | 1,915.7 s, **173.6 MiB/s** | 1,382.7 s, **240.5 MiB/s** |
| streams | 12.00 of 16 | 15.15 of 16 |
| md5 | ok (857 s read-back) | ok (835 s) |
| wire | 1.01× | 1.01× |
| GDC resets retried | 36 | 12 |
| manifest commits | 404 s, 720 batches (21% of wall) | 136 s, 122 batches (10%) |
| model (§13.75) | 149 | 218 |

**+38.5% throughput, and 8.9 minutes less download time on this object.** The gain is the one
§13.73 attributed to per-request waits. With 16× fewer requests, the average number of
streams actually receiving bytes rose from 12.0 to 15.15 of 16.

**The model was low on both runs, by 16% and 10%,** and predicted a slightly larger gain
(+46%) than was measured. It got the direction and most of the size of the gain from 12 GiB
prefixes alone. That supported the change; it did not substitute for this run.

**Resets fell with the request count:** 36 over 5,196 chunks against 12 over 325. So they
track connections more than bytes. Each one is retried from the chunk's frontier, so none of
them cost a re-fetch.

**The destination was deliberately not production's.** Two local NVMe SSDs striped (737 GB,
758-819 MB/s by `dd`) were used so the disk could not cap either run: peak writes of 822
and 500 MiB/s show it never did. Production does not use local SSD. There, this gain appears
on fast destinations (the bucket route, NFS). `LocalizeToDisk` to pd-standard is still bound
by its 44-88 MiB/s, where the rule changes little beyond needing fewer connections to reach
that ceiling.

The BAM existed only on the node's local SSDs and was destroyed with the node. The GDC token
was a 0600 file, shredded by the run script's exit trap, and every result file was scanned for
it before leaving the node.

### 13.77 The bucket route at full size, and a token that expired mid-run

The same 324.75 GiB BAM, now from DRS (DCF → AWS S3, presigned) into a gcsfuse-mounted
localization bucket: the bucket-compose route. 16 connections, n1-standard-8 in us-east1-b.
The bucket was private (public access prevention enforced), and the run script emptied it
between runs and on exit. DRS was chosen over the GDC API because §13.76 had already
settled the GDC API's per-request cost, and S3 is fast enough to load the route itself.

| | fixed 64 MiB (5,196 parts) | size-scaled 1 GiB (325 parts) |
|---|---|---|
| relay | 1,348.8 s, **246.5 MiB/s** | 845.6 s, **393.3 MiB/s**; 813.0 s (409.0) on the first attempt |
| compose | 55.6 s | **16.2 s** |
| streams | 13.44 of 16 | 15.34 of 16 |
| io | 14% read / 86% write | 25% read / 75% write |
| md5 | not run | **ok**, 2,835.5 s read-back |

**+60-66% on the relay, and compose 3.4× faster.** The mechanism differs from the GDC API's.
The route is upload-bound (86% write), and every part is its own resumable upload session,
with a start POST and a finalizing request. 325 sessions spend far less of the upload on
per-part overhead than 5,196.

#### The md5 read-back is now the bottleneck: 117 MiB/s

47 minutes to re-read what took 14 to relay, the same ~111 MiB/s measured in §13.54. A
composite object has no md5, so a whole-file md5 source (DRS, the GDC API) can only be
verified by reading it back. The reader count, VERIFY_READ_WORKERS = 2, is tuned for
pd-standard, not for GCS ranged reads. **Worth measuring more readers here**; not attempted.

#### Found: a GCS token trusted for 55 minutes, dead after ~23 (fixed in `cfac695`)

The first size-scaled attempt relayed and composed, then failed its read-back with `HTTP 401:
Invalid Credentials`. `_fetch_token` takes `gcloud … print-access-token`, which returns
gcloud's *cached* token. That token may be minutes from expiry, and the command reports no
lifetime. The code assumed 3600 s and cached it for 55 minutes. Nothing refreshed on a 401,
so every later request reused the dead token until the read-back gave up; with the
requeue-then-retry path that would have been a lost 9-minute read-back. A production relay
lasting hours takes the same path.

The fix has two parts:

* A 401 drops the cached token and resends the request once. It drops the token only if it
  is still the one rejected, so 16 workers do not throw away each other's refresh. This
  applies in `GcsClient.request` and `GcsObjectSource._open`; a second 401 is an error.
* Tokens from a subprocess are cached for 3 minutes, inside gcloud's own ~3m45s refresh
  margin.

Every other bucket test stubbed `token()` out, which is why none could see this. The new
tests run the real cache against a fake GCS that rejects expired tokens; the fix's six
mutations are all caught. The re-run above is on the fix, and its read-back completed.

#### `wire` counts the read-back

The benchmark counts NIC bytes for the whole downloader process. On this route that includes
the md5 read-back from GCS, so a verified run reads **2.02×** and is flagged `DUPLICATE
FETCHING`. That is a false alarm: nothing was fetched from the source twice. The failed
attempt's 1.21× was the ~68 GiB it read back (~9 minutes at ~128 MiB/s) before the 401. Its
logged "642 MB/s" was the full size divided by elapsed time, not a rate. **The ratio should
exclude the verify phase on this route.** That is a benchmark reporting fix, not done here.

#### Also recorded

An attempt from the GDC API was stopped a minute in, in favor of DRS. Its trap emptied the
bucket (leaving 6 in-flight parts, removed immediately after) and shredded the token. The
node suspended itself after each run through a watcher; its service account was given
`compute.instanceAdmin.v1` on that instance alone, which went away with the instance.

### 13.78 The bucket route's md5 read-back: bigger blocks, not more readers

§13.77 left the md5 read-back as the bucket route's bottleneck: 47 minutes at 117 MiB/s for
the 324.75 GiB BAM, against a 14-minute relay. `verify_bucket_object` reads through
`ranged_blocks`: up to `decode_readahead` (default 4) ranged GETs of READ_BUFFER (8 MiB) in
flight, consumed in order by one md5.

**Method.** n1-standard-8 in us-east1-b, private bucket in us-east1, the worker image and the
committed downloader (`cfac695`). Unique AES-CTR data was uploaded as 1 GiB parts and composed
into two composites (128 GiB and 64 GiB). The parts were uploaded as `gcloud` parallel
composites, so the objects have 2,816 and 1,408 components rather than one per part. Every
trial read its own never-read 8 GiB window, because a just-composed object's cold read is
what production pays. The loop is `ranged_blocks`' own: `GcsClient.download_range` feeding
one md5 in order. On this node md5 alone runs at **511 MiB/s**, so that is the ceiling.

| depth × block | MiB/s |
|---|---|
| **4 × 8 MiB (today)** | 118.2, 125.8 (production measured 117) |
| 8 × 8 | 169.1, 179.9 |
| 16 × 8 | 141.9, 154.3 |
| 32 × 8 | 153.0, 150.4 |
| 2 × 32 | 150.2 |
| **4 × 32** | 211.6, 229.7, 232.4 |
| 6 × 32 | 207.6 |
| 8 × 32 | 151.6, 172.3 |
| 16 × 32 | 165.4, 170.7 |
| 32 × 32 | 171.2, 169.1 |
| 2 × 64 | 168.5 |
| **4 × 64** | **275.6, 234.4** |
| 6 × 64 | 227.3 |
| **4 × 128** | **298.7** |

The first round's two repetitions ran the grid in opposite orders, and each cell agreed
within ~10%.

**More readers do not help: depth 4 is best at every block size, and beyond it the rate
falls to a ~150-175 MiB/s plateau.** That is the same non-monotonic shape as the decode's
read-ahead sweep (§13.54). **Bigger blocks do help**, and were still helping at 128 MiB.

**The cause is a fixed cost per request, not a single CPU core.** The harness used a median
2.04 cores and at most 2.8. `urllib` sends `Connection: close`, so every ranged GET pays a new
TLS handshake and a fresh TCP ramp-up. The three depth-4 points fit about **0.15 s per
request plus ~75 MiB/s per stream**, which caps depth 4 near 300 MiB/s. A deeper queue adds
more ramp-ups competing for the same path, not more throughput.

**What it is worth, for the 324.75 GiB read-back:**

| | read-back | memory in flight |
|---|---|---|
| 4 × 8 MiB (today) | 47 min | 32 MiB |
| 4 × 64 MiB | ~22 min (2.1×) | 256 MiB |
| 4 × 128 MiB | ~19 min (2.4×, one rep) | 512 MiB |

**Recommended:** read back in 64 MiB blocks at depth 4, which only needs a different `block`
in `verify_bucket_object`. The decode path's 8 MiB block should stay: its consumer is a gzip
inflater, and its sweep was measured separately. 128 MiB is 15% better again, but rests on one
repetition and doubles the memory. Persistent connections (one per reader) would attack the
0.15 s directly, and are the larger lever if the read-back is still the bottleneck after this.

#### Implemented (`76451ea`) and verified

`VERIFY_READBACK_BLOCK = 64 MiB` is now `verify_bucket_object`'s default. It is fixed rather
than scaled like the write chunks: the rate depends on the block, not the object's size, and
each block is held whole in memory, so a 1 GiB block would buy ~15% for 4 GiB in flight. The
decode keeps READ_BUFFER.

Verified with the **real `verify_bucket_object`** on three fresh, never-read 32 GiB composites
of deterministic AES-CTR data. Their md5s were computed on the node by regenerating the
keystream, so nothing was read back beforehand. The runs were ordered new, old, new:

| object | block | read-back | md5 |
|---|---|---|---|
| C | 64 MiB (new default) | 131.1 s, **249.9 MiB/s** | ok |
| A | 8 MiB (old, control) | 258.5 s, 126.8 MiB/s | ok |
| B | 64 MiB (new default) | 136.3 s, **240.5 MiB/s** | ok |

**1.93×**, with the two new runs 4% apart on either side of the control. For the 324.75 GiB
BAM that is ~23 minutes of read-back instead of 47.

### 13.79 Keep-alive for the read-back: no measurable gain at 64 MiB

§13.78 blamed a fixed ~0.15 s per ranged GET, from a fit to three depth-4 points, on the new
TLS connection each request opens, and named persistent connections as the next lever. They
were built (`98f12c2`): one `http.client` connection per reader thread for `download_range`,
with request()'s error classes, the single 401 refresh, one retry for a kept-alive
connection the server has closed, and a urllib fallback when a proxy is configured.

**Measured on fresh composites, same night and node (n1-standard-8, Intel Haswell,
us-east1-b), every window cold:**

| 4 × 64 MiB | keep-alive off | keep-alive on |
|---|---|---|
| harness windows | 125.3, 134.0, 142.5, 139.1 | 135.8, 131.4, 131.6, 136.3 |
| real `verify_bucket_object`, 32 GiB | 138.7 (md5 ok) | 139.3 (md5 ok) |

Also: 4 × 16 MiB with keep-alive read 122.6 and 129.3, and 8 × 64 MiB with keep-alive read
164.0 and 166.7. **Keep-alive made no measurable difference at the 64 MiB block.** Either
the 0.15 s is not connection setup, or at 64 MiB it is lost in the variance.

**The whole night ran at about half of §13.78's rates, and the code was ruled out.** §13.78's
4 × 64 MiB read 234-276 in the harness and 240-250 in the real verify. Here, yesterday's exact
module (`76451ea`, the one that measured 245) was run in rotation with today's, on fresh
windows: **128.3 and 125.9 MiB/s**, the slowest of the three variants. Also checked on this
node:

* Raw `curl`, 4 parallel × 1 GiB cold: **409.5 MiB/s**, so the network was not capped.
* One stream on warm data: Python `download_range` read 161-179 MiB/s and a streamed urllib
  read 293, against 131-204 for `curl`. Python has no per-stream deficit.
* md5 alone: 490 MiB/s, against 511 on §13.78's node, so the CPU does not explain it either.

What varied is the rate of **several cold ranged reads at once from one Python process**:
about 130 MiB/s tonight, 240-276 on §13.78's node. That makes the environment (host,
bucket, time) a factor of ~2 in this measurement. One consequence: §13.78's 1.93× for 64 MiB
blocks holds, because it compared blocks within one run, but its absolute times do not
transfer between nodes. Another: the depth result flipped. Here 8 × 64 beat 4 × 64 by 23%,
where §13.78 had 8 × 32 losing to 4 × 32. The depth optimum is environment-dependent, and
DEFAULT_DECODE_READAHEAD is left at 4.

**Decision pending:** keep-alive is neutral at 64 MiB and adds code, a failure mode (stale
connections) and a proxy caveat. Keep it or revert it.
