# Bucket-mounted localization

How canine gets a task's input files onto a worker, and every knob that controls it.

Scope: the `localize_to_persistent_disk` path — despite the name, this has not used a GCE
persistent disk since inputs moved to per-localization GCS buckets. This is what
`wolf.LocalizeToBucket` triggers, and it is the path essentially every wolF workflow takes.
`use_scratch_disk` is a separate mechanism (job *outputs*, still a real disk) and is only
mentioned where the two interact.

All line references are to this repo. The implementation lives in
`canine/localization/base.py`; naming and cache helpers in `canine/utils.py`; the user-facing
tasks in `wolF/wolf/localization.py`.

---

## 1. The shape of it

```
wolf.LocalizeToBucket(files={...})                       wolF/wolf/localization.py:24
  └── Task(extra_localization_args={"localize_to_persistent_disk": True, **kwargs})
        └── conf["localization"]                          wolF/wolf/task.py:324
              └── canine.localization.nfs.NFSLocalizer(backend, **conf["localization"])
                                                          wolF/wolf/task.py:1389
                    └── AbstractLocalizer                 canine/localization/base.py:67
```

`NFSLocalizer` subclasses `AbstractLocalizer`, so every kwarg in §5 is accepted there. The name
is historical: NFS refers to how the controller shares the *job workspace*, not to how inputs
travel. Under this path no localized input byte transits the shared mount.

Two phases, both emitted as bash and run **on the worker**, never on the controller:

| Phase | Built by | What it does |
|---|---|---|
| **Produce** | `create_bucket_mount()` → `bucket_upload_script()` | Create the bucket if needed, put the objects in it, label it |
| **Consume** | mount block in `job_setup_teardown()` | gcsfuse-mount the bucket read-only, verify, take a lease, start a heartbeat |

The same job script usually does both. A job whose inputs are already bucket-resident does only
the consume half; a `LocalizeToBucket` task (whose `script` is `":"`, a no-op —
`wolF/wolf/localization.py:35`) exists to do only the produce half.

---

## 2. Life of one localization

1. **Hash the input set.** `create_bucket_mount()` (`base.py:1090`) builds a content hash from
   each input's `name + basename + hash`, via the same `hash_set()` used for the old RODISK
   names. Identical input sets therefore converge on the same bucket.

2. **Name the bucket** (`utils.py:270`):

   ```
   wolf-<project_number>-<region>-<21 chars of content hash>
   ```

   - **project number, not project ID** — stable across project renames.
   - **region in the name** — buckets are regional; the same content localized in two regions
     needs two globally-unique names. Region comes from `_zone_to_region()`
     (`utils.py:235`), which just strips the zone suffix.
   - **21 hex chars** (`LOCALIZATION_BUCKET_HASH_LEN`, `utils.py:268`) — the longest hash that
     still fits 63 chars with the longest current region name (`northamerica-northeast1`, 23
     chars) and a 12-digit project number. 84 bits; ~5e-14 collision odds at 1e6 buckets.
   - The result is regex-validated, so a longer future project number fails here rather than at
     GCS (`utils.py:290`).

   Which zone is used comes from `AbstractLocalizer.backend_zone()` (`base.py:1004`): the
   backend's `"zone"` key, else `"compute_zone"`, else `get_default_gcp_zone()` — which raises
   if it cannot determine one rather than guessing.

3. **Plan the uploads.** Each input becomes an `UploadItem` with a `kind` (§4). No controller-side
   check of whether the bucket exists: the worker decides, because bucket creation *is* the
   mutex.

4. **Worker runs `bucket_upload_script()`** — §3.

5. **Worker mounts and consumes** — §6.

6. **Objects age out** under the bucket's lifecycle rule. Nothing deletes them explicitly.

---

## 3. Creating the bucket, and the concurrency protocol

`bucket_upload_script()` (`base.py:1145`). Everything is coordinated through the bucket itself,
because that is the only state every worker VM can see.

### Creation is the mutex

```bash
gcloud storage buckets create gs://<bucket> \
  --location=<region> \
  --soft-delete-duration=0 \
  --lifecycle-file="$CANINE_BUCKET_LC"
```

Bucket names are globally unique, so of N racing workers exactly one succeeds and the rest get
HTTP 409. No compare-and-swap on a sentinel object is needed — and `gcloud storage buckets
update` has no `--if-metageneration-match` to build one with anyway.

Two flags matter:

- **`--soft-delete-duration=0`** disables GCS soft delete, which new buckets otherwise get at 7
  days. Without it, every expired object is retained *and billed* for a further week — a 1-day
  expiry on a multi-TB localization would still pay for 7 days of invisible copies. This is a
  cache; nothing here is worth undeleting.
- **`--lifecycle-file`** applies the expiry rule *at creation*. A second call to set it might
  never happen if the job dies in between.

The rule (`base.py:1270`):

```json
{"rule":[{"action":{"type":"Delete"},"condition":{"daysSinceCustomTime": <localization_expiry_days>}}]}
```

Note `daysSinceCustomTime`, not age. Every object is written with `--custom-time`, and the
customTime is what gets refreshed to keep live content alive. An object with no customTime is
**invisible to this rule** and would never expire — which is why the gcsfuse write path
(§4c) explicitly stamps it afterwards.

The rule carries **no prefix filter**, so it applies to `_MOUNTS/` leases exactly as it applies
to data objects. That is deliberate (§7).

### The label state machine

The `wolf` bucket label carries the state. A worker that loses the create race reads it:

| Label | Meaning | Worker does |
|---|---|---|
| *(absent)* | Owner created the bucket but has not labelled it yet — `buckets create` cannot set labels | Wait |
| `working` | Someone is uploading | Wait |
| `success` + objects present | Content is there | Skip upload, refresh customTime |
| `success` + objects **absent** | Content aged out under the lifecycle rule | Re-upload |
| `stale` | A previous uploader timed out or died and released its claim | Take over and upload |

The `success`-but-empty case is not hypothetical: with a 1-day expiry it is the normal end state
of any bucket nobody touched for a day. Checking the label alone would mount an empty bucket.
So the check is label **and** `gcloud storage ls` of the expected objects (`base.py:1297`).

`stale` needs its own branch rather than falling into the wait: nobody else is going to finish
that upload, so waiting would deadlock until the timeout. `-n` on the copies keeps a double
take-over harmless.

### Waiting, and giving up

- **Bucket not yet readable.** Under real contention the dominant 409 is the *transient* one
  ("A conflicting operation is currently in progress"), which means the create is still in
  flight — not that the bucket is readable. So the worker polls `buckets describe` up to 30
  times at 2s before reading the label; failing that, `exit 5` (`base.py:1289`).
- **Someone else is uploading.** `sleep 60`, up to `bucket_upload_wait_tries` times (default 60,
  so a ~1 hour ceiling). On timeout the worker sets `wolf=stale`, releases the claim, and
  `exit 5` (`base.py:1314`).

**`exit 5` means "requeue this shard on another node"** — canine treats it as retryable rather
than a workflow failure. Every give-up path here uses it, because every cause is transient or
node-local.

---

## 4. The three upload paths

`create_bucket_mount()` tags each input with a `kind` (`base.py:1135`) based on its handler.
The whole point is that **no localized byte transits the shared NFS mount** — routing inputs
across the controller's disk twice (once on download, once on re-upload) is the regression this
design replaced.

### a. `server_side` — `gs://` sources → `HandleGSURL`

```bash
gcloud storage cp -r -n<rp> --custom-time="$CANINE_BUCKET_CT" <src> <dst>
```

A GCS rewrite: no bytes touch any VM at all. `-n` so a requeued shard does not re-copy.
`rp_string` because the *source* may be requester-pays even though our bucket is not. A
directory source is copied into its parent, or `cp -r gs://a/d gs://B/k/d` would produce
`gs://B/k/d/d/...` (`base.py:1188`).

### b. `copy` — files already on the shared mount → `HandleRegularFile` (`localization_mode == "local"`)

Typically an upstream task's output. The worker uploads it from where it already lives, adding
no NFS traffic beyond the read.

Deliberately **not** pre-validated controller-side. The obvious checks don't work: "under
`staging_dir`" is wrong because upstream outputs live in sibling task directories, and
`os.path.ismount()`/`st_dev` cannot see the shared mount at all when the controller has it on
the same block device as `/` (the usual layout — `mountpoint /mnt/nfs` says yes while
`os.path.ismount` says no). A worker that genuinely can't read the file gets a precise "No such
file or directory" from `cp`, which beats a heuristic that rejects valid inputs
(`base.py:1200`).

### c. `mount` — everything else: `s3://`, `drs://`, GDC, signed URLs, plain http

No server-side copy exists, so this VM has to move the bytes — but still not via NFS. The bucket
is mounted **read-write** on its own mountpoint and the download is written straight into it:

```bash
sudo mkdir -p /mnt/localize/<bucket>
sudo chown $(id -u):$(id -g) /mnt/localize/<bucket>
timeout -k 60 60 gcsfuse --implicit-dirs <bucket> /mnt/localize/<bucket>
  <handler's own localization_command writes into the mount>
fusermount -u /mnt/localize/<bucket>
gcloud storage ls <each object>                       # objects only exist after the unmount
gcloud storage objects update <objects> --custom-time="$CANINE_BUCKET_CT"
```

Four things worth knowing:

- **No `-o ro`**, unlike the consumer mount, and it lives at `/mnt/localize/...` rather than
  `/mnt/bucketmounts/...`. The consumer loop's "mount if not already mounted, no backoff" logic
  is only safe because those mounts are read-only; nothing writable should outlive this block.
- **It depends on gcsfuse streaming writes** (`--enable-streaming-writes`, default true since
  gcsfuse 3.x; the worker image pins 3.11.2). Without them gcsfuse buffers the whole object under
  `--temp-dir` on the 25 GB boot disk and ENOSPCs on a large input. Streaming writes are
  append-only from offset 0, so **the download must be sequential** — canine's handlers are
  (`aws s3api get-object --range` piped through `cat >>`, `curl -C - -o`), but a
  multipart/parallel writer would silently fall back to staging.
- **The unmount is what finalizes the objects**, which is what makes `wolf=success` mean "the
  content is actually there" rather than "the writes were issued".
- **gcsfuse cannot set customTime on write.** Hence the explicit `objects update` — without it
  these objects would be invisible to the lifecycle rule and the bucket would grow forever.

Inputs with `localization_mode == "stream"`, `StringLiteral`, and already-localized
`bucketmount://`/`rodisk://` URLs are excluded from the plan entirely (`base.py:1065`).

---

## 5. Options

### 5a. Localizer options (`AbstractLocalizer.__init__`, `base.py:73`)

Pass from wolF via `LocalizeToBucket(files=..., <kwarg>=...)`, or workflow-wide via
`Workflow(common_task_opts={...})`, or per-task via `extra_localization_args={...}`.

**Bucket localization:**

| Option | Default | Effect |
|---|---|---|
| `localize_to_persistent_disk` | `False` | Master switch for this whole path. `LocalizeToBucket` sets it `True`. Also forces `common = False` (`base.py:158`). |
| `localization_expiry_days` | `1` | `daysSinceCustomTime` in the lifecycle rule. Bounds *idle* storage, not the life of a running workflow — live content has its clock refreshed on every localization and by the heartbeat. Raise it if you re-run the same inputs over several days and would rather pay for storage than re-transfer. |
| `bucketmount_heartbeat_seconds` | `3600` | How often a held mount re-stamps customTime (§7). Must be `< localization_expiry_days * 86400 / 4`, else `ValueError` at construction (`base.py:181`) — for the default 1-day expiry, anything `< 21600`. |
| `bucket_upload_wait_tries` | `60` | 60-second polls a worker waits for a sibling's upload before declaring the claim stale and requeueing (`exit 5`). Default is a ~1 hour ceiling. |
| `allow_requester_pays` | `False` | If `False`, reading from a requester-pays source raises instead of silently billing `project`. |
| `persistent_disk_dry_run` | `False` | Return the `bucketmount://` URLs that *would* be produced without creating or uploading anything. |

**General:**

| Option | Default | Effect |
|---|---|---|
| `project` | ADC default | Project whose number goes in the bucket name and which is billed. |
| `staging_dir` | random UUID | Job workspace on the shared mount. Not in the input data path. |
| `common` | `True` | De-duplicate inputs shared across shards. Forced off when localizing to buckets. |
| `transfer_bucket` | `None` | Legacy intermediate transfer bucket. Unused on this path. |
| `token` | `None` | Auth token for handlers that need one (GDC, DRS). |
| `cleanup_job_workdir` | `False` | Delete workspace files not captured as outputs. Also settable via `Workflow(common_task_opts={"cleanup_job_workdir": True})`. |

**Scratch disk (job *outputs* — separate mechanism, listed for completeness):**

| Option | Default |
|---|---|
| `use_scratch_disk` | `False` |
| `scratch_disk_size` | `10` (GB) |
| `scratch_disk_type` | `"standard"` (or `"ssd"`) |
| `scratch_disk_name` | random |
| `scratch_disk_job_avoid` | `True` |
| `protect_disk` | `False` — adds label `protect:yes`, blocking automatic deletion |
| `files_to_copy_to_outputs` | `{}` — output keys copied from the scratch disk back to NFS |
| `persistent_disk_type` | `"standard"` |

### 5b. Backend options that affect localization

| Option | Where | Default | Effect |
|---|---|---|---|
| `compute_zone` | `imageTransient.__init__` (`:79`) | auto-detect | Picks the bucket's **region**. Auto-detection now raises rather than defaulting to a guess. |
| `zone` | `gcpTransient` (`:77`) | auto-detect | Same, different key name — `backend_zone()` reads both. |
| `rapid_cache` | `imageTransient` (`:90`), `gcpTransient` (`:59`) | `False` | Opt-in Rapid Cache (formerly Anywhere Cache). See the caveat below. |
| `rapid_cache_ttl` | same | `"1d"` | Keep aligned with `localization_expiry_days`. A longer cache TTL means paying to cache objects that have already been deleted — at 7d against a 1d expiry that was ~7x the cache bill for no benefit. |

Rapid Cache is opt-in because it is not free and not always a win: cache storage bills per
GiB-hour at roughly 4x standard storage (Iowa: $0.0001233/GiB-hour, ~$0.089/GiB-month), while
the transfer it saves is $0/GiB within North America. An in-region workload pays purely for read
latency — worth it for an input read by many shards, wasteful for read-once work. Provisioning
failure is logged and startup continues; it affects speed, not correctness
(`imageTransient.py:189`).

> **Caveat — verify before relying on it.** `get_or_create_rapid_cache()` is called on
> `config["storage_bucket"]` (`imageTransient.py:192`, `gcpTransient.py:254`), which
> `__enter__` sets to the *workflow* bucket `canine-<project>-<workflow_name>`
> (`get_or_create_workflow_bucket`, `utils.py:294`). The per-localization `wolf-...` buckets are
> never passed to it, and `grep storage_bucket canine/localization/*.py` returns nothing — the
> localization path never reads that config key at all. As written, `rapid_cache=True` therefore
> provisions a cache on a bucket that bucket-mounted localization does not read. The workflow
> bucket is a leftover from the earlier one-bucket-per-workflow design.

Also note the Rapid Cache is **zonal** while the localization bucket is **regional**: one cache
instance per zone per bucket, and a worker in another zone of the same region gets a silent miss.

### 5c. wolF task API (`wolF/wolf/localization.py`)

| Class | Purpose |
|---|---|
| `LocalizeToBucket(files={...}, **kwargs)` | Produce a localization. `kwargs` pass straight through as `extra_localization_args`. `job_avoid=False`, script is a no-op. |
| `DeleteLocalizedFiles(disk=<bucketmount:// URL>)` | Evict one input key's objects early (§8). |
| `DeleteDisk` | **Deprecated** alias for the above; emits a `DeprecationWarning` (`:135`). |
| `LocalizeToDisk` | **Deprecated** alias for `LocalizeToBucket`; warns on every use. |
| `BatchLocalDisk(files=...)` | Legacy alias for `LocalizeToBucket`. |

> **Gotcha:** `BatchLocalDisk.__init__` accepts `**kwargs` but calls
> `super().__init__(files = files)` (`wolF/wolf/localization.py:163`), silently dropping them.
> `BatchLocalDisk(files=..., localization_expiry_days=3)` does nothing. Use `LocalizeToBucket`.

---

## 6. Consuming: mount, verify, lease

The mount block runs per bucket (`base.py:1885`), driven by these exports:

| Variable | Value |
|---|---|
| `CANINE_N_BUCKETMOUNTS` | how many distinct buckets this job needs |
| `CANINE_BUCKETMOUNT_<i>` | bucket name |
| `CANINE_BUCKETMOUNT_DIR_<i>` | `/mnt/bucketmounts/<bucket>` |

Order matters, and each step exists because of a specific failure:

1. **Clear a stale FUSE endpoint.** A job that finished on this node moments ago may have
   unmounted this same path. Every subsequent operation on it — `stat`, `mkdir`, even `flock` —
   then fails with `ENOTCONN`/`EACCES` rather than `ENOENT`, so it has to be cleared before the
   path is touched at all (`base.py:1918`).

2. **`mkdir` + `chown` to the invoking user.** The mountpoint must be writable by whoever runs
   gcsfuse; `fusermount3` refuses otherwise. Deliberately *not* fixed with `sudo gcsfuse`:
   mounting as the invoking user keeps it readable without `-o allow_other`, and podman maps the
   task container's root to this same UID (`wolf/task.py` `--uidmap`), so the task container can
   read it too.

3. **Mount read-only**, if not already mounted:

   ```bash
   timeout -k 60 60 gcsfuse -o ro --implicit-dirs <bucket> /mnt/bucketmounts/<bucket>
   ```

   The **whole bucket** is mounted; each input's object path lives in its symlink, so one gcsfuse
   process serves every input from that bucket. Unlike a RODISK there's no cross-node attach race
   to guard — gcsfuse supports many concurrent read-only mounts — so it's a plain "mount if not
   mounted" with no backoff. gcsfuse resolves credentials via ADC and does **not** read
   `CLOUDSDK_CONFIG`, so `GOOGLE_APPLICATION_CREDENTIALS` is pointed explicitly at the
   credentials the image stages (`base.py:1952`); without this the authenticating identity
   depends on whatever ADC happens to resolve to.

4. **Reachability check** (`bucketmount_reachability_check()`, `base.py:1435`). Every input
   symlink pointing into this mount must resolve, or `exit 5`. A mounted-but-empty bucket is
   gcsfuse's nastiest failure mode: the mount succeeds, `mountpoint -q` passes, and reads come
   back ENOENT far from the cause. Two ways to land there — the objects were never written, or
   this node already had the bucket mounted from *before* they were and its metadata cache is
   stale.

5. **Take the busy-lock.** `flock -os <lock> sleep infinity &`, pid recorded. This is not a
   cross-node attach race — it stops a concurrent job on the *same node* from having the mount
   pulled out from under it by another job's teardown. The lock file lives **beside** the
   mountpoint, not inside it: locking the mountpoint directory itself (as this once did) puts the
   lock inside the thing it protects, so after any unmount `flock` cannot even open the path and
   the teardown guard silently stops guarding (`base.py:1928`).

6. **Register the mount lease** (§7).

7. **Start the heartbeat** (§7).

Steps 4–7 sit **outside** the "mount if not already mounted" conditional. A job landing on a node
where the bucket is already mounted does no mounting itself, but is every bit as much a consumer.

---

## 7. Leases and the heartbeat

These two solve the same underlying problem — *this content is in use, don't reclaim it* — at two
different timescales.

### The lease: who is reading right now

GCS has no server-side notion of "who has this gcsfuse-mounted"; gcsfuse is just a client. So the
bucket itself is the only registry every VM can see. Each consumer drops a marker
(`bucketmount_lease_register()`, `base.py:1336`):

```
gs://<bucket>/_MOUNTS/$(hostname)-${SLURM_JOB_ID:-$$}-${SLURM_ARRAY_TASK_ID:-0}
```

Keyed by hostname + job + array task, so it is unique across VMs *and* across array shards
co-scheduled on one VM. Teardown removes the exact URLs it recorded — never a pattern — so a job
can only ever free its own (`base.py:1421`). The URLs are tracked in
`${CANINE_JOB_INPUTS}/.bucketmount_leases`.

Writing a lease is best-effort: failure warns rather than killing the job. It is bookkeeping, and
losing it costs a redundant re-localization at worst.

`DeleteLocalizedFiles` refuses to delete anything while any `_MOUNTS/` object exists (§8). This
was added after a live failure: two sibling flows whose uploads were job-avoided deleted the
objects a third flow was about to read, leaving the bucket labelled `success` but empty.

### The heartbeat: what happens past 24 hours

**The problem.** `customTime` is stamped once, at upload and at mount, and never renewed. The
lifecycle rule carries no prefix filter. So a job holding a mount longer than
`localization_expiry_days` loses two things at once:

- **Its lease expires.** A sibling flow's `DeleteLocalizedFiles` then sees no holder and deletes
  content that is actively mounted — precisely the use-after-free the lease exists to prevent.
- **Worse: the data objects themselves expire.** GCS deletes the inputs out from under gcsfuse
  with no second flow involved at all. A single task running past 24 hours is enough. gcsfuse
  pins the object generation at `open()`, so an in-flight read fails (404/412) rather than
  silently returning wrong bytes — a hard failure, not corruption, but a failure.

The dbGaP workload never exposed this (each job is one transfer, well under an hour). It bites
any workflow whose task reads a localized input for more than a day, which is an ordinary shape
for alignment or variant-calling work.

**The fix** (`bucketmount_heartbeat_start()`, `base.py:1369`). Alongside the mount, start a loop
that periodically re-stamps customTime across the whole bucket:

```bash
CANINE_BUCKETMOUNT_HB=${CANINE_JOB_INPUTS}/.bucketmount_heartbeat_${CANINE_BUCKETMOUNT}.sh
cat > "${CANINE_BUCKETMOUNT_HB}" <<'CANINE_HEARTBEAT_EOF'
while true; do
  sleep <bucketmount_heartbeat_seconds>
  gcloud storage objects update "gs://$1/**" --custom-time="$(date -u +...)" > /dev/null 2>&1 || :
done
CANINE_HEARTBEAT_EOF
set +e; bash "${CANINE_BUCKETMOUNT_HB}" "${CANINE_BUCKETMOUNT}" & \
  echo $! >> ${CANINE_JOB_INPUTS}/.bucketmount_heartbeat_pids; set -e
```

Design points, each load-bearing:

- **The update is bucket-wide** (`gs://$1/` plus a recursive wildcard), so one call per beat
  covers the `_MOUNTS/` lease *and* the data objects — both failure modes above, together.
- **The bucket is passed as `$1`, not inherited.** The heredoc is quoted so `$(date)` is
  evaluated per beat rather than frozen at write time — but that also means
  `CANINE_BUCKETMOUNT`, a plain unexported shell variable, would be empty in the child and every
  beat would update `gs:///**`. This was caught by rendering the generated script, not by
  reading it.
- **Failure is non-fatal** (`|| :`), same reasoning as the lease write.
- **Self-healing by construction.** If the worker dies, the loop dies with it, nothing is renewed,
  and the content expires on the normal schedule. That is exactly what the lease design already
  relies on.
- **The interval is validated at construction** (`base.py:181`), not discovered as a mid-run
  deletion. 1 hour against a 1-day window gives 24 beats per window.

Teardown kills the recorded pids (`bucketmount_heartbeat_stop()`, `base.py:1407`), so content
resumes ageing normally once nobody holds it.

**Rejected alternative:** excluding `_MOUNTS/` from the lifecycle rule via `matchesPrefix`. It
fixes the lease problem only, leaves the data-object problem untouched, and makes a lease orphaned
by a crashed job *permanent* — turning a self-healing failure into one that blocks that content's
reclamation forever.

---

## 8. Teardown and deletion

Teardown order (`base.py:2073`–`2097`), and it is an order, not a list:

1. `bucketmount_heartbeat_stop()` — stop refreshing customTime first.
2. Kill the busy-lock pids.
3. Per bucket: if `flock -n` succeeds (nobody else on this node holds it) **and** it is still a
   mountpoint, `fusermount -u`. Otherwise log "busy, likely in use by another job" and leave it.
4. `bucketmount_lease_release()` — drop this job's `_MOUNTS/` objects.

**Nothing deletes the bucket.** Objects age out under the lifecycle rule; the empty bucket
survives. That is the intended steady state — the next task needing that content finds
"labelled `success` but objects absent" and re-localizes.

`DeleteLocalizedFiles` (`wolF/wolf/localization.py:67`) exists to free storage early rather than
waiting for the rule. It:

- deletes `gs://<bucket>/<input_key>/` — one input key's objects, not the bucket, not the worker's
  boot disk, not a scratch disk, not the shared mount;
- **skips entirely if any `_MOUNTS/` lease exists**, warning instead;
- is a cache eviction, not a deallocation. Because deleting one input of a multi-input
  localization leaves the bucket labelled `success` but incomplete, *all* of that localization's
  inputs get re-fetched next time.

The re-localization recovery only protects a **producer**, which re-checks and re-uploads. A
consumer already holding a `bucketmount://` URL has no source to re-fetch from and can only fail
— which is why the lease check is a hard skip rather than a warning-and-proceed.

Normally you don't need this task at all.

---

## 9. Quick reference

**Paths on the worker**

| Path | What |
|---|---|
| `/mnt/bucketmounts/<bucket>` | consumer mount, read-only |
| `/mnt/bucketmounts/.<bucket>.lock` | busy-lock, deliberately outside the mount |
| `/mnt/localize/<bucket>` | read-write upload mount, unmounted before the task runs |
| `${CANINE_JOB_INPUTS}/.bucketmount_leases` | lease URLs this job took |
| `${CANINE_JOB_INPUTS}/.bucketmount_lock_pids` | busy-lock pids |
| `${CANINE_JOB_INPUTS}/.bucketmount_heartbeat_pids` | heartbeat pids |

**In the bucket**

| Object | What |
|---|---|
| `<input_name>/<basename>` | a localized input |
| `_MOUNTS/<host>-<job>-<shard>` | a live consumer's lease |

**Labels:** `wolf=working` · `wolf=success` · `wolf=stale`

**`exit 5`** anywhere in this path means *requeue this shard on another node* — used for every
give-up, because every cause here is transient or node-local.

**Tests:** `canine/test/test_localizer_bucket_upload_pure.py` (bucket naming, layout, state
machine, upload plan), `canine/test/test_localizer_reachability_pure.py` (reachability, leases,
heartbeat), `canine/test/test_rapid_cache_pure.py`. All pure — no cluster, no GCP credentials.
