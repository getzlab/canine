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
   mutex. A local path on the NFS share (`/mnt/nfs/...`) is not planned at all: it passes through
   as its path (§4b), and is left out of the content hash.

4. **The controller uploads local paths**, if there are any (§4b): files on its own disks, which
   no worker can read.

5. **Worker runs `bucket_upload_script()`** — §3.

6. **Worker mounts and consumes** — §6.

7. **Objects age out** under the bucket's lifecycle rule. Nothing deletes them explicitly.

---

## 3. Creating the bucket, and the concurrency protocol

`bucket_upload_script()` (`base.py:1145`). Everything is coordinated through the bucket itself,
because that is the only state every worker VM can see.

### Creation is the mutex

```bash
gcloud storage buckets create gs://<bucket> \
  --location=<region> \
  --soft-delete-duration=0 \
  --public-access-prevention \
  --uniform-bucket-level-access \
  --lifecycle-file="$CANINE_BUCKET_LC"
```

(`BUCKET_CREATE_FLAGS` in `base.py`, shared by the node and the controller.)

Bucket names are globally unique, so of N racing workers exactly one succeeds and the rest get
HTTP 409. No compare-and-swap on a sentinel object is needed — and `gcloud storage buckets
update` has no `--if-metageneration-match` to build one with anyway.

Three things matter:

- **`--soft-delete-duration=0`** disables GCS soft delete, which new buckets otherwise get at 7
  days. Without it, every expired object is retained *and billed* for a further week — a 1-day
  expiry on a multi-TB localization would still pay for 7 days of invisible copies. This is a
  cache; nothing here is worth undeleting.
- **`--lifecycle-file`** applies the expiry rule *at creation*. A second call to set it might
  never happen if the job dies in between.
- **The bucket is private.** Localized inputs include protected data such as BAMs.
  `--public-access-prevention` refuses any `allUsers` or `allAuthenticatedUsers` grant (HTTP 412).
  `--uniform-bucket-level-access` puts access entirely in IAM. Created this way, a bucket gives
  the project's owners and editors read/write and its viewers read (`legacyObjectOwner`,
  `legacyObjectReader`), and project-, folder- and org-level storage roles still apply. So nodes,
  users and admins troubleshooting see no difference. There are no per-object ACLs: to let
  someone outside those roles read a bucket, grant it on the bucket,
  `gcloud storage buckets add-iam-policy-binding gs://<bucket> --member=user:<email> --role=roles/storage.objectViewer`.

  Buckets created before this (never deleted, only emptied) get public access prevention the
  next time a job uploads into them: it rides on the `wolf=working` label update, so costs no
  extra call. They do not get uniform access. Turned on for an existing bucket, it drops the
  object ACLs that project editors and viewers read through and adds no IAM role in their place:
  tested, reading the bucket's own object then returned 403.

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

### Who uploads: the claim object

Only bucket creation is a mutex, and after it every decision used to be made per job from
the `wolf` label. With 50–100 workflows sharing reference files, and so sharing one bucket,
an expired, stale or abandoned bucket was taken over by every job that saw it at once. At
100 jobs, 40–50 uploaded the same inputs, and up to 14 failed on bucket-label rate limits
(`PARALLEL_LOCALIZATION.md` §13.87).

So **the claim object `gs://<bucket>/.wolf_claim` decides who uploads**:

* **Taking an upload means winning a compare-and-swap.** A job reads the claim's generation
  (0 if absent) and writes the claim with `--if-generation-match=<that generation>`; exactly
  one racer succeeds. The bucket's creator takes the claim the same way.
* **A claim is live while its server-side update time is under 600 s old**
  (`BUCKET_HEARTBEAT_STALE`). The owner refreshes it every 60 s
  (`BUCKET_HEARTBEAT_INTERVAL`), conditional on its own generation, so it can never keep
  someone else's claim alive. A live claim is waited on, whatever the label says.
* **A dead or absent claim is taken**, by exactly one job, whether the label says
  `working`, `stale`, or nothing. This covers an owner that vanished without running its
  trap: preemption, a deleted node, or a torn-down cluster. It is taken 10 minutes after
  the last heartbeat, instead of after `bucket_upload_wait_tries`.
* **A requeued job takes back its own claim at once.** The claim records its owner as
  custom metadata (`canine_owner` = the submitting controller's host and `SLURM_JOB_ID`, both
  kept across a SLURM requeue). A job that finds a live-looking claim with its own owner knows
  the holder is its previous attempt, since SLURM runs one attempt at a time. It takes the
  claim back with the same compare-and-swap, instead of waiting out the 10 minutes. A
  controller-side run is never requeued and gets an owner no job matches.
* **An upload that does not finish releases its claim.** On a failure, or scancel's SIGTERM
  (`KillWait` gives it 300 s), an EXIT trap deletes the claim (conditional on its generation)
  and labels the bucket `stale`, so one waiter takes over at its next poll. The exit code is
  kept.
* **On success**, the owner labels the bucket `wolf=success`, then releases the claim. A job
  that wins a claim re-reads the label before uploading, because an owner may have finished
  between that job's two reads.

The label still carries the state, for readers and for older canine versions, but only
`success` changes what a job does:

| Label | Meaning | Worker does |
|---|---|---|
| `success` + objects present | Content is there | Skip upload, refresh customTime |
| `success` + objects **absent** | Content aged out under the lifecycle rule | Take the claim and re-upload |
| `working`, `stale`, or absent | An upload is in progress, was abandoned, or has not started | Take the claim if it is dead or absent, else wait |

The `success`-but-empty case is not hypothetical: with a 1-day expiry it is the normal end state
of any bucket nobody touched for a day. Checking the label alone would mount an empty bucket.
So the check is label **and** `gcloud storage ls` of the expected objects.

**Label writes are retried** with jitter, up to 8 times: GCS allows about one bucket-metadata
update per second, and `gcloud_exp_backoff` does not retry that 429, because it retries only
"Quota exceeded".

**Copies are still made safe against a race.** Two jobs can still both copy an object, since
the pre-claim protocol exists in older canine versions still running. `-n` alone does not
make that harmless: it checks for the destination before copying, so a sibling that writes
the object in between makes the second copy fail with HTTP 412 (`GcsPreconditionFailedError`).
Each server-side copy, and each local path the controller uploads, is therefore rerun up to three times, ten seconds
apart. The rerun sees the object and skips it with exit 0 (`Skipping existing destination item
(no-clobber)`). Downloads onto the read-write mount need no retry here, since the parallel
downloader handles two writers on one object itself.

### Waiting, and giving up

- **Bucket not yet readable.** Under real contention the dominant 409 is the *transient* one
  ("A conflicting operation is currently in progress"), which means the create is still in
  flight — not that the bucket is readable. So the worker polls `buckets describe` up to 30
  times at 2s before reading the label; failing that, `exit 5` (`base.py:1289`).
- **Someone else is uploading.** `sleep 60`, up to `bucket_upload_wait_tries` times (default 90,
  so a ~1.5 hour ceiling), while another job holds a live claim. On timeout the worker
  `exit 5`s, but no longer marks the bucket stale: the claim it waited on was live, so its
  uploader is alive. **This ceiling has to exceed the longest localization you expect**, or
  waiters requeue on healthy uploads. It is no longer the recovery time when an uploader
  dies; the claim's heartbeat is (10 minutes). Sized in
  `canine/test/BENCHMARK_RUNBOOK.md` §6.7.

**`exit 5` means "requeue this shard on another node"** — canine treats it as retryable rather
than a workflow failure. Every give-up path here uses it, because every cause is transient or
node-local.

### When no node is needed: settling on the controller

A `LocalizeToBucket` job does nothing but localize, and every one used to take an exclusive
n1-standard-8. That included jobs whose bucket was already populated, and jobs whose inputs
were all server-side copies moving no bytes through the node. When 50–100 concurrent workflows
share reference files, their localization jobs all resolve to the same bucket, so 99 nodes
booted to do nothing.

**The controller moves no data, except local paths.** It is shared by every workflow in a
large run, so only metadata, GCS server-side copies, and local paths (§4b), which no worker can
read, happen there. Any input that is downloaded needs a node:

| upload kind | needs a node? |
|---|---|
| `server_side` (gs://, not gzip-encoded) | no: a GCS rewrite, with no bytes through anything |
| `local` (a local path, not on the NFS share) | no: the controller uploads it |
| `mount` (http, S3, GDC, DRS, gzip-encoded gs://) | **yes**: a download onto the read-write mount |

So after localizing and before submitting, wolF's `LocalizeToBucket.after_localize()` asks
canine to settle each job on the controller (`Orchestrator.resolve_localizations_on_controller`
→ `AbstractLocalizer.resolve_on_controller`):

* **Every input is `server_side` or `local`:** the controller runs the job's emitted
  claim-and-upload block itself, the same `bucket_upload_script` output a node runs, except that
  it uploads local paths under the claim rather than only checking for them. That is normally a
  no-op, since they were uploaded when the job was localized. If one has gone missing since, it
  is uploaded again here, rather than sent to a node that could only fail on it. The
  claim, heartbeat, release trap and retries all apply, so it competes fairly with node-side
  uploaders. Waiting on another job's live claim holds a controller thread, not a node.
* **Some input is `mount`:** the controller checks only whether the bucket is
  already populated (`bucket_populated_script`: `success` plus every object present),
  refreshing customTime if so. That check is read-only, so the node job still does any
  claiming, uploading and downloading.

A job settled this way is dropped from the batch, the way job avoidance drops one, and is never
submitted. Its `stdout` and `stderr` are the controller-side output, written where
`delocalization.py` would put them. Anything that doesn't settle is submitted to a node exactly
as before: a failed controller run falls back to a node job. The log says which of these
happened, so it never claims a transfer that did not occur:

| log line (`localization job <id>: ...; no node needed`) | meaning |
|---|---|
| `already localized in gs://<bucket>, found complete` | an earlier or concurrent workflow's job had filled the bucket; nothing transferred |
| `localized in gs://<bucket> by another job, waited for its upload` | another job held a live claim; this one waited on the controller until the bucket was complete |
| `transferred on the controller into gs://<bucket> (server-side copies)` | this job won the claim and made the copies itself |
| `transferred on the controller into gs://<bucket> (local paths uploaded from the controller)` | this job won the claim, and its inputs were local paths (with `server-side copies, ` first if it also had those) |

A job whose bucket is not yet complete never returns early. It waits on the other job's live
claim, on the controller or on its node, until the bucket is labelled `success` with every
object present, so a downstream task never mounts a partial localization.

**Only `LocalizeToBucket` does this.** An ordinary task that localizes its inputs to a bucket
still needs its node for its own script, so the hook (`Task.after_localize`) does nothing
by default.

**Turning it off**, on by default:

* **On the controller, at runtime:** `export CANINE_DISABLE_CONTROLLER_LOCALIZATION=1` before
  running wolF. It must be set where wolF runs, not on the nodes, since the controller makes
  the decision.
* **Per localization:** `wolf.LocalizeToBucket(files = {...}, resolve_on_controller = False)`.

Measured at 100 concurrent workflows on an n2-standard-8 controller
(`PARALLEL_LOCALIZATION.md` §13.88):

* **Server-side inputs,** on a new or an expired bucket: 0 node jobs.
* **Inputs to download, on a populated bucket:** 0 node jobs.
* **Inputs to download, on an unpopulated bucket:** 100 node jobs. They still need nodes.

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

**Content-Encoding: gzip objects do not take this path.** A `gs://` object can carry
`Content-Encoding: gzip` as transport metadata, meant to be served transparently decompressed.
The rewrite above preserves that metadata, and the still-compressed bytes under it, verbatim.
Every reader of the bucket-mounted destination then gets the gzip stream; gcsfuse does not
decode. A consumer with no gzip-awareness of its own cannot know it needs to decompress: this
was confirmed live as a GATK `.dict` reference input failing with "Failed to load reference
dictionary", with nothing in the error pointing at Content-Encoding.

Clearing the metadata afterward (`objects update --clear-content-encoding`) was tried first,
live, and does **not** fix it. The stored bytes stay compressed whatever the tag says (matching
md5/crc32c before and after). The bytes themselves have to be decoded, and a rewrite cannot do
that.

So such an object is classified **at plan time** (`HandleGSURL.transport_gzip`), from the
metadata that sizing already fetched, so it costs no extra request. It is routed to the
`mount` kind (c. below) instead. There, the
parallel downloader reads the stored bytes over the JSON API (`--gs-source`, with `userProject`
for requester-pays) and verifies them against the object's md5. It then decodes them straight
onto the read-write mount, and nothing is staged on local disk
(`HandleGSURL.downloader_command`). A name promising gzip content (`.vcf.gz`, `.bam`, ...)
whose decoded bytes are not gzip keeps its stored bytes, because the encoding was set by mistake
on a file that is only singly compressed. The fallback, used when the downloader is unavailable,
is the handler's `gcloud storage cp`, which also decodes. Note that it leaves
`.gcloud_tracker_dir/` and `.gcloud_manifest` beside the object on the mount.

This replaced an earlier runtime branch here: `describe`, then
`gcloud storage cat | gunzip > $(mktemp)`, then re-upload. That branch had four problems:

- It staged the whole decompressed object on the boot disk, which runs out of space at gnomAD
  scale.
- It decoded mislabelled `.vcf.gz` files into plain text.
- It fell through to the plain copy, reproducing the bug, whenever `describe` failed.
- It relied on `gcloud storage cat` never transcoding.

See `PARALLEL_LOCALIZATION.md` §13.71.

**Directory sources** get the same treatment one object at a time. The Funcotator data sources
directory has 35 of its 46 objects gzip-encoded, among them a GATK `.dict`.
`HandleGSURL.gzip_members` names them from the listing that sizing already fetched, so it costs
no extra request, and each one becomes its own `mount` item at its path within the directory.
`cp` has no way to leave objects out, so the rest of the directory is copied with an rsync
instead. Between buckets, rsync is also a server-side copy:

```bash
gcloud storage rsync -r -n<rp> --custom-time="$CANINE_BUCKET_CT" --exclude='^(?:<name>|...)$' <src> <dst>
```

`--exclude` is a Python regex over names relative to the source, anchored and escaped here, so
it matches exactly the encoded names. The destination is the directory itself, because rsync
copies a directory's contents. Everything lands under one prefix in one bucket, so consumers
still mount a single path. A directory with no encoded objects keeps the plain `cp -r`. Copying
the whole directory and then decoding over the top was rejected, because it leaves encoded
objects readable until each decode lands.

### b. Local paths: on the NFS share, passed through; anywhere else, `local`

A local path (`HandleRegularFile`, `localization_mode == "local"`) is either on the NFS share or
not, and that decides everything. The test is the prefix `SHARED_MOUNT = "/mnt/nfs"` (`base.py`),
applied after resolving symlinks, because the share is mounted there on the controller and on
every worker. It is not a device comparison: the controller usually has the share on the same
block device as `/`. On that layout, `df` and `st_dev` call every controller path shared, and
`os.path.ismount` cannot see the share at all.

**On the share: passed through.** This is typically an upstream task's output, or a reference
directory on the share. Every worker already reads it where it is, so it is not copied into a
bucket and not counted in the content hash. Its path is the result: `LocalizeToBucket` returns it
instead of a `bucketmount://` URL, and a task localizing to a bucket gets it as a string. A string
input holding a share path passes through the same way; `NFSLocalizer` turns share paths that are
not canine outputs into strings. A symlink elsewhere that points into the share passes the share
path it resolves to, because the link would dangle on a worker. An input set with nothing else
to localize needs no bucket.

**Anywhere else: `local`, uploaded by the controller.** Any local path not under `/mnt/nfs` is on
a disk of the controller that workers can't see. That includes a link on the share that points
off it. So the controller uploads it, in `job_setup_teardown`, when the job is localized and
before anything is submitted (`upload_local_paths` running `local_upload_script`):

* It creates the bucket if it's absent, with the node's lifecycle, soft-delete and privacy settings.
* It runs `gcloud storage cp -r -n --custom-time`, rerun on a lost race.
* It takes no claim and sets no label. The object is content-addressed, so every job uploading it
  uploads the same bytes, and `-n` skips what is already there.
* A failed upload fails localizing the task.

For a `LocalizeToBucket` whose inputs are all server-side or local, no node is involved: the
controller settles it (§3). A node runs only when the job also downloads something, when
controller settling is off, or when the task has its own script to run. That node's
`bucket_upload_script` only checks that each `local` object is there (`gcloud storage ls`). If
one expired while the job waited in the queue, the check exits 1, not 5. A requeue reruns only
the node's script, which can't upload it; wolF's rerun of the task localizes again, and so
uploads it again.

`NFSLocalizer` uses the same rule outside bucket localization. It symlinks a share path into the
job's inputs and copies any other local path. Both decisions used to come from `same_volume`
(`df -P` against `staging_dir`). On the usual layout, that symlinked controller-only files, and
workers couldn't follow the links.

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
  Objects the parallel downloader composes already carry one: it stamps its own at compose
  time, which is later than `$CANINE_BUCKET_CT`, and GCS refuses to move a customTime earlier
  ("Custom time cannot be decreased", HTTP 400). So when the update fails, the script checks
  each object with `objects describe` and fails only if one has no customTime at all.

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
| `bucket_upload_wait_tries` | `90` | 60-second polls a job waits on another job's live claim before requeueing (`exit 5`) — a ~1.5 hour ceiling. **Derived, not guessed**: the bucket route measures 0.57 h for the largest real input, so 0.57 h + the 60 s create ceiling, doubled, is 71 polls. It must exceed the longest upload, or waiters requeue on healthy ones. A dead uploader no longer costs this window: its claim's heartbeat goes stale after 10 minutes and one waiter takes over (§3). Scale by **bytes** if your largest set relays more (`pdl claim --localization-bytes`); a BAM plus indices is ~1.0x, two BAMs is 2.0x, and `gs://` inputs do not count. `canine/test/BENCHMARK_RUNBOOK.md` §6.6/§6.7. |
| `resolve_on_controller` | `True` | Let `LocalizeToBucket` settle a job on the controller when no node is needed (§3, "When no node is needed"). `CANINE_DISABLE_CONTROLLER_LOCALIZATION`, set on the controller, turns it off at runtime. |
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
| `LocalizeToBucket(files={...}, **kwargs)` | Produce a localization. `kwargs` pass straight through as `extra_localization_args`. `job_avoid=False`, script is a no-op, and a job needing no node is settled on the controller instead of submitted (§3). |
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

Bucket mounts live for the **node's lifetime**: once a bucket is mounted on a node it stays
mounted until the VM is deleted, and no job ever unmounts it. An unmount in one job's teardown
raced every other shard still reading the same mount on that node, and no lock closed that race
cheaply.

1. **Create the mount-setup lock file, beside the mountpoint.** `CANINE_BUCKETMOUNT_MOUNTLOCK`,
   created as root (the parent dir may still be root-owned from a prior bucket's first `mkdir`)
   and immediately `chown`'d to the invoking user, so later plain (non-`sudo`) opens succeed. It
   lives beside the mountpoint, not inside it, so it stays openable whatever state the mount is
   in. `mkdir -p` and repeated `touch`/`chown`-to-the-same-owner are safe under unsynchronized
   concurrent execution, so this step needs no lock.

   **Fast path:** if the bucket is already mounted and live -- `mountpoint -q` passes and `stat`
   answers within 10 s, the `timeout` guarding against a hung FUSE daemon -- steps 2-5 are skipped
   entirely, lock included. That is now the common case for every shard after a node's first.

2. **Acquire the mount-setup lock** (`flock -x -w 300 200 ... ) 200>$CANINE_BUCKETMOUNT_MOUNTLOCK`,
   wrapping steps 3-5 below). Shards of the same scatter job routinely start on the same node in
   the same instant; without serialization, several independently see "not mounted yet" and race
   to run `gcsfuse` on the identical path concurrently. Confirmed live: every racing shard failed,
   with `fusermount3` reporting *"the user doesn't have write-access on the mount point: read-only
   file system"* — a symptom of the race, not of the mountpoint's real permissions or a stale
   mount. No shard holds the lock past this subshell, so contention is only ever with other shards
   currently in this same race.

   `-w` is a defensive bound against a holder that's genuinely hung, not a way to cap normal
   contention — the lock is scoped to the holder's own subshell fd, so it's released automatically
   the instant that process exits for any reason, including a crash or preemption, meaning there's
   no scenario where it's stuck held forever. It errs generous (300s) rather than tight: an earlier
   90s bound was itself confirmed live to be too short — one shard among just 8 contenders on one
   node still timed out, even though a sibling shard on the same node mounted the same bucket in
   under a second moments earlier. `flock` gives no fairness guarantee among waiters, so real wait
   times under ordinary contention can run well past what any single mount's own duration suggests.

3. **Clear a stale FUSE endpoint.** A mount whose gcsfuse daemon has died -- no longer another
   job's unmount, since none happens, but a crashed daemon still leaves one -- makes every
   subsequent operation on the path (`stat`, `mkdir`, even `flock`) fail with `ENOTCONN`/`EACCES`
   rather than `ENOENT`, so it has to be cleared before the path is touched at all.

4. **`mkdir` + `chown` to the invoking user.** The mountpoint must be writable by whoever runs
   gcsfuse; `fusermount3` refuses otherwise. Deliberately *not* fixed with `sudo gcsfuse`:
   mounting as the invoking user keeps it readable without `-o allow_other`, and podman maps the
   task container's root to this same UID (`wolf/task.py` `--uidmap`), so the task container can
   read it too.

5. **Mount read-only**, if not already mounted:

   ```bash
   timeout -k 60 60 gcsfuse -o ro --implicit-dirs <bucket> /mnt/bucketmounts/<bucket>
   ```

   Two things keep that mount alive past the job that made it:

   - **`200>&-`**: gcsfuse must not inherit the mount-setup lock's fd. The daemon lives for the
     node's lifetime, so an inherited fd would hold the lock forever and every later shard
     would time out in step 2.
   - **The daemon is moved out of the job's cgroup**, into the root cgroup of each controller
     Slurm tracks (`freezer`, `cpuset`, `memory`, `devices`, and the v2 root). Slurm's
     `proctrack/cgroup` kills every process in a job's cgroup when the job ends. Left there, the
     daemon died with its first job and every other shard on the node was left reading a dead
     mount ("Transport endpoint is not connected"). Confirmed live both ways. A side effect:
     gcsfuse's memory is no longer charged against the job's `--mem` request.

   The **whole bucket** is mounted; each input's object path lives in its symlink, so one gcsfuse
   process serves every input from that bucket. Unlike a RODISK there's no cross-node attach race
   to guard — gcsfuse supports many concurrent read-only mounts — so once mounted, every shard
   reads from it fully concurrently for the rest of its run (step 2's lock only serializes getting
   to that point, not any of the actual work). gcsfuse resolves credentials via ADC and does
   **not** read `CLOUDSDK_CONFIG`, so `GOOGLE_APPLICATION_CREDENTIALS` is pointed explicitly at the
   credentials the image stages (`base.py:2001`); without this the authenticating identity
   depends on whatever ADC happens to resolve to.

6. **Reachability check** (`bucketmount_reachability_check()`, `base.py:1435`). Every input
   symlink pointing into this mount must resolve, or `exit 5`. A mounted-but-empty bucket is
   gcsfuse's nastiest failure mode: the mount succeeds, `mountpoint -q` passes, and reads come
   back ENOENT far from the cause. Two ways to land there — the objects were never written, or
   this node already had the bucket mounted from *before* they were and its metadata cache is
   stale.

7. **Register the mount lease** (§7).

8. **Start the heartbeat** (§7).

Steps 6–8 sit **outside** the "mount if not already mounted" conditional, and outside the
fast path. A job landing on a node
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

Teardown undoes only this job's own bookkeeping, in order:

1. `bucketmount_heartbeat_stop()` — stop refreshing customTime first.
2. `bucketmount_lease_release()` — drop this job's `_MOUNTS/` objects.

**No consumer bucket mount is unmounted.** They are node-lifetime (§6) and go away with the VM.
(The read-write *upload* mount is a different thing: the upload script unmounts it before the
task runs, because the unmount is what finalizes its objects, §4.)

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
| `/mnt/bucketmounts/<bucket>` | consumer mount, read-only, lives for the node's lifetime |
| `/mnt/bucketmounts/.<bucket>.mountlock` | mount-setup lock, deliberately outside the mount |
| `/mnt/localize/<bucket>` | read-write upload mount, unmounted before the task runs |
| `${CANINE_JOB_INPUTS}/.bucketmount_leases` | lease URLs this job took |
| `${CANINE_JOB_INPUTS}/.bucketmount_heartbeat_pids` | heartbeat pids |

**In the bucket**

| Object | What |
|---|---|
| `<input_name>/<basename>` | a localized input |
| `_MOUNTS/<host>-<job>-<shard>` | a live consumer's lease |
| `.wolf_claim` | the uploader's claim; its generation is the takeover token, its update time the heartbeat |

**Labels:** `wolf=working` · `wolf=success` · `wolf=stale` (only `success` changes what a job does)

**`exit 5`** anywhere in this path means *requeue this shard on another node* — used for every
give-up, because every cause here is transient or node-local.

**Tests:** `canine/test/test_localizer_bucket_upload_pure.py` (bucket naming, layout, the
claim protocol with a stateful fake gcloud and a 20-job race, upload plan, and
`TestContentEncoding` for routing gzip-encoded objects to the mount),
`canine/test/test_localizer_reachability_pure.py` (reachability, leases, heartbeat),
`canine/test/test_rapid_cache_pure.py`. All pure — no cluster, no GCP credentials.
