# Running the §8.5 localization benchmark

Step-by-step procedure for `benchmark_localization.py` on a mock-up wolF worker node.

Everything in the 794-test unit suite runs against fakes. This measures what a fake cannot,
and settles the `connections` default before it ships.

**Read §0 before creating anything.** Two of the notes there change what you should run
first, and one of them can turn a 90-minute job into a 12-hour one.

---

## 0. Before you start

### What you need

* `gcloud` authenticated, with permission to create instances and disks in the project.
* Quota for one `n1-standard-8` and ~120 GB of `pd-standard` in your zone.
* A test object of ~50 GB, and its md5 (§2 makes one).

### Cost, honestly

| Item | Rough cost |
|---|---|
| `n1-standard-8`, on-demand | ~$0.38/hour |
| `pd-standard`, 54 GB | ~$0.002/GB/day — negligible |
| Ingress **into** GCP | free |
| Egress **out of S3**, if the bucket bills you | ~$0.09/GB → a 5-setting sweep on 50 GB is ~250 GB ≈ **$22** |

Reading from GCS in the same region is free, so **do the connection sweep against a GCS
object** and use a real S3/GDC object only for the one-off correctness runs in §6.3. This
is the single biggest cost lever in the whole procedure.

### The runtime trap

The localization disk is `pd-standard` (`base.py:1002`), whose write throughput is
provisioned **per gigabyte** — roughly 0.12 MB/s per GB. `create_persistent_disk` sizes it
at the object size plus 5%, so ~54 GB for a 50 GB object, which is on the order of
**6-7 MB/s**.

If that holds, one 50 GB download takes over two hours and a 5-setting sweep takes half a
day. **Measure the disk ceiling first (§4) and read the estimate it prints before starting
§6.** You may well want a 10 GB object rather than 50 GB for the pd-standard sweep.

### Use a non-preemptible node

For §4-§6 create the node **without** `--preemptible`. A real preemption mid-measurement
silently ruins a run. §7 is the only step that wants a preemptible node, and it creates its
own.

---

## 1. Create the mock node

```bash
export ZONE=us-central1-a          # match your data's region to avoid egress charges
export PROJECT=$(gcloud config get-value project)
export NODE=pdl-bench

gcloud compute instances create $NODE \
  --zone            $ZONE \
  --machine-type    n1-standard-8 \
  --image-family    slurm-gcp-docker-v3 \
  --image-project   broad-getzlab-workflows \
  --boot-disk-size  50GB \
  --boot-disk-type  pd-standard \
  --scopes          cloud-platform \
  --tags            caninetransientimage
```

Two deliberate choices:

* **`n1-standard-8` is not the worker default.** `gcpTransient.py:52` defaults `worker_type`
  to `n1-highcpu-2` — 2 vCPU, ~4 Gbps, a quarter of the egress ceiling §2 reasons from.
  `n1-standard-8` is what `LocalizeToDisk` requests via `partition` with `--exclusive`
  (`wolF/wolF/localization.py:35`), matching the `8 cpus / 28200 realmemory` entry in
  `slurm_gcp_docker/conf/nodetypes.json`. Benchmarking a default worker tunes the wrong NIC.
* **No `--metadata startup-script`.** The real `worker_startup_script.sh` sources
  `/mnt/nfs/clust_conf/...` and will fail without a controller. We start the container by
  hand in §3 instead.

`--scopes cloud-platform` lets `gcloud` inside the container attach disks and mint the
token Route B needs, straight from the metadata server.

---

## 2. Create the test object

On the node, because generating 50 GB locally and uploading is faster than anything else
and gives you the md5 in the same pass:

```bash
gcloud compute ssh $NODE --zone $ZONE

# on the node
export BUCKET=your-scratch-bucket
export SIZE=$((50 * 1024 * 1024 * 1024))     # 53687091200

openssl enc -aes-256-ctr -pass pass:pdlbench -nosalt < /dev/zero 2>/dev/null \
  | head -c $SIZE \
  | tee >(md5sum > /tmp/bench.md5) \
  | gcloud storage cp - gs://$BUCKET/pdl-bench-50g.bin

cat /tmp/bench.md5      # save this
```

`openssl` as a pseudorandom source runs at GB/s, where `/dev/urandom` would take minutes.
Incompressible data matters: zeros would let transport compression distort the throughput
numbers.

Record for later:

```bash
export URL=https://storage.googleapis.com/$BUCKET/pdl-bench-50g.bin
export MD5=$(cut -d' ' -f1 /tmp/bench.md5)
```

> Using a pre-existing object instead? Get its size and md5 with
> `gcloud storage objects describe gs://B/O --format='value(size,md5_hash)'`. The md5 comes
> back base64 — convert with
> `base64 -d <<< "$B64" | xxd -p`. Objects with `Content-Encoding: gzip` or a
> `componentCount` have no usable md5; pick a different object.

---

## 3. Start the container and copy the script in

The localization script runs as a SLURM job step, and `slurmd` runs **inside** the
slurm_gcp_docker container. That is the context whose `/bin/sh`, tool inventory and mount
table are the real ones, so the benchmark must run there too.

```bash
# on the node
sudo docker run -dti --rm --pid host --network host --privileged \
  -v /dev:/dev \
  --entrypoint /bin/bash --name slurm broadinstitute/slurm_gcp_docker
```

These are `worker_startup_script.sh:60`'s flags minus the NFS and docker-socket mounts,
which need a controller. Note what is **not** here: `/mnt/rwdisks` is not bind-mounted in
the real worker either, which is exactly why the localization disk has to be mounted from
inside the container (§4) and why probing the host tells you nothing.

The script locates the downloader at `../localization/parallel_download.py` relative to
itself, so the two files must keep that layout:

```bash
# from your workstation, in the canine repo
gcloud compute scp --zone $ZONE \
  canine/test/benchmark_localization.py \
  canine/localization/parallel_download.py \
  $NODE:/tmp/

# on the node
mkdir -p /tmp/pdl/test /tmp/pdl/localization
cp /tmp/benchmark_localization.py /tmp/pdl/test/
cp /tmp/parallel_download.py      /tmp/pdl/localization/
sudo docker cp /tmp/pdl slurm:/tmp/pdl
```

Define a shorthand — every later step uses it:

```bash
# on the node
pdl() { sudo docker exec slurm python3 /tmp/pdl/test/benchmark_localization.py "$@"; }
```

Your shell expands `$URL` and friends before `docker exec` sees them, so the variables stay
on the host side and nothing needs `-e`.

### Probe

```bash
pdl probe
```

Confirm before going further:

* **"Running INSIDE a container"** — if it warns instead, you are on the host and every
  answer below it is the wrong machine's.
* **`/bin/sh` → dash.** The image is `ubuntu:22.04`, so this is the shell that rejects the
  `[[ ]]` and process substitution the emitted commands use. This is the empirical
  confirmation of §13.3.
* **`python3` is 3.8** on the current image. `parallel_download.py` is stdlib-only and
  parses under 3.8, so §8.5 is not blocked on the image's 3.8 → 3.14 upgrade.
* **`curl`, `gcloud`, `aws` all present.**

---

## 4. Create the localization disk, and measure its ceiling first

Mirrors `base.py:1000-1060` — same type, same `mkfs` flags, same mount options.

```bash
# on the node
export DISK=canine-bench-$(date +%s)
export DISK_GB=54                    # 1 + 50GB/0.95e9, as create_persistent_disk computes

gcloud compute disks create $DISK \
  --size $DISK_GB"GB" --type pd-standard --zone $ZONE --labels wolf=canine
gcloud compute instances attach-disk $NODE \
  --zone $ZONE --disk $DISK --device-name $DISK

# mount INSIDE the container, as the real localization script does
sudo docker exec slurm bash -c '
  set -eux
  while [ ! -b /dev/disk/by-id/google-'"$DISK"' ]; do sleep 1; done
  mkfs.ext4 -m 0 -E lazy_itable_init=0,lazy_journal_init=0,discard \
    /dev/disk/by-id/google-'"$DISK"'
  mkdir -p /mnt/rwdisks/'"$DISK"'
  mount -o discard,defaults /dev/disk/by-id/google-'"$DISK"' /mnt/rwdisks/'"$DISK"'
'
```

### Measure the write ceiling before committing to a sweep

One minute now can save you eleven hours:

```bash
sudo docker exec slurm bash -c \
  'dd if=/dev/zero of=/mnt/rwdisks/'"$DISK"'/ddtest bs=1M count=4000 \
     oflag=direct conv=fdatasync; rm -f /mnt/rwdisks/'"$DISK"'/ddtest'
```

`oflag=direct` bypasses the page cache, so this is the disk and not RAM. Take the MB/s it
reports and work out the sweep's runtime:

```
one download ≈ 50 GB ÷ (dd MB/s)      →  ×5 settings = total
```

| dd reports | One 50 GB download | 5-setting sweep | What to do |
|---|---|---|---|
| < 50 MB/s | > 17 min | > 1.5 h | **Drop to a 10 GB object** for §6.1. The disk is the limit and a smaller object shows it just as clearly. |
| 50-300 MB/s | 3-17 min | 15-85 min | Proceed as written. |
| > 300 MB/s | < 3 min | < 15 min | Proceed; the NIC may well be the limit. |

Re-run `pdl probe` now that the disk is mounted — it will print the PD type, provisioned
size and the implied per-GB write cap, which should agree with `dd`. If they disagree,
trust `dd`.

---

## 5. Sanity run

Prove the whole path works on something small before spending hours on it:

```bash
pdl sweep --url "$URL" --size $SIZE --md5 "$MD5" \
          --dest-dir /mnt/rwdisks/$DISK --connections 4 --json /tmp/sanity.json
```

It will warn that the object is too small to mean anything if you point it at a small
object — that warning is correct and you should not record those numbers. What you want
here is `md5 = ok` and a non-zero throughput.

---

## 6. The measurements

### 6.1 Connection sweep on the localization disk — the §8.5 headline

```bash
pdl sweep --url "$URL" --size $SIZE --md5 "$MD5" \
          --dest-dir /mnt/rwdisks/$DISK \
          --connections 1 4 8 12 16 \
          --json /tmp/sweep-pd.json
```

`connections 1` is the **legacy path**, not the new code throttled: the downloader declines
at `connections <= 1` and synthesizes `curl -C - -sSL`. That is the baseline the ≥4× claim
is measured against.

Read from the output:

* **speedup** vs. that baseline, and whether it clears 4×;
* **the knee** — the lowest connection count within 5% of the best, which is what the
  default should be (currently 8);
* **peak NIC vs. peak disk write**, which decides whether the network or the PD is the
  limit. §2 predicted the PD; §0 predicts it strongly;
* **peak RSS**, which should be roughly flat across settings and unrelated to object size.

### 6.2 The same object to tmpfs — isolates the NIC

This is the experiment that turns "the disk is probably the limit" into a measurement.
Identical object, identical settings, no disk in the path at all:

```bash
sudo docker exec slurm bash -c 'mkdir -p /dev/shm/pdl'

pdl sweep --url "$URL_12G" --size $SIZE_12G --md5 "$MD5_12G" \
          --dest-dir /dev/shm/pdl \
          --connections 1 4 8 12 16 \
          --json /tmp/sweep-shm.json
```

n1-standard-8 has 28.2 GB of RAM, so make a **separate ~12 GB object** with §2 for this
step rather than reusing the 50 GB one — it will not fit, and the sweep writes one copy at
a time. Omit `--md5` entirely if you would rather skip verification here; passing an empty
string silently disables the check, which is worse than saying so.

Cross-reference the two sweeps:

* **tmpfs much faster than pd-standard** → the disk is the limit. Tuning `connections` for
  `LocalizeToDisk` is close to pointless, and the real lever is the disk `--type`/size in
  `create_persistent_disk`. That is a cost decision, not a code one.
* **tmpfs about the same** → the network or the source is the limit, and the sweep's knee
  is the answer to take forward.

### 6.3 Correctness against the real sources

Once for each source type, at the chosen connection count. This is correctness, not
throughput, so a smaller object is fine — and for S3 this is where egress gets billed, so
keep it small deliberately.

```bash
# S3 (https endpoint or a presigned URL)
pdl sweep --url "$S3_URL" --size $S3_SIZE --md5 "$S3_MD5" \
          --dest-dir /mnt/rwdisks/$DISK --connections 8 --json /tmp/s3.json

# GDC
pdl sweep --url "$GDC_URL" --size $GDC_SIZE --md5 "$GDC_MD5" \
          --dest-dir /mnt/rwdisks/$DISK --connections 8 --json /tmp/gdc.json
```

`md5 = ok` is the whole result. A `BAD` here is a release blocker.

### 6.4 Resume and page-cache loss

```bash
pdl resume --url "$URL" --size $SIZE --md5 "$MD5" \
           --dest-dir /mnt/rwdisks/$DISK --connections 8 \
           --json /tmp/resume.json
```

SIGKILLs the download at 25%, 50% and 75%, then lets it finish. Check:

* **final md5 CORRECT**;
* **refetched overhead** — the claim is that only the uncommitted tail is refetched, so
  this should be far below the whole-chunk-per-kill figure it prints for comparison;
* whether it says **frontier** or **checkpoint fallback**. On ext4 it should say frontier,
  and if so *this is the first time that code path has ever run* — it fails the SEEK_HOLE
  probe on APFS, so every local test used the checkpoint fallback;
* the **punch-hole** section, which is Linux-only and skipped locally. It simulates bytes
  the process wrote being gone because the VM vanished — the case SIGKILL cannot produce.

### 6.5 Route B against real GCS

Route B has never touched real infrastructure — not the auth path, not resumable sessions,
not compose. It needs a bucket mounted in the container so `select_route` sees a non-POSIX
destination. In this deployment those mounts come from the `.rclone*.sh` scripts on NFS, so
on a mock node create one by hand:

```bash
sudo docker exec slurm bash -c \
  'gcsfuse --implicit-dirs '"$BUCKET"' /mnt/bucket || echo "gcsfuse not in the image"'

pdl routeb --url "$URL" --size $SIZE --md5 "$MD5" \
           --gs-url gs://$BUCKET/pdl-routeb-out.bin \
           --mount-dir /mnt/bucket \
           --json /tmp/routeb.json
```

The token source line matters: **metadata server** is what a real worker uses; a **gcloud
fallback** means the metadata path failed and is worth investigating on its own.

If `gcsfuse` is not in the image, Route B cannot be reached this way — note it as unverified
rather than assuming it works.

---

## 7. Forced preemption

Cannot be driven from the VM being preempted, and needs the SLURM requeue path to observe
the resume, so it belongs in a real wolF run:

```bash
pdl preempt        # prints the procedure
```

In short: submit a `LocalizeToDisk`-only task on a preemptible worker, wait for the
`.k9pdl.json` manifest to appear without `.k9pdl.done`, then **delete** the worker from
another machine (delete, not stop — stop is a clean shutdown). Confirm SLURM requeues, the
same disk re-attaches, the log shows `resuming: N/M chunks already complete`, the final md5
is right, and the summed transferred bytes are close to one object rather than two.

The case worth engineering deliberately: kill the worker in the window **between** the
download finishing and the disk being labelled `finished=yes`. The `.k9pdl.done` marker
should make that resume without re-downloading anything.

---

## 8. NFS destination

Everything above measures the `LocalizeToDisk` path. Ordinary inputs localize to the
NFS-mounted job directory instead, which has entirely different throughput characteristics
— the controller's disk plus a network hop — and there is no faithful way to fake it on a
standalone node.

Run §6.1 and §6.4 again on a real wolF cluster with `--dest-dir` pointing into `/mnt/nfs`,
and record it as a separate result. Do not average it with the pd-standard numbers: two
different ceilings averaged together hides both.

---

## 9. Collect and tear down

```bash
# from your workstation
gcloud compute scp --zone $ZONE "$NODE:/tmp/*.json" ./benchmark-results/
```

Teardown — **the disk outlives the instance and keeps billing**:

```bash
gcloud compute instances delete $NODE --zone $ZONE --quiet
gcloud compute disks delete $DISK --zone $ZONE --quiet
gcloud storage rm gs://$BUCKET/pdl-bench-50g.bin gs://$BUCKET/pdl-routeb-out.bin
gcloud compute disks list --filter="name~canine-bench"    # confirm nothing is left
```

---

## What to write down

Record these in `update_localization.md` §13 whatever the outcome — a negative result here
is as useful as a positive one, and more useful than an unmeasured assumption:

| Question | Where it comes from |
|---|---|
| ≥4× speedup vs. single stream? | §6.1 verdict |
| `connections` default (currently 8) | §6.1 knee |
| NIC or disk as the limit | §6.1 peaks, confirmed by §6.2 |
| Is `pd-standard` viable for `LocalizeToDisk`? | §4 `dd` + §6.2 |
| Memory bounded? | §6.1 peak RSS |
| Frontier or checkpoint on ext4? | §6.4 |
| Punch-hole recovery correct? | §6.4 |
| Route B on real GCS, and its token source | §6.5 |
| `/bin/sh` in the container | §3 probe |
| Resume across a real preemption | §7 |
