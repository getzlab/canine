# Running the §8.5 localization benchmark

Step-by-step procedure for `benchmark_localization.py` on a mock-up wolF worker node.

Everything in the 794-test unit suite runs against fakes. This measures what a fake cannot,
and settles the `connections` default before it ships.

**Read §0 before creating anything.** The disk, not the network, is likely to be the
binding constraint once the downloader lands, and §0 changes both what you run first and
what you can expect to get.

---

## 0. Before you start

### What you need

* `gcloud` authenticated, with permission to create instances, disks and snapshots.
  (§4.2 only; snapshots are not needed for the main measurements.)
* Quota for one `n1-standard-8` and ~1 TB of persistent disk in your zone (§4 creates
  several 316 GB disks, though not all at once).
* Test objects of ~12 GB and ~300 GB, and their md5s (§2 makes both).

### Cost, honestly

| Item | Rough cost |
|---|---|
| `n1-standard-8`, on-demand | ~$0.38/hour |
| `pd-standard`, 316 GB, for a few hours | a few cents |
| Ingress **into** GCP | free |
| Egress **out of S3/GDC**, if it bills you | ~$0.09/GB → **one** 300 GB run is ~$27, a 5-setting sweep is ~$135 |

Reading from GCS in the same region is free, so **find the knee against a GCS object** and
touch the real source only for the one-off correctness and confirmation runs. This is by
far the biggest cost lever here, and it is why §6 is ordered the way it is: connection
scaling is a property of the source's per-connection limits, and a free GCS object exhibits
that behaviour well enough to pick a default.

### Read this first: the disk caps you at ~1.8×, and the type is not negotiable

The workload driving this work is **~300 GB BAMs from non-GCS sources, currently taking
upwards of 4 hours**. That is ~21 MB/s, and it is the number to beat.

The localization disk is `pd-standard` (`base.py:1002`), whose throughput is provisioned
**per gigabyte** — GCP documents ~0.12 MB/s/GB for both read and write.
`create_persistent_disk` sizes it at the object size plus 5%, which for 300 GB is **316 GB**:

| Disk type at 316 GB | Write ceiling | Best case for 300 GB | Read-only fan-out |
|---|---|---|---|
| `pd-standard` (today) | ~38 MB/s | **2.2 h** | **unlimited** |
| `pd-balanced` | ~89 MB/s | 0.94 h | max 10 VMs |
| `pd-ssd` | ~152 MB/s | 0.55 h | max 10 VMs |

Today's 21 MB/s is already **55% of the pd-standard ceiling**. So parallel downloading
alone — however perfectly it works — can win at most **1.8×** on this path, taking 4 hours
to about 2.2. The §8.5 target of ≥4× is *unreachable on pd-standard at this object size*,
and no `connections` value changes that.

**`pd-standard` is a deliberate design choice, not an oversight.** It is the only type that
attaches read-only to an unlimited number of VMs; `pd-balanced` and `pd-ssd` cap at 10. The
localization disk's whole purpose is to be re-attached as a rodisk and fanned out across
however many shards consume it, so the faster types are simply not available for the
*published* artifact. Do not "fix" this by changing the type.

That leaves two directions, and this document measures the first:

1. **Disk conversion** — download to a fast single-attach disk, then convert it to
   pd-standard via snapshot, and delete the download disk. The writer is single-attach so
   its type is unconstrained; only the published disk needs the fan-out. Whether this wins
   at all is pure arithmetic on two rates nobody has measured — see §4.2, which settles it
   in about half an hour.
2. **A GCS bucket with read caching**, which sidesteps persistent disks entirely and has no
   fan-out limit at all. Being benchmarked separately on canine's `fuse-localize` branch;
   out of scope here.

Two consequences for how you run this:

* **§4.1 and §4.2 are the most important measurements in this document.** They cost under
  an hour, no egress, and no downloads, and they bound everything §6 can achieve. Do them
  before any download benchmark.
* **Do not sweep at 300 GB.** Five settings × 2.2 h is eleven hours. §6 finds the knee on a
  12 GB object in tmpfs, then does a single confirmation run at full size — about three
  hours instead of eleven.

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

## 2. Create the test objects

You need **two**: a ~12 GB object for the sweeps (§6.1, §6.2) and a ~300 GB one for the
single confirmation run (§6.3). Generate them on the node — it is faster than uploading
from anywhere else, and it gives you the md5 in the same pass:

```bash
gcloud compute ssh $NODE --zone $ZONE

# on the node
export BUCKET=your-scratch-bucket

make_object() {   # make_object <gib> <name>
  local bytes=$(( $1 * 1024 * 1024 * 1024 ))
  openssl enc -aes-256-ctr -pass pass:pdlbench$1 -nosalt < /dev/zero 2>/dev/null \
    | head -c $bytes \
    | tee >(md5sum | cut -d' ' -f1 > /tmp/$2.md5) \
    | gcloud storage cp - gs://$BUCKET/$2
  echo "$2  $bytes bytes  md5 $(cat /tmp/$2.md5)"
}

make_object  12 pdl-bench-12g.bin
make_object 300 pdl-bench-300g.bin      # ~10 min at GB/s, uploads as it generates
```

`openssl` as a pseudorandom source runs at GB/s, where `/dev/urandom` would take an hour at
this size. Incompressible data matters: zeros would let transport compression distort the
throughput numbers. Nothing is ever written to local disk — the data streams straight to
GCS, so the node needs no space for it.

Record for later:

```bash
export SIZE_12G=$((12 * 1024 * 1024 * 1024))
export URL_12G=https://storage.googleapis.com/$BUCKET/pdl-bench-12g.bin
export MD5_12G=$(cat /tmp/pdl-bench-12g.bin.md5)

export SIZE_300G=$((300 * 1024 * 1024 * 1024))
export URL_300G=https://storage.googleapis.com/$BUCKET/pdl-bench-300g.bin
export MD5_300G=$(cat /tmp/pdl-bench-300g.bin.md5)
```

For a realistic end-to-end check, also pick a **real 300 GB BAM** from the source that is
slow today — that is the actual subject of the exercise, and §6.3 against it is the number
worth quoting. Note it will be BGZF, so already incompressible, and its `Content-Length`
is its true size.

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
export DISK_GB=316                   # 1 + 300e9/0.95e9, as create_persistent_disk computes

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

### 4.1 The disk-type comparison — do this before anything else

Ten minutes, no egress, no downloads, and it decides whether the rest of the project can
reach its target. Create all three types at the size `create_persistent_disk` would pick,
and measure each:

```bash
# on the node
for TYPE in pd-standard pd-balanced pd-ssd; do
  D=ddtest-$TYPE
  gcloud compute disks create $D --size 316GB --type $TYPE --zone $ZONE --quiet
  gcloud compute instances attach-disk $NODE --zone $ZONE --disk $D --device-name $D
  sudo docker exec slurm bash -c "
    while [ ! -b /dev/disk/by-id/google-$D ]; do sleep 1; done
    mkfs.ext4 -q -m 0 -E lazy_itable_init=0,lazy_journal_init=0,discard \
      /dev/disk/by-id/google-$D
    mkdir -p /mnt/dd/$D && mount -o discard,defaults /dev/disk/by-id/google-$D /mnt/dd/$D
    echo -n '$TYPE  write: '
    dd if=/dev/zero of=/mnt/dd/$D/f bs=1M count=8000 oflag=direct conv=fdatasync 2>&1 \
      | tail -1
    sync; echo 3 > /proc/sys/vm/drop_caches
    echo -n '$TYPE  read : '
    dd if=/mnt/dd/$D/f of=/dev/null bs=1M iflag=direct 2>&1 | tail -1
    umount /mnt/dd/$D"
  gcloud compute instances detach-disk $NODE --zone $ZONE --disk $D --quiet
  gcloud compute disks delete $D --zone $ZONE --quiet
done
```

`oflag=direct` / `iflag=direct` bypass the page cache, so these are the disk and not RAM.
The read number matters as much as the write one — the localization disk is re-attached
read-only as a rodisk and read by every downstream consumer.

Compare against the predictions and against today's 21 MB/s:

| Measured write | Reading |
|---|---|
| pd-standard ≈ 38 MB/s | Table in §0 confirmed. Parallelism alone tops out at **1.8×**; ≥4× needs a different disk type. This is the expected outcome. |
| pd-standard ≫ 38 MB/s | The per-GB model does not apply here (check whether the disk was created at the size you think). Re-plan using the measured number. |
| pd-balanced ≈ 89 MB/s | ≥4× is reachable *if* the source can be parallelized that far — which §6.2 measures. |

**If pd-standard confirms at ~38 MB/s, stop and decide the disk question before spending
hours on download benchmarks.** The change is one word in `base.py:1002`; the tradeoff is
in §10.

Also re-run `pdl probe` now that a disk is mounted — it prints the PD type, provisioned size
and implied per-GB cap, which should agree with `dd`. If they disagree, trust `dd`.

### 4.1b Oversized pd-standard: the same speed without changing type

pd-standard throughput is provisioned per gigabyte, so the fast disk does not have to be a
different *type* — it can be a bigger pd-standard, which keeps unlimited read-only fan-out.
Confirm the model and find the real per-instance ceiling:

```bash
for GB in 10 50 100 200 316 742 2000; do
  D=ddsize-$GB
  gcloud compute disks create $D --size ${GB}GB --type pd-standard --zone $ZONE --quiet
  gcloud compute instances attach-disk $NODE --zone $ZONE --disk $D --device-name $D
  sudo docker exec slurm bash -c "
    while [ ! -b /dev/disk/by-id/google-$D ]; do sleep 1; done
    echo -n '${GB}GB write: '
    dd if=/dev/zero of=/dev/disk/by-id/google-$D bs=1M count=8000 \
       oflag=direct conv=fdatasync 2>&1 | tail -1
    echo -n '${GB}GB read : '
    dd if=/dev/disk/by-id/google-$D of=/dev/null bs=1M count=8000 iflag=direct 2>&1 | tail -1"
  gcloud compute instances detach-disk $NODE --zone $ZONE --disk $D --quiet
  gcloud compute disks delete $D --zone $ZONE --quiet
done
```

Predicted, at 0.12 MB/s/GB with a ~240 MB/s per-instance ceiling:

| Size | 10 | 50 | 100 | 200 | 316 | 742 | 2000 |
|---|---|---|---|---|---|---|---|
| MB/s | 1.2 | 6 | 12 | 24 | 38 | 89 | 240 |

The large end tells you whether §10's sizing table is real: if 2000 GB gives ~240 MB/s, a
300 GB download takes **21 minutes** rather than 2.2 hours.

**The small end may matter more, and is the reason to include it.** Reference disks are
MB to low tens of GB and are kept for a week or more, so the model predicts a 10 GB
reference disk runs at **1.2 MB/s and 7.5 IOPS** — which would make reading a 3 GB
reference take 42 minutes, on every task that mounts it.

**I doubt that is what happens, and the doubt is worth stating.** If reference loading
really cost 40+ minutes per task, it would be the loudest complaint in the pipeline, and it
is not — the 4-hour BAM download is. That is direct operational evidence that the linear
per-GB model breaks down at small sizes, whether through a throughput floor, burst credits,
or something else. So treat the small-disk row as a **hypothesis this sweep tests**, not as
a finding.

Which way it resolves changes what to do next:

* **small disks really are ~1.2 MB/s** → that is a larger and far cheaper problem than the
  BAM download. Oversizing a reference disk from 10 GB to 200 GB is a 20× speedup for
  **$1.75/week**, and it affects every task that mounts it. Fix that before touching
  localization.
* **small disks perform fine** → the per-GB model has a floor, §10's sizing rule needs no
  floor term, and the reference-disk regime can be left alone.

The catch is cost, and §10 works it through. The short version: oversizing is **not**
justified by download time alone, but it may be justified several times over by consumer
reads, and which of those is true depends on §4.1c.
### 4.1c Read-only fan-out — already measured, no experiment needed

**Settled by the Getz Lab's own testing: read throughput is per-attachment, not shared.**
Speed does not degrade as more VMs attach the disk read-only.

Two consequences, and they point in opposite directions:

**Good news about the current design.** The failure mode I had been worried about does not
exist. There is no scenario where a 316 GB disk is quietly dividing 38 MB/s among fifty
readers, so the rodisk has never been a fan-out bottleneck. Aggregate read bandwidth scales
with the number of consumers.

**And it confirms the assumption behind §10's arithmetic.** Each consumer independently
gets the disk's provisioned rate, so a bigger disk makes *every* reader faster, not just the
localizing writer. Savings scale with `1 + N` — one writer plus N readers — which is exactly
how §10's table is computed.

No measurement required here. Skip to §4.2, or straight to §6 if you accept §10's
conclusion.
### 4.2 Disk conversion — bounded at $0.22, so do not measure it

**Nothing to run here. The arithmetic bounds the prize below the complexity cost whatever
the snapshot rates turn out to be**, which is a better outcome than a measurement: it does
not depend on any GCP behaviour that might be misremembered.

Two facts settled by the Getz Lab independently, both of which I had wrong:

* **A snapshot CAN restore into a smaller disk.** I claimed the opposite and used it to
  reach a conclusion, which was doubly wrong — the claim was false, and this document
  already contained a two-minute test I should have run before relying on it. Snapshots
  capture the data; the restore-floor behaviour I was describing belongs to images.
* **Read throughput is per-attachment, not shared** (§4.1c).

With shrinking available, conversion's best form is: download onto a big fast disk,
snapshot, restore at whatever size you want to publish, delete the big disk. So why does it
still lose?

**Because the published disk should be oversized anyway, and an oversized disk already
writes fast.** Per-attachment reads mean read speed scales with the published size, so
§10's optimum is ~742 GB — and 742 GB already writes at 89 MB/s. Conversion can therefore
only buy the difference between downloading *on the disk you are going to publish* and
downloading on a bigger one:

| Download on | Localization | vs 742 GB direct | Worth |
|---|---|---|---|
| 742 GB (no conversion) | 0.94 h | — | — |
| 1000 GB → restore 742 GB | 0.69 h | 0.24 h | $0.09 |
| 2000 GB → restore 742 GB | 0.35 h | 0.59 h | **$0.22** |

Then subtract the big disk's own cost for that hour (~$0.11 at 2000 GB) and the snapshot
plus hydration time, which is unmeasured and comes straight off the top. **The ceiling is
about eleven cents**, for a three-object state machine, a lazily-hydrating published disk,
and delayed first-availability under contention.

The other variant — convert in order to publish *small* and save storage — is worse still.
It saves $1.12 of retention over 48 h and costs every consumer 1.26 h ($0.48). One or two
consumers make it a loss, and 24-48 h retention implies consumers.

**The general point, which is the useful one:** the same oversizing that consumers need for
reads gives the fast write for free. Once the disk is sized for reads there is nothing left
for conversion to optimise. That is why this is dead on arithmetic rather than on
capability, and why no measurement can revive it.

## 5. Sanity run

Prove the whole path works on something small before spending hours on it:

```bash
pdl sweep --url "$URL_12G" --size $SIZE_12G --md5 "$MD5_12G" \
          --dest-dir /mnt/rwdisks/$DISK --connections 4 --json /tmp/sanity.json
```

It will warn that the object is too small to mean anything if you point it at a small
object — that warning is correct and you should not record those numbers. What you want
here is `md5 = ok` and a non-zero throughput.

---

## 6. The measurements

Ordered cheapest-and-most-informative first. §6.1 and §6.2 together answer the design
questions in about 45 minutes and no source egress; §6.3 is the only full-size run.

### 6.1 Find the knee in tmpfs — no disk in the path

Do this before any disk-destined download. With the disk removed entirely, this measures
what the *source and the NIC* can do, which is the ceiling parallelism could ever reach —
and the knee it finds is the `connections` default.

```bash
sudo docker exec slurm bash -c 'mkdir -p /dev/shm/pdl'

pdl sweep --url "$URL_12G" --size $SIZE_12G --md5 "$MD5_12G" \
          --dest-dir /dev/shm/pdl \
          --connections 1 4 8 12 16 \
          --json /tmp/sweep-shm.json
```

Make a **separate ~12 GB object** with §2 for this — n1-standard-8 has 28.2 GB of RAM and
tmpfs is memory. (Omit `--md5` if you would rather skip verification; passing an empty
string silently disables the check, which is worse than saying so.)

`connections 1` is the **legacy path**, not the new code throttled: the downloader declines
at `connections <= 1` and synthesizes `curl -C - -sSL`. That is the baseline the ≥4× claim
is measured against.

Read from the output:

* **the knee** — the lowest connection count within 5% of the best. This is the default to
  ship (currently 8);
* **peak throughput**, which is the ceiling any disk-destined run is bounded by;
* **peak RSS**, which should be flat across settings and unrelated to object size;
* **md5 ok** at every setting.

Do this against a same-region GCS object first because it is free. If the knee against the
real S3/GDC source might differ — plausible, since per-connection throttling is
source-specific — repeat with a ~12 GB slice of the real source and accept the ~$1 of
egress. That is much cheaper than discovering it at 300 GB.

### 6.2 The same sweep to the localization disk — where the ceiling bites

Same object, same settings, destination changed:

```bash
pdl sweep --url "$URL_12G" --size $SIZE_12G --md5 "$MD5_12G" \
          --dest-dir /mnt/rwdisks/$DISK \
          --connections 1 4 8 12 16 \
          --json /tmp/sweep-pd.json
```

Cross-reference §6.1, §6.2 and the `dd` figure from §4.1:

* **§6.2 plateaus at roughly the `dd` write number, well below §6.1** → the disk is the
  limit, as §0 predicts. `connections` beyond the knee buys nothing on this path, and the
  lever is the disk `--type`, not the downloader.
* **§6.2 ≈ §6.1** → the source or the NIC is the limit and the knee is the whole answer.
* **peak NIC vs. peak disk write** in the output should corroborate whichever it is.

Note the 12 GB object gets a 316 GB disk here, so the per-GB ceiling is the *300 GB* one —
that is deliberate. Sizing the disk to the small object would give a ~13 GB disk at ~1.6 MB/s
and measure a situation that never occurs.

### 6.3 Direct to pd-standard at full size — the primary result

**This is the number everything else is judged against, and it may well be the answer on
its own.** Not a confirmation run: it is the shipping configuration, unchanged except for
the downloader, and every alternative in §4.2 and §10 has to beat *this* by enough to
justify its complexity.

```bash
pdl sweep --url "$URL_300G" --size $SIZE_300G --md5 "$MD5_300G" \
          --dest-dir /mnt/rwdisks/$DISK \
          --connections $KNEE \
          --json /tmp/direct-300g.json
```

Run it against the real slow BAM too, since that is the actual subject:

```bash
pdl sweep --s3-bucket "$BAM_BUCKET" --s3-key "$BAM_KEY" \
          --s3-endpoint-url "$S3_ENDPOINT" \
          --dest-dir /mnt/rwdisks/$DISK --connections $KNEE \
          --json /tmp/direct-300g-realbam.json
```

Expect roughly `300 GB ÷ (the §6.2 plateau)`. Report it as **"4 h → X h"** against today's
behaviour, not as the sweep's internal speedup.

Record alongside it the thing that makes this configuration valuable and the alternatives
expensive: **time to `finished=yes`**. That label is when the data becomes available to
every other workflow waiting on it (§6.7), and on this path it lands the moment the
download completes.

### 6.7 Contention: several workflows localizing the same inputs

The case the existing design is built around, and the one an alternative is most likely to
break. `create_persistent_disk` derives the disk name from a hash of the inputs, so
concurrent workflows needing the same data converge on **one** disk, and the protocol is
carried entirely by that single object:

| State of the disk | What an arriving worker does |
|---|---|
| has label `finished=yes` | mount read-only, no localization at all |
| exists, listed `users` (attached elsewhere) | `exit 5` — requeue and wait for the builder |
| exists, no users, no `finished` | resume building it |
| absent | create it |

`finished=yes` is applied only after localization succeeds (`base.py:1564`, and gated on
`CANINE_JOB_RC -eq 0` in the teardown variant), so it is a single atomic commit on the same
object that holds the work in progress. That identity is what makes the race safe.

Measure it, because the numbers matter for judging alternatives:

```bash
# from your workstation: three workers, same inputs, staggered starts
for i in 1 2 3; do
  wolf ... &     # or three sbatch submissions of the same LocalizeToDisk task
  sleep 120
done
```

Record:

* **time to `finished=yes`** from the first worker's start — this is first-availability, and
  it is what every waiting workflow actually experiences;
* that workers 2 and 3 **exit 5 and park** rather than each starting a duplicate download;
* that exactly **one** disk is created, and no orphans are left
  (`gcloud compute disks list --filter="name~canine-"`);
* what happens if the *builder* is preempted mid-download — another waiter should take over
  and resume, not restart.

The last one is the interesting interaction with this project: resume state now lives in
`.k9pdl.json` **on the disk being built**, so a second worker taking over inherits it and
continues rather than re-downloading. Worth confirming directly.

### 6.4 Correctness against the real sources

Once for each source type, at the chosen connection count. This is correctness, not
throughput, so a smaller object is fine — and where egress is billed, keep it small
deliberately.

**For an S3 source you supply neither `--size` nor `--md5`.** `head-object` already reports
both: the ETag is the md5 for a single-part object and the md5-of-md5s for a multipart one,
and the benchmark derives the size, picks `--check-md5` or `--check-etag --part-length`
accordingly, and recomputes the digest itself to check the result independently.

```bash
# S3 API path -- one `aws` process per chunk, and what runs when presigning is unavailable
pdl sweep --s3-bucket "$S3_BUCKET" --s3-key "$S3_KEY" \
          --dest-dir /mnt/rwdisks/$DISK --connections 8 --json /tmp/s3-api.json

# a store that is not Amazon's: same flags plus the endpoint
pdl sweep --s3-bucket "$S3_BUCKET" --s3-key "$S3_KEY" \
          --s3-endpoint-url "$S3_ENDPOINT" \
          --dest-dir /mnt/rwdisks/$DISK --connections 8 --json /tmp/s3-other.json

# public bucket: no credentials, path-style URL against the endpoint
pdl sweep --s3-bucket "$S3_BUCKET" --s3-key "$S3_KEY" \
          --s3-endpoint-url "$S3_ENDPOINT" --no-sign-request \
          --dest-dir /mnt/rwdisks/$DISK --connections 8 --json /tmp/s3-public.json

# presigned-URL path -- the preferred one, no `aws` per chunk. `probe` prints the URL.
pdl sweep --url "$PRESIGNED_URL" --size $S3_SIZE \
          --dest-dir /mnt/rwdisks/$DISK --connections 8 --json /tmp/s3-presigned.json

# GDC
pdl sweep --url "$GDC_URL" --size $GDC_SIZE --md5 "$GDC_MD5" \
          --dest-dir /mnt/rwdisks/$DISK --connections 8 --json /tmp/gdc.json
```

`hash = ok` is the whole result. A `BAD` here is a release blocker. A `-` means nothing was
verified — check the `verify:` line at the top of the output, because an unverified pass
proves much less than it looks like it does.

Worth running **both** S3 paths where presigning works: they are different code
(`HttpSource` versus `S3ApiSource`, the latter spawning one `aws` process per chunk), and
the throughput gap between them is a real finding — it tells you what the per-chunk process
overhead costs, and therefore how much it matters that presigning keeps working.

### S3-compatible stores that are not Amazon's

canine supports these throughout: `HandleAWSURL`'s `aws_endpoint_url` becomes
`--endpoint-url` on `head-object`, on `presign` and on the per-chunk fallback, and public
objects get a path-style URL built against the endpoint rather than
`bucket.s3.amazonaws.com`. What varies between implementations, and what
`probe --s3-bucket … --s3-key … --s3-endpoint-url …` reports:

| Checked | Why it matters |
|---|---|
| `head-object` succeeds | everything else depends on it; failure usually means credentials, the endpoint, or a missing `--no-sign-request` |
| **ranged GET honoured** | the assumption the entire design rests on. A store or proxy that ignores `Range` and returns the whole body cannot be chunked at all |
| **what the ETag means** | AWS semantics are md5, or md5-of-md5s with `-N`. An opaque ETag is not a reproducible digest, so `check_hash` would fail on *correct* data — localize those inputs with hashing off, or supply an md5 out of band |
| **presign works** | decides whether the fast single-code-path source is available, or whether every chunk pays an `aws` process |

Also note that the §0 cost table assumes AWS egress pricing. For an on-premises or
institutional S3-compatible store, egress may be free or billed entirely differently — in
which case the "sweep against GCS, not the real source" advice is unnecessary caution and
you can sweep against the real store directly. Check before optimizing for a cost you do
not actually pay.

### 6.5 Resume and page-cache loss

```bash
pdl resume --url "$URL_12G" --size $SIZE_12G --md5 "$MD5_12G" \
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

### 6.6 Route B against real GCS

Route B has never touched real infrastructure — not the auth path, not resumable sessions,
not compose. It needs a bucket mounted in the container so `select_route` sees a non-POSIX
destination. In this deployment those mounts come from the `.rclone*.sh` scripts on NFS, so
on a mock node create one by hand:

```bash
sudo docker exec slurm bash -c \
  'gcsfuse --implicit-dirs '"$BUCKET"' /mnt/bucket || echo "gcsfuse not in the image"'

pdl routeb --url "$URL_12G" --size $SIZE_12G --md5 "$MD5_12G" \
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
gcloud storage rm gs://$BUCKET/pdl-bench-12g.bin gs://$BUCKET/pdl-bench-300g.bin \
                  gs://$BUCKET/pdl-routeb-out.bin
gcloud compute disks list --filter="name~canine-bench"    # confirm nothing is left
```

---
## 10. What to do about the disk

`pd-standard` stays — it is the only type with unlimited read-only fan-out, and that is what
the rodisk exists for. But its throughput is provisioned **per gigabyte**, so the speed is
available from a *bigger pd-standard*: no type change, no snapshot, no new failure modes.
This is a sizing question.

Costs below are simply **provisioned size × how long the disk exists**, which is how these
disks are actually billed — there is no snapshot-and-rehydrate step in the current design,
so nothing else enters into it. Retention is **24-48 hours**.

### The saving that is unconditional: localization latency

Localization blocks. Nothing that needs the data proceeds until `finished=yes` lands, and
under contention every parked workflow waits on it (§6.7). So this saving is real regardless
of what the consumers look like:

| Disk | Write | 300 GB takes | Time saved | extra @24h | extra @48h | cost per hour saved @48h |
|---|---|---|---|---|---|---|
| 316 GB (today) | ~38 MB/s | 2.20 h | — | — | — | — |
| **742 GB** | ~89 MB/s | **0.94 h** | 1.26 h | $0.56 | $1.12 | **$0.89/h** |
| 1000 GB | ~120 MB/s | 0.69 h | 1.50 h | $0.90 | $1.80 | $1.20/h |
| 2000 GB | ~240 MB/s | 0.35 h | 1.85 h | $2.21 | $4.43 | $2.39/h |

742 GB is the most efficient point on that last column, and 0.94 h against today's 4 hours
is **4.3×** — clearing the §8.5 target that 316 GB pd-standard cannot reach at all.

### The saving that is conditional: IO-bound consumers only

Reads are per-attachment (§4.1c), so a bigger disk reads faster for every consumer
independently. **But that only helps tasks whose runtime is actually IO-limited.** A
process-bound task computes while it reads and does not care what the disk can do, so for
those the read saving is **zero**. Only genuinely IO-bound consumers count:

| IO-bound consumers | 742 GB net @24h | net @48h |
|---|---|---|
| 0 | −$0.08 | −$0.64 |
| 1 | +$0.40 | −$0.16 |
| 2 | +$0.87 | +$0.31 |
| 5 | +$2.31 | +$1.75 |

So the number that decides whether oversizing is cost-*positive* is not the consumer count
but the **IO-bound** consumer count, which is a property of the pipelines, not of the data.

### Recommendation: 742 GB, framed as buying latency rather than saving money

**Worst case — 48 h retention, not one IO-bound consumer — a 742 GB disk costs $0.64 more
per disk and takes 1.26 hours off a blocking step.** That is the honest floor, and it is
the right way to think about this: it is not a cost optimisation, it is buying pipeline
latency for well under a dollar. Given that the entire premise of this work is that 4 hours
is too long, sixty-four cents for the first 1.26 of those hours is a straightforward trade.
With two or more IO-bound consumers it also pays for itself outright.

Going beyond 742 GB gets steadily worse value — $2.39 per hour saved at 2000 GB versus
$0.89 — so only go bigger if you know a specific input is read by many IO-bound tasks.

### Reference disks are a different regime

Not all these disks are 300 GB localization caches. **Reference disks are MB to low tens of
GB and are kept for a week or more**, and they invert every assumption above:

| | Localization disk | Reference disk |
|---|---|---|
| Size | ~316 GB | 10 GB - low tens |
| Retention | 24-48 h | **a week or more** |
| Predicted throughput | 38 MB/s | **1.2 MB/s at 10 GB** |
| Written | once, by one worker | once, rarely |
| Read | by that workflow's tasks | by **everything**, repeatedly |

Two things follow, in opposite directions.

**Oversizing is nearly free here, in absolute terms.** 10 GB → 200 GB is a 20× throughput
increase for **$1.75/week** per disk. Against a disk that every task mounts, that is
trivially worth it — *if* the small-disk penalty is real (§4.1b tests it; I suspect it is
not, and say why there).

**But the floor multiplies by disk count, which is where it could get expensive.** A 200 GB
floor costs +$1.75/week per disk, so:

| Concurrent reference disks | Extra cost/week |
|---|---|
| 10 | $17 |
| 50 | $87 |
| 100 | $175 |

That is the one number I would need to set a floor responsibly, and I do not have it.

### If it becomes a code change: a floor, not a threshold

My earlier suggestion — `natural_size if natural_size < 50 else natural_size * 2.35` —
was wrong, because it leaves small disks at exactly the size where pd-standard is worst.
A rule with a **floor** covers both regimes:

```python
disk_gb = max(int(natural_size * 2.35), FLOOR_GB)
```

| Data | natural | → with FLOOR=100 | → with FLOOR=200 |
|---|---|---|---|
| 3 GB reference | 10 GB | 100 GB (12 MB/s) | 200 GB (24 MB/s) |
| 10 GB | 11 GB | 100 GB | 200 GB |
| 30 GB | 32 GB | 100 GB | 200 GB |
| 300 GB BAM | 316 GB | 742 GB (89 MB/s) | 742 GB |

Note the multiplier is what governs the large case and the floor governs the small one, so
they can be tuned independently. Do not set `FLOOR_GB` until §4.1b says whether small disks
are actually slow and you know the concurrent disk count — a floor is a standing cost on
every disk, unlike the multiplier, which only bites on the large ones that benefit most.

### Where that leaves the alternatives

* **Conversion** — dead on arithmetic, bounded at about eleven cents. See §4.2; the
  argument does not depend on the consumer profile, because oversizing is justified by
  *write* latency alone and an oversized disk already writes fast.
* **`fuse-localize`** — still the cleanest answer to the underlying problem, and unaffected
  by any of this. Benchmarked separately.

### What to measure

1. **§4.1b**, the size sweep — confirm 742 GB really gives ~89 MB/s before believing any of
   the above. Ten minutes, no egress.
2. **§6.3**, direct to pd-standard at full size — the baseline, and with the downloader it
   may already be enough at 316 GB.
3. **§6.7**, contention — that `finished=yes` timing and the exit-5 parking behave as
   expected, since that is what the latency argument rests on.
4. ~~§4.1c fan-out~~ and ~~§4.2 conversion~~ — both closed, no measurement needed.

---

## What to write down

Record these in `update_localization.md` §13 whatever the outcome — a negative result here
is as useful as a positive one, and more useful than an unmeasured assumption:

| Question | Where it comes from |
|---|---|
| **pd-standard write/read at 316 GB** — the constraint everything else sits under | §4.1 `dd` |
| **Is snapshot conversion viable, or arithmetically dead?** | §4.2 extrapolation |
| Does a snapshot-restored disk read slower than a natively-written one? | §4.2 final `dd` vs §4.1 read |
| `connections` default (currently 8) | §6.1 knee |
| Source/NIC ceiling, independent of any disk | §6.1 peak throughput |
| Disk or network as the limit | §6.2 plateau vs §6.1, and the reported peaks |
| Speedup on the real 300 GB BAM vs. today's 4 h | §6.3 |
| Memory bounded? | §6.1 peak RSS |
| md5 correct from S3 and GDC | §6.4 |
| Frontier or checkpoint on ext4? | §6.5 |
| Punch-hole recovery correct? | §6.5 |
| Route B on real GCS, and its token source | §6.6 |
| `/bin/sh` in the container | §3 probe |
| Resume across a real preemption | §7 |

Report the §6.3 number as **"4 h → X h on the real BAM"**, not as the sweep's internal
speedup. The internal figure is measured against a single stream on the same hardware; the
number that matters to anyone waiting on a pipeline is the one against today's behaviour.
