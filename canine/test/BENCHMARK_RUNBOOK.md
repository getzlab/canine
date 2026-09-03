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
for GB in 316 742 2000; do
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

Predicted ~38 / ~89 / ~240 MB/s, the last being the per-instance ceiling rather than
`0.12 × 2000`. If 2000 GB really gives ~240 MB/s, a 300 GB download takes **21 minutes**
instead of 2.2 hours — a 6.3× win with no type change and no snapshot machinery at all.

The catch is cost, and §10 works it through. The short version: oversizing is **not**
justified by download time alone, but it may be justified several times over by consumer
reads, and which of those is true depends on §4.1c.

### 4.1c Does read-only fan-out share the disk's throughput?

A localization disk is re-attached read-only to many VMs. Two possible models:

* **shared** — the disk has one provisioned rate and N readers split it. A 316 GB disk with
  20 readers would give each 1.9 MB/s;
* **per-attachment** — each reader gets up to the disk's provisioned rate, so 38 MB/s each.

Note what this does *not* decide: oversizing speeds up reads either way, because the
per-GB provisioning is a property of the disk. (An earlier version of this section had that
wrong.) What §4.1c decides is **how bad the status quo is** — under the shared model,
today's 316 GB disks are a severe bottleneck for exactly the fan-out they exist to provide,
which would be a bigger finding than anything else in this document.

```bash
# one RO disk, several readers at the same time
gcloud compute disks create fanout-test --size 316GB --type pd-standard --zone $ZONE --quiet
# ...write some data to it first, then detach and:
for i in 1 2 3; do
  gcloud compute instances create fanout-$i --zone $ZONE --machine-type n1-standard-8 \
    --image-family slurm-gcp-docker-v3 --image-project broad-getzlab-workflows --quiet
  gcloud compute instances attach-disk fanout-$i --zone $ZONE \
    --disk fanout-test --device-name d --mode ro
done

# measure ONE reader alone, then all three simultaneously, and compare aggregate
gcloud compute ssh fanout-1 --zone $ZONE --command \
  'sudo dd if=/dev/disk/by-id/google-d of=/dev/null bs=1M count=4000 iflag=direct'
# then launch all three at once and sum the reported rates
```

* **aggregate ≈ single-reader rate** → throughput is shared per-disk. Oversizing benefits
  every consumer, and §10's cost case is strong.
* **aggregate ≈ 3× single** → per-attachment. Oversizing helps localization only, and the
  8-20 hour break-even in §10 is the whole story.

### 4.2 Does the disk-conversion idea actually win?

**First, a constraint that may rule out the shrink step entirely.** GCP requires a disk
created from a snapshot to be **at least as large as the disk the snapshot came from**. If
that holds, you cannot download onto a large fast disk, snapshot it, and restore into a
small 316 GB one — the restore floor is the original size. Verify it in two minutes before
designing anything around it:

```bash
gcloud compute disks create shrink-src --size 742GB --type pd-standard --zone $ZONE --quiet
gcloud compute snapshots create shrink-snap --source-disk shrink-src \
  --source-disk-zone $ZONE --quiet
# expected to FAIL:
gcloud compute disks create shrink-dst --source-snapshot shrink-snap \
  --size 316GB --type pd-standard --zone $ZONE
```

If it fails, then the only ways to fill a small pd-standard are a normal write — capped at
its own 38 MB/s, which is the problem we started with — or a same-size-or-larger restore.
**Conversion cannot shrink, so "download fast, publish small" is not available**, and the
choice becomes: accept 2.2 h, or keep the oversized disk and pay for it (§10).

That leaves conversion useful only for changing *type* while keeping size, e.g. pd-balanced
742 GB → pd-standard 742 GB. Which is worth measuring, but note it no longer saves any disk
cost — so it has to justify itself purely on the fan-out limit.

**Then the arithmetic, because it rules out the obvious version too.**

Any path that ends by *writing* 300 GB onto a pd-standard disk is bounded by that disk's
~38 MB/s, so:

```
download → pd-balanced (0.94 h), then copy → pd-standard (2.2 h)   = 3.1 h
download → pd-standard directly                                    = 2.2 h
```

A plain copy is **worse than doing nothing**. The conversion can only win if the published
disk is filled by a mechanism that is *not* subject to its own provisioned write throughput
— which means snapshot restore, where GCP hydrates from Cloud Storage on its own
infrastructure rather than through the guest.

So the whole idea reduces to two rates nobody has measured:

| Rate | Why it might beat the guest ceiling | If it doesn't |
|---|---|---|
| **snapshot creation** from the fast disk | reads happen inside GCP, not through the VM | ≥0.94 h, and the budget is blown |
| (an **image** instead would track the whole provisioned disk, not the data — strictly worse for an oversized source) | | |
| **restore + hydration** onto pd-standard | writes happen inside GCP, not through the VM | ≥2.2 h, and conversion is pointless |

The budget is tight: download-to-fast-disk already spends 0.94 h of the 2.2 h baseline, so
**snapshot + restore must together finish in under ~1.26 h just to break even**, and in
about 0.1 h to reach the ≥4× target. Measure it:

**Snapshots and images are not the same thing, and the difference is material here.**

| | Captures | Billed on | Relevance |
|---|---|---|---|
| **Snapshot** | the data on the disk | **used** data (~300 GB) | the right primitive for conversion |
| **Image** | the entire disk | the **whole provisioned** disk (742 GB, or 2000 GB) | 2.5-6.7× more storage for an oversized source |

This matters precisely because the fast source disk is *oversized*. A snapshot of a 2000 GB
disk holding 300 GB bills for ~300 GB; an image of the same disk bills for 2000 GB. Anyone
reaching for `gcloud compute images create` here would multiply the intermediate's storage
cost by nearly seven for no benefit, and the time to create it would track the full disk
rather than the data.

Measure both if you evaluate this at all, and do not assume the snapshot figures carry over
to images — the extrapolation below is only valid for snapshots.

```bash
# on the node. 100 GB of data, extrapolate x3 for 300 GB.
#
# The extrapolation assumes snapshot TIME tracks used blocks, the way snapshot BILLING
# does. That is an inference, not a documented guarantee -- which is the point of
# measuring it rather than reasoning about it. It does NOT hold for an image, whose work
# tracks the whole provisioned disk.
export SRC=conv-src DST=conv-dst SNAP=conv-snap

gcloud compute disks create $SRC --size 316GB --type pd-balanced --zone $ZONE --quiet
gcloud compute instances attach-disk $NODE --zone $ZONE --disk $SRC --device-name $SRC
sudo docker exec slurm bash -c "
  while [ ! -b /dev/disk/by-id/google-$SRC ]; do sleep 1; done
  dd if=/dev/zero of=/dev/disk/by-id/google-$SRC bs=1M count=100000 oflag=direct"

echo '=== snapshot create'
time gcloud compute snapshots create $SNAP --source-disk $SRC --source-disk-zone $ZONE --quiet

echo '=== disk create from snapshot (returns fast; hydration is lazy)'
time gcloud compute disks create $DST --source-snapshot $SNAP \
       --type pd-standard --size 316GB --zone $ZONE --quiet
gcloud compute instances attach-disk $NODE --zone $ZONE --disk $DST --device-name $DST

echo '=== full sequential read: this is BOTH the hydration rate and what consumers see'
sudo docker exec slurm bash -c "
  while [ ! -b /dev/disk/by-id/google-$DST ]; do sleep 1; done
  dd if=/dev/disk/by-id/google-$DST of=/dev/null bs=1M count=100000 iflag=direct"
```

Multiply the snapshot and read times by 3 for the 300 GB case, and add the 0.94 h download:

| Extrapolated snapshot + hydration | Verdict |
|---|---|
| < 0.3 h | **Strong win.** ~1.2 h total vs 2.2 h baseline, and worth building. |
| 0.3 - 1.2 h | Marginal. 1.3-2.1 h total for a lot of new machinery and new preemption states. |
| > 1.2 h | **Dead.** Slower than writing straight to pd-standard. |

### The second-order risk, which the arithmetic hides

A disk created from a snapshot is hydrated **lazily**: it is usable immediately, but reads
of not-yet-restored blocks fetch from the snapshot and are slower until restore completes.
The last `dd` above measures exactly that, and it is what *every downstream consumer* would
experience.

So conversion can succeed at its stated goal and still lose overall, by moving cost from one
localization onto every task that reads the rodisk — which is the wrong direction, since
unlimited fan-out is the entire reason pd-standard is there. **If the final read is
materially slower than the ~38 MB/s a natively-written pd-standard disk gives, that is a
finding against the approach even if the snapshot rates look good.** Compare it against the
§4.1 pd-standard read number, which is the honest baseline.

---

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

`pd-standard` stays — it is the only type with unlimited read-only fan-out, and that is
what the rodisk exists for. But its throughput is provisioned **per gigabyte**, so the same
speed is available from a *bigger pd-standard* with no type change and no snapshot
machinery. That makes this a sizing question, and sizing is a cost question.

**Cache disks are retained 24-48 hours**, which is the number that decides it.

### Localization alone does not justify oversizing

0.12 MB/s/GB, $0.04/GB/month, n1-standard-8 at $0.379/h, 300 GB object:

| Disk | Write | 300 GB takes | VM time saved | extra disk @24h | net @24h | extra @48h | net @48h |
|---|---|---|---|---|---|---|---|
| 316 GB (today) | ~38 MB/s | 2.20 h | — | — | — | — | — |
| 742 GB | ~89 MB/s | 0.94 h | $0.48 | +$0.56 | **−$0.08** | +$1.12 | **−$0.64** |
| 1000 GB | ~120 MB/s | 0.69 h | $0.57 | +$0.90 | **−$0.33** | +$1.80 | **−$1.23** |
| 2000 GB | ~240 MB/s | 0.35 h | $0.70 | +$2.21 | **−$1.51** | +$4.43 | **−$3.73** |

Every row is a net loss. The download is a one-off; the disk bill runs for two days.

### The consumers justify it, and the bar is low

Each task that reads the whole object saves the difference in read time — the same per-GB
rate applies to reads:

| Disk | One consumer reading 300 GB | saves vs 316 GB |
|---|---|---|
| 316 GB | 2.20 h | — |
| 742 GB | 0.94 h | $0.48 of its own VM time |
| 2000 GB | 0.35 h | $0.70 |

Consumers needed to break even:

| Disk | @24h retention | @48h retention |
|---|---|---|
| 742 GB | **1.2** | **2.3** |
| 1000 GB | 1.6 | 3.2 |
| 2000 GB | 3.2 | 6.3 |

**And retention is itself evidence of reuse.** A disk is kept for 48 hours because
something is still reading it — otherwise it would be deleted. So the case with the highest
storage cost is precisely the case with the most consumers paying for it. The two columns
are correlated, not independent.

### Recommendation: oversize modestly, around 2.35×

**742 GB for a 300 GB object** looks like the sweet spot:

* ~89 MB/s, so 300 GB in **0.94 h** — a **4.3× improvement on today's 4 hours**, which
  clears §8.5's target that pd-standard at 316 GB cannot;
* breaks even at 1-2 consumers, and with 24-48 h retention there are almost certainly more;
* unlimited fan-out preserved, no type change, no snapshot, no new state machine;
* $29.68/month — within 6% of what a 316 GB `pd-balanced` would have cost ($31.60). It
  delivers pd-balanced speed at pd-balanced price *without* the 10-VM cap.

2000 GB is harder to justify: it needs 3-6 consumers and costs $4.43 extra at 48 h for
another 0.6 h of localization.

**One subtlety if this becomes a code change.** Sizing for a throughput target rather than
for the data — `disk_gb = target_mbps / 0.12` — over-provisions absurdly for small inputs:
a 10 GB object wanting 89 MB/s would get a 742 GB disk, 74× oversized. So it has to be a
*multiplier with a floor and a ceiling*, or applied only above a size threshold. Something
like `disk_gb = natural_size if natural_size < 50 else natural_size * 2.35`. The 300 GB BAM
case is what matters here, but the same formula runs for every input.

### A gap in the model above: the intermediate artifact

Everything in this section prices **disks and VM time only**. It never priced the snapshot
or image that conversion would create, which is an omission that matters given how it
interacts with oversizing.

Approximate rates — verify these, since they move regionally and I have already been wrong
once in this area:

| Artifact | Billed on | 300 GB of data on a 742 GB disk, 24 h |
|---|---|---|
| `pd-standard` disk | provisioned size | 742 GB → ~$0.98 |
| **Snapshot** | **used data** | ~300 GB → ~$0.26 |
| **Image** | **whole provisioned disk** | 742 GB → materially more than the snapshot |

Two consequences:

**Conversion must use snapshots, not images.** With an oversized source the difference is
roughly the oversizing factor — 2.5× at 742 GB, 6.7× at 2000 GB.

**And a snapshot is cheaper storage than a disk for a retained artifact.** That is worth
noticing given the 24-48 h retention: holding ~300 GB of snapshot costs less than holding a
316 GB pd-standard disk, because one is billed on data and the other on provisioned size.
A design that snapshots the disk and deletes it, restoring on demand, would be *cheaper to
retain*.

I am not proposing that, for two reasons that are already established above: restoring
cannot shrink below the source disk's size (§4.2), and a restored disk hydrates lazily so
the first consumer pays for it. But it does mean the retention cost driving §10's
arithmetic is not fixed — it is a consequence of retaining a *disk* specifically, and worth
keeping in view if retention ever becomes the dominant cost.

### Where that leaves the alternatives

* **Conversion** — now clearly the wrong tool. It cannot shrink (§4.2), so it cannot save
  the storage cost that is the only argument against oversizing; it delays
  first-availability under contention; and it replaces a single-object commit protocol with
  a three-object state machine. Oversizing gets the same speed for the same money with none
  of that.
* **`fuse-localize`** — still the cleanest answer to the underlying problem, and unaffected
  by any of this. Benchmarked separately.

### What to measure to confirm

1. **§6.3**, direct to 316 GB pd-standard: the honest baseline, and it may be enough.
2. **§4.1b**, the size sweep: confirm 742 GB really gives ~89 MB/s before believing the table.
3. **§4.1c**, fan-out: if throughput is shared, today's disks are throttling consumers badly
   and oversizing is urgent rather than optional.
4. **The one number I cannot get from here**: how many tasks typically read one cache disk.
   That is a workflow question, and per the tables above it is what actually decides the
   sizing. Two or more, and 742 GB pays for itself.

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
