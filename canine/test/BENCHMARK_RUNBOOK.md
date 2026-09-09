# Running the §8.5 localization benchmark

Step-by-step procedure for `benchmark_localization.py` on a mock-up wolF worker node.

Everything in the 794-test unit suite runs against fakes. This measures what a fake cannot,
and settles the `connections` default before it ships.

**Read §0 before creating anything.** It explains why §4.1's ten-minute `dd` comes first:
whether the persistent disk is a real constraint on this path is unsettled, and that answer
underpins every other number here. §4 is measurement, §10 is the cost decision that
consumes it — run all of §4 regardless of what you expect §10 to conclude.

---

## 0. Before you start

### What you need

* `gcloud` authenticated, with permission to create instances, disks and snapshots.
  (§4.2 only; snapshots are not needed for the main measurements.)
* Both `gcloud auth login` **and** `gcloud auth application-default login` run **on the
  node** — see §2. The second is what `gcsfuse` and the client libraries read, and §3
  mounts the file it writes into the container.
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

### Read this first: measure the disk before believing anything about it

The workload driving this work is **~300 GB BAMs from non-GCS sources, currently taking
upwards of 4 hours**. That is ~21 MB/s, and it is the number to beat.

**Earlier revisions of this document claimed the disk caps the achievable speedup at ~1.8×.
That claim is retracted.** It came from applying GCP's per-gigabyte pd-standard throughput
figures (~0.12 MB/s/GB, so ~38 MB/s at the 316 GB disk `create_persistent_disk` provisions)
to the localization write, and concluding that today's 21 MB/s was already 55% of a hard
ceiling.

The Getz Lab's evidence points the other way: **the throughput limit that actually bites is
on the transfer, not on read/write to a disk attached to a VM.** Reference disks are 10 GB
and would be unusable at the 1.2 MB/s the per-GB model predicts for them — and they are not.

If that is right, then:

* 21 MB/s is **source-bound, not disk-bound** — a single-stream limitation, which is
  precisely what parallel ranged GETs fix;
* the downloader has **full headroom**, and ≥4× is available rather than arithmetically
  impossible;
* **§10's entire sizing discussion is moot.** Oversizing the disk would buy nothing,
  because the disk was never the constraint.

That is the best available outcome, and it is the hypothesis to test first.

**So: run §4.1's `dd` before drawing any conclusion, and read it as the gate.**

| §4.1 `dd` on a 316 GB pd-standard | What it means |
|---|---|
| **≫ 38 MB/s** (say 100 MB/s+) | The per-GB model does not describe this path. The disk is not the localization bottleneck, today's 21 MB/s is source-bound, and the downloader has full headroom. **This is what the evidence predicts.** |
| **≈ 38 MB/s** | The per-GB model holds. The disk is close to binding, and the achievable localization time — hence the VM-hours in §10's cost model — is set by disk size. |

**Run the whole of §4 either way.** Earlier revisions told you to skip parts of it once the
economics looked settled, which was wrong twice over: the measurements are what *validate*
the model every cost estimate rests on, and a benchmark exists to find out what is true, not
to confirm a decision already taken. §4 is measurement; §10 is the decision that consumes
it. Keeping them separate is the point.

One thing that is *not* conditional: **`pd-standard` is required, not an oversight.** It is
the only type that attaches read-only to an unlimited number of VMs; `pd-balanced` and
`pd-ssd` cap at 10, and unlimited fan-out is what the rodisk exists to provide. Whatever
§4.1 says, do not "fix" anything by changing the type.

Two notes on running order:

* **§4.1 first, always.** Ten minutes, no egress, no downloads, and every cost estimate in
  §10 is derived from its result.
* **Do not sweep at 300 GB.** Five settings would be hours. §6 finds the knee on a 12 GB
  object in tmpfs, then does one full-size run.

### Use a non-preemptible node

For §4-§6 create the node **without** `--preemptible`. A real preemption mid-measurement
silently ruins a run. §7 is the only step that wants a preemptible node, and it creates its
own.

---

## 1. Create the mock node

```bash
export ZONE=us-central1-a          # match your data's region to avoid egress charges
export PROJECT=your-project        # or: $(gcloud config get-value project)
export NODE=pdl-bench

# `gcloud config get-value project` prints "(unset)" rather than failing, which would
# turn every command below into a confusing error. Check it before creating anything.
case "$PROJECT" in ""|"(unset)") echo "PROJECT is not set" >&2; return 2>/dev/null || exit 1;; esac

gcloud compute instances create $NODE \
  --project         $PROJECT \
  --zone            $ZONE \
  --machine-type    n1-standard-8 \
  --image-family    slurm-gcp-docker-v3 \
  --image-project   broad-getzlab-workflows \
  --boot-disk-size  50GB \
  --boot-disk-type  pd-standard \
  --scopes          cloud-platform \
  --tags            caninetransientimage
```

**`--project` is on every `gcloud compute` command in this document, deliberately.** §9
deletes an instance, its disks and the test objects; if the active configuration points
somewhere else those commands either fail confusingly or, worse, find something with a
matching name to delete. `--image-project` is separate and stays
`broad-getzlab-workflows` — that is where the worker image lives, not where your node
goes.

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
token the bucket-compose route needs, straight from the metadata server.

---

## 2. Create the test objects

You need **two**: a ~12 GB object for the sweeps (§6.1, §6.2) and a ~300 GB one for the
§6.3 confirmation run. Both are generated on the node, which is faster than uploading from
anywhere else and yields the md5 in the same pass.

Everything from here to §8 runs on the node, so set that shell up first.

### Log in, and set the node's shell up

`$PROJECT`, `$ZONE` and `$NODE` were exported on your **workstation**. Logging in gives
you a fresh shell on the VM where none of them exist — and most of §2 and §4 runs there,
with `gcloud` commands that reference all three. Re-establish them from the metadata
server rather than retyping, so they cannot disagree with the node you are actually on:

```bash
# from your workstation, where these are still set
gcloud compute ssh $NODE --project $PROJECT --zone $ZONE
```

```bash
# on the node
md() { curl -s -H Metadata-Flavor:Google \
       "http://metadata.google.internal/computeMetadata/v1/$1"; }
export PROJECT=$(md project/project-id)
export NODE=$(md instance/name)
export ZONE=$(basename "$(md instance/zone)")
export BUCKET=your-scratch-bucket

echo "$PROJECT / $ZONE / $NODE"      # sanity-check before creating anything
```

**Then start tmux, before anything long.** §2 generates a 300 GB object and §6.3 runs a
multi-hour download; both die with the ssh session if they are in the foreground of it.
The container survives a disconnect — it is a daemon — but the `pdl` wrapper and every
`gcloud` command are foreground processes in your shell.

```bash
# on the node
if ! command -v tmux >/dev/null; then
  sudo apt-get update -qq && sudo apt-get install -y tmux
fi
tmux new -s bench
```

Export the variables **before** starting tmux, or re-export them inside it: a session you
attach to later keeps the environment of the shell that created it, not of the shell
attaching. To get back after a disconnect:

```bash
gcloud compute ssh $NODE --project $PROJECT --zone $ZONE --command 'tmux attach -t bench'
# or, once logged in:  tmux attach -t bench
```

`Ctrl-b d` detaches and leaves the work running.

### Authenticate

```bash
# on the node -- one browser round-trip
gcloud auth login --no-launch-browser --update-adc

# only if ADC then warns that no quota project is set
gcloud auth application-default set-quota-project $PROJECT
```

**`--update-adc` is the flag that matters.** Authenticating the `gcloud` CLI and writing
`application_default_credentials.json` are separate things: the CLI reads its own
credential store, while client libraries and `gcsfuse` read the ADC file. Without
`--update-adc` you would need `gcloud auth application-default login` as a second browser
round-trip — and if you skip ADC altogether, §6.6 fails to mount a bucket while every
`gcloud` command appears to work fine.

`--no-launch-browser` because there is no browser on the VM: it prints a URL to open
locally and a code to paste back.

Note that this puts **user** credentials into ADC rather than a service account's. That is
what production does too — `docker_copy_gcloud_credentials.sh` propagates user credentials
over NFS (§3) — so it is the identity worth testing with.

**Why authenticate at all**, when the node already has a service account? Because that is
the default compute SA, and your scratch bucket and source objects are likely reachable by
*you* rather than by it. If the SA does have access, skip this — but check rather than
assume, since the failure otherwise surfaces as an opaque 403 partway through a 300 GB
transfer.

### Create the objects

```bash
# on the node, inside tmux -- the 300 GB object takes ~10 minutes
make_object() {   # make_object <gib> <name>
  local bytes=$(( $1 * 1024 * 1024 * 1024 ))
  openssl enc -aes-256-ctr -pass pass:pdlbench$1 -nosalt < /dev/zero 2>/dev/null \
    | head -c $bytes \
    | tee >(md5sum | cut -d' ' -f1 > /tmp/$2.md5) \
    | gcloud storage cp - gs://$BUCKET/$2
  echo "$2  $bytes bytes  md5 $(cat /tmp/$2.md5)"
}

make_object  12 pdl-bench-12g.bin
make_object 300 pdl-bench-300g.bin      # ~90 min -- measured, see below
```

`openssl` as a pseudorandom source runs at GB/s, where `/dev/urandom` would take an hour at
this size. Incompressible data matters: zeros would let transport compression distort the
throughput numbers. Nothing is ever written to local disk — the data streams straight to
GCS, so the node needs no space for it.

#### Measured: ~57-59 MiB/s, so budget ~90 minutes for the 300 GB object

| Object | Rate | Elapsed |
|---|---|---|
| 12 GiB | 59.4 MiB/s | ~3.5 min |
| 300 GiB | 57.1 MiB/s | **~90 min** |

(Bucket in the same region as the node, different project.) An earlier draft of this
document guessed "~10 min at GB/s" — wrong by 9×. Run it inside tmux.

**What the number means, and what it does not.** The two rates differ by 4% across a 25×
size difference, so this is a stable plateau rather than a warm-up effect. It is also only
**3% of the ~2 GB/s the NIC is supposed to do**, and about 2.9× today's 21 MB/s BAM
download.

**Three candidates were possible; two are now measured out.**

| Candidate | Result | Verdict |
|---|---|---|
| the generating pipeline | `openssl \| head \| tee >(md5sum)` → **408 MiB/s** (4 GiB in 10.0 s) | not the limit — 6.2× headroom |
| the 50 GB boot disk, via stdin staging | **0 MiB written** during a 4096 MiB upload | not in the path at all |
| the NIC | 69 MB/s is **3.4%** of ~2 GB/s | not the limit |

Note the control pipeline included a full `md5sum` pass the upload path does *not* have,
and still ran 6.2× faster — so the headroom is a lower bound. **By elimination, ~57-66
MiB/s is the single-stream rate**, and today's 21 MB/s BAM download is a third of even
that, presumably source-side per-connection throttling.

**One confound survives**, and §6.1 settles it for free: `gcloud storage` is Python and
hashes crc32c inline, so some of the 60 MB/s may be its own overhead rather than the
stream. §6.1's `connections 1` row is `curl`, not gcloud —

* **curl also lands near 60 MB/s** → the stream is genuinely the limit, and the whole
  premise of this work is confirmed on measured ground;
* **curl reaches 200+ MB/s** → gcloud's Python was the limit, the real single-stream
  baseline is much higher, and the speedup available from parallelism is correspondingly
  smaller.

Either way, record it: the single-stream number is the denominator of every speedup figure
this benchmark produces.

The commands that produced the table above, for reference:

```bash
# on the node. 1: the pipeline with the upload removed -- generator ceiling only
time { openssl enc -aes-256-ctr -pass pass:ctl -nosalt < /dev/zero 2>/dev/null \
       | head -c $((4 * 1024 * 1024 * 1024)) \
       | tee >(md5sum > /dev/null) > /dev/null; }

# 2: does an upload actually touch the boot disk? Compare sectors written before/after.
BOOT=$(lsblk -no PKNAME "$(findmnt -no SOURCE /)" 2>/dev/null || echo sda)
before=$(awk -v d="$BOOT" '$3==d {print $10}' /proc/diskstats)
openssl enc -aes-256-ctr -pass pass:ctl2 -nosalt < /dev/zero 2>/dev/null \
  | head -c $((4 * 1024 * 1024 * 1024)) \
  | gcloud storage cp - gs://$BUCKET/disktest.bin
after=$(awk -v d="$BOOT" '$3==d {print $10}' /proc/diskstats)
echo "boot disk wrote $(( (after - before) * 512 / 1024 / 1024 )) MiB during a 4096 MiB upload"
gcloud storage rm gs://$BUCKET/disktest.bin
```

Roughly 4 GiB written means gcloud is staging through the boot disk and the rate is a disk
measurement. Near zero means it is streaming, and ~57 MiB/s **is** the single-stream rate
to GCS in-region — in which case it becomes the baseline §6.1's `connections 1` row should
land near, and anything substantially above it at higher connection counts is this
project's thesis confirmed on measured ground.

**None of this affects the actual localization measurements** in any case. The downloader
`pwrite`s straight into the destination on the localization disk and `verify()` reads back
from the same place; §6.1 writes to tmpfs. The boot disk was only ever in the path for
generating the test objects — and the 0 MiB result above says it was not even there.

Worth being precise about the configuration, since it is what makes the result usable: the
0 MiB was measured on the **50 GB** boot disk §1 specifies, not on an enlarged one. So
there is no gap between what was tested and what the runbook tells you to create, and no
reason to size it up.

#### Faster alternative: compose the big object server-side

90 minutes of VM time is avoidable. Upload the 12 GiB object once, then have GCS build the
300 GiB one from 25 copies of it — a metadata operation that transfers no data:

```bash
for i in $(seq 1 25); do
  gcloud storage cp gs://$BUCKET/pdl-bench-12g.bin gs://$BUCKET/part-$i.bin &
done; wait                          # server-side copies, also no data transfer
gcloud storage compose $(for i in $(seq 1 25); do echo gs://$BUCKET/part-$i.bin; done) \
  gs://$BUCKET/pdl-bench-300g.bin
```

**The tradeoff is verification.** A composite object has no `md5Hash` — only crc32c — so
the benchmark will report the 300 GB runs as unverified. That is acceptable *if* §6.4 and
§6.3's real-BAM run carry the correctness load, which they do: the real BAM has a genuine
ETag. Use compose for throughput, the real BAM for correctness, and do not let the
convenient object silently become the only thing you tested.

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
  -v $HOME/.config/gcloud:/root/.config/gcloud:ro \
  -v $HOME/.aws:/root/.aws:ro \
  --entrypoint /bin/bash --name slurm broadinstitute/slurm_gcp_docker
```

These are `worker_startup_script.sh:60`'s flags minus the NFS and docker-socket mounts,
which need a controller — **plus a credentials mount, which replaces a step we are
skipping.**

The real worker runs `docker_copy_gcloud_credentials.sh`, which copies gcloud credentials
out of `/mnt/nfs/credentials/gcloud/` into the container: in production the container
authenticates with **user credentials propagated over NFS**, not with the node's service
account. Starting the container by hand bypasses that entirely, so without the mount above
it has no gcloud configuration at all — `gcloud auth print-access-token` fails and only the
metadata server answers.

Mounting `~/.config/gcloud` read-only reaches the same end state as that script, and it is
why §2's `application-default login` must happen **before** this step: the file it writes
is what gets mounted.

`~/.aws` is mounted for the same reason. The measurements run inside the container via
`docker exec`, so the `aws` CLI needs credentials *there*; a copy on the node alone is
invisible to it.

**Put them at the canonical path and nothing has to carry them.** Both `aws` and the
benchmark find `~/.aws/credentials` on their own, so no key ever appears in a command
line — where it would be visible in `ps` to every user on the box and recorded in shell
history. That matters more when the keys are issued by someone else and you cannot rotate
them at will.

```bash
# from your workstation
gcloud compute ssh $NODE --project $PROJECT --zone $ZONE --command 'mkdir -m 700 -p ~/.aws'
gcloud compute scp --project $PROJECT --zone $ZONE \
  ~/.aws/credentials "$NODE:~/.aws/credentials"
gcloud compute ssh $NODE --project $PROJECT --zone $ZONE --command 'chmod 600 ~/.aws/credentials'
```

The endpoint can live there too, which keeps it off command lines as well:

```bash
# on the node -- optional; --s3-endpoint-url overrides it
cat > ~/.aws/config <<'EOF'
[default]
endpoint_url = https://your-object-store.example.org
EOF
```

`probe` reports which *source* it resolved — a path and a profile name — and never the
key itself. Use `--s3-profile NAME` for a non-default profile; if no credentials are found
anywhere, `--no-sign-request` is added automatically, so a private bucket fails as a clear
403 rather than a confusing signature error.

Both mounts are `:ro` deliberately — nothing in the benchmark should be able to modify
your credentials, and a read-only mount makes that structural rather than a matter of
care. Note what is **not** here: `/mnt/rwdisks` is not bind-mounted in
the real worker either, which is exactly why the localization disk has to be mounted from
inside the container (§4) and why probing the host tells you nothing.

The script locates the downloader at `../localization/parallel_download.py` relative to
itself, so the two files must keep that layout:

```bash
# from your workstation, in the canine repo
gcloud compute scp --project $PROJECT --zone $ZONE \
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

gcloud compute disks create $DISK --project $PROJECT \
  --size $DISK_GB"GB" --type pd-standard --zone $ZONE --labels wolf=canine
gcloud compute instances attach-disk $NODE --project $PROJECT \
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
  gcloud compute disks create $D --project $PROJECT \
    --size 316GB --type $TYPE --zone $ZONE --quiet
  gcloud compute instances attach-disk $NODE --project $PROJECT \
    --zone $ZONE --disk $D --device-name $D
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
  gcloud compute instances detach-disk $NODE --project $PROJECT --zone $ZONE \
    --disk $D --quiet
  gcloud compute disks delete $D --project $PROJECT --zone $ZONE --quiet
done
```

`oflag=direct` / `iflag=direct` bypass the page cache, so these are the disk and not RAM.
The read number matters as much as the write one — the localization disk is re-attached
read-only as a rodisk and read by every downstream consumer.

Compare against the predictions and against today's 21 MB/s:

| Measured write on 316 GB pd-standard | Reading |
|---|---|
| **≫ 38 MB/s** — e.g. 100 MB/s+ | **The likely outcome, per §0.** The per-GB model does not describe this path; the disk is not the localization bottleneck; today's 21 MB/s is source-bound; the downloader has full headroom and ≥4× is available. |
| **≈ 38 MB/s** | The per-GB model holds. The disk is close to binding, and achievable localization time is set by disk size. |

Then continue to §4.1b either way — a single size tells you the rate, but only the sweep
tells you whether the rate *scales with size*, which is the actual claim under test.

Take the **read** number too, and against a small disk as well as a large one — a 10 GB
disk reading at ~1.2 MB/s versus ~100 MB/s is the same question in its starkest form, and
reference disks live at that size (§10).

Whatever it shows, record it — this is the number every cost estimate in §10 depends on,
and it is the cheapest measurement in the document.

Also re-run `pdl probe` now that a disk is mounted — it prints the PD type, provisioned size
and implied per-GB cap, which should agree with `dd`. If they disagree, trust `dd`.

### 4.1b Oversized pd-standard: the same speed without changing type

pd-standard throughput is provisioned per gigabyte, so the fast disk does not have to be a
different *type* — it can be a bigger pd-standard, which keeps unlimited read-only fan-out.
Confirm the model and find the real per-instance ceiling:

```bash
for GB in 10 50 100 200 316 742 2000; do
  D=ddsize-$GB
  gcloud compute disks create $D --project $PROJECT \
    --size ${GB}GB --type pd-standard --zone $ZONE --quiet
  gcloud compute instances attach-disk $NODE --project $PROJECT \
    --zone $ZONE --disk $D --device-name $D
  sudo docker exec slurm bash -c "
    while [ ! -b /dev/disk/by-id/google-$D ]; do sleep 1; done
    echo -n '${GB}GB write: '
    dd if=/dev/zero of=/dev/disk/by-id/google-$D bs=1M count=8000 \
       oflag=direct conv=fdatasync 2>&1 | tail -1
    echo -n '${GB}GB read : '
    dd if=/dev/disk/by-id/google-$D of=/dev/null bs=1M count=8000 iflag=direct 2>&1 | tail -1"
  gcloud compute instances detach-disk $NODE --project $PROJECT --zone $ZONE \
    --disk $D --quiet
  gcloud compute disks delete $D --project $PROJECT --zone $ZONE --quiet
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
### 4.2 Disk conversion — off the critical path, but here is what it would tell you

**Optional.** The arithmetic bounds the prize below the complexity cost whatever the
snapshot rates turn out to be, so no result here can change the conversion decision — but
that is a statement about the *decision*, not a reason the numbers are worthless. Snapshot
creation and hydration rates for a 300 GB disk are useful to know for their own sake
(recovery, re-hydrating an expired cache, any future design that leans on snapshots), and
nobody here has measured them. Run it if you want the datum; skip it if you are short on
time. It costs roughly half an hour and no egress.

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
that is deliberate. Sizing the disk to the small object would give a ~13 GB disk at
~1.6 MB/s and measure a situation that never occurs.

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

**Record the download/verify split.** `verify()` reads the whole object back on this
route, so the wall-clock is download plus a 300 GB re-read. The downloader emits
`k9pdl-phase download …` and `k9pdl-phase verify …`, and the benchmark parses both into
its `--json` output and prints a `download vs verify` summary with a recommendation.
That split is what decides §10's machine-type question — a large verify share means the
cores are doing real work.

Record alongside it the thing that makes this configuration valuable and the alternatives
expensive: **time to `finished=yes`**. That label is when the data becomes available to
every other workflow waiting on it (§6.7), and on this path it lands the moment the
download completes.

### 6.4 Correctness against the real sources

Once for each source type, at the chosen connection count. This is correctness, not
throughput, so a smaller object is fine — and where egress is billed, keep it small
deliberately.

**For an S3 source you supply neither `--size` nor `--md5`.** `head-object` already reports
both: the ETag is the md5 for a single-part object and the md5-of-md5s for a multipart one,
and the benchmark derives the size, picks `--check-md5` or `--check-etag --part-length`
accordingly, and recomputes the digest itself to check the result independently.

Set the source up once. A non-Amazon endpoint needs a scheme on the URL, and the key may
contain slashes, so quote it:

```bash
# on the node
export S3_BUCKET=your-bucket
export S3_KEY=path/to/object.bam
export S3_ENDPOINT=https://your-object-store.example.org   # omit if in ~/.aws/config
```

**Probe the endpoint before transferring anything.** It costs one HEAD and one 1 KiB
ranged GET, and it tells you whether the store honours `Range`, what its ETag means, and
whether presigning works — the three things that differ between S3 implementations:

```bash
pdl probe --s3-bucket "$S3_BUCKET" --s3-key "$S3_KEY" --s3-endpoint-url "$S3_ENDPOINT"
```

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

### 6.6 the bucket-compose route against real GCS

The bucket-compose route has never touched real infrastructure — not the auth path, not
resumable sessions, not compose. It needs a bucket mounted in the container so that
`select_route` sees a non-POSIX
destination. In this deployment those mounts come from the `.rclone*.sh` scripts on NFS, so
on a mock node create one by hand:

```bash
sudo docker exec slurm bash -c '
  # gcsfuse resolves credentials via ADC and does NOT read CLOUDSDK_CONFIG, so point it
  # at the file explicitly. Without this the authenticating identity is whatever ADC
  # happens to resolve to -- the metadata-server SA or the mounted user credentials --
  # which silently works in one project and fails in another. This mirrors what the
  # fuse-localize branch does in base.py before mounting.
  ADC=/root/.config/gcloud/application_default_credentials.json
  [ -f "$ADC" ] && export GOOGLE_APPLICATION_CREDENTIALS=$ADC \
    || echo "no ADC in the container -- did §2 run application-default login before §3?" >&2
  mkdir -p /mnt/bucket
  gcsfuse --implicit-dirs '"$BUCKET"' /mnt/bucket || echo "gcsfuse not in the image"'

pdl routeb --url "$URL_12G" --size $SIZE_12G --md5 "$MD5_12G" \
           --gs-url gs://$BUCKET/pdl-routeb-out.bin \
           --mount-dir /mnt/bucket \
           --json /tmp/routeb.json
```

**Read the token source line carefully, because this is where the mock node can diverge
from production.** A real worker authenticates with user credentials copied from NFS by
`docker_copy_gcloud_credentials.sh`, so it takes the `gcloud` path. With §3's credentials
mount in place, `gcloud` should work here too — that is what the mount is for. If it
reports the **metadata server** instead, the mount is missing or unreadable and you are
exercising the compute service account rather than the identity production uses, which may
have different bucket permissions.

If `gcsfuse` is not in the image, the bucket-compose route cannot be reached this way —
note it as unverified rather than assuming it works.

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

**One interaction worth confirming while you are there**, since it is a property of this
change rather than of the existing design: resume state lives in `.k9pdl.json` **on the
disk being built**. So when a second worker takes over a disk whose builder died, it
inherits that state and should continue rather than restart. The existing protocol already
parks other workers on `exit 5` while a disk has `users`, so this is the natural
hand-off — but nothing has verified that the successor actually resumes.

That is the whole of the multi-worker question worth testing here. A wider contention or
scale test is out of scope: per-disk costs compound at hundreds of concurrent disks, but
that is arithmetic (§10), not something a benchmark needs to reproduce.

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
gcloud compute scp --project $PROJECT --zone $ZONE \
  "$NODE:/tmp/*.json" ./benchmark-results/
```

Teardown — **the disk outlives the instance and keeps billing**:

```bash
gcloud compute instances delete $NODE --project $PROJECT --zone $ZONE --quiet
gcloud compute disks delete $DISK --project $PROJECT --zone $ZONE --quiet
gcloud storage rm gs://$BUCKET/pdl-bench-12g.bin gs://$BUCKET/pdl-bench-300g.bin \
                  gs://$BUCKET/pdl-routeb-out.bin

# confirm nothing is left behind
gcloud compute disks list --project $PROJECT --filter="name~canine-bench"
gcloud compute disks list --project $PROJECT --filter="name~ddsize- OR name~ddtest- OR name~conv-"
```

The `gcloud storage` lines carry no `--project`: bucket names are global, so the path
alone identifies the object. Every `gcloud compute` line needs one, because there the
project decides which resource is found.

The second `disks list` catches the scratch disks §4.1 and §4.1b create in loops — if a
loop was interrupted partway, its `detach-disk`/`delete` never ran and a 316 GB or 2000 GB
disk is still billing.

---
## 10. The cost decision

> Read after §4, not instead of it. §4 measures; this section decides.

The objective is a **cheaper** localization, not merely a faster one. So price the whole
thing: VM-hours plus disk-hours, on preemptible workers (`gcpTransient.py` defaults
`preemptible=True`, and the downloader is preemption-safe), 48 h retention.

The ×300 column is **arithmetic, not a test** — these run at hundreds of concurrent unique
BAM disks, so per-disk differences compound. Nothing in this runbook asks you to create 300
disks.

| Scenario | VM | disk | total | ×300 |
|---|---|---|---|---|
| today: 4.0 h, n1-standard-8 | $0.32 | $0.83 | $1.15 | $345 |
| downloader 4×: 1.0 h, same VM | $0.08 | $0.83 | $0.91 | **$273** |
| …and oversize the disk to 742 GB | $0.08 | $1.95 | $2.03 | **$608** |
| …instead, localize on a 2-vCPU node | $0.01 | $0.83 | $0.85 | **$254** |

**1. The disk is the larger line item, and the downloader cannot touch it.** At 48 h
retention the disk costs $0.83 against $0.32 of VM time, and retention is set by reuse, not
by how fast the disk was filled. The downloader's cost ceiling is the VM share — about 21%.
It is still worth shipping; it is just not where most of the money is.

**2. Do not oversize the disk.** A disk exists for 48 h but is *written* for a few of them,
so paying for gigabytes across the whole lifetime to save time in a small fraction of it
never recovers: +$1.12/disk of storage against $0.10/disk of preemptible VM time. Earlier
revisions of this document recommended 742 GB; that used on-demand pricing and a single-disk
view, and is withdrawn. The reasoning is preserved in `update_localization.md` §13.16 and
§13.18.

**3. The machine type is worth questioning, but localization is not as CPU-free as I
first claimed.** `LocalizeToDisk` pins `n1-standard-8` with `--exclusive`
(`wolF/wolF/localization.py:35`). §2 justified that as "the download owns the whole node" —
an argument that nothing should *compete* with localization, not that it needs 8 vCPUs and
28 GB of RAM. Even a 2-vCPU n1 has a ~500 MB/s egress cap, far above any rate in play.

But **verification is a genuine multi-core workload**, which I had waved away as "a few
minutes of one core":

* on the in-place route, `verify()` is a **full read-back of the object** — 300 GB
  downloaded, then 300 GB read again. The per-part digests computed during transfer live
  on `BucketChunkSink`, so only the bucket-compose route skips this;
* for an S3 **multipart** ETag the read-back parallelizes across parts (each part's md5 is
  independent), and now does — so it will use as many cores as `connections`;
* a **whole-file** md5 cannot be parallelized at all, so it is one core for as long as it
  takes to read 300 GB.

So the smaller-node question is really "how many cores does verification want, and does the
read-back or the download dominate?" — which §6.3 answers directly, since its wall-clock
includes both. **Measure the split before choosing a machine type**: if verification is a
large share, cores are doing real work and the saving is smaller than the table suggests.
`slurm_gcp_docker/conf/nodetypes.json` has no 2-vCPU entry, so a partition would have to be
added before this could be tried at all.

**Ranked by effect on total cost:** retention (dominant, but a workflow decision), then
machine type, then localization speed. Oversizing moves it backwards.

One thing that holds regardless: **`pd-standard` is required.** It is the only type that
attaches read-only to an unlimited number of VMs (`pd-balanced` and `pd-ssd` cap at 10), and
unlimited fan-out is what the rodisk exists for. Do not change the type.

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
| the bucket-compose route on real GCS, and its token source | §6.6 |
| `/bin/sh` in the container | §3 probe |
| Resume across a real preemption | §7 |

Report the §6.3 number as **"4 h → X h on the real BAM"**, not as the sweep's internal
speedup. The internal figure is measured against a single stream on the same hardware; the
number that matters to anyone waiting on a pipeline is the one against today's behaviour.
