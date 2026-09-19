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

Export it here, because §3's probe and §6.3 both use it:

```bash
# on the node -- the real source under test
export S3_BUCKET=your-bucket
export S3_KEY=path/to/CTSP-....WholeGenome.bam
export S3_ENDPOINT=https://your-object-store.example.org   # omit if set in ~/.aws/config
```

No `--size` or `--md5` for this one: `head-object` reports the size, and the ETag is the
md5 for a single-part object or the md5-of-md5s for a multipart one. §3's probe confirms
which before anything is transferred.

If you also have a plain-HTTP GDC source to compare, set those too — that path supplies
size and hash out of band rather than from a HEAD:

```bash
# on the node -- optional, only for §6.4's GDC comparison
export GDC_URL=https://api.gdc.cancer.gov/data/...
export GDC_SIZE=...
export GDC_MD5=...
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

### First: both mount sources must already exist

The container mounts `~/.config/gcloud` and `~/.aws` from the node. **Docker creates a
missing bind-mount source as a directory owned by `root`**, so starting the container
first leaves you with a root-owned `~/.aws` that your own `scp` into it then fails to
write, with a permission error that does not obviously point back here. Populate both
before `docker run`, not after.

`~/.config/gcloud` is already there if you ran §2's `gcloud auth login --update-adc` —
that is the file this mount exists to carry, and the reason authentication comes before
this section.

`~/.aws` needs creating. Put the credentials at the canonical path and nothing has to
carry them: both `aws` and the benchmark find `~/.aws/credentials` on their own, so no key
ever appears in a command line — where it would be visible in `ps` to every user on the
box and recorded in shell history. That matters more when the keys are issued by someone
else and you cannot rotate them at will.

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

Check both exist and are yours before continuing — if either is `root`-owned, remove it
with `sudo rm -rf` and redo the step above:

```bash
# on the node
ls -ld ~/.aws ~/.config/gcloud
```

### Then start the container

```bash
# on the node
# --shm-size from the host, the way worker_startup_script.sh:54 computes it. Docker
# defaults /dev/shm to 64 MiB, which §6.1 (a 12 GiB write to tmpfs) would exhaust
# immediately.
SHM_SIZE=$(df -BM --output=size /dev/shm | sed 1d | tr -d ' ' | tr 'M' 'm')

sudo docker run -dti --rm --pid host --network host --privileged \
  -v /dev:/dev \
  -v $HOME/.config/gcloud:/root/.config/gcloud:ro \
  -v $HOME/.aws:/root/.aws:ro \
  --shm-size "$SHM_SIZE" \
  --entrypoint /bin/bash --name slurm broadinstitute/slurm_gcp_docker
```

These are `worker_startup_script.sh:60`'s flags minus the NFS and docker-socket mounts,
which need a controller — **plus the two credentials mounts, which replace a step we are
skipping.**

The real worker runs `docker_copy_gcloud_credentials.sh`, which copies gcloud credentials
out of `/mnt/nfs/credentials/gcloud/` into the container: in production the container
authenticates with **user credentials propagated over NFS**, not with the node's service
account. Starting the container by hand bypasses that entirely, so without these mounts it
has no credentials at all — `gcloud auth print-access-token` fails, only the metadata
server answers, and the `aws` CLI has nothing to sign with. The measurements run inside the
container via `docker exec`, so a copy on the node alone is invisible to them.

Both mounts are `:ro` deliberately — nothing in the benchmark should be able to modify your
credentials, and a read-only mount makes that structural rather than a matter of care.

`probe` reports which credential *source* it resolved — a path and a profile name — and
never the key itself. Use `--s3-profile NAME` for a non-default profile; if no credentials
are found anywhere, `--no-sign-request` is added automatically, so a private bucket fails
as a clear 403 rather than a confusing signature error.

Note what is **not** mounted: `/mnt/rwdisks` is not bind-mounted in the real worker either,
which is exactly why the localization disk has to be mounted from inside the container (§4)
and why probing the host tells you nothing.

Copy both scripts into **one directory** in the container. The benchmark looks for
`parallel_download.py` beside itself first, so no directory tree needs reconstructing:

```bash
# from your workstation, in the canine repo
gcloud compute scp --project $PROJECT --zone $ZONE \
  canine/test/benchmark_localization.py \
  canine/localization/parallel_download.py \
  $NODE:/tmp/

# on the node
sudo docker exec slurm mkdir -p /tmp/pdl
sudo docker cp /tmp/benchmark_localization.py slurm:/tmp/pdl/
sudo docker cp /tmp/parallel_download.py      slurm:/tmp/pdl/
```

Two single-file `docker cp`s, so re-running this to update the scripts overwrites them.

That is the point of copying files rather than the directory: `docker cp dir c:/tmp/pdl`
onto an existing `/tmp/pdl` **nests** it, producing `/tmp/pdl/dir` and silently leaving the
old scripts in place — so an update appears to succeed while `pdl` keeps running the stale
copy. Verified against a real container. Use `dir/.` if you ever do need to copy a tree.

Confirm the container is running what you think it is:

```bash
sudo docker exec slurm md5sum /tmp/pdl/benchmark_localization.py \
                              /tmp/pdl/parallel_download.py
md5sum /tmp/benchmark_localization.py /tmp/parallel_download.py
```

Define a shorthand — every later step uses it:

```bash
# on the node
pdl() { sudo docker exec slurm python3 /tmp/pdl/benchmark_localization.py "$@"; }
```

Your shell expands `$URL` and friends before `docker exec` sees them, so the variables stay
on the host side and nothing needs `-e`.

### Probe — run it twice, for different reasons

The two halves of `probe` have different prerequisites, so it is worth running here **and**
again after §4.

**Now**, with the S3 source if you have one. Nothing in this part needs the localization
disk, and it gates §6.4:

```bash
pdl probe
pdl probe --s3-bucket "$S3_BUCKET" --s3-key "$S3_KEY" --s3-endpoint-url "$S3_ENDPOINT"
```

Confirm before going further:

* **"Running INSIDE a container"** — if it warns instead, you are on the host and every
  answer below it is the wrong machine's.
* **`/bin/sh` → `/usr/bin/dash`, `BASH_VERSION=<not bash>`.** **Confirmed on the image.**
  This is the shell that rejects the `[[ ]]` and process substitution every emitted command
  uses, so pinning `executable=/bin/bash` throughout was load-bearing rather than
  defensive — `shell=True` alone would have run them under dash.
* **`python3` is 3.14.6.** The image has already been through its 3.8 → 3.14 upgrade, so
  the earlier note about `parallel_download.py` parsing under 3.8 is moot (still true,
  just no longer relevant).
* **8 cpus / 29.38 GiB**, matching the `n1-standard-8` entry in `nodetypes.json`.
* **`curl`, `gcloud`, `aws`, `md5sum`, `od` all present.** `gcsfuse` may not be — the probe
  now reports it, and §6.6 is the only thing that needs it.
* **Every candidate destination will be `overlay` at this point**, which is the container's
  own writable layer on the boot disk rather than a localization disk. The probe says so
  and declines to draw a frontier conclusion from it. **Re-run `probe` after §4** mounts the
  real disk; that result is the one to record.

**Do not trust a `punch-hole : no` from before this was fixed.** The probe gated on
`hasattr(os, "fallocate")`, and Python exposes no `fallocate()` taking a mode — so it
answered "no" on every platform and filesystem, ext4 included. It now calls `fallocate(2)`
through ctypes, so the answer means something. Whether overlay supports hole punching is
genuinely unknown until a fixed probe reports on it; ext4 should say yes.

With S3 arguments it additionally reports, none of which needs the disk:

* **`credentials`** — the resolved source, a path and profile name, never the key. NOT
  FOUND here means the container predates §3's `~/.aws` mount and must be restarted, since
  mounts cannot be added to a running container.
* **`ranged GET`** — the assumption the whole design rests on. A store that ignores `Range`
  cannot be chunked at all.
* **the ETag shape** — `<32 hex>-<N>` is multipart, so neither `--size` nor `--md5` is
  needed anywhere. Opaque means this store does not follow AWS semantics and hash
  verification would fail on correct data.
* **`presign`** — whether the fast single-code-path source is available, or every chunk
  pays an `aws` process.

The `backing` lines and `gcsfuse` in the tool list are also the visible proof that an
updated script is the one running — stronger evidence than `md5sum`, since it is what the
container actually executed.

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
### 4.0 Preflight after any disconnect or container restart

Three things live in the container and none survive it. `docker run --rm` means a restart
comes back with an empty mount namespace, and the failures are all quiet: `/mnt/tmpfs`
missing sends a 4 GiB write to the container's overlay filesystem, `/mnt/rwdisks/$DISK`
missing does the same, and `pdl` missing is just "command not found". Node-side `df` cannot
see any of them (§6.1), so a check from the wrong side reports healthy.

Run this first, every time:

```bash
# on the node
sudo docker ps --filter name=slurm --format '{{.Names}} up {{.Status}}' \
  || echo "CONTAINER GONE -- redo §3"

export DISK=$(ls /dev/disk/by-id/ 2>/dev/null | grep '^google-' \
              | grep -v 'persistent-disk-0' | sed 's/^google-//' | head -1)
echo "DISK=${DISK:-<none attached>}"

sudo docker exec slurm sh -c '
  for m in /mnt/tmpfs /mnt/rwdisks/'"${DISK:-nodisk}"'; do
    if mountpoint -q "$m"; then df -h "$m" | tail -1
    else echo "NOT MOUNTED: $m"; fi
  done
  ls /tmp/pdl/*.py 2>/dev/null || echo "SCRIPTS MISSING: redo §3s docker cp"
'
type pdl >/dev/null 2>&1 || echo "pdl shorthand not defined -- redo §3"
```

**Quote anything the container should expand.** `sudo docker exec slurm df -h
/mnt/rwdisks/*` looks like a container-side check and is not one: the node's shell expands
the glob first, finds no `/mnt/rwdisks` (it is a container path — §6.1), passes the pattern
through literally, and `df` errors. A `||` fallback then reports "nothing mounted" about a
disk that was mounted fine. Node-side expansion is sometimes what you want — `$DISK` above
is deliberately expanded before `docker exec` — so the rule is to be deliberate: single
quotes for the container, double or bare for the node.

What to re-run for each result:

| Result | Fix |
|---|---|
| `CONTAINER GONE` | §3 in full — container, scripts, `pdl` |
| `NOT MOUNTED: /mnt/tmpfs` | the tmpfs block in §6.1 |
| `NOT MOUNTED: /mnt/rwdisks/...` | the re-mount below — **not** §4, which would `mkfs` |
| `SCRIPTS MISSING` | the two `docker cp` lines in §3 |
| `pdl shorthand not defined` | the function definition in §3 |
| `DISK=<none attached>` | §4 in full, including `create` and `attach-disk` |

Re-mounting a disk that already has a filesystem — the common case, since the disk outlives
the container and the instance:

```bash
sudo docker exec slurm bash -c '
  set -eu
  DEV=/dev/disk/by-id/google-'"$DISK"'
  blkid "$DEV" || { echo "NO FILESYSTEM -- this one does need §4s mkfs"; exit 1; }
  mkdir -p /mnt/rwdisks/'"$DISK"'
  mountpoint -q /mnt/rwdisks/'"$DISK"' || mount -o discard,defaults "$DEV" /mnt/rwdisks/'"$DISK"'
  df -h /mnt/rwdisks/'"$DISK"'
'
```

The `blkid` guard is the point. §4's `mkfs` on 316 GB with `lazy_itable_init=0` takes
minutes and would discard the filesystem whose write rate §4.1 measured — so a
"just re-run §4" reflex costs both the time and the baseline.

### 4.1 `dd` the disk you just created — the gate

Ten minutes, no egress, no downloads, **and no additional disks**: measure the localization
disk §4 created. An earlier version of this section created three more (pd-standard,
pd-balanced, pd-ssd) to compare types, which is ~950 GB of provisioning to answer a
question the fan-out constraint has since closed — `pd-balanced` and `pd-ssd` cap at 10
read-only attachments and cannot back the published artifact whatever their throughput
(§10). The only live question is whether the per-gigabyte model describes this path at all.

First make sure nothing is already writing to the disk — see the warning below:

```bash
# on the node
sudo docker exec slurm sh -c 'ps -eo pid,etime,args | grep "[d]d if="' \
  && echo "^ a dd is already running; kill it before measuring" \
  || echo "clear"
```

```bash
# on the node
sudo docker exec slurm bash -c '
  set -e
  D=/mnt/rwdisks/'"$DISK"'
  F=$D/ddtest.$$
  trap "rm -f $F" EXIT
  echo "=== write"
  dd if=/dev/zero of=$F bs=1M count=8000 \
     oflag=direct conv=fdatasync status=progress
  sync
  echo "=== read"
  dd if=$F of=/dev/null bs=1M iflag=direct status=progress'
```

> **`docker exec` does not forward Ctrl-C.** Without `-t` the docker *client* exits on
> SIGINT while the process inside the container keeps running. So interrupting this leaves
> a `dd` writing to the disk, and the next attempt then measures **two writers sharing the
> bandwidth** — halving the apparent rate — before the straggler reaches its cleanup and
> deletes the file the new run just wrote. That failure mode presents as a read error
> immediately after a successful write, which is confusing enough to be worth naming.
>
> Hence `$$` in the filename and the `trap`: each run cleans up only its own file, so a
> straggler cannot delete a live measurement. Check with the `ps` above before trusting
> any number, and `sudo docker exec slurm pkill -f "dd if="` to clear one.

`oflag=direct` / `iflag=direct` bypass the page cache, so these are the disk and not RAM.
8000 MiB is enough to be past any burst behaviour and small enough to finish quickly even
if the pessimistic figure is right.

**`status=progress` and no `| tail -1`, deliberately.** This takes between 35 seconds and
four minutes depending on which way the answer falls, and an earlier version printed a
label with `echo -n` and piped `dd` through `tail`, so it emitted `write: ` and then
nothing until it finished — indistinguishable from a shell waiting for input. Live progress
costs nothing and removes the ambiguity.

**Take the read number too.** It is what every downstream consumer of the rodisk
experiences, and — more immediately — what `verify()`'s full 279 GB read-back will run at,
which §6.3 reports as its own phase.

**MEASURED, 2026-09: 92.3 MB/s write, 91.6 MB/s read** — 2.43× the per-GB model's
prediction of 37.9 MB/s. The model does not describe this path, so §10 does not apply, and
today's 21 MB/s is **23% of the disk**: 4.4× headroom.

| Measured write on the 316 GB pd-standard | Reading |
|---|---|
| **≫ 38 MB/s** — what happened, at 92.3 | The per-GB model does not describe this path; the disk is not the current bottleneck; today's 21 MB/s is source-bound; ≥4× is available and the disk binds at 4.44×. §10 does not apply. |
| **≈ 38 MB/s** | The per-GB model holds and §10's analysis is live. Did not occur. |

**Read and write are symmetric, which is the consequential part.** `verify()` re-reads the
whole object on this route, so at 91.6 MB/s that read-back costs 0.91 h against the
download's 0.90 h — **half the wall clock**, turning 4.44× into 2.21×. See §13.19 and
§13.31: making the in-place route hash during transfer is worth exactly a doubling, and is
the highest-value change left.

Then **re-run `pdl probe`**. There is now a block-device-backed candidate, so it reports
for the first time on a real destination:

* **`SEEK_HOLE`** on ext4 — if yes, frontier recovery is live, and it has never executed on
  any machine (it fails the probe on APFS, so every local test used the checkpoint
  fallback);
* **`punch-hole`** — unsupported on overlay, and needed for §6.5's page-cache-loss test,
  the one case SIGKILL cannot simulate;
* **`pd type` / `write cap`** — the per-GB prediction, to set against what `dd` just
  measured. If they disagree, trust `dd`.

### 4.1a Concurrency: a single stream does not saturate the disk

**§4.1's 92.3 MB/s is the single-stream rate, not the disk's ceiling.** The evidence came
from an accident: when a straggler `dd` overlapped a fresh one, the fresh one still managed
**69.5 MB/s — 75% of solo, not 50%**. A fixed-bandwidth device would have given each 46.
So aggregate was around 139 MB/s, roughly **1.5× solo from two writers**, and the
relationship is non-linear.

That matters because **nothing in this system writes with one stream.** The downloader
issues `pwrite` from N threads, and `multipart_etag` reads from N threads. Both are the
multi-stream case, so the ceiling they see is the aggregate — not 92.

```bash
# on the node. ~16 GB written at the widest setting; a few minutes total.
sudo docker exec slurm bash -c '
  set -e
  D=/mnt/rwdisks/'"$DISK"'
  MIB=2000
  for N in 1 2 4 8; do
    rm -f $D/cc.*
    start=$(date +%s)
    for i in $(seq 1 $N); do
      dd if=/dev/zero of=$D/cc.$i bs=1M count=$MIB oflag=direct conv=fdatasync \
        2>/dev/null &
    done
    wait
    el=$(( $(date +%s) - start )); [ $el -eq 0 ] && el=1
    echo "write $N x ${MIB} MiB in ${el}s = $(( N * MIB / el )) MiB/s aggregate"
  done
  sync
  for N in 1 2 4 8; do
    start=$(date +%s)
    for i in $(seq 1 $N); do
      dd if=$D/cc.$i of=/dev/null bs=1M iflag=direct 2>/dev/null &
    done
    wait
    el=$(( $(date +%s) - start )); [ $el -eq 0 ] && el=1
    echo "read  $N x ${MIB} MiB in ${el}s = $(( N * MIB / el )) MiB/s aggregate"
  done
  rm -f $D/cc.*'
```

The read sweep reuses the eight files the write sweep leaves, so `N` readers read `N`
distinct files — which is what the downloader and the verifier actually do, rather than
several threads contending on one file.

**MEASURED, 2026-09 — concurrency does not help, and hurts reads:**

| aggregate MiB/s | N=1 | N=2 | N=4 | N=8 |
|---|---|---|---|---|
| write | 90 | 86 | 81 | 81 |
| read | 86 | 85 | 76 | **62** |

The disk is a **fixed-bandwidth resource at ~85-90 MiB/s**. Write aggregate is flat to
−10%; read aggregate *declines 28%* from one reader to eight. Extra streams add
interleaving cost and win nothing, which settles the question §4.1 raised: **92 MB/s is the
ceiling, not a single-stream figure, and 4.44× is a cap.**

Two consequences, both acted on:

* `verify()` used to pass `workers=connections`, so at the default 8 the read-back would
  have run at 62 MiB/s instead of 86 — **21 minutes slower** on a 279 GiB object. Now
  `VERIFY_READ_WORKERS = 2`, which costs ~1% here and leaves headroom for a destination
  where hashing rather than IO binds (tmpfs, local SSD).
* the read-back therefore costs roughly **1 h**, and no amount of read concurrency removes
  it. The only way to recover it is to not read the object at all — in-transfer hashing,
  task #19.

#### Measure `multipart_etag` directly, rather than modelling it

Read throughput alone does not predict the read-back's rate, because each worker reads and
hashes **serially** — a lone worker leaves the disk idle while it hashes. With md5 measured
at 700 MiB/s and the disk at 86, one worker achieves `1/(1/86 + 1/700) = 77 MiB/s`, and
overlapping two recovers about 5%. That is a model with assumptions in it; measure the real
thing:

```bash
# on the node -- needs a multi-part-sized file on the disk
sudo docker exec slurm bash -c '
  D=/mnt/rwdisks/'"$DISK"'
  [ -f $D/hashtest ] || dd if=/dev/zero of=$D/hashtest bs=1M count=4000 oflag=direct
  F=$D/hashtest python3 - <<'"'"'PY'"'"'
import importlib.util, os, time
spec = importlib.util.spec_from_file_location("pdl", "/tmp/pdl/parallel_download.py")
pdl = importlib.util.module_from_spec(spec); spec.loader.exec_module(pdl)
path, part = os.environ["F"], 29 * 1024 * 1024
size = os.path.getsize(path)
for n in (1, 2, 3, 4, 8):
    os.system("sync")
    dropped = os.system("echo 3 > /proc/sys/vm/drop_caches") == 0
    t0 = time.monotonic()
    pdl.multipart_etag(path, part, workers=n)
    el = time.monotonic() - t0
    print("workers={:<2} {:>6.1f} MiB/s{}".format(
        n, size / 1048576 / el, "" if dropped else "   (CACHED -- meaningless)"))
PY
  rm -f $D/hashtest'
```

**If it cannot drop caches the numbers are worthless** — every run after the first reads
from RAM — so the script says so rather than reporting a fiction. `--privileged` should
allow it.

Whatever the peak is, set `VERIFY_READ_WORKERS` to it. The constant currently says 2 on the
strength of the model above.

### 4.1b Does the rate scale with size? — optional, and quota-heavy

**Run this only if §4.1 came back near 38 MB/s.** A single measurement tells you the rate;
only a sweep tells you whether it *scales with size*, which is the per-GB model's actual
claim. But if §4.1 already showed ~100 MB/s on a 316 GB disk, the model is dead and there
is nothing left for a curve to establish.

Note the cost before running it: seven disks totalling **~3.4 TB** of transient
provisioning. If regional quota is tight, the informative subset is the **small** end —
`10 50 100 200` is 380 GB and answers the reference-disk question in §10, which is the one
with a live decision attached.

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

The large end tells you whether §10's sizing table is real — though §10 concludes against
oversizing on cost grounds regardless, so this is now confirmation of a model rather than
input to a decision.

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

### 4.1d Does hashing cost anything on top of reading? — 5 minutes, no egress

The benchmark verifies the destination itself, by re-reading it and recomputing the
multipart ETag (`file_multipart_etag`). On a 279 GiB object that is the better part of an
hour — comparable to the download it is checking — so it roughly **doubles the wall time
of every setting in a sweep**. It cannot be optimised away: reusing the downloader's
in-transfer digests would mean the verdict was the downloader's own, and a multipart ETag
cannot be sampled, because it is only checkable by reading every byte.

What *can* be asked is whether any of that hour is CPU. The arithmetic says barely:

* md5 runs **~600–760 MB/s on one core** — about **7×** the disk. Keeping up with 86 MiB/s
  needs roughly 0.11 of a core.
* So hashing is ~1/7 of read time, and overlapping it perfectly saves **at most ~15%**.
  That is the entire prize, at any core count.
* Against that, §4.1a's read curve **falls** with concurrency — 86 / 85 / 76 / 62 MiB/s at
  1 / 2 / 4 / 8 readers. At 4 readers the 12% read loss eats nearly the whole prize; at 8
  it costs more than the prize is worth. Two is where the curves cross, which is why both
  the downloader and the benchmark use `VERIFY_READ_WORKERS = 2`.

**Threads, not processes.** `hashlib` releases the GIL, so threads already scale md5 near
linearly (753 → 1464 → 2853 → 4987 MB/s at 1/2/4/8). Processes measured strictly slower at
every width (536 → 1310 → 2477 → 4208) purely on spawn cost, before any of the IPC or
per-process file handles a real implementation would need. Multiprocessing only wins when
the GIL is held, and here it is not.

The one thing the arithmetic cannot settle is whether the 15% is available at all, because
buffered reads already overlap: the kernel prefetches block N+1 while Python hashes block
N. If readahead is doing its job, a single thread is already at the device rate and
threading the hasher wins nothing. Two commands decide it:

```bash
# on the node, with the disk otherwise IDLE -- any concurrent download invalidates this
sudo docker exec slurm sh -c '
  sync; echo 3 > /proc/sys/vm/drop_caches 2>/dev/null
  dd if=/mnt/rwdisks/'"$DISK"'/bench.16.bin of=/dev/null bs=8M count=4000'

sudo docker exec slurm sh -c '
  sync; echo 3 > /proc/sys/vm/drop_caches 2>/dev/null
  dd if=/mnt/rwdisks/'"$DISK"'/bench.16.bin bs=8M count=4000 | md5sum'
```

| result | meaning |
|---|---|
| within a few percent | readahead already overlaps the hash. The prize is ~0, `--verify-workers 1` is as good as 2, and nothing needs changing. |
| `md5sum` ~15% slower | the read and the hash really are serialised. ~8 minutes per 279 GiB verify is available, and 2 workers is the right default. |
| `md5sum` ≫ 15% slower | something other than md5 is in the way (Python-level copying, a small block size). Worth a look before accepting the verify cost as inherent. |

If you want the answer for the real code path rather than `dd`, sweep `--verify-workers`
— it exists to re-measure this balance, not to tune a run, and it **must not change the
ETag**, only the time taken to produce it. That needs a source and a digest, so it belongs
with §6.5a rather than here; the block is at the end of that section.

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

**Reporting a run.** Two parts of a sweep's output should not leave the node verbatim. The
`object :` line names the endpoint, bucket and object path next to a
`<N bytes of query string redacted>` marker — a presigned URL carries the access key ID in
`X-Amz-Credential`, so treat the line as credential-adjacent even redacted. And the
`... [k9pdl] NN.N%` / `... still verifying` heartbeats are dozens of near-identical lines
carrying nothing the `phases:` and `io :` lines do not. Strip both:

```bash
grep -v -e '\[k9pdl\]' -e 'still verifying' -e '^object *:' sweep.txt
```

What remains — the table row, the indented metric lines, and the verdict block — is the
whole measurement.

### 6.1 Find the knee in tmpfs — no disk in the path

Do this before any disk-destined download. With the disk removed entirely, this measures
what the *source and the NIC* can do, which is the ceiling parallelism could ever reach —
and the knee it finds is the `connections` default.

```bash
# on the node -- check there is actually room before writing 12 GiB to it
sudo docker exec slurm sh -c 'mkdir -p /dev/shm/pdl && df -h /dev/shm'
```

If that shows **64M**, the container was started without `--shm-size` (see §3) and this
step cannot run as written. Either restart the container with the flag — it holds no
state — or mount a tmpfs of your own inside it, which needs no restart since the container
is `--privileged`:

```bash
# on the node -- alternative to restarting
sudo docker exec slurm sh -c '
  mkdir -p /mnt/tmpfs
  mountpoint -q /mnt/tmpfs || mount -t tmpfs -o size=16G tmpfs /mnt/tmpfs
  df -h /mnt/tmpfs'
```

**That mount exists only inside the container.** `--pid host` shares the PID namespace, not
the mount namespace, so a node-side `df -h /mnt/tmpfs` reports `No such file or directory`
and a node-side `ls` shows nothing — which reads like the mount failed when it did not.
Every check on this destination has to go through `docker exec`:

```bash
# on the node
sudo docker exec slurm df -h /mnt/tmpfs
```

It also does not survive the container. `docker run --rm` means a restarted container comes
back without it, and `pdl` would then write 12 GiB to the container's overlay filesystem
instead — a different measurement, on a different backing store, reported as if it were
this one. `pdl probe --dest-dir /mnt/tmpfs` names the backing, so it is worth one look
before a long run.

**The object needs an auth header.** `https://storage.googleapis.com/BUCKET/OBJECT` is
anonymous, and the bucket §2 uploaded to is private — so every setting fails with 403 in
under a second. `--header` is how the GCS handlers pass credentials, and the downloader
forwards it on every ranged GET:

```bash
export GCS_AUTH="Authorization: Bearer $(gcloud auth print-access-token)"

pdl sweep --url "$URL_12G" --size $SIZE_12G --md5 "$MD5_12G" \
          --header "$GCS_AUTH" \
          --dest-dir /mnt/tmpfs \
          --connections 1 4 8 12 16 \
          --json /tmp/sweep-shm.json
```

Access tokens last about an hour, so re-mint it if a long sweep starts failing partway —
and note that the `connections 1` row shells out to `curl`, which gets the header too.

Make a **separate ~12 GB object** with §2 for this — n1-standard-8 has 28.2 GB of RAM and
tmpfs is memory. (Omit `--md5` if you would rather skip verification; passing an empty
string silently disables the check, which is worse than saying so.)

`connections 1` is the **legacy path**, not the new code throttled: the downloader declines
at `connections <= 1` and synthesizes `curl -C - -sSL`. That is the baseline the ≥4× claim
is measured against.

Read from the output:

* **the knee** — the lowest connection count within 5% of the best. This is the default to
  ship (currently 8). Export it, since §6.2 and §6.3 both use it:

  ```bash
  export KNEE=8        # whatever the sweep reported
  ```

* **peak throughput**, which is the ceiling any disk-destined run is bounded by;
* **peak RSS**, which should be flat across settings and unrelated to object size;
* **md5 ok** at every setting.

Do this against a same-region GCS object first because it is free. If the knee against the
real S3/GDC source might differ — plausible, since per-connection throttling is
source-specific — repeat with a ~12 GB slice of the real source and accept the ~$1 of
egress. That is much cheaper than discovering it at 300 GB.

#### The same knee, against the real source — and why it needs `--prefix`

```bash
# presign in the CONTAINER (that is where `aws` lives), with a window long enough
# to outlast the whole sweep
export PRESIGNED_URL=$(sudo docker exec slurm \
  aws --endpoint-url "$S3_ENDPOINT" s3 presign "s3://$S3_BUCKET/$S3_KEY" \
      --expires-in 43200)
test -n "$PRESIGNED_URL" || echo "presign produced nothing -- check the endpoint and creds"

pdl sweep --url "$PRESIGNED_URL" --prefix \
          --size $((12 * 1024 * 1024 * 1024)) \
          --dest-dir /mnt/tmpfs --connections 1 4 8 12 16 \
          --json /tmp/knee-gdc.json
```

No `--s3-*` flags here, for the reason spelled out in §6.4: they would derive the ETag of
the *whole* 279 GiB object, which a 12 GiB slice cannot match, so every row would fail
verification. (The benchmark now declines rather than letting you find out at row 1.)

**This baseline is not the one your 4-hour localizations ran.** `--url` selects
`HttpSource`, whose `connections=1` row is a single ranged `curl`. Production's legacy path
for an `s3://` input is `aws s3api get-object --range` — `file_handlers.py:1239` — and
`aws` is a Python CLI, which §13.38 measured costing 1.7× against `curl` on the same
object (`gcloud storage` 60 MB/s versus `curl` 103 MB/s). So the curl baseline probably
*understates* the speedup, by a factor nobody has measured.

Run the `aws`-based baseline too, then, and report both:

```bash
# S3ApiSource: --url absent, so connections=1 is `aws s3api get-object --range`,
# the command production actually replaced
pdl sweep --s3-bucket "$S3_BUCKET" --s3-key "$S3_KEY" \
          --s3-endpoint-url "$S3_ENDPOINT" --prefix \
          --size $((12 * 1024 * 1024 * 1024)) \
          --dest-dir /mnt/tmpfs --connections 1 4 8 12 16 \
          --json /tmp/knee-gdc-s3api.json
```

`--prefix` matters here for the same reason: production's range is `bytes=$SZ-`, open-ended
because there `--size` is always the whole object. Under a truncated `--size` that fetches
all 279 GiB; `--prefix` bounds it to `bytes=$SZ-<last>`.

This run also measures the per-chunk process cost on the parallel rows, which is the
reason the presigned path is preferred — so its parallel numbers are a *floor*, and the
presigned rows above are what production does when presigning works. Two knees, two
baselines: the honest speedup for the BAM is the `aws s3api` baseline against the
presigned parallel rows.

How big that per-chunk cost is, stated as a prediction so the run can contradict it:
chunks are `align_up(64 MiB, 29 MiB)` = **87 MiB**, so the 279 GiB object is **3283
chunks** — three parts each, not one process per part. Only `connections` processes run at
once and each overlaps a full chunk's transfer, so at ~1 s of Python startup and 8
connections that is roughly **7 minutes** spread across the download, not a serial pile of
startups. If the gap between the two paths' parallel rows is far wider than that, the cost
is something else and worth chasing.

One thing this comparison does *not* turn on is connection reuse. `HttpSource` calls
`urllib.request.urlopen` per chunk, and urllib neither pools nor keeps alive — it sends
`Connection: close` — so both paths pay a TCP and TLS handshake per chunk, 3283 apiece.
Pooling those (one connection per worker) is an optimization available to the presigned
path and not taken; if the sweep shows the two paths closer than the startup estimate
predicts, handshake cost dominating both is the likely reason.

**A prefix run needs `--object-size`, and the benchmark discovers it for you.** This is
the trap that voided the first two GDC sweeps. `probe_range` compares the server's
declared total against the size it was given; handed a 4 GiB prefix of a 279 GiB object it
concluded Range was not honoured, raised `RangeNotSupported`, and the downloader dropped
to a single stream — for **every** row, while the table went on reporting them as 1, 4, 8,
12 and 16 connections. Nothing else looked wrong: right byte count, `wire: 1.01x`, real
throughput. The flat line everyone read as "this source caps aggregate bandwidth" was one
curl measured five times.

Two things now prevent a repeat. `--prefix` makes the benchmark learn the object's total
from `Content-Range` on a one-byte GET and pass it as `--object-size`, and the header says
what it found:

```
object size: 278.94 GiB -- fetching a 4.00 GiB prefix
```

`UNKNOWN` there means the probe failed and every row is about to fall back — stop and fix
that first. And any row that falls back is now marked `NOT PARALLEL` with the server's own
reason, with a `NOT A PARALLEL MEASUREMENT` section in the verdict, so the connection
column can never again be read as real when it isn't.

**`--prefix` is not optional when `--size` is smaller than the object.** The parallel rows
plan chunks over `[0, size)` and stop there, but the `connections=1` row is the legacy path,
and the downloader's single-stream fallback synthesizes `curl -C - -sSL -o dest url` with
**no range** — correct in production, where `--size` is always the whole object, and wrong
here. Without `--prefix` that one row quietly pulls all 279 GiB into a 16 GiB tmpfs; this
was observed, it fills the mount, and because the file is unlinked on failure the space
does not come back until the process is killed. `--prefix` makes the benchmark emit a
ranged `curl --fail -sSL -r 0-N` baseline instead, so every row fetches the same bytes.
The sweep header says which baseline it used — check it.

The consequence is that this run is **throughput only**. A silently truncated transfer
would not be caught, so correctness against this source rests on §6.3 and §6.4 at full
size, which are also the only runs that exercise in-transfer part hashing.

### 6.1a Where is the ceiling? — ANSWERED: per connection

**Do not run this section.** It is kept for the reasoning, not the procedure. The flat
sweep it was designed to explain was an artifact: `probe_range` rejected every prefix
request and the downloader fell back to a single stream on all five rows, so the sweep
measured one curl five times. With `--object-size` supplied, the same source and the same
node give:

| | throughput | streams | note |
|---|---|---|---|
| 1 connection (ranged curl) | 16.62 MiB/s | — | the legacy path, unaffected by the bug |
| 16 connections | **230.11 MiB/s** | 15.05 of 16 | `wire: 1.01x`, so no duplicate fetching |

**13.85×**, against a target of ≥4×. 230.11 / 15.05 effective streams is 15.29 MiB/s per
stream, essentially the single-stream rate — so throughput is *linear* in connections and
the cap is **per connection**, the first row of the table below. No experiment is needed
to choose between the four; the arms would only reconfirm it.

Two consequences worth carrying forward:

* **The disk is now the binding limit, not the source.** pd-standard writes at 92.3 MB/s
  (88.0 MiB/s, §4.1), which is reached at about **5.3 connections**. For the 279 GiB BAM:
  4.77 h single-stream, 54 min disk-bound, 21 min if the destination could keep up. So
  §6.2 — the same sweep to the localization disk — is the measurement that now decides
  the project, and §10's cost model has to be rebuilt around a disk ceiling rather than a
  source ceiling.

  *Both projections here were optimistic, and §6.3 says by how much.* The "54 min
  disk-bound" figure assumed the disk ceiling would be reached at full size the way it is
  at 4 GiB. It is not: the measured full-size run is **97 min at 49.04 MiB/s**, 56% of the
  same device that the 4 GiB prefix drives to 90%. Arithmetic on a 4 GiB row does not
  extrapolate to 279 GiB, and §6.5c is open on why.
* **`MAX_CONNECTIONS` is 16 and 16 was still scaling.** That matters only for destinations
  faster than pd-standard; for the primary case the disk saturates first, so raising it is
  not obviously worth the extra sockets. Worth revisiting if a run ever targets tmpfs or a
  local SSD for real.

The original reasoning, for the record. A flat line is consistent with four different
ceilings that call for four different responses:

| Ceiling is per… | What would lift it | Cost to exploit |
|---|---|---|
| connection | more connections | nothing — already built |
| **signed URL** | **one URL per worker** | small change to `HttpSource` |
| client IP / account | more VMs, or different credentials | large, or impossible |
| object | nothing on the client side | none — the target is unreachable |

The sweep cannot tell them apart, because it uses **one** presigned URL for every
connection. Neither can `peak NIC`, which equals throughput under all four. So test it
directly, with `curl` rather than the downloader, so that a defect in our own code cannot
be mistaken for a property of the source.

```bash
# on the node. /dev/null as the destination: this measures the source, nothing else.
STREAM=$((256 * 1024 * 1024))      # 256 MiB per stream, 4 streams => 1 GiB per arm

presign_one () {   # $1 varies the window, which varies the signature -- see below
  sudo docker exec slurm aws --endpoint-url "$S3_ENDPOINT" \
    s3 presign "s3://$S3_BUCKET/$S3_KEY" --expires-in "$1"
}

# four URLs, each with a different window so each has a different signature
URLS=()
for i in 0 1 2 3; do URLS+=("$(presign_one $((43200 + i)))"); done
test "${URLS[0]}" != "${URLS[1]}" \
  || echo "URLs are identical -- the windows did not vary the signature"

# --- arm C: baseline. one URL, one stream.
time curl --fail -sS -r 0-$((STREAM - 1)) -o /dev/null "${URLS[0]}"

# --- arm A: one URL, four concurrent streams on disjoint ranges.
time ( for i in 0 1 2 3; do
         S=$((i * STREAM))
         curl --fail -sS -r $S-$((S + STREAM - 1)) -o /dev/null "${URLS[0]}" &
       done; wait )

# --- arm B: four DISTINCT URLs, the same four ranges.
time ( for i in 0 1 2 3; do
         S=$((i * STREAM))
         curl --fail -sS -r $S-$((S + STREAM - 1)) -o /dev/null "${URLS[$i]}" &
       done; wait )
```

`--expires-in $((43200 + i))` is what makes the four URLs distinct: `X-Amz-Expires` is
part of the canonical query string SigV4 signs, so changing it by one second changes the
signature. Calling `presign` four times in the same second with identical arguments
returns the *same* URL, and arm B would silently become a second copy of arm A — which is
why the `test "$U0" != "$U1"` line is there rather than assumed.

#### Check that the four streams really are four connections

The experiment assumes four `curl` processes open four sockets. They do — curl's connection
cache is per-process, so separate invocations cannot reuse each other's connections — but
this is the assumption the whole result rests on, so watch it rather than trust it. While
arm A is running, from a second shell:

```bash
# on the node, during arm A or B
S3_HOST=${S3_ENDPOINT#https://}
ss -tn state established "dst $(getent hosts "$S3_HOST" | awk '{print $1}')"
```

Four rows is what you want. One row means something is multiplexing, and the only thing
that can is a proxy — so check for one before starting, since an `http_proxy` inherited
from the environment or a `~/.curlrc` would quietly turn all three arms into the same
measurement:

**Check both contexts, and do not let one stand in for the other.** `docker exec` hands the
process the *container's* environment, not your shell's, so a node-side `http_proxy` never
reaches the downloader and a container-side one never appears in the node's `env`. The
arms below run node-side; the sweep's `connections=1` baseline curl runs inside the
container, where the file that would affect it is `/root/.curlrc`, not `~/.curlrc`.

Rather than enumerate config sources and hope the list is complete, ask curl what it did:

```bash
# on the node -- covers the arms below
curl -v -sS -r 0-0 -o /dev/null "$PRESIGNED_URL" 2>&1 \
  | grep -E '^\* Connected to|[Pp]roxy|^< HTTP/'

# in the container -- covers the sweep's baseline row
sudo docker exec slurm sh -c 'curl -v -sS -r 0-0 -o /dev/null "'"$PRESIGNED_URL"'" 2>&1' \
  | grep -E '^\* Connected to|[Pp]roxy|^< HTTP/' 

# and the config sources, in the right places
env | grep -i proxy || echo "node: no proxy env"
test -f ~/.curlrc && cat ~/.curlrc || echo "node: no ~/.curlrc"
sudo docker exec slurm sh -c '
  env | grep -i proxy || echo "container: no proxy env"
  ls -la /root/.curlrc 2>/dev/null || echo "container: no /root/.curlrc"'
```

**Anchor those patterns.** A bare `grep -Ei "HTTP/"` also matches curl's `> GET ...` trace
line, which is the whole request URL — so it prints `X-Amz-Credential` (your access key ID)
and `X-Amz-Signature` to the terminal. `^< HTTP/` takes only the response status; `^\*
Connected to` only the connection line. This was learned by doing it the other way and
putting a live signature in the scrollback.

`Connected to` naming the endpoint host is what you want in both. A different host, a
`port 3128`, or any `Proxy-` line means a proxy is in the path. `HTTP/2` is not a problem
here — it multiplexes within one connection held by one process, and these are separate
processes.

A proxy in the container but not on the node would explain the flat sweep on its own, with
no per-URL or per-account theory required: the sweep would have been measuring the proxy,
and the node-side arms would then disagree with it for reasons that have nothing to do
with signatures.

`urllib` reads `http_proxy`/`https_proxy` from its own environment too, so this applies to
the downloader's ranged GETs and not only to the baseline curl.

HTTP/2 is the other multiplexing mechanism and is *not* a confound here, for the same
reason: it multiplexes streams within one connection, held by one process. It would matter
if a single `curl` were given all four ranges at once, which is why the arms use four
processes instead.

Worth recording the peer addresses too, because a per-backend cap and a per-account cap
are otherwise indistinguishable:

```bash
getent hosts "$S3_HOST"      # one address, or several?
```

If the endpoint round-robins across several backends and throughput *still* does not
scale, the cap is being applied somewhere shared — an account or object quota rather than
a per-server limit.

The same question about our own code has an answer: `HttpSource.open_range` calls
`urllib.request.urlopen`, and urllib sends `Connection: close` on every request — verified
directly against a server that records what arrived, not inferred from the documentation.
So each chunk gets its own TCP connection and 16 workers really are 16 connections. That
is also why there is no connection reuse to lose by using the S3 API path instead; see
§6.1's note.

Reading it, with C as the single-stream time for 256 MiB:

* **B ≈ C, A ≈ 4×C** — per signed URL. Mint one URL per worker and the ceiling lifts.
  This is the outcome worth hoping for: `HandleAWSURL` already presigns on the node, so it
  becomes presigning N times and handing `HttpSource` a list.
* **A ≈ B ≈ 4×C** — neither: concurrency works and the earlier flat sweep was a defect in
  the downloader, not the source. The `streams:` line added to the sweep output in §6.1
  will say so directly; re-run that first.
* **A ≈ B ≈ C** — per IP, per account, or per object. Nothing on this VM lifts it, and the
  ≥4× target is unreachable against this source from one node. That is a real answer, and
  it redirects the work to the disk-sharing and caching approaches rather than to
  parallelism.

Egress is ~3 GiB, a few minutes, well under a dollar. Do not skip it on cost grounds: the
whole ≥4× premise for the motivating workload rests on which row this lands in.

### 6.2 The same sweep to the localization disk — where the ceiling bites

**Pick the range so it can falsify the prediction, not confirm it.** The disk is expected
to plateau near 88.0 MiB/s (§4.1) at about 5.5 connections, given ~16 MiB/s per stream
from §6.1. The temptation is to sweep 1/4/6/8/12 — enough to show the plateau and stop.
Don't:

* `dd` measured **one sequential writer**. Concurrent `pwrite`s at 16 offsets are a
  different workload, and §4.1a found read throughput *falling* with concurrency
  (86/85/76/62 MiB/s at 1/2/4/8). If writes do the same, 16 is materially worse than 8 —
  which changes the default and is a result, not noise.
* §6.1 ran 1/4/8/12/16. Dropping a setting costs the like-for-like comparison against the
  destination-free numbers, which is the whole point of running the same sweep twice.
* If the fastest row is also the highest tried, `NO KNEE FOUND` fires and the run is
  inconclusive — so a narrow range does not even save time.

A row is ~47s at disk speed. There is no cost argument for a narrow range here; include
6 because it is where the plateau is predicted, and keep 16 because it is where the
prediction could break.

```bash
pdl sweep --url "$PRESIGNED_URL" --prefix \
          --size $((4 * 1024 * 1024 * 1024)) \
          --dest-dir /mnt/rwdisks/$DISK --connections 1 4 6 8 12 16 \
          --json /tmp/knee-disk.json
```

Unlike §6.1, this run *does* say something about the disk: `peak disk` becomes non-zero
and the saturation heuristic in the verdict applies.


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

#### Measured

4 GiB prefix of the real BAM, presigned URL, to the 316 GB pd-standard disk. `dd` on this
disk reads 88.0 MiB/s and every row's `peak disk write` lands at 87.7–96.9 MiB/s:

| conns | throughput | streams | mean disk | vs 1 conn |
|---|---|---|---|---|
| 1 | 15.96 MiB/s | — (legacy curl) | — | 1.00× |
| 4 | 37.61 MiB/s | 2.36 of 4 | — | 2.36× |
| 6 | 46.61 MiB/s | 2.86 of 6 | — | 2.92× |
| 8 | 54.90 MiB/s | 3.37 of 8 | — | 3.44× |
| 12 | 59.48 MiB/s | 3.64 of 12 | — | 3.73× |
| 16 | 66.49 MiB/s | 4.12 of 16 | — | **4.17×** |
| 16, after the deferred writer (§6.5a) | **78.59 MiB/s** | 5.32 of 16 | 78.61 MiB/s | **4.92×** |

**Both predictions in the bullets above were wrong, and keeping 16 is what showed it.**
Concurrency did not hurt writes the way §4.1a found it hurting reads — throughput rose
monotonically to 16. And the run did *not* plateau: 16 was both the fastest and the highest
setting tried, so `NO KNEE FOUND` fired and the sweep was formally inconclusive about the
default. Had the range stopped at 12, "plateau at 59 MiB/s" would have been recorded as a
result.

The deferred manifest writer then moved this row from 66.49 to 78.59 MiB/s (**+18.2%**) and
flipped the verdict from `MOSTLY disk-bound` to `the DISK is the limit: sustained writes are
within 10% of the device's peak`.

**Do not trust that 90% figure, and do not trust these rows as disk measurements at all.**
§6.5d established that a 4 GiB write on a 28 GB-RAM node largely lands in page cache and
the process exits before writeback completes — the full-size run's first two heartbeat
intervals absorb 32.5 and 27.6 GB before settling. So these rows measure memory as much as
the device, and the sustained rate is §6.3's 49 MiB/s. The *relative* comparison between
rows is still sound, because every row is contaminated identically; the absolute numbers
and the "% of device" are not.

Hold that conclusion to *this extent*. §6.3 runs the same configuration at 279 GiB and gets
56% of the same device.

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
pdl sweep --s3-bucket "$S3_BUCKET" --s3-key "$S3_KEY" \
          --s3-endpoint-url "$S3_ENDPOINT" \
          --dest-dir /mnt/rwdisks/$DISK --connections $KNEE \
          --json /tmp/direct-300g-realbam.json
```

Expect roughly `300 GB ÷ (the §6.2 plateau)`. Report it as **"4 h → X h"** against today's
behaviour, not as the sweep's internal speedup.

**Add `--keep` if you might want to measure the disk afterwards.** The sweep unlinks the
payload and its sidecars at the end of each row by default — correctly, since an orphaned
`.k9pdl.done` with no file is what made a later run exit 0 after 108 bytes — so a 3h22m
full-size run leaves an empty filesystem behind. §6.5h's read arm then has nothing to read,
and the only way back is another 1h37m of transfer. Deleting it is the right default for a
sweep of several rows on a 316 GB disk; it is the wrong one for the single-row §6.3 run
whose artifact is the largest thing you will ever have to measure against.

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

#### Measured — the headline, and the target is missed

The real BAM, whole object, `S3ApiSource`, to the 316 GB pd-standard disk, 16 connections.
Run twice: once before the deferred manifest writer and once after.

```bash
# what was actually run. No --size: it is derived from head-object, so probe_range's
# declared total matches and nothing falls back (see §6.1a). No $KNEE -- that variable
# was never defined anywhere; 16 is the §6.2 figure.
pdl sweep --s3-bucket "$S3_BUCKET" --s3-key "$S3_KEY" \
          --s3-endpoint-url "$S3_ENDPOINT" \
          --dest-dir /mnt/rwdisks/$DISK --connections 16 \
          --json /tmp/direct-300g-realbam.json
```

| | throughput | wall | hash | verify | mean disk | vs today |
|---|---|---|---|---|---|---|
| today, single stream | 15.96 MiB/s | 4.97 h | — | — | — | 1.00× |
| before the writer fix | 48.50 MiB/s | 98.1 min | `ok` | 0.0s | 48.87 MiB/s | 3.04× |
| **after the writer fix** | **49.04 MiB/s** | **97.1 min** | **`ok`** | **0.0s** | 49.05 MiB/s | **3.07×** |
| the disk's own ceiling | 88.07 MiB/s | 54.1 min | | | 88.07 MiB/s | 5.52× |

**Report it as 4.97 h → 1.62 h.** That is the number the project exists for, and it is
real: 278.91 GiB, ETag `2872df08129ad09ead7eb25839421345-9849` verified, on the shipping
configuration.

**It is also 3.07×, under §8.5's ≥4×.** Say that plainly rather than quoting §6.2's 4.92×,
which is a 4 GiB figure.

Three things this run settles:

* **Correctness on the real object.** `hash ok` against a 9849-part multipart ETag. Every
  other measurement in this runbook is throughput; this is the only correctness result
  that matters, and it is the release gate.
* **In-transfer hashing (#19) pays for itself completely.** `verify 0.0s` *with* a real
  digest means the ETag was assembled from part md5s recorded as the bytes went past. The
  read-back it replaces is 278.91 GiB at the 91.6 MiB/s read rate from §4.1 — **~52
  minutes**, which would otherwise be over a third of the total. Do not read the 0.0s as
  "verification is cheap"; it is cheap because it was moved into the transfer.
* **Memory is bounded by connections, not by object size.** 86.24 MiB peak RSS on a
  279 GiB object, against 63.64 MiB on 4 GiB at the same connection count.

And one it does not settle, which is now the open question:

**The deferred writer bought +18.2% at 4 GiB and +1.1% here.** 66.49 → 78.59 MiB/s at
prefix size; 48.50 → 49.04 MiB/s at full size. So the `chunk_done` fsync barrier §6.5a
diagnosed was real, was fixed, and was *never the full-size bottleneck* — `chunk_ready` is
now 2.9s over 3283 calls (0% of the worker pool) and `commit` runs 100% of wall entirely
off the pool, yet the object still arrives at 56% of what the device demonstrably absorbs
(49.05 against 88.07 MiB/s) where the 4 GiB prefix reaches 90%.

The degradation is a function of **extent**, and §6.5c separates the two candidates that
remain — a source that is slower for deep cold ranges, versus our own write path degrading
as the sparse file's extent tree grows. Until that is answered, the honest summary is:
**4.97 h → 1.62 h, verified correct, 3.07× against a 4× target, with ~35 minutes of
unexplained headroom against the disk.**

The GCS-composed 300 GB object (`$URL_300G`) was never run: the real BAM is the subject,
its egress is free, and a same-region GCS object measures a source that is not slow.

### 6.4 Correctness against the real sources

Once for each source type, at the chosen connection count. This is correctness, not
throughput, so a smaller object is fine — and where egress is billed, keep it small
deliberately.

**For an S3 source you supply neither `--size` nor `--md5`.** `head-object` already reports
both: the ETag is the md5 for a single-part object and the md5-of-md5s for a multipart one,
and the benchmark derives the size, picks `--check-md5` or `--check-etag --part-length`
accordingly, and recomputes the digest itself to check the result independently.

`$S3_BUCKET`, `$S3_KEY` and `$S3_ENDPOINT` come from §2. A non-Amazon endpoint needs a
scheme on the URL, and a key containing slashes wants quoting — both already handled there.

**Probe the endpoint before transferring anything**, if you skipped that in §3. It costs
one HEAD and one 1 KiB
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

# presigned-URL path -- the preferred one, and what production uses when presign works.
# Pass BOTH: --url carries the transfer, --s3-* supply the metadata. The S3 coordinates
# are used for head-object only, so the size, ETag and part length are still derived
# automatically while the bytes move over plain ranged HTTP.
# `aws` lives in the CONTAINER, not on the node -- probe reports /usr/local/bin/aws
# because probe itself runs via docker exec. And --expires-in because the default is one
# hour, which a multi-setting sweep of a large object can outlast; the sweep would then
# fail partway with a signature error that looks like a source problem.
export PRESIGNED_URL=$(sudo docker exec slurm \
  aws --endpoint-url "$S3_ENDPOINT" s3 presign "s3://$S3_BUCKET/$S3_KEY" \
      --expires-in 43200)
test -n "$PRESIGNED_URL" || echo "presign produced nothing -- check the endpoint and creds"
pdl sweep --url "$PRESIGNED_URL" \
          --s3-bucket "$S3_BUCKET" --s3-key "$S3_KEY" \
          --dest-dir /mnt/rwdisks/$DISK --connections 8 --json /tmp/s3-presigned.json

# NOTE: pass --s3-* only when downloading the WHOLE object, as this example does. They
# make the benchmark derive the ETag from head-object, which describes the full object --
# so a truncated --size assembles a different part count, verify() raises, discard()
# deletes the file, and every row exits 1. For a prefix, omit them (see §6.1). Combining
# them with --prefix is now refused outright rather than left to you to remember: the
# sweep reports NOT VERIFIED with the reason instead of failing every row.

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

How the two are selected, since it is easy to get the wrong one by accident:

| Arguments | Source | Notes |
|---|---|---|
| `--url` alone | `HttpSource` | no size/ETag derivation — you must supply `--size` |
| `--url` **and** `--s3-bucket/--s3-key` | `HttpSource` | transfer over HTTP, metadata from `head-object`. **Use this for the presigned path.** |
| `--s3-bucket/--s3-key` alone | `S3ApiSource` | one `aws` process per chunk |

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

Against the §2 GCS object, if it still exists and you still have its md5:

```bash
pdl resume --url "$URL_12G" --size $SIZE_12G --md5 "$MD5_12G" \
           --dest-dir /mnt/rwdisks/$DISK --connections 8 \
           --json /tmp/resume.json
```

Against the **real source**, which is better — same code path, and it is the object whose
behaviour matters. A prefix carries no digest, so establish one first:

```bash
# on the node. head-object for the true length, so --object-size is not guessed
export OBJ_SIZE=$(sudo docker exec slurm aws --endpoint-url "$S3_ENDPOINT" \
  s3api head-object --bucket "$S3_BUCKET" --key "$S3_KEY" \
  --query ContentLength --output text)
export PREFIX=$((4 * 1024 * 1024 * 1024))

# A. one clean fetch, to get a digest to resume against (~1 min + ~45s of md5)
sudo docker exec slurm python3 /tmp/pdl/parallel_download.py \
  --url "$PRESIGNED_URL" --object-size "$OBJ_SIZE" \
  --dest /mnt/rwdisks/$DISK/ref.bin --size "$PREFIX" --connections 16
export PREFIX_MD5=$(sudo docker exec slurm md5sum /mnt/rwdisks/$DISK/ref.bin | cut -d' ' -f1)
echo "PREFIX_MD5=$PREFIX_MD5"
# the sidecar too: a .k9pdl.done marker outliving its file is the state that makes a
# later run exit 0 without downloading anything
sudo docker exec slurm sh -c 'rm -f /mnt/rwdisks/'"$DISK"'/ref.bin \
                                    /mnt/rwdisks/'"$DISK"'/.ref.bin.k9pdl.*'

# B. the resume run itself
pdl resume --url "$PRESIGNED_URL" --prefix \
           --size "$PREFIX" --md5 "$PREFIX_MD5" \
           --dest-dir /mnt/rwdisks/$DISK --connections 16 \
           --json /tmp/resume.json
```

`--connections 16` rather than §6.5's original 8, to match §6.1/§6.2 so the numbers are
comparable. An explicit `--md5` overrides the `--prefix` ETag guard, which is the one case
where verifying a prefix is possible at all.

SIGKILLs the download at 25%, 50% and 75%, then lets it finish. Check:

* **final md5 CORRECT**;
* **refetched overhead** — the claim is that only the uncommitted tail is refetched, so
  this should be far below the whole-chunk-per-kill figure it prints for comparison;
* whether it says **frontier** or **checkpoint fallback**. On ext4 it should say frontier,
  and if so *this is the first time that code path has ever run* — it fails the SEEK_HOLE
  probe on APFS, so every local test used the checkpoint fallback;
* the **punch-hole** section, which is Linux-only and skipped locally. It simulates bytes
  the process wrote being gone because the VM vanished — the case SIGKILL cannot produce.

### 6.5a Re-measure after the deferred manifest writer

The full-size run reached 48.50 MiB/s against a 97.59 MiB/s disk floor — 3.04×, under the
≥4× target — with `chunk_done` consuming **683 of 982 worker-seconds (70%)**. Per-chunk
completion cost three fsyncs and a rename each, serialised on `Manifest._lock`, and it is
the only cost that scales with the *chunk count* rather than the byte count: 3283 chunks
at full size against 64 at 4 GiB.

Commits are now batched onto a single writer thread. This section re-runs §6.2, §6.5 and
§6.3 against it, in that order — cheapest falsification first.

**Update the scripts before anything else.** Both files changed, and a stale
`parallel_download.py` in the container would measure the old code while the report
claimed otherwise:

```bash
# from your workstation, in the canine repo
gcloud compute scp --project $PROJECT --zone $ZONE \
  canine/test/benchmark_localization.py \
  canine/localization/parallel_download.py \
  $NODE:/tmp/

# on the node
sudo docker exec slurm mkdir -p /tmp/pdl
sudo docker cp /tmp/benchmark_localization.py slurm:/tmp/pdl/
sudo docker cp /tmp/parallel_download.py      slurm:/tmp/pdl/

# the copies must match the workstation's, or you are measuring the old code
sudo docker exec slurm md5sum /tmp/pdl/benchmark_localization.py \
                              /tmp/pdl/parallel_download.py
md5sum /tmp/benchmark_localization.py /tmp/parallel_download.py
```

Then §4.0's preflight, then the sweep. Same object, same range, same destination as §6.2,
so the rows are directly comparable:

```bash
pdl sweep --url "$PRESIGNED_URL" --prefix \
          --size $((4 * 1024 * 1024 * 1024)) \
          --dest-dir /mnt/rwdisks/$DISK --connections 16 \
          --json /tmp/knee-disk-after.json
```

**Read the two bookkeeping lines before the throughput.** The report now prints:

```
chunk_ready: 0.4s over 64 calls (mean 0.006s, 0% of the worker pool)
commit : 12.1s over 9 batches (64 chunks, mean batch 7.1, 3% of wall, off the worker pool)
```

* `chunk_ready` is the worker-side remainder. It should be **near zero** — that is the 70%
  collapsing, and it is the claim under test.
* `commit` runs off the worker pool, so its share is of the wall clock. **`mean batch` is
  the number that says the mechanism ran.** At 1.0 the writer is drained as fast as it is
  filled, nothing was amortised, and the commits are still per-chunk; the report tags that
  `NOT BATCHED` rather than leaving it to be inferred from a throughput figure, which
  cannot distinguish it from the fix working.

Expect throughput near the §4.1 `dd` floor (~97 MiB/s) rather than 72 MiB/s.

Then the resume run, unchanged from §6.5 — **the before-picture is already recorded**:
three kills, four attempts of ~1.01 GiB each, 4.03 GiB total for a 4.00 GiB object,
refetched 32.98 MiB (0.8%), hash CORRECT, SEEK_HOLE frontier recovery, punch-hole 111.80
MiB refetched with the hash still CORRECT.

```bash
pdl resume --url "$PRESIGNED_URL" --prefix \
           --size "$PREFIX" --md5 "$PREFIX_MD5" \
           --dest-dir /mnt/rwdisks/$DISK --connections 16 \
           --json /tmp/resume-after.json
```

**What to expect, and the one thing that would be a real finding.** Batching widens the
window between an fsync and its commit from one chunk to one round, so a preemption can
now lose a batch's worth of *markers* — never bytes. On ext4 the frontier recovers those
from the file's own extents at no cost, so **refetched should still be ~0.8%**. If it has
risen materially, the frontier is not recognising physically-complete chunks whose markers
were lost, which is the case `TestResumeWithMissingDoneMarkers` covers locally and the
only regression this change can plausibly cause. Check `frontier` and not `checkpoint
fallback` in the output before drawing any conclusion from the number.

#### What the node actually measured

```
16  52.12s  78.59 MiB/s  peak NIC 266.54  peak disk 87.78  RSS 63.64 MiB
    chunk_ready: 0.0s over 64 calls (mean 0.000s, 0% of the worker pool)
    commit : 46.6s over 2 batches (64 chunks, mean batch 32.0, 91% of wall)
    streams: 5.32 of 16 concurrent on average
```

The 70% is gone, and `mean batch 32.0` says the mechanism ran rather than being inferred.
Throughput went 72.06 → 78.59 MiB/s, which is 82% → **90% of the device**.

`commit` at 91% of wall is **not** overhead, and reading it as overhead is the easy
mistake here: 46.6s for ~4 GiB is 87 MiB/s, which is exactly `peak disk write`. Workers
write into page cache and the fsync waits for the platter, so that 91% is the disk, not
bookkeeping. `streams 5.32 of 16` is writeback throttling — it is what disk-bound looks
like. **At 4 GiB there is nothing left to win**; the 2× projection only ever applied to
the 3283-chunk full-size run.

Resume went the other way, as predicted but by a different mechanism than the paragraph
above anticipated: **refetched 90.78 MiB (2.2%)**, up from 32.98 MiB (0.8%), hash still
CORRECT, still `frontier recovery`, punch-hole 111.81 MiB and CORRECT. Marker loss is not
the cause — SIGKILL does not drop page cache, so the frontier could have recovered those
for free. The cause is **ext4 delayed allocation**: pages written but not yet fsynced have
no extents, so `SEEK_HOLE` correctly reports them as holes and the frontier rewinds to
before them. The old per-chunk fsync forced allocation every ~87 MiB; it now happens once
per batch. ~30 MiB lost per kill against a ~2 GiB batch window means the frontier is still
recovering nearly all of it, and 2.2% remains 34× better than a broken frontier (~3 GiB
over three kills).

Note that the obvious remedy — a time-bounded flush in the writer — would do nothing. The
writer never idles during a saturated download; it commits back-to-back, so the window is
bounded by *commit duration*, not by a gap between commits, and a timer would never fire.
Shrinking it means **capping batch size**, which trades directly against the batching this
section exists to measure. Read the next subsection before reaching for it.

#### Resume again at the full-size chunk geometry

**Run this before deciding the 2.2% is acceptable.** Every resume measurement so far has
used 64 MiB chunks, because that is what a 4 GiB prefix produces at the default
`--min-chunk`. Production does not: the real object's multipart ETag has 30408704-byte
parts, so the chunk plan rounds up to `3 × 30408704 = 91226112` bytes (87.0 MiB), which is
how 278.91 GiB becomes 3283 chunks. Uncommitted in-flight bytes scale with
`connections × chunk`, so 16 × 87 MiB is 1.4 GiB in flight against 16 × 64 MiB's 1.0 GiB —
a 1.4× wider exposure that the 4 GiB run never exercised.

This is minutes on the same 4 GiB prefix, and it is the cheap way to find out whether the
number generalises before spending 279 GiB of egress on the assumption that it does:

```bash
pdl resume --url "$PRESIGNED_URL" --prefix \
           --size "$PREFIX" --md5 "$PREFIX_MD5" \
           --dest-dir /mnt/rwdisks/$DISK --connections 16 \
           --min-chunk 91226112 \
           --json /tmp/resume-after-87m.json
```

**How to read it, and why the percentage is the wrong number.** 2.2% is a ratio from a
4 GiB object killed three times; the quantity that transfers to production is **loss per
preemption**, which was ~30 MiB (90.78 MiB over 3 kills) against ~11 MiB before batching.
On a 279 GiB localization that is 0.010% versus 0.004% — about a quarter of a second at
78 MiB/s.

* **~30 MiB per kill again** → the residual is the last fraction of a second of dirty
  pages, independent of chunk size, and background writeback is allocating extents far
  ahead of the fsync. Nothing to fix; a batch-size cap would buy back a quarter-second per
  preemption and re-couple commit cost to chunk count, which is the thing the deferred
  writer removed.
* **~40 MiB per kill (scaling with 1.4× the chunk)** → the window really is set by
  in-flight chunk bytes. Still small, but it means the cost grows with chunk size, so
  revisit if the chunk plan ever gets much coarser.
* **Hundreds of MiB per kill** → a genuine finding, and the case for bounding the batch
  explicitly. Check `frontier` and not `checkpoint fallback` first: a silent fallback would
  produce exactly this and has nothing to do with batching.

One caveat on the report's own arithmetic: the "a per-attempt loss of a whole chunk would
show up as roughly N" line is computed as `(attempts - 1) × min_chunk × connections`, which
at this geometry is 4.2 GiB — larger than the 4 GiB object. That comparison degenerates
here; read the refetched bytes directly and ignore the projected budget.

**Keep this separate from the preemption question it resembles.** SIGKILL does not drop
page cache, so it cannot produce the case that actually matters on preemptible VMs: the
machine vanishing with dirty pages unwritten. That is the punch-hole run, it refetched
111.81 MiB, and it is **unchanged** by batching — roughly 4× the delalloc delta. If the
goal is reducing what a preemption costs, that is the number to attack, and batch size is
not the lever.

#### §4.1d's hashing question, against the real code path

The benchmark's own ETag re-read roughly doubles the wall time of every setting, and
§4.1d bounds the prize from threading it at ~15%. This asks the same question of the
actual implementation rather than `dd`. `$PRESIGNED_URL` and `$PREFIX_MD5` are both
defined by now, which is why it lives here:

```bash
for W in 1 2 4; do
  echo "--- verify-workers $W"
  pdl sweep --url "$PRESIGNED_URL" --prefix --size "$PREFIX" \
            --md5 "$PREFIX_MD5" \
            --dest-dir /mnt/rwdisks/$DISK --connections 16 --verify-workers $W \
            --json /tmp/vw-$W.json
done
```

`--md5` is not optional here. Without a digest the verify phase hashes nothing and every
row reports 0.0s — the §6.2 trap, where `verify 0.0s` meant "no verification happened",
not "verification is free". A sweep of `--verify-workers` against no digest would produce
three identical zeros and look like a clean negative result.

Read the `verify` phase, not the throughput: `--verify-workers` cannot touch the download.
If all three are within a few percent, readahead was already overlapping the hash and
`VERIFY_READ_WORKERS` could as well be 1. The hash itself must be identical at every
width; if it is not, stop — that is a correctness bug in the hasher, not a tuning result.

#### Before the full-size run: re-copy the harness

`benchmark_localization.py` had a **Popen/PIPE deadlock**. `run_download` polled the child
without reading a byte of its stderr until it exited. A pipe holds 64 KiB and the
downloader logs a ~70-byte progress line every 5s, so after ~900 lines — about **78
minutes** — the child blocks in `write()` forever and the loop polls a process that can
never exit. The threshold sits under the full-size run and over every other measurement,
which is why sweeps, resume runs and probes never hit it.

This cost an overnight 279 GiB run: no row, no `--json`, and a destination that had
reached ~97% and stopped. It is a harness bug, not a downloader bug — the download itself
was nearly finished. Note honestly that it is **not understood why the earlier ~98-minute
pre-fix full-size run survived the same harness**; it may have been invoked directly
rather than through `pdl sweep`.

stderr is now drained on its own thread, and a progress line is echoed at most once every
five minutes so a multi-hour run is distinguishable from a wedged one. **Re-copy both
scripts (the block at the top of this section) before the full-size run.**

Also decide what to do with the stale destination first. The overnight run left ~278 GiB
on disk — `alloc` was 542276216 × 512B blocks against a 299481061742-byte object, so it
was **~97% complete** when the harness wedged. `pdl sweep` unlinks `bench.<conns>.bin` and
its `.k9pdl.*` sidecars before each setting, so there is no ENOSPC risk, but there is also
**no way to resume through `sweep`** — it will throw those bytes away and re-pay the full
279 GiB of egress.

`parallel_download.py` was not changed by the harness fix, so that file and its sidecar
manifest are still valid. Finishing the tail directly costs ~2 GiB instead of 279 GiB and
answers the two questions that do not need a clean timing — does `verify` stay at 0.0s,
and does the hash match:

```bash
sudo docker exec slurm python3 /tmp/pdl/parallel_download.py \
  --url "" --s3-bucket "$S3_BUCKET" --s3-key "$S3_KEY" \
  --s3-extra-args="--endpoint-url $S3_ENDPOINT" \
  --dest /mnt/rwdisks/$DISK/bench.16.bin \
  --size 299481061742 --connections 16 --min-chunk $((64 * 1024 * 1024)) \
  --check-etag 2872df08129ad09ead7eb25839421345-9849 --part-length 30408704
```

That run is **not** a throughput measurement — it starts 97% done. It is a free check that
the deferred writer's part digests survived a SIGKILL, which is exactly the failure mode
the full-size run is meant to rule out. Do it before the timed sweep, which will delete
the file regardless.

Note that `pkill -f benchmark_localization.py` does **not** kill the downloader it spawned
— that is `python3 .../parallel_download.py` and has to be matched separately. Check for
both before starting anything:

```bash
sudo docker exec slurm sh -c \
  'ps -eo pid,etime,args | grep -E "[p]arallel_download|[b]enchmark_localization"'
```

Finally the full-size run, the number the target is judged against:

```bash
pdl sweep --s3-bucket "$S3_BUCKET" --s3-key "$S3_KEY" \
          --s3-endpoint-url "$S3_ENDPOINT" \
          --dest-dir /mnt/rwdisks/$DISK --connections 16 \
          --json /tmp/direct-300g-realbam-after.json
```

~98 minutes before; ~49 expected, though the 4 GiB row above suggests the disk will cap it
well short of that. Three things to record:

* `mean batch` here, not just in the sweep — 3283 chunks is where batching matters, and a
  batch size that is healthy at 64 chunks says nothing about 3283.
* `verify 0.0s` must **persist**. The in-transfer part digests are recorded in the same
  manifest write as the done markers now; if verify has become non-zero, the digests are
  not surviving and a 279 GiB read-back is back. The `etag from N recorded part digests,
  M re-read` line says which.
* hash ok against `2872df08…-9849`, the same multipart ETag the pre-fix run matched.

### 6.5b HTTP vs the S3 API, at prefix size

This is the one measurement that decides whether the project meets its target, and after
the `--prefix` fix it costs about four minutes.

The full-size run above reached **49.04 MiB/s** with 15.47 of 16 streams busy — **3.17
MiB/s per stream**. The 4 GiB presigned run reached 78.59 MiB/s with 5.32 streams busy —
**14.77 MiB/s per stream**, 4.6× better. Two explanations fit the same numbers:

* **the transport.** `S3ApiSource` shells out to `aws s3api get-object` per chunk, with no
  connection pooling — every chunk pays TCP and TLS setup. `HttpSource` holds its
  connections open.
* **the object, or the size.** The presigned run fetched the first 4 GiB; the S3 run
  fetched all 279 GiB. A prefix sits in whatever the store caches; the tail does not.

Per-stream throughput cannot separate those, because the two runs differed in *both*
transport and extent. The A/B below holds the object, the extent and the destination fixed
and changes only the transport, which is the only way to attribute the 4.6×.

Both rows fetch the same 4 GiB prefix, so `--md5 "$PREFIX_MD5"` from §6.5 verifies both.
Without a digest, `verify` reports 0.0s because it hashed nothing — the §6.2 trap.

```bash
# S3 API path -- the one the full-size run measured
pdl sweep --s3-bucket "$S3_BUCKET" --s3-key "$S3_KEY" \
          --s3-endpoint-url "$S3_ENDPOINT" \
          --prefix --size "$PREFIX" --md5 "$PREFIX_MD5" \
          --dest-dir /mnt/rwdisks/$DISK --connections 16 \
          --json /tmp/ab-s3api.json

# presigned HTTP path, same bytes of the same object
pdl sweep --url "$PRESIGNED_URL" \
          --prefix --size "$PREFIX" --md5 "$PREFIX_MD5" \
          --dest-dir /mnt/rwdisks/$DISK --connections 16 \
          --json /tmp/ab-http.json
```

**Check the `streams` line on the S3 row before reading anything else.** `--prefix` was
silently ignored on the S3 path until the fix above: `probe_range` compared the object's
full 279 GiB against `--size`, decided Range was not honoured, and ran a single stream
while the header still said 16. If the S3 row reports ~1.0 of 16 concurrent, the node is
running a stale harness — re-copy both scripts with §6.5a's block and confirm the node's
md5s match the workstation's, then start over. Every other number on that row would look
like a healthy transfer, because it is one; only `streams` gives it away.

What the result means:

* **HTTP much faster at the same extent** — the transport is the cause, and the fix is
  connection reuse in `S3ApiSource` (or routing S3 through presigned URLs). The target is
  reachable: 78.59 MiB/s is already ~3.7× the 21 MB/s baseline.
* **both around 49 MiB/s** — the transport is exonerated and the earlier 78.59 was a
  caching artifact of the prefix. The ceiling is the store, and no client-side change
  reaches 4×. Say so plainly rather than tuning against it.
* **both around 79 MiB/s** — the extent is what matters, not the transport, and the
  full-size number is bounded by something that only appears deep into the object. Neither
  A/B row would show it; that needs the full-size presigned run in §6.4.

Egress is free on this object, so the full-size presigned run is also affordable if the
prefix A/B comes out ambiguous. Use §6.4's combined form — passing both `--url` and the
S3 coordinates keeps ETag verification while transferring over HTTP:

```bash
pdl sweep --url "$PRESIGNED_URL" \
          --s3-bucket "$S3_BUCKET" --s3-key "$S3_KEY" \
          --s3-endpoint-url "$S3_ENDPOINT" \
          --dest-dir /mnt/rwdisks/$DISK --connections 16 \
          --json /tmp/direct-300g-http.json
```

Mint the URL with `--expires-in 43200`: at 49 MiB/s the run is ~98 minutes, and a URL that
expires mid-transfer surfaces only as a retry count, because `S3ApiSource` sends its
child's stderr to `DEVNULL`.

#### Measured

Both arms: 4 GiB prefix of the same object, `--md5 $PREFIX_MD5`, same disk, 16 connections,
minutes apart. `hash ok` on both.

| | download | total | streams | per stream | verify | commit | mean disk |
|---|---|---|---|---|---|---|---|
| `S3ApiSource` | 75.29 MiB/s | 62.64 MiB/s | 8.46 of 16 | 8.90 | 10.7s | 46.3s, 3 batches, 85% of wall | 62.66 (71% of peak) |
| `HttpSource` | **80.47 MiB/s** | **67.59 MiB/s** | 5.17 of 16 | 15.56 | 9.2s | 46.6s, 3 batches, 92% of wall | 67.61 (77% of peak) |

**6.9% apart on the download phase.** Shelling out to `aws s3api` per chunk is measurably
worse, and the presigned path is the right default for the reasons already in §6.1 — but
5 MiB/s is not the 4.6× the per-stream figures above predicted, and it is not where the
target lives. The transport is exonerated; §6.5c takes the question from here.

Note the inversion, because it is the whole lesson: the *slower* arm reports *more* streams
busy (8.46 against 5.17). `chunk_ready` is 0% of the worker pool in both, so nothing is
blocked on the commit thread — the extra stream-time is `aws` process startup and TLS
handshake, which `open_range` pays inside the counted region. A higher `streams` figure is
not a healthier run.

### 6.5c The transport is not the problem — find what actually is

§6.5b ran and **exonerated the transport**. Same object, same 4 GiB prefix, same
destination, only the source class differs:

| run | download | streams | per stream |
|---|---|---|---|
| S3ApiSource, 4 GiB prefix | 75.29 MiB/s | 8.46 | 8.90 |
| HttpSource, 4 GiB prefix | 80.47 MiB/s | 5.17 | 15.56 |
| S3ApiSource, 279 GiB full | 49.04 MiB/s | 15.47 | 3.17 |

**6.9% apart at equal extent.** Shelling out to `aws s3api` per chunk costs something, but
not the 4.6× the per-stream figures implied. Connection pooling in `S3ApiSource` is worth
at most those 5 MiB/s, which is not where the target lives.

**Do not use per-stream throughput to compare sources.** It is `aggregate ÷ streams`, and
both terms move with the *destination*, not the source:

* `streams` counts time a stream is **open**, not time it is **delivering**. Every `aws`
  process spawn, TLS handshake and — crucially — every moment a worker sits blocked on a
  full write path counts as busy. So backpressure *inflates* the denominator.
* When the disk is the binding constraint, the numerator is pinned regardless.

The two prefix rows make the trap explicit: 8.46 × 8.90 and 5.17 × 15.56 are the same
product from wildly different factors. And the full-size row's 15.47 of 16 is not a sign of
health — it is the signature of sixteen workers blocked on the write path. That reading is
what sent this investigation after the transport; it was wrong, and the A/B cost four
minutes to say so.

**The constant worth explaining is `peak disk write`: 87.78, 88.07, 87.87, 87.94 MiB/s
across four runs spanning 4 GiB to 279 GiB — a 0.33% spread.** That is a real device
ceiling, not a coincidence. Every run reaches it; the question is only what fraction of the
time each sustains it. The prefix runs hold 71–77% of it. The full-size run holds 56%.

So the gap is **extent**, not transport, and two candidates remain. Both are local:

* **The source is slower for deep, cold ranges.** A 4 GiB prefix is the object's head,
  which is what every client touches and what any cache in front of the store will hold.
  250 GiB in is cold.
* **Our write path degrades as the file grows.** The destination is created sparse with
  `ftruncate` and chunks land **out of order** (see the header notes at the top of
  `parallel_download.py`). Out-of-order writes into a sparse file build an extent tree
  whose update cost grows with the number of extents, and the filesystem was at 90% full
  by the end of the full-size run, where ext4's allocator works hardest. Neither effect is
  visible in 4 GiB.

Note the second one trades directly against resume: `posix_fallocate` would give
contiguous extents, but its unwritten extents read back as data, which destroys the
`SEEK_HOLE` frontier recovery §6.5 measured working. Do not "fix" it before measuring it.

**A third candidate, already eliminated — do not re-derive it.** ENOSPC waiting looks
exactly like this: the disk finished the run 90% full, and `_await_space` blocks a worker
for up to 600s at a time, which would present as a stall with no throughput and no error.
It did not happen. The 5823s run used the drained-stderr harness, which echoes ENOSPC
markers (§6.5a), and printed none. Ruled out on evidence rather than on argument.

**A fourth, still open and cheap to check while you are in there:** does the frontier scan
itself scale with the file? `chunk_frontier()` walks extents with `SEEK_HOLE`/`SEEK_DATA`,
and a scan whose cost grows with the number of extents would compound with the same
fragmentation the second candidate proposes — the two would be indistinguishable from the
outside and would need separating by where the time lands, not by the total.

**Corroborating detail for the extent hypothesis:** `commit`'s share of wall clock rises
with extent — 85%, 91%, 92% across the three 4 GiB runs and **100%** at 279 GiB. The
commit thread is off the worker pool in all four, so this is not backpressure on the
workers; it means the single writer thread never once caught up at full size. Whatever is
slow is slow *in the write path*, not in the workers waiting on it.

**One unexplained constant, noted rather than theorised.** `peak NIC` across five runs
spanning 4 GiB to 279 GiB: 266.54, 267.98, 268.21, 268.34, 268.66 MiB/s — a **0.80%**
spread, which is 2.25 Gbit/s. The benchmark annotates that line with "n1-standard-8 cap is
~2 GB/s", and 268 MiB/s is **eight times below** that. Two readings, and this runbook
should not pick one without evidence: either the instance's real egress ceiling is nothing
like the quoted figure, or `Sampler`'s tick quantisation is manufacturing the constant. It
matters only if some future change lifts the disk out of the way — at 88 MiB/s of disk
nothing is near it — but a number that steady across a 70× range of object sizes is not
noise, and should not be quoted as headroom until someone checks which it is.

**A caveat on `filefrag`:** the 279 GiB destination was deleted by the sweep's own
cleanup, so fragmentation cannot be measured after the fact. It has to be sampled
*during* a long run, or the run has to be repeated with `--keep`.

This test separates them for about two minutes of free egress, by **removing the disk
entirely** — sixteen ranged GETs straight to `/dev/null`, at the head and then 250 GiB in:

```bash
cat > /tmp/source_ceiling.sh <<'EOS'
#!/bin/sh
# Sixteen parallel ranged GETs to /dev/null: the source's rate with no write path at all.
# 256 MiB x 16 = 4 GiB per pass, so each pass is directly comparable to a 6.5b row.
U="$1"
CHUNK=268435456
for BASE in 0 268435456000 268435456000; do    # head, deep, deep again (cache check)
  START=$(date +%s)
  i=0
  while [ "$i" -lt 16 ]; do
    OFF=$((BASE + i * CHUNK))
    curl -sS --fail -r "$OFF-$((OFF + CHUNK - 1))" "$U" -o /dev/null &
    i=$((i + 1))
  done
  wait
  EL=$(( $(date +%s) - START ))
  [ "$EL" -eq 0 ] && EL=1
  echo "base=$BASE  ${EL}s  $((4096 / EL)) MiB/s"
done
EOS
sudo docker cp /tmp/source_ceiling.sh slurm:/tmp/pdl/
sudo docker exec slurm sh /tmp/pdl/source_ceiling.sh "$PRESIGNED_URL"
```

Read it against the 88 MiB/s device ceiling, not against each other:

* **Head ≫ 88 MiB/s (say 200+)** — the source has headroom the disk never let us use, and
  the destination is the whole story. Nothing in the download path is worth tuning; a
  faster PD (or pd-ssd) is the entire remaining win, which is a provisioning decision
  rather than a code one.
* **Head ≈ 80 and deep ≈ 49** — the store is slower for cold ranges and 49 MiB/s is close
  to the ceiling for this object. The target is not reachable by any client-side change,
  and that should be reported plainly rather than tuned against.
* **Head ≈ deep, both well above 88** — the source is uniform and fast, so the degradation
  is ours: the extent-tree/allocator hypothesis. That is the only branch where a code
  change helps, and `filefrag -v` on the destination after a long run is the confirmation.

The third pass repeats the deep range. If it is much faster than the second, something is
caching and neither number describes a cold localization.

#### Measured — the source is exonerated

```
base=0              18s  227 MiB/s
base=268435456000   17s  240 MiB/s   (~250 GiB in)
base=268435456000   16s  256 MiB/s   (repeat)
```

Timing is whole seconds on ~17s, so ±6%: **these are one number, not a trend.** Call it
~240 MiB/s at any depth.

That is the third branch above, and it settles the question:

* **No cold-range penalty.** Deep is if anything *faster* than the head. A store that
  served its cached object head quickly and cold interior slowly would show the opposite.
* **No caching artifact.** If a cache explained the deep number, pass 2 would be slow and
  pass 3 fast. Passes 2 and 3 both beat pass 1.
* **The source has 2.6× more headroom than the disk can take** (240 against 88.07 MiB/s)
  and **4.6× more than the full-size run achieved** (against 49.04).
* **It agrees with §6.1 by a completely different route.** 227–256 MiB/s to `/dev/null`
  with `curl` and no canine code at all, against 227–230 MiB/s to tmpfs through the
  downloader. Two independent measurements of the same ceiling.

**So the ~35 minutes §6.3 leaves unexplained is ours, in the write path.** Of the four
candidates, the source is now eliminated alongside ENOSPC, and the two that remain are
both local and both scale with extent: the sparse file's extent tree, and the
`SEEK_HOLE` frontier scan that walks it.

#### §6.5d Separate extent depth from disk fullness — no network, ~3 minutes

The two survivors are still confounded, because the full-size run changed both at once: it
wrote a 279 GiB sparse file **and** filled the disk to 90%. `dd` on the disk *now* cannot
tell them apart — the sweep deleted its 279 GiB destination, so the disk is empty again and
`dd` would only re-measure §4.1.

Write the same 4 GiB of real data into two sparse files of very different apparent size,
out of order, and the only variable left is extent depth:

```bash
sudo docker exec slurm sh -c '
  D=/mnt/rwdisks/'"$DISK"'
  for SIZE_GIB in 4 279; do
    F=$D/extent.$SIZE_GIB.bin
    rm -f $F; truncate -s ${SIZE_GIB}G $F
    # 64 x 64 MiB at descending offsets: out of order, spread across the whole extent
    START=$(date +%s)
    i=63
    while [ $i -ge 0 ]; do
      OFF=$(( i * (SIZE_GIB * 1024 / 64) ))
      dd if=/dev/zero of=$F bs=1M count=64 seek=$OFF conv=notrunc,nocreat 2>/dev/null
      i=$((i - 1))
    done
    sync
    echo "apparent ${SIZE_GIB}GiB: $(( $(date +%s) - START ))s for 4 GiB out of order"
    filefrag $F
    rm -f $F
  done'
```

Both passes write 4 GiB and leave the disk equally empty, so fullness is held constant.

* **279 GiB pass much slower, and `filefrag` reports far more extents** → the extent tree
  is the cause, and `posix_fallocate` is the lever — at the cost of `SEEK_HOLE` resume
  (see the caveat above), so the manifest would have to become the primary recovery
  mechanism rather than the backstop.
* **Both passes the same** → extent depth is innocent, and what remains is disk fullness
  or the frontier scan. Fullness is then testable by filling the disk to 90% with a
  ballast file and repeating §4.1's `dd`; the frontier scan by timing `chunk_frontier()`
  directly, since it is the only other thing that walks the tree.

Note this measures `dd`'s sequential-per-chunk writes, not the downloader's interleaved
ones, so it is a **lower bound** on the effect: sixteen workers writing concurrently at
scattered offsets can only fragment more than one process writing 64 MiB at a time.

#### Measured — and the premise of §6.5c/§6.5d was wrong

```
apparent   4GiB: 54s for 4 GiB out of order   5 extents
apparent 279GiB: 49s for 4 GiB out of order   5 extents
```

**Read the first arm's stride before reading anything else.** `4 * 1024 / 64` is 64 MiB,
exactly the write size, so that arm tiled `0..4096 MiB` with no gaps — it wrote
contiguously in descending order, which ext4 coalesces. It is a control, not an arm, and
5 extents is the correct answer for it.

The second arm did scatter, and `filefrag -v` proves it: 4464 MiB logical stride, 64 MiB
writes, `du` 279G apparent against 4.1G allocated. "5 extents" is `filefrag` counting
**physical fragments**, not logical islands — the physical offsets run contiguously
(`50651136`, `50667520`, `50683904`, …), so ext4 packed all 64 logically-scattered islands
into one packed physical run. Logical scatter costs nothing here: 84 MiB/s against the
control's 76.

**But the whole line of inquiry rested on a degradation that does not exist.** The
full-size run's own heartbeat, in the output already pasted into this runbook, gives the
bytes between successive progress lines:

```
32.5  27.6  18.5 | 14.0 13.9 14.1 13.8 13.9 13.9 14.0 13.8 13.8 13.9 14.0 14.0 13.9 14.0 13.9   GB
```

`HEARTBEAT_INTERVAL` is 300 s and the gate is `now - last_echo < echo_every`, so these are
time intervals and the deltas are genuine rates:

```
103.3  87.9  58.9 | 44.4 44.1 44.7 44.0 44.1 44.1 44.5 43.9 43.9 44.2 44.5 44.6 44.2 44.5 44.1   MiB/s
```

**It decays for three intervals and then holds flat to 1.7% for the remaining fifteen —
75 minutes.** Correcting an earlier reading of this data as flat throughout: the decay is
real, and it is the page-cache settling curve. What matters is where it settles and for how
long. Disk fullness went from roughly 10% to 90% *during the flat stretch*, and extent depth
grew the whole way; neither can produce 1.7% of variation across 75 minutes. Both are
eliminated, for free, by data collected before either hypothesis existed.

And the first three intervals are the explanation for everything else: **32.5 + 27.6 GB
absorbed before writeback caught up, on a node with 28 GB of RAM.** A 4 GiB prefix run
never leaves that regime — the file fits in page cache several times over and the process
exits before the disk has seen most of it. So:

* **The prefix runs' 75–80 MiB/s is not a disk rate**, and §6.2's "90% of the device" is
  an artifact of measuring memory. `dd conv=fdatasync` in §4.1 forces the flush and is
  honest; the sweep does not, and cannot, because the downloader's job ends at `close()`.
* **There was never a 4 GiB-vs-279 GiB degradation to explain.** There is one cache-
  assisted short measurement and one true sustained measurement, which were never
  comparable. §6.5c's source test was still worth its two minutes — it independently
  confirmed 240 MiB/s of source headroom — but the question it was built to answer was
  malformed.

#### §6.5e The only question left: what does this disk do under *our* write pattern?

49 MiB/s sustained is the real number. §4.1's 92.3 MB/s is **one sequential writer with
`conv=fdatasync`**; the downloader is sixteen concurrent writers at scattered offsets with
a periodic full-file `fsync` underneath. §4.1a already found this disk losing throughput
to concurrency on *reads* — 86/85/76/62 MiB/s at 1/2/4/8 — so the pattern is the obvious
suspect and it has never been measured for writes.

```bash
sudo docker exec slurm sh -c '
  D=/mnt/rwdisks/'"$DISK"'
  START=$(date +%s); i=0
  while [ $i -lt 16 ]; do
    dd if=/dev/zero of=$D/conc.$i.bin bs=1M count=1024 conv=fdatasync 2>/dev/null &
    i=$((i + 1))
  done
  wait
  echo "16 concurrent writers: $(( $(date +%s) - START ))s for 16 GiB"
  rm -f $D/conc.*.bin
  START=$(date +%s)
  dd if=/dev/zero of=$D/seq.bin bs=1M count=16384 conv=fdatasync 2>/dev/null
  echo "1 sequential writer  : $(( $(date +%s) - START ))s for 16 GiB"
  rm -f $D/seq.bin'
```

`conv=fdatasync` on both arms, so neither can hide in page cache. 16 GiB each, ~6 minutes
total, no network.

* **Concurrent ≈ 49 MiB/s, sequential ≈ 88** → the disk simply is this slow under our
  access pattern, nothing in the download path is broken, and 1.62 h is the honest answer
  for pd-standard. The lever is the disk type or fewer, larger sequential writes — a
  design change with its own resume cost, not a bug fix.
* **Both ≈ 88** → the disk handles our pattern fine and the loss is inside the downloader
  after all, with the periodic full-file `fsync` as the remaining suspect: it is the one
  thing `dd` does not reproduce.

#### Measured — the disk is fine; the loss is ours

```
16 concurrent writers: 196s for 16 GiB   ->  83.6 MiB/s
1 sequential writer  : 191s for 16 GiB   ->  85.8 MiB/s
```

Within 3% of each other and both within 5% of §4.1's `dd`. **Write concurrency costs this
disk nothing** — the reads-degrade-with-concurrency result in §4.1a does not transfer, and
the suspicion that pd-standard simply could not take sixteen writers is dead. The device
delivers ~84 MiB/s under our concurrency and the downloader gets **49.04**, a **1.70×** gap
that is now unambiguously inside our own code.

**And this test has a gap that names the next suspect.** Sixteen `dd`s write sixteen
*separate files* — sixteen inodes, sixteen independent locks, sixteen independent
writeback contexts. The downloader writes sixteen streams into **one** file, with the
commit thread calling `fsync()` on that same inode underneath them. On ext4 an `fsync`
takes the inode's lock and forces a journal commit; concurrent `pwrite`s to the same inode
stall behind it. Nothing in §6.5e reproduces that, and it is consistent with every number
we have: `commit` occupying 100% of wall at full size, and `streams` reading 15.47 of 16
"busy" while each delivers only 3.17 MiB/s — workers blocked *inside* `pwrite`, which the
stream accounting counts as busy (§6.5c).

#### §6.5f One inode or sixteen — the last cheap discriminator

Identical bytes and concurrency to §6.5e, changing only the number of inodes:

```bash
sudo docker exec slurm sh -c '
  D=/mnt/rwdisks/'"$DISK"'
  F=$D/single.bin
  rm -f $F; truncate -s 16G $F
  START=$(date +%s); i=0
  while [ $i -lt 16 ]; do
    dd if=/dev/zero of=$F bs=1M count=1024 seek=$(( i * 1024 )) \
       conv=notrunc,nocreat,fdatasync 2>/dev/null &
    i=$((i + 1))
  done
  wait
  echo "16 writers, ONE file: $(( $(date +%s) - START ))s for 16 GiB"
  rm -f $F'
```

Each `dd` carries `fdatasync`, so sixteen flushes land on one inode while fifteen other
writers are mid-write — the downloader's pattern without any of its code.

* **≈ 196s (84 MiB/s)** → the inode is not the contention point either, and the remaining
  difference is something only the downloader does: the `SEEK_HOLE` frontier scan per
  chunk, the in-transfer md5, or the manifest write itself. Instrument `PosixChunkSink.write`
  the way `chunk_done` was instrumented in §6.5a rather than guessing again.
* **Materially slower, approaching ~330s (49 MiB/s)** → single-inode `fsync` contention is
  the whole gap, and the fix is structural: fewer full-file `fsync`s (the commit thread
  already batches — batch harder), `sync_file_range` on just the committed extents instead
  of `fsync` on the whole inode, or the stage-publish route's separate files.

Either way this is the last test that needs no code. After it, the answer is in the
downloader and wants instrumentation, not a shell script.

#### Measured — the inode costs 5%, not 70%

```
16 writers, ONE file: 206s for 16 GiB  ->  79.5 MiB/s
```

against 196 s / 83.6 MiB/s for the same sixteen writers across sixteen files. **One shared
inode costs 5%.** Single-inode `fsync` contention is real and is not the gap: closing it
entirely would buy 5 points of the 41 that are missing.

So the disk, measured under the downloader's own access pattern — sixteen concurrent
writers, one inode, `fdatasync` underneath — delivers **79.5 MiB/s**, and the downloader
settles at **44.2 MiB/s: 56% of it.** Every external explanation is now eliminated:

| candidate | verdict | evidence | extent |
|---|---|---|---|
| the source | eliminated | ~240 MiB/s at any depth, §6.5c | 250 GiB deep ✅ |
| ENOSPC waiting | eliminated | no ENOSPC markers in the drained stderr, §6.5c | full run ✅ |
| logical scatter / extent tree | eliminated | 84 vs 76 MiB/s, 5 physical fragments, §6.5d | 4 GiB ⚠️ |
| disk fullness | eliminated | 1.7% variation across 10%→90% full, §6.5d | 4 GiB arms ⚠️ |
| write concurrency | eliminated | 83.6 vs 85.8 MiB/s, §6.5e | 16 GiB ⚠️ |
| shared inode + `fsync` | 5% of the 44% gap | 206 s vs 196 s, above | 16 GiB ⚠️ |

**Read the right-hand column before trusting the middle one.** Four of these six
eliminations were measured over 4–16 GiB, and §6.5h found that this device delivers
88 MiB/s for its first few minutes and 44 MiB/s thereafter. So the ⚠️ rows are eliminating
candidates in a regime the full-size run spends 3% of its time in — not because the page
cache inflated them (§6.5h measured the cache *costing* 13%, refuting that) but because
they were too short to leave the burst.

**The hypothesis below was tested in §6.5g and is false.** It is kept because the
reasoning is worth having on record next to its refutation, and because the two loose ends
noted at the end of it were the tell.

**The leading hypothesis is now the read loop's structure, and it is a hypothesis.**
`download_chunk` reads a block and then writes it, in the same thread: `buf =
stream.read(want)` followed by `self.sink.write(...)`. Network and disk never overlap
*within* a worker, so each worker's rate is `1/(1/r_net + 1/r_disk)` — the same series
effect the `VERIFY_READ_WORKERS` comment records for read-then-hash. At the aggregate that
is `1/(1/240 + 1/79.5) = 59.7 MiB/s`, and the measured 44.2 sits **below even that**, so
if this is the mechanism something else is stacked on top of it.

Two loose ends that argue against banking it as the answer: the 4 GiB prefix rows reach
79.8 MiB/s, which this model cannot produce, and the settled 44.2 is 26% under the model's
own ceiling.

Both loose ends were the real signal. A measurement above the model's ceiling and a
measurement below it, at the same time, means the two are not measuring the same system —
which is what §6.5g/§6.5h converged on: the 79.8 is a short-extent burst figure and the
44.2 is what the device sustains. **When a model is bracketed by its own data, suspect the
data's provenance before refining the model.**

**Further shell tests are exhausted.** The next measurement belongs inside the downloader,
and it now exists.

#### §6.5g The read/write split — `k9pdl-io`

`download_chunk` now times `stream.read()` and `sink.write()` separately and emits:

```
k9pdl-io read 1234.567s write 2345.678s other 12.345s over 285621 blocks
         (read 34% of loop, overlap ceiling 1.53x)
```

and the sweep prints it as an `io :` line beside `streams:` and `commit :`. Nothing to
run but the sweep itself — re-upload both scripts and repeat §6.3.

How to read it:

* **`read %`** is the share of worker-loop time the network held the disk idle for. Both
  halves run on the same thread in sequence, so a worker is never doing both at once.
* **`overlap ceiling`** is `(read + write) / max(read, write)` — what a reader/writer split
  with a queue between them could recover. It is **bounded by 2.0**, at `read == write`,
  because a queue can only hide the smaller half behind the larger one. It is *not*
  `1/(1 - read share)`: that form assumes reads become free, is unbounded, and printed
  infinity the first time a test wrote to a fast local disk.
* **`other`** is the rest of the loop — the progress accounting. Note the in-transfer md5
  lives inside `sink.write()` on the in-place route, so #19's cost is counted under
  `write`, not separately.

Against the full-size numbers (44.2 MiB/s settled, 79.5 available):

* **ceiling ≈ 1.8x or better, with `read %` near 45** → read/write serialisation is the
  gap, and decoupling the loop recovers most of it. The fix is a bounded queue between a
  reader pool and a writer pool, and it is worth ~35 minutes per BAM.
* **ceiling ≈ 1.1x, `write` dominating** → the workers are simply waiting on the disk,
  and since §6.5f showed the device delivering 79.5 MiB/s under this exact pattern, the
  loss is between `pwrite` returning and bytes reaching the platter — writeback
  throttling, which is a `dirty_ratio` question, not a code one.
* **`other` large** → neither; look at the progress accounting itself.

The instrumentation is four `time.time()` calls per 1 MiB block, accumulated per-chunk
rather than per-block so the shared lock is taken ~3283 times on a 279 GiB object instead
of ~285000. A test pins that a 32 MiB local download stays under 30s with it on, because
an instrument that changes what it measures is worse than none.

Pair it with the destination in isolation, on the disk in its current state:

```bash
sudo docker exec slurm sh -c \
  'dd if=/dev/zero of=/mnt/rwdisks/'"$DISK"'/ddtest bs=1M count=8192 conv=fdatasync 2>&1 \
   | tail -1; rm -f /mnt/rwdisks/'"$DISK"'/ddtest'
```

That is sequential, so it is an **upper bound** on what our out-of-order sparse writes can
achieve — if even `dd` only reaches ~50 MiB/s on the now-90%-full filesystem, the write
path is exonerated too and the disk simply is what it is.

#### Measured — serialization is not the gap, and 95% of the loop is `pwrite`

```
16   5818.82   49.08 MiB/s   250.94 MiB/s peak NIC   88.00 MiB/s peak disk   ok
     phases: download 5818.3s  verify 0.0s
     streams: 15.40 of 16 concurrent on average
     io     : read 4651.5s / write 84948.8s / other 4.4s (5% read, overlap ceiling 1.05x)
     chunk_ready: 8.0s over 3283 calls (0% of the worker pool)
     commit : 5811.9s over 2 batches (3283 chunks, 100% of wall, off the worker pool)
     wire   : 1.01x payload (280.56 GiB on the NIC)
```

**The read/write serialization hypothesis is dead.** The ceiling is **1.05×**, not the
1.8× that decoupling would have to offer to be worth building. The loop is 95% `pwrite`
and 5% `read`, so there is essentially nothing for a queue to hide.

The instrument agrees with the stream accounting, which is the first thing to check before
believing any of it: `(4651.5 + 84948.8 + 4.4) / 5818.3 = 15.40`, and `streams:` — computed
independently, from per-stream open/close timestamps — also says 15.40. Two separate clocks
on the same fact. Reading the split as occupancy:

| | worker-seconds | workers busy at any instant |
|---|---|---|
| `write` | 84948.8 | **14.60 of 16** |
| `read` | 4651.5 | 0.80 |
| `other` | 4.4 | 0.00 |

Fourteen and a half of sixteen workers were permanently inside `pwrite`. Per worker that is
**3.36 MiB/s** of write, and `3.36 × 14.60 = 49.09 MiB/s`, which closes against the table's
throughput — the split accounts for the whole transfer, with nothing hiding outside it.

**`read %` understates the network, and that is the interesting part.** Each stream, while
it was actually inside `stream.read()`, moved 61.4 MiB/s. Sixteen of those would be
982 MiB/s, four times the 240 MiB/s §6.5c measured against `/dev/null` — so the read timer
is plainly not measuring wire time. It is measuring how long it takes to drain a socket
buffer that filled while the worker was blocked on the disk. **The kernel's receive buffer
is already acting as the reader/writer queue the fix would have added**, which is precisely
why the ceiling is 1.05× and why the series model in §6.5f over-predicted the loss. The
ceiling is honest about the *fix's* value; it is not a measurement of the network's cost.

So the loss is downstream of `pwrite` being called, and the remaining question is whether
`pwrite` is slow because of what we ask of it or because of what the device can do.

#### And the same run reported a 1h45m cost as `0.0s`

`phases: verify 0.0s` is the **downloader's** verify, and it is genuinely 0.0s — #19
assembled the ETag from part digests recorded in flight. But the benchmark then runs its
own independent re-read (`announce_verification` → `file_multipart_etag`, deliberately not
trusting the downloader's verdict), and *that* emitted 21 heartbeats: **at least 1h45m**,
for 278.91 GiB, which is **43–45 MiB/s**. It appears in no column, no phase, and no total.
The run then prints "the post-hoc read-back was skipped entirely" directly underneath an
hour and three quarters of post-hoc read-back.

That is the recurring defect again, in its purest form so far: a reported number that is
true of the thing it names and wrong about the run it describes. Two fixes, both applied —
the benchmark's own re-read is now timed and reported as its own `re-read :` line, and the
#19 narrative no longer claims the read-back was skipped when the benchmark just did one.

**The 43–45 MiB/s is the most important number in the run, and it was nearly thrown away.**
`file_multipart_etag`'s docstring records this same operation — 279 GiB, 2 readers, this
class of device — at **85 MiB/s**. It just measured 43–45. And the downloader's write, on
the same disk at the same moment, settles at 44.2–49. Read and write, our code and plain
`dd`-grade sequential I/O, all land on ~45 MiB/s.

Which inverts §6.5e/§6.5f's verdict of "a defect — ours". Look at the extent of every
figure those conclusions rest on:

| figure | extent | duration |
|---|---|---|
| 79.5 MiB/s, 16 writers one inode (§6.5f) | 16 GiB | 206 s |
| 83.6 / 85.8 MiB/s, write concurrency (§6.5e) | 16 GiB | ~200 s |
| 84 / 76 MiB/s, scatter (§6.5d) | 4 GiB | ~50 s |
| **43–45 MiB/s, 2-reader re-read (this run)** | **279 GiB** | **6300 s** |
| **49.08 MiB/s, the downloader (this run)** | **279 GiB** | **5818 s** |

Every "the disk can do 79–88" measurement is ≤16 GiB and ≤3.5 minutes. Both full-extent,
hour-plus measurements say ~45. §6.5d did test fullness and found 1.7% variation — but with
4 GiB arms, so it could only have eliminated fullness *at short extent*. A 1.70× gap
"inside our own code" and a 1.7× short-burst-versus-sustained ratio on a `pd-standard` are
the same number, and nothing measured so far distinguishes them.

§6.5h went on to measure exactly that: **88 MiB/s over 4 GiB, 44 MiB/s over 279 GiB, with
the page cache demonstrated to be irrelevant to either.** The duration hypothesis is the
surviving one.

#### §6.5h Is 45 MiB/s the device? — `O_DIRECT`, ~4 minutes, no egress

Do not settle this with another long run; settle it by removing the page cache, which is
what made the short arms untrustworthy in the first place. `O_DIRECT` makes a 4 GiB test
tell the truth about a 279 GiB one.

First, what the device actually is — provisioned size sets a `pd-standard`'s sustained
ceiling, and this disk was created fresh for this session:

```bash
# on the node
gcloud compute disks describe "$DISK" --zone "$ZONE" \
  --format='value(sizeGb,type.basename())'
sudo docker exec slurm sh -c 'lsblk -o NAME,SIZE,ROTA | head'
```

**Measured: `316  pd-standard`.** A spec-sheet prediction was made here and it was wrong:
`pd-standard` is rated at 0.12 MB/s per provisioned GB, giving `316 × 0.12 = 36.2 MiB/s`,
and the `dd` arms below measure **88 MiB/s**. Either the constant does not apply to this
configuration or there is a per-instance floor; whichever it is, **the device is the
authority and the spec sheet is not.** Recorded because the prediction shaped the next test
and nearly shaped the conclusion.

Then read and write the same disk with the cache out of the path. The read arm uses the BAM
that is already there, so it costs nothing and destroys nothing:

```bash
# on the node -- what is actually on the disk. The sweep writes `bench.<connections>.bin`,
# NOT the source object's name.
sudo docker exec slurm sh -c 'ls -laS /mnt/rwdisks/'"$DISK"'/ | head'

# on the node -- read, 4 GiB from 128 GiB into the largest file, cache bypassed
sudo docker exec slurm sh -c '
  BIG=$(ls -S /mnt/rwdisks/'"$DISK"'/bench.*.bin 2>/dev/null | head -1)
  echo "reading $BIG"
  dd if="$BIG" of=/dev/null bs=1M count=4096 skip=131072 iflag=direct 2>&1 | tail -1'
```

**In practice the disk was empty but `lost+found`, and the §6.5g run deleted the file
itself.** Not a later sweep — `command_sweep` unlinks the payload and its sidecars at the
*end* of each row unless `--keep` is passed (`benchmark_localization.py`, `if not
args.keep`), and it is right to: an orphaned `.bench.N.bin.k9pdl.done` with no file is the
state that made a later run exit 0 after 108 bytes. So a 3h22m full-size run leaves nothing
behind to measure, and the 279 GiB of writes it just performed become unrepeatable without
paying for them again.

> **Pass `--keep` on any §6.3 run you intend to follow with a destination measurement.**
> There is no way to recover the file afterwards, and re-creating it costs another 1h37m.

That is the only cost of the omission here, because the free space is better than the file
was: write 4 GiB yourself, read it back, and run the *same* 4 GiB buffered. Direct and
cached then sit side by side on identical bytes with identical tooling, which is the actual
experiment — the ≤16 GiB figures in §6.5d–§6.5f are suspect precisely because nothing ever
ran both ways on the same work.

```bash
# on the node -- all three arms, ~6 minutes at 36 MiB/s (halve `count` to shorten)
sudo docker exec slurm sh -c '
  D=/mnt/rwdisks/'"$DISK"'
  df -h "$D" | tail -1
  echo "== write, O_DIRECT"
  dd if=/dev/zero of="$D"/ddtest bs=1M count=4096 oflag=direct 2>&1 | tail -1
  echo "== read back, O_DIRECT"
  dd if="$D"/ddtest of=/dev/null bs=1M count=4096 iflag=direct 2>&1 | tail -1
  echo "== write, buffered + fdatasync -- what 6.5e/6.5f measured"
  dd if=/dev/zero of="$D"/ddtest2 bs=1M count=4096 conv=fdatasync 2>&1 | tail -1
  rm -f "$D"/ddtest "$D"/ddtest2'
```

Note the disk is now nearly empty rather than 90% full, so this measures the device rather
than the device-at-fullness. That is the right control here — §6.5d already found fullness
worth 1.7% — but it does mean the arms are not directly comparable to the §6.5g re-read,
which ran against a full one.

#### Measured — the cache was never the mechanism, and duration is

```
/dev/sdb  310G  28K  310G  1%  /mnt/rwdisks/canine-bench-...
write, O_DIRECT            4 GiB / 46.4s   92.5 MB/s  =  88.2 MiB/s
read,  O_DIRECT            4 GiB / 46.8s   91.7 MB/s  =  87.5 MiB/s
write, buffered+fdatasync  4 GiB / 53.3s   80.7 MB/s  =  77.0 MiB/s
```

**Buffered is 13% *slower* than direct.** The page cache costs this workload throughput; it
does not lend any. So the hypothesis this section was built on — that the ≤16 GiB arms in
§6.5d–§6.5f were inflated by RAM — is **refuted**, and the `⚠️` extent column above is
wrong about the reason those figures should be treated carefully. They were honest
measurements of this device. Two further consequences worth stating plainly: the `O_DIRECT`
test **controlled for the wrong variable**, since neither regime turns out to depend on the
cache, and the instrument therefore could not have distinguished the two hypotheses
whatever it returned. Choosing a control that both candidates are indifferent to is a
distinct failure from measuring nothing at all, and harder to notice.

What survives is the pattern, now with the cache eliminated as an explanation for it:

| operation | tool | direction | extent | duration | rate |
|---|---|---|---|---|---|
| `dd oflag=direct` | dd | write | 4 GiB | 46 s | **88.2 MiB/s** |
| `dd iflag=direct` | dd | read | 4 GiB | 47 s | **87.5 MiB/s** |
| `dd conv=fdatasync` | dd | write | 4 GiB | 53 s | 77.0 MiB/s |
| 16 writers, one inode (§6.5f) | sh | write | 16 GiB | 206 s | 79.5 MiB/s |
| the downloader (§6.5g) | python | write | 279 GiB | 5818 s | **44.2 MiB/s settled** |
| `file_multipart_etag` re-read (§6.5g) | python | read | 279 GiB | 6300 s | **43–45 MiB/s** |

Two tools, both directions, cached and uncached: **everything short lands at 77–88, and
everything long lands at 43–45.** The only variable that tracks the split is duration.

And the transition was already recorded, in §6.5c's heartbeat series — read it again now
that there is something to compare it to:

```
103.3  87.9  58.9 | 44.4 44.1 ... 44.2 ...   MiB/s   (300 s intervals)
```

That is not noise settling. It **starts at the `dd` figure**, decays over three intervals —
about 73 GiB of writes — and then holds flat to 1.7% for 75 minutes at half of it. A device
that delivers 88 MiB/s for the first minutes and 44 MiB/s thereafter explains every row of
the table above, in both directions, for both tools, with no appeal to our code at all.

**So §6.5e/§6.5f's "a defect — ours" is very likely wrong, but not because their numbers
were inflated — because they were too short to leave the burst regime.** The downloader's
44.2 MiB/s is what this disk sustains; the 79.5 MiB/s it was being judged against is what
this disk does for its first three minutes. Nothing in §6.5c–§6.5f ran long enough to
measure the thing the full-size run is limited by.

This is not yet proven. It is one inference from a decay curve measured through the
downloader, and the confirming test is to reproduce that curve with no downloader in the
path (§6.5i).

#### §6.5i Reproduce the decay with `dd` alone — ~25 minutes, no egress

Twelve 8 GiB stages, each timed separately, `O_DIRECT` throughout so the cache is out of
the path in every one. If the rate falls from ~88 toward ~44 somewhere around stage 8–9,
the burst-then-sustained profile is the device's and the investigation is over.

```bash
# on the node -- 96 GiB total, one rate per 8 GiB stage
sudo docker exec slurm sh -c '
  D=/mnt/rwdisks/'"$DISK"'
  i=0
  while [ $i -lt 12 ]; do
    printf "stage %2d (%3d GiB in): " "$i" "$((i*8))"
    dd if=/dev/zero of="$D"/burst bs=1M count=8192 seek=$((i*8192)) \
       oflag=direct conv=notrunc 2>&1 | tail -1
    i=$((i+1))
  done
  rm -f "$D"/burst'
```

The trend is usually obvious by stage 5. Interrupting is safe but leaves the file behind —
`sudo docker exec slurm rm -f /mnt/rwdisks/$DISK/burst` afterwards, or the next §6.3 run
will have 96 GiB less room than it expects.

How to read it:

* **decays ~88 → ~44 and holds** → confirmed, and the whole §6.5c–§6.5g investigation
  resolves to "the disk is a 316 GB `pd-standard`". 3.07× is the device's answer, the
  downloader has no write-path defect, and the only remaining lever is the destination —
  a larger `pd-standard` (throughput scales with provisioned GB), `pd-balanced`/`pd-ssd`,
  or several disks striped. That is §10, and §10 needs redoing with these numbers.
* **flat at ~88 for all twelve stages** → the device sustains 88 MiB/s and the decay is
  something the downloader does over time. Then the gap is real and ours, and the suspects
  are what `pwrite` does differently from `dd`: 1 MiB blocks against dd's 1 MiB *sequential*
  ones, sparse out-of-order offsets across 16 workers, and the `fdatasync` cadence — in
  that order, and all three are cheap to test with the same harness.
* **decays but to ~60, not ~44** → both, in some proportion. Take the measured sustained
  figure as the new denominator and re-derive the gap; do not keep using 79.5.

#### Measured — confirmed. The disk is the answer, and there is no gap.

```
stage  0 (  0 GiB in):  93.10 s   92.3 MB/s
stage  1 (  8 GiB in):  93.31 s   92.1 MB/s
stage  2 ( 16 GiB in):  93.31 s   92.1 MB/s
stage  3 ( 24 GiB in):  93.30 s   92.1 MB/s
stage  4 ( 32 GiB in):  93.30 s   92.1 MB/s
stage  5 ( 40 GiB in):  93.30 s   92.1 MB/s
stage  6 ( 48 GiB in):  93.30 s   92.1 MB/s
stage  7 ( 56 GiB in): 140.55 s   61.1 MB/s   <- the knee
stage  8 ( 64 GiB in): 186.67 s   46.0 MB/s
stage  9 ( 72 GiB in): 186.65 s   46.0 MB/s
stage 10 ( 80 GiB in): 186.63 s   46.0 MB/s
stage 11 ( 88 GiB in): 194.36 s   44.2 MB/s
```

Plain `dd`, `O_DIRECT`, single stream, no downloader, no page cache, no Python. **Flat to
0.2% for seven stages at 92.1 MB/s (87.8 MiB/s), a knee in the eighth, then flat again at
46.0 MB/s (43.9 MiB/s).** The ratio is `92.1 / 46.0 = 2.002×` — the disk delivers exactly
double its sustained rate for the first ~56 GiB and then halves.

Now use it to predict the full-size run, with nothing from the run itself as input:

| segment | rate from `dd` | time |
|---|---|---|
| first 56 GiB | 87.8 MiB/s | 652.9 s |
| next 8 GiB (the knee) | 58.3 MiB/s | 140.6 s |
| remaining 214.9 GiB | 43.9 MiB/s | 5016.6 s |
| **predicted total** | | **5810.0 s** |
| **§6.5g measured** | | **5818.3 s** |

**A three-parameter model of the device, fitted only to `dd`, predicts the downloader's
279 GiB wall clock to 8.3 seconds — 0.14%.** And the settled rates: the downloader holds
**44.2 MiB/s** where a single sequential `O_DIRECT` `dd` sustains **43.9**. Sixteen
concurrent workers issuing sparse, out-of-order 1 MiB `pwrite`s, with `fdatasync` and an
in-transfer md5 on top, are **0.8% faster than `dd`**.

**There is no gap. The investigation is closed.** §6.5c through §6.5g were measuring a
316 GB `pd-standard`'s burst rate and calling the difference a defect. The downloader is at
the device's ceiling and has been throughout.

For the record, this also retires the §6.5g conclusion that "the loss is between `pwrite`
returning and bytes reaching the platter — writeback throttling". It is neither writeback
nor throttling: the bytes reach the platter at exactly the rate the platter accepts them.

#### What this means for canine, which is the actual point

`canine/localization/base.py:920` sizes the localization disk to the payload plus 5%:

```python
disk_size = max(10, 1 + int(disk_size / (0.95*10**9)))
```

For this BAM that computes **316 GB** — the benchmark disk is not a coincidence, it is
precisely what production would provision. And on `pd-standard`, **that line does not only
choose capacity, it chooses throughput**, because sustained rate scales with provisioned
size. Sizing a disk to fit the data exactly therefore guarantees the slowest sustained
transfer that data can have. The tightest possible disk is the slowest possible disk, and
nothing in the code says so.

**A faster disk type is not available on this path.** `pd-standard` is the only type that
can be mounted read-only at scale, and the whole point of the rodisk pattern is that one
localized disk is attached read-only to many workers. So the type is fixed by the *read*
path's fan-out requirement, and the *write* path — the 1.62 h this benchmark measures —
pays for a choice it does not get to make. `pd-balanced` is not merely absent from
`persistent_disk_type`'s `"standard" | "ssd"` vocabulary; it is unusable here. (The
constraint does not apply to `scratch_disk_type`, which is per-node and never shared, so
if scratch ever lands on the critical path that lever is still open.)

**That leaves size as the only in-family lever.** At the observed
`46.0 / 316 = 0.1456 MB/s per GB`, reaching 64 MiB/s sustained — the rate that would put
§6.3 at the ≥4× target — needs roughly a **460 GB** disk, a one-line change to the 5%
margin. Note the tradeoff before reaching for it: `base.py:920` sizes *every* rodisk, so a
flat multiplier inflates thousands of small ones to buy throughput that only large
payloads can use. If it is done at all it should be conditional on the payload being big
enough for sustained rate to matter, which is a design question rather than a constant.

**Do not act on the 460 GB from arithmetic.** A `pd-standard` spec figure was quoted from
memory in §6.5h and was wrong by 2.4×; the same risk applies to assuming the per-GB scaling
stays linear out to 460 GB, or that the 2.002× burst multiplier and the 56 GiB knee move
with provisioned size at all. §6.5j measures it with the same 25-minute ladder.

#### The strategic answer is probably not a disk at all

There is a concurrent effort — **`origin/fuse-localize`** — that replaces the disk on this
path entirely. `64e56fd` adds `create_bucket_mount()` as a GCS-backed alternative to
`create_persistent_disk()` for `LocalizeToDisk` inputs: uploads go straight to a
deterministic per-workflow regional bucket and are consumed through `gcsfuse`, with a
content-addressed `_SUCCESS` marker for cross-run dedup. `get_or_create_rapid_cache()`
provisions a zonal Rapid Cache (formerly Anywhere Cache) over that bucket with
`--enable-ingest-on-write`, best-effort, so a provisioning failure degrades to normal
bucket latency rather than failing the workflow.

If that lands, **the 43.9 MiB/s ceiling this entire investigation converged on stops
applying**, because there is no `pd-standard` in either the write or the read path. Two
things from §6.5c–§6.5i that carry over regardless:

* **§6.6 becomes the measurement that matters**, not §6.5j. The bucket-compose route has
  still never touched real infrastructure, and it is now the strategic path rather than a
  fallback.
* **Measure the bucket path over ≥96 GiB, not 4.** The single most expensive error in this
  section was comparing a 4 GiB number to a 279 GiB one; a cached bucket has every reason
  to show its own burst-then-settle curve (cache fill, then origin rate on a miss), and a
  quick `gcsfuse` benchmark would reproduce the same mistake in a new medium. Run the
  §6.5i ladder against the mount.

So: treat §6.5j as the contingency that keeps the current path viable if `fuse-localize`
slips, and §6.6 as the one that decides where this actually ends up.

#### §6.5j Does a bigger pd-standard go faster? — ~30 minutes per size

Only worth running if the current disk path has to survive. Create each candidate, run the
§6.5i ladder on it, record **burst rate, knee position and sustained rate**. Nothing else
is needed: §6.5i demonstrated that those three numbers predict a full-size localization to
0.14%, so sizes can be compared without downloading 279 GiB again.

All candidates are `pd-standard` — the read-only-at-scale requirement rules out the others
on this path, so this sweeps size alone.

```bash
# on the node -- one candidate; repeat at 460 and 640
CAND=canine-cand-std-460
gcloud compute disks create "$CAND" --zone "$ZONE" --type pd-standard --size 460GB
gcloud compute instances attach-disk pdl-bench --disk "$CAND" --zone "$ZONE"
# then mkfs + mount it per §4, and run the §6.5i ladder against it
```

Three sizes — 316 (already measured), 460, 640 — answer the question the arithmetic cannot:
whether sustained rate really scales linearly with provisioned GB, and whether the 2.002×
burst multiplier and the 56 GiB knee scale with it too. If sustained rate is flat across
all three, the per-GB model is wrong and size is not a lever either, which would leave
`fuse-localize` as the only route to ≥4×.

Note the ladder must run past the knee at *each* size. If the knee scales with the disk,
a 640 GB candidate may not reach it within 96 GiB — extend the stage count until two
consecutive stages agree, or the run will report a burst rate as if it were sustained.
That is the same error this section spent four subsections making.

Remember to detach and delete each candidate (§9); a forgotten 640 GB disk outlives the
experiment that needed it.

### 6.6 the bucket-compose route against real GCS

> **§6.5i promoted this section.** It was written as a completeness item for a fallback
> route. With the disk path measured at a hard 43.9 MiB/s sustained, and `pd-standard`
> fixed by the read-only-at-scale requirement, this is now the likeliest route to the ≥4×
> target and the measurement that decides the outcome. It also converges with
> `origin/fuse-localize`, whose `create_bucket_mount()` makes a GCS mount the *primary*
> destination for `LocalizeToDisk` inputs rather than an alternative.
>
> **Run the §6.5i ladder against the mount before running anything else here.** A Rapid
> Cache bucket has every reason to show its own burst-then-settle curve — cache fill at
> zonal SSD rate, then origin rate on a miss — and a 4 GiB `gcsfuse` benchmark would
> reproduce, in a new medium, the single most expensive error in this document. Two
> consecutive stages agreeing is the bar.

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

**`gcsfuse` is not in the image on `wolf-2.0-update`**, and neither is `rclone` — its
Dockerfile install is commented out, though `conf/rclone.conf` ships. Only `fuse-overlayfs`
is present, for podman. So this step needs one of:

* `apt-get install` gcsfuse in the running container (needs Google's gcsfuse apt repo);
* the `fuse-localize` image, which is where this is being evaluated anyway;
* recording the bucket-compose route as **unverified**, which is the honest default given
  that branch is benchmarked separately.

### Why the mount goes inside the container

A reasonable alternative is to mount the bucket on the *host* and let the container see it.
That works, but only with **`rshared` mount propagation** — a plain bind mount is
point-in-time, so the container sees whatever was at the path when it started and a *later*
host mount is invisible to it. FUSE adds a second condition: mounts are private to the
mounting user, so a container process gets `EACCES` unless the mount was made with
`allow_other`.

canine does both, on the controller: `dockerTransient.py:151` bind-mounts `/mnt` and `/dev`
with `propagation="rshared"`, and its `rclone mount` passes `--allow-other` along with
`--uid $HOST_UID --gid $HOST_GID`.

But the **worker** does not. `worker_startup_script.sh:60` binds only `/mnt/nfs`, with no
propagation flag and no `/mnt`. And canine creates its FUSE mounts *inside* the container
regardless — `dockerTransient.py` invokes `rclone mount` through `self.invoke(...)`, not on
the host. Mounting inside the container therefore matches production on both counts, which
is why the command above does that rather than adding `rshared` to §3.

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

**The JSONs are inside the container, not on the node.** `pdl` is `docker exec`, so every
`--json /tmp/x.json` wrote to the container's `/tmp`. Copying from `$NODE:/tmp/*.json`
collects *nothing* — and the next command deletes the instance holding the only copy. Get
them out of the container first:

```bash
# on the node
mkdir -p ~/results
for f in $(sudo docker exec slurm sh -c 'ls /tmp/*.json'); do
  sudo docker cp "slurm:$f" ~/results/
done
sudo chown -R "$USER" ~/results
ls -l ~/results        # must be non-empty before you tear anything down
```

```bash
# from your workstation
gcloud compute scp --project $PROJECT --zone $ZONE \
  "$NODE:~/results/*.json" ./benchmark-results/
ls -l ./benchmark-results/     # again: confirm before teardown
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
| today: 4.97 h, n1-standard-8 | $0.40 | $0.83 | $1.23 | $369 |
| **measured: 1.62 h, same VM** | **$0.13** | **$0.83** | **$0.96** | **$288** |
| …and oversize the disk to 742 GB | $0.13 | $1.95 | $2.08 | **$624** |
| …instead, localize on a 2-vCPU node | $0.02 | $0.83 | $0.85 | **$255** |
| hypothetical, at the disk's ceiling: 0.90 h | $0.07 | $0.83 | $0.90 | **$270** |

Row 1 and row 2 are **measured** (§6.3): 4.97 h single-stream against 1.62 h at 16
connections, both on the real 279 GiB BAM to a 316 GB pd-standard disk. The VM figures
derive from the $0.08/h preemptible n1-standard-8 rate implied by the original table. Rows
3–5 are arithmetic on those.

The last row prices the ~35 minutes §6.3 leaves unexplained. **It is worth $18 per 300
disks — about 6%.** That is the honest size of the prize §6.5c is chasing, and it is
small enough that "report the 3.07× and move on" is a defensible answer if the cause turns
out to be the source rather than our write path.

**1. The disk is the larger line item, and the downloader cannot touch it.** At 48 h
retention the disk costs $0.83 against $0.40 of VM time, and retention is set by reuse, not
by how fast the disk was filled. The downloader's cost ceiling is the VM share — about 33%
of the per-disk total, of which the measured run captures $0.27 (**22% off the total**, $81
per 300 disks). It is still worth shipping; it is just not where most of the money is.

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

| Question | Where it comes from | Answer |
|---|---|---|
| **pd-standard write/read at 316 GB** — the constraint everything else sits under | §4.1 `dd` | 92.3 MB/s write, 91.6 MB/s read; `peak disk write` 87.7–88.1 MiB/s across every sweep |
| **Is snapshot conversion viable, or arithmetically dead?** | §4.2 extrapolation | not measured — left off the critical path |
| Does a snapshot-restored disk read slower than a natively-written one? | §4.2 final `dd` vs §4.1 read | not measured |
| `connections` default (currently 8) | §6.1 knee | **16.** No knee against the source (linear to the `MAX_CONNECTIONS` cap); 16 is where the *disk* saturates |
| Source/NIC ceiling, independent of any disk | §6.1 peak throughput | 227–230 MiB/s to tmpfs; ~16 MiB/s per connection, so the cap is per-connection |
| Disk or network as the limit | §6.2 plateau vs §6.1, and the reported peaks | the **disk**, at prefix size: 78.61 of 87.78 MiB/s = 90% |
| Speedup on the real 300 GB BAM vs. today's 4 h | §6.3 | **4.97 h → 1.62 h = 3.07×**, under the ≥4× target |
| Memory bounded? | §6.1 peak RSS | yes — 86.24 MiB on 279 GiB, 63.64 MiB on 4 GiB at the same connection count |
| md5 correct from S3 and GDC | §6.4 | `hash ok` on the full 279 GiB BAM against a 9849-part ETag |
| Frontier or checkpoint on ext4? | §6.5 | **frontier** — first time that path has run anywhere |
| Punch-hole recovery correct? | §6.5 | yes — 111.8 MiB refetched after a 64 MiB hole, hash CORRECT, both before and after the writer change |
| Resume overhead | §6.5 / §6.5a | 0.8% before the deferred writer, **2.2%** after; a broken frontier would show ~75% |
| Does in-transfer hashing (#19) pay off? | §6.3 | yes, completely — `verify 0.0s` with a real ETag, ~52 min of read-back avoided |
| HTTP vs the S3 API | §6.5b | 6.9% apart on the download phase; the presigned path wins but the transport is not the bottleneck |
| Why full size runs at 56% of the device when 4 GiB runs at 90% | §6.5c / §6.5d | **the question was malformed.** The 4 GiB rows measure page cache (28 GB RAM), so 90% is an artifact; the full-size run's throughput is **flat to 1.7%** across 85% of its duration, which eliminates fullness and extent growth outright. Source, ENOSPC and logical scatter all separately eliminated |
| Is 49 MiB/s sustained a defect or just this disk under our access pattern? | §6.5e → **settled §6.5i** | **just the disk.** Plain `dd`, `O_DIRECT`, single stream: 92.1 MB/s flat for 56 GiB, then 46.0 MB/s flat — exactly 2.002×. Every ≤16 GiB arm measured the burst |
| Is single-inode `fsync` contention the 1.70×? | §6.5f | **moot — there is no 1.70×.** Same ≤16 GiB burst caveat as above |
| Did the page cache inflate the short arms? | §6.5h | **no — refuted.** `O_DIRECT` measures 88.2 MiB/s write and 87.5 read; buffered + `fdatasync` measures 77.0. The cache *costs* 13%. The `O_DIRECT` test controlled for a variable neither hypothesis depended on |
| **Is the downloader leaving anything on the table?** | **§6.5i** | **no.** It settles at **44.2 MiB/s** where single-stream sequential `O_DIRECT` `dd` sustains **43.9** — 16 concurrent sparse out-of-order writers with `fdatasync` and in-transfer md5 are 0.8% *faster* than `dd`. A three-parameter device model fitted only to `dd` predicts the 279 GiB run's wall clock to **0.14%** |
| Is `read`/`write` serialization in `download_chunk` the gap? | §6.5g | **no — falsified.** The loop is **95% `write`, 5% `read`**, overlap ceiling **1.05×**, so a reader/writer split has nothing to recover: 14.60 of 16 workers sat permanently inside `pwrite`. The kernel's socket receive buffer is already the queue that fix would have added |
| Then what is the remaining gap? | §6.5g–§6.5i | **none. Closed.** The gap was burst-versus-sustained throughout: §6.5c–§6.5f compared the downloader's sustained rate against the disk's burst rate |
| **pd-standard burst vs sustained at 316 GB** | §6.5i | **92.1 MB/s (87.8 MiB/s) for the first 56 GiB, knee over the next 8, then 46.0 MB/s (43.9 MiB/s) flat — exactly 2.002×.** Never size a transfer off a `dd` that finishes in a minute |
| **Where is the remaining time, then?** | §6.5i | **in `base.py:920`**, which sizes the localization disk to payload + 5% — 316 GB for this BAM, exactly the benchmark disk. On `pd-standard` that line sets throughput, not just capacity, so the tightest disk is the slowest disk |
| Can a faster disk type fix it? | §6.5i | **no — not on this path.** `pd-standard` is the only type mountable read-only at scale, and the rodisk pattern requires exactly that. The type is fixed by the read path's fan-out; the write path pays for it. Size is the only in-family lever (§6.5j) |
| So what is the actual route to ≥4×? | §6.5i / §6.6 | **`origin/fuse-localize`**, most likely. `create_bucket_mount()` + Rapid Cache with `--enable-ingest-on-write` removes `pd-standard` from both paths, so this whole ceiling stops applying. That promotes §6.6 from a fallback to the measurement that decides the outcome — and it must be run over ≥96 GiB, not 4 |
| What does the benchmark's own verification cost? | §6.5g | **43–45 MiB/s over 278.91 GiB, ≥1h45m** — and it was reported as `0.0s` until this run, because `phases: verify` is the downloader's and this read-back is the harness's. Now printed as `mean re-read :` |
| Source ceiling with no disk in the path at all | §6.5c | 227–256 MiB/s to `/dev/null`, head and 250 GiB deep alike — agrees with §6.1's tmpfs figure by a different route |
| the bucket-compose route on real GCS, and its token source | §6.6 | not measured — `gcsfuse` absent from the image |
| `/bin/sh` in the container | §3 probe | dash |
| Resume across a real preemption | §7 | not measured — needs a SLURM cluster, not a single node |

Report the §6.3 number as **"4.97 h → 1.62 h on the real BAM"**, not as the sweep's
internal speedup. The internal figure is measured against a single stream on the same
hardware; the number that matters to anyone waiting on a pipeline is the one against
today's behaviour.

And report the **3.07×** alongside it rather than §6.2's 4.92×. The larger figure is true
of a 4 GiB prefix and not of the object anyone localizes, and the difference between them
is the open question in §6.5c.

Do not report the 3.07× as "under target because of a defect in our write path". §6.5i
measured the device directly: **3.07× is what a 316 GB `pd-standard` gives you**, the
downloader runs 0.8% faster than `dd` on it, and the ≥4× target is a disk-provisioning
question (§6.5j, §10) rather than a code one. Report it as *"3.07×, at the destination's
ceiling"* — the qualifier is what stops the next reader re-opening §6.5c.
