# move_to_bucket: scratch-disk outputs moved to a bucket

A scratch disk (`use_scratch_disk`) holds a job's outputs until every downstream task has read
them. wolF waits for a whole task, all its shards, before starting the next, so each shard's disk
sits idle while the slowest shards run, then while the join reads it. In a full-size sharded
hard-clip run (alignment_pipeline_wolF, 2026-11-11) the 25 shard disks, 265 GB each, cost $4.46,
more than the shards' nodes ($4.37): 6.6 TB held about 12 hours, of which each shard used 1-4.

`move_to_bucket` is a localizer flag, beside `protect_disk`. When a job succeeds, canine moves its
outputs from the scratch disk to a bucket, deletes the disk, and returns the outputs as
`bucketmount://` URLs. Downstream tasks already consume those (LOCALIZATION.md §6), so nothing
downstream changes. A disk then costs only while its job runs. The outputs cost half as much per
GB in a bucket ($0.02/GB-month standard storage against $0.04 for pd-standard), and they are
smaller than the disk that held them (at most 15 GB of a 265 GB shard disk).

It is the output-side counterpart of `localize_to_persistent_disk`, and reuses its machinery:
bucket naming and creation, lifecycle expiry, the consumer mount, leases and heartbeat, and
`DeleteLocalizedFiles` for early eviction. It moves outputs where `files_to_copy_to_outputs`
copies them, in the job's delocalization.

## Behaviour

The teardown of a job whose script exits 0, while its scratch disk is still mounted:

1. **Creates the task's bucket** if it doesn't exist (`move_bucket_script`), with the same flags as
   localization (`BUCKET_CREATE_FLAGS`: private, no soft delete) and `move_to_bucket_expiry_days`
   in its lifecycle rule. Every shard of a task races to create it; creation is the mutex, and a
   409 means a sibling won (LOCALIZATION.md §3).
2. **Delocalization moves the outputs** (`delocalization.py`, `move_to_bucket`): every file or
   directory the task's output patterns match, other than stdout, stderr and the outputs in
   `files_to_copy_to_outputs`, which still go to the NFS.
   - They go to `gs://<bucket>/<job ID>/<path in the workspace>`, in **one** `gcloud storage cp
     -r -n` call (one per directory, if a pattern reaches below the workspace), so they upload in
     parallel, and directories whole. Parallel composite uploads are on, so a large file uploads
     in parallel parts too. A file two outputs' patterns match is copied once.
   - Each object gets a customTime, which the lifecycle rule counts from. GCS writes an object
     whole or not at all, gcloud checks its crc32c, and `-n` makes a rerun copy only what is
     missing.
   - Then the objects' URLs are written to `gs://<bucket>/<job ID>/.wolf_moved`, the outputs'
     symlinks in the job's output directory are pointed into the bucket's mount
     (`/mnt/bucketmounts/<bucket>/<job ID>/<path>`), and the same list is written to the output
     directory's `.moved_to_bucket` (`MOVED_MARKER`).
   - **All or nothing.** If a copy fails (after three tries) or the record can't be written, no
     symlink changes and no marker is written: the outputs stay on the disk.
3. **The scratch disk's teardown** unmounts and detaches it as before, then **deletes it** if the
   marker is there. Without the marker it is kept and labelled `finished=yes` (and `protect=yes`
   with `protect_disk`), exactly as without `move_to_bucket`. A failed delete only warns; the
   disk is unlabelled, so the lab's cleanup takes it.

Delocalization on the controller then reads each output's symlink (`scratch_output_url`): one into
a bucket mount is `bucketmount://<bucket>/<job ID>/<path>`, any other `rodisk://<disk>/<path>`.

A job that fails keeps its disk and its `rodisk://` outputs, as without `move_to_bucket`, so a
retry resumes from the disk.

## The bucket

One bucket per task, not per shard (`move_bucket_name`): a scatter of hundreds of shards would
otherwise create hundreds of buckets, against GCS's bucket-creation rate limit (about one per two
seconds per project). The name follows localization's scheme, `wolf-<project number>-<region>-
<hash>`, with the hash taken from `scratch_disk_name`, the name all the task's shards share (each
shard's disk is that name plus `-<job ID>`). wolF makes that name from the task's script, docker
image and inputs, so a rerun of the same task finds the same bucket.

Each shard is under its job ID, so `DeleteLocalizedFiles`, which deletes `gs://<bucket>/<first path
part>/`, evicts one shard. `DeleteDisk` already passes `bucketmount://` paths to it, so workflows
that delete their scratch disks need no change.

No claim object is needed (LOCALIZATION.md §3): each shard writes only its own prefix, and Slurm
runs one attempt of a shard at a time.

## Expiry

The moved outputs are the only copy, unlike localized inputs, which can be fetched again. If they
expire before a downstream task reads them, the producing shard has to run again. That is the same
trade a scratch disk makes now (the lab's cleanup deletes an unprotected idle disk after about
4 h), with a longer window: `move_to_bucket_expiry_days`, default 7, covers a workflow stopped and
resumed within the week. Consumers refresh customTime while they hold the mount (LOCALIZATION.md
§7), so a long downstream task doesn't lose its inputs. Workflows that delete their outputs
(`DeleteDisk`) free them at once; the rule is the backstop.

## Job avoidance

A finished scratch disk (`finished=yes`) still makes the localization step exit 15, skipping the
job; that covers a job whose move failed and kept its disk. The skipped job mounts the disk
read-only, like any read-only disk, so it has no scratch-disk teardown, but its delocalization
still runs and moves the outputs off the disk. Once the read-only disks are detached, the disk is
then deleted (`moved_disk_teardown`, as on a job that ran). If another job on another node still
has it attached, GCE refuses, and the disk is left for the lab's cleanup. A job whose outputs were moved has no
disk, so the orchestrator avoids it on its output directory instead, as it avoids a job whose
outputs are all copied to the NFS (`job_avoid`): if it succeeded, its declared outputs match, its
NFS copies are there, and every object its `.moved_to_bucket` lists is still in the bucket
(`moved_outputs_present`, object metadata only). One that is gone (deleted, or expired) means the
job runs again from the start.

## Options

| Option | Default | Effect |
|---|---|---|
| `move_to_bucket` | `False` | Move a successful job's outputs off its scratch disk to a bucket, and delete the disk. Requires `use_scratch_disk`. |
| `move_to_bucket_expiry_days` | `7` | Lifecycle expiry of the task's bucket. |

## wolF

Set per task, like `protect_disk`: `extra_localization_args = {"move_to_bucket": True}`. In
alignment_pipeline_wolF that replaces `PROTECTED_SCRATCH_DISK` on the shard tasks (hard-clip,
ApplyBQSR, and possibly bwa).

## Still to measure

- **Consumers' read speed:** a join reading 25 shards through gcsfuse rather than from 25
  attached disks. Measure on the hard-clip join (356 GB) before relying on it for markdup-sized
  joins. It also saves the join attaching and detaching 25 disks.
- **Time added to each job:** copying 10-15 GB from a pd-standard disk to GCS, on a node already
  paid for. The disk's read throughput, which grows with its size, likely bounds it.

## Tests

`canine/test/test_move_to_bucket_pure.py`, with a fake gcloud copying into a local directory: the
outputs go in one call, directories whole and files below the workspace in their directory; the
symlinks and the record; a failed copy leaves everything on the disk; the disk deleted, kept and
labelled, or kept; the output URLs; avoidance with the outputs present or gone; the bucket's name
and creation.
