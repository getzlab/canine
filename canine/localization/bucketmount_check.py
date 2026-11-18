"""
Reports what gcsfuse logged for a job's bucket mounts while the job ran, and fails the
job if gcsfuse failed a read of one of its inputs.

    python3 bucketmount_check.py <job inputs dir> [--report-only]

A read gcsfuse cannot serve reaches the job's tool as an I/O error, which the tool may
swallow and exit 0 with partial output: getdRangerSupportingReads reported success after
pysam's "IOError: truncated file" on a mounted bam, and the half-finished file failed a
later task on every retry. Run after the tool exits 0, this turns that into a failed job,
which canine retries.

Each line of <job inputs dir>/.bucketmount_logs is "bucket log offset mountdir", written
as the job starts using the mount: the log's size then marks where the job's part of it
begins. The mount is shared by every job on the node, so only an ERROR naming one of this
job's inputs (an input symlinked into the mount, or a file inside one that is a
directory) fails it. Warnings are reported only: gcsfuse retries through network outages
of at least six minutes, logging warnings, and the reads complete intact.

The record is consumed, so whichever of the entrypoint and teardown runs first reports.

Staged onto the node and run as `python3 <path>` outside any job container, so it may
import only the standard library.
"""
import os
import sys

EXIT_READ_FAILED = 3
MAX_REPORTED_LINES = 200


def job_objects(inputs_dir, mount_dir):
    """Object names, relative to the bucket, of the inputs symlinked into mount_dir."""
    prefix = mount_dir.rstrip("/") + "/"
    objects = set()
    for name in os.listdir(inputs_dir):
        path = os.path.join(inputs_dir, name)
        if not os.path.islink(path):
            continue
        # readlink, not realpath: resolving through a failed FUSE mount can hang
        target = os.path.normpath(os.path.join(inputs_dir, os.readlink(path)))
        if target.startswith(prefix):
            objects.add(target[len(prefix):])
    return objects


def names_an_object(line, objects):
    """
    gcsfuse quotes object names, escaped inside its text-format message:
    The file \\"obj/a.bin\\" was modified or deleted ...
    """
    line = line.replace('\\"', '"')
    return next((o for o in sorted(objects)
                 if '"{}"'.format(o) in line or '"{}/'.format(o) in line), None)


def new_lines(log, offset):
    size = os.path.getsize(log)
    # smaller than at the mark: gcsfuse rotated it, so all of it is newer
    if size < offset:
        offset = 0
    with open(log, "rb") as fh:
        fh.seek(offset)
        return fh.read().decode("utf-8", "replace").splitlines()


def check(inputs_dir, report_only=False, out=sys.stderr):
    record = os.path.join(inputs_dir, ".bucketmount_logs")
    if not os.path.exists(record):
        return 0
    failed = []
    with open(record) as fh:
        entries = [line.split() for line in fh if line.strip()]
    for entry in entries:
        bucket, log, offset = entry[0], entry[1], int(entry[2])
        if not os.path.isfile(log):
            continue
        lines = new_lines(log, offset)
        if not lines:
            continue
        print("WARNING: gcsfuse logged the following for bucket {} while this job ran "
              "(the mount is shared by every job on this node):".format(bucket), file=out)
        for line in lines[:MAX_REPORTED_LINES]:
            print(line, file=out)
        if len(lines) > MAX_REPORTED_LINES:
            print("... {} more lines in {}".format(len(lines) - MAX_REPORTED_LINES, log), file=out)
        if len(entry) < 4:
            continue
        objects = job_objects(inputs_dir, entry[3])
        for line in lines:
            if "severity=ERROR" in line:
                name = names_an_object(line, objects)
                if name is not None and (bucket, name) not in failed:
                    failed.append((bucket, name))
    os.remove(record)
    for bucket, name in failed:
        print("ERROR: gcsfuse failed a read of gs://{}/{}, an input of this job, while it "
              "ran; its outputs may be incomplete.".format(bucket, name), file=out)
    if failed and not report_only:
        return EXIT_READ_FAILED
    return 0


if __name__ == "__main__":
    sys.exit(check(sys.argv[1], report_only="--report-only" in sys.argv[2:]))
