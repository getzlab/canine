from __future__ import print_function
import argparse
import glob
import google_crc32c
import multiprocessing
import multiprocessing.pool
import os
import re
import shutil
import subprocess
import shlex
import sys

"""
This is not actually part of the canine package
This is helper code which is run in the backend
"""

# Where the consuming side gcsfuse-mounts a bucket. Must match
# AbstractLocalizer.job_setup_teardown's mount_dir in localization/base.py:
# outputs are recorded as (initially broken) symlinks into this path, and
# NFSLocalizer.delocalize reads them back out as bucketmount:// URLs.
BUCKETMOUNT_ROOT = "/mnt/bucketmounts"


def upload_one(job):
    """
    Upload one output file to the results bucket. Module-level so it can be
    dispatched through multiprocessing.Pool.

    -n (no-clobber) makes a retried shard idempotent rather than re-uploading
    outputs a previous attempt already finalized.
    """
    src, dst, custom_time = job
    cmd = ["gcloud", "storage", "cp", "-r", "-n"]
    if custom_time:
        cmd.append("--custom-time=" + custom_time)
    cmd += [src, dst]
    proc = subprocess.run(cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    return (dst, proc.returncode, proc.stderr.decode(errors = "replace"))


def workspace_objpath(target, results_bucket, results_prefix, jobId):
    """
    Object path of a file that the job wrote directly into the read-write bucket
    workspace (workdir_mode="bucket").

    The workspace is mounted at <WORKDIR_MOUNT_ROOT>/<bucket>/<prefix>/<shard>/workspace,
    so the object path is just whatever follows the bucket name in the mount path.
    Derived from the resolved path rather than reconstructed, so it stays correct
    if the mount layout changes.
    """
    abs_target = os.path.abspath(target)
    marker = "/" + results_bucket + "/"
    idx = abs_target.find(marker)
    if idx == -1:
        raise ValueError(
          "output {!r} is not inside the read-write mount of bucket {!r}; "
          "cannot derive its object path".format(abs_target, results_bucket)
        )
    return abs_target[idx + len(marker):]


def same_volume(a, b):
    """
    From NFSLocalizer
    Check if file a and b exist on same device
    """
    vols = subprocess.check_output(
      "df {} {} | awk 'NR > 1 {{ print $1 }}'".format(
        a,
        b
      ),
      shell = True
    )
    return len(set(vols.decode("utf-8").rstrip().split("\n"))) == 1

def compute_crc32c(path, fast = False):
    """
    Adapted from localization/file_handlers.py.
    """
    hash_alg = google_crc32c.Checksum()
    buffer_size = 1024 * 1024

    if os.path.isdir(path):
        files = glob.iglob(path + "/**", recursive = True)
    else:
        files = [path]

    for f in files:
        if os.path.isdir(f):
            continue

        file_size = int(os.path.getsize(f))
        file_size_MiB = int(file_size/1024**2)
        print(f"Hashing file {f} ({file_size_MiB} MiB)", file = sys.stderr, flush = True)

        # if fast mode is enabled, only compute hash at 1024 locations in each
        # file (equivalent to number of iterations to hash a 1 GiB file)
        skip_size = 0
        if file_size_MiB > 1024 and fast:
            skip_size = int(file_size//(1024**3/buffer_size) - buffer_size)
            print(f"{f} is >1 GiB; using fast mode", file = sys.stderr, flush = True)

        c = 0
        ct = 0
        with open(f, "rb") as fp:
            while True:
                # in slow mode: output message every ~100 MB (1 GiB/(10*buffer size))
                # in fast mode: output message every 100 hash operations
                if c > 0 and ((not fast and not c % int(1024/10)) or (fast and not c % 100)):
                    print(f"Hashing file {f}; {int(buffer_size*ct/1024**2)}/{file_size_MiB} MiB completed", file = sys.stderr, flush = True)

                data = fp.read(buffer_size)
                if not data:
                    break
                hash_alg.update(data)
                if fast and skip_size > 0:
                    fp.seek(skip_size, 1)
                    ct += (skip_size + buffer_size)//buffer_size
                else:
                    ct += 1
                c += 1

    return hash_alg.hexdigest().decode().upper()

def main(output_dir, jobId, patterns, copy, scratch, finished_scratch,
         results_bucket = None, results_prefix = "", custom_time = None,
         workdir_in_bucket = False, save_intermediates = False):
    jobdir = os.path.join(output_dir, str(jobId))
    if not os.path.isdir(jobdir):
        os.makedirs(jobdir)
    matched_files = []
    # (src, gs:// dst, custom_time) triples, uploaded together after the scan
    uploads = []
    with open(os.path.join(jobdir, '.canine_job_manifest'), 'w') as manifest:
        for name, pattern in patterns:
            n_matched = 0
            for target in glob.iglob(pattern):
                # construct output file path
                if name in {'stdout', 'stderr'}:
                    dest = os.path.join(jobdir, name)
                else:
                    dest = os.path.join(jobdir, name, os.path.relpath(target))

                # populate output directory
                if not os.path.lexists(dest):
                    if not os.path.isdir(os.path.dirname(dest)):
                        os.makedirs(os.path.dirname(dest))
                    # we've output to a scratch disk; create (broken) symlink to RODISK mount
                    if scratch and name not in copy:
                        os.symlink(os.path.abspath(target), dest)
                    # the job ran in a worker-local workdir and its outputs belong
                    # in the results bucket. Record the future gcsfuse mount path
                    # -- broken until a downstream task mounts the bucket, exactly
                    # like the scratch-disk branch above -- and queue the upload.
                    #
                    # This branch MUST precede the same_volume() check below:
                    # target is on local disk and jobdir is on the shared mount, so
                    # same_volume() is False and the copy branch would haul every
                    # output byte back across the controller, which is the round
                    # trip local_workdir exists to remove.
                    # stdout/stderr are deliberately excluded: they are tiny, they
                    # already live at $CANINE_JOB_ROOT on the shared mount rather
                    # than in the (local) workdir, and keeping them there means
                    # they stay readable from the controller for debugging without
                    # mounting anything. wolF's WolfTaskResults also requires both
                    # to be present as real files, and a bucket symlink is broken
                    # on the controller -- NFSLocalizer.delocalize()'s
                    # os.path.isfile() check would drop them and the epilog would
                    # fail with KeyError: 'stderr'.
                    elif (results_bucket is not None and name not in copy
                          and name not in {'stdout', 'stderr'}):
                        # relative to output_dir, not jobdir, so the shard id is
                        # part of the object path -- otherwise every shard would
                        # write over the same objects.
                        objpath = os.path.relpath(dest, output_dir)
                        if results_prefix:
                            objpath = results_prefix + "/" + objpath
                        if workdir_in_bucket:
                            # The workspace IS the bucket: `target` is already an
                            # object. Point at where it actually lives rather than
                            # copying it to a second location.
                            objpath = workspace_objpath(
                              target, results_bucket, results_prefix, jobId
                            )
                        else:
                            uploads.append((
                              os.path.abspath(target),
                              "gs://{}/{}".format(results_bucket, objpath),
                              custom_time,
                            ))
                        os.symlink(
                          "{}/{}/{}".format(BUCKETMOUNT_ROOT, results_bucket, objpath),
                          dest
                        )
                    # Same volume check catches outputs from outside the workspace
                    elif name in copy or not same_volume(target, jobdir):
                        print(f'INFO: copying (not symlinking) file \'{target}\' (name "{name}", pattern "{pattern}")', file = sys.stderr)
                        if os.path.isfile(target):
                            shutil.copyfile(os.path.abspath(target), dest)
                        else:
                            shutil.copytree(target, dest)
                    # TODO: is st_dev check equivalent to same_volume?
                    elif os.stat(target).st_dev == os.stat(os.path.dirname(dest)).st_dev:
                        os.symlink(os.path.relpath(target, os.path.dirname(dest)), dest)
                    else:
                        os.symlink(os.path.abspath(target), dest)

                # write job manifest
                manifest.write("{}\t{}\t{}\t{}\n".format(
                    jobId,
                    name,
                    pattern,
                    os.path.relpath(dest.strip(), output_dir)
                ))

                # append to array of files that will be hashed
                matched_files.append([dest, target])

                n_matched += 1

            # warn if no files matched; make log in manifest
            if n_matched == 0:
                print('WARNING: output name "{0}" (pattern "{1}") not found.'.format(name,  pattern), file = sys.stderr)
                manifest.write("{}\t{}\t{}\t{}\n".format(
                    jobId,
                    name,
                    pattern,
                    "//not_found"
                ))
            else:
                print('INFO: matched output name "{0}" (pattern "{1}")'.format(name,  pattern), file = sys.stderr)

    # push outputs to the results bucket. Worker -> GCS directly; no byte goes
    # through the controller.
    #
    # Done per-file rather than with `gcloud storage rsync`: the outputs tree is
    # a tree of symlinks (and, in this mode, deliberately broken ones), and
    # rsync ignores symlinks by default while --no-ignore-symlinks would upload
    # them as placeholder objects rather than the data they point at.
    # save_intermediates: move the WHOLE workspace, not just declared outputs, so a
    # failed or suspicious task can still be inspected after the worker is gone.
    # Under a local workdir the workspace dies with the VM, which is exactly the
    # debuggability the shared-NFS layout used to provide.
    #
    # Kept under its own _workspace/ prefix so it can never collide with declared
    # outputs and can be given its own lifecycle rule later if the volume warrants
    # it -- intermediates are typically orders of magnitude larger than results.
    if save_intermediates and not workdir_in_bucket:
        workspace = os.environ.get("CANINE_JOB_WORKSPACE")
        if workspace and os.path.isdir(workspace):
            dst = "gs://{}/{}/{}/_workspace/".format(
              results_bucket, results_prefix.rstrip("/"), jobId
            ) if results_prefix else "gs://{}/{}/_workspace/".format(results_bucket, jobId)
            print(
              'Saving intermediates: {} -> {}'.format(workspace, dst),
              file = sys.stderr, flush = True
            )
            _dst, rc, err = upload_one((workspace.rstrip("/") + "/", dst, custom_time))
            if rc != 0:
                # Non-fatal: the task itself succeeded and its declared outputs are
                # already safe. Losing a debugging aid must not fail the shard.
                print(
                  'WARNING: could not save intermediates to {}: {}'.format(dst, err.strip()),
                  file = sys.stderr
                )
        else:
            print(
              'WARNING: save_intermediates set but CANINE_JOB_WORKSPACE is not a '
              'directory; nothing saved.', file = sys.stderr
            )

    if uploads:
        print(
          'Uploading {} output(s) to gs://{} ...'.format(len(uploads), results_bucket),
          file = sys.stderr, flush = True
        )
        # Threads, not processes: each task is a blocking subprocess call, so
        # there is no GIL contention to escape, and a thread pool avoids
        # pickling the work items across a fork.
        pool = multiprocessing.pool.ThreadPool(8)
        try:
            failures = [
              (dst, rc, err) for dst, rc, err in pool.imap_unordered(upload_one, uploads)
              if rc != 0
            ]
        finally:
            pool.terminate()

        if failures:
            # Hard failure. The output symlinks written above point into the
            # bucket, so leaving a partial upload behind would hand downstream
            # tasks links that resolve to nothing -- far worse than failing the
            # shard here and letting it be retried.
            for dst, rc, err in failures:
                print(
                  'ERROR: failed to upload {} (exit {}): {}'.format(dst, rc, err.strip()),
                  file = sys.stderr
                )
            sys.exit(1)
        print('Uploaded {} output(s).'.format(len(uploads)), file = sys.stderr, flush = True)

    # compute checksums for all files, if job exited successfully
    if os.environ["CANINE_JOB_RC"] == "0":
        print('Computing CRC32C checksums ...', file = sys.stderr, flush = True, end = "")
        pool = multiprocessing.Pool(8)
        crc_results = []
        for dest, f in matched_files:
            crc_path = os.path.join(os.path.dirname(dest), "." + os.path.basename(dest) + ".crc32c")
            # if we are delocalizing from a finished scratch disk, do not bother
            # recomputing checksum for this file if it already exists
            if scratch and finished_scratch and os.path.exists(crc_path):
                continue
            crc_results.append((pool.apply_async(compute_crc32c, (f, scratch)), crc_path))

        for res in crc_results:
            crc = res[0].get()
            crc_path = res[1]
            with open(crc_path, "w") as crc32c_file:
                crc32c_file.write(crc + "\n")
        pool.terminate()
        print(' done', file = sys.stderr, flush = True)

if __name__ == '__main__':
    parser = argparse.ArgumentParser('canine-delocalizer')
    parser.add_argument(
        'dest',
        help="Destination directory",
    )
    parser.add_argument(
        'jobId',
        help="Job ID"
    )
    parser.add_argument(
        '-p', '--pattern',
        nargs=2,
        action='append',
        help="Pattern name and pattern. in form '-p {name} {pattern}'",
        default=[]
    )
    parser.add_argument(
        '-c', '--copy',
        action='append',
        help="Copy output <name> instead of symlinking",
        default=[]
    )
    parser.add_argument(
        '-s', '--scratch',
        action='store_true',
        help="Outputs were written to a scratch disk; will create (broken) symlinks to the scratch diskmountpoint"
    )
    parser.add_argument(
        '-F', '--finished_scratch',
        action='store_true',
        help="Scratch disk was finished; delocalizer is only running to (re)generate output directory. Will skip computing CRC32 hashes if they were precomputed to save time."
    )
    parser.add_argument(
        '--results_bucket',
        default=None,
        help="Bucket name (no gs:// prefix) to upload outputs to. Set when the job ran "
             "in a worker-local workdir; outputs are recorded as symlinks into the "
             "future gcsfuse mount of this bucket rather than copied to the shared mount."
    )
    parser.add_argument(
        '--results_prefix',
        default="",
        help="Object path prefix within --results_bucket, identifying this task run."
    )
    parser.add_argument(
        '--custom_time',
        default=None,
        help="RFC3339 timestamp stamped on uploaded objects. The results bucket's "
             "lifecycle rule is keyed on daysSinceCustomTime, so this is what starts "
             "each object's expiry clock."
    )
    parser.add_argument(
        '--workdir_in_bucket',
        action='store_true',
        help="The job workspace was itself a read-write mount of --results_bucket, so "
             "outputs are already objects; record pointers to them instead of uploading."
    )
    parser.add_argument(
        '--save_intermediates',
        action='store_true',
        help="Also copy the entire job workspace (not just declared outputs) to "
             "<results_prefix>/<shard>/_workspace/, so it can be inspected after the "
             "worker is gone."
    )
    args = parser.parse_args()
    main(
      args.dest, args.jobId, args.pattern, set(args.copy), args.scratch,
      args.finished_scratch, args.results_bucket, args.results_prefix, args.custom_time,
      args.workdir_in_bucket, args.save_intermediates
    )
