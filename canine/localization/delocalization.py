from __future__ import print_function
import argparse
import glob
import google_crc32c
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

def compute_crc32c(path):
    """
    Adapted from localization/file_handlers.py.
    """
    hash_alg = google_crc32c.Checksum()
    buffer_size = 8 * 1024

    if os.path.isdir(path):
        files = glob.iglob(path + "/**", recursive = True)
    else:
        files = [path]

    for f in files:
        if os.path.isdir(f):
            continue

        file_size_MiB = int(os.path.getsize(path)/1024**2)
        print(f"Hashing file {f} ({file_size_MiB} MiB)", file = sys.stderr, flush = True)

        ct = 0
        with open(f, "rb") as fp:
            while True:
                # output message every 100 MiB
                if ct > 0 and not ct % int(100*1024**2/buffer_size):
                    print(f"Hashing file {f}; {int(buffer_size*ct/1024**2)}/{file_size_MiB} MiB completed", file = sys.stderr, flush = True)

                data = fp.read(buffer_size)
                if not data:
                    break
                hash_alg.update(data)
                ct += 1

    return hash_alg.hexdigest().decode().upper()

def main(output_dir, jobId, patterns, copy, scratch):
    jobdir = os.path.join(output_dir, str(jobId))
    if not os.path.isdir(jobdir):
        os.makedirs(jobdir)
    matched_files = []
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
                if not os.path.exists(dest):
                    if not os.path.isdir(os.path.dirname(dest)):
                        os.makedirs(os.path.dirname(dest))
                    # we've output to a scratch disk; create (broken) symlink to RODISK mount
                    if scratch:
                        os.symlink(os.path.abspath(target), dest)
                    # Same volume check catches outputs from outside the workspace
                    elif copy or not same_volume(target, jobdir):
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

    # compute checksums for all files
    print('Computing CRC32C checksums ...', file = sys.stderr, flush = True, end = "")
    for dest, f in matched_files:
        # don't hash stdout/stderr
        if f.startswith(".."):
            continue
        # compute hash for this file
        with open(os.path.join(
            os.path.dirname(dest),
            "." + os.path.basename(dest) + ".crc32c"
          ), "w") as crc32c_file:
            crc32c_file.write(compute_crc32c(f) + "\n")
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
        action='store_true',
        help="Copy outputs instead of symlinking"
    )
    parser.add_argument(
        '-s', '--scratch',
        action='store_true',
        help="Outputs were written to a scratch disk; will create (broken) symlinks to the scratch diskmountpoint"
    )
    args = parser.parse_args()
    main(args.dest, args.jobId, args.pattern, args.copy, args.scratch)
