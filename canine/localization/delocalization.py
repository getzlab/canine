from __future__ import print_function
import argparse
import glob
import os
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

def get_gsfile_crc32c(path):
    """
    From wolf.Task
    """
    ## TODO: does not work for directories
    cmd = 'gsutil hash -c -h {}'.format(shlex.quote(path))
    output = subprocess.check_output(cmd, shell=True).decode()
    output = output.rstrip().split("\n")
    output = [o for o in output if re.search(r"^\tHash \(crc32c\):\t\t", o)]
    assert len(output) == 1
    output = output[0]
    output = re.sub(r"^\tHash \(crc32c\):\t\t", "", output)
    return output

def main(output_dir, jobId, patterns, copy):
    jobdir = os.path.join(output_dir, str(jobId))
    if not os.path.isdir(jobdir):
        os.makedirs(jobdir)
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
                    if os.path.isfile(target):
                        # Same volume check catches outputs from outside the workspace
                        if copy or not same_volume(target, jobdir):
                            shutil.copyfile(os.path.abspath(target), dest)
                        elif os.stat(target).st_dev == os.stat(os.path.dirname(dest)).st_dev:
                            os.symlink(os.path.relpath(target, os.path.dirname(dest)), dest)
                        else:
                            os.symlink(os.path.abspath(target), dest)
                    else:
                        shutil.copytree(target, dest)

                # compute checksum
                if name not in {'stdout', 'stderr'}:
                    try:
                        crc32 = get_gsfile_crc32c(target)
                        with open(
                          os.path.join(
                            jobdir,
                            name,
                            os.path.dirname(os.path.relpath(target)),
                            "." + os.path.basename(target) + ".crc32c"
                          ),
                          "w"
                        ) as crc32_file:
                            crc32_file.write(crc32 + "\n")
                    except subprocess.CalledProcessError:
                        print("Error computing checksum for {}!".format(target), file = sys.stderr)

                # write job manifest
                manifest.write("{}\t{}\t{}\t{}\n".format(
                    jobId,
                    name,
                    pattern,
                    os.path.relpath(dest.strip(), output_dir)
                ))

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
    args = parser.parse_args()
    main(args.dest, args.jobId, args.pattern, args.copy)
