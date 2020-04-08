from __future__ import print_function
import argparse
import glob
import os
import shutil

"""
This is not actually part of the canine package
This is helper code which is run in the backend
"""

def main(output_dir, jobId, patterns, copy):
    jobdir = os.path.join(output_dir, str(jobId))
    if not os.path.isdir(jobdir):
        os.makedirs(jobdir)
    with open(os.path.join(jobdir, '.canine_job_manifest'), 'w') as manifest:
        for name, pattern in patterns:
            for target in glob.iglob(pattern):
                if name in {'stdout', 'stderr'}:
                    dest = os.path.join(jobdir, name)
                else:
                    dest = os.path.join(jobdir, name, os.path.relpath(target))
                manifest.write("{}\t{}\t{}\n".format(
                    jobId,
                    name,
                    os.path.relpath(dest.strip(), output_dir)
                ))
                if not os.path.exists(dest):
                    if not os.path.isdir(os.path.dirname(dest)):
                        os.makedirs(os.path.dirname(dest))
                    if os.path.isfile(target):
                        if copy:
                            shutil.copyfile(os.path.abspath(target), dest)
                        else:
                            os.symlink(os.path.abspath(target), dest)
                    else:
                        shutil.copytree(target, dest)

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
