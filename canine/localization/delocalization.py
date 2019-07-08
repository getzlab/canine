from __future__ import print_function
import argparse
import glob
import os
import shutil

"""
This is not actually part of the canine package
This is helper code which is run in the backend
"""

def main(dest, jobId, patterns):
    jobdir = os.path.join(dest, str(jobId))
    if not os.path.isdir(jobdir):
        os.makedirs(jobdir)
    for name, pattern in patterns:
        for target in glob.iglob(pattern):
            dest = os.path.join(jobdir, name, os.path.basename(target))
            if not os.path.exists(dest):
                if not os.path.isdir(os.path.dirname(dest)):
                    os.makedirs(os.path.dirname(dest))
                if os.path.isfile(target):
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
    args = parser.parse_args()
    main(args.dest, args.jobId, args.pattern)
