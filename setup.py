from setuptools import setup
import re
import os
import sys

ver_info = sys.version_info
if ver_info < (3,5,4):
    raise RuntimeError("canine requires at least python 3.5.4")

with open(os.path.join(os.path.dirname(__file__), 'canine', 'orchestrator.py')) as r:
    version = re.search(r'version = \'(\d+\.\d+\.\d+[-_a-zA-Z0-9]*)\'', r.read()).group(1)

with open("README.md") as r:
    long_description = r.read()

setup(
    name = 'canine',
    version = version,
    packages = [
        'canine',
        'canine.backends',
        'canine.adapters',
        'canine.localization'
    ],
    package_data={
        '':[
            'backends/slurm-gcp/*',
            'backends/slurm-gcp/scripts/*',
            'backends/slurm-docker/src/*'
        ],
    },
    entry_points={
        'console_scripts':[
            'canine = canine.__main__:main',
            'canine-transient = canine.__main__:boot_transient',
            'canine-xargs = canine.__main__:xargs'
        ]
    },
    description = 'A dalmatian-based job manager to schedule tasks using SLURM',
    url = 'https://github.com/broadinstitute/canine',
    author = 'Aaron Graubert - Broad Institute - Cancer Genome Computational Analysis',
    author_email = 'aarong@broadinstitute.org',
    long_description = long_description,
    long_description_content_type = 'text/markdown',
    install_requires = [
        'paramiko>=2.5.0',
        'pandas>=0.24.1',
        'google-auth>=1.6.3',
        'PyYAML>=5.1',
        'agutil>=4.1.0',
        'hound>=0.2.0',
        'firecloud-dalmatian>=0.0.17',
        'google-api-python-client>=1.7.11',
        'docker>=4.1.0',
        'psutil>=5.6.7'
    ],
    classifiers = [
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: System :: Clustering",
        "Topic :: System :: Distributed Computing",
        "Typing :: Typed",
        "License :: OSI Approved :: BSD License"
    ],
    license="BSD3"
)
