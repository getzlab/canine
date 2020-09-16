from setuptools import setup
import re
import os
import sys

ver_info = sys.version_info
if ver_info < (3,7,0):
    raise RuntimeError("canine requires at least python 3.7")

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
        'canine.backends.dummy',
        'canine.adapters',
        'canine.localization'
    ],
    package_data={
        '':[
            'backends/slurm-gcp/*',
            'backends/slurm-gcp/scripts/*',
            'backends/slurm-docker/src/*',
            'backends/dummy/*',
            'backends/dummy/conf/*'
        ],
    },
    entry_points={
        'console_scripts':[
            'canine = canine.__main__:main',
            'canine-transient = canine.__main__:boot_transient',
            'canine-xargs = canine.__main__:xargs'
        ]
    },
    description = 'A modular, high-performance computing solution to run jobs using SLURM',
    url = 'https://github.com/getzlab/canine',
    author = 'Aaron Graubert - Broad Institute - Cancer Genome Computational Analysis - Getz Lab',
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
        'psutil>=5.6.7',
        'port-for>=0.4',
        'crayons>=0.3.0',
        'tables>=3.6.1'
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
