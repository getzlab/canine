from setuptools import setup
import re
import os

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
            'backends/slurm-gcp/scripts/*'
        ],
    },
    entry_points={
        'console_scripts':[
            'canine = canine.__main__:main',
            'canine-transient = canine.__main__:boot_transient'
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
        'hound>=0.1.9',
        'firecloud-dalmatian>=0.0.11'
    ],
    classifiers = [
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator",
    ],
    license="BSD3"
)
