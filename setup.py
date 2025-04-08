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
            'backends/dummy/conf/*',
            'localization/debug.sh'
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
        'pandas==1.4.2',
        'numpy>=1.18.0',
        'google-auth>=1.6.3',
        'PyYAML>=5.1',
        'agutil>=4.1.0',
        'hound>=0.2.0',
        'firecloud-dalmatian @ git+https://github.com/getzlab/dalmatian@d39177e',
        'google-api-python-client>=1.7.11',
        'docker>=4.1.0',
        'psutil>=5.6.7',
        'port-for>=0.4',
        'tables>=3.6.1',
        'google-crc32c>=1.5.0',
        'google-cloud-compute>=1.6.1',
    ] + (['slurm_gcp_docker @ git+https://github.com/getzlab/slurm_gcp_docker@n4-hyperdisk']
          if not os.path.exists("/.dockerenv") else []
        ), # avoid circular dependency of slurm_gcp_docker -> wolf -> canine,
    python_requires = ">3.7",
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
