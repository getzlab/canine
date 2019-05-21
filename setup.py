from setuptools import setup
from canine import __version__

setup(
    name = 'canine',
    version = __version__,
    packages = [
        'canine',
    ],
    description = 'A dalmatian-based job manager to schedule tasks using SLURM',
    url = 'https://github.com/broadinstitute/canine',
    author = 'Aaron Graubert - Broad Institute - Cancer Genome Computational Analysis',
    author_email = 'aarong@broadinstitute.org',
    long_description = "A dalmatian-based job manager to schedule tasks using SLURM",
    # long_description_content_type = 'text/markdown',
    install_requires = [
    ],
    classifiers = [
        "Programming Language :: Python :: 3",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator",
    ],
    license="BSD3"
)
