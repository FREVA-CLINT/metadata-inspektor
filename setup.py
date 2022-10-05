#!/usr/bin/env python3
"""Setup script for packaging metadata-inspektor."""

from pathlib import Path
import re
from setuptools import setup, find_packages


def read(*parts):
    """Read the content of a file."""
    script_path = Path(__file__).parent
    with script_path.joinpath(*parts).open() as f:
        return f.read()


def find_version(*parts):
    """The the version in a given file."""
    vers_file = read(*parts)
    match = re.search(r'^__version__ = "(\d+.\d+.\d+)"', vers_file, re.M)
    if match is not None:
        return match.group(1)
    raise RuntimeError("Unable to find version string.")


setup(
    name="metadata_inspector",
    version=find_version("src", "metadata_inspector", "_version.py"),
    author="Martin Bergemann",
    author_email="bergemann@dkrz.de",
    maintainer="Martin Bergemann",
    url="https://gitlab.dkrz.de/freva/metadata-inspektor.git",
    description="Inspect metadata of weather/climate datasets",
    long_description=read("README.md"),
    license="GPLv3",
    packages=find_packages("src"),
    package_dir={"": "src"},
    entry_points={
        "console_scripts": ["metadata-inspector = metadata_inspector:cli"]
    },
    install_requires=[
        "cftime",
        "dask",
        "hurry.filesize",
        "h5netcdf",
        "zarr",
        "xarray",
    ],
    extras_require={
        "tests": [
            "black",
            "pytest",
            "pandas",
            "mock",
            "numpy",
            "requests-mock",
            "pytest-env",
            "pytest-cov",
            "testpath",
            "flake8",
            "mypy",
            "types-mock",
            "types-PyYAML",
            "types-requests",
            "types-setuptools",
        ],
    },
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3",
    ],
)
