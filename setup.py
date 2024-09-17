#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

with open(here / "README.rst") as readme_file:
    readme = readme_file.read()

with open(here / "HISTORY.rst") as history_file:
    history = history_file.read()

with open(here / "requirements.txt") as requirements_file:
    requirements = requirements_file.read().splitlines()

# requirements = ["tomlkit", "requests", "tqdm", "pandas", "astropy", "spiceypy", "dask", "yarl", "matplotlib"]
dev_requirements = ["pytest"]

setup(
    author="PlanetaryPy Developers",
    author_email="kmichael.aye@gmail.com",
    python_requires=">=3.9, <4",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    description="Core package for planetary science tools.",
    entry_points={
        "console_scripts": [
            "planetarypy=planetarypy.cli:main",
        ],
    },
    install_requires=requirements,
    extras_require={'dev': dev_requirements},
    include_package_data=True,
    license="BSD license",
    long_description=readme + "\n\n" + history,
    keywords="planetarypy",
    name="planetarypy",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    test_suite="tests",
    tests_require=["pytest"],
    url="https://github.com/planetarypy/planetarypy",
    version="0.2.0",
)
