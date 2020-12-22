#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

with open(here / "README.rst") as readme_file:
    readme = readme_file.read()

with open(here / "HISTORY.rst") as history_file:
    history = history_file.read()

requirements = [ ]

setup(
    author="PlanetaryPy Developers",
    author_email="kmichael.aye@gmail.com",
    python_requires=">=3.6, <4",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    description="Core package for planetary science tools.",
    entry_points={
        "console_scripts": [
            'planetarypy=planetarypy.cli:main',
        ],
    },
    install_requires=requirements,
    license="BSD license",
    long_description=readme + "\n\n" + history,
    keywords="planetarypy",
    name="planetarypy",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    test_suite="tests",
    tests_require=["pytest"],
    url="https://github.com/michaelaye/planetarypy",
    version="0.1.0",
)