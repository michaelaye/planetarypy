# planetarypy

[![PyPI](https://img.shields.io/pypi/v/planetarypy.svg)](https://pypi.python.org/pypi/planetarypy)
[![Documentation](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://michaelaye.github.io/planetarypy/)
[![codecov](https://codecov.io/gh/planetarypy/planetarypy/branch/main/graph/badge.svg)](https://codecov.io/gh/planetarypy/planetarypy)

Core package for planetary science tools.

## Warning

This package is in early alpha.

Core dev Michael Aye has developed a set of tools that, step by step and under reviews
of the `planetarypy` org technical committee, will be incorporated into a new core library
for working in planetary science in Python.
That package will be open for public contribution and collaboration.

The approximate feature set (also see below) that this package will cover can be checked out at
the [documentation site](https://michaelaye.github.io/planetarypy/).

* Free software: BSD-3 license
* Documentation: https://michaelaye.github.io/planetarypy/
* Part of the [PlanetaryPy](https://planetarypy.org/) Organisation.

## Installation

From PyPI (once published):

```bash
pip install planetarypy
```

From GitHub:

```bash
pip install git+https://github.com/planetarypy/planetarypy.git
```

For development installation:

```bash
# Clone the repository
git clone https://github.com/planetarypy/planetarypy.git
cd planetarypy

# Install in development mode with dev dependencies
pip install -e ".[dev]"
```

## Features

### PDS Index Tools

The package includes robust tools for working with PDS (Planetary Data System) indices:

- **Index Configuration**: Manage URLs and access timestamps in separate configuration files
- **Dynamic URL Discovery**: Automatically find and use the latest index files for mission data
- **Index Browsing**: Easily explore available missions, instruments, and indices

```python
from planetarypy.pds import print_available_indexes, get_mission_names, get_instrument_names, get_index_names

# Display all available PDS indices in a tree structure
print_available_indexes()

# Get a list of all missions
missions = get_mission_names()

# Get all instruments for a specific mission
instruments = get_instrument_names('mro')

# Get all indices for a specific mission and instrument
indices = get_index_names('cassini.iss')
```

- **Index Management**: Download, update, and convert indices to high-performance parquet format

```python
from planetarypy.pds import Index

# Create an index object
idx = Index('cassini.iss.ring_summary')

# Check if updates are available
idx.check_for_updates()

# Download the index
idx.download()
```

## General scope

First and foremost this package shall provide support in working with planetary science data.

With working we mean:

- locating
- retrieving
- reading
- further processing

of data.

### Locating

This library manages, via its PDS tools, multiple PDS3 index files per instrument that can be
used for identifying data of interest.
These index files are automatically downloaded and converted to the very performant
(and cloud-ready) parquet file format.
Parquet is able to store advanced datatypes like nan-capable integer and full datetime objects,
as opposed to HDF5.

### Retrieving

The interface to getting data is via a path-retrieving function based on a PDS product-id.
If that product-id is available locally, the path will be returned.
If it is not, it will previously be downloaded, stored in a systematic fashion organized by
mission and instrument, and then the local path will be returned.

### Reading

For now, the library only returns the path to the object and the user needs to sort out the
reading process.
A recently funded NASA project Planetary Data Reader will be integrated here, so that basic
reading into memory can be provided.

As such, we anticipate two classes of reading support:

1. basic reading into numpy and/or xarray
2. added reader functionality like basic plots and basic geospatial processing, as supported by
   interested parties

There will exist larger other packages that focus on working with a given instrument's data,
in which case that package could become an affiliated package with the `planetarypy` GitHub
organization, if so desired.

### Further processing

In the future, additional frequently used procedures will be added to this library, e.g.

- frequently used GDAL/rasterio procedures
- frequently used SPICE operations, e.g. surface illumination on a given body

## Project History

This project evolved through several iterations, each building on lessons learned.

### Origins: planetarypy (2015-2020)

The original `planetarypy` was a private collection of planetary science tools
created by K.-Michael Aye, inspired by the organizational approach of `astropy`.
Key features included NASA factsheet parsing, PDS index tools for Cassini ISS
and MRO CTX, SPICE kernel management, and Mars-specific image processing.

### Evolution: nbplanetary (2021-2025)

The project was rewritten using nbdev (notebook-driven development), significantly
expanding to include full instrument modules (CTX, HiRISE, UVIS, CISS, Diviner),
ISIS integration via kalasiris, Dask-based parallel processing, and CLI tools.

### Current Focus (2025-present)

The current `planetarypy` represents a deliberate refocusing on core functionality.
Rather than maintaining all features, the goal is to provide fewer features that
work reliably and are well-documented. Advanced features from previous iterations
may be reintroduced as the core stabilizes.

See [CHANGELOG.md](CHANGELOG.md) for detailed version history.

## Contributing

Feedback, issues, and contributions are always gratefully welcomed. See the
[Contributing Guide](https://michaelaye.github.io/planetarypy/contributing.html)
for details on how to help and set up a development environment. 