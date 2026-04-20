# planetarypy

[![PyPI](https://img.shields.io/pypi/v/planetarypy.svg)](https://pypi.python.org/pypi/planetarypy)
[![Documentation](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://michaelaye.github.io/planetarypy/)
[![codecov](https://codecov.io/gh/planetarypy/planetarypy/branch/main/graph/badge.svg)](https://codecov.io/gh/planetarypy/planetarypy)

Python tools for planetary science data access.

> **Note for users of v0.32 and earlier (nbplanetary):**
> This version (0.50+) is a ground-up rewrite with a new API.
> If you need functionality from the previous version, it remains available at
> [github.com/michaelaye/nbplanetary](https://github.com/michaelaye/nbplanetary).
> Features from nbplanetary will be reintegrated over time.

* Free software: BSD-3 license
* Documentation: https://michaelaye.github.io/planetarypy/
* Part of the [PlanetaryPy](https://planetarypy.org/) Organisation.

## Installation

```bash
pip install planetarypy
```

## Features

### PDS Index Retrieval

90+ PDS cumulative index files, auto-downloaded and cached as Parquet:

```python
from planetarypy import pds

df = pds.get_index("mro.ctx.edr")       # 164,103 CTX images as DataFrame
df = pds.get_index("cassini.iss.index")  # 407,299 Cassini ISS images
```

### PDS Catalog

65 missions, 2042 product types from the entire PDS archive:

```python
from planetarypy.catalog import list_missions, list_products, fetch_product

list_missions()              # ['apollo', 'cassini', 'dawn', ...]
list_products("mro.ctx")     # ['edr']

# Download any product by ID
fetch_product("mro.ctx.edr", "P02_001916_2221_XI_42N027W")
```

Direct data access for 58 product types across 29 instruments on 15 missions.

### SPICE Kernels

37 archived missions with date-filtered kernel subsets:

```python
from planetarypy.spice import archived_kernels as ak

mk = ak.get_metakernel_and_files("mro", start="2024-01-01", stop="2024-01-31")
```

### Command-Line Interface

```bash
plp fetch mro.ctx.edr P02_001916_2221_XI_42N027W   # download a product
plp hibrowse PSP_003092_0985_RED                    # HiRISE browse JPEG
plp hifetch PSP_003092_0985_RED                     # HiRISE full product
plp ctxqv J05_046771_1950                           # CTX quickview
plp catalog build                                   # build catalog DB
```

## General scope

First and foremost this package provides support in working with planetary science data.

With working we mean:

- locating
- retrieving
- reading
- further processing

of data.

### Locating

This library manages, via its PDS tools, 90+ PDS3 index files per instrument that can be
used for identifying data of interest.
These index files are automatically downloaded and converted to the very performant
(and cloud-ready) parquet file format.
Parquet is able to store advanced datatypes like nan-capable integer and full datetime objects,
as opposed to HDF5.

The PDS Catalog module provides a searchable database of 65 missions, 2042 product types,
built from the [pdr-tests](https://github.com/MillionConcepts/pdr-tests) repository
into a local DuckDB database.

### Retrieving

The interface to getting data is via `fetch_product()` based on a dotted product key and
a PDS product ID.
If the product is available locally, the path will be returned.
If it is not, it will be downloaded, stored in a systematic fashion organized by
mission and instrument, and then the local path will be returned.

```python
from planetarypy.catalog import fetch_product

path = fetch_product("mro.ctx.edr", "P02_001916_2221_XI_42N027W")
```

Direct data access is currently supported for 58 product types across 29 instruments
on 15 missions, resolved via PDS cumulative index files.

### Reading

For now, the library returns the path to the object and the user needs to sort out the
reading process.
The [Planetary Data Reader (pdr)](https://github.com/MillionConcepts/pdr) can be used
to read most PDS3 and PDS4 products into memory.

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

## Development Installation

```bash
git clone https://github.com/planetarypy/planetarypy.git
cd planetarypy
pip install -e ".[dev]"
```

## Contributing

Feedback, issues, and contributions are always gratefully welcomed. See the
[Contributing Guide](https://michaelaye.github.io/planetarypy/contributing.html)
for details on how to help and set up a development environment.