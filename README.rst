planetarypy
===========


.. image:: https://img.shields.io/pypi/v/planetarypy.svg
        :target: https://pypi.python.org/pypi/planetarypy

.. image:: https://readthedocs.org/projects/planetarypy/badge/?version=latest
        :target: https://planetarypy.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status

.. image:: https://codecov.io/gh/planetarypy/planetarypy/branch/main/graph/badge.svg
        :target: https://codecov.io/gh/planetarypy/planetarypy
        
Core package for planetary science tools.

Warning
-------

This package is in early alpha.

Core dev Michael Aye has developed a set of tools that, step by step and under reviews
of the `planetarypy` org technical committee, will be incorporated into a new core library
for working in planetary science in Python.
That package will be open for public contribution and collaboration.

The approximate feature set (also see below) that this package will cover can be checked out at
https://michaelaye.github.io/nbplanetary where the documentation for the current package lives.


* Free software: BSD-3 license
* Documentation (not yet deployed): https://planetarypy.readthedocs.io.
* Part of the `PlanetaryPy`_ Organisation.


Installation
------------

From PyPI (once published):

.. code-block:: bash

    pip install planetarypy

From GitHub:

.. code-block:: bash

    pip install git+https://github.com/planetarypy/planetarypy.git

For development installation:

.. code-block:: bash

    # Clone the repository
    git clone https://github.com/planetarypy/planetarypy.git
    cd planetarypy
    
    # Install in development mode with dev dependencies
    pip install -e ".[dev]"

General scope
-------------

First and foremost this package shall provide support in working with planetary science data.

With working we mean:

- locating
- retrieving
- reading
- further processing

of data.

Locating
^^^^^^^^

This library manages, via its PDS tools, multiple PDS3 index files per instrument that can be
used for identifying data of interest.
These index files are automatically downloaded and converted to the very performant
(and cloud-ready) parquet file format.
Parquet is able to store advanced datatypes like nan-capable integer and full datetime objects,
as opposed to HDF5.

Retrieving
^^^^^^^^^^

The interface to getting data is via a path-retrieving function based on a PDS product-id.
If that product-id is available locally, the path will be returned.
If it is not, it will previously be downloaded, stored in a systematic fashion organized by
mission and instrument, and then the local path will be returned.

Reading
^^^^^^^

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

Further processing
^^^^^^^^^^^^^^^^^^

In the future, additional frequently used procedures will be added to this library, e.g.

- frequently used GDAL/rasterio procedures
- frequently used SPICE operations, e.g. surface illumination on a given body

Contributing
------------

Feedback, issues, and contributions are always gratefully welcomed. See the
contributing guide for details on how to help and setup a development
environment, see :ref:Contributing .


.. _PlanetaryPy: https://planetarypy.org/
