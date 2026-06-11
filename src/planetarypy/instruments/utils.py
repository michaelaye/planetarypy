"""Backward-compatible shim.

``read_image`` moved to :mod:`planetarypy.io` (it's a general-purpose reader, not
instrument-specific). Importing it from here still works for existing callers.
"""

from planetarypy.io import read_image  # noqa: F401
