"""SPICE configuration.

This module contains common configuration elements used across the SPICE-related modules.
"""

__all__ = [
    "KERNEL_STORAGE",
    "NAIF_URL",
    "BASE_URL",
]

from yarl import URL

from ..config import config

# Base paths and URLs
KERNEL_STORAGE = config.storage_root / "spice_kernels"
KERNEL_STORAGE.mkdir(exist_ok=True, parents=True)

NAIF_URL = URL("https://naif.jpl.nasa.gov")
BASE_URL = NAIF_URL / "cgi-bin/subsetds.pl"
