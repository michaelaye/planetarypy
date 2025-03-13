"""SPICE generic kernels management.

This module provides functions for downloading and loading generic SPICE kernels
that are required for basic operations and calculations.
"""

__all__ = [
    "GENERIC_STORAGE",
    "GENERIC_URL",
    "generic_kernel_names",
    "generic_kernel_paths",
    "download_generic_kernels",
    "load_generic_kernels",
    "show_loaded_kernels",
]

from pathlib import Path

import spiceypy as spice

from ..utils import url_retrieve
from .config import KERNEL_STORAGE, NAIF_URL

# Generic kernels configuration
GENERIC_STORAGE = KERNEL_STORAGE / "generic"
GENERIC_STORAGE.mkdir(exist_ok=True, parents=True)
GENERIC_URL = NAIF_URL / "pub/naif/generic_kernels/"

generic_kernel_names = [
    "lsk/naif0012.tls",
    "pck/pck00010.tpc",
    "pck/de-403-masses.tpc",
    "spk/planets/de430.bsp",
    "spk/satellites/mar097.bsp",
]
generic_kernel_paths = [GENERIC_STORAGE.joinpath(i) for i in generic_kernel_names]


def download_generic_kernels(overwrite=False):
    """Download all kernels required for basic SPICE operations.

    Parameters
    ----------
    overwrite : bool, optional
        Whether to overwrite existing files, by default False
    """
    dl_urls = [GENERIC_URL / i for i in generic_kernel_names]
    for dl_url, savepath in zip(dl_urls, generic_kernel_paths):
        if savepath.exists() and not overwrite:
            print(
                savepath.name,
                "already downloaded. Use `overwrite=True` to download again.",
            )
            continue
        savepath.parent.mkdir(exist_ok=True, parents=True)
        url_retrieve(dl_url, savepath)


def load_generic_kernels():
    """Load all kernels in generic_kernels list.

    Loads pure planetary bodies meta-kernel without spacecraft data.
    Downloads any missing generic kernels.
    """
    if any([not p.exists() for p in generic_kernel_paths]):
        download_generic_kernels()
    for kernel in generic_kernel_paths:
        spice.furnsh(str(kernel))


def show_loaded_kernels():
    """Print overview of all currently loaded SPICE kernels."""
    count = spice.ktotal("all")
    if count == 0:
        print("No kernels loaded at this time.")
    else:
        print("The loaded files are:\n(paths relative to kernels.KERNEL_STORAGE)\n")
    for which in range(count):
        out = spice.kdata(which, "all", 100, 100, 100)
        print("Position:", which)
        p = Path(out[0])
        print("Path", p.relative_to(KERNEL_STORAGE))
        print("Type:", out[1])
        print("Source:", out[2])
        print("Handle:", out[3])
        # print("Found:", out[4])
