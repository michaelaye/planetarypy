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
    "spk/satellites/mar099s.bsp",
]
generic_kernel_paths = [GENERIC_STORAGE.joinpath(i) for i in generic_kernel_names]

# Satellite ephemeris kernels per planetary system.
# Downloaded on demand via load_system(). These are large files because
# they cover decades of ephemeris at high precision.
# Each includes the planet-to-barycenter offset AND satellite positions.
SATELLITE_KERNELS = {
    "jupiter": "spk/satellites/jup365.bsp",   # ~1.1 GB, all Jupiter moons
    "saturn":  "spk/satellites/sat441.bsp",    # ~630 MB, all Saturn moons
    "neptune": "spk/satellites/nep097.bsp",    # ~100 MB, all Neptune moons
    "pluto":   "spk/satellites/plu060.bsp",    # ~129 MB, Pluto system
}

def _body_to_system(body_name: str) -> str | None:
    """Determine which planetary system a body belongs to.

    Uses the NAIF body code: 5xx=Jupiter, 6xx=Saturn, etc.
    """
    body = body_name.upper()
    # Direct planet matches
    planet_map = {
        "JUPITER": "jupiter", "SATURN": "saturn",
        "URANUS": "uranus", "NEPTUNE": "neptune", "PLUTO": "pluto",
    }
    if body in planet_map:
        return planet_map[body]
    # Moon codes: first digit of (code % 1000) tells the planet
    try:
        code = spice.bodn2c(body)
    except Exception:
        return None
    planet_code = code // 100
    return {5: "jupiter", 6: "saturn", 7: "uranus",
            8: "neptune", 9: "pluto"}.get(planet_code)


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


_loaded_systems = set()


_SYSTEM_SIZES = {
    "jupiter": "~1.1 GB",
    "saturn": "~630 MB",
    "neptune": "~100 MB",
    "pluto": "~129 MB",
}


def load_system(system: str):
    """Download and load satellite ephemeris for a planetary system.

    Downloads the SPK file on first call, then loads it into SPICE.
    Subsequent calls for the same system are no-ops.

    These files are large (100 MB to 1+ GB) because they contain
    decades of ephemeris at high precision. They are cached locally
    after first download.

    Parameters
    ----------
    system : str
        One of: "jupiter", "saturn", "neptune", "pluto".

    Raises
    ------
    ValueError
        If the system is not available.
    """
    system = system.lower()
    if system in _loaded_systems:
        return
    if system not in SATELLITE_KERNELS:
        available = ", ".join(sorted(SATELLITE_KERNELS.keys()))
        raise ValueError(
            f"No satellite SPK available for '{system}'. "
            f"Available systems: {available}"
        )
    kernel_name = SATELLITE_KERNELS[system]
    kernel_path = GENERIC_STORAGE / kernel_name
    if not kernel_path.exists():
        kernel_path.parent.mkdir(exist_ok=True, parents=True)
        url = GENERIC_URL / kernel_name
        size = _SYSTEM_SIZES.get(system, "large")
        print(f"Downloading {system.title()} satellite ephemeris ({size})...")
        print(f"  {kernel_name}")
        print(f"  This is a one-time download, cached at {kernel_path.parent}")
        url_retrieve(url, kernel_path)
    spice.furnsh(str(kernel_path))
    _loaded_systems.add(system)


def ensure_system_for_body(body_name: str):
    """Load satellite ephemeris if needed for a given body.

    Called automatically by Spicer when a body needs ephemeris data
    that isn't covered by the generic kernels.

    Parameters
    ----------
    body_name : str
        NAIF body name.
    """
    system = _body_to_system(body_name)
    if system is not None and system in SATELLITE_KERNELS:
        load_system(system)


def show_loaded_kernels():
    """Show overview of all currently loaded SPICE kernels.

    Returns
    -------
    list
        List of loaded kernel paths relative to KERNEL_STORAGE
    """
    count = spice.ktotal("all")
    loaded_kernels = []
    if count == 0:
        print("No kernels loaded at this time.")
    else:
        print("The loaded files are:\n(paths relative to kernels.KERNEL_STORAGE)\n")
    for which in range(count):
        out = spice.kdata(which, "all", 100, 100, 100)
        print("Position:", which)
        p = Path(out[0])
        rel_path = p.relative_to(KERNEL_STORAGE)
        print("Path", rel_path)
        print("Type:", out[1])
        print("Source:", out[2])
        print("Handle:", out[3])
        loaded_kernels.append(str(rel_path))
    return loaded_kernels
