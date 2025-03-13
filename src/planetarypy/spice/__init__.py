"""SPICE integration for planetarypy.

This package provides tools for working with SPICE kernels and performing
SPICE-related calculations for planetary science.
"""

from .archived_kernels import (
    Subsetter,
    datasets,
    get_metakernel_and_files,
    is_start_valid,
    is_stop_valid,
    list_kernels_for_day,
)
from .config import (
    BASE_URL,
    KERNEL_STORAGE,
    NAIF_URL,
)
from .generic_kernels import (
    GENERIC_STORAGE,
    GENERIC_URL,
    download_generic_kernels,
    generic_kernel_names,
    generic_kernel_paths,
    load_generic_kernels,
    show_loaded_kernels,
)
from .spicer import (
    EarthSpicer,
    EnceladusSpicer,
    IllumAngles,
    Mars_Ls_now,
    MarsSpicer,
    MoonSpicer,
    PlutoSpicer,
    Radii,
    Spicer,
    SurfaceCoords,
    TritonSpicer,
    make_axis_rotation_matrix,
)

__all__ = [
    # From archived_kernels
    "datasets",
    "is_start_valid",
    "is_stop_valid",
    "Subsetter",
    "get_metakernel_and_files",
    "list_kernels_for_day",
    # From config
    "KERNEL_STORAGE",
    "NAIF_URL",
    "BASE_URL",
    # From generic_kernels
    "GENERIC_STORAGE",
    "GENERIC_URL",
    "generic_kernel_names",
    "generic_kernel_paths",
    "download_generic_kernels",
    "load_generic_kernels",
    "show_loaded_kernels",
    # From spicer
    "Radii",
    "make_axis_rotation_matrix",
    "IllumAngles",
    "SurfaceCoords",
    "Spicer",
    "MarsSpicer",
    "TritonSpicer",
    "EnceladusSpicer",
    "PlutoSpicer",
    "EarthSpicer",
    "MoonSpicer",
    "Mars_Ls_now",
]
