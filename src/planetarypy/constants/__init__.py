"""Per-body cartographic and physical constants from NAIF PCK files,
versioned by IAU report edition.

For fundamental physical constants (``G``, ``c``, ``h``, ``k_B``, ...) and
headline solar-system values (``M_sun``, ``R_sun``, ``M_earth``, ``R_earth``),
use :mod:`astropy.constants` instead — those are codata-versioned and
authoritative. This module fills in the per-body PCK detail astropy
doesn't carry: triaxial radii, GMs, poles, prime meridians, and
derived masses for ~140 bodies.

Time-travel for reproducibility studies works by importing from a
specific IAU-edition submodule:

    from planetarypy.constants import Mars                # current default
    from planetarypy.constants.iau2009 import Mars as Mars_2009
    delta_GM = Mars.GM - Mars_2009.GM

The current default is :mod:`planetarypy.constants.iau2015` (the latest
IAU report NAIF has packaged into a generic PCK). When NAIF ships a new
PCK, a new ``iauNNNN`` submodule is added without breaking existing
imports.

See :doc:`/explanation/constants_design` for the design rationale.
"""

from planetarypy.constants.base import (
    Body,
    BodyRegistry,
    Constant,
)

__all__ = [
    "Body",
    "BodyRegistry",
    "Constant",
]

# Discovery helpers and per-edition modules will be added as the
# subpackage fills out (regenerator script first, then iau2009.py /
# iau2015.py, then top-level re-exports of the default edition's bodies).
