"""One-stop constants for planetary work.

Three layers live under this module, mergeable in any combination:

1. **Fundamental physics constants** (``G``, ``c``, ``h``, ``k_B``,
   ``N_A``, ``sigma_sb``, ``m_e``, ``m_p``, ``M_sun``, ``R_earth``, ...) —
   re-exported from :mod:`astropy.constants` so you don't have to keep
   two imports separate. These are CODATA-versioned and authoritative;
   nothing here repackages or substitutes them.

2. **Per-body PCK constants** (triaxial radii, GMs, poles, prime
   meridians, rotation rates) for ~145 solar-system bodies, sourced from
   NAIF SPICE PCK kernels and versioned by IAU report edition. The
   default edition is IAU 2015; earlier editions are reachable via
   :mod:`planetarypy.constants.iau2009` for reproducibility studies.

   GMs themselves are *not* IAU-edition-specific — they come from JPL's
   ``gm_de440.tpc`` ephemeris kernel, layered on top of each IAU PCK at
   build time so the latest GM solution wins regardless of which IAU
   edition you pick. Edition drift therefore manifests in cartographic
   and orientation fields (radii, ``pole_ra``, ``pole_dec``, ``pm``,
   rotation rate), not in GM.

3. **NSSDC "textbook parameters"** (bond albedo, surface pressure,
   satellite count, orbital elements, ...) for the Sun, eight planets,
   the Moon, and Pluto — merged transparently into each Body.

   NSSDC is NASA's **National Space Science Data Center** at Goddard
   Space Flight Center, maintainer of the canonical per-body planetary
   fact sheets (D. R. Williams, since 1996). NSSDC's per-field
   publication history is exposed via
   :mod:`planetarypy.constants.nssdc` for time-travel work.

Examples:

    from planetarypy.constants import G, c, M_sun           # astropy.constants
    from planetarypy.constants import Mars                  # PCK + NSSDC merged
    from planetarypy.constants import planets, moons, find_body

    Mars.GM                        # PCK (IAU 2015)
    Mars.bond_albedo               # NSSDC (latest capture)
    Mars.at_time('2001').bond_albedo  # NSSDC as of 2001

    Mars.at_time('2012').pole_dec        # what a 2012 paper would see (IAU 2009)
    Mars.at_time('2024').pole_dec        # current default (IAU 2015) — ~1.5° apart

See :doc:`/explanation/constants_design` for the design rationale and
:doc:`/tutorials/constants_tutorial` for a walkthrough.
"""

from typing import Optional

# Re-export astropy's fundamental physics constants so callers don't have
# to manage two imports. Excluded explicitly:
#   - astropy.constants.u (atomic mass unit Quantity) — collides with the
#     conventional ``import astropy.units as u`` alias most users have.
#     Reach it as ``astropy.constants.u`` when needed.
#   - astropy.constants.iau2015 (internal data submodule) — would shadow
#     our ``planetarypy.constants.iau2015`` body-constants submodule.
from astropy.constants import (
    G, c, h, hbar, k_B, N_A, R, sigma_sb, sigma_T, b_wien,
    e, eps0, mu0, muB, Ryd, alpha, a0, atm, g0,
    m_e, m_p, m_n,
    M_sun, R_sun, L_sun, GM_sun, L_bol0,
    M_earth, R_earth, GM_earth,
    M_jup, R_jup, GM_jup,
    au, pc, kpc,
)

from planetarypy.constants.base import (
    Body,
    BodyRegistry,
    Constant,
    century,
)
from planetarypy.constants import iau2009, iau2015, nssdc

# The current default edition. Importing ``planetarypy.constants.X``
# resolves to ``planetarypy.constants.iau2015.X`` (Mars, Saturn, ...).
# Earlier editions are still reachable explicitly via their submodule.
from planetarypy.constants.iau2015 import *  # noqa: F401, F403  (body aliases)
from planetarypy.constants.iau2015 import bodies  # explicit so type checkers see it

DEFAULT_IAU_YEAR = 2015
AVAILABLE_IAU_YEARS = (2009, 2015)


# ── Discovery helpers ──────────────────────────────────────────────────


def sun() -> list[Body]:
    """The Sun (NAIF 10) as a single-element list, for symmetry with
    :func:`planets` / :func:`moons` / etc."""
    return [bodies[10]] if 10 in bodies else []


def planets() -> list[Body]:
    """All 9 planets (Mercury through Pluto). Pluto is also in
    :func:`dwarf_planets` — overlap by design, matching IAU's dual
    classification."""
    return bodies.by_class("planet")


def moons(of=None) -> list[Body]:
    """All moons in the registry, or the moons of one parent.

    ``of`` may be a NAIF id (e.g. ``699``), a body name (``"Saturn"``,
    case-insensitive), or a :class:`Body` instance. With no argument,
    returns a flat list across all planets.
    """
    return bodies.by_class("moon", parent=of)


def asteroids() -> list[Body]:
    """All asteroids (NAIF id range 2_000_000 ≤ id < 1_000_000_000),
    including alternate / mission-target IDs like Didymos and
    Dimorphos."""
    return bodies.by_class("asteroid")


def comets() -> list[Body]:
    """All comets (NAIF 1_NNNNNN range)."""
    return bodies.by_class("comet")


def dwarf_planets() -> list[Body]:
    """Bodies flagged as IAU-classified dwarf planets (Pluto, Ceres, ...).

    Pluto appears in *both* :func:`planets` and :func:`dwarf_planets` —
    matches IAU's dual classification.
    """
    return [b for b in bodies.values()
            if isinstance(b, Body) and b.dwarf_planet]


def mission_visited() -> list[Body]:
    """Small bodies that have been flown by, orbited, or landed on
    (Bennu, Ryugu, Eros, Ceres, Vesta, 67P, Tempel 1, ...)."""
    return [b for b in bodies.values()
            if isinstance(b, Body) and b.mission_visited]


def find_body(query) -> Optional[Body]:
    """Best-effort lookup by NAIF id or case-insensitive name. ``None``
    on miss (unlike ``bodies[query]`` which raises ``KeyError``).
    """
    return bodies.find(query)


def at_time(body, field: str, date) -> Optional[Constant]:
    """Date-aware single-field lookup across PCK + NSSDC sources.

    Function form of ``Body.at_time(date).<field>``. Returns the
    :class:`Constant` for ``body.field`` current as of ``date``.

    ``body`` may be a name (``"mars"``, case-insensitive), a NAIF id
    (``499``), or a :class:`Body` instance. ``date`` accepts ``"2001"``
    / ``"2001-06"`` / ``"2001-06-15"``.

    Returns ``None`` if the body isn't in the registry, the field isn't
    known for that body, or no source has data for that date.
    """
    b = body if isinstance(body, Body) else find_body(body)
    if b is None:
        return None
    snapshot = b.at_time(date)
    return getattr(snapshot, field, None)


_ASTROPY_CONSTANTS = [
    "G", "c", "h", "hbar", "k_B", "N_A", "R", "sigma_sb", "sigma_T",
    "b_wien", "e", "eps0", "mu0", "muB", "Ryd", "alpha", "a0", "atm", "g0",
    "m_e", "m_p", "m_n",
    "M_sun", "R_sun", "L_sun", "GM_sun", "L_bol0",
    "M_earth", "R_earth", "GM_earth",
    "M_jup", "R_jup", "GM_jup",
    "au", "pc", "kpc",
]

__all__ = [
    "Body",
    "BodyRegistry",
    "Constant",
    "century",
    "iau2009",
    "iau2015",
    "nssdc",
    "bodies",
    "DEFAULT_IAU_YEAR",
    "AVAILABLE_IAU_YEARS",
    "sun",
    "planets",
    "moons",
    "asteroids",
    "comets",
    "dwarf_planets",
    "mission_visited",
    "find_body",
    "at_time",
    *_ASTROPY_CONSTANTS,
]
