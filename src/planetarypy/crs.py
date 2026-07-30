"""Planetary coordinate reference systems via IAU codes.

Thin, ``pyproj``-only helpers that resolve IAU planetary CRS and build
feature-centered local projections. The body's ellipsoid / radii are carried
by the IAU code itself — nothing is looked up or hardcoded here.

PROJ ships a single IAU CRS edition (``IAU_2015``); there is no
``IAU_2009``/``IAU_2006``/… authority, so everything here builds against
``IAU_2015``. (This is unrelated to :mod:`planetarypy.constants`, which *does*
carry multiple IAU PCK editions for body parameters.)

Body *names* are resolved to NAIF ids via :mod:`planetarypy.constants` (one
shared body registry); pass a NAIF id integer to skip that import. On a fresh
install, the first name resolution triggers the one-time NSSDC archive
download that ``planetarypy.constants`` performs.

Examples
--------
>>> from planetarypy.crs import body_crs, local_crs
>>> body_crs("mars")                   # Mars ocentric geographic CRS
>>> body_crs(499, system="ographic")   # by NAIF id
>>> local_crs(137.4, -4.6, "mars")     # azeqd centered on Gale crater
"""

from pyproj import CRS
from pyproj.exceptions import CRSError

__all__ = ["body_crs", "local_crs", "get_crs", "projected_crs"]

# IAU code = naif_id * 100 + variant offset. Offset 0 ("Sphere / Ocentric")
# exists for every body; 1 ("Ographic") only for some (e.g. Mars, Jupiter —
# not the Moon or Vesta).
_SYSTEM_OFFSET = {"ocentric": 0, "ographic": 1}

# Projected variants sit at offsets 10..90 in steps of 5, each followed by a
# 3-slot system triple. Note the triple differs from _SYSTEM_OFFSET above:
# for projected codes slot 0 is the *sphere*, and ocentric moves to slot 2.
_PROJECTION_OFFSET = {
    "equirectangular": 10,
    "equirectangular_180": 15,
    "sinusoidal": 20,
    "sinusoidal_180": 25,
    "north_polar": 30,
    "south_polar": 35,
    "mollweide": 40,
    "mollweide_180": 45,
    "robinson": 50,
    "robinson_180": 55,
    "transverse_mercator": 60,
    "orthographic": 65,
    "orthographic_180": 70,
    "lambert_conic_conformal": 75,
    "lambert_azimuthal_equal_area": 80,
    "albers_equal_area": 85,
    "mercator": 90,
}
_PROJECTED_SYSTEM_OFFSET = {"sphere": 0, "ographic": 1, "ocentric": 2}

# PROJ ships only the IAU 2015 CRS edition (no IAU_2009/2006/... authority),
# so this is the single authority we build against.
_IAU_AUTHORITY = "IAU_2015"


def _resolve_naif_id(body) -> int:
    """Resolve a body (NAIF id int, or name str) to its NAIF integer id."""
    if isinstance(body, int):
        return body
    # Name lookup via the shared body registry. On a fresh install this
    # triggers constants' one-time NSSDC archive download; pass a NAIF id
    # integer to avoid importing constants at all.
    from planetarypy.constants import bodies

    found = bodies.find(body)
    if found is None:
        raise ValueError(
            f"Unknown body {body!r}. Pass a name known to "
            "planetarypy.constants, or a NAIF id integer (e.g. 499 for Mars)."
        )
    return found.naif_id


def body_crs(body, system: str = "ocentric") -> CRS:
    """Return a body's geographic CRS from the IAU 2015 authority.

    Parameters
    ----------
    body : str or int
        Body name (resolved via :mod:`planetarypy.constants`) or NAIF id.
    system : {"ocentric", "ographic"}
        Latitude convention. ``"ocentric"`` (the spherical IAU definition) is
        available for every body; ``"ographic"`` only for bodies that define
        it (e.g. Mars, Jupiter — not the Moon or Vesta).

    Returns
    -------
    pyproj.CRS
        The ellipsoid/radii come from the IAU code itself.
    """
    try:
        offset = _SYSTEM_OFFSET[system]
    except KeyError:
        raise ValueError(
            f"system must be one of {sorted(_SYSTEM_OFFSET)}, got {system!r}."
        ) from None
    naif_id = _resolve_naif_id(body)
    code = naif_id * 100 + offset
    try:
        return CRS.from_authority(_IAU_AUTHORITY, code)
    except CRSError:
        raise ValueError(
            f"No {_IAU_AUTHORITY} {system!r} CRS for body {body!r} "
            f"(code {code}). The body may only define an 'ocentric' system, "
            f"or be absent from {_IAU_AUTHORITY}."
        ) from None


def local_crs(lon: float, lat: float, body, *, system: str = "ocentric") -> CRS:
    """Azimuthal-Equidistant CRS centered on ``(lon, lat)`` for ``body``.

    Built on the body's IAU geodetic CRS, so its sphere/ellipsoid comes from
    the IAU code (nothing looked up). Use for feature-centered work — local
    buffering, annulus geometry, distance-true measurements near the center.

    Parameters
    ----------
    lon, lat : float
        Center longitude/latitude in degrees.
    body : str or int
        Body name or NAIF id.
    system :
        See :func:`body_crs`.

    Returns
    -------
    pyproj.CRS
        A projected CRS (metres) centered on the given point.
    """
    from pyproj.crs import ProjectedCRS
    from pyproj.crs.coordinate_operation import AzimuthalEquidistantConversion

    geodetic = body_crs(body, system).geodetic_crs
    return ProjectedCRS(
        name=f"AzimuthalEquidistant({lat:.4f}, {lon:.4f}) on {body}",
        conversion=AzimuthalEquidistantConversion(
            latitude_natural_origin=lat,
            longitude_natural_origin=lon,
        ),
        geodetic_crs=geodetic,
    )


def projected_crs(body, projection: str, system: str = "sphere") -> CRS:
    """Return one of a body's standard projected CRS from the IAU 2015 authority.

    Parameters
    ----------
    body : str or int
        Body name (resolved via :mod:`planetarypy.constants`) or NAIF id.
    projection : str
        Projection key, e.g. ``"north_polar"``, ``"equirectangular"``,
        ``"mercator"``. The ``*_180`` variants are the clon=180 forms.
    system : {"sphere", "ographic", "ocentric"}
        Figure of the body. ``"sphere"`` exists for every projection; the other
        two are the biaxial-ellipsoid forms and only exist for bodies that
        define an ographic system.

    Returns
    -------
    pyproj.CRS

    Examples
    --------
    >>> projected_crs(499, "north_polar").to_authority()
    ('IAU_2015', '49930')
    """
    try:
        proj_offset = _PROJECTION_OFFSET[projection]
    except KeyError:
        raise ValueError(
            f"projection must be one of {sorted(_PROJECTION_OFFSET)}, "
            f"got {projection!r}."
        ) from None
    try:
        sys_offset = _PROJECTED_SYSTEM_OFFSET[system]
    except KeyError:
        raise ValueError(
            f"system must be one of {sorted(_PROJECTED_SYSTEM_OFFSET)}, "
            f"got {system!r}."
        ) from None
    naif_id = _resolve_naif_id(body)
    code = naif_id * 100 + proj_offset + sys_offset
    try:
        return CRS.from_authority(_IAU_AUTHORITY, code)
    except CRSError:
        raise ValueError(
            f"No {_IAU_AUTHORITY} {projection!r}/{system!r} CRS for body "
            f"{body!r} (code {code})."
        ) from None


def get_crs(body, system: str = "default") -> CRS:
    """craterpy-compatible alias for :func:`body_crs`.

    ``system="default"`` maps to ``"ocentric"``. Unlike craterpy's original,
    this does NOT accept an arbitrary CRS string as ``system`` (no
    exception-driven passthrough) — construct such CRS with pyproj directly.
    """
    if system == "default":
        system = "ocentric"
    return body_crs(body, system)
