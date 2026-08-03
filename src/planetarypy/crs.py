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
>>> body_crs("mars")                   # Mars IAU sphere (a == b)
>>> body_crs(499, system="ographic")   # by NAIF id
>>> local_crs(137.4, -4.6, "mars")     # azeqd centered on Gale crater
"""

import contextlib
import contextvars
import warnings

from pyproj import CRS
from pyproj.exceptions import CRSError

__all__ = [
    "body_crs",
    "local_crs",
    "get_crs",
    "projected_crs",
    "set_target_crs",
    "get_target_crs",
    "clear_target_crs",
    "target_crs",
    "resolve_crs",
    "announce_conversion",
    "CRSConversionWarning",
]


class CRSConversionWarning(UserWarning):
    """Emitted when data is reprojected without the caller asking explicitly.

    Its own category so it can be silenced deliberately —
    ``warnings.filterwarnings("ignore", category=CRSConversionWarning)`` —
    without muting everything else.
    """


# A ContextVar rather than a plain module global: it makes the `target_crs`
# context manager correct by construction (reset by token, so nesting and early
# exceptions both unwind properly) and keeps threads and asyncio tasks from
# stamping on each other's session setting.
_TARGET_CRS: contextvars.ContextVar = contextvars.ContextVar(
    "planetarypy_target_crs", default=None
)


def _as_crs(crs_like) -> CRS:
    """Accept anything pyproj accepts — CRS, EPSG/IAU string, dict, WKT."""
    return crs_like if isinstance(crs_like, CRS) else CRS.from_user_input(crs_like)


def set_target_crs(crs_like):
    """Set a session-wide CRS that later operations convert their output to.

    Once set, helpers returning georeferenced data reproject to it instead of
    handing back whatever their upstream source happened to use — so a session
    mixing a USGS gazetteer shapefile (ESRI authority), a HiRISE GeoTIFF
    (IAU_2015) and PSA footprints stays in one frame without restating it at
    every call.

    Returns the previous value, so it can be restored.

    Parameters
    ----------
    crs_like : CRS or str or dict or None
        Anything :meth:`pyproj.CRS.from_user_input` accepts. ``None`` clears it.
    """
    previous = _TARGET_CRS.get()
    _TARGET_CRS.set(None if crs_like is None else _as_crs(crs_like))
    return previous


def get_target_crs():
    """The session-wide target CRS, or ``None`` if unset."""
    return _TARGET_CRS.get()


def clear_target_crs() -> None:
    """Forget the session-wide target CRS."""
    _TARGET_CRS.set(None)


@contextlib.contextmanager
def target_crs(crs_like):
    """Context manager form of :func:`set_target_crs`.

    >>> with target_crs("IAU_2015:49900"):
    ...     gdf = nomenclature.features("mars")   # arrives in 49900
    >>> # previous setting restored here, even if the block raised

    Nests correctly and restores on exception: the ContextVar token is reset in
    a ``finally``.
    """
    token = _TARGET_CRS.set(None if crs_like is None else _as_crs(crs_like))
    try:
        yield get_target_crs()
    finally:
        _TARGET_CRS.reset(token)


def resolve_crs(explicit=None, *, fallback=None):
    """Pick the CRS an operation should produce, by precedence.

    ``explicit`` (what the caller passed) beats the session target, which beats
    ``fallback`` (usually the body's own IAU CRS). Returns ``None`` only when all
    three are ``None`` — callers should read that as "leave the data alone".

    Central so every consumer resolves precedence identically, and there is one
    site to change if the rules ever grow.
    """
    if explicit is not None:
        return _as_crs(explicit)
    session = _TARGET_CRS.get()
    if session is not None:
        return session
    return None if fallback is None else _as_crs(fallback)


def announce_conversion(source, target, *, what: str = "data") -> None:
    """Tell the user about a reprojection they did not explicitly request.

    Silent reprojection is how authority mismatches become quiet wrongness — the
    USGS gazetteer ships ``ESRI:104905`` while planetarypy standardises on
    ``IAU_2015``, and those differ in more than their label.

    Deliberately a ``warnings.warn`` rather than a log line: planetarypy disables
    its loguru logger by default for library use, so ``logger.info`` here would
    be invisible to exactly the people who need to see it.
    """
    if source is None or target is None:
        return
    src, tgt = _as_crs(source), _as_crs(target)
    if src == tgt:
        return

    def _label(c):
        auth = c.to_authority()
        return ":".join(auth) if auth else (c.name or "unknown CRS")

    warnings.warn(
        f"{what} reprojected from {_label(src)} to {_label(tgt)} automatically. "
        "Pass an explicit CRS, or set planetarypy.crs.set_target_crs(...), to "
        "choose deliberately.",
        CRSConversionWarning,
        stacklevel=3,
    )

# IAU code = naif_id * 100 + variant offset, and the geographic triple is the
# SAME as the projected one below: 0 is the sphere, 1 ographic, 2 ocentric.
#
# This used to read {"ocentric": 0, "ographic": 1}, which quietly handed back a
# sphere to anyone asking for "ocentric" — IAU_2015:49900 is named
# "Mars (2015) - Sphere / Ocentric" and has a == b == 3396190, while the real
# ocentric ellipsoid is 49902 (b = 3376200) and was unreachable through this API
# entirely. Doing geodesy on a sphere while believing you asked for an ellipsoid
# is exactly the class of error this module exists to prevent.
#
# Offset 0 exists for every body. Offsets 1 and 2 exist only where the body
# defines an ellipsoid (Mars, Mercury, Jupiter — not the Moon, Venus, Europa).
_SYSTEM_OFFSET = {"sphere": 0, "ographic": 1, "ocentric": 2}

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


def body_crs(body, system: str = "sphere") -> CRS:
    """Return a body's geographic CRS from the IAU 2015 authority.

    Parameters
    ----------
    body : str or int
        Body name (resolved via :mod:`planetarypy.constants`) or NAIF id.
    system : {"sphere", "ocentric", "ographic"}
        Which figure of the body to use.

        - ``"sphere"`` (default) — the IAU sphere, ``a == b``. Defined for every
          body, and the common currency in planetary work: ISIS operates on
          spheres, many published products are on one, and a shared sphere
          avoids datum-shift surprises when stacking heterogeneous datasets in
          GIS. This is what earlier versions returned for ``"ocentric"``.
        - ``"ocentric"`` — planetocentric latitudes on the body's *ellipsoid*.
        - ``"ographic"`` — planetographic latitudes on the body's ellipsoid.

        The two ellipsoidal systems exist only for bodies that define an
        ellipsoid (Mars, Mercury, Jupiter — not the Moon, Venus or Europa).

    Returns
    -------
    pyproj.CRS
        The figure and radii come from the IAU code itself; nothing is
        hardcoded here.

    Examples
    --------
    >>> body_crs("mars").to_authority()              # sphere, a == b
    ('IAU_2015', '49900')
    >>> body_crs("mars", system="ocentric").to_authority()
    ('IAU_2015', '49902')
    >>> body_crs("mars", system="ographic").to_authority()
    ('IAU_2015', '49901')
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
        extra = (
            " Only bodies with a defined ellipsoid have 'ocentric' and "
            "'ographic' variants; try system='sphere'."
            if system in ("ocentric", "ographic")
            else ""
        )
        raise ValueError(
            f"No {_IAU_AUTHORITY} {system!r} CRS for body {body!r} "
            f"(code {code}).{extra}"
        ) from None


def local_crs(lon: float, lat: float, body, *, system: str = "sphere") -> CRS:
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

    ``system="default"`` maps to ``"sphere"`` — the IAU sphere, which is what
    this alias has always returned. It is spelled ``"sphere"`` rather than
    ``"ocentric"`` since the offset tables were corrected: offset 0 is the
    sphere, and ``"ocentric"`` now means the ellipsoid at offset 2.

    Unlike craterpy's original, this does NOT accept an arbitrary CRS string as
    ``system`` (no exception-driven passthrough) — construct such CRS with
    pyproj directly.
    """
    if system == "default":
        system = "sphere"
    return body_crs(body, system)
