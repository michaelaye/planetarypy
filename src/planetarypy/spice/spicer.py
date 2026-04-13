"""Spicer: surface illumination calculations via SPICE.

Provides a clean OO interface for computing illumination geometry,
solar flux, and related quantities at surface points on solar system bodies.

Requires spiceypy and the NAIF generic kernels (LSK + PCK). The set of
supported bodies is determined by the loaded PCK kernel — the default
``pck00011.tpc`` includes the Sun, all 8 planets, Pluto, 70+ moons,
and a few asteroids (Ceres, Vesta, Lutetia). Additional bodies become
available if you load further kernels via spiceypy.

Examples
--------
>>> from planetarypy.spice.spicer import Spicer
>>> mars = Spicer("MARS")
>>> illum = mars.illumination(lon=137.4, lat=-4.6, time="2024-01-15T12:00:00")
>>> illum.solar_incidence
45.2
>>> illum.solar_flux
450.3

Use ``units=True`` to get astropy Quantities instead of plain floats:

>>> mars = Spicer("MARS", units=True)
>>> illum = mars.illumination(lon=137.4, lat=-4.6, time="2024-01-15T12:00:00")
>>> illum.solar_incidence
<Quantity 45.2 deg>
"""

from __future__ import annotations

import datetime as dt
from collections import namedtuple
from dataclasses import dataclass, field
from math import tau

import dateutil.parser as tparser
import numpy as np
import spiceypy as spice

from .generic_kernels import load_generic_kernels

# Ensure generic kernels are available
load_generic_kernels()

Radii = namedtuple("Radii", "a b c")


def _rotate_vector(vector, axis, angle_rad):
    """Rotate a vector around an axis (right-hand rule, CCW looking along axis).

    Matches the SPICE (axisar) and scipy convention.

    Parameters
    ----------
    vector : array-like
        3D vector to rotate.
    axis : array-like
        Axis of rotation (will be normalized).
    angle_rad : float
        Rotation angle in radians.

    Returns
    -------
    np.ndarray
        Rotated 3D vector.
    """
    from scipy.spatial.transform import Rotation

    axis = np.array(axis, dtype=np.float64)
    axis /= np.linalg.norm(axis)
    r = Rotation.from_rotvec(angle_rad * axis)
    return r.apply(vector)


def _parse_time(time) -> dt.datetime:
    """Parse a time argument into a datetime."""
    if time is None:
        return dt.datetime.now()
    if isinstance(time, dt.datetime):
        return time
    return tparser.parse(time)


def _to_et(time) -> float:
    """Convert a time argument to SPICE ephemeris time."""
    t = _parse_time(time)
    return spice.utc2et(t.isoformat())


def _maybe_quantity(value, unit_str, use_units: bool):
    """Optionally wrap a value in an astropy Quantity."""
    if not use_units:
        return value
    from astropy import units as u
    return value * u.Unit(unit_str)


# ── Result dataclasses ───────────────────────────────────────────────


@dataclass(frozen=True)
class IlluminationResult:
    """Result of an illumination calculation.

    All angles in degrees, flux in W/m². When created with
    ``units=True``, values are astropy Quantities.
    """

    solar_incidence: float
    """Solar incidence angle [deg]."""

    emission: float
    """Emission angle [deg] (0 if no observer specified)."""

    phase: float
    """Phase angle [deg] (0 if no observer specified)."""

    solar_flux: float
    """Solar flux at the surface [W/m²] (0 if incidence > 90°)."""

    subsolar_lon: float
    """Sub-solar longitude [deg]."""

    subsolar_lat: float
    """Sub-solar latitude [deg]."""

    l_s: float
    """Solar longitude (season) [deg]."""

    local_solar_time: str
    """Local solar time string."""

    def __repr__(self):
        return (
            f"IlluminationResult(\n"
            f"  solar_incidence={self.solar_incidence},\n"
            f"  emission={self.emission},\n"
            f"  phase={self.phase},\n"
            f"  solar_flux={self.solar_flux},\n"
            f"  l_s={self.l_s},\n"
            f"  local_solar_time='{self.local_solar_time}'\n"
            f")"
        )


# ── Spicer class ─────────────────────────────────────────────────────


class Spicer:
    """Surface illumination calculator for any solar system body.

    Parameters
    ----------
    body : str
        NAIF body name (e.g. "MARS", "MOON", "ENCELADUS").
    units : bool
        If True, return astropy Quantities with units.
        If False (default), return plain floats.

    Examples
    --------
    >>> mars = Spicer("MARS")
    >>> mars.radii
    Radii(a=3396.19, b=3396.19, c=3376.2)
    >>> mars.ref_frame
    'IAU_MARS'
    """

    L_SUN = 3.828e26  # Solar luminosity [W]

    def __init__(self, body: str, units: bool = False):
        self._body = body.upper()
        self._units = units

    @property
    def body(self) -> str:
        return self._body

    @property
    def ref_frame(self) -> str:
        return f"IAU_{self._body}"

    @property
    def target_id(self) -> int:
        return spice.bodn2c(self._body)

    @property
    def radii(self) -> Radii:
        """Body radii [km]."""
        _, r = spice.bodvrd(self._body, "RADII", 3)
        return Radii(*r)

    def _ensure_ephemeris(self):
        """Try to load satellite ephemeris for this body.

        On first failure, attempts to download and load the appropriate
        planetary system SPK. These can be large (100 MB to 1+ GB).
        """
        from .generic_kernels import ensure_system_for_body, _body_to_system, SATELLITE_KERNELS, _loaded_systems
        system = _body_to_system(self._body)
        if system is not None and system in SATELLITE_KERNELS:
            if system not in _loaded_systems:
                from .generic_kernels import _SYSTEM_SIZES, GENERIC_STORAGE
                kernel_path = GENERIC_STORAGE / SATELLITE_KERNELS[system]
                if not kernel_path.exists():
                    size = _SYSTEM_SIZES.get(system, "large")
                    print(f"Body '{self._body}' needs {system.title()} satellite ephemeris ({size}).")
            ensure_system_for_body(self._body)
        else:
            raise RuntimeError(
                f"No ephemeris data available for '{self._body}'. "
                f"Load additional SPICE kernels via spiceypy.furnsh()."
            )

    def solar_longitude(self, time=None) -> float:
        """Solar longitude L_s [deg] at the given time (default: now)."""
        et = _to_et(time)
        try:
            ls = np.rad2deg(spice.lspcn(self._body, et, "NONE"))
        except Exception:
            self._ensure_ephemeris()
            ls = np.rad2deg(spice.lspcn(self._body, et, "NONE"))
        return _maybe_quantity(ls, "deg", self._units)

    @property
    def Ls(self) -> float:
        """Current solar longitude L_s [deg]."""
        return self.solar_longitude()

    def subsolar_point(self, time=None) -> tuple[float, float]:
        """Sub-solar point (lon, lat) in degrees at the given time."""
        et = _to_et(time)
        try:
            spoint, _, _ = spice.subslr(
                "Intercept/ellipsoid", self._body, et,
                self.ref_frame, "NONE", self._body,
            )
        except Exception:
            self._ensure_ephemeris()
            spoint, _, _ = spice.subslr(
                "Intercept/ellipsoid", self._body, et,
                self.ref_frame, "NONE", self._body,
            )
        coords = spice.reclat(spoint)
        lon = np.degrees(coords[1])
        lat = np.degrees(coords[2])
        if lon < 0:
            lon += 360.0
        return (
            _maybe_quantity(lon, "deg", self._units),
            _maybe_quantity(lat, "deg", self._units),
        )

    def center_to_sun(self, time=None):
        """Vector from body center to Sun [km] at the given time."""
        et = _to_et(time)
        try:
            pos, _ = spice.spkpos("SUN", et, self.ref_frame, "NONE", self._body)
        except Exception:
            self._ensure_ephemeris()
            pos, _ = spice.spkpos("SUN", et, self.ref_frame, "NONE", self._body)
        return pos

    def solar_constant(self, time=None) -> float:
        """Solar constant at body center distance [W/m²]."""
        cts = self.center_to_sun(time)
        dist_m = spice.vnorm(cts) * 1000.0  # km → m
        flux = self.L_SUN / (2 * tau * dist_m**2)
        return _maybe_quantity(flux, "W/m2", self._units)

    def illumination(self, lon: float, lat: float, time=None,
                     observer: str | None = None,
                     tau_atm: float = 0.0,
                     slope: float = 0.0,
                     aspect: float = 0.0) -> IlluminationResult:
        """Compute illumination at a surface point.

        Parameters
        ----------
        lon, lat : float
            Planetocentric longitude and latitude [deg].
        time : str or datetime, optional
            Observation time. Default: now.
        observer : str, optional
            NAIF observer name (e.g. "MRO") for emission/phase angles.
            If None, only solar incidence is computed.
        tau_atm : float
            Atmospheric optical depth for flux attenuation (default 0).
        slope : float
            Surface slope in degrees (0 = flat). The surface normal is
            tilted toward north by this amount.
        aspect : float
            Aspect angle in degrees, clockwise from north. Rotates the
            tilted normal around the vertical. Only used when slope > 0.

        Returns
        -------
        IlluminationResult
        """
        et = _to_et(time)

        # Surface point in rectangular body-fixed coords
        spoint = spice.srfrec(self.target_id, np.radians(lon), np.radians(lat))

        # Surface normal
        snormal = spice.surfnm(*self.radii, spoint)

        # Sun vector
        sun_pos, _ = spice.spkpos("SUN", et, self.ref_frame, "NONE", self._body)
        sun_dir = spice.vsub(sun_pos, spoint)

        if observer is not None:
            # Full illumination angles via ilumin
            _, _, phase_r, solar_r, emission_r = spice.ilumin(
                "Ellipsoid", self._body, et, self.ref_frame,
                "NONE", observer, spoint,
            )
            phase = np.degrees(phase_r)
            solar_inc = np.degrees(solar_r)
            emission = np.degrees(emission_r)
        else:
            # Simple: angle between sun direction and surface normal
            solar_inc = np.degrees(spice.vsep(sun_dir, snormal))
            phase = float("nan")
            emission = float("nan")

        # Compute effective normal for flux (may be tilted)
        if slope > 0:
            # Tilt the normal toward north
            to_north = spice.vsub(
                (0.0, 0.0, self.radii.c),  # north pole
                spoint,
            )
            tilt_axis = spice.vcrss(spoint, to_north)
            flux_normal = _rotate_vector(snormal, tilt_axis, np.radians(slope))

            # Rotate around the original normal by aspect angle
            if aspect != 0:
                flux_normal = _rotate_vector(flux_normal, snormal, np.radians(aspect))
        else:
            flux_normal = snormal

        # Solar flux using the (possibly tilted) surface normal
        dist_m = spice.vnorm(sun_pos) * 1000.0
        sc = self.L_SUN / (2 * tau * dist_m**2)
        flux_angle = spice.vsep(sun_dir, flux_normal)
        if solar_inc > 90.0 or np.degrees(flux_angle) > 90.0:
            flux = 0.0
        else:
            flux = sc * np.cos(flux_angle)
            if tau_atm > 0:
                flux *= np.exp(-tau_atm / np.cos(np.radians(solar_inc)))

        # Sub-solar point
        ss_lon, ss_lat = self.subsolar_point(time)

        # Solar longitude
        l_s = np.rad2deg(spice.lspcn(self._body, et, "NONE"))

        # Local solar time
        _, _, _, lst_str, _ = spice.et2lst(
            et, self.target_id, np.radians(lon), "PLANETOCENTRIC",
        )

        q = self._units
        return IlluminationResult(
            solar_incidence=_maybe_quantity(solar_inc, "deg", q),
            emission=_maybe_quantity(emission, "deg", q),
            phase=_maybe_quantity(phase, "deg", q),
            solar_flux=_maybe_quantity(flux, "W/m2", q),
            subsolar_lon=_maybe_quantity(
                ss_lon.value if q else ss_lon, "deg", False),
            subsolar_lat=_maybe_quantity(
                ss_lat.value if q else ss_lat, "deg", False),
            l_s=_maybe_quantity(l_s, "deg", q),
            local_solar_time=lst_str,
        )

    def illumination_at(self, point, time=None, observer: str | None = None,
                        tau_atm: float = 0.0) -> IlluminationResult:
        """Compute illumination at a planetarypy Point.

        Parameters
        ----------
        point : planetarypy.geo.Point
            Must have lon/lat.
        time, observer, tau_atm
            See ``illumination()``.
        """
        if point.lon is None or point.lat is None:
            raise ValueError("Point must have lon/lat coordinates")
        return self.illumination(point.lon, point.lat, time=time,
                                 observer=observer, tau_atm=tau_atm)

    def sun_direction_at(self, lon: float, lat: float, time=None,
                         pixel_res: float = 0.5) -> tuple[float, float]:
        """Compute a surface point toward the sun for azimuth calculation.

        Returns lon/lat of a point within one pixel of the origin but
        in the direction of the subsolar point. Use with
        ``Point.azimuth_to()`` to get solar azimuth.

        Parameters
        ----------
        lon, lat : float
            Origin surface point [deg].
        time : str or datetime, optional
        pixel_res : float
            Pixel size in km (default 0.5).

        Returns
        -------
        sun_lon, sun_lat : float
            Lon/lat [deg] of point toward sun.
        """
        et = _to_et(time)
        spoint = spice.srfrec(self.target_id, np.radians(lon), np.radians(lat))

        # Subsolar point
        subsolar, _, _ = spice.subslr(
            "Intercept/ellipsoid", self._body, et,
            self.ref_frame, "NONE", self._body,
        )

        # Vector from spoint toward subsolar, projected onto tangent plane
        poB = spice.vsub(subsolar, spoint)
        hpoB = spice.vperp(poB, spoint)
        upoB = spice.vhat(hpoB)

        # Scale to within a pixel
        scale = (pixel_res / 1000.0) / 2.0
        spoB = spice.vscl(scale, upoB)
        nB = spice.vadd(spoint, spoB)

        coords = spice.reclat(nB)
        sun_lon = np.degrees(coords[1])
        sun_lat = np.degrees(coords[2])
        if sun_lon < 0:
            sun_lon += 360.0

        return (
            _maybe_quantity(sun_lon, "deg", self._units),
            _maybe_quantity(sun_lat, "deg", self._units),
        )

    def _project_to_tangent_plane(self, spoint, target_point):
        """Project a target point onto the tangent plane at spoint.

        Returns a unit vector in the tangent plane pointing from spoint
        toward the projection of target_point.
        """
        diff = spice.vsub(target_point, spoint)
        tangent = spice.vperp(diff, spoint)
        return spice.vhat(tangent)

    def _tangent_north(self, spoint):
        """Unit vector in the tangent plane at spoint pointing toward north."""
        north_pole = (0.0, 0.0, self.radii.c)
        return self._project_to_tangent_plane(spoint, north_pole)

    def solar_azimuth_at(self, lon: float, lat: float, time=None) -> float:
        """Compute solar azimuth at a surface point via SPICE.

        Returns the azimuth of the sun measured clockwise from north
        in the local tangent plane. This is the standard geographic
        convention (N=0°, E=90°, S=180°, W=270°).

        Independent of any PDS index — computed directly from SPICE
        ephemeris.

        Parameters
        ----------
        lon, lat : float
            Planetocentric longitude and latitude [deg].
        time : str or datetime, optional

        Returns
        -------
        float
            Solar azimuth [deg], clockwise from north.
        """
        et = _to_et(time)
        spoint = spice.srfrec(self.target_id, np.radians(lon), np.radians(lat))

        # Subsolar point
        subsolar, _, _ = spice.subslr(
            "Intercept/ellipsoid", self._body, et,
            self.ref_frame, "NONE", self._body,
        )

        # Project sun and north directions onto tangent plane
        sun_tangent = self._project_to_tangent_plane(spoint, subsolar)
        north_tangent = self._tangent_north(spoint)

        # East = north × surface_normal (right-hand rule)
        snormal = spice.surfnm(*self.radii, spoint)
        east_tangent = spice.vcrss(north_tangent, snormal)
        # Normalize (should already be unit but be safe)
        east_tangent = spice.vhat(east_tangent)

        # Azimuth: angle from north, positive toward east (CW from above)
        north_comp = spice.vdot(sun_tangent, north_tangent)
        east_comp = spice.vdot(sun_tangent, east_tangent)
        az = np.degrees(np.arctan2(east_comp, north_comp)) % 360

        return _maybe_quantity(az, "deg", self._units)

    def north_azimuth_at(self, lon: float, lat: float, time=None) -> float:
        """Compute north azimuth in a spacecraft image via SPICE.

        This would require the instrument boresight and orientation —
        not available with generic kernels alone. For now, raises
        NotImplementedError. Use the PDS index NORTH_AZIMUTH instead.

        (Included as a placeholder for future mission kernel support.)
        """
        raise NotImplementedError(
            "North azimuth in image space requires mission-specific SPICE "
            "kernels (CK, IK). Use the PDS index NORTH_AZIMUTH for now."
        )

    @staticmethod
    def supported_bodies() -> list[tuple[int, str, float]]:
        """List all bodies available for Spicer calculations.

        Returns bodies that have radii defined in the currently loaded
        SPICE kernels (typically the generic PCK kernel ``pck00011.tpc``).
        Load additional kernels via spiceypy to extend this list.

        Returns
        -------
        list of (naif_id, name, equatorial_radius_km)
        """
        bodies = []
        for code in list(range(0, 1000)) + list(range(2000000, 2000100)):
            try:
                name = spice.bodc2n(code)
                _, radii = spice.bodvrd(name, "RADII", 3)
                if radii[0] > 0:
                    bodies.append((code, name, float(radii[0])))
            except Exception:
                pass
        return sorted(bodies, key=lambda x: x[0])

    def __repr__(self):
        return f"Spicer('{self._body}', units={self._units})"
