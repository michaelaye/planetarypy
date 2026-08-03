"""The CRS a caller asks for must have the *figure* they asked for.

These tests exist because `body_crs(body, system="ocentric")` used to return
IAU_2015:49900 — "Mars (2015) - Sphere / Ocentric", a == b == 3396190 — while
the actual ocentric ellipsoid, 49902 with b = 3376200, could not be reached
through the API at all. Every test then in place passed, because they all
asserted on authority *codes*. A code is only a label; the thing that makes a
result right or wrong is the ellipsoid behind it, so that is what these assert.
"""

import pytest

from planetarypy import crs

# Bodies that define an ellipsoid, so all three systems exist.
TRIAXIAL = [("mars", 499), ("mercury", 199)]

# Bodies IAU models as a sphere only — offsets 1 and 2 do not exist.
SPHERE_ONLY = [("moon", 301), ("venus", 299)]


def _figure(c):
    """('sphere' | 'ellipsoid', a, b) for a CRS."""
    e = c.ellipsoid
    a = e.semi_major_metre
    b = e.semi_minor_metre or a
    return ("sphere" if abs(a - b) < 1e-6 else "ellipsoid"), a, b


# --- the actual regression -------------------------------------------------


@pytest.mark.parametrize("body,_naif", TRIAXIAL)
def test_sphere_really_is_a_sphere(body, _naif):
    kind, a, b = _figure(crs.body_crs(body, system="sphere"))
    assert kind == "sphere", f"{body} 'sphere' returned a={a} b={b}"


@pytest.mark.parametrize("body,_naif", TRIAXIAL)
def test_ocentric_really_is_an_ellipsoid(body, _naif):
    """The regression: 'ocentric' used to hand back a sphere."""
    kind, a, b = _figure(crs.body_crs(body, system="ocentric"))
    assert kind == "ellipsoid", f"{body} 'ocentric' returned a sphere (a=b={a})"
    assert b < a


@pytest.mark.parametrize("body,_naif", TRIAXIAL)
def test_ographic_really_is_an_ellipsoid(body, _naif):
    kind, a, b = _figure(crs.body_crs(body, system="ographic"))
    assert kind == "ellipsoid"
    assert b < a


@pytest.mark.parametrize("body,_naif", TRIAXIAL)
def test_the_three_systems_are_three_distinct_crs(body, _naif):
    """Two names returning the same CRS is how the old bug hid."""
    got = {
        s: crs.body_crs(body, system=s).to_authority()
        for s in ("sphere", "ocentric", "ographic")
    }
    assert len(set(got.values())) == 3, f"{body} systems collapsed: {got}"


# --- code layout, so a PROJ reshuffle is caught too -------------------------


@pytest.mark.parametrize("body,naif", TRIAXIAL)
def test_offsets_follow_the_iau_triple(body, naif):
    """sphere=+0, ographic=+1, ocentric=+2 — the same triple as projected codes."""
    for system, offset in (("sphere", 0), ("ographic", 1), ("ocentric", 2)):
        auth, code = crs.body_crs(body, system=system).to_authority()
        assert auth == "IAU_2015"
        assert int(code) == naif * 100 + offset


def test_geographic_and_projected_tables_agree():
    """The two tables disagreed for years; they describe the same triple."""
    from planetarypy.crs import _PROJECTED_SYSTEM_OFFSET, _SYSTEM_OFFSET

    assert _SYSTEM_OFFSET == _PROJECTED_SYSTEM_OFFSET


# --- known values ----------------------------------------------------------


def test_mars_radii_match_iau_2015():
    """Anchored to published numbers, not just to internal consistency."""
    _, a, b = _figure(crs.body_crs("mars", system="ocentric"))
    assert a == pytest.approx(3396190.0)
    assert b == pytest.approx(3376200.0)

    _, a_s, b_s = _figure(crs.body_crs("mars", system="sphere"))
    assert a_s == pytest.approx(3396190.0)
    assert b_s == pytest.approx(3396190.0)


# --- defaults and back-compatibility ---------------------------------------


def test_default_is_the_sphere():
    """Default stayed the sphere across the fix, so nothing silently moved."""
    assert crs.body_crs("mars").to_authority() == ("IAU_2015", "49900")
    assert _figure(crs.body_crs("mars"))[0] == "sphere"


def test_get_crs_default_still_returns_the_sphere():
    """craterpy-compatible alias must not change what it hands back."""
    assert crs.get_crs("mars").to_authority() == crs.body_crs("mars").to_authority()
    assert _figure(crs.get_crs("mars"))[0] == "sphere"


def test_local_crs_defaults_to_the_sphere():
    assert _figure(crs.local_crs(77.4, 18.4, "mars"))[0] == "sphere"


# --- sphere-only bodies ----------------------------------------------------


@pytest.mark.parametrize("body,_naif", SPHERE_ONLY)
def test_sphere_only_bodies_work(body, _naif):
    assert _figure(crs.body_crs(body, system="sphere"))[0] == "sphere"


@pytest.mark.parametrize("body,_naif", SPHERE_ONLY)
@pytest.mark.parametrize("system", ["ocentric", "ographic"])
def test_sphere_only_bodies_reject_ellipsoid_systems(body, _naif, system):
    """Fail loudly rather than silently falling back to the sphere."""
    with pytest.raises(ValueError, match="sphere"):
        crs.body_crs(body, system=system)


def test_unknown_system_is_rejected():
    with pytest.raises(ValueError, match="system must be one of"):
        crs.body_crs("mars", system="geocentric")


def test_error_lists_the_valid_systems():
    with pytest.raises(ValueError) as excinfo:
        crs.body_crs("mars", system="nonsense")
    message = str(excinfo.value)
    for system in ("sphere", "ocentric", "ographic"):
        assert system in message


# --- WKT-level checks, and which radius the sphere uses --------------------


def test_wkt_inverse_flattening_yields_the_polar_radius():
    """WKT stores a and 1/f, not b — so derive b and check it, not just pyproj."""
    import re

    wkt = crs.body_crs("mars", system="ocentric").to_wkt()
    match = re.search(r'ELLIPSOID\["[^"]+",([0-9.]+),([0-9.]+)', wkt)
    a, inv_f = float(match.group(1)), float(match.group(2))
    assert a == pytest.approx(3396190.0)
    assert a * (1 - 1 / inv_f) == pytest.approx(3376200.0, abs=0.5)


def test_sphere_wkt_declares_zero_flattening():
    import re

    wkt = crs.body_crs("mars", system="sphere").to_wkt()
    match = re.search(r'ELLIPSOID\["[^"]+",([0-9.]+),([0-9.]+)', wkt)
    assert float(match.group(2)) == 0.0


def test_the_iau_sphere_uses_the_equatorial_radius_not_the_mean():
    """Worth pinning: the IAU sphere is a, not (2a+b)/3.

    Mars' mean radius is 3389526.67 m but the sphere CRS is 3396190 m — a
    6.7 km difference. Assuming the sphere is the mean radius is an easy and
    silent error.
    """
    from planetarypy import constants

    sphere_r = crs.body_crs("mars", system="sphere").ellipsoid.semi_major_metre
    ell = crs.body_crs("mars", system="ocentric").ellipsoid
    mean_r = (2 * ell.semi_major_metre + ell.semi_minor_metre) / 3

    assert sphere_r == pytest.approx(ell.semi_major_metre)
    assert sphere_r != pytest.approx(mean_r, abs=1.0)
    assert mean_r == pytest.approx(constants.Mars.mean_radius.to("m").value, abs=0.01)
