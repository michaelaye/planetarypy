"""Tests for the user-facing ``planetarypy.constants`` public API.

Covers:
- top-level body access via attribute (Mars, Saturn, Bennu, ...)
- discovery helpers (planets, moons, asteroids, comets, dwarf_planets,
  mission_visited, sun, find_body)
- time-travel: iau2009 vs iau2015 submodule imports coexist cleanly
- integration sanity: a few key values match published literature
  within a small tolerance.
"""

from __future__ import annotations

import pytest
import astropy.units as u

import planetarypy.constants as c
from planetarypy.constants import iau2009, iau2015
from planetarypy.constants.base import Body, Constant


# ── Top-level attribute access (default edition = iau2015) ──────────────


class TestTopLevelAccess:
    def test_default_edition_is_iau2015(self):
        assert c.DEFAULT_IAU_YEAR == 2015
        # Direct comparison: the default Mars IS the iau2015 Mars
        assert c.Mars is iau2015.Mars

    def test_available_editions(self):
        assert c.AVAILABLE_IAU_YEARS == (2009, 2015)

    def test_named_planets_present(self):
        for name in ("Mercury", "Venus", "Earth", "Mars",
                     "Jupiter", "Saturn", "Uranus", "Neptune", "Pluto"):
            assert hasattr(c, name), f"missing {name} at top level"

    def test_sample_moons_present(self):
        for name in ("Moon", "Phobos", "Titan", "Europa", "Charon"):
            assert hasattr(c, name)

    def test_sample_small_bodies_present(self):
        # Mission targets we successfully wired in v1
        for name in ("Bennu", "Vesta", "Ceres", "Lutetia"):
            assert hasattr(c, name)


# ── Discovery helpers ──────────────────────────────────────────────────


class TestDiscoveryHelpers:
    def test_sun_returns_one_body(self):
        result = c.sun()
        assert len(result) == 1
        assert result[0].name == "SUN"
        assert result[0].naif_id == 10

    def test_planets_returns_all_nine(self):
        names = [b.name for b in c.planets()]
        assert names == ["MERCURY", "VENUS", "EARTH", "MARS",
                         "JUPITER", "SATURN", "URANUS", "NEPTUNE", "PLUTO"]

    def test_moons_no_arg_is_flat_list(self):
        all_moons = c.moons()
        assert len(all_moons) > 50              # ~70 in the PCK
        assert all(b.body_class == "moon" for b in all_moons)
        # Includes moons from multiple planets
        parents = {b.parent for b in all_moons}
        assert 499 in parents and 599 in parents and 699 in parents

    def test_moons_filter_by_name(self):
        saturn_moons = c.moons(of="Saturn")
        # All have the same parent
        assert {b.parent for b in saturn_moons} == {699}
        # Includes the classical moons
        names = {b.name for b in saturn_moons}
        for cls in ("MIMAS", "ENCELADUS", "TETHYS", "DIONE", "RHEA",
                    "TITAN", "IAPETUS"):
            assert cls in names

    def test_moons_filter_by_naif_id(self):
        saturn_moons_by_id = c.moons(of=699)
        saturn_moons_by_name = c.moons(of="Saturn")
        assert {b.naif_id for b in saturn_moons_by_id} == \
               {b.naif_id for b in saturn_moons_by_name}

    def test_moons_filter_by_body(self):
        saturn = c.find_body("Saturn")
        saturn_moons = c.moons(of=saturn)
        assert all(b.parent == saturn.naif_id for b in saturn_moons)

    def test_asteroids(self):
        asteroids = c.asteroids()
        assert len(asteroids) > 30
        assert all(b.body_class == "asteroid" for b in asteroids)

    def test_comets(self):
        comets = c.comets()
        assert all(b.body_class == "comet" for b in comets)
        # 67P should be present
        names = {b.name for b in comets}
        assert any("CHURYUMOV" in n for n in names)

    def test_dwarf_planets_includes_pluto_and_ceres(self):
        dps = c.dwarf_planets()
        names = {b.name for b in dps}
        assert "PLUTO" in names
        assert "CERES" in names

    def test_pluto_in_both_planets_and_dwarf_planets(self):
        """Documented overlap: matches IAU's dual classification."""
        planet_names = {b.name for b in c.planets()}
        dp_names = {b.name for b in c.dwarf_planets()}
        assert "PLUTO" in planet_names
        assert "PLUTO" in dp_names

    def test_mission_visited_includes_known_targets(self):
        names = {b.name for b in c.mission_visited()}
        # We curated these in regenerate_constants.py and have data for them
        assert "BENNU" in names
        assert "CERES" in names
        assert "VESTA" in names

    def test_find_body_by_name(self):
        b = c.find_body("Bennu")
        assert b is not None
        assert b.naif_id == 2101955

    def test_find_body_case_insensitive(self):
        assert c.find_body("mars") is c.find_body("MARS") is c.find_body("Mars")

    def test_find_body_by_naif_id(self):
        assert c.find_body(499).name == "MARS"

    def test_find_body_returns_none_on_miss(self):
        assert c.find_body("Nibiru") is None
        assert c.find_body(99999999) is None


# ── Time-travel: per-edition submodules coexist ─────────────────────────


class TestTimeTravel:
    def test_both_editions_loaded(self):
        assert hasattr(iau2009, "Mars")
        assert hasattr(iau2015, "Mars")

    def test_iau2015_is_the_default(self):
        assert c.Mars is iau2015.Mars
        assert c.Mars is not iau2009.Mars

    def test_orientation_differs_between_editions(self):
        # The IAU report editions differ on rotational elements (the
        # whole reason for the time-travel architecture). Check that
        # at least one orientation field actually differs.
        coords_changed = (
            iau2009.Mars.pole_ra.value != iau2015.Mars.pole_ra.value
            or iau2009.Mars.pole_dec.value != iau2015.Mars.pole_dec.value
        )
        assert coords_changed, (
            "Expected IAU 2015 Mars orientation to differ from IAU 2009"
        )

    def test_both_editions_carry_their_own_iau_year(self):
        assert iau2009.Mars.GM.iau_year == 2009
        assert iau2015.Mars.GM.iau_year == 2015


# ── Integration: numerical sanity checks against literature ─────────────


class TestNumericalSanity:
    """Spot-check a few values against published literature within
    reasonable tolerance. If these fail, something's wrong with either
    the regenerator or the source kernels."""

    def test_mars_radii_iau2015(self):
        # Mars (IAU 2015): a = b = 3396.19 km, c = 3376.20 km
        assert c.Mars.radii[0].value == pytest.approx(3396.19, rel=1e-4)
        assert c.Mars.radii[2].value == pytest.approx(3376.20, rel=1e-4)

    def test_mars_GM(self):
        # Mars GM ≈ 4.282837e4 km³/s² (DE440)
        assert c.Mars.GM.to(u.km**3 / u.s**2).value == \
               pytest.approx(4.282837e4, rel=1e-4)

    def test_mars_mass(self):
        # Mars mass ≈ 6.417e23 kg
        assert c.Mars.mass.to(u.kg).value == pytest.approx(6.417e23, rel=1e-3)

    def test_mars_density(self):
        # Mars density ≈ 3933 kg/m³
        assert c.Mars.density.to(u.kg / u.m**3).value == \
               pytest.approx(3933, rel=1e-2)

    def test_earth_radii(self):
        # Earth (IAU): a = b = 6378.1366 km, c = 6356.7519 km
        assert c.Earth.radii[0].value == pytest.approx(6378.14, rel=1e-4)
        assert c.Earth.radii[2].value == pytest.approx(6356.75, rel=1e-4)

    def test_jupiter_GM(self):
        # Jupiter GM ≈ 1.2669e8 km³/s²
        assert c.Jupiter.GM.to(u.km**3 / u.s**2).value == \
               pytest.approx(1.2669e8, rel=1e-4)

    def test_ceres_mass_is_nonzero(self):
        # Ceres GM is in dawn_ceres_v06.tpc, so mass should be derivable
        ceres = c.find_body("Ceres")
        assert ceres.mass is not None
        # ~9.4e20 kg
        assert ceres.mass.to(u.kg).value == pytest.approx(9.4e20, rel=1e-2)

    def test_bennu_has_shape_no_GM(self):
        # v1 caveat: Bennu kernel ships shape but no GM
        bennu = c.find_body("Bennu")
        assert bennu.radii is not None
        assert bennu.GM is None
        assert bennu.mass is None  # consequence of GM=None

    def test_titan_present_with_radii(self):
        titan = c.find_body("Titan")
        assert titan is not None
        # Titan radius ≈ 2575 km
        assert titan.radii[0].value == pytest.approx(2575, rel=1e-2)

    def test_constant_carries_iau_provenance(self):
        gm = c.Mars.GM
        assert isinstance(gm, Constant)
        assert gm.iau_year == 2015
        assert gm.body == "Mars"
        assert gm.name == "GM"
        assert "IAU 2015" in gm.reference


# ── Repr / introspection ────────────────────────────────────────────────


class TestRepr:
    def test_constant_repr_shows_provenance(self):
        s = repr(c.Mars.GM)
        assert "Mars.GM" in s
        assert "IAU 2015" in s

    def test_body_repr(self):
        s = repr(c.Mars)
        assert "MARS" in s and "499" in s and "planet" in s
