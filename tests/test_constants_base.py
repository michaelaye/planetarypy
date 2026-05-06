"""Tests for ``planetarypy.constants.base``.

Covers the foundation types — :class:`Constant` (Quantity subclass with
provenance metadata), :class:`Body` (frozen dataclass with derived
mass/density properties), and :class:`BodyRegistry` (NAIF-id +
case-insensitive name lookup).

The ``Constant`` subclass is exercised in detail because it's the
freshly-introduced API surface — math, slicing, repr, equality, and
unit conversion are all expected to work like Quantity while preserving
metadata on view operations.
"""

from __future__ import annotations

import math

import numpy as np
import pytest
import astropy.units as u

from planetarypy.constants.base import (
    Body,
    BodyRegistry,
    Constant,
)


# ── Constant ────────────────────────────────────────────────────────────


def _make_gm(body="Mars", year=2015):
    return Constant(
        4.282837362069909e4 * u.km ** 3 / u.s ** 2,
        name="GM",
        body=body,
        description="Gravitational parameter",
        reference=f"IAU {year}",
        iau_year=year,
        pck_source="pck00011.tpc",
    )


def _make_radii(body="Mars"):
    return Constant(
        [3396.19, 3396.19, 3376.20] * u.km,
        name="radii",
        body=body,
        description="Triaxial body radii (a, b, c)",
        reference="IAU 2015",
        iau_year=2015,
        pck_source="pck00011.tpc",
    )


class TestConstantConstruction:
    def test_from_quantity(self):
        c = _make_gm()
        assert c.value == pytest.approx(42828.37362069909)
        assert c.unit == u.km ** 3 / u.s ** 2

    def test_from_value_unit_pair(self):
        c = Constant(3396.19, u.km, name="r", body="Mars",
                     reference="IAU 2015", iau_year=2015,
                     pck_source="pck00011.tpc")
        assert c.value == pytest.approx(3396.19)
        assert c.unit == u.km

    def test_from_array_quantity(self):
        c = _make_radii()
        assert c.shape == (3,)
        assert c[0].value == pytest.approx(3396.19)

    def test_metadata_accessors(self):
        c = _make_gm()
        assert c.name == "GM"
        assert c.body == "Mars"
        assert c.description == "Gravitational parameter"
        assert c.reference == "IAU 2015"
        assert c.iau_year == 2015
        assert c.pck_source == "pck00011.tpc"

    def test_metadata_defaults_when_omitted(self):
        c = Constant(1.0, u.km)
        assert c.name == ""
        assert c.body == ""
        assert c.iau_year == 0


class TestConstantQuantityAPI:
    """A Constant must walk and quack like a Quantity."""

    def test_is_a_quantity(self):
        assert isinstance(_make_gm(), u.Quantity)

    def test_value_and_unit(self):
        c = _make_gm()
        assert isinstance(c.value, float)
        assert c.unit.is_equivalent(u.km ** 3 / u.s ** 2)

    def test_to_unit_conversion(self):
        radii = _make_radii()
        in_meters = radii.to(u.m)
        assert in_meters[0].value == pytest.approx(3396190.0)
        assert in_meters.unit == u.m

    def test_addition_in_consistent_units(self):
        a = Constant(1.0 * u.km, name="a", body="X", reference="r",
                     iau_year=1, pck_source="p")
        b = Constant(500.0 * u.m, name="b", body="X", reference="r",
                     iau_year=1, pck_source="p")
        result = a + b
        assert result.to(u.km).value == pytest.approx(1.5)

    def test_multiplication_by_scalar(self):
        c = Constant(10.0 * u.km, name="r", body="X", reference="r",
                     iau_year=1, pck_source="p")
        doubled = c * 2
        assert doubled.value == pytest.approx(20.0)
        assert doubled.unit == u.km

    def test_division_yields_dimensionless(self):
        a = Constant(10.0 * u.km, name="a", body="X", reference="r",
                     iau_year=1, pck_source="p")
        b = Constant(2.0 * u.km, name="b", body="X", reference="r",
                     iau_year=1, pck_source="p")
        ratio = a / b
        assert float(ratio) == pytest.approx(5.0)
        # Dimensionless after kilometers cancel
        assert ratio.unit == u.dimensionless_unscaled

    def test_compose_with_astropy_constants(self):
        from astropy.constants import G
        gm = _make_gm()
        # A real-world calculation: derived mass via GM / G
        mass = (gm / G).to(u.kg)
        # Mars is roughly 6.4e23 kg
        assert mass.value == pytest.approx(6.417e23, rel=1e-3)


class TestConstantSliceAndViewMetadata:
    """View operations (slicing, copying) preserve metadata."""

    def test_index_preserves_metadata(self):
        radii = _make_radii(body="Mars")
        a = radii[0]
        # Slicing yields a Constant (or a Quantity-like) with metadata intact
        assert getattr(a, "body", None) == "Mars" or a.body == "Mars"
        assert a.name == "radii"
        assert a.iau_year == 2015

    def test_slice_preserves_metadata(self):
        radii = _make_radii(body="Saturn")
        first_two = radii[:2]
        assert first_two.shape == (2,)
        assert first_two.body == "Saturn"
        assert first_two.iau_year == 2015

    def test_copy_preserves_metadata(self):
        radii = _make_radii(body="Earth")
        copy = radii.copy()
        assert copy.body == "Earth"
        assert copy.iau_year == 2015


class TestConstantNumeric:
    def test_numeric_equality_against_quantity(self):
        gm = _make_gm()
        bare_quantity = 4.282837362069909e4 * u.km ** 3 / u.s ** 2
        assert gm == bare_quantity

    def test_comparison_operators(self):
        a = Constant(1.0 * u.km, name="a", body="X", reference="r",
                     iau_year=1, pck_source="p")
        b = Constant(2.0 * u.km, name="b", body="X", reference="r",
                     iau_year=1, pck_source="p")
        assert a < b
        assert b > a
        assert a != b

    def test_power(self):
        c = Constant(2.0 * u.km, name="r", body="X", reference="r",
                     iau_year=1, pck_source="p")
        squared = c ** 2
        assert squared.value == pytest.approx(4.0)
        assert squared.unit == u.km ** 2


class TestConstantRepr:
    def test_repr_includes_provenance_when_complete(self):
        gm = _make_gm()
        s = repr(gm)
        assert "Mars.GM" in s
        assert "IAU 2015" in s

    def test_repr_falls_back_to_quantity_when_metadata_missing(self):
        c = Constant(1.0, u.km)
        s = repr(c)
        # Plain Quantity-style repr (no "Constant ...")
        assert "<Constant" not in s


# ── Body ────────────────────────────────────────────────────────────────


class TestBodyConstruction:
    def test_minimal_body(self):
        b = Body(name="MARS", naif_id=499, body_class="planet")
        assert b.name == "MARS"
        assert b.naif_id == 499
        assert b.body_class == "planet"
        assert b.parent is None
        assert b.dwarf_planet is False
        assert b.mission_visited is False

    def test_body_is_frozen(self):
        b = Body(name="MARS", naif_id=499, body_class="planet")
        with pytest.raises((AttributeError, Exception)):
            b.name = "EARTH"      # frozen dataclass forbids reassignment

    def test_repr(self):
        b = Body(name="MARS", naif_id=499, body_class="planet")
        s = repr(b)
        assert "MARS" in s and "499" in s and "planet" in s


class TestBodyDerivedProperties:
    """mass / density use astropy.constants.G at call time."""

    def _mars(self) -> Body:
        return Body(
            name="MARS", naif_id=499, body_class="planet",
            radii=_make_radii(body="Mars"),
            GM=_make_gm(body="Mars"),
        )

    def test_mass_returns_quantity(self):
        m = self._mars().mass
        assert isinstance(m, u.Quantity)
        assert m.unit == u.kg
        # Mars: ~6.4e23 kg
        assert m.value == pytest.approx(6.417e23, rel=1e-3)

    def test_mass_is_plain_quantity_not_constant(self):
        # Derived value: provenance is "see GM and G" → plain Quantity
        m = self._mars().mass
        assert not isinstance(m, Constant)

    def test_mass_is_none_when_gm_missing(self):
        b = Body(name="X", naif_id=1, body_class="other",
                 radii=_make_radii(body="X"))
        assert b.mass is None

    def test_density_returns_quantity_in_kg_per_m3(self):
        d = self._mars().density
        assert isinstance(d, u.Quantity)
        assert d.unit == u.kg / u.m ** 3
        # Mars: ~3933 kg/m³
        assert d.value == pytest.approx(3933, rel=1e-2)

    def test_density_is_none_when_radii_missing(self):
        b = Body(name="X", naif_id=1, body_class="other",
                 GM=_make_gm(body="X"))
        assert b.density is None


# ── BodyRegistry ────────────────────────────────────────────────────────


@pytest.fixture
def reg() -> BodyRegistry:
    mars = Body(name="MARS", naif_id=499, body_class="planet")
    saturn = Body(name="SATURN", naif_id=699, body_class="planet")
    titan = Body(name="TITAN", naif_id=606, body_class="moon", parent=699)
    enceladus = Body(name="ENCELADUS", naif_id=602, body_class="moon", parent=699)
    phobos = Body(name="PHOBOS", naif_id=401, body_class="moon", parent=499)
    bennu = Body(name="BENNU", naif_id=2101955, body_class="asteroid",
                 mission_visited=True)
    return BodyRegistry({
        b.naif_id: b for b in (mars, saturn, titan, enceladus, phobos, bennu)
    })


class TestBodyRegistryLookup:
    def test_by_naif_id(self, reg):
        assert reg[499].name == "MARS"

    def test_by_name(self, reg):
        assert reg["MARS"].naif_id == 499

    def test_by_name_is_case_insensitive(self, reg):
        assert reg["mars"].naif_id == 499
        assert reg["Mars"].naif_id == 499
        assert reg["TiTan"].naif_id == 606

    def test_keyerror_on_missing(self, reg):
        with pytest.raises(KeyError):
            reg["NIBIRU"]
        with pytest.raises(KeyError):
            reg[12345]

    def test_contains(self, reg):
        assert 499 in reg
        assert "mars" in reg
        assert 12345 not in reg
        assert "NIBIRU" not in reg

    def test_find_returns_none_on_miss(self, reg):
        assert reg.find("NIBIRU") is None
        assert reg.find(12345) is None

    def test_find_succeeds(self, reg):
        assert reg.find("MARS").naif_id == 499
        assert reg.find(499).name == "MARS"


class TestBodyRegistryFiltering:
    def test_by_class_planet(self, reg):
        planets = reg.by_class("planet")
        assert {b.name for b in planets} == {"MARS", "SATURN"}

    def test_by_class_moon_no_parent(self, reg):
        moons = reg.by_class("moon")
        assert {b.name for b in moons} == {"TITAN", "ENCELADUS", "PHOBOS"}

    def test_by_class_moon_with_parent_id(self, reg):
        saturn_moons = reg.by_class("moon", parent=699)
        assert {b.name for b in saturn_moons} == {"TITAN", "ENCELADUS"}

    def test_by_class_moon_with_parent_name(self, reg):
        saturn_moons = reg.by_class("moon", parent="Saturn")
        assert {b.name for b in saturn_moons} == {"TITAN", "ENCELADUS"}

    def test_by_class_moon_with_parent_body(self, reg):
        saturn = reg["SATURN"]
        saturn_moons = reg.by_class("moon", parent=saturn)
        assert {b.name for b in saturn_moons} == {"TITAN", "ENCELADUS"}

    def test_by_class_with_unknown_parent_name(self, reg):
        # Graceful: empty list, not an exception
        assert reg.by_class("moon", parent="NIBIRU") == []

    def test_by_class_asteroid(self, reg):
        assert [b.name for b in reg.by_class("asteroid")] == ["BENNU"]
