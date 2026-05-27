"""Unit tests for ``planetarypy.constants.nssdc._loader``.

Focused on the unit-coercion surface — explicit unit-string lookups,
the per-field default-unit fallback for NSSDC's unit-less rows, and
the precedence rules between them.
"""

from __future__ import annotations

import astropy.units as u
import pytest

from planetarypy.constants.nssdc._loader import (
    _FIELD_DEFAULT_UNITS,
    coerce_value,
)


class TestCoerceValueBasicUnitLookup:

    def test_known_unit_returns_quantity(self):
        q = coerce_value(2440.5, "km")
        assert q.value == pytest.approx(2440.5)
        assert q.unit == u.km

    def test_scaled_unit_applies_factor(self):
        # "10^24 kg" → multiply value by 1e24 and tag as kg.
        q = coerce_value(0.6419, "10^24 kg")
        assert q.value == pytest.approx(6.419e23)
        assert q.unit == u.kg

    def test_normalized_x_prefix(self):
        # "x 10^6 km" should normalize to "10^6 km" before lookup.
        q = coerce_value(2.0, "x 10^6 km")
        assert q.value == pytest.approx(2e6)
        assert q.unit == u.km

    def test_unknown_unit_returns_none(self):
        # Astropy's fallback Unit() parse may raise — coerce returns None.
        q = coerce_value(1.0, "definitely not a unit ███")
        assert q is None

    def test_none_value_returns_none(self):
        assert coerce_value(None, "km") is None

    def test_none_unit_falls_back_to_dimensionless(self):
        # No field_name AND no unit → dimensionless via _UNIT_MAP[None].
        q = coerce_value(0.5, None)
        assert q.unit == u.dimensionless_unscaled
        assert q.value == pytest.approx(0.5)


class TestFieldDefaultUnitFallback:
    """When NSSDC omits a unit but the field has a conventional one,
    the default kicks in. Earth publishes ``mean_molecular_weight:
    28.97`` (no unit); atmospheric-chemistry convention is g/mol."""

    def test_default_applied_when_unit_absent(self):
        q = coerce_value(28.97, None, field_name="mean_molecular_weight")
        assert q.value == pytest.approx(28.97)
        assert q.unit == u.g / u.mol

    def test_default_applied_for_empty_string_unit(self):
        # Some captures emit "" rather than None for an absent unit.
        q = coerce_value(28.97, "", field_name="mean_molecular_weight")
        assert q.unit == u.g / u.mol

    def test_explicit_unit_overrides_default(self):
        # Source provided an explicit unit — that wins, the default is
        # NOT applied (would silently lose information otherwise).
        q = coerce_value(43.34, "g/mole", field_name="mean_molecular_weight")
        assert q.unit == u.g / u.mol  # same unit, came from the source

    def test_unknown_field_falls_back_to_dimensionless(self):
        # A field name we haven't documented a default for behaves like
        # the no-field-name case.
        q = coerce_value(0.5, None, field_name="some_unknown_field")
        assert q.unit == u.dimensionless_unscaled

    def test_field_name_only_consulted_for_absent_unit(self):
        # field_name must NOT override a known unit string.
        q = coerce_value(28.97, "kg", field_name="mean_molecular_weight")
        assert q.unit == u.kg

    def test_default_table_entries_are_astropy_units(self):
        # Guard against accidentally storing a string in the table.
        for field, unit in _FIELD_DEFAULT_UNITS.items():
            assert isinstance(unit, u.UnitBase), (
                f"_FIELD_DEFAULT_UNITS[{field!r}] must be an astropy "
                f"UnitBase, got {type(unit).__name__}"
            )
