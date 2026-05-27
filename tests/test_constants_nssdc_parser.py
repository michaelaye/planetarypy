"""Unit tests for ``scripts/parse_nssdc_archive.parse_capture``.

These exercise the parser on small synthetic HTML inputs rather than the
913-capture archive — fast feedback on regex behaviour for the
free-text atmosphere fallback (uncertainty, range, scientific
notation, qualifier prefixes, default-unit fallback).

The ``tests/test_constants_vs_sources.py`` slow tier still validates
the parser end-to-end against the real captures and the golden values.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(_SCRIPTS_DIR))
try:
    import parse_nssdc_archive as _parser  # noqa: E402
finally:
    sys.path.pop(0)


def _wrap_atmosphere(rows: str) -> str:
    """Wrap a chunk of free-text rows in the minimal HTML the parser
    expects: an ``<h3>...Atmosphere...</h3>`` section header followed by
    plain-text rows, terminated by ``<hr>`` so the free-text fallback's
    lookahead (``(?=<h3|<hr)``) finds the section boundary.
    ``parse_capture`` keys both the section walker and the free-text
    fallback off the "Atmosphere" keyword."""
    return (
        "<html><body>"
        "<h3>Test Body Atmosphere</h3>\n"
        f"{rows}\n"
        "<hr>"
        "</body></html>"
    )


class TestScientificNotationValues:
    """Atmosphere-fallback regex accepts NSSDC's scientific-notation
    shapes: bare ``10^N`` (implies coefficient 1) and ``X x 10^N`` (or
    ``X × 10^N``)."""

    def test_bare_power_of_ten(self):
        # NSSDC writes "~10^-15 bar" for Mercury's surface pressure.
        # No explicit coefficient → mantissa is 1.
        parsed = _parser.parse_capture(
            _wrap_atmosphere("Surface pressure: ~10^-15 bar")
        )
        entry = parsed["fields"]["surface_pressure"]
        assert entry["value"] == pytest.approx(1e-15)
        assert entry["unit"] == "bar"

    def test_coefficient_times_power_of_ten_lowercase_x(self):
        parsed = _parser.parse_capture(
            _wrap_atmosphere("Surface pressure: 2.5 x 10^16 kg")
        )
        entry = parsed["fields"]["surface_pressure"]
        assert entry["value"] == pytest.approx(2.5e16)
        assert entry["unit"] == "kg"

    def test_coefficient_times_power_of_ten_unicode_x(self):
        # NSSDC has used the Unicode "×" in some captures.
        parsed = _parser.parse_capture(
            _wrap_atmosphere("Surface density: 3.5 × 10^-3 kg/m^3")
        )
        entry = parsed["fields"]["surface_density"]
        assert entry["value"] == pytest.approx(3.5e-3)
        assert entry["unit"] == "kg/m^3"

    def test_positive_exponent(self):
        parsed = _parser.parse_capture(
            _wrap_atmosphere("Average temperature: 10^3 K")
        )
        entry = parsed["fields"]["average_temperature"]
        assert entry["value"] == pytest.approx(1000.0)


class TestQualifierPrefixes:
    """NSSDC sprinkles ``~`` / ``<`` / ``>`` / ``≈`` in front of values
    as approximate-value markers. Up to ``<~`` stacked has been seen."""

    @pytest.mark.parametrize("prefix", ["", "~", "<", ">", "≈", "<~", "~~"])
    def test_qualifier_prefixes_are_skipped(self, prefix):
        parsed = _parser.parse_capture(
            _wrap_atmosphere(f"Average temperature: {prefix}214 K")
        )
        entry = parsed["fields"]["average_temperature"]
        assert entry["value"] == pytest.approx(214.0)
        assert entry["unit"] == "K"


class TestUncertaintyExtraction:
    """``X +/- Y unit`` and ``X ± Y unit`` produce ``{value: X,
    uncertainty: Y, unit: unit}``."""

    def test_plus_minus_ascii(self):
        parsed = _parser.parse_capture(
            _wrap_atmosphere("Surface density: ~0.016 +/- 0.006 kg/m^3")
        )
        entry = parsed["fields"]["surface_density"]
        assert entry["value"] == pytest.approx(0.016)
        assert entry["uncertainty"] == pytest.approx(0.006)
        assert entry["unit"] == "kg/m^3"
        assert "range" not in entry

    def test_plus_minus_unicode(self):
        parsed = _parser.parse_capture(
            _wrap_atmosphere("Surface density: 0.020 ± 0.005 kg/m^3")
        )
        entry = parsed["fields"]["surface_density"]
        assert entry["value"] == pytest.approx(0.020)
        assert entry["uncertainty"] == pytest.approx(0.005)


class TestRangeExtraction:
    """``X - Y unit`` produces ``{range: {min: X, max: Y}, unit: unit}``
    with ``value`` OMITTED — the archive is interpretation-free; the
    runtime loader decides whether to surface NaN or the midpoint."""

    def test_range_with_unit(self):
        parsed = _parser.parse_capture(
            _wrap_atmosphere("Scale height: 19.1 - 20.3 km")
        )
        entry = parsed["fields"]["scale_height"]
        assert "value" not in entry, "range entries must omit value"
        assert entry["range"] == {"min": 19.1, "max": 20.3}
        assert entry["unit"] == "km"

    def test_range_without_unit(self):
        # Mean molecular weight: 2.53 - 2.69 (Neptune-style)
        parsed = _parser.parse_capture(
            _wrap_atmosphere("Mean molecular weight: 2.53 - 2.69")
        )
        entry = parsed["fields"]["mean_molecular_weight"]
        assert "value" not in entry
        assert entry["range"] == {"min": 2.53, "max": 2.69}
        assert entry["unit"] is None

    def test_range_with_qualifier_and_no_spaces(self):
        # Pluto-style: "~16-25 g/mole"
        parsed = _parser.parse_capture(
            _wrap_atmosphere("Mean molecular weight: ~16-25 g/mole")
        )
        entry = parsed["fields"]["mean_molecular_weight"]
        assert "value" not in entry
        assert entry["range"] == {"min": 16.0, "max": 25.0}
        assert entry["unit"] == "g/mole"


class TestNewlineDoesNotBleedIntoUnit:
    """Bug: the older atmosphere regex used ``\\s*`` between value and
    unit, which greedily consumed newlines, allowing the next line's
    section header to be captured as the unit (×278 readings on
    "Atmospheric composition: ..."). Fix uses ``[ \\t]*`` to stop at the
    end of the value's line."""

    def test_unit_capture_stops_at_newline(self):
        parsed = _parser.parse_capture(_wrap_atmosphere(
            "Mean molecular weight: 28.97 \n"
            "Atmospheric composition (by volume, dry air): "
        ))
        entry = parsed["fields"]["mean_molecular_weight"]
        assert entry["value"] == pytest.approx(28.97)
        # Unit must be None (NSSDC didn't write one on this line), NOT
        # the next line's header text.
        assert entry["unit"] is None
        assert "Atmospheric" not in (entry.get("unit") or "")
