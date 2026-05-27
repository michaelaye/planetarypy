"""Tests for the NSSDC range/uncertainty plumbing in
``planetarypy.constants.nssdc``: the config-driven ``range_strategy``
("nan" vs "midpoint"), the ``RangeWarning`` emission gate, and
uncertainty propagation through ``_make_constant``.
"""

from __future__ import annotations

import warnings

import astropy.units as u
import numpy as np
import pytest

from planetarypy.constants import nssdc
from planetarypy.constants.base import Range, RangeWarning
from planetarypy.constants.nssdc import _loader


def _make_record(**overrides) -> _loader.NSSDCRecord:
    """Build an NSSDCRecord for testing — all required fields default
    to sensible values; override per test."""
    defaults = dict(
        body="neptune",
        field="scale_height",
        value=None,
        unit="km",
        page_date="2024-10-06",
        wayback_timestamp="20241006132807",
        wayback_url="https://example/neptune",
        uncertainty=None,
        range_min=None,
        range_max=None,
    )
    defaults.update(overrides)
    return _loader.NSSDCRecord(**defaults)


@pytest.fixture(autouse=True)
def _reset_range_warning_gate():
    """Tests assume a fresh process — reset the once-per-process gate
    before each test runs."""
    nssdc._range_warning_emitted = False
    yield
    nssdc._range_warning_emitted = False


# ── Range strategy: "nan" (default) ─────────────────────────────────────


class TestRangeStrategyNan:
    """Default ``range_strategy = "nan"`` leaves ``value`` as NaN, keeps
    the bounds in ``.range``, and never emits a warning."""

    @pytest.fixture
    def _strategy_nan(self, monkeypatch):
        from planetarypy.config import config
        monkeypatch.setitem(config.tomldoc, "constants",
                            {"range_strategy": "nan"})

    def test_range_record_gives_nan_value(self, _strategy_nan):
        rec = _make_record(range_min=19.1, range_max=20.3)
        c = nssdc._make_constant(rec, body_label="Neptune")
        assert np.isnan(float(c.value))
        assert c.unit == u.km

    def test_range_record_carries_range_object(self, _strategy_nan):
        rec = _make_record(range_min=19.1, range_max=20.3)
        c = nssdc._make_constant(rec, body_label="Neptune")
        assert c.range == Range(19.1, 20.3)
        assert c.range.midpoint == pytest.approx(19.7)
        assert c.range.half_width == pytest.approx(0.6)

    def test_no_warning_emitted_in_nan_mode(self, _strategy_nan):
        rec = _make_record(range_min=19.1, range_max=20.3)
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always")
            nssdc._make_constant(rec, body_label="Neptune")
        assert not any(
            issubclass(w.category, RangeWarning) for w in captured
        ), "RangeWarning should never fire in nan-strategy mode"


# ── Range strategy: "midpoint" (opt-in) ─────────────────────────────────


class TestRangeStrategyMidpoint:
    """Opt-in ``range_strategy = "midpoint"``: ``value`` is the range
    midpoint, ``.range`` is still populated, and a one-time
    :class:`RangeWarning` fires per process."""

    @pytest.fixture
    def _strategy_midpoint(self, monkeypatch):
        from planetarypy.config import config
        monkeypatch.setitem(config.tomldoc, "constants",
                            {"range_strategy": "midpoint"})

    def test_value_is_midpoint(self, _strategy_midpoint):
        rec = _make_record(range_min=19.1, range_max=20.3)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RangeWarning)
            c = nssdc._make_constant(rec, body_label="Neptune")
        assert float(c.value) == pytest.approx(19.7)
        assert c.unit == u.km
        # Bounds still accessible.
        assert c.range == Range(19.1, 20.3)

    def test_warning_fires_first_time(self, _strategy_midpoint):
        rec = _make_record(range_min=19.1, range_max=20.3)
        with pytest.warns(RangeWarning):
            nssdc._make_constant(rec, body_label="Neptune")

    def test_warning_does_not_fire_twice(self, _strategy_midpoint):
        rec = _make_record(range_min=19.1, range_max=20.3)
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always")
            # First call should emit.
            nssdc._make_constant(rec, body_label="Neptune")
            # Subsequent calls in the same process must not re-emit.
            nssdc._make_constant(rec, body_label="Neptune")
            nssdc._make_constant(rec, body_label="Neptune")
        n_range_warnings = sum(
            1 for w in captured if issubclass(w.category, RangeWarning)
        )
        assert n_range_warnings == 1, (
            f"Expected one RangeWarning, got {n_range_warnings}"
        )

    def test_warning_suppressed_by_config(self, monkeypatch):
        from planetarypy.config import config
        monkeypatch.setitem(config.tomldoc, "constants", {
            "range_strategy": "midpoint",
            "warn_on_range_midpoint": False,
        })
        rec = _make_record(range_min=19.1, range_max=20.3)
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always")
            nssdc._make_constant(rec, body_label="Neptune")
        assert not any(
            issubclass(w.category, RangeWarning) for w in captured
        ), "Config-suppressed warning should not fire"


# ── Uncertainty plumbing ────────────────────────────────────────────────


class TestUncertaintyPropagation:
    """Uncertainty travels from NSSDCRecord through ``_make_constant`` to
    ``Constant.uncertainty`` regardless of range strategy."""

    def test_uncertainty_round_trip(self):
        rec = _make_record(
            field="surface_density", value=0.016, unit="kg/m^3",
            uncertainty=0.006,
        )
        c = nssdc._make_constant(rec, body_label="Mars")
        assert float(c.value) == pytest.approx(0.016)
        assert c.uncertainty == pytest.approx(0.006)
        assert c.range is None
        assert "± 0.006" in repr(c)

    def test_uncertainty_defaults_to_zero(self):
        rec = _make_record(field="bond_albedo", value=0.25, unit=None)
        c = nssdc._make_constant(rec, body_label="Mars")
        assert c.uncertainty == 0.0


# ── End-to-end repr ─────────────────────────────────────────────────────


class TestEndToEndRepr:
    """The repr for the NaN+range case shows ``[range X–Y]`` instead of
    the meaningless ``nan``."""

    def test_repr_shows_range_when_value_is_nan(self, monkeypatch):
        from planetarypy.config import config
        monkeypatch.setitem(config.tomldoc, "constants",
                            {"range_strategy": "nan"})
        rec = _make_record(
            field="scale_height", range_min=19.1, range_max=20.3, unit="km",
        )
        c = nssdc._make_constant(rec, body_label="Neptune")
        s = repr(c)
        assert "[range 19.1" in s
        assert "20.3]" in s
        assert "nan" not in s.lower()
