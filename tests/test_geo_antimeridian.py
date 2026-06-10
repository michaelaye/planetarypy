"""Tests for anti-meridian (±180° longitude) handling in planetarypy.geo."""
from __future__ import annotations

import pytest

pytest.importorskip("shapely")

from planetarypy.geo import normalise_lon_bounds, split_at_antimeridian


class TestNormaliseLonBounds:
    def test_normal_interval(self):
        assert normalise_lon_bounds(10, 50) == (10.0, 50.0, False)

    def test_antimeridian_wrap(self):
        # narrow band straddling ±180 (raw 170..190, width < 180)
        assert normalise_lon_bounds(170, 190) == (170.0, -170.0, True)

    def test_prime_meridian_wrap(self):
        # narrow feature near 0° encoded the long way (8..359, width > 180)
        lo, hi, crosses = normalise_lon_bounds(8, 359)
        assert crosses is False
        assert lo == pytest.approx(-1.0)
        assert hi == pytest.approx(8.0)


class TestSplitAtAntimeridian:
    def test_degenerate_returns_empty(self):
        assert split_at_antimeridian([(0, 0), (1, 1)]) == []

    def test_no_crossing_single_polygon(self):
        box = [(10, -5), (20, -5), (20, 5), (10, 5)]
        parts = split_at_antimeridian(box)
        assert len(parts) == 1
        assert parts[0].is_valid

    def test_crossing_splits_into_two_hemispheres(self):
        # box from 170°E to -170°E, straddling the antimeridian
        box = [(170, -10), (-170, -10), (-170, 10), (170, 10)]
        parts = split_at_antimeridian(box)
        assert len(parts) == 2
        # neither piece straddles ±180 any more
        for p in parts:
            lons = [c[0] for c in p.exterior.coords]
            assert max(lons) - min(lons) < 180
        # one piece sits in each hemisphere
        maxlons = sorted(max(c[0] for c in p.exterior.coords) for p in parts)
        assert maxlons[0] <= 0 < maxlons[1]

    def test_pole_containing_returns_valid_polygons(self):
        import antimeridian  # noqa: F401 — the pole branch needs it

        cap = [(0, 85), (90, 85), (180, 85), (-90, 85)]
        parts = split_at_antimeridian(cap)
        assert len(parts) >= 1
        assert all(p.is_valid and not p.is_empty for p in parts)
