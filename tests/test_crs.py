"""Tests for planetarypy.crs — IAU planetary CRS construction.

Mostly NAIF-id based so they stay network-free (no constants import); one
test exercises the name->NAIF-id path via planetarypy.constants.
"""
from __future__ import annotations

import pytest

pytest.importorskip("pyproj")

from planetarypy.crs import body_crs, get_crs, local_crs


class TestBodyCrs:
    def test_mars_by_naif_id_is_geographic_with_radius(self):
        crs = body_crs(499)  # Mars, ocentric — radii come from the IAU code
        assert crs.is_geographic
        assert crs.ellipsoid.semi_major_metre == pytest.approx(3396190.0)

    def test_name_resolves_via_constants(self):
        # the name path imports planetarypy.constants to map "mars" -> 499
        assert body_crs("mars").to_wkt() == body_crs(499).to_wkt()

    def test_ographic_available_for_mars(self):
        assert body_crs(499, system="ographic").is_geographic

    def test_ographic_missing_for_moon_raises(self):
        with pytest.raises(ValueError, match="ographic"):
            body_crs(301, system="ographic")  # Moon defines no ographic code

    def test_unknown_system_raises(self):
        with pytest.raises(ValueError, match="system must be"):
            body_crs(499, system="bogus")

    def test_unknown_body_name_raises(self):
        with pytest.raises(ValueError, match="Unknown body"):
            body_crs("notaplanet")


class TestLocalCrs:
    def test_centered_azeqd_maps_center_to_origin(self):
        from pyproj import Transformer

        lon, lat = 137.4, -4.6
        local = local_crs(lon, lat, 499)
        assert local.is_projected
        geod = body_crs(499).geodetic_crs
        t = Transformer.from_crs(geod, local, always_xy=True)
        x, y = t.transform(lon, lat)
        assert x == pytest.approx(0.0, abs=1e-3)
        assert y == pytest.approx(0.0, abs=1e-3)


class TestGetCrs:
    def test_default_is_ocentric(self):
        assert get_crs(499).to_wkt() == body_crs(499, "ocentric").to_wkt()
