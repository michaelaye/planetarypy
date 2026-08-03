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
        crs = body_crs(499)  # Mars, IAU sphere — radii come from the IAU code
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
    def test_default_is_the_sphere(self):
        """Was `test_default_is_ocentric`, and it encoded the bug.

        It asserted get_crs(499) == body_crs(499, "ocentric") and passed only
        because "ocentric" wrongly resolved to offset 0, the sphere. Comparing
        two names without checking the figure is precisely how that survived;
        see tests/test_crs_figure.py.
        """
        assert get_crs(499).to_wkt() == body_crs(499, "sphere").to_wkt()

    def test_default_is_not_the_ocentric_ellipsoid(self):
        assert get_crs(499).to_authority() != body_crs(499, "ocentric").to_authority()


class TestProjectedCrs:
    def test_mars_north_polar_code(self):
        from planetarypy.crs import projected_crs

        assert projected_crs(499, "north_polar").to_authority() == ("IAU_2015", "49930")

    def test_mars_south_polar_code(self):
        from planetarypy.crs import projected_crs

        assert projected_crs(499, "south_polar").to_authority() == ("IAU_2015", "49935")

    def test_equirectangular_clon_variants_differ(self):
        from planetarypy.crs import projected_crs

        assert projected_crs(499, "equirectangular").to_authority()[1] == "49910"
        assert projected_crs(499, "equirectangular_180").to_authority()[1] == "49915"

    def test_sphere_variant_uses_equatorial_radius(self):
        from planetarypy.crs import projected_crs

        crs = projected_crs(499, "north_polar")
        assert crs.ellipsoid.semi_major_metre == pytest.approx(3396190.0)
        assert crs.ellipsoid.is_semi_minor_computed or (
            crs.ellipsoid.semi_minor_metre == pytest.approx(3396190.0)
        )

    def test_ellipsoid_variants_offset_by_one_and_two(self):
        from planetarypy.crs import projected_crs

        assert projected_crs(499, "north_polar", "ographic").to_authority()[1] == "49931"
        assert projected_crs(499, "north_polar", "ocentric").to_authority()[1] == "49932"

    def test_projected_is_not_geographic(self):
        from planetarypy.crs import projected_crs

        assert projected_crs(499, "north_polar").is_projected

    def test_unknown_projection_raises(self):
        from planetarypy.crs import projected_crs

        with pytest.raises(ValueError, match="projection must be one of"):
            projected_crs(499, "not_a_projection")

    def test_unknown_system_raises(self):
        from planetarypy.crs import projected_crs

        with pytest.raises(ValueError, match="system must be one of"):
            projected_crs(499, "north_polar", "bogus")

    def test_body_without_ographic_raises(self):
        from planetarypy.crs import projected_crs

        with pytest.raises(ValueError, match="No IAU_2015"):
            projected_crs(301, "north_polar", "ographic")
