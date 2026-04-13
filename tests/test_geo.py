"""Tests for the geo module — coordinate transforms without GDAL."""

import numpy as np
import pytest
from affine import Affine

from planetarypy.geo import (
    pixel_to_xy,
    xy_to_pixel,
    xy_to_lonlat,
    lonlat_to_xy,
    image_azimuth,
    image_azimuth_cw_from_right,
    is_within,
    pixel_resolution,
    Point,
)


# A simple equirectangular transform: 100m/pixel, origin at (-1000, 2000)
SIMPLE_TRANSFORM = Affine(100.0, 0.0, -1000.0,
                          0.0, -100.0, 2000.0)


class TestPixelToXY:
    def test_origin(self):
        x, y = pixel_to_xy(SIMPLE_TRANSFORM, 0, 0)
        assert x == pytest.approx(-950.0)  # half pixel offset
        assert y == pytest.approx(1950.0)

    def test_known_point(self):
        x, y = pixel_to_xy(SIMPLE_TRANSFORM, 10, 20)
        assert x == pytest.approx(-950.0 + 1000.0)
        assert y == pytest.approx(1950.0 - 2000.0)

    def test_array_input(self):
        samples = np.array([0, 10, 20])
        lines = np.array([0, 10, 20])
        xs, ys = pixel_to_xy(SIMPLE_TRANSFORM, samples, lines)
        assert len(xs) == 3


class TestXYToPixel:
    def test_roundtrip(self):
        x, y = pixel_to_xy(SIMPLE_TRANSFORM, 15, 25)
        s, l = xy_to_pixel(SIMPLE_TRANSFORM, x, y)
        assert s == pytest.approx(15, abs=0.5)
        assert l == pytest.approx(25, abs=0.5)


class TestXYToLonLat:
    def test_identity_for_geographic(self):
        """If CRS is already geographic, xy_to_lonlat should be ~identity."""
        from pyproj import CRS
        crs = CRS.from_authority("IAU_2015", 49900)  # Mars geographic
        lon, lat = xy_to_lonlat(crs, 45.0, -10.0)
        assert lon == pytest.approx(45.0)
        assert lat == pytest.approx(-10.0)


class TestLonLatToXY:
    def test_identity_for_geographic(self):
        from pyproj import CRS
        crs = CRS.from_authority("IAU_2015", 49900)  # Mars geographic
        x, y = lonlat_to_xy(crs, 45.0, -10.0)
        assert x == pytest.approx(45.0)
        assert y == pytest.approx(-10.0)

    def test_roundtrip_projected(self):
        """Roundtrip through Mars north polar stereographic (IAU code)."""
        from pyproj import CRS
        crs = CRS.from_authority("IAU_2015", 49930)
        x, y = lonlat_to_xy(crs, 45.0, 85.0)
        lon, lat = xy_to_lonlat(crs, x, y)
        assert lon == pytest.approx(45.0, abs=1e-6)
        assert lat == pytest.approx(85.0, abs=1e-6)


class TestImageAzimuth:
    def test_north(self):
        """Straight up (decreasing line) should be 0 degrees."""
        az = image_azimuth(100, 100, 100, 50)
        assert az == pytest.approx(0.0, abs=0.01)

    def test_east(self):
        """Straight right (increasing sample) should be 90 degrees."""
        az = image_azimuth(100, 100, 150, 100)
        assert az == pytest.approx(90.0, abs=0.01)

    def test_south(self):
        """Straight down (increasing line) should be 180 degrees."""
        az = image_azimuth(100, 100, 100, 150)
        assert az == pytest.approx(180.0, abs=0.01)

    def test_west(self):
        """Straight left (decreasing sample) should be 270 degrees."""
        az = image_azimuth(100, 100, 50, 100)
        assert az == pytest.approx(270.0, abs=0.01)

    def test_northeast(self):
        az = image_azimuth(0, 0, 10, -10)
        assert az == pytest.approx(45.0, abs=0.01)


class TestImageAzimuthCWFromRight:
    """HiRISE convention: clockwise from 3 o'clock (right/east)."""

    def test_right(self):
        """Straight right should be 0 degrees."""
        az = image_azimuth_cw_from_right(100, 100, 150, 100)
        assert az == pytest.approx(0.0, abs=0.01)

    def test_down(self):
        """Straight down should be 90 degrees."""
        az = image_azimuth_cw_from_right(100, 100, 100, 150)
        assert az == pytest.approx(90.0, abs=0.01)

    def test_left(self):
        """Straight left should be 180 degrees."""
        az = image_azimuth_cw_from_right(100, 100, 50, 100)
        assert az == pytest.approx(180.0, abs=0.01)

    def test_up(self):
        """Straight up should be 270 degrees."""
        az = image_azimuth_cw_from_right(100, 100, 100, 50)
        assert az == pytest.approx(270.0, abs=0.01)

    def test_down_right(self):
        """Down-right (45 deg in image space)."""
        az = image_azimuth_cw_from_right(0, 0, 10, 10)
        assert az == pytest.approx(45.0, abs=0.01)


class TestIsWithin:
    """Test with Mars geographic CRS (IAU_2015:49900)."""

    @pytest.fixture()
    def mars_da(self):
        import xarray as xr
        import rioxarray  # noqa: F401
        # 100x100 image, 0.1 deg/pixel, covering lon 130-140, lat 0-10
        t = Affine(0.1, 0, 130.0, 0, -0.1, 10.0)
        xs = 130.0 + (np.arange(100) + 0.5) * 0.1
        ys = 10.0 - (np.arange(100) + 0.5) * 0.1
        da = xr.DataArray(
            np.zeros((100, 100)), dims=["y", "x"],
            coords={"x": xs, "y": ys},
        )
        da = da.rio.set_spatial_dims(x_dim="x", y_dim="y")
        da = da.rio.write_crs("IAU_2015:49900")
        da = da.rio.write_transform(t)
        return da

    def test_point_inside(self, mars_da):
        assert is_within(mars_da, lon=135.0, lat=5.0)

    def test_point_outside(self, mars_da):
        assert not is_within(mars_da, lon=150.0, lat=5.0)

    def test_edge(self, mars_da):
        # Just outside the northern edge
        assert not is_within(mars_da, lon=135.0, lat=11.0)


class TestPoint:
    @pytest.fixture()
    def mars_da(self):
        import xarray as xr
        import rioxarray  # noqa: F401
        t = Affine(0.1, 0, 130.0, 0, -0.1, 10.0)
        xs = 130.0 + (np.arange(100) + 0.5) * 0.1
        ys = 10.0 - (np.arange(100) + 0.5) * 0.1
        da = xr.DataArray(
            np.zeros((100, 100)), dims=["y", "x"],
            coords={"x": xs, "y": ys},
        )
        da = da.rio.set_spatial_dims(x_dim="x", y_dim="y")
        da = da.rio.write_crs("IAU_2015:49900")
        da = da.rio.write_transform(t)
        return da

    def test_from_lonlat(self):
        p = Point(lon=137.85, lat=-5.08, crs="IAU_2015:49900")
        assert p.lon == pytest.approx(137.85)
        assert p.lat == pytest.approx(-5.08)
        assert "Mars" in repr(p)

    def test_to_xy(self):
        p = Point(lon=45.0, lat=-10.0, crs="IAU_2015:49900")
        x, y = p.to_xy("IAU_2015:49910")
        assert x == pytest.approx(45.0 * np.pi / 180 * 3396190, rel=0.01)

    def test_from_pixel(self, mars_da):
        p = Point(sample=50, line=50, source=mars_da)
        assert p.lon == pytest.approx(135.0, abs=0.1)
        assert p.lat == pytest.approx(5.0, abs=0.1)
        assert p.sample == 50

    def test_from_lonlat_with_source(self, mars_da):
        p = Point(lon=135.0, lat=5.0, source=mars_da)
        assert p.sample == pytest.approx(50, abs=1)
        assert p.line == pytest.approx(50, abs=1)

    def test_is_within(self, mars_da):
        inside = Point(lon=135.0, lat=5.0, crs="IAU_2015:49900")
        assert inside.is_within(mars_da)

        outside = Point(lon=150.0, lat=5.0, crs="IAU_2015:49900")
        assert not outside.is_within(mars_da)

    def test_azimuth_to(self, mars_da):
        p1 = Point(sample=50, line=50, source=mars_da)
        p2 = Point(sample=50, line=30, source=mars_da)
        az = p1.azimuth_to(p2)
        assert az == pytest.approx(0.0, abs=0.01)  # due north

    def test_to_shapely(self):
        shapely = pytest.importorskip("shapely")
        from shapely.geometry import Point as ShapelyPoint
        p = Point(lon=45.0, lat=-10.0, crs="IAU_2015:49900")
        sp = p.to_shapely()
        assert isinstance(sp, ShapelyPoint)
        assert sp.x == pytest.approx(45.0)
        assert sp.y == pytest.approx(-10.0)


class TestPixelResolution:
    def test_simple(self):
        rx, ry = pixel_resolution(SIMPLE_TRANSFORM)
        assert rx == 100.0
        assert ry == 100.0

    def test_nonsquare(self):
        t = Affine(50.0, 0.0, 0.0, 0.0, -100.0, 0.0)
        rx, ry = pixel_resolution(t)
        assert rx == 50.0
        assert ry == 100.0
