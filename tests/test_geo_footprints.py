"""Tests for the GDAL-native projected-raster geometry helpers in geo.py.

These read CRS/footprint/overlaps straight from rasters via rasterio, so
they work on any GDAL-readable format (ISIS .cub, GeoTIFF, …) with no ISIS.
Small GeoTIFFs are synthesized in tmp_path; nodata borders exercise the
valid-data (not bounding-box) footprint logic.
"""
from __future__ import annotations

import numpy as np
import pytest
from affine import Affine

rasterio = pytest.importorskip("rasterio")

from planetarypy.geo import (  # noqa: E402  (after importorskip)
    footprints_to_gdf,
    is_projected,
    overlaps,
    raster_footprint,
)

# 1 unit / pixel, origin at (x0, 0), north-up.
def _transform(x0=0.0):
    return Affine(1.0, 0.0, x0, 0.0, -1.0, 0.0)


def _write(path, array, *, crs="EPSG:3857", x0=0.0, nodata=0):
    with rasterio.open(
        path, "w", driver="GTiff",
        height=array.shape[0], width=array.shape[1], count=1,
        dtype=array.dtype, crs=crs, transform=_transform(x0), nodata=nodata,
    ) as dst:
        dst.write(array, 1)
    return path


class TestIsProjected:
    def test_projected_crs_true(self, tmp_path):
        f = _write(tmp_path / "p.tif", np.ones((4, 4), "uint8"), crs="EPSG:3857")
        assert is_projected(f) is True

    def test_geographic_crs_false(self, tmp_path):
        f = _write(tmp_path / "g.tif", np.ones((4, 4), "uint8"), crs="EPSG:4326")
        assert is_projected(f) is False

    def test_accepts_open_dataset(self, tmp_path):
        f = _write(tmp_path / "p.tif", np.ones((4, 4), "uint8"))
        with rasterio.open(f) as ds:
            assert is_projected(ds) is True


class TestRasterFootprint:
    def test_excludes_nodata_border(self, tmp_path):
        # 10x10 all-nodata except a valid 6x6 block (rows/cols 2..8).
        arr = np.zeros((10, 10), "uint8")
        arr[2:8, 2:8] = 1
        f = _write(tmp_path / "block.tif", arr, nodata=0)
        fp = raster_footprint(f)
        # 6x6 valid pixels at 1 unit each → area 36, not the 100 bbox.
        assert fp.area == pytest.approx(36.0)

    def test_all_nodata_returns_none(self, tmp_path):
        f = _write(tmp_path / "empty.tif", np.zeros((5, 5), "uint8"), nodata=0)
        assert raster_footprint(f) is None


class TestFootprintsToGdf:
    def test_one_row_per_source_keyed_by_name(self, tmp_path):
        gpd = pytest.importorskip("geopandas")
        a = _write(tmp_path / "AAA.tif", np.ones((4, 4), "uint8"))
        b = _write(tmp_path / "BBB.tif", np.ones((4, 4), "uint8"))
        gdf = footprints_to_gdf([a, b])
        assert isinstance(gdf, gpd.GeoDataFrame)
        # default id keeps the extension (lossless / format-distinguishing)
        assert list(gdf["id"]) == ["AAA.tif", "BBB.tif"]
        assert gdf.crs == rasterio.crs.CRS.from_string("EPSG:3857")

    def test_duplicate_ids_raise(self, tmp_path):
        pytest.importorskip("geopandas")
        # same filename in two dirs → default .name collides → guard fires
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        a = _write(d1 / "x.tif", np.ones((4, 4), "uint8"))
        b = _write(d2 / "x.tif", np.ones((4, 4), "uint8"))
        with pytest.raises(ValueError, match="duplicate ids"):
            footprints_to_gdf([a, b])
        # disambiguating via id_fn=str (full path) succeeds
        gdf = footprints_to_gdf([a, b], id_fn=str)
        assert len(gdf) == 2

    def test_custom_id_fn(self, tmp_path):
        pytest.importorskip("geopandas")
        a = _write(tmp_path / "AAA.tif", np.ones((4, 4), "uint8"))
        gdf = footprints_to_gdf([a], id_fn=lambda p: "custom")
        assert list(gdf["id"]) == ["custom"]

    def test_crs_mismatch_raises(self, tmp_path):
        pytest.importorskip("geopandas")
        a = _write(tmp_path / "a.tif", np.ones((4, 4), "uint8"), crs="EPSG:3857")
        b = _write(tmp_path / "b.tif", np.ones((4, 4), "uint8"), crs="EPSG:4326")
        with pytest.raises(ValueError, match="one CRS"):
            footprints_to_gdf([a, b])


class TestOverlaps:
    def test_overlapping_pair(self, tmp_path):
        pytest.importorskip("geopandas")
        # Two full 10x10 rasters offset by x0=5 → overlap is x[5,10]×y[0,-10].
        a = _write(tmp_path / "a.tif", np.ones((10, 10), "uint8"), x0=0)
        b = _write(tmp_path / "b.tif", np.ones((10, 10), "uint8"), x0=5)
        gdf = footprints_to_gdf([a, b])
        ov = overlaps(gdf)
        assert len(ov) == 1
        assert set(ov.iloc[0][["id_1", "id_2"]]) == {"a.tif", "b.tif"}
        assert ov.iloc[0]["area"] == pytest.approx(50.0)  # 5 wide × 10 tall

    def test_disjoint_pair_empty(self, tmp_path):
        pytest.importorskip("geopandas")
        a = _write(tmp_path / "a.tif", np.ones((10, 10), "uint8"), x0=0)
        b = _write(tmp_path / "b.tif", np.ones((10, 10), "uint8"), x0=50)
        ov = overlaps(footprints_to_gdf([a, b]))
        assert len(ov) == 0
