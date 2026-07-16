"""Tests for planetarypy.datasets (body-namespaced remote rasters).

Registry/namespace logic is offline; the live windowed COG read is ``slow``.
The module must import without rasterio/rioxarray (they're lazy in read_window),
so these registry tests have no optional-dep guard.
"""

import pytest

from planetarypy import datasets


def test_bodies_and_list():
    assert "mars" in datasets.bodies()
    keys = [r.key for r in datasets.list_datasets("mars")]
    assert "dlr.mars.hrsc.level3_eqc" in keys
    assert datasets.list_datasets("nosuchbody") == []


def test_body_namespace_access():
    r = datasets.mars.hrsc_level3
    assert r.body == "mars"
    assert r.crs == "IAU_2015:49910"
    assert r.nodata == -32768.0
    assert r.vsicurl.startswith("/vsicurl/https://")


def test_namespace_unknown_dataset_errors():
    with pytest.raises(AttributeError, match="no dataset"):
        datasets.mars.not_a_dataset


def test_unknown_body_errors():
    with pytest.raises(AttributeError):
        datasets.pluto  # no registered raster for pluto


@pytest.mark.slow
def test_read_window_live(tmp_path):
    pytest.importorskip("rioxarray")
    out = tmp_path / "patch.tif"
    da = datasets.mars.hrsc_level3.read_window(0, 0, 1.0, out=out)
    assert da.shape == (1, 1185, 1185)  # 1° at 50 m/px
    assert da.rio.crs is not None
    assert out.exists()


def test_box_lonlat_anchors():
    from planetarypy.datasets import _box_lonlat
    assert _box_lonlat(0, 0, 1.0, "center") == (-0.5, -0.5, 0.5, 0.5)
    assert _box_lonlat(0, 0, 1.0, "lower-left") == (0, 0, 1, 1)     # SW corner
    assert _box_lonlat(0, 0, 1.0, "sw") == (0, 0, 1, 1)
    assert _box_lonlat(0, 0, 1.0, "upper-left") == (0, -1, 1, 0)    # NW corner
    assert _box_lonlat(0, 0, 1.0, "ne") == (-1, -1, 0, 0)
    import pytest
    with pytest.raises(ValueError, match="anchor must be"):
        _box_lonlat(0, 0, 1.0, "middle")


# ── STAC-backed kind ─────────────────────────────────────────────────


def test_stac_collection_registered():
    coll = datasets.mars.themis_mosaics
    assert coll.kind == "stac"
    assert coll.collection == "mo_themis_controlled_mosaics"
    assert "moon" in datasets.bodies()       # usgs.moon.lola.dtms


def test_pick_cog_asset():
    from planetarypy.datasets import _pick_cog_asset
    cog = {"image": {"href": "a.tif",
                     "type": "image/tiff; application=geotiff; profile=cloud-optimized"}}
    assert _pick_cog_asset(cog, None) == "a.tif"
    assert _pick_cog_asset({"dem": {"href": "d.tif", "type": "x"}}, "dem") == "d.tif"
    assert _pick_cog_asset({"t": {"href": "t.png", "type": "image/png"}}, None) is None


def test_as_source():
    from planetarypy.datasets import _as_source, _UrlSource
    r = datasets.mars.hrsc_level3
    assert _as_source(r) is r
    assert isinstance(_as_source("https://x/y.tif"), _UrlSource)
    assert _as_source("dlr.mars.hrsc.level3_eqc").key == "dlr.mars.hrsc.level3_eqc"
    with pytest.raises(KeyError):
        _as_source("not_a_key_or_url")


def test_stac_search_mocked(monkeypatch):
    import requests

    captured = {}

    class FakeResp:
        def raise_for_status(self):
            pass

        def json(self):
            return {"features": [{
                "id": "x", "bbox": [1, 2, 3, 4],
                "properties": {"datetime": "2020-01-01"},
                "assets": {"image": {
                    "href": "https://s3/x.tif",
                    "type": "image/tiff; application=geotiff; profile=cloud-optimized"}},
            }]}

    def fake_get(url, params=None, timeout=None):
        captured["url"] = url
        captured["params"] = params
        return FakeResp()

    monkeypatch.setattr(requests, "get", fake_get)
    items = datasets.mars.themis_mosaics.at(10.0, -20.0)
    assert captured["url"].endswith("/search")
    assert captured["params"]["collections"] == "mo_themis_controlled_mosaics"
    w, s, e, n = (float(v) for v in captured["params"]["bbox"].split(","))
    assert w < 10.0 < e and s < -20.0 < n      # point expanded to non-zero bbox
    assert len(items) == 1
    assert items[0].cog_url == "https://s3/x.tif"
    assert items[0].vsicurl == "/vsicurl/https://s3/x.tif"


def test_stac_items_unpack_mocked(monkeypatch):
    import requests

    captured = {}

    class FakeResp:
        def raise_for_status(self): pass
        def json(self):
            return {"features": [
                {"id": "mc28_hellas_dir", "bbox": [1, 2, 3, 4],
                 "assets": {"image": {"href": "https://s3/mc28.tif",
                     "type": "image/tiff; application=geotiff; profile=cloud-optimized"}}},
                {"id": "no_cog", "assets": {"meta": {"href": "x.json", "type": "application/json"}}},
            ]}

    def fake_get(url, params=None, timeout=None):
        captured["url"] = url; captured["params"] = params
        return FakeResp()

    monkeypatch.setattr(requests, "get", fake_get)
    items = datasets.mars.themis_mosaics.items(limit=5)
    assert captured["url"].endswith("/collections/mo_themis_controlled_mosaics/items")
    assert captured["params"]["limit"] == 5
    assert [it.id for it in items] == ["mc28_hellas_dir"]   # non-COG item dropped
    assert items[0].cog_url == "https://s3/mc28.tif"


def test_stac_collections_discover_mocked(monkeypatch):
    import requests

    captured = {}

    class FakeResp:
        def raise_for_status(self): pass
        def json(self):
            return {"collections": [
                {"id": "lunar_orbiter_laser_altimeter", "title": "LOLA", "description": "  shots  "},
                {"id": "mro_ctx_controlled_usgs_dtms", "title": "CTX DTMs"},
            ]}

    def fake_get(url, timeout=None):
        captured["url"] = url
        return FakeResp()

    monkeypatch.setattr(requests, "get", fake_get)
    cols = datasets.stac_collections("https://stac.astrogeology.usgs.gov/api")
    assert captured["url"].endswith("/collections")
    assert [c["id"] for c in cols] == ["lunar_orbiter_laser_altimeter", "mro_ctx_controlled_usgs_dtms"]
    assert cols[0]["description"] == "shots"          # stripped
    assert cols[1]["description"] == ""               # missing -> empty


@pytest.mark.slow
def test_stac_live_read():
    pytest.importorskip("rioxarray")
    items = datasets.mars.themis_mosaics.at(-90, -30, limit=1)
    assert items
    da = items[0].read_window(-90, -30, 0.3)
    assert da.rio.crs is not None


# ── browse() (COG-viewer launcher) — offline; never opens a browser ──────────


@pytest.fixture
def no_browser(monkeypatch):
    """Fail loudly if browse() tries to open a real browser during a test."""
    import webbrowser

    def boom(*a, **k):
        raise AssertionError("browse() opened a browser with open_browser=False")

    monkeypatch.setattr(webbrowser, "open", boom)


def test_browse_default_and_preset(no_browser):
    assert datasets.browse(open_browser=False) == datasets.VIEWER_URL
    url = datasets.browse("robbins_spole", open_browser=False)
    assert url.startswith(datasets.VIEWER_URL + "?")
    assert "preset=robbins_spole" in url


def test_browse_registry_generates_proj4(no_browser):
    # RemoteRaster with an IAU code -> proj4 generated by pyproj (no hand-written CRS).
    from urllib.parse import parse_qs, urlsplit

    q = parse_qs(urlsplit(datasets.mars.hrsc_level3.browse(open_browser=False)).query)
    assert q["cog"][0].endswith("level3-iau-eqc.tif")
    assert "+proj=eqc" in q["crs"][0] and "+R=3396190" in q["crs"][0]
    assert q["nodata"][0] == "-32768.0"


def test_browse_stac_collection_needs_point(no_browser):
    with pytest.raises(ValueError, match="lon= and lat="):
        datasets.browse(datasets.mars.themis_mosaics, open_browser=False)


def test_viewer_proj4_from_iau_code():
    from planetarypy.datasets import _viewer_proj4

    class _Src:
        crs = "IAU_2015:49910"

    p4 = _viewer_proj4(_Src())
    assert "+proj=eqc" in p4 and "+R=3396190" in p4
