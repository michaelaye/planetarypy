"""Tests for ``planetarypy.search`` (PDS registry wrapper).

Deterministic tests mock the ``pds.api-client`` layer so they run without the
extra installed or any network. One ``slow`` test hits the live registry.
"""

import sys
from pathlib import Path

import pytest

from planetarypy import search


# ── fakes for the pds.api-client layer ───────────────────────────────


class _FakeProduct:
    def __init__(self, id, properties):
        self.id = id
        self.properties = properties


class _FakeResp:
    def __init__(self, data):
        self.data = data


class _FakeApi:
    def __init__(self, products, capture):
        self._products = products
        self._capture = capture

    def product_list(self, q=None, fields=None, limit=100):
        self._capture.update(q=q, fields=fields, limit=limit)
        return _FakeResp(self._products[:limit])


@pytest.fixture
def fake_api(monkeypatch):
    capture = {}
    products = [
        _FakeProduct(
            "urn:nasa:pds:demo:p1::1.0",
            {
                "pds:Identification_Area.pds:title": ["First"],
                "ops:Data_File_Info.ops:file_ref": ["https://x/p1.img"],
                "ops:Label_File_Info.ops:file_ref": ["https://x/p1.xml"],
                "empty": [],
                "multi": ["a", "b"],
            },
        ),
        _FakeProduct(
            "urn:nasa:pds:demo:p2::1.0",
            {
                "pds:Identification_Area.pds:title": ["Second"],
                "ops:Data_File_Info.ops:file_ref": ["null"],
            },
        ),
    ]
    monkeypatch.setattr(
        search, "_api",
        lambda host=search._DEFAULT_HOST: _FakeApi(products, capture),
    )
    return capture


# ── _build_q ─────────────────────────────────────────────────────────


def test_build_q_combines_filters():
    q = search._build_q(
        target="urn:t", instrument=None, instrument_host=None,
        investigation=None, processing_level="raw", before=None, after=None,
        observationals=True, lidvid=None, query=None,
    )
    assert 'ref_lid_target eq "urn:t"' in q
    assert 'pds:Primary_Result_Summary.pds:processing_level eq "Raw"' in q
    assert 'product_class eq "Product_Observational"' in q
    assert " and " in q


def test_build_q_lidvid_vs_lid():
    assert 'lidvid eq "a::1.0"' == search._build_q(
        target=None, instrument=None, instrument_host=None, investigation=None,
        processing_level=None, before=None, after=None, observationals=False,
        lidvid="a::1.0", query=None,
    )
    assert 'lid eq "a"' == search._build_q(
        target=None, instrument=None, instrument_host=None, investigation=None,
        processing_level=None, before=None, after=None, observationals=False,
        lidvid="a", query=None,
    )


def test_build_q_empty_is_none():
    assert search._build_q(
        target=None, instrument=None, instrument_host=None, investigation=None,
        processing_level=None, before=None, after=None, observationals=False,
        lidvid=None, query=None,
    ) is None


# ── _flatten ─────────────────────────────────────────────────────────


def test_flatten():
    assert search._flatten([]) is None       # empty → None (peppi's crash case)
    assert search._flatten(["x"]) == "x"      # single → scalar
    assert search._flatten(["a", "b"]) == ["a", "b"]
    assert search._flatten("scalar") == "scalar"


# ── search_products ──────────────────────────────────────────────────


def test_search_products_dataframe(fake_api):
    import pandas as pd

    df = search.search_products(query='lid like "urn:nasa:pds:demo*"', limit=10)
    assert list(df.index) == ["urn:nasa:pds:demo:p1::1.0", "urn:nasa:pds:demo:p2::1.0"]
    assert df.index.name == "lidvid"
    # single-element lists flattened to scalars; empty → missing; multi kept
    p1 = df.loc["urn:nasa:pds:demo:p1::1.0"]
    assert p1["pds:Identification_Area.pds:title"] == "First"
    assert pd.isna(p1["empty"])
    assert p1["multi"] == ["a", "b"]
    assert fake_api["q"] == 'lid like "urn:nasa:pds:demo*"'
    assert fake_api["limit"] == 10


# ── product_file_urls ────────────────────────────────────────────────


def test_product_file_urls_skips_null_and_takes_both():
    props = {
        "ops:Data_File_Info.ops:file_ref": ["https://x/a.img"],
        "ops:Label_File_Info.ops:file_ref": ["https://x/a.xml"],
    }
    assert search.product_file_urls(props) == ["https://x/a.img", "https://x/a.xml"]
    assert search.product_file_urls({"ops:Data_File_Info.ops:file_ref": ["null"]}) == []
    assert search.product_file_urls({}) == []


# ── fetch_pds_product ────────────────────────────────────────────────


def test_fetch_pds_product_downloads(tmp_path, monkeypatch):
    monkeypatch.setattr(search, "get_product",
                        lambda lidvid, host=search._DEFAULT_HOST: {
                            "ops:Data_File_Info.ops:file_ref": ["https://x/a.img"],
                            "ops:Label_File_Info.ops:file_ref": ["https://x/a.xml"],
                        })
    import planetarypy.utils as utils
    monkeypatch.setattr(utils, "have_internet", lambda: True)
    calls = []

    def fake_retrieve(url, outfile):
        calls.append(url)
        Path(outfile).write_text("x")

    monkeypatch.setattr(utils, "url_retrieve", fake_retrieve)
    paths = search.fetch_pds_product("urn:nasa:pds:demo:p1::1.0", dest=tmp_path)
    assert len(paths) == 2 and len(calls) == 2
    assert {p.name for p in paths} == {"a.img", "a.xml"}
    # second call: files exist → no re-download
    calls.clear()
    search.fetch_pds_product("urn:nasa:pds:demo:p1::1.0", dest=tmp_path)
    assert calls == []


def test_fetch_pds_product_offline_raises(monkeypatch):
    from planetarypy.catalog import OfflineError
    import planetarypy.utils as utils
    monkeypatch.setattr(utils, "have_internet", lambda: False)
    with pytest.raises(OfflineError):
        search.fetch_pds_product("urn:nasa:pds:demo:p1::1.0")


# ── lazy-extra guard ─────────────────────────────────────────────────


def test_api_missing_extra_message(monkeypatch):
    # Simulate the extra not being installed.
    monkeypatch.setitem(sys.modules, "pds.api_client", None)
    with pytest.raises(ImportError, match="search"):
        search._api()


# ── live ─────────────────────────────────────────────────────────────


@pytest.mark.slow
def test_live_registry_query():
    pytest.importorskip("pds.api_client")
    df = search.search_products(
        query='lid like "urn:nasa:pds:cassini_iss_saturn*"',
        fields=["ops:Data_File_Info.ops:file_ref"],
        limit=2,
    )
    assert not df.empty
    urls = search.product_file_urls(
        {"ops:Data_File_Info.ops:file_ref": [df.iloc[0, 0]]}
    )
    assert urls and urls[0].startswith("https://")
