"""Tests for ``planetarypy.psa`` (ESA PSA TAP resolver).

Deterministic tests mock the TAP layer; one ``slow`` test hits the live PSA.
"""

import zipfile

import pytest

from planetarypy import psa


# ── query parsing ────────────────────────────────────────────────────


def test_query_parses_metadata_and_data(monkeypatch):
    class _Resp:
        def raise_for_status(self):
            pass

        def json(self):
            return {
                "metadata": [{"name": "a"}, {"name": "b"}],
                "data": [[1, 2], [3, 4]],
            }

    monkeypatch.setattr(psa.requests, "get", lambda *a, **k: _Resp())
    assert psa.query("SELECT a,b FROM t") == [{"a": 1, "b": 2}, {"a": 3, "b": 4}]


# ── resolve / resolve_all ────────────────────────────────────────────


def test_resolve_returns_access_url(monkeypatch):
    captured = {}

    def fake_query(adql, **k):
        captured["adql"] = adql
        return [{"granule_uid": "DS:DATA:PID::1.0", "access_url": "https://x/dl"}]

    monkeypatch.setattr(psa, "query", fake_query)
    assert psa.resolve("PID") == "https://x/dl"
    assert "LIKE '%PID%'" in captured["adql"]
    assert "psa.epn_core" in captured["adql"]


def test_resolve_escapes_single_quote(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        psa, "query", lambda adql, **k: captured.update(adql=adql) or []
    )
    psa.resolve("O'Brien")
    assert "O''Brien" in captured["adql"]  # ADQL-escaped


def test_resolve_none_when_no_match(monkeypatch):
    monkeypatch.setattr(psa, "query", lambda adql, **k: [])
    assert psa.resolve("nope") is None


def test_resolve_all_returns_rows(monkeypatch):
    rows = [
        {"granule_uid": "a", "access_url": "u1"},
        {"granule_uid": "b", "access_url": "u2"},
    ]
    monkeypatch.setattr(psa, "query", lambda adql, **k: rows)
    assert psa.resolve_all("x") == rows


# ── missions / instruments ───────────────────────────────────────────


def test_missions_builds_dataframe(monkeypatch):
    captured = {}

    def fake_query(adql, **k):
        captured["adql"] = adql
        return [{"mission": "Rosetta", "products": 10}, {"mission": "MEX", "products": 5}]

    monkeypatch.setattr(psa, "query", fake_query)
    df = psa.missions()
    assert list(df.columns) == ["mission", "catalog", "products"]
    assert df.iloc[0]["mission"] == "Rosetta"
    # best-effort name bridge: PSA "Rosetta" -> catalog code "rosetta"
    assert df.iloc[0]["catalog"] == "rosetta"
    assert "GROUP BY instrument_host_name" in captured["adql"]


def test_instruments_filtered(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        psa, "query",
        lambda adql, **k: captured.update(adql=adql)
        or [{"mission": "Mars Express", "instrument": "HRSC", "products": 3}],
    )
    df = psa.instruments("Mars Express")
    assert list(df.columns) == ["mission", "instrument", "products"]
    assert "LIKE '%Mars Express%'" in captured["adql"]


def test_instruments_unfiltered_has_no_where(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        psa, "query", lambda adql, **k: captured.update(adql=adql) or []
    )
    psa.instruments()
    assert "WHERE" not in captured["adql"]


# ── product_types ────────────────────────────────────────────────────


def test_product_types(monkeypatch):
    captured = {}

    def fake_query(adql, **k):
        captured["adql"] = adql
        return [
            {"granule_gid": "MEX-M-ASPERA3-2-EDR-IMA-EXT4-V1.0:DATA", "products": 59040},
            {"granule_gid": "MEX-M-ASPERA3-4-DDR-IMA-EXT3-V1.0:DATA", "products": 48816},
        ]

    monkeypatch.setattr(psa, "query", fake_query)
    df = psa.product_types("Mars Express", "ASPERA")
    assert list(df.columns) == ["dataset_id", "products"]
    assert df.iloc[0]["dataset_id"] == "MEX-M-ASPERA3-2-EDR-IMA-EXT4-V1.0"
    assert "instrument_host_name LIKE '%Mars Express%'" in captured["adql"]
    assert "instrument_name LIKE '%ASPERA%'" in captured["adql"]
    assert "GROUP BY granule_gid" in captured["adql"]


def test_product_types_no_instrument(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        psa, "query", lambda adql, **k: captured.update(adql=adql) or []
    )
    psa.product_types("Rosetta")
    assert "instrument_name" not in captured["adql"]


# ── examples (catalog key -> DATA_SET_ID -> N products) ──────────────


def test_examples_accepts_dataset_id_directly(monkeypatch):
    # A PSA dataset id (contains '-') must bypass the catalog entirely.
    captured = {}

    def fake_query(adql, **k):
        captured["adql"] = adql
        return [{"granule_uid": "MEX-M-ASPERA3-2-EDR-IMA-EXT4-V1.0:DATA:P1::1.0",
                 "access_url": "https://u"}]

    monkeypatch.setattr(psa, "query", fake_query)
    df = psa.examples("MEX-M-ASPERA3-2-EDR-IMA-EXT4-V1.0", n=1)
    assert "LIKE 'MEX-M-ASPERA3-2-EDR-IMA-EXT4-V1.0:%'" in captured["adql"]
    assert df["product_id"].tolist() == ["P1"]


def test_granule_product_id():
    assert psa._granule_product_id("DS-1:DATA:ELSSCIH_X::21.0") == "ELSSCIH_X"


def test_examples_chains_catalog_to_dataset(monkeypatch):
    import pandas as pd

    import planetarypy.catalog as cat

    monkeypatch.setattr(
        cat, "example_products", lambda key: pd.DataFrame({"product_id": ["SEED1"]})
    )
    monkeypatch.setattr(
        psa, "resolve_all",
        lambda pid, **k: [{"granule_uid": "DS-1:DATA:SEED1::1.0", "access_url": "u"}],
    )
    captured = {}

    def fake_query(adql, **k):
        captured["adql"] = adql
        return [
            {"granule_uid": "DS-1:DATA:P1::1.0", "access_url": "https://a"},
            {"granule_uid": "DS-1:DATA:P2::1.0", "access_url": "https://b"},
        ]

    monkeypatch.setattr(psa, "query", fake_query)
    df = psa.examples("mex.aspera.els_edr_high", n=2)
    assert list(df.columns) == ["product_id", "granule_uid", "access_url"]
    assert df["product_id"].tolist() == ["P1", "P2"]
    assert "LIKE 'DS-1:%'" in captured["adql"]  # derived DATA_SET_ID, no mission name


def test_examples_empty_when_key_unknown(monkeypatch):
    import planetarypy.catalog as cat

    def boom(key):
        raise KeyError("nope")

    monkeypatch.setattr(cat, "example_products", boom)
    df = psa.examples("x.y.z")
    assert df.empty
    assert list(df.columns) == ["product_id", "granule_uid", "access_url"]


# ── fetch_psa_product ────────────────────────────────────────────────


def test_fetch_downloads_and_extracts(tmp_path, monkeypatch):
    import planetarypy.utils as utils

    monkeypatch.setattr(
        psa, "resolve_all",
        lambda pid, **k: [{"granule_uid": f"DS-1:DATA:{pid}::1.0",
                           "access_url": "https://x/product"}],
    )
    monkeypatch.setattr(utils, "have_internet", lambda: True)

    def fake_retrieve(url, outfile):
        with zipfile.ZipFile(outfile, "w") as zf:
            zf.writestr("PROD.LBL", "label")
            zf.writestr("PROD.DAT", "data")

    monkeypatch.setattr(utils, "url_retrieve", fake_retrieve)
    paths = psa.fetch_psa_product("PROD", dest=tmp_path)
    assert {p.name for p in paths} == {"PROD.LBL", "PROD.DAT"}
    assert (tmp_path / "PROD.LBL").read_text() == "label"


def test_fetch_with_key_uses_catalog_layout(tmp_path, monkeypatch):
    import planetarypy.catalog as cat
    import planetarypy.utils as utils

    monkeypatch.setattr(
        psa, "resolve_all",
        lambda pid, **k: [{"granule_uid": f"DS-1:DATA:{pid}::1.0",
                           "access_url": "https://x/product"}],
    )
    monkeypatch.setattr(utils, "have_internet", lambda: True)
    monkeypatch.setattr(
        utils, "url_retrieve",
        lambda url, outfile: zipfile.ZipFile(outfile, "w").close(),
    )
    captured = {}

    def fake_default_dir(mission, instrument, product_type, product_id):
        captured["args"] = (mission, instrument, product_type, product_id)
        return tmp_path / mission / instrument / product_type / product_id

    monkeypatch.setattr(cat, "default_product_dir", fake_default_dir)
    psa.fetch_psa_product("PID", key="mex.hrsc.refdr3", extract=False)
    assert captured["args"] == ("mex", "hrsc", "refdr3", "PID")
    assert (tmp_path / "mex" / "hrsc" / "refdr3" / "PID").is_dir()


def test_fetch_no_extract_returns_zip(tmp_path, monkeypatch):
    import planetarypy.utils as utils

    monkeypatch.setattr(
        psa, "resolve_all",
        lambda pid, **k: [{"granule_uid": f"DS-1:DATA:{pid}::1.0",
                           "access_url": "https://x/product"}],
    )
    monkeypatch.setattr(utils, "have_internet", lambda: True)
    monkeypatch.setattr(
        utils, "url_retrieve",
        lambda url, outfile: zipfile.ZipFile(outfile, "w").close(),
    )
    paths = psa.fetch_psa_product("PROD", dest=tmp_path, extract=False)
    assert len(paths) == 1 and paths[0].suffix == ".zip"


def test_fetch_not_found_raises(tmp_path, monkeypatch):
    import planetarypy.utils as utils

    monkeypatch.setattr(utils, "have_internet", lambda: True)
    monkeypatch.setattr(psa, "resolve_all", lambda pid, **k: [])
    with pytest.raises(ValueError, match="No PSA product"):
        psa.fetch_psa_product("PROD", dest=tmp_path)


def test_fetch_default_path_groups_by_dataset(tmp_path, monkeypatch):
    from planetarypy.config import config
    import planetarypy.utils as utils

    monkeypatch.setattr(
        psa, "resolve_all",
        lambda pid, **k: [{"granule_uid": "MEX-DS-1:DATA:PROD::1.0",
                           "access_url": "u"}],
    )
    monkeypatch.setattr(utils, "have_internet", lambda: True)
    monkeypatch.setattr(config, "storage_root", tmp_path)
    monkeypatch.setattr(
        utils, "url_retrieve",
        lambda url, outfile: zipfile.ZipFile(outfile, "w").close(),
    )
    psa.fetch_psa_product("PROD", extract=False)  # no dest, no key
    assert (tmp_path / "psa" / "MEX-DS-1" / "PROD").is_dir()


def test_fetch_offline_raises(monkeypatch):
    from planetarypy.catalog import OfflineError
    import planetarypy.utils as utils

    monkeypatch.setattr(utils, "have_internet", lambda: False)
    with pytest.raises(OfflineError):
        psa.fetch_psa_product("PROD")


# ── live ─────────────────────────────────────────────────────────────


@pytest.mark.slow
def test_live_resolve_mars_express():
    url = psa.resolve("IMA_AZ1120140041730C_ACCS01")
    assert url and url.startswith("https://")
