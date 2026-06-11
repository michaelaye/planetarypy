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


# ── fetch_psa_product ────────────────────────────────────────────────


def test_fetch_downloads_and_extracts(tmp_path, monkeypatch):
    import planetarypy.utils as utils

    monkeypatch.setattr(psa, "resolve", lambda pid: "https://x/product")
    monkeypatch.setattr(utils, "have_internet", lambda: True)

    def fake_retrieve(url, outfile):
        with zipfile.ZipFile(outfile, "w") as zf:
            zf.writestr("PROD.LBL", "label")
            zf.writestr("PROD.DAT", "data")

    monkeypatch.setattr(utils, "url_retrieve", fake_retrieve)
    paths = psa.fetch_psa_product("PROD", dest=tmp_path)
    assert {p.name for p in paths} == {"PROD.LBL", "PROD.DAT"}
    assert (tmp_path / "PROD.LBL").read_text() == "label"


def test_fetch_no_extract_returns_zip(tmp_path, monkeypatch):
    import planetarypy.utils as utils

    monkeypatch.setattr(psa, "resolve", lambda pid: "https://x/product")
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
    monkeypatch.setattr(psa, "resolve", lambda pid: None)
    with pytest.raises(ValueError, match="No PSA product"):
        psa.fetch_psa_product("PROD", dest=tmp_path)


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
