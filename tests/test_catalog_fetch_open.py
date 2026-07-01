"""Tests for ``fetch_product(..., open=True)`` — download-and-open wiring.

Deterministic: the resolver/downloader layer is mocked so no network is
touched, but the *opening* exercises the real reader on the bundled PDS3
label fixture.
"""

from pathlib import Path
from types import SimpleNamespace

from planetarypy.catalog import fetch_product
from planetarypy.catalog._resolver import DownloadedProduct

DATA = Path(__file__).parent / "data"
LABEL = DATA / "test_index.lbl"


def _patch_resolver(monkeypatch):
    """Make fetch_product resolve to the bundled label without any network."""
    resolved = SimpleNamespace(
        product_id="TEST_PID", label_file=LABEL.name, file_urls={}
    )
    monkeypatch.setattr(
        "planetarypy.catalog._resolver.resolve_product",
        lambda *a, **k: resolved,
    )
    monkeypatch.setattr(
        "planetarypy.catalog._resolver._local_product_dir",
        lambda *a, **k: DATA,
    )
    monkeypatch.setattr(
        "planetarypy.catalog._resolver.download_product",
        lambda *a, **k: [LABEL],
    )


def test_fetch_product_default_returns_downloaded_product(monkeypatch):
    _patch_resolver(monkeypatch)
    r = fetch_product("mission.instr.type", "TEST_PID")
    assert isinstance(r, DownloadedProduct)
    assert r.label_file == LABEL


def test_fetch_product_open_returns_opened_object(monkeypatch):
    _patch_resolver(monkeypatch)
    d = fetch_product("mission.instr.type", "TEST_PID", open=True)
    assert not isinstance(d, DownloadedProduct)
    assert "INDEX_TABLE" in d.keys()
    assert len(d["INDEX_TABLE"]) == 5
