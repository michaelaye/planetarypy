"""Tests for ``planetarypy.io`` (the one-call data opener)."""

from pathlib import Path

import pytest

import planetarypy
from planetarypy import io
from planetarypy.catalog._resolver import DownloadedProduct

DATA = Path(__file__).parent / "data"
LABEL = DATA / "test_index.lbl"


def test_open_pds3_label_returns_table():
    d = io.open(LABEL)
    assert "INDEX_TABLE" in d.keys()
    df = d["INDEX_TABLE"]
    assert len(df) == 5  # FILE_RECORDS = 5 in the bundled label


def test_top_level_open_and_read_are_the_io_functions():
    assert planetarypy.open is io.open
    assert planetarypy.read is io.read


def test_read_is_an_alias_for_open():
    assert io.read(LABEL).keys() == io.open(LABEL).keys()


def test_projected_suffix_routes_to_read_image(tmp_path, monkeypatch):
    sentinel = object()
    called = {}

    def fake_read_image(p):
        called["path"] = p
        return sentinel

    monkeypatch.setattr("planetarypy.io.read_image", fake_read_image)
    result = io.open(tmp_path / "scene.tif")
    assert result is sentinel
    assert called["path"] == tmp_path / "scene.tif"


def test_projected_false_forces_generic_reader(monkeypatch):
    # A .tif would normally route to read_image; projected=False forces pdr.
    def boom(p):
        raise AssertionError("read_image must not be called when projected=False")

    monkeypatch.setattr("planetarypy.io.read_image", boom)
    # Point at the real label but with a forced-generic flag; pdr opens it.
    d = io.open(LABEL, projected=False)
    assert "INDEX_TABLE" in d.keys()


def test_downloaded_product_open_uses_label(monkeypatch):
    captured = {}

    def fake_open(target, **kwargs):
        captured["target"] = target
        return "opened"

    monkeypatch.setattr(io, "open", fake_open)
    dp = DownloadedProduct(
        product_id="X",
        local_dir=DATA,
        files=[DATA / "test_index.tab", LABEL],
        label_file=LABEL,
    )
    assert dp.open() == "opened"
    assert captured["target"] == LABEL  # label preferred over the .tab


def test_downloaded_product_open_single_data_file(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        io, "open", lambda target, **k: captured.setdefault("t", target)
    )
    tab = DATA / "test_index.tab"
    dp = DownloadedProduct(
        product_id="X", local_dir=DATA, files=[tab], label_file=None
    )
    dp.open()
    assert captured["t"] == tab


def test_downloaded_product_open_ambiguous_raises():
    dp = DownloadedProduct(
        product_id="X",
        local_dir=DATA,
        files=[DATA / "a.img", DATA / "b.img"],
        label_file=None,
    )
    with pytest.raises(ValueError, match="unambiguous"):
        dp.open()
