"""Tests for the fetch fast-path work:

* ``get_index`` in-process memoization + ``clear_index_cache`` (#1)
* ``DownloadedProduct.file_urls`` carried from the single resolution (#2)
* ``StorageContext`` resolver contract + ``ResolvedProduct.meta`` reuse so a
  storage resolver needn't re-read the index for a value already resolved (#3)

All deterministic — ``Index`` is stubbed, no disk or network.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from planetarypy.pds import clear_index_cache, get_index


def _stub_index(df: pd.DataFrame, counter: dict):
    """Return an ``Index`` stand-in that counts how often it's constructed."""
    class _I:
        def __init__(self, *a, **kw):
            counter["n"] = counter.get("n", 0) + 1
        def ensure_parquet(self, force=False):
            return False
        def download(self):
            pass
        @property
        def update_available(self):
            return False
        @property
        def dataframe(self):
            return df
    return _I


# ── #1 memoization ──────────────────────────────────────────────────────


class TestGetIndexMemoization:

    def test_second_call_does_not_reconstruct_index(self, monkeypatch):
        counter: dict = {}
        df = pd.DataFrame({"PRODUCT_ID": ["A", "B"], "VOLUME_ID": ["v1", "v2"]})
        monkeypatch.setattr("planetarypy.pds.Index", _stub_index(df, counter))

        get_index("demo.cam.edr")
        get_index("demo.cam.edr")
        get_index("demo.cam.edr")
        assert counter["n"] == 1  # loaded once, served from cache after

    def test_clear_forces_reload(self, monkeypatch):
        counter: dict = {}
        df = pd.DataFrame({"PRODUCT_ID": ["A"]})
        monkeypatch.setattr("planetarypy.pds.Index", _stub_index(df, counter))

        get_index("demo.cam.edr")
        clear_index_cache("demo.cam.edr")
        get_index("demo.cam.edr")
        assert counter["n"] == 2

    def test_returned_frame_is_isolated_from_cache(self, monkeypatch):
        counter: dict = {}
        df = pd.DataFrame({"PRODUCT_ID": ["A", "B"]})
        monkeypatch.setattr("planetarypy.pds.Index", _stub_index(df, counter))

        first = get_index("demo.cam.edr")
        first["EXTRA"] = 1  # in-place mutation like get_edr_index does
        assert "EXTRA" not in get_index("demo.cam.edr").columns

    def test_refresh_flag_bypasses_and_refreshes_cache(self, monkeypatch):
        counter: dict = {}
        df = pd.DataFrame({"PRODUCT_ID": ["A"]})
        monkeypatch.setattr("planetarypy.pds.Index", _stub_index(df, counter))

        get_index("demo.cam.edr")                       # populate
        get_index("demo.cam.edr", force_refresh=True)   # must reload
        assert counter["n"] == 2

    def test_columns_filter_applies_to_cached_frame(self, monkeypatch):
        counter: dict = {}
        df = pd.DataFrame({"PRODUCT_ID": ["A"], "VOLUME_ID": ["v1"]})
        monkeypatch.setattr("planetarypy.pds.Index", _stub_index(df, counter))

        get_index("demo.cam.edr")  # warm cache
        out = get_index("demo.cam.edr", columns=["PRODUCT_ID"])
        assert list(out.columns) == ["PRODUCT_ID"]
        assert counter["n"] == 1


# ── #3 StorageContext + meta ────────────────────────────────────────────


class TestStorageContext:

    def test_context_resolver_receives_resolved(self, monkeypatch):
        from planetarypy.catalog import _resolver
        from planetarypy.catalog._resolver import (
            ResolvedProduct,
            _local_product_dir,
            register_storage_resolver,
        )
        monkeypatch.setattr(
            _resolver, "_STORAGE_RESOLVERS", dict(_resolver._STORAGE_RESOLVERS)
        )
        seen = {}

        def resolver(ctx):
            seen["ctx"] = ctx
            return Path("/x") / ctx.product_type / ctx.product_id

        register_storage_resolver("demo.cam", resolver)
        rp = ResolvedProduct(
            product_id="PID", url_stem="http://h/vol", files=["PID.IMG"],
            label_file=None, source="index", meta={"VOLUME_ID": "vol"},
        )
        out = _local_product_dir("demo", "cam", "edr", "PID", resolved=rp)
        assert out == Path("/x/edr/PID")
        assert seen["ctx"].resolved is rp
        assert seen["ctx"].mission == "demo"

    def test_legacy_two_arg_resolver_still_works(self, monkeypatch):
        from planetarypy.catalog import _resolver
        from planetarypy.catalog._resolver import (
            _local_product_dir,
            register_storage_resolver,
        )
        monkeypatch.setattr(
            _resolver, "_STORAGE_RESOLVERS", dict(_resolver._STORAGE_RESOLVERS)
        )
        register_storage_resolver("demo.cam", lambda pt, pid: Path("/y") / pt / pid)
        out = _local_product_dir("demo", "cam", "edr", "PID")
        assert out == Path("/y/edr/PID")

    def test_resolved_product_meta_defaults_none(self):
        from planetarypy.catalog._resolver import ResolvedProduct
        rp = ResolvedProduct(
            product_id="P", url_stem="http://h", files=["P.IMG"],
            label_file=None, source="catalog",
        )
        assert rp.meta is None

    def test_downloaded_product_file_urls_defaults_empty(self):
        from planetarypy.catalog._resolver import DownloadedProduct
        dp = DownloadedProduct(product_id="P", local_dir=Path("/d"), files=[])
        assert dp.file_urls == {}


# ── #3 CTX volume reuse ─────────────────────────────────────────────────


class TestCtxVolumeReuse:

    def test_volume_read_from_meta_without_index_load(self, monkeypatch):
        ctx_edr = pytest.importorskip(
            "planetarypy.instruments.mro.ctx.ctx_edr"
        )
        from planetarypy.catalog._resolver import ResolvedProduct, StorageContext

        # Any index load must blow up, proving the meta path avoids it.
        def _boom(*a, **k):
            raise AssertionError("get_edr_index must not be called")
        monkeypatch.setattr(ctx_edr, "get_edr_index", _boom)

        rp = ResolvedProduct(
            product_id="D12_031858_1876_XN_07N286W",
            url_stem="https://x/mro/ctx/mrox_1828/data",
            files=["D12_031858_1876_XN_07N286W.IMG"],
            label_file=None, source="index",
            meta={"VOLUME_ID": "MROX_1828"},
        )
        ctx = StorageContext(
            mission="mro", instrument="ctx", product_type="edr",
            product_id=rp.product_id, resolved=rp,
        )
        out = ctx_edr._ctx_local_product_dir(ctx)
        assert out.parts[-2:] == ("mrox_1828", "D12_031858_1876_XN_07N286W")

    def test_volume_from_resolved_helper(self):
        ctx_edr = pytest.importorskip(
            "planetarypy.instruments.mro.ctx.ctx_edr"
        )
        from planetarypy.catalog._resolver import ResolvedProduct

        assert ctx_edr._volume_from_resolved(None) is None
        rp_no_meta = ResolvedProduct(
            product_id="P", url_stem="http://h", files=["P.IMG"],
            label_file=None, source="catalog",
        )
        assert ctx_edr._volume_from_resolved(rp_no_meta) is None
        rp = ResolvedProduct(
            product_id="P", url_stem="http://h", files=["P.IMG"],
            label_file=None, source="index", meta={"VOLUME_ID": " MROX_1828 "},
        )
        assert ctx_edr._volume_from_resolved(rp) == "mrox_1828"
