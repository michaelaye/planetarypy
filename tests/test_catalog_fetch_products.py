"""Tests for ``planetarypy.catalog.fetch_products`` batch API."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

import planetarypy.catalog as catalog_mod
from planetarypy.catalog import (
    BatchFetchResult,
    OfflineError,
    fetch_products,
)


def _fake_downloaded(pid: str) -> MagicMock:
    """Stand-in for a DownloadedProduct."""
    dp = MagicMock()
    dp.product_id = pid
    dp.local_dir = Path(f"/tmp/{pid}")
    dp.files = [Path(f"/tmp/{pid}/{pid}.IMG")]
    dp.label_file = None
    return dp


@pytest.fixture
def online(monkeypatch):
    """Assume the preflight check passes — most tests want this."""
    monkeypatch.setattr(catalog_mod, "have_internet", lambda: True, raising=False)
    # have_internet is imported lazily from utils inside fetch_products, so
    # also patch at the source.
    import planetarypy.utils as utils_mod
    monkeypatch.setattr(utils_mod, "have_internet", lambda: True)


class TestFetchProductsHappyPath:

    def test_all_success_preserves_input_order(self, monkeypatch, online):
        monkeypatch.setattr(
            catalog_mod, "fetch_product",
            lambda key, pid, **kw: _fake_downloaded(pid),
        )
        out = fetch_products("mro.ctx.edr", ["P_A", "P_B", "P_C"], workers=2)
        assert [r.product_id for r in out] == ["P_A", "P_B", "P_C"]
        assert all(r.ok for r in out)
        assert all(r.exception is None for r in out)
        assert [r.downloaded.product_id for r in out] == ["P_A", "P_B", "P_C"]

    def test_returns_BatchFetchResult_instances(self, monkeypatch, online):
        monkeypatch.setattr(
            catalog_mod, "fetch_product",
            lambda key, pid, **kw: _fake_downloaded(pid),
        )
        out = fetch_products("mro.ctx.edr", ["P_A"], workers=1)
        assert isinstance(out[0], BatchFetchResult)
        assert out[0].ok is True

    def test_empty_pid_list_returns_empty_list_no_preflight(self, monkeypatch):
        # Empty input should NOT trigger have_internet — fast no-op.
        called = {"have_internet": 0}
        import planetarypy.utils as utils_mod
        def _spy():
            called["have_internet"] += 1
            return False
        monkeypatch.setattr(utils_mod, "have_internet", _spy)
        out = fetch_products("mro.ctx.edr", [], workers=4)
        assert out == []
        assert called["have_internet"] == 0


class TestFetchProductsPartialFailure:

    def test_one_failure_does_not_break_batch(self, monkeypatch, online):
        def _stub(key, pid, **kw):
            if pid == "BAD":
                raise RuntimeError(f"resolver couldn't find {pid}")
            return _fake_downloaded(pid)
        monkeypatch.setattr(catalog_mod, "fetch_product", _stub)

        out = fetch_products("mro.ctx.edr", ["P_A", "BAD", "P_C"], workers=2)
        assert [r.product_id for r in out] == ["P_A", "BAD", "P_C"]
        assert [r.ok for r in out] == [True, False, True]
        assert isinstance(out[1].exception, RuntimeError)
        assert "couldn't find BAD" in str(out[1].exception)
        assert out[1].downloaded is None
        # The other two are unaffected:
        assert out[0].downloaded.product_id == "P_A"
        assert out[2].downloaded.product_id == "P_C"

    def test_all_failure_returns_all_failed_results(self, monkeypatch, online):
        def _always_fail(key, pid, **kw):
            raise IOError(f"network bonk on {pid}")
        monkeypatch.setattr(catalog_mod, "fetch_product", _always_fail)

        out = fetch_products("mro.ctx.edr", ["A", "B", "C"], workers=2)
        assert [r.ok for r in out] == [False, False, False]
        assert all(isinstance(r.exception, IOError) for r in out)


class TestFetchProductsOfflinePreflight:

    def test_offline_raises_OfflineError_before_pool_launches(self, monkeypatch):
        import planetarypy.utils as utils_mod
        monkeypatch.setattr(utils_mod, "have_internet", lambda: False)

        # If have_internet is honored, fetch_product is never called.
        called = {"n": 0}
        def _never_called(key, pid, **kw):
            called["n"] += 1
            return _fake_downloaded(pid)
        monkeypatch.setattr(catalog_mod, "fetch_product", _never_called)

        with pytest.raises(OfflineError, match="No internet connection"):
            fetch_products("mro.ctx.edr", ["P_A", "P_B"])
        assert called["n"] == 0

    def test_skip_online_check_bypasses_preflight(self, monkeypatch):
        import planetarypy.utils as utils_mod
        monkeypatch.setattr(utils_mod, "have_internet", lambda: False)
        monkeypatch.setattr(
            catalog_mod, "fetch_product",
            lambda key, pid, **kw: _fake_downloaded(pid),
        )

        out = fetch_products(
            "mro.ctx.edr", ["P_A"], skip_online_check=True,
        )
        assert out[0].ok is True


class TestFetchProductsArgumentForwarding:

    def test_label_only_force_passed_through(self, monkeypatch, online):
        seen_kwargs = []
        def _capture(key, pid, **kw):
            seen_kwargs.append(kw)
            return _fake_downloaded(pid)
        monkeypatch.setattr(catalog_mod, "fetch_product", _capture)

        fetch_products(
            "mro.ctx.edr", ["P_A"],
            label_only=True, force=True, workers=1,
        )
        assert seen_kwargs[0]["label_only"] is True
        assert seen_kwargs[0]["force"] is True
