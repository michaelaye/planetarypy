"""Tests for the public instrument-extension seam.

These are the registration hooks external instrument packages plug into:
``register_index``, ``register_storage_resolver`` + ``default_product_dir``,
and ``register_meta_handler``.
"""

import pytest

from planetarypy.catalog import (
    IndexConfig,
    default_product_dir,
    register_index,
    register_storage_resolver,
)
from planetarypy.catalog import _index_resolver, _resolver
from planetarypy.pds import register_meta_handler
from planetarypy.pds import meta_display


@pytest.fixture
def clean_registries(monkeypatch):
    """Snapshot/restore the global registries so tests don't leak entries."""
    monkeypatch.setattr(
        _index_resolver, "INDEX_REGISTRY", dict(_index_resolver.INDEX_REGISTRY)
    )
    monkeypatch.setattr(
        _resolver, "_STORAGE_RESOLVERS", dict(_resolver._STORAGE_RESOLVERS)
    )
    monkeypatch.setattr(
        meta_display, "_META_HANDLERS", dict(meta_display._META_HANDLERS)
    )


def test_register_index_roundtrips(clean_registries):
    assert not _index_resolver.has_index("demo", "cam", "edr")
    cfg = IndexConfig(index_key="demo.cam.edr", archive_url="https://x/demo")
    register_index("demo", "cam", "edr", cfg)
    assert _index_resolver.has_index("demo", "cam", "edr")
    assert _index_resolver.get_index_config("demo", "cam", "edr") is cfg


def test_default_product_dir_layout():
    p = default_product_dir("mro", "ctx", "edr", "J03_001/bad")
    # {storage_root}/mro/ctx/edr/<sanitized-pid>
    assert p.parts[-4:] == ("mro", "ctx", "edr", "J03_001_bad")


def test_register_storage_resolver_overrides_default(clean_registries, tmp_path):
    from planetarypy.catalog._resolver import _local_product_dir

    # No resolver → default layout
    d = _local_product_dir("demo", "cam", "edr", "PID1")
    assert d.parts[-4:] == ("demo", "cam", "edr", "PID1")

    # Registered resolver wins
    register_storage_resolver("demo.cam", lambda pt, pid: tmp_path / pt / pid)
    d2 = _local_product_dir("demo", "cam", "edr", "PID1")
    assert d2 == tmp_path / "edr" / "PID1"


def test_register_meta_handler_precedence(clean_registries):
    assert meta_display.get_handler("demo.cam.edr") is None
    sentinel = lambda *a, **k: "HANDLED"  # noqa: E731
    register_meta_handler("demo.cam.edr", sentinel)
    assert meta_display.get_handler("demo.cam.edr") is sentinel
