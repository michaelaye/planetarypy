"""Tests for the catalog resolver module."""

import json
import pytest
import duckdb

from planetarypy.catalog._resolver import (
    ResolvedProduct,
    resolve_product,
    _local_product_dir,
    ProductNotFoundError,
)
from planetarypy.catalog._schema import (
    create_schema,
    insert_instrument,
    insert_product_type,
    insert_product,
)


@pytest.fixture
def catalog_con(tmp_path, monkeypatch):
    """Create an in-memory catalog DB and patch get_catalog to use it."""
    con = duckdb.connect(":memory:")
    create_schema(con)

    # Insert test data
    insert_instrument(con, "test_instr", "test_mission", "test_instr", "auto", 1)
    insert_product_type(con, "test_instr", "edr", {"label": "D"}, has_test_csv=True)
    insert_product(con, "test_instr", "edr", {
        "label_file": "PROD_001.LBL",
        "files": json.dumps(["PROD_001.LBL", "PROD_001.IMG"]),
        "product_id": "PROD_001",
        "url_stem": "https://example.com/data/vol_001",
    })
    insert_product(con, "test_instr", "edr", {
        "label_file": "PROD_002.LBL",
        "files": json.dumps(["PROD_002.LBL", "PROD_002.IMG"]),
        "product_id": "PROD_002",
        "url_stem": "https://example.com/data/vol_002",
    })

    # Patch get_catalog to return our test connection
    import planetarypy.catalog._resolver as dl_mod

    def mock_get_catalog():
        return con

    monkeypatch.setattr("planetarypy.catalog.get_catalog", mock_get_catalog)
    yield con
    con.close()


class TestResolvedProduct:
    def test_file_urls(self):
        rp = ResolvedProduct(
            product_id="P1",
            url_stem="https://example.com/data",
            files=["P1.LBL", "P1.IMG"],
            label_file="P1.LBL",
            source="catalog",
        )
        urls = rp.file_urls
        assert urls["P1.LBL"] == "https://example.com/data/P1.LBL"
        assert urls["P1.IMG"] == "https://example.com/data/P1.IMG"

    def test_label_url(self):
        rp = ResolvedProduct(
            product_id="P1",
            url_stem="https://example.com/data/",
            files=["P1.LBL"],
            label_file="P1.LBL",
            source="catalog",
        )
        assert rp.label_url == "https://example.com/data/P1.LBL"

    def test_label_url_none(self):
        rp = ResolvedProduct(
            product_id="P1",
            url_stem="https://example.com/data",
            files=["P1.IMG"],
            label_file=None,
            source="catalog",
        )
        assert rp.label_url is None

    def test_trailing_slash_handled(self):
        rp = ResolvedProduct(
            product_id="P1",
            url_stem="https://example.com/data/",
            files=["P1.IMG"],
            label_file=None,
            source="catalog",
        )
        assert rp.file_urls["P1.IMG"] == "https://example.com/data/P1.IMG"


class TestResolveCatalog:
    def test_found(self, catalog_con):
        rp = resolve_product("test_mission", "test_instr", "edr", "PROD_001")
        assert rp.product_id == "PROD_001"
        assert rp.url_stem == "https://example.com/data/vol_001"
        assert rp.files == ["PROD_001.LBL", "PROD_001.IMG"]
        assert rp.source == "catalog"

    def test_not_found(self, catalog_con):
        with pytest.raises(ProductNotFoundError, match="NONEXISTENT"):
            resolve_product("test_mission", "test_instr", "edr", "NONEXISTENT")

    def test_different_products_different_urls(self, catalog_con):
        rp1 = resolve_product("test_mission", "test_instr", "edr", "PROD_001")
        rp2 = resolve_product("test_mission", "test_instr", "edr", "PROD_002")
        assert rp1.url_stem != rp2.url_stem


class TestLocalProductDir:
    def test_basic_path(self):
        path = _local_product_dir("mro", "ctx", "edr", "PRODUCT_001")
        assert path.parts[-4:] == ("mro", "ctx", "edr", "PRODUCT_001")

    def test_sanitizes_slashes(self):
        path = _local_product_dir("mro", "ctx", "edr", "some/product/id")
        assert "some_product_id" in str(path)
