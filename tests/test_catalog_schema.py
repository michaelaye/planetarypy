"""Tests for the catalog DuckDB schema."""

import pytest
import duckdb

from planetarypy.catalog._schema import (
    create_schema,
    reset_schema,
    insert_instrument,
    insert_product_type,
    insert_product,
)


@pytest.fixture
def con():
    """Create an in-memory DuckDB connection with schema."""
    conn = duckdb.connect(":memory:")
    create_schema(conn)
    yield conn
    conn.close()


def test_create_schema(con):
    """Test that schema creates all tables."""
    tables = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
    ).fetchall()
    table_names = {t[0] for t in tables}
    assert "instruments" in table_names
    assert "product_types" in table_names
    assert "products" in table_names


def test_insert_instrument(con):
    """Test instrument insertion."""
    insert_instrument(con, "cassini_iss", "cassini", "iss", "auto", 8)
    row = con.execute("SELECT * FROM instruments WHERE folder_name='cassini_iss'").fetchone()
    assert row is not None
    assert row[1] == "cassini"
    assert row[2] == "iss"
    assert row[4] == 8


def test_insert_product_type(con):
    """Test product type insertion."""
    insert_instrument(con, "cassini_iss", "cassini", "iss", "auto", 1)
    metadata = {
        "manifest": "img_usgs_cassini",
        "fn_must_contain": [".IMG"],
        "label": "A",
    }
    insert_product_type(con, "cassini_iss", "edr_sat", metadata, has_test_csv=True)
    row = con.execute(
        "SELECT * FROM product_types WHERE folder_name='cassini_iss' AND product_key='edr_sat'"
    ).fetchone()
    assert row is not None
    assert row[5] == "img_usgs_cassini"  # manifest (after normalized_type, phase, format)
    assert row[11] == "A"  # label_type


def test_insert_product(con):
    """Test product insertion."""
    insert_instrument(con, "cassini_iss", "cassini", "iss", "auto", 1)
    insert_product_type(con, "cassini_iss", "edr_sat", {"label": "A"})
    insert_product(con, "cassini_iss", "edr_sat", {
        "label_file": "N1234.LBL",
        "files": '["N1234.LBL", "N1234.IMG"]',
        "product_id": "1_N1234",
        "url_stem": "http://example.com/data",
        "hash": '{"IMAGE": "abc"}',
    })
    row = con.execute("SELECT * FROM products").fetchone()
    assert row is not None
    assert row[4] == "1_N1234"  # product_id


def test_catalog_view(con):
    """Test the catalog view joins correctly."""
    insert_instrument(con, "cassini_iss", "cassini", "iss", "auto", 1)
    insert_product_type(con, "cassini_iss", "edr_sat", {"label": "A"})
    insert_product(con, "cassini_iss", "edr_sat", {
        "product_id": "1_N1234",
        "url_stem": "http://example.com",
    })
    row = con.execute("SELECT * FROM catalog").fetchone()
    assert row[0] == "cassini"  # mission
    assert row[1] == "iss"  # instrument
    assert row[3] == "edr_sat"  # product_key


def test_reset_schema(con):
    """Test that reset drops and recreates tables."""
    insert_instrument(con, "test", "test", "test", "auto", 0)
    assert con.execute("SELECT COUNT(*) FROM instruments").fetchone()[0] == 1
    reset_schema(con)
    assert con.execute("SELECT COUNT(*) FROM instruments").fetchone()[0] == 0
