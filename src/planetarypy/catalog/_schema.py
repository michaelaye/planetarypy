"""DuckDB schema definitions and operations for the PDS catalog."""

import datetime as dt
import json
from pathlib import Path

from loguru import logger

try:
    import duckdb
except ImportError:
    raise ImportError(
        "DuckDB is required for the catalog module. "
        "Install it with: pip install planetarypy[catalog]"
    )

DB_FILENAME = "pdr_catalog.duckdb"

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS instruments (
    folder_name      VARCHAR PRIMARY KEY,
    mission          VARCHAR,
    instrument       VARCHAR,
    mapping_status   VARCHAR,
    num_product_types INTEGER,
    ingested_at      TIMESTAMP
);

CREATE TABLE IF NOT EXISTS product_types (
    folder_name      VARCHAR,
    product_key      VARCHAR,
    normalized_type  VARCHAR,
    phase            VARCHAR DEFAULT '',
    format           VARCHAR DEFAULT '',
    manifest         VARCHAR,
    fn_must_contain  VARCHAR,
    fn_ends_with     VARCHAR,
    fn_regex         VARCHAR,
    url_must_contain VARCHAR,
    url_regex        VARCHAR,
    label_type       VARCHAR,
    support_np       BOOLEAN DEFAULT FALSE,
    has_test_csv     BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (folder_name, product_key)
);

CREATE TABLE IF NOT EXISTS products (
    folder_name      VARCHAR,
    product_key      VARCHAR,
    label_file       VARCHAR,
    files            VARCHAR,
    product_id       VARCHAR,
    url_stem         VARCHAR,
    hash             VARCHAR,
    url_status       VARCHAR DEFAULT 'unchecked',
    url_checked_at   TIMESTAMP,
    FOREIGN KEY (folder_name, product_key)
        REFERENCES product_types(folder_name, product_key)
);

CREATE OR REPLACE VIEW catalog AS
SELECT
    i.mission,
    i.instrument,
    i.folder_name,
    pt.product_key,
    pt.normalized_type,
    pt.phase,
    pt.format,
    pt.manifest,
    pt.label_type,
    pt.support_np,
    pt.has_test_csv,
    p.product_id,
    p.label_file,
    p.url_stem,
    p.url_status
FROM product_types pt
JOIN instruments i USING (folder_name)
LEFT JOIN products p USING (folder_name, product_key);
"""


def get_connection(storage_root: Path) -> "duckdb.DuckDBPyConnection":
    """Get a DuckDB connection to the catalog database.

    Parameters
    ----------
    storage_root : Path
        The planetarypy storage root directory

    Returns
    -------
    duckdb.DuckDBPyConnection
        Connection to the catalog database
    """
    db_path = storage_root / "catalog" / DB_FILENAME
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def create_schema(con: "duckdb.DuckDBPyConnection") -> None:
    """Create the catalog tables and view if they don't exist."""
    con.execute(CREATE_TABLES_SQL)


def reset_schema(con: "duckdb.DuckDBPyConnection") -> None:
    """Drop and recreate all catalog tables (for rebuilds)."""
    con.execute("DROP VIEW IF EXISTS catalog")
    con.execute("DROP TABLE IF EXISTS products")
    con.execute("DROP TABLE IF EXISTS product_types")
    con.execute("DROP TABLE IF EXISTS instruments")
    create_schema(con)


def _to_json(value) -> str | None:
    """Convert a value to JSON string, or None."""
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return json.dumps(value)
    return json.dumps(value)


def insert_instrument(
    con: "duckdb.DuckDBPyConnection",
    folder_name: str,
    mission: str,
    instrument: str,
    mapping_status: str,
    num_product_types: int,
) -> None:
    """Insert an instrument record."""
    con.execute(
        """INSERT OR REPLACE INTO instruments
           (folder_name, mission, instrument, mapping_status, num_product_types, ingested_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        [folder_name, mission, instrument, mapping_status, num_product_types,
         dt.datetime.now(dt.timezone.utc)],
    )


def insert_product_type(
    con: "duckdb.DuckDBPyConnection",
    folder_name: str,
    product_key: str,
    metadata: dict,
    has_test_csv: bool = False,
    normalized_type: str = "",
    phase: str = "",
    fmt: str = "",
) -> None:
    """Insert a product type record from parsed selection_rules metadata."""
    con.execute(
        """INSERT OR REPLACE INTO product_types
           (folder_name, product_key, normalized_type, phase, format,
            manifest, fn_must_contain, fn_ends_with,
            fn_regex, url_must_contain, url_regex, label_type, support_np, has_test_csv)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            folder_name,
            product_key,
            normalized_type or product_key,
            phase,
            fmt,
            metadata.get("manifest"),
            _to_json(metadata.get("fn_must_contain")),
            _to_json(metadata.get("fn_ends_with")),
            _to_json(metadata.get("fn_regex")),
            _to_json(metadata.get("url_must_contain")),
            _to_json(metadata.get("url_regex")),
            metadata.get("label"),
            metadata.get("support_np", False),
            has_test_csv,
        ],
    )


def insert_product(
    con: "duckdb.DuckDBPyConnection",
    folder_name: str,
    product_key: str,
    row: dict,
) -> None:
    """Insert a product record from a parsed CSV row."""
    con.execute(
        """INSERT INTO products
           (folder_name, product_key, label_file, files, product_id, url_stem, hash)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [
            folder_name,
            product_key,
            row.get("label_file", ""),
            row.get("files", ""),
            row.get("product_id", ""),
            row.get("url_stem", ""),
            row.get("hash", ""),
        ],
    )
