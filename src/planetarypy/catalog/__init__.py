"""PDS Catalog: index of all instruments and products from the PDS archive.

Built from the MillionConcepts pdr-tests repository, this module provides
a local DuckDB database cataloging all known PDS instruments and their
data products, following the mission/instrument/product access scheme.

Usage:
    from planetarypy.catalog import build_catalog, list_missions

    # Build the catalog (clones pdr-tests repo, parses definitions, populates DB)
    build_catalog()

    # Query the catalog
    list_missions()
    list_instruments("cassini")
    list_product_types("cassini", "iss")
    browse_products("cassini", "iss", "edr_sat")

    # Review ambiguous mission mappings
    ambiguous_mappings()
"""

from pathlib import Path

import pandas as pd
from loguru import logger

from planetarypy.config import config


def build_catalog(force: bool = False) -> dict:
    """Build (or rebuild) the PDS catalog database from pdr-tests.

    Clones the pdr-tests repository, parses all selection_rules.py and
    CSV test files, and populates a DuckDB database.

    Parameters
    ----------
    force : bool
        If True, force re-clone of pdr-tests and rebuild DB from scratch

    Returns
    -------
    dict
        Summary with counts of missions, instruments, product types, products
    """
    from planetarypy.catalog._repo import ensure_repo
    from planetarypy.catalog._parser import (
        list_definition_dirs,
        parse_selection_rules,
        find_test_csvs,
        parse_test_csv,
    )
    from planetarypy.catalog._mission_map import (
        resolve_mission_instrument,
        split_product_key,
    )
    from planetarypy.catalog._schema import (
        get_connection,
        reset_schema,
        insert_instrument,
        insert_product_type,
        insert_product,
    )

    storage_root = config.storage_root

    # Step 1: Ensure repo is cloned/updated
    repo_path = ensure_repo(storage_root, force_update=force)

    # Step 2: Open DB and reset if rebuilding
    con = get_connection(storage_root)
    if force:
        reset_schema(con)
    else:
        from planetarypy.catalog._schema import create_schema
        create_schema(con)
        # Check if already populated
        count = con.execute("SELECT COUNT(*) FROM instruments").fetchone()[0]
        if count > 0:
            logger.info(f"Catalog already has {count} instruments. Use force=True to rebuild.")
            con.close()
            return _get_summary(storage_root)

    # Step 3: Parse all definition directories
    dirs = list_definition_dirs(repo_path)
    logger.info(f"Found {len(dirs)} instrument definitions to process.")

    stats = {
        "instruments": 0,
        "product_types": 0,
        "products": 0,
        "parse_errors": [],
        "ambiguous": [],
    }

    for defdir in dirs:
        folder_name = defdir.name
        rules_file = defdir / "selection_rules.py"

        # Parse selection rules
        file_info = parse_selection_rules(rules_file)
        if not file_info:
            stats["parse_errors"].append(folder_name)
            logger.warning(f"No file_information found in {folder_name}/selection_rules.py")
            continue

        # Resolve mission/instrument
        mission, instrument, mapping_status = resolve_mission_instrument(folder_name)
        if mapping_status == "ambiguous":
            stats["ambiguous"].append(folder_name)

        # Find matching CSV test files
        csv_matches = find_test_csvs(defdir, list(file_info.keys()))

        if instrument == "_split":
            # Multi-instrument folder: split product keys by prefix
            # Group product keys by their resolved instrument
            from collections import defaultdict
            instr_groups: dict[str, list[str]] = defaultdict(list)
            for product_key in file_info:
                instr_name, _ = split_product_key(folder_name, product_key)
                instr_groups[instr_name].append(product_key)

            for instr_name, product_keys in instr_groups.items():
                # Synthetic folder_name for this instrument
                synth_folder = f"{folder_name}__{instr_name}"
                insert_instrument(
                    con, synth_folder, mission, instr_name,
                    "split", len(product_keys),
                )
                stats["instruments"] += 1

                for product_key in product_keys:
                    metadata = file_info[product_key]
                    if not isinstance(metadata, dict):
                        logger.warning(
                            f"Skipping {folder_name}/{product_key}: "
                            f"metadata is {type(metadata).__name__}, not dict"
                        )
                        continue
                    _, stripped_key = split_product_key(folder_name, product_key)
                    has_csv = product_key in csv_matches
                    insert_product_type(
                        con, synth_folder, stripped_key, metadata, has_csv,
                    )
                    stats["product_types"] += 1

                    if has_csv:
                        rows = parse_test_csv(csv_matches[product_key])
                        for row in rows:
                            insert_product(con, synth_folder, stripped_key, row)
                            stats["products"] += 1
        else:
            # Single-instrument folder: insert directly
            insert_instrument(con, folder_name, mission, instrument,
                              mapping_status, len(file_info))
            stats["instruments"] += 1

            for product_key, metadata in file_info.items():
                if not isinstance(metadata, dict):
                    logger.warning(
                        f"Skipping {folder_name}/{product_key}: "
                        f"metadata is {type(metadata).__name__}, not dict"
                    )
                    continue
                has_csv = product_key in csv_matches
                insert_product_type(con, folder_name, product_key, metadata, has_csv)
                stats["product_types"] += 1

                if has_csv:
                    rows = parse_test_csv(csv_matches[product_key])
                    for row in rows:
                        insert_product(con, folder_name, product_key, row)
                        stats["products"] += 1

    con.close()

    # Step 4: Rewrite broken USGS URLs to working mirrors
    from planetarypy.catalog._url_rewrite import rewrite_catalog_urls
    rewrite_counts = rewrite_catalog_urls(storage_root)
    stats["urls_rewritten"] = rewrite_counts.get("rewritten", 0)
    stats["urls_no_rule"] = rewrite_counts.get("no_rule", 0)

    # Report
    logger.info(
        f"Catalog built: {stats['instruments']} instruments, "
        f"{stats['product_types']} product types, {stats['products']} products"
    )
    if stats["ambiguous"]:
        logger.warning(
            f"{len(stats['ambiguous'])} ambiguous mission mappings: "
            f"{', '.join(stats['ambiguous'])}"
        )
    if stats["parse_errors"]:
        logger.warning(
            f"{len(stats['parse_errors'])} parse errors: "
            f"{', '.join(stats['parse_errors'])}"
        )

    return stats


def get_catalog() -> "duckdb.DuckDBPyConnection":
    """Open and return a connection to the existing catalog database.

    Raises
    ------
    FileNotFoundError
        If the catalog has not been built yet
    """
    from planetarypy.catalog._schema import get_connection, DB_FILENAME

    db_path = config.storage_root / "catalog" / DB_FILENAME
    if not db_path.exists():
        raise FileNotFoundError(
            "Catalog database not found. Run build_catalog() first."
        )
    return get_connection(config.storage_root)


def _parse_dotted_key(key: str, expected_parts: int) -> tuple[str, ...]:
    """Parse a dotted key like 'lro.diviner' or 'cassini.iss.edr_sat'.

    Parameters
    ----------
    key : str
        Dotted key string
    expected_parts : int
        Expected number of parts (2 for mission.instrument, 3 for mission.instrument.product)

    Returns
    -------
    tuple[str, ...]
        The split parts

    Raises
    ------
    ValueError
        If the key doesn't have the expected number of parts
    """
    parts = key.split(".")
    if len(parts) != expected_parts:
        raise ValueError(
            f"Expected {expected_parts} dot-separated parts, "
            f"got {len(parts)}: '{key}'"
        )
    return tuple(parts)


def list_missions() -> list[str]:
    """List all missions in the catalog."""
    con = get_catalog()
    result = con.execute(
        "SELECT DISTINCT mission FROM instruments ORDER BY mission"
    ).fetchall()
    con.close()
    return [r[0] for r in result]


def list_instruments(mission: str) -> list[str]:
    """List all instruments for a given mission.

    Parameters
    ----------
    mission : str
        Mission name (e.g. 'cassini')
    """
    con = get_catalog()
    result = con.execute(
        "SELECT DISTINCT instrument FROM instruments WHERE mission = ? ORDER BY instrument",
        [mission],
    ).fetchall()
    con.close()
    return [r[0] for r in result]


def list_product_types(key: str, instrument: str | None = None) -> list[str]:
    """List all product types for a given mission and instrument.

    Accepts either dotted key or separate arguments:
        list_product_types("lro.diviner")
        list_product_types("lro", "diviner")

    Parameters
    ----------
    key : str
        Either a dotted key 'mission.instrument' or just the mission name
    instrument : str, optional
        Instrument name, required if key is not a dotted key
    """
    if instrument is None:
        mission, instrument = _parse_dotted_key(key, 2)
    else:
        mission = key
    con = get_catalog()
    result = con.execute(
        """SELECT pt.product_key
           FROM product_types pt
           JOIN instruments i USING (folder_name)
           WHERE i.mission = ? AND i.instrument = ?
           ORDER BY pt.product_key""",
        [mission, instrument],
    ).fetchall()
    con.close()
    return [r[0] for r in result]


def browse_products(
    key: str,
    instrument: str | None = None,
    product_key: str | None = None,
) -> pd.DataFrame:
    """Get all product entries for a given mission/instrument/product type.

    Accepts either dotted key or separate arguments:
        browse_products("cassini.iss.edr_sat")
        browse_products("cassini", "iss", "edr_sat")

    Parameters
    ----------
    key : str
        Either a dotted key 'mission.instrument.product' or just the mission name
    instrument : str, optional
        Instrument name, required if key is not a dotted key
    product_key : str, optional
        Product type key, required if key is not a dotted key

    Returns
    -------
    pd.DataFrame
        Product entries with columns: product_id, label_file, url_stem, etc.
    """
    if instrument is None and product_key is None:
        mission, instrument, product_key = _parse_dotted_key(key, 3)
    elif instrument is not None and product_key is not None:
        mission = key
    else:
        raise ValueError("Provide either a dotted key or all three arguments")
    con = get_catalog()
    df = con.execute(
        """SELECT p.*
           FROM products p
           JOIN product_types pt USING (folder_name, product_key)
           JOIN instruments i USING (folder_name)
           WHERE i.mission = ? AND i.instrument = ? AND pt.product_key = ?""",
        [mission, instrument, product_key],
    ).fetchdf()
    con.close()
    return df


def ambiguous_mappings() -> pd.DataFrame:
    """Return all instruments with ambiguous mission/instrument mapping.

    These need manual review by the user.
    """
    con = get_catalog()
    df = con.execute(
        """SELECT folder_name, mission, instrument, num_product_types
           FROM instruments
           WHERE mapping_status = 'ambiguous'
           ORDER BY folder_name"""
    ).fetchdf()
    con.close()
    return df


def search(query: str) -> pd.DataFrame:
    """Search the catalog for products matching a query string.

    Searches across mission, instrument, product_key, and product_id.
    """
    con = get_catalog()
    pattern = f"%{query}%"
    df = con.execute(
        """SELECT mission, instrument, product_key, product_id, url_stem
           FROM catalog
           WHERE mission ILIKE ? OR instrument ILIKE ?
              OR product_key ILIKE ? OR product_id ILIKE ?
           ORDER BY mission, instrument, product_key
           LIMIT 100""",
        [pattern, pattern, pattern, pattern],
    ).fetchdf()
    con.close()
    return df


def summary() -> pd.DataFrame:
    """Get a summary of the catalog contents grouped by mission."""
    con = get_catalog()
    df = con.execute(
        """SELECT
               mission,
               COUNT(DISTINCT instrument) as instruments,
               COUNT(DISTINCT folder_name || '.' || product_key) as product_types,
               COUNT(DISTINCT product_id) FILTER (WHERE product_id IS NOT NULL) as products
           FROM catalog
           GROUP BY mission
           ORDER BY mission"""
    ).fetchdf()
    con.close()
    return df


def fetch_product(
    key: str,
    product_id: str,
    *,
    instrument: str | None = None,
    product_key: str | None = None,
    files: list[str] | None = None,
    label_only: bool = False,
    force: bool = False,
) -> Path:
    """Download a product and return the local directory path.

    Parameters
    ----------
    key : str
        Either a dotted key 'mission.instrument.product_type' or just mission
    product_id : str
        Product identifier (e.g. '1_N1523786525.118')
    instrument : str, optional
        Instrument name, required if key is not a dotted key
    product_key : str, optional
        Product type key, required if key is not a dotted key
    files : list[str] | None
        Specific filenames to download. None = all files.
    label_only : bool
        If True, download only the label file.
    force : bool
        If True, re-download even if files exist locally.

    Returns
    -------
    Path
        Local directory containing the downloaded product files
    """
    from planetarypy.catalog._download import (
        resolve_product,
        download_product,
        _local_product_dir,
    )

    if instrument is None and product_key is None:
        mission, instrument, product_key = _parse_dotted_key(key, 3)
    elif instrument is not None and product_key is not None:
        mission = key
    else:
        raise ValueError("Provide either a dotted key or all three arguments")

    resolved = resolve_product(mission, instrument, product_key, product_id)
    local_dir = _local_product_dir(mission, instrument, product_key, product_id)
    download_product(
        resolved, local_dir, files=files, label_only=label_only, force=force,
    )
    return local_dir


def get_product_url(
    key: str,
    product_id: str,
    *,
    instrument: str | None = None,
    product_key: str | None = None,
) -> str:
    """Return the remote base URL for a product without downloading.

    Parameters
    ----------
    key : str
        Either a dotted key 'mission.instrument.product_type' or just mission
    product_id : str
        Product identifier

    Returns
    -------
    str
        The url_stem for this product
    """
    from planetarypy.catalog._download import resolve_product

    if instrument is None and product_key is None:
        mission, instrument, product_key = _parse_dotted_key(key, 3)
    elif instrument is not None and product_key is not None:
        mission = key
    else:
        raise ValueError("Provide either a dotted key or all three arguments")

    resolved = resolve_product(mission, instrument, product_key, product_id)
    return resolved.url_stem


def list_product_files(
    key: str,
    product_id: str,
    *,
    instrument: str | None = None,
    product_key: str | None = None,
) -> dict[str, str]:
    """Return the files and their URLs for a product.

    Parameters
    ----------
    key : str
        Dotted key 'mission.instrument.product_type' or just mission
    product_id : str
        Product identifier

    Returns
    -------
    dict[str, str]
        Mapping of filename -> full URL
    """
    from planetarypy.catalog._download import resolve_product

    if instrument is None and product_key is None:
        mission, instrument, product_key = _parse_dotted_key(key, 3)
    elif instrument is not None and product_key is not None:
        mission = key
    else:
        raise ValueError("Provide either a dotted key or all three arguments")

    resolved = resolve_product(mission, instrument, product_key, product_id)
    return resolved.file_urls


def _get_summary(storage_root: Path) -> dict:
    """Get summary counts from an existing catalog."""
    from planetarypy.catalog._schema import get_connection

    con = get_connection(storage_root)
    result = {
        "instruments": con.execute("SELECT COUNT(*) FROM instruments").fetchone()[0],
        "product_types": con.execute("SELECT COUNT(*) FROM product_types").fetchone()[0],
        "products": con.execute("SELECT COUNT(*) FROM products").fetchone()[0],
    }
    con.close()
    return result
