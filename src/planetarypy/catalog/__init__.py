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
    list_products("cassini", "iss")
    example_products("cassini", "iss", "edr_sat")

    # Review ambiguous mission mappings
    ambiguous_mappings()
"""

from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    import duckdb

from planetarypy.catalog._objects import Mission, Instrument  # noqa: F401
from planetarypy.catalog._resolver import (  # noqa: F401
    DownloadedProduct,
    register_storage_resolver,
    default_product_dir,
)
from planetarypy.catalog._index_resolver import (  # noqa: F401
    IndexConfig,
    register_index,
)
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
        normalize_product_key,
        apply_instrument_overrides,
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
                    norm_type, phase, fmt = normalize_product_key(stripped_key)
                    norm_type, phase, fmt = apply_instrument_overrides(
                        norm_type, phase, fmt, mission, instr_name,
                    )
                    has_csv = product_key in csv_matches
                    insert_product_type(
                        con, synth_folder, stripped_key, metadata, has_csv,
                        normalized_type=norm_type, phase=phase, fmt=fmt,
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
                norm_type, phase, fmt = normalize_product_key(product_key)
                norm_type, phase, fmt = apply_instrument_overrides(
                    norm_type, phase, fmt, mission, instrument,
                )
                has_csv = product_key in csv_matches
                insert_product_type(
                    con, folder_name, product_key, metadata, has_csv,
                    normalized_type=norm_type, phase=phase, fmt=fmt,
                )
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


def _assert_schema_current(con) -> None:
    """Surface a clear error when the on-disk catalog DB predates the current schema.

    The ``normalized_type`` column on ``product_types`` was introduced in the
    big PDS-catalog rewrite (planetarypy 0.53+). Older DBs only have
    ``label_type`` and the binder for queries like ``example_products`` /
    ``list_products(include_phases=True)`` / ``search`` fails opaquely with
    ``Binder Error: Table 'pt' does not have a column named 'normalized_type'``.
    Detect that mismatch up front and tell the caller exactly what to do.
    """
    cols = {row[1] for row in con.execute(
        "PRAGMA table_info(product_types)").fetchall()}
    if "normalized_type" not in cols:
        con.close()
        raise RuntimeError(
            "Stale catalog DB schema (built with planetarypy ≤ 0.52). "
            "Run `plp catalog build --force` to rebuild — the source-of-truth "
            "(pdr-tests + INDEX_REGISTRY) is unchanged, so no data is lost."
        )


def get_catalog() -> "duckdb.DuckDBPyConnection":
    """Open and return a connection to the existing catalog database.

    Raises
    ------
    FileNotFoundError
        If the catalog has not been built yet.
    RuntimeError
        If the catalog DB exists but its schema predates the current code
        (e.g. built with planetarypy ≤ 0.52).
    """
    from planetarypy.catalog._schema import get_connection, DB_FILENAME

    db_path = config.storage_root / "catalog" / DB_FILENAME
    if not db_path.exists():
        raise FileNotFoundError(
            "Catalog database not found. Run build_catalog() first."
        )
    con = get_connection(config.storage_root)
    _assert_schema_current(con)
    return con


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


def list_instruments(mission: str, *, include_misc: bool = False) -> list[str]:
    """List all instruments for a given mission.

    Parameters
    ----------
    mission : str
        Mission name (e.g. 'cassini')
    include_misc : bool
        If True, include the '_misc' catch-all instrument that holds
        unclassified product types. Default False.
    """
    con = get_catalog()
    if include_misc:
        result = con.execute(
            "SELECT DISTINCT instrument FROM instruments WHERE mission = ? ORDER BY instrument",
            [mission],
        ).fetchall()
    else:
        result = con.execute(
            "SELECT DISTINCT instrument FROM instruments "
            "WHERE mission = ? AND instrument != '_misc' ORDER BY instrument",
            [mission],
        ).fetchall()
    con.close()
    return [r[0] for r in result]


def list_products(
    key: str,
    instrument: str | None = None,
    *,
    include_phases: bool = False,
) -> list[str] | pd.DataFrame:
    """List all product types for a given mission and instrument.

    By default, returns normalized product type names (e.g. 'edr' instead
    of 'edr_sat'). Use ``include_phases=True`` to see the phase breakdown.

    Accepts either dotted key or separate arguments:
        list_products("cassini.iss")
        list_products("cassini", "iss")
        list_products("cassini.iss", include_phases=True)

    Parameters
    ----------
    key : str
        Either a dotted key 'mission.instrument' or just the mission name
    instrument : str, optional
        Instrument name, required if key is not a dotted key
    include_phases : bool
        If True, return a DataFrame with ``normalized_type``, ``phase``,
        ``format``, ``product_key`` and ``source`` columns. The ``source``
        column carries the pdr-tests definition folder (e.g. ``dawn__vir``
        vs ``dawn_certified__vir``) and explains why an instrument can have
        multiple rows that share the other four — they're parallel archive
        provenances for the same logical product.
        If False (default), return a deduplicated list of normalized type names.
    """
    if instrument is None:
        mission, instrument = _parse_dotted_key(key, 2)
    else:
        mission = key
    con = get_catalog()
    if include_phases:
        df = con.execute(
            """SELECT pt.normalized_type, pt.phase, pt.format,
                      pt.product_key, pt.folder_name AS source
               FROM product_types pt
               JOIN instruments i USING (folder_name)
               WHERE i.mission = ? AND i.instrument = ?
               ORDER BY pt.normalized_type, pt.phase, pt.format, source""",
            [mission, instrument],
        ).fetchdf()
        con.close()
        return df
    else:
        result = con.execute(
            """SELECT DISTINCT pt.normalized_type
               FROM product_types pt
               JOIN instruments i USING (folder_name)
               WHERE i.mission = ? AND i.instrument = ?
               ORDER BY pt.normalized_type""",
            [mission, instrument],
        ).fetchall()
        con.close()
        return [r[0] for r in result]


def example_products(
    key: str,
    instrument: str | None = None,
    product_key: str | None = None,
    *,
    phase: str | None = None,
) -> pd.DataFrame:
    """Get all product entries for a given mission/instrument/product type.

    Accepts either dotted key or separate arguments:
        example_products("cassini.iss.edr")       # all EDR phases
        example_products("cassini.iss.edr", phase="saturn")  # Saturn only
        example_products("cassini", "iss", "edr")

    The product type is matched against the normalized_type column first,
    then falls back to exact product_key match for backward compatibility.

    Parameters
    ----------
    key : str
        Either a dotted key 'mission.instrument.product' or just the mission name
    instrument : str, optional
        Instrument name, required if key is not a dotted key
    product_key : str, optional
        Product type key, required if key is not a dotted key
    phase : str, optional
        Filter by mission phase (e.g. 'saturn', 'jupiter', 'cruise').
        Only used when matching by normalized_type.

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
    # Try normalized_type first, fall back to exact product_key
    if phase is not None:
        df = con.execute(
            """SELECT p.*
               FROM products p
               JOIN product_types pt USING (folder_name, product_key)
               JOIN instruments i USING (folder_name)
               WHERE i.mission = ? AND i.instrument = ?
                 AND pt.normalized_type = ? AND pt.phase = ?""",
            [mission, instrument, product_key, phase],
        ).fetchdf()
    else:
        df = con.execute(
            """SELECT p.*
               FROM products p
               JOIN product_types pt USING (folder_name, product_key)
               JOIN instruments i USING (folder_name)
               WHERE i.mission = ? AND i.instrument = ?
                 AND (pt.normalized_type = ? OR pt.product_key = ?)""",
            [mission, instrument, product_key, product_key],
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

    Searches across mission, instrument, normalized_type, product_key,
    and product_id. Searching for 'edr' will find both 'edr_sat' and
    'edr_evj' variants.
    """
    con = get_catalog()
    pattern = f"%{query}%"
    df = con.execute(
        """SELECT mission, instrument, normalized_type, phase, format,
                  product_key, product_id, url_stem
           FROM catalog
           WHERE mission ILIKE ? OR instrument ILIKE ?
              OR normalized_type ILIKE ? OR product_key ILIKE ?
              OR product_id ILIKE ?
           ORDER BY mission, instrument, normalized_type, phase, format
           LIMIT 100""",
        [pattern, pattern, pattern, pattern, pattern],
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
    local_dir: Path | None = None,
    open: bool = False,
) -> DownloadedProduct:
    """Download a PDS product and return where it landed plus what was written.

    Parameters
    ----------
    key : str
        Either a dotted key ``'mission.instrument.product_type'`` or just the
        mission name (in which case ``instrument`` and ``product_key`` are
        required).
    product_id : str
        Product identifier, e.g. ``'P02_001916_2221_XI_42N027W'``.
        Accepts the bare-PID form returned by
        :func:`planetarypy.pds.get_example_pid`; PDS path/extension and
        flight-software version suffixes are normalized away during
        matching.
    instrument : str, optional
        Instrument name. Required when ``key`` is just a mission.
    product_key : str, optional
        Product type key. Required when ``key`` is just a mission.
    files : list[str] | None
        Specific filenames to download. ``None`` (default) downloads every
        file the resolver returns for this product.
    label_only : bool
        If ``True``, download only the PDS label file. Mutually exclusive
        with a populated ``files`` argument.
    force : bool
        If ``True``, re-download even if files already exist locally.
    local_dir : Path, optional
        Override the storage location. When ``None`` (default), the
        catalog's per-instrument layout is used
        (``{storage_root}/{mission}/{instrument}/{product_type}/{pid}/``).
    open : bool
        If ``True``, open the downloaded product in memory and return that
        object (as :func:`planetarypy.open`) instead of the
        ``DownloadedProduct``. Convenience for download-and-open in one call.

    Returns
    -------
    DownloadedProduct
        Bundle containing ``local_dir`` (folder), ``files`` (absolute paths
        of every file written by this call), ``label_file`` (convenience
        pointer to the PDS label, if any), and ``product_id`` (the
        canonical identifier the resolver matched). When ``open=True``, the
        opened in-memory object is returned instead.

    Examples
    --------
    >>> from planetarypy.catalog import fetch_product
    >>> r = fetch_product("mro.ctx.edr", "P02_001916_2221_XI_42N027W")
    >>> r.local_dir
    PosixPath('.../mro/ctx/edr/P02_001916_2221_XI_42N027W')
    >>> r.files
    [PosixPath('.../P02_001916_2221_XI_42N027W.IMG'),
     PosixPath('.../P02_001916_2221_XI_42N027W.LBL')]
    >>> r.label_file.name
    'P02_001916_2221_XI_42N027W.LBL'
    """
    from planetarypy.catalog._resolver import (
        DownloadedProduct,
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
    if local_dir is None:
        local_dir = _local_product_dir(
            mission, instrument, product_key, resolved.product_id,
            resolved=resolved,
        )
    written = download_product(
        resolved, local_dir, files=files, label_only=label_only, force=force,
    )

    label_path: Path | None = None
    if resolved.label_file:
        candidate = local_dir / resolved.label_file
        if candidate in written:
            label_path = candidate

    result = DownloadedProduct(
        product_id=resolved.product_id,
        local_dir=local_dir,
        files=written,
        label_file=label_path,
        file_urls=resolved.file_urls,
    )
    if open:
        return result.open()
    return result


class OfflineError(RuntimeError):
    """Raised by :func:`fetch_products` when the preflight internet check fails.

    The batch fetcher refuses to launch a parallel pool against a server it
    can't reach — every worker would just error individually, polluting the
    report with redundant connection-failure noise. Pass
    ``skip_online_check=True`` to override (useful for offline mirrors or
    when ``have_internet()`` itself is the unreliable component).
    """


class BatchFetchResult:
    """Outcome of a single product in a :func:`fetch_products` batch.

    Attributes
    ----------
    product_id : str
        The PID that was requested (NOT the canonical PID the resolver
        matched, which lives at ``downloaded.product_id`` on success).
    downloaded : DownloadedProduct or None
        Populated on success; ``None`` if this PID's fetch raised.
    exception : Exception or None
        Captured exception on failure; ``None`` on success.
    """
    __slots__ = ("product_id", "downloaded", "exception")

    def __init__(
        self,
        product_id: str,
        downloaded: "DownloadedProduct | None",
        exception: Exception | None,
    ):
        self.product_id = product_id
        self.downloaded = downloaded
        self.exception = exception

    @property
    def ok(self) -> bool:
        return self.exception is None

    def __repr__(self) -> str:
        if self.ok:
            return f"BatchFetchResult({self.product_id!r}, ok=True)"
        return (
            f"BatchFetchResult({self.product_id!r}, ok=False, "
            f"exception={type(self.exception).__name__}: {self.exception})"
        )


def fetch_products(
    key: str,
    product_ids,
    *,
    workers: int = 4,
    instrument: str | None = None,
    product_key: str | None = None,
    files: list[str] | None = None,
    label_only: bool = False,
    force: bool = False,
    local_dir: Path | None = None,
    skip_online_check: bool = False,
) -> list[BatchFetchResult]:
    """Download a batch of PDS products in parallel; continues past failures.

    Thin wrapper over :func:`fetch_product` using
    :func:`planetarypy.utils.parallel_map`. Per-PID exceptions are captured
    into the returned :class:`BatchFetchResult` so a single bad PID never
    breaks the rest of the batch.

    Parameters
    ----------
    key : str
        Dotted key ``'mission.instrument.product_type'``, or just the
        mission name (in which case ``instrument`` and ``product_key`` are
        required). Passed through verbatim to ``fetch_product`` per PID.
    product_ids : Iterable[str]
        PIDs to download. Order is preserved in the returned list.
    workers : int, default 4
        Thread-pool size. Safe default for typical PDS servers; raise for
        bulk downloads against tolerant servers.
    instrument, product_key, files, label_only, force, local_dir
        Forwarded to :func:`fetch_product` unchanged. ``local_dir`` is
        shared across all PIDs in the batch — pass ``None`` (default) to
        get the catalog's per-PID layout.
    skip_online_check : bool, default False
        When ``False`` (default), :func:`planetarypy.utils.have_internet`
        is called before launching the pool; if it returns ``False``,
        :class:`OfflineError` is raised. Pass ``True`` to bypass the
        preflight (offline mirrors, captive networks, etc.).

    Returns
    -------
    list[BatchFetchResult]
        One result per input PID, in input order. ``result.ok`` is the
        quickest success check; ``result.downloaded`` holds the same
        :class:`DownloadedProduct` you'd get from a single
        ``fetch_product`` call on success.

    Raises
    ------
    OfflineError
        If the preflight internet check fails and
        ``skip_online_check=False``.

    Examples
    --------
    >>> from planetarypy.catalog import fetch_products
    >>> results = fetch_products(
    ...     "mro.ctx.edr",
    ...     ["P02_001916_2221_XI_42N027W", "P03_001234_2222_XI_43N028W"],
    ...     workers=4,
    ... )
    >>> [r.ok for r in results]
    [True, True]
    >>> [r.downloaded.product_id for r in results if r.ok]
    ['P02_001916_2221_XI_42N027W', 'P03_001234_2222_XI_43N028W']
    """
    from planetarypy.utils import have_internet, parallel_map

    pids = list(product_ids)
    if not pids:
        return []

    if not skip_online_check and not have_internet():
        raise OfflineError(
            "No internet connection detected — refusing to launch parallel "
            "downloads. Pass skip_online_check=True to override (e.g. for "
            "offline mirrors or captive networks)."
        )

    def _fetch_one(pid: str) -> "DownloadedProduct":
        return fetch_product(
            key, pid,
            instrument=instrument, product_key=product_key,
            files=files, label_only=label_only, force=force,
            local_dir=local_dir,
        )

    triples = parallel_map(
        _fetch_one, pids,
        workers=workers, executor="thread",
        desc=f"Fetching {len(pids)} products",
    )
    return [
        BatchFetchResult(product_id=pid, downloaded=result, exception=exc)
        for (pid, result, exc) in triples
    ]


def get_product_urls(
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
    from planetarypy.catalog._resolver import resolve_product

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
