"""PDS Tools, to work with Planetary Data System datasets.
"""


from loguru import logger
from pandas import DataFrame

from typing import Iterable

from planetarypy.pds.index_main import Index, InventoryIndex
from planetarypy.pds.utils import (
    complete_pid,
    get_example_pid,
    get_index_names,
    get_instrument_names,
    get_meta,
    get_mission_names,
    pid_column,
    print_available_indexes,
    read_index_slice,
    rebuild_pid_cache,
)

__all__ = [
    "Index",
    "InventoryIndex",
    "get_index",
    "missing_pids",
    "pid_column",
    "print_available_indexes",
    "get_mission_names",
    "get_instrument_names",
    "get_index_names",
    "get_example_pid",
    "get_meta",
    "read_index_slice",
    "read_pids_file",
    "complete_pid",
    "rebuild_pid_cache",
]


def get_index(
    dotted_index_key: str,
    allow_refresh: bool = False,
    force_refresh: bool = False,
    rebuild_parquet: bool = False,
    force_config_update: bool = False,
    *,
    pids: Iterable[str] | None = None,
) -> DataFrame:
    """Retrieve a specific index file .

    A check is made for possible updates to the index file once per day.

    Parameters
    ----------
    dotted_index_key : str
        Main identifier for the index to retrieve. Example: 'mro.ctx.edr'
    allow_refresh : bool
        If True, download the latest version if an update is available.
    force_refresh : bool
        Download the latest version unconditionally.
    rebuild_parquet : bool
        If True, rebuild the parquet file from existing downloaded files only.
    force_config_update : bool
        If True, force update of local URL config file from remote source.
    pids : Iterable[str], keyword-only, optional
        Product IDs to filter the returned DataFrame to. When provided,
        rows are matched by exact string comparison against the index's
        product-id column (resolved via :func:`pid_column`). PIDs that
        don't match any row are silently dropped from the result; use
        :func:`missing_pids` to find them.

    Returns
    -------
    pandas.DataFrame
        DataFrame containing the index data, read from the local parquet file.
        When ``pids`` is given, only rows matching those PIDs are returned.
    """
    # Use InventoryIndex for special inventory files that have multi-target CSV format
    if dotted_index_key.endswith('.inventory'):
        index = InventoryIndex(dotted_index_key, force_config_update=force_config_update)
    else:
        index = Index(dotted_index_key, force_config_update=force_config_update)

    # Ensure parquet exists; optionally rebuild from existing files only
    downloaded = index.ensure_parquet(force=rebuild_parquet)

    # Skip refresh logic if we just downloaded in ensure_parquet
    if not downloaded:
        # Check update_available only once to avoid repeated remote checks
        update_avail = index.update_available if not force_refresh else False
        if (allow_refresh and update_avail) or force_refresh:
            logger.debug(
                f"Refreshing index {dotted_index_key}, downloading latest version."
            )
            index.download()
        elif update_avail:
            # Warn user that an update is available but not being downloaded
            logger.warning(
                f"Update available for {dotted_index_key}. "
                "Call get_index() with allow_refresh=True to download the latest version."
            )

    df = index.dataframe
    if pids is not None:
        col = pid_column(dotted_index_key, df)
        pid_set = set(map(str, pids))
        df = df[df[col].astype(str).isin(pid_set)]
    return df


def read_pids_file(
    source,
    *,
    index_key: str | None = None,
    pid_key: str | None = None,
    suffix: str | None = None,
) -> list[str]:
    """Read PIDs from a file or stdin, with CSV column resolution.

    Dispatch rule:

    - ``.csv`` extension OR ``pid_key`` is given → parsed with pandas.
      The product-id column is determined by ``pid_key`` (explicit
      override) or :func:`pid_column` (auto-detect via the catalog's
      ``IndexConfig`` registry, using ``index_key``). Raises
      :class:`ValueError` listing the CSV's columns when neither
      resolves — so the caller can pass the right ``pid_key``.
    - Anything else (including ``"-"`` for stdin when ``pid_key`` is
      absent) → one PID per line; blanks and ``#``-prefixed comments
      stripped. Delegates to :func:`planetarypy.utils.read_pids`.

    Setting ``pid_key`` is the explicit signal that the input is
    tabular; the CSV path is then taken regardless of the source —
    including stdin, where pandas reads from ``sys.stdin``. This makes
    ``head file.csv | plp fetch ... --pids-from - --pid-key COL`` work
    the same way as ``plp fetch ... --pids-from file.csv``.

    Designed to back the ``--pids-from`` CLI option in
    ``plp fetch`` / ``plp indexes select`` so users can feed a saved CSV
    (e.g. the output of ``plp indexes select --format csv``) without
    pre-extracting a one-PID-per-line file.

    Parameters
    ----------
    source : Path or str
        File path, or ``"-"`` for stdin.
    index_key : str, optional
        Dotted index key used to auto-detect the PID column in CSVs via
        the catalog's IndexConfig registry. Ignored when ``pid_key`` is
        given. Accepts either an index key (``cassini.iss.index``) or a
        catalog product key (``cassini.iss.edr_sat``) — ``pid_column``
        looks up both forms.
    pid_key : str, optional
        Explicit column name for CSV input; wins over auto-detection.
        Must be an existing column in the CSV. **When set, also forces
        CSV parsing on stdin / non-csv-extension paths** — the flag is
        the user's declaration that the input is tabular.
    suffix : str, optional
        Appended to each PID after reading. Convenient for files that
        carry observation-level identifiers when the downstream caller
        actually needs a more specific product (e.g. HiRISE obsids
        ``PSP_003092_0985`` + ``"_RED"`` → ``PSP_003092_0985_RED``).
        Applied after CSV column extraction; the underlying values are
        not mutated.

    Returns
    -------
    list[str]
        PIDs in file order. No deduplication. Empty values from the CSV
        column are NOT filtered — that's the caller's responsibility.

    Raises
    ------
    ValueError
        For ``.csv`` input when neither ``pid_key`` nor ``index_key``
        resolves to an existing column.
    KeyError
        When ``pid_key`` is given but not present in the CSV columns.
    """
    src_str = str(source)

    # Decide whether to parse as CSV. An explicit ``pid_key`` flips us
    # into CSV mode regardless of source kind — the user is saying
    # "this is tabular data, here's the column" and we honor that even
    # for stdin or for unconventional extensions.
    use_csv = (
        pid_key is not None
        or (src_str != "-" and src_str.lower().endswith(".csv"))
    )

    if not use_csv:
        from planetarypy.utils import read_pids as _read_text_pids
        pids = _read_text_pids(source)
        if suffix:
            pids = [f"{p}{suffix}" for p in pids]
        return pids

    import pandas as pd
    if src_str == "-":
        import sys
        df = pd.read_csv(sys.stdin)
    else:
        df = pd.read_csv(source)

    if pid_key is not None:
        if pid_key not in df.columns:
            raise KeyError(
                f"pid_key={pid_key!r} is not a column of {source!s}. "
                f"Available columns: {list(df.columns)}"
            )
        col = pid_key
    else:
        col = None
        if index_key is not None:
            try:
                col = pid_column(index_key, df)
            except KeyError:
                col = None
        if col is None:
            raise ValueError(
                f"Cannot auto-detect a PID column in {source!s}. "
                f"CSV columns are {list(df.columns)}. "
                "Pass pid_key=<column> to select one explicitly."
            )

    pids = df[col].astype(str).tolist()
    if suffix:
        pids = [f"{p}{suffix}" for p in pids]
    return pids


def missing_pids(
    df: DataFrame,
    dotted_index_key: str,
    pids: Iterable[str],
) -> list[str]:
    """Return the subset of ``pids`` that are NOT present in ``df``.

    Useful for batch-PID workflows to surface "which IDs did the index not
    know about?" alongside the filtered DataFrame returned by
    ``get_index(key, pids=...)``.

    Parameters
    ----------
    df : pandas.DataFrame
        Index DataFrame (filtered or full — only the configured product-id
        column needs to be present).
    dotted_index_key : str
        Index key to resolve the product-id column name via
        :func:`pid_column`.
    pids : Iterable[str]
        PIDs to check.

    Returns
    -------
    list[str]
        PIDs in input order that are absent from ``df``'s product-id column.
        Order is preserved; duplicates in the input are NOT deduplicated
        (the caller decides whether that matters).
    """
    col = pid_column(dotted_index_key, df)
    present = set(df[col].astype(str))
    return [p for p in pids if str(p) not in present]
