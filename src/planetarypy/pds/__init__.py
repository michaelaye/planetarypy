"""PDS Tools, to work with Planetary Data System datasets.
"""


from loguru import logger
from pandas import DataFrame

from typing import Iterable

from planetarypy.pds.index_main import Index, InventoryIndex
from planetarypy.pds.meta_display import register_meta_handler
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
    "resolve_pids",
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
    "register_meta_handler",
]


def get_index(
    dotted_index_key: str,
    allow_refresh: bool = False,
    force_refresh: bool = False,
    rebuild_parquet: bool = False,
    force_config_update: bool = False,
    *,
    pids: Iterable[str] | None = None,
    columns: Iterable[str] | None = None,
    prefix: bool = False,
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
    prefix : bool, keyword-only, default False
        When True, any ``pids`` entry that has no exact match but is a
        leading prefix of one or more PRODUCT_IDs expands to all of those
        rows (see :func:`resolve_pids`). Exact matches are unaffected.
    columns : Iterable[str], keyword-only, optional
        Column names to project the returned DataFrame to, in the order
        given. Exact (case-sensitive) match against the parquet's column
        set; unknown names raise :class:`KeyError` listing the available
        columns. ``None`` (default) keeps every column.

    Returns
    -------
    pandas.DataFrame
        DataFrame containing the index data, read from the local parquet file.
        When ``pids`` is given, only rows matching those PIDs are returned;
        when ``columns`` is given, only those columns are returned.
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
        if prefix:
            mapping = resolve_pids(dotted_index_key, pids, df, prefix=True)
            wanted = {full for ids in mapping.values() for full in ids}
        else:
            wanted = set(map(str, pids))
        df = df[df[col].astype(str).isin(wanted)]
    if columns is not None:
        requested = list(columns)
        missing = [c for c in requested if c not in df.columns]
        if missing:
            raise KeyError(
                f"Column(s) not in {dotted_index_key!r}: {missing!r}. "
                f"Available columns: {list(df.columns)!r}"
            )
        df = df[requested]
    return df


def read_pids_file(
    source,
    *,
    index_key: str | None = None,
    pid_key: str | None = None,
    suffix: str | None = None,
) -> list[str]:
    """Read PIDs from a file or stdin, with CSV column resolution.

    Dispatch rule (first match wins):

    1. ``pid_key`` is given → CSV mode regardless of source.
    2. File with ``.csv``, ``.tsv`` or ``.tab`` extension → CSV mode.
    3. Stdin (``"-"``) whose first non-blank line contains a comma or a
       tab → CSV mode (small heuristic so ``head file.csv | plp fetch
       ...`` Just Works without an explicit flag).
    4. Anything else → plain text; one PID per line, blanks and
       ``#``-prefixed comments stripped.

    In CSV mode the **delimiter is auto-detected** from the header line:
    a tab in the first line selects TSV, otherwise comma. This makes
    tab-separated exports (a common spreadsheet "download as TSV"
    output) parse into real columns instead of collapsing into one. The
    product-id column is then determined by ``pid_key`` (explicit
    override) or :func:`pid_column` (auto-detect via the catalog's
    ``IndexConfig`` registry, using ``index_key``). Raises
    :class:`ValueError` listing the columns when neither resolves — so
    the caller can pass the right ``pid_key``.

    The comma/tab sniff is intentionally tiny. PDS product IDs don't
    contain commas or tabs, so plain-text input is reliably
    distinguishable from tabular input at the first-line level. The cost
    of being wrong is a clear ValueError pointing the user at
    ``--pid-key``.

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
    is_stdin = src_str == "-"

    # Buffer stdin once so we can both sniff (CSV vs plain text) and
    # parse without having to re-read a stream that can only be drained
    # once. File paths don't need this — we can read them twice.
    stdin_text: str | None = None
    first_line = ""
    if is_stdin:
        import sys
        stdin_text = sys.stdin.read()
        first_line = next(
            (line for line in stdin_text.splitlines() if line.strip()), ""
        )

    # Decide whether to parse as CSV:
    #   1. Explicit ``pid_key`` is the user's tabular-data declaration.
    #   2. File with ``.csv`` extension — the conventional hint.
    #   3. Stdin whose first non-blank line contains a comma — a tiny
    #      heuristic that catches the `head file.csv | plp fetch ...`
    #      idiom without requiring an explicit flag. Plain-text PIDs
    #      that happen to contain commas would be misclassified, but
    #      PDS product IDs don't use commas, so the false-positive rate
    #      is effectively zero in practice.
    if pid_key is not None:
        use_csv = True
    elif is_stdin:
        use_csv = ("," in first_line) or ("\t" in first_line)
    else:
        use_csv = src_str.lower().endswith((".csv", ".tsv", ".tab"))

    if not use_csv:
        if is_stdin:
            # We've already consumed stdin into stdin_text; mirror the
            # plain-text parsing rules from utils.read_pids inline so
            # we don't try to drain an empty sys.stdin.
            pids = []
            for raw_line in stdin_text.splitlines():
                line = raw_line.rstrip()
                stripped = line.lstrip()
                if not stripped or stripped.startswith("#"):
                    continue
                pids.append(line)
        else:
            from planetarypy.utils import read_pids as _read_text_pids
            pids = _read_text_pids(source)
        if suffix:
            pids = [f"{p}{suffix}" for p in pids]
        return pids

    import pandas as pd
    if is_stdin:
        import io
        sep = "\t" if "\t" in first_line else ","
        df = pd.read_csv(io.StringIO(stdin_text), sep=sep)
    else:
        # Peek the header to choose the delimiter, so tab-separated
        # exports (a common spreadsheet "download as TSV" output) parse
        # into real columns instead of collapsing into a single one.
        with open(source) as _f:
            header = next((ln for ln in _f if ln.strip()), "")
        sep = "\t" if "\t" in header else ","
        df = pd.read_csv(source, sep=sep)

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


def resolve_pids(
    dotted_index_key: str,
    pids: Iterable[str],
    df: DataFrame,
    *,
    prefix: bool = False,
) -> dict[str, list[str]]:
    """Map each requested PID to the full PRODUCT_IDs it resolves to.

    For each input PID, in order:

    - if it exactly matches a value in the index's product-id column
      (resolved via :func:`pid_column`), it maps to ``[pid]``;
    - else if ``prefix`` is True and it is a leading prefix
      (``str.startswith``) of one or more PRODUCT_IDs, it maps to all of
      those, sorted;
    - else it maps to ``[]`` (the PID is missing).

    An exact match always wins over prefix expansion. This is the generic
    "short product ID" mechanism: a HiRISE obsid handed to a per-CCD index
    expands to every CCD product, a CTX orbit prefix expands to that orbit's
    products, etc. — with no instrument-specific logic.

    Returns
    -------
    dict[str, list[str]]
        Mapping from input PID to its resolved PRODUCT_IDs. Duplicate input
        PIDs collapse to a single key.
    """
    col = pid_column(dotted_index_key, df)
    colvals = df[col].astype(str)
    present = set(colvals)
    out: dict[str, list[str]] = {}
    for pid in pids:
        p = str(pid)
        if p in present:
            out[p] = [p]
        elif prefix:
            out[p] = sorted(colvals[colvals.str.startswith(p)].tolist())
        else:
            out[p] = []
    return out
