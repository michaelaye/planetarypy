"""General utilities for working with PDS data.

This module provides common, general-purpose utility functions for the PDS subpackage.
"""

__all__ = [
    "complete_pid",
    "get_mission_names",
    "get_instrument_names",
    "get_index_names",
    "print_available_indexes",
    "get_example_pid",
    "get_meta",
    "read_index_slice",
    "rebuild_pid_cache",
    "reorder_meta_row",
    "pid_column",
]

from typing import TYPE_CHECKING

from .static_index import ConfigHandler
from .dynamic_index import DYNAMIC_URL_HANDLERS

if TYPE_CHECKING:
    import pandas as pd


def _all_dotted_index_keys() -> list[str]:
    """Return all dotted index keys mission.instrument.index from static and dynamic sources.

    Parameters
    ----------
    None

    Returns
    -------
    list[str]
        Sorted list of all available dotted index keys.
    """
    # Static: flatten nested config to dotted keys
    config = ConfigHandler()
    config_doc = config.to_dict()

    static_keys: set[str] = set()

    def _collect_keys(d: dict, parent: str = ""):
        for k, v in d.items():
            new_parent = f"{parent}.{k}" if parent else k
            if isinstance(v, dict):
                _collect_keys(v, new_parent)
            else:
                static_keys.add(new_parent)

    _collect_keys(config_doc)

    # Dynamic keys are already dotted
    dynamic_keys = set(DYNAMIC_URL_HANDLERS.keys())

    return sorted(static_keys | dynamic_keys)


def get_mission_names() -> list[str]:
    """Return a sorted list of all available missions (from static and dynamic configs)."""
    keys = _all_dotted_index_keys()
    missions = {k.split(".")[0] for k in keys if k}
    return sorted(missions)


def get_instrument_names(mission: str) -> list[str]:
    """Return a sorted list of all instruments for a given mission.

    Drawn from both static and dynamic configs.
    """
    keys = _all_dotted_index_keys()
    instruments = set()
    for k in keys:
        parts = k.split(".")
        if len(parts) >= 2 and parts[0] == mission:
            instruments.add(parts[1])
    return sorted(instruments)


def get_index_names(mission_instrument: str) -> list[str]:
    """Return a sorted list of all index names for a given mission and instrument.

    Drawn from both static and dynamic configs.
    """
    mission, instrument = mission_instrument.split(".")
    keys = _all_dotted_index_keys()
    indexes = set()
    for k in keys:
        parts = k.split(".")
        if len(parts) >= 3 and parts[0] == mission and parts[1] == instrument:
            indexes.add(".".join(parts[2:]))
    return sorted(indexes)


def print_available_indexes(
    filter_mission: str | None = None,
    filter_instrument: str | None = None,
    *,
    keys_only: bool = False,
) -> list[str] | None:
    """List available index keys from static config plus dynamic handlers.

    Combines all dotted index keys found in the remote static configuration
    with the dynamic indexes registered in ``DYNAMIC_URL_HANDLERS``.

    When ``keys_only`` is False (default), prints a tree of missions → instruments → indexes,
    optionally filtered by mission/instrument. When ``keys_only`` is True, returns a sorted
    list of dotted index keys instead of printing.

    Args:
        filter_mission: If provided, only include this mission
        filter_instrument: If provided, only include this instrument (requires filter_mission)
        keys_only: When True, return a list of keys instead of printing a tree

    Returns:
        - list[str] when ``keys_only`` is True
        - None when printing a tree (``keys_only`` is False)

    Examples:
        >>> from planetarypy.pds.utils import print_available_indexes
        >>> print_available_indexes(keys_only=True)  # returns ["cassini.iss.index", ...]
        >>> print_available_indexes('mro')           # prints tree for mro only
        >>> print_available_indexes('mro', 'ctx')    # prints tree for mro.ctx only
    """
    # Gather all keys once, then filter
    all_keys = set(_all_dotted_index_keys())

    def _passes_filters(key: str) -> bool:
        if filter_mission and not key.startswith(filter_mission + "."):
            return False
        if filter_instrument:
            parts = key.split(".")
            if len(parts) < 2:
                return False
            if not (parts[0] == filter_mission and parts[1] == filter_instrument):
                return False
        return True

    filtered_keys = sorted(k for k in all_keys if _passes_filters(k))

    if keys_only:
        return filtered_keys

    # 4) Print tree structure
    if not filtered_keys:
        print("No indexes found for the given filters.")
        return None

    print("PDS Indexes Configuration:")
    # Build nested dict for printing
    tree: dict[str, dict[str, list[str]]] = {}
    for key in filtered_keys:
        m, i, idx = key.split(".", 2)
        tree.setdefault(m, {}).setdefault(i, []).append(idx)

    missions = sorted(tree.keys())
    for m_idx, mission in enumerate(missions):
        m_prefix = "└── " if m_idx == len(missions) - 1 else "├── "
        m_indent = "    " if m_idx == len(missions) - 1 else "│   "
        print(f"{m_prefix}{mission}")

        instruments = sorted(tree[mission].keys())
        for i_idx, instrument in enumerate(instruments):
            i_prefix = "└── " if i_idx == len(instruments) - 1 else "├── "
            i_indent = "    " if i_idx == len(instruments) - 1 else "│   "
            print(f"{m_indent}{i_prefix}{instrument}")

            indexes = sorted(tree[mission][instrument])
            for idx_idx, index in enumerate(indexes):
                idx_prefix = "└── " if idx_idx == len(indexes) - 1 else "├── "
                print(f"{m_indent}{i_indent}{idx_prefix}{index}")

    return None


# Fallback product-id columns to try when an index has no matching
# IndexConfig entry in the catalog registry. Order matters.
_PID_COL_FALLBACKS = ("PRODUCT_ID", "FILE_NAME", "IMAGE_ID", "OBSERVATION_ID")


def pid_column(index_key: str, df: "pd.DataFrame") -> str:
    """Resolve the product-id column name for ``index_key`` against ``df``.

    Mirrors the resolution order used by the PID-completion cache:

      1. ``IndexConfig.product_id_col`` for the matching registry entry, if any.
      2. ``_PID_COL_FALLBACKS`` (``PRODUCT_ID``, ``FILE_NAME``, ``IMAGE_ID``,
         ``OBSERVATION_ID``) — in that order.

    Whichever candidate is actually present in ``df.columns`` wins.

    Parameters
    ----------
    index_key : str
        Dotted index key, e.g. ``'mro.ctx.edr'``.
    df : pandas.DataFrame
        DataFrame to inspect — the column must exist there to be returned.

    Returns
    -------
    str
        Column name to use as the product-id column.

    Raises
    ------
    KeyError
        If no candidate column is present in ``df``.
    """
    candidate_cols: list[str] = []
    try:
        from planetarypy.catalog._index_resolver import INDEX_REGISTRY
        for cfg in INDEX_REGISTRY.values():
            if (cfg.index_key == index_key
                    or index_key in cfg.extra_index_keys):
                candidate_cols.append(cfg.product_id_col)
                break
    except Exception:
        # Catalog registry isn't required for fallback resolution.
        pass
    for fallback in _PID_COL_FALLBACKS:
        if fallback not in candidate_cols:
            candidate_cols.append(fallback)

    for col in candidate_cols:
        if col in df.columns:
            return col
    raise KeyError(
        f"No suitable product-id column found in {index_key!r}. "
        f"Tried: {candidate_cols!r}; available columns: {list(df.columns)!r}"
    )

# Known PDS file extensions to strip when the picked value is a filename
# rather than a bare PID (e.g. cassini.uvis.index stores
# "/COUVIS_0001/DATA/D1999_007/EUV1999_007_17_05.LBL" in its FILE_NAME column).
_PDS_FILE_EXTENSIONS = (
    ".LBL", ".IMG", ".TAB", ".DAT", ".FIT", ".JP2", ".QUB", ".XML",
)

import re as _re  # noqa: E402

_PDS_EXT_RE = _re.compile(
    r"\.(?:" + "|".join(e.lstrip(".") for e in _PDS_FILE_EXTENSIONS) + r")$",
    _re.IGNORECASE,
)
# Trailing flight-software / version suffix on otherwise-bare PIDs, e.g.
# cassini.iss.index stores "1_N1454725799.122" where ".122" is the
# FLIGHT_SOFTWARE_VERSION_ID, not part of the PID itself.
_VERSION_SUFFIX_RE = _re.compile(r"\.\d+$")


def _index_config_for(index_key: str):
    """Return the catalog ``IndexConfig`` matching ``index_key``, or None."""
    try:
        from planetarypy.catalog._index_resolver import INDEX_REGISTRY
        for cfg in INDEX_REGISTRY.values():
            if cfg.index_key == index_key or index_key in cfg.extra_index_keys:
                return cfg
    except Exception:
        pass
    return None


def _normalize_pid(value: str, index_key: str | None = None) -> str:
    """Bare-PID normalization with optional per-index prefix stripping.

    Applies :func:`_bare_pid` first (path/extension and ``.NNN`` version
    suffix); then, if ``index_key`` resolves to an :class:`IndexConfig`
    with a non-empty ``pid_strip_prefix_re``, removes that pattern from
    the start (e.g. cassini.iss ``1_N1454725799`` → ``N1454725799``).

    Idempotent: passing an already-normalized value through again is a
    no-op, so the same function can normalize both stored values and
    user input before comparison.
    """
    bare = _bare_pid(value)
    if index_key is not None:
        cfg = _index_config_for(index_key)
        if cfg is not None and cfg.pid_strip_prefix_re:
            bare = _re.sub(cfg.pid_strip_prefix_re, "", bare)
    return bare


def _bare_pid(value: str) -> str:
    """Normalize a PDS index value into its bare product identifier.

    Two-step strategy:
    1. If the value's basename ends in a known PDS file extension
       (``.LBL``, ``.IMG``, ``.TAB`` …), treat it as a path-with-file
       and return the bare stem (drops directories *and* extension).
       Handles e.g. cassini.uvis.index FILE_NAME values.
    2. Otherwise, strip a trailing ``.<digits>`` version suffix only.
       Keeps slashes that are part of the PID intact (e.g. mgs.moc.edr
       PRODUCT_IDs of the form ``FHA/00435`` where ``/`` is a separator,
       not a path).
    """
    from pathlib import PurePosixPath

    name = PurePosixPath(value).name or value
    if _PDS_EXT_RE.search(name):
        return _PDS_EXT_RE.sub("", name)
    return _VERSION_SUFFIX_RE.sub("", value)


def get_example_pid(instr_key: str) -> str:
    """Return an example product ID from a registered PDS index.

    Looks up an arbitrary product identifier from the cumulative index
    associated with ``instr_key`` — useful for CLI examples, completion
    seeds, smoke tests, and notebook demos.

    Parameters
    ----------
    instr_key : str
        Dotted index key in the form ``<mission>.<instrument>.<index>``,
        e.g. ``"mro.ctx.edr"`` or ``"cassini.iss.index"``. Must be one of
        the keys registered in ``~/.planetarypy_index_urls.toml`` or in
        the dynamic handler registry.

    Returns
    -------
    str
        A product identifier from the index, stripped of PDS whitespace
        padding.

    Raises
    ------
    ValueError
        If ``instr_key`` is not a registered index, or if the index
        contains no recognizable product-id column.
    """
    registered = set(_all_dotted_index_keys())
    if instr_key not in registered:
        raise ValueError(
            f"Unknown index key: {instr_key!r}. "
            "Use planetarypy.pds.print_available_indexes() to list valid keys."
        )

    # Prefer the product-id column configured in the catalog INDEX_REGISTRY
    # for this index (handles non-standard cases like UVIS using FILE_NAME),
    # then fall back to common conventions.
    from planetarypy.catalog._index_resolver import INDEX_REGISTRY

    candidate_cols: list[str] = []
    for cfg in INDEX_REGISTRY.values():
        if cfg.index_key == instr_key or instr_key in cfg.extra_index_keys:
            candidate_cols.append(cfg.product_id_col)
            break
    for fallback in _PID_COL_FALLBACKS:
        if fallback not in candidate_cols:
            candidate_cols.append(fallback)

    # Lazy import: planetarypy.pds.__init__ imports from this module.
    from planetarypy.pds import get_index

    df = get_index(instr_key, allow_refresh=False)

    for col in candidate_cols:
        if col not in df.columns:
            continue
        series = df[col].dropna().astype(str).str.strip()
        series = series[series != ""]
        if series.empty:
            continue
        # Skip "UNK" placeholder rows (early Galileo SSI cruise frames) and
        # CRU-prefixed product IDs (MRO/CTX cruise data — not representative
        # of mapping-phase products). Fall back to them if nothing else exists.
        usable = series[
            (series.str.upper() != "UNK")
            & ~series.str.upper().str.startswith("CRU")
        ]
        picked = usable.iloc[0] if not usable.empty else series.iloc[0]
        # Use the per-index normalization (path/extension + version suffix +
        # IndexConfig.pid_strip_prefix_re) so the example matches the
        # canonical user-facing form returned by complete_pid and accepted
        # by get_meta — e.g. cassini.iss → 'N1454725799', not '1_N1454725799'.
        return _normalize_pid(picked, instr_key)

    raise ValueError(
        f"No product-id column found in index {instr_key!r}. "
        f"Tried: {list(candidate_cols)}. Available columns: {list(df.columns)}"
    )


def read_index_slice(
    index_key: str,
    filters=None,
    columns: list[str] | None = None,
):
    """Read a column-projected, predicate-pushed-down slice of a PDS index.

    Use this in place of :func:`get_index` whenever you only need a few
    columns or a few rows. Parquet is columnar at the storage level, so
    column projection avoids reading the bytes of unused columns;
    predicate pushdown filters at the row-group level via min/max
    statistics, skipping irrelevant chunks entirely. On the HiRISE EDR
    index (2.6 M rows) a per-obsid lookup goes from ~3.3 s to ~0.03 s.

    Parameters
    ----------
    index_key : str
        Dotted index key (e.g. ``"mro.hirise.edr"``).
    filters : list, optional
        PyArrow predicate-pushdown filters in the form
        ``[(column, op, value)]`` or DNF-style nested lists. ``op`` is
        one of ``"="``, ``"!="``, ``"<"``, ``"<="``, ``">"``, ``">="``,
        ``"in"``, ``"not in"``. Values are compared byte-equal — match
        the canonical (typically uppercase, unpadded) form stored in
        parquet.
    columns : list of str, optional
        Subset of columns to read. ``None`` (default) loads all columns.

    Returns
    -------
    pandas.DataFrame
        The filtered, projected slice. Empty if no rows match.
    """
    import pandas as pd

    from planetarypy.pds import Index

    idx = Index(index_key)
    idx.ensure_parquet()
    return pd.read_parquet(
        idx.local_parq_path, columns=columns, filters=filters,
    )


def _pid_cache_path(index_key: str):
    """Per-index sorted PID-cache file under the configured storage_root."""
    from pathlib import Path

    from planetarypy.config import config

    parts = index_key.split(".")
    if len(parts) != 3:
        raise ValueError(
            f"Expected dotted key 'mission.instrument.indexname', got {index_key!r}"
        )
    mission, instrument, indexname = parts
    return Path(config.storage_root) / mission / instrument / f"pids_{indexname}.txt"


def _completion_id_col_for(index_key: str) -> str:
    """Column to use for shell tab completion of an index.

    Honors :attr:`IndexConfig.completion_id_col` when set (e.g. HiRISE
    surfaces ``OBSERVATION_ID`` so users tab through obsids, not the
    28× more numerous channel PRODUCT_IDs); otherwise falls back to
    ``product_id_col``, then to ``PRODUCT_ID``.
    """
    try:
        from planetarypy.catalog._index_resolver import INDEX_REGISTRY
        for cfg in INDEX_REGISTRY.values():
            if cfg.index_key == index_key or index_key in cfg.extra_index_keys:
                return cfg.completion_id_col or cfg.product_id_col
    except Exception:
        pass
    return "PRODUCT_ID"


def rebuild_pid_cache(index_key: str):
    """Rebuild the sorted bare-PID cache for an index.

    Reads only the configured completion column via column projection,
    normalizes each value through :func:`_normalize_pid` (PDS
    path/extension, version suffix, and per-index prefix), uppercases
    for prefix matching, deduplicates, and writes a sorted text file.
    """
    cache = _pid_cache_path(index_key)
    cache.parent.mkdir(parents=True, exist_ok=True)
    pid_col = _completion_id_col_for(index_key)
    df = read_index_slice(index_key, columns=[pid_col])
    series = (
        df[pid_col]
        .dropna()
        .astype(str)
        .str.strip()
    )
    series = series[series != ""]
    bare = series.map(lambda v: _normalize_pid(v, index_key)).str.upper()
    bare = bare[bare != ""].drop_duplicates().sort_values()
    cache.write_text("\n".join(bare) + "\n")
    return cache


def complete_pid(
    incomplete: str, index_key: str, *, max_results: int = 50,
) -> list[str]:
    """Return registered product IDs matching a prefix, for tab completion.

    Backed by a sorted text cache built on first use from the index's
    configured product-id column (``PRODUCT_ID`` for most indexes,
    ``FILE_NAME`` for cassini.uvis, etc.). PDS path/extension and
    flight-software version suffixes are normalized away so the user
    types the bare PID they expect (``EUV1999_007_17_05`` not
    ``/COUVIS_0001/.../EUV1999_007_17_05.LBL``; ``1_N1454725799`` not
    ``1_N1454725799.122``).

    Parameters
    ----------
    incomplete : str
        Prefix to match against (case-insensitive).
    index_key : str
        Dotted index key, e.g. ``"cassini.uvis.index"``.
    max_results : int
        Cap on matches (default 50, plenty for shell completion).

    Returns
    -------
    list of str
        Sorted, uppercased matches. Empty list if anything goes wrong
        (the shell completion path must never raise).
    """
    try:
        cache = _pid_cache_path(index_key)
        if not cache.is_file():
            rebuild_pid_cache(index_key)
    except Exception:
        return []

    prefix = incomplete.upper()
    matches: list[str] = []
    try:
        with open(cache) as f:
            for line in f:
                pid = line.rstrip()
                if pid.startswith(prefix):
                    matches.append(pid)
                    if len(matches) >= max_results:
                        break
                elif matches:
                    # Sorted file: stop once we pass the prefix range.
                    break
    except Exception:
        return []
    return matches


def reorder_meta_row(row):
    """Reorder a meta row's fields to ``[*_ID/FILE_NAME] + [*ANGLE*] + rest``.

    Used by the generic ``get_meta`` path and by per-instrument handlers
    that want the same ordering for their long-form output.
    """

    def _is_id_field(name: str) -> bool:
        upper = str(name).upper()
        return (
            "_ID" in upper
            or upper in {
                "FILE_NAME", "FILE_SPECIFICATION_NAME",
                "FILE_NAME_SPECIFICATION", "PATH_NAME",
            }
        )

    cols = list(row.index)
    id_cols = [c for c in cols if _is_id_field(c)]
    angle_cols = [c for c in cols
                  if "ANGLE" in str(c).upper() and c not in id_cols]
    rest = [c for c in cols if c not in id_cols and c not in angle_cols]
    return row.reindex(id_cols + angle_cols + rest)


def get_meta(instr_key: str, product_id: str, long: bool = False):
    """Return the metadata row for a product ID from a registered PDS index.

    Generalizes lookup across all indexes by trying the catalog-registered
    product-id column first, then common fallbacks. Matching is tolerant
    of case differences and PDS path/extension/version-suffix decoration
    (same normalization as ``_bare_pid``).

    Indexes registered in :mod:`planetarypy.pds.meta_display` get
    instrument-specific shaping (e.g. HiRISE EDR collapses one
    observation's 28 channel rows into a short per-color summary).

    Parameters
    ----------
    instr_key : str
        Dotted index key, e.g. ``"mro.ctx.edr"`` or ``"cassini.iss.index"``.
    product_id : str
        Product identifier to look up. May be bare (e.g.
        ``"P02_001916_2221_XI_42N027W"``) or include a PDS path/extension.
    long : bool
        Per-instrument long-form toggle (currently used by HiRISE: with an
        obsid input, picks the RED3_1 channel and returns the full row).
        Generic indexes ignore this flag.

    Returns
    -------
    pandas.Series
        The matched row, indexed by column name and pre-ordered for display.
        String values are stripped of PDS whitespace padding.

    Raises
    ------
    ValueError
        If ``instr_key`` is not registered, or no row matches ``product_id``.
    """
    registered = set(_all_dotted_index_keys())
    if instr_key not in registered:
        raise ValueError(
            f"Unknown index key: {instr_key!r}. "
            "Use planetarypy.pds.print_available_indexes() to list valid keys."
        )

    # Per-instrument override: HiRISE etc. own their entire match/shape pipeline.
    from planetarypy.pds.meta_display import get_handler

    handler = get_handler(instr_key)
    if handler is not None:
        return handler(instr_key, product_id, long=long)

    # Prefer the configured product-id column when this index has an entry
    # in the catalog INDEX_REGISTRY (handles non-standard cases like UVIS
    # using FILE_NAME); otherwise fall back to common conventions.
    from planetarypy.catalog._index_resolver import INDEX_REGISTRY

    candidate_cols: list[str] = []
    for cfg in INDEX_REGISTRY.values():
        if cfg.index_key == instr_key or instr_key in cfg.extra_index_keys:
            candidate_cols.append(cfg.product_id_col)
            break
    for fallback in _PID_COL_FALLBACKS:
        if fallback not in candidate_cols:
            candidate_cols.append(fallback)

    from planetarypy.pds import get_index

    df = get_index(instr_key, allow_refresh=False)

    pid = str(product_id).strip()
    pid_norm = _normalize_pid(pid, instr_key)
    # Bare/normalized pass is only meaningful when at least one side is
    # decorated (path/extension/version suffix or per-index prefix). On
    # bare-vs-bare it's a slow no-op — mro.hirise.edr's 2.6M-row
    # PRODUCT_ID column would burn ~4 s for nothing.
    input_is_decorated = pid_norm != pid
    pid_norm_upper = pid_norm.upper()

    # Cheap probe: does this column carry decoration anywhere in a sample?
    _DECORATION_RE = _re.compile(
        r"\.\d+$|\.(?:lbl|img|tab|dat|fit|jp2|qub|xml)$", _re.IGNORECASE,
    )

    for col in candidate_cols:
        if col not in df.columns:
            continue
        series = df[col].astype(str).str.strip()

        mask = series == pid
        if not mask.any():
            mask = series.str.upper() == pid.upper()
        if not mask.any():
            # Skip the per-row Python apply when neither side could possibly
            # need normalization.
            sample = series.head(500)
            column_is_decorated = sample.str.contains(
                _DECORATION_RE, regex=True, na=False
            ).any()
            cfg = _index_config_for(instr_key)
            has_index_prefix = bool(cfg and cfg.pid_strip_prefix_re)
            if input_is_decorated or column_is_decorated or has_index_prefix:
                mask = (
                    series.apply(lambda v: _normalize_pid(v, instr_key))
                    .str.upper() == pid_norm_upper
                )
        if mask.any():
            row = df.loc[mask].iloc[-1].copy()
            # Strip PDS whitespace padding from string values for display.
            for k, v in row.items():
                if isinstance(v, str):
                    row[k] = v.strip()
            return reorder_meta_row(row)

    raise ValueError(
        f"Product {product_id!r} not found in index {instr_key!r}. "
        f"Tried columns: {[c for c in candidate_cols if c in df.columns]}."
    )
