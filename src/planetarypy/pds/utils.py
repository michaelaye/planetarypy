"""General utilities for working with PDS data.

This module provides common, general-purpose utility functions for the PDS subpackage.
"""

__all__ = [
    "get_mission_names",
    "get_instrument_names",
    "get_index_names",
    "print_available_indexes",
    "get_example_pid",
]

from .static_index import ConfigHandler
from .dynamic_index import DYNAMIC_URL_HANDLERS


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
    """Return a sorted list of all instruments for a given mission (from static and dynamic configs)."""
    keys = _all_dotted_index_keys()
    instruments = set()
    for k in keys:
        parts = k.split(".")
        if len(parts) >= 2 and parts[0] == mission:
            instruments.add(parts[1])
    return sorted(instruments)


def get_index_names(mission_instrument: str) -> list[str]:
    """Return a sorted list of all index names for a given mission and instrument (from static and dynamic configs)."""
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
        # Skip "UNK" placeholder rows (e.g. early Galileo SSI cruise frames)
        # but fall back to it if every row is UNK.
        non_unk = series[series.str.upper() != "UNK"]
        picked = non_unk.iloc[0] if not non_unk.empty else series.iloc[0]
        return _bare_pid(picked)

    raise ValueError(
        f"No product-id column found in index {instr_key!r}. "
        f"Tried: {list(candidate_cols)}. Available columns: {list(df.columns)}"
    )
