"""General utilities for working with PDS data.

This module provides common, general-purpose utility functions for the PDS subpackage.
"""

__all__ = [
    "get_mission_names",
    "get_instrument_names",
    "get_index_names",
    "print_available_indexes",
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
