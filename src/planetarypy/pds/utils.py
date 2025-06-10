"""General utilities for working with PDS data.

This module provides common, general-purpose utility functions for the PDS subpackage.
"""

__all__ = [
    "list_missions",
    "list_instruments",
    "list_indexes",
    "list_available_indexes",
    "get_index",
]

from typing import Optional

import pandas as pd

from planetarypy.pds.indexes import Index


def list_missions() -> list[str]:
    """List all available missions in the PDS index configuration.

    Returns:
        List of mission names

    Examples:
        >>> from planetarypy.pds.utils import list_missions
        >>> missions = list_missions()
        >>> print(missions)
        ['cassini', 'go', 'lro', 'mro']
    """
    from .index_config import urls_config

    missions = []
    if "missions" in urls_config.tomldoc:
        # Get all keys that are tables (missions) and not comments or metadata
        missions = [
            key
            for key in urls_config.tomldoc["missions"].keys()
            if hasattr(urls_config.tomldoc["missions"][key], "keys")
        ]

    return sorted(missions)


def list_instruments(mission: str) -> list[str]:
    """List all instruments for a given mission.

    Args:
        mission: Mission name (e.g., 'cassini', 'mro')

    Returns:
        List of instrument names

    Examples:
        >>> from planetarypy.pds.utils import list_instruments
        >>> instruments = list_instruments('cassini')
        >>> print(instruments)
        ['iss', 'uvis']
    """
    from .index_config import urls_config

    instruments = []

    # Check if mission exists in config
    if (
        "missions" in urls_config.tomldoc
        and mission in urls_config.tomldoc["missions"]
        and hasattr(urls_config.tomldoc["missions"][mission], "keys")
    ):
        # Get all keys that are tables (instruments) and not comments or metadata
        instruments = [
            key
            for key in urls_config.tomldoc["missions"][mission].keys()
            if hasattr(urls_config.tomldoc["missions"][mission][key], "keys")
        ]

    return sorted(instruments)


def list_indexes(mission_instrument: str) -> list[str]:
    """List all indexes for a given mission and instrument.

    Args:
        mission_instrument: Dotted index key (e.g., 'cassini.iss')

    Returns:
        List of index names

    Examples:
        >>> from planetarypy.pds.utils import list_indexes
        >>> indexes = list_indexes('cassini', 'iss')
        >>> print(indexes)
        ['index', 'moon_summary', 'ring_summary', 'saturn_summary']
    """
    from .index_config import urls_config

    mission, instrument = mission_instrument.split(".")
    indexes = []
    path = ["missions", mission, instrument]

    # Navigate to the instrument section
    current = urls_config.tomldoc
    for part in path:
        if part not in current or not hasattr(current[part], "keys"):
            return []
        current = current[part]

    # Extract index names (keys with string values, not nested tables)
    for key, value in current.items():
        # Skip comments and check if value is a URL (string) and not a table
        if not str(key).startswith("#") and isinstance(value, str):
            indexes.append(key)

    return sorted(indexes)


def list_available_indexes(
    filter_mission: Optional[str] = None, filter_instrument: Optional[str] = None
) -> None:
    """Print an ASCII tree diagram of all missions, instruments, and indexes.

    This function displays a hierarchical view of the PDS index configuration,
    showing missions, instruments, and indexes in a tree structure.

    Args:
        filter_mission: If provided, only show this mission
        filter_instrument: If provided, only show this instrument
            (filter_mission must also be provided)

    Examples:
        >>> from planetarypy.pds.utils import print_pds_tree
        >>> # Print all missions, instruments, and indexes
        >>> print_pds_tree()
        >>> # Print only information for the 'mro' mission
        >>> print_pds_tree('mro')
        >>> # Print only information for the 'mro' mission's 'ctx' instrument
        >>> print_pds_tree('mro', 'ctx')
    """
    missions = list_missions()

    if filter_mission:
        if filter_mission not in missions:
            print(f"Mission '{filter_mission}' not found.")
            return
        missions = [filter_mission]

    if not missions:
        print("No missions found in configuration.")
        return

    print("PDS Indexes Configuration:")

    for m_idx, mission in enumerate(missions):
        # Mission prefix
        if m_idx == len(missions) - 1:
            m_prefix = "└── "
            m_indent = "    "
        else:
            m_prefix = "├── "
            m_indent = "│   "

        print(f"{m_prefix}{mission}")

        # Get instruments
        instruments = list_instruments(mission)

        if filter_instrument:
            if filter_instrument not in instruments:
                print(
                    f"{m_indent}Instrument '{filter_instrument}' not found in mission '{mission}'."
                )
                continue
            instruments = [filter_instrument]

        for i_idx, instrument in enumerate(instruments):
            # Instrument prefix
            if i_idx == len(instruments) - 1:
                i_prefix = "└── "
                i_indent = "    "
            else:
                i_prefix = "├── "
                i_indent = "│   "

            print(f"{m_indent}{i_prefix}{instrument}")

            # Get indexes
            indexes = list_indexes(f"{mission}.{instrument}")

            for idx_idx, index in enumerate(indexes):
                # Index prefix
                if idx_idx == len(indexes) - 1:
                    idx_prefix = "└── "
                else:
                    idx_prefix = "├── "

                print(f"{m_indent}{i_indent}{idx_prefix}{index}")


def get_index(
    dotted_index_key: str, refresh: bool = False, force: bool = False
) -> "pd.DataFrame":
    """Retrieve a specific index file .

    If the file is not yet downloaded, it will be, its time strings will be
    converted to datetime objects to enable proper time-based filtering, and the
    resulting DataFrame will be written to disk as a Parquet file for future use.

    Args:
        dotted_index_key (e.g. "mro.ctx.edr")
        refresh (bool): If True, check for updates and download the latest index file.
                        Setting this to False will increase performance as it will not
                        check for updated files on the PDS server.
                        Default is False.
        force (bool): If True, force download even if the index is already the newest,
                      useful if the index file broke for some reason, like interrupted
                      download or processing.
                      Default is False.

    Returns:
        pd.DataFrame

    Examples:
        >>> from planetarypy.pds.utils import get_index
        >>> df = get_index('mro.ctx.edr')
    """
    index = Index(dotted_index_key)
    if not index.local_label_path.is_file():
        index.download()
    # only do time-consuming update check if refresh is True
    if (refresh and index.update_available) or force:
        index.download()
    return index.parquet
