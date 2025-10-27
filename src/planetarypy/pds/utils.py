"""General utilities for working with PDS data.

This module provides common, general-purpose utility functions for the PDS subpackage.
"""

__all__ = [
    "list_missions",
    "list_instruments",
    "list_indexes",
    "list_available_indexes",
    "simple_replace_in_file",
]


import pandas as pd


def simple_replace_in_file(filename, old_text, new_text):
    """Simple replacement of text in a file."""
    with open(filename, "r") as file:
        content = file.read()

    # Simple string replacement
    content = content.replace(old_text, new_text)

    with open(filename, "w") as file:
        file.write(content)

    print(f"Replaced '{old_text}' with '{new_text}' in {filename}")


def list_missions(config_doc: dict = None) -> list[str]:
    """List all available missions in the PDS index configuration.

    Args:
        config_doc: Optional pre-loaded config dict to avoid re-loading

    Returns:
        List of mission names

    Examples:
        >>> from planetarypy.pds.utils import list_missions
        >>> list_missions()
        ['cassini', 'go', 'lro', 'mro']
    """
    if config_doc is None:
        from .index_config import load_config

        config_doc = load_config()

    return list(config_doc.keys())


def list_instruments(mission: str, config_doc: dict = None) -> list[str]:
    """List all instruments for a given mission.

    Args:
        mission: Mission name (e.g., 'cassini', 'mro')
        config_doc: Optional pre-loaded config dict to avoid re-loading

    Returns:
        List of instrument names

    Examples:
        >>> from planetarypy.pds.utils import list_instruments
        >>> list_instruments('cassini')
        ['iss', 'uvis']
    """
    if config_doc is None:
        from .index_config import load_config

        config_doc = load_config()

    return list(config_doc[mission].keys())


def list_indexes(mission_instrument: str, config_doc: dict = None) -> list[str]:
    """List all indexes for a given mission and instrument.

    Args:
        mission_instrument: Dotted mission.instrument key (e.g., 'cassini.iss')
        config_doc: Optional pre-loaded config dict to avoid re-loading

    Returns:
        List of index names

    Examples:
        >>> from planetarypy.pds.utils import list_indexes
        >>> list_indexes('cassini.iss')
        ['index', 'moon_summary', 'ring_summary', 'saturn_summary']
    """
    if config_doc is None:
        from .index_config import load_config

        config_doc = load_config()

    mission, instrument = mission_instrument.split(".")
    return list(config_doc[mission][instrument].keys())


def list_available_indexes(
    filter_mission: str | None = None, filter_instrument: str | None = None
) -> None:
    """Print an ASCII tree diagram of all missions, instruments, and indexes.

    This function displays a hierarchical view of the PDS index configuration,
    showing missions, instruments, and indexes in a tree structure.

    Args:
        filter_mission: If provided, only show this mission
        filter_instrument: If provided, only show this instrument
            (filter_mission must also be provided)

    Examples:
        >>> from planetarypy.pds.utils import list_available_indexes
        >>> # Print all missions, instruments, and indexes
        >>> list_available_indexes()
        >>> # Print only information for the 'mro' mission
        >>> list_available_indexes('mro')
        >>> # Print only information for the 'mro' mission's 'ctx' instrument
        >>> list_available_indexes('mro', 'ctx')
    """
    # Load config once and pass it to all functions
    from .index_config import load_config

    config_doc = load_config()

    missions = list_missions(config_doc)

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
        instruments = list_instruments(mission, config_doc)

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
            indexes = list_indexes(f"{mission}.{instrument}", config_doc)

            for idx_idx, index in enumerate(indexes):
                # Index prefix
                if idx_idx == len(indexes) - 1:
                    idx_prefix = "└── "
                else:
                    idx_prefix = "├── "

                print(f"{m_indent}{i_indent}{idx_prefix}{index}")

