"""PDS Tools, to work with Planetary Data System datasets.
"""


from loguru import logger
from pandas import DataFrame

from planetarypy.pds.index_main import Index, InventoryIndex
from planetarypy.pds.utils import (
    get_index_names,
    get_instrument_names,
    get_mission_names,
    print_available_indexes,
)

__all__ = [
    "Index",
    "InventoryIndex",
    "get_index",
    "print_available_indexes",
    "get_mission_names",
    "get_instrument_names",
    "get_index_names",
]


def get_index(
    dotted_index_key: str,
    allow_refresh: bool = False,
    force_refresh: bool = False,
    rebuild_parquet: bool = False,
    force_config_update: bool = False,
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
    
    Returns
    -------
    pandas.DataFrame
        DataFrame containing the index data, read from the local parquet file.
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

    return index.dataframe
