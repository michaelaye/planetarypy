"""
PDS Tools, to work with Planetary Data System datasets.

Includes tools specifically designed for PDS3 label parsing and data format understanding.
"""
from loguru import logger
from pandas import DataFrame
from planetarypy.pds.index_main import Index


def get_index(
    dotted_index_key: str,
    allow_refresh: bool = False,
    force_refresh: bool = False,
    rebuild_parquet: bool = False,
) -> DataFrame:
    """Retrieve a specific index file .

    If the file is not yet downloaded, it will be, its time strings will be
    converted to datetime objects to enable proper time-based filtering, and the
    resulting DataFrame will be written to disk as a Parquet file for future use.

    Args:
        dotted_index_key (e.g. "mro.ctx.edr")
        allow_refresh (bool): 
            If True, check for updates and download the latest index file.
            Setting this to False will increase performance as it will not check for 
            updated files on the PDS server.
            Default is False.
        force_refresh (bool): 
            If True, force download even if the index is already the newest,
            useful if the index file broke for some reason, like interrupted
            download or processing.
            Default is False.
        rebuild_parquet (bool):
            If True, rebuild the parquet cache from existing label+table files
            without re-downloading. Useful if conversion failed but download succeeded.
            Default is False.

    Returns:
        pd.DataFrame

    Examples:
        >>> from loguru import logger
        >>> logger.enable("planetarypy")  # Enable INFO-level logging (pre-configured)
        >>> from planetarypy.pds import get_index
        >>> df = get_index('mro.ctx.edr')
        
        >>> # Force refresh to get latest version
        >>> df = get_index('mro.ctx.edr', force_refresh=True)
        
        >>> # Rebuild parquet only (no download)
        >>> df = get_index('mro.ctx.edr', rebuild_parquet=True)
    """
    index = Index(dotted_index_key)
    # Ensure parquet exists; optionally rebuild from existing files only
    downloaded = index.ensure_parquet(force=rebuild_parquet)

    # Skip refresh logic if we just downloaded in ensure_parquet
    if not downloaded:
        if (allow_refresh and index.update_available) or force_refresh:
            logger.debug(f"Refreshing index {dotted_index_key}, downloading latest version.")
            index.download()
        elif index.update_available:
            # Warn user that an update is available but not being downloaded
            logger.warning(
                f"Update available for {dotted_index_key}. "
                "Call get_index() with allow_refresh=True to download the latest version."
            )

    return index.dataframe
