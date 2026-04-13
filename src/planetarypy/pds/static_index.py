"""Static index management with configuration-based URLs.

This module handles indexes with fixed URLs that are managed through configuration files.
Update logic focuses on checking for newer file timestamps at known stable URLs.
"""

import datetime
from pathlib import Path
from urllib.request import URLError
from loguru import logger
from yarl import URL

from .. import utils
from .index_logging import AccessLog



class ConfigHandler(utils.NestedTomlDict):
    """Handler for the statix index URLs configuration file.

    - Download from planetarypy config repo if it is not present.
    - Check once per day if there's an updated version available.
    - Read out the URLs for the static indexes, based on dotted keys like "mro.ctx.edr".

    Parameters
    ----------
    local_path : str | None
        If needed, a local path with more indexes could be provided.    
    
    """
    FNAME = "planetarypy_index_urls.toml"
    BASE_URL = URL(
        "https://raw.githubusercontent.com/planetarypy/planetarypy_configs/refs/heads/main/"
    )
    CONFIG_URL = BASE_URL / FNAME
    CONFIG_PATH = Path.home() / f".{FNAME}"

    def __init__(self, local_path: str | None = None, force_update: bool = False):
        self.path = Path(local_path) if local_path else self.CONFIG_PATH
        self.log = AccessLog("indexes.static.config")
        
        if not self.path.is_file():
            logger.info(f"Downloading fresh static config from {self.CONFIG_URL}.")
            utils.url_retrieve(str(self.CONFIG_URL), self.path, disable_tqdm=True)
            self.log.log_update_time()
        elif force_update or self.should_update:
            self._check_and_update_config()
            
        super().__init__(self.path)

    def _check_and_update_config(self):
        """Check for config updates and notify about new entries."""
        result = utils.compare_remote_file(str(self.CONFIG_URL), self.path)
        
        if result["error"]:
            logger.warning(f"Could not check for config updates: {result['error']}")
            return
            
        if result["has_updates"]:
            # Load old and new configs to compare entries
            old_config = utils.NestedTomlDict(self.path)
            new_config = utils.NestedTomlDict(result["remote_tmp_path"])
            
            # Find new entries by comparing the flattened key sets
            old_keys = self._get_all_keys(old_config.to_dict())
            new_keys = self._get_all_keys(new_config.to_dict())
            added_keys = new_keys - old_keys
            
            if added_keys:
                logger.info(f"New index entries available: {', '.join(sorted(added_keys))}")
            
            # Replace the local config with the updated one
            result["remote_tmp_path"].replace(self.path)
            logger.info(f"Updated static config from {self.CONFIG_URL}")
            self.log.log_update_time()
            
            # Clean up temp file if it still exists
            if result["remote_tmp_path"].exists():
                result["remote_tmp_path"].unlink()
        else:
            logger.debug("Static config is up to date")
            self.log.log_check_time()

    def _get_all_keys(self, d, parent_key=""):
        """Recursively get all dotted keys from a nested dictionary."""
        keys = set()
        for k, v in d.items():
            key = f"{parent_key}.{k}" if parent_key else k
            if isinstance(v, dict):
                keys.update(self._get_all_keys(v, key))
            else:
                keys.add(key)
        return keys

    @property
    def should_update(self) -> bool:
        """Check if the config file should be updated (if older than one day)."""
        last_update = self.log.last_update
        if last_update is None:
            return True
        time_since = datetime.datetime.now() - last_update
        return time_since > datetime.timedelta(days=1)
    
    def get_url(self, key) -> URL:
        return URL(str(self.get(key)))
    
    def _delete(self):
        """Delete the local configuration file."""
        if self.path.is_file():
            self.path.unlink()
            logger.info(f"Deleted static config file at {self.path}")
        else:
            logger.warning(f"Static config file at {self.path} does not exist, cannot delete.") 


class StaticRemoteHandler:
    """Handler for static remote indexes with fixed URLs from configuration.

    This handler deals with all aspects of the status of a remotely stored index file.
    It is a helper class-based attribute for the Index class, to determine if updates are available and if a remote
    check should be performed (limited to once per day).
    This class differs from the DynamicRemoteHandler, as the updates there look for new URLs, not for new files
    at the same URL.

    Note: The "helping" here STOPS after providing the right URL to use for an index key and some logical flags that
    indicate if there's new data at the remote - this class does NOT handle downloading or caching the index file itself.
    From the moment of having an URL and a reason to use it, the actual Index class should take over.
    """

    def __init__(self, index_key: str, force_config_update: bool = False):
        self.index_key = index_key
        self.config = ConfigHandler(force_update=force_config_update)
        self.log = AccessLog(key=index_key)

        self._remote_timestamp = None
        if self.should_check:
            self.get_remote_timestamp()

    @property
    def url(self) -> URL:
        """Get the URL of the static index."""
        return self.config.get_url(self.index_key)
    
    @property
    def should_check(self) -> bool:
        """Determine if an update check should be performed."""
        return self.log.should_check

    def get_remote_timestamp(self) -> datetime.datetime | None:
        """Get the last modified timestamp of the remote index file."""
        try:
            tstamp = utils.get_remote_timestamp(self.url)
        except URLError as e:
            logger.warning(f"Could not retrieve remote timestamp for {self.url}: {e}")
            return None
        else:
            self.log.log_remote_timestamp(tstamp)
            self._remote_timestamp = tstamp
        return tstamp
    
    @property
    def update_available(self) -> bool:
        """Check if an update is available based on remote timestamp."""
        if self.log.update_available:  # no need to check further if we logged this.
            return True
        elif not self.should_check:
            logger.debug(f"Skipping update check for {self.index_key}, checked recently.")
            return False
        
        logged_timestamp = self.log.get(self.index_key, "remote_timestamp")
        if logged_timestamp is not None:
            remote_time = logged_timestamp
        else:
            remote_time = self.remote_timestamp
        
        if remote_time is None:
            return False
        
        last_update = self.log.last_update
        if last_update is None:
            logger.info(f"No previous update logged for {self.index_key}, update available")
            self.log.log_update_available(True)
            return True
        
        if remote_time > last_update:
            logger.info(f"Update available for {self.index_key}: remote timestamp {remote_time} > last update {last_update}")
            self.log.log_update_available(True)
            return True
        else:
            logger.debug(f"No update available for {self.index_key}: remote time {remote_time} <= last update {last_update}")
            return False

