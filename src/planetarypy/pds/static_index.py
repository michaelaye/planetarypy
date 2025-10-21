"""Static index management with configuration-based URLs.

This module handles indexes with fixed URLs that are managed through configuration files.
Update logic focuses on checking for newer file timestamps at known stable URLs.
"""

import datetime
import os
from pathlib import Path
from urllib.request import URLError

import tomlkit
from loguru import logger
from yarl import URL

from .. import utils
from .index_logging import AccessLog



class ConfigHandler(utils.NestedTomlDict):
    """Handler for the statix index URLs configuration file.

    - Download from planetarypy config repo if it is not present.
    - Check once per day if there's an updated version available.
    - Read out the URLs for the static indexes, based on dotted keys like "mro.ctx.edr".
    """
    FNAME = "planetarypy_index_urls.toml"
    BASE_URL = URL(
        "https://raw.githubusercontent.com/planetarypy/planetarypy_configs/refs/heads/main/"
    )
    CONFIG_URL = BASE_URL / FNAME
    CONFIG_PATH = Path.home() / f".{FNAME}"

    def __init__(self, local_path: str | None = None):
        self.path = Path(local_path) if local_path else self.CONFIG_PATH
        self.log = AccessLog("indexes.static.config")
        if not self.path.is_file() or self.should_update:
            logger.info(f"Downloading fresh static config from {self.CONFIG_URL}.")
            utils.url_retrieve(str(self.CONFIG_URL), self.path, disable_tqdm=True)
            self.log.log_update_time()
        super().__init__(self.path)

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
    
    def delete(self):
        """Delete the local configuration file."""
        if self.path.is_file():
            self.path.unlink()
            logger.info(f"Deleted static config file: {self.path}")
        else:
            logger.warning(f"Static config file does not exist: {self.path}")


class StaticRemoteHandler:
    """Represents a static remote index with a fixed URL.
    
    This handler deals with all aspects of the status of a remotely stored index file.
    It is a helper class-based attribute for the Index class, to determine if updates are available and if a remote
    check should be performed (limited to once per day).

    This class differs from the DynamicRemoteHandler, as the updates there look for new URLs, not for new files
    at the same URL.

    Note: The "helping" here STOPS after providing the right URL to use for an index key and some logical flags that
    indicate if there's new data at the remote - this class does NOT handle downloading or caching the index file itself.
    From the moment of having an URL and a reason to use it, the actual Index class should take over.
    """

    def __init__(self, index_key: str):
        self.index_key = index_key
        self.config = ConfigHandler()
        self.log = AccessLog(key=index_key)

    @property
    def url(self) -> URL:
        """Get the URL of the static index."""
        return self.config.get_url(self.index_key)
    
    @property
    def should_check_for_update(self) -> bool:
        """Determine if an update check should be performed."""
        return self.log.should_check

    @property
    def remote_timestamp(self) -> datetime.datetime | None:
        """Get the last modified timestamp of the remote index file."""
        try:
            return utils.get_remote_timestamp(self.url)
        except URLError as e:
            logger.warning(f"Could not retrieve remote timestamp for {self.url}: {e}")
            return None
        
    @property
    def is_update_available(self) -> bool:
        """Check if an update is available based on remote timestamp."""
        remote_time = self.remote_timestamp
        if remote_time is None:
            return False
        
        last_update = self.log.last_update
        if last_update is None:
            logger.info(f"No previous update logged for {self.index_key}, update available")
            return True
        
        if remote_time > last_update:
            logger.info(f"Update available for {self.index_key}: remote time {remote_time} > last update {last_update}")
            return True
        else:
            logger.debug(f"No update available for {self.index_key}: remote time {remote_time} <= last update {last_update}")
            return False
        


class StaticIndex:
    """Manages static index URLs and their updates."""

    def __init__(self, config_path: str | None = None):
        self.config_path = self._get_config_path(config_path)
        self.config_doc = None
        self.load_config()


    def check_for_updates(self) -> bool:
        """Check if static configuration needs updating (once per day)."""
        last_check = access_log.get_timestamp("static_config_check")
        last_download = access_log.get_timestamp("static_config_download")

        # Determine when we last had activity
        timestamps = [t for t in [last_check, last_download] if t is not None]
        if not timestamps:
            logger.info("No static config check timestamp found, checking for updates")
            return self._update_from_remote()

        last_activity = max(timestamps)
        if utils.is_older_than_hours(last_activity, 24):
            logger.info(
                "Last static config check older than one day, checking for updates"
            )
            return self._update_from_remote()
        else:
            logger.debug(
                f"Static config is up to date (last activity: {last_activity})"
            )
            return False

    def _update_from_remote(self) -> bool:
        """Update static configuration from remote source."""
        try:
            logger.info(f"Checking for static config updates from {STATIC_CONFIG_URL}")
            current_content = self.config_path.read_text()
            comparison = utils.compare_remote_content(
                STATIC_CONFIG_URL, current_content, timeout=30
            )

            if comparison["error"]:
                logger.warning(f"Could not update static config: {comparison['error']}")
                access_log.set_timestamp("static_config_check", datetime.datetime.now())
                return False

            if comparison["has_updates"]:
                logger.info("Remote static config has updates, downloading new version")
                self.config_path.write_text(comparison["remote_content"])
                self.load_config(auto_create=False)
                access_log.set_timestamp(
                    "static_config_update", datetime.datetime.now()
                )
                logger.info("Static config updated successfully from remote")
                return True
            else:
                logger.info("Remote static config is identical to local config")
                access_log.set_timestamp("static_config_check", datetime.datetime.now())
                return False

        except Exception as e:
            logger.warning(f"Could not check static config updates: {e}")
            access_log.set_timestamp("static_config_check", datetime.datetime.now())
            return False


