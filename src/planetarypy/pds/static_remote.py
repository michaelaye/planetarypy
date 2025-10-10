"""Static Remote URL management for PDS indexes.

This module handles URL resolution and update checking for indexes with static,
configuration-based URLs where updates happen as newer files at the same URL.
"""

__all__ = ["StaticRemoteManager", "StaticRemote", "get_static_manager"]

import datetime
import os
from pathlib import Path

import pooch
import tomlkit
from loguru import logger
from yarl import URL

from planetarypy import utils
from planetarypy.pds.index_logging import access_log

# Static configuration settings
STATIC_CONFIG_FNAME = "planetarypy_index_urls.toml"
STATIC_BASE_URL = URL(
    "https://raw.githubusercontent.com/planetarypy/planetarypy_configs/refs/heads/main/"
)
STATIC_CONFIG_URL = STATIC_BASE_URL / STATIC_CONFIG_FNAME


class StaticRemoteManager:
    """Unified static URL configuration manager.

    This consolidates the functionality from IndexConfigManager into the static remote system.
    """

    def __init__(self, config_path: str | None = None, auto_create: bool = True):
        self.config_path = self._get_config_path(config_path)
        self.config_doc = None
        self.load_config(auto_create=auto_create)

    def _get_config_path(self, custom_path: str | None = None) -> Path:
        """Get path to static index configuration file."""
        if custom_path:
            return Path(custom_path)
        return Path(
            os.getenv("PLANETARYPY_INDEX_URLS", Path.home() / f".{STATIC_CONFIG_FNAME}")
        )

    def load_config(self, auto_create: bool = True) -> dict:
        """Load static URL configuration from file."""
        if not self.config_path.exists():
            if not auto_create:
                raise FileNotFoundError(f"Config file not found: {self.config_path}")
            logger.info("Config file doesn't exist, creating default configuration")
            self.create_default_config()
        else:
            logger.debug(f"Reading static config from: {self.config_path}")

        content = self.config_path.read_text()
        self.config_doc = tomlkit.loads(content)
        return self.config_doc

    def create_default_config(self):
        """Download and create default configuration."""
        try:
            logger.info(f"Downloading index URLs config from {STATIC_CONFIG_URL}")
            # Ensure directory exists
            self.config_path.parent.mkdir(parents=True, exist_ok=True)

            downloaded_file = pooch.retrieve(
                url=str(STATIC_CONFIG_URL),
                known_hash=None,  # Always get latest
                path=pooch.os_cache("planetarypy"),
                fname=STATIC_CONFIG_FNAME,
            )

            with open(downloaded_file, "r") as f:
                remote_content = f.read()

            # Validate TOML content
            _ = tomlkit.loads(remote_content)

            # Save to local config path
            self.config_path.write_text(remote_content)
            logger.info(f"Created default config at {self.config_path}")
            access_log.set_timestamp("static_config_download", datetime.datetime.now())

        except Exception as e:
            logger.error(f"Could not download config from {STATIC_CONFIG_URL}: {e}")
            raise RuntimeError(f"Failed to download static configuration: {e}") from e

    def get_url(self, key: str) -> str:
        """Get URL for a static index key."""
        if self.config_doc is None:
            self.load_config()

        try:
            # First try flat key (legacy support)
            if key in self.config_doc:
                return str(self.config_doc.get(key, ""))

            # Try nested structure: mission.instrument.index
            parts = key.split(".")
            if len(parts) >= 3:
                mission, instrument, index_name = parts[0], parts[1], parts[2]

                # First try: section_key = "mission.instrument"
                section_key = f"{mission}.{instrument}"
                if section_key in self.config_doc and isinstance(
                    self.config_doc[section_key], dict
                ):
                    return str(self.config_doc[section_key].get(index_name, ""))

                # Second try: nested under mission key
                if (
                    mission in self.config_doc
                    and isinstance(self.config_doc[mission], dict)
                    and instrument in self.config_doc[mission]
                    and isinstance(self.config_doc[mission][instrument], dict)
                ):
                    return str(self.config_doc[mission][instrument].get(index_name, ""))

            return ""
        except (KeyError, TypeError) as e:
            logger.error(f"Error getting URL for {key}: {e}")
            return ""

    def set_url(self, key: str, url: str):
        """Set URL for a static index key."""
        if self.config_doc is None:
            self.load_config()

        # Handle nested structure for 3-part keys
        parts = key.split(".")
        if len(parts) >= 3:
            mission, instrument, index_name = parts[0], parts[1], parts[2]
            section_key = f"{mission}.{instrument}"

            # Ensure the section exists
            if section_key not in self.config_doc:
                self.config_doc[section_key] = {}
            elif not isinstance(self.config_doc[section_key], dict):
                self.config_doc[section_key] = {}

            self.config_doc[section_key][index_name] = url
        else:
            # Fall back to flat key
            self.config_doc[key] = url

        self.save_config()

    def save_config(self):
        """Save configuration to file."""
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.config_path, "w") as f:
            tomlkit.dump(self.config_doc, f)

    def check_for_updates(self) -> bool:
        """Check if static configuration needs updating (once per day)."""
        last_check = access_log.get_timestamp("static_config_check")
        last_download = access_log.get_timestamp("static_config_download")

        # Determine when we last had activity
        last_activity = max(
            last_check or datetime.datetime.min, last_download or datetime.datetime.min
        )

        if last_activity and not utils.is_older_than_hours(last_activity, 24):
            logger.debug(f"Static config checked recently (last: {last_activity})")
            return False

        logger.info("Checking static configuration for updates")
        access_log.set_timestamp("static_config_check", datetime.datetime.now())

        try:
            # Check if remote configuration has newer content
            current_content = (
                self.config_path.read_text() if self.config_path.exists() else ""
            )
            remote_different = utils.compare_remote_content(
                str(STATIC_CONFIG_URL), current_content
            )

            if remote_different:
                logger.info("Newer static configuration available, downloading")
                self.create_default_config()
                return True
            else:
                logger.debug("Static configuration is up to date")
                return False

        except Exception as e:
            logger.warning(f"Could not check static configuration for updates: {e}")
            return False


class StaticRemote:
    """Handles static remote indexes with configuration-based URLs."""

    def __init__(self, index_key: str, manager: StaticRemoteManager | None = None):
        """Initialize a StaticRemote for the given index.

        Args:
            index_key: The dot-separated index key (e.g., 'go.ssi.edr')
            manager: Optional StaticRemoteManager instance
        """
        self.index_key = index_key
        self.manager = manager or get_static_manager()

    @property
    def url(self) -> str:
        """Get the URL for this index from configuration."""
        url = self.manager.get_url(self.index_key)
        if not url:
            logger.warning(f"No static URL configured for index: {self.index_key}")
        return url

    @property
    def table_url(self) -> str:
        """Get the table URL for this index.

        For static indexes, we attempt to infer the table URL from the label URL,
        but this may need enhancement to read the actual table filename from the label.
        """
        label_url = self.url
        if not label_url:
            return ""

        # Basic inference: replace .lbl with .tab
        if label_url.endswith(".lbl"):
            return label_url.replace(".lbl", ".tab")

        # TODO: Parse the label file to get the actual table filename
        logger.warning(f"Cannot infer table URL from label URL: {label_url}")
        return ""

    def check_for_updates(self, speed: str = "normal") -> bool:
        """Check if this static index needs updating.

        Args:
            speed: Update frequency ('fast', 'normal', 'thorough')

        Returns:
            True if updates were found/applied
        """
        if speed == "fast":
            return False  # fast mode skips updates

        return self.manager.check_for_updates()


# Global manager instance
_default_manager: StaticRemoteManager | None = None


def get_static_manager() -> StaticRemoteManager:
    """Get or create the default static manager instance."""
    global _default_manager
    if _default_manager is None:
        _default_manager = StaticRemoteManager()
    return _default_manager
