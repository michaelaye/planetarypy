"""Dynamic Remote URL management for PDS indexes.

This module handles URL discovery and caching for indexes with dynamic URLs
that are discovered through web scraping. Updates happen as entirely new URLs,
not file replacements at existing URLs.
"""

from __future__ import annotations

__all__ = ["DynamicRemoteManager", "DynamicRemote", "DYNAMIC_URL_HANDLERS"]

import datetime
import os
from pathlib import Path

import tomlkit
from loguru import logger

from .. import utils
from .ctx_index import CTXIndex
from .index_logging import access_log
from .lroc_index import LROCIndex

# Dynamic URL handlers registry
DYNAMIC_URL_HANDLERS = {
    "mro.ctx": CTXIndex,
    "lro.lroc": LROCIndex,
}

# Dynamic URL cache settings
DYNAMIC_CACHE_FNAME = "planetarypy_dynamic_index_urls.toml"


class DynamicRemoteManager:
    """Manages dynamic index URLs and their discovery/caching."""

    def __init__(self, cache_path: str | None = None):
        self.cache_path = self._get_cache_path(cache_path)
        self.cache_doc = None
        self.load_cache()

    def _get_cache_path(self, custom_path: str | None = None) -> Path:
        """Get path to dynamic URL cache file."""
        if custom_path:
            return Path(custom_path)
        return Path(
            os.getenv(
                "PLANETARYPY_DYNAMIC_CACHE", Path.home() / f".{DYNAMIC_CACHE_FNAME}"
            )
        )

    def load_cache(self):
        """Load dynamic URL cache from file."""
        if not self.cache_path.exists():
            logger.info("Dynamic URL cache doesn't exist, creating empty cache")
            self.cache_doc = {}
            self.save_cache()
        else:
            logger.debug(f"Reading dynamic URL cache from: {self.cache_path}")
            content = self.cache_path.read_text()
            self.cache_doc = tomlkit.loads(content)

    def save_cache(self):
        """Save cache to file."""
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.cache_path, "w") as f:
            tomlkit.dump(self.cache_doc, f)

    def get_cached_url(self, key: str) -> str | None:
        """Get cached URL for a dynamic index key."""
        if self.cache_doc is None:
            self.load_cache()

        try:
            # Try flat key first
            if key in self.cache_doc:
                entry = self.cache_doc[key]
                if isinstance(entry, dict):
                    return entry.get("url")
                return str(entry)  # Legacy format

            # Try nested structure
            parts = key.split(".")
            if len(parts) >= 3:
                mission, instrument, index_name = parts[0], parts[1], parts[2]
                section_key = f"{mission}.{instrument}"
                if (
                    section_key in self.cache_doc
                    and isinstance(self.cache_doc[section_key], dict)
                    and index_name in self.cache_doc[section_key]
                ):
                    entry = self.cache_doc[section_key][index_name]
                    if isinstance(entry, dict):
                        return entry.get("url")
                    return str(entry)

            return None
        except (KeyError, TypeError) as e:
            logger.error(f"Error getting cached URL for {key}: {e}")
            return None

    def set_cached_url(
        self, key: str, url: str, discovered_at: datetime.datetime | None = None
    ):
        """Cache a discovered URL with metadata."""
        if self.cache_doc is None:
            self.load_cache()

        if discovered_at is None:
            discovered_at = datetime.datetime.now()

        entry = {
            "url": url,
            "discovered_at": discovered_at.isoformat(),
            "handler": self._get_handler_name_for_key(key),
        }

        # Handle nested structure for 3-part keys
        parts = key.split(".")
        if len(parts) >= 3:
            mission, instrument, index_name = parts[0], parts[1], parts[2]
            section_key = f"{mission}.{instrument}"

            if section_key not in self.cache_doc:
                self.cache_doc[section_key] = {}
            elif not isinstance(self.cache_doc[section_key], dict):
                self.cache_doc[section_key] = {}

            self.cache_doc[section_key][index_name] = entry
        else:
            # Fall back to flat key
            self.cache_doc[key] = entry

        self.save_cache()

        # Update access log
        access_log.set_timestamp(f"dynamic_discovery_{key}", discovered_at)

    def _get_handler_name_for_key(self, key: str) -> str:
        """Get handler name for a given key."""
        instrument_key = ".".join(key.split(".")[0:2])
        handler_class = DYNAMIC_URL_HANDLERS.get(instrument_key)
        return handler_class.__name__ if handler_class else "unknown"

    def discover_latest_url(self, key: str, force: bool = False) -> str | None:
        """Discover the latest URL for a dynamic index."""
        instrument_key = ".".join(key.split(".")[0:2])
        handler_class = DYNAMIC_URL_HANDLERS.get(instrument_key)

        if not handler_class:
            raise ValueError(f"No dynamic handler available for {key}")

        # Check if we need to discover (rate limiting)
        if not force:
            last_discovery = access_log.get_timestamp(f"dynamic_discovery_{key}")
            if last_discovery and not utils.is_older_than_hours(last_discovery, 24):
                logger.debug(
                    f"Dynamic URL for {key} was discovered recently, using cache"
                )
                return self.get_cached_url(key)

        try:
            logger.info(
                f"Discovering latest URL for {key} using {handler_class.__name__}"
            )
            handler = handler_class()
            latest_url = str(handler.latest_index_label_url)

            if latest_url:
                self.set_cached_url(key, latest_url)
                logger.info(f"Discovered and cached URL for {key}: {latest_url}")
                return latest_url
            else:
                logger.warning(f"No URL discovered for {key}")
                return None

        except Exception as e:
            logger.error(f"Error discovering URL for {key}: {e}")
            # Fall back to cached URL if available
            cached_url = self.get_cached_url(key)
            if cached_url:
                logger.info(f"Using cached URL for {key} due to discovery error")
            return cached_url


class DynamicRemote:
    """Manages remote URL access for dynamic indexes."""

    def __init__(
        self,
        index_key: str,
        manager: DynamicRemoteManager | None = None,
        handler_class: type | None = None,
    ):
        """Initialize dynamic remote.

        Args:
            index_key: Dotted key identifying the index
            manager: Optional DynamicRemoteManager instance
            handler_class: Optional specific handler class
        """
        self.index_key = index_key
        self.manager = manager or DynamicRemoteManager()

        # Determine handler
        self.instrument_key = ".".join(index_key.split(".")[0:2])
        self.handler_class = handler_class or DYNAMIC_URL_HANDLERS.get(
            self.instrument_key
        )
        if not self.handler_class:
            raise ValueError(f"No dynamic handler available for {index_key}")

        self.handler = self.handler_class()
        self._url = None

    @property
    def url(self) -> str:
        """Get the current URL for this index."""
        if self._url is None:
            self._url = self._resolve_url()
        return self._url

    def _resolve_url(self) -> str:
        """Resolve URL through discovery or cache."""
        # Try cache first
        cached_url = self.manager.get_cached_url(self.index_key)
        if cached_url:
            return cached_url

        # Discover latest URL
        discovered_url = self.manager.discover_latest_url(self.index_key)
        return discovered_url or ""

    def update_available(self) -> bool:
        """Check if URL update is available (new location discovered)."""
        current_url = self._url or self.manager.get_cached_url(self.index_key)
        if not current_url:
            return True  # No cached URL, discovery needed

        # For dynamic indexes, updates come as new URLs, not file changes
        try:
            latest_url = self.manager.discover_latest_url(self.index_key, force=True)
            if latest_url and latest_url != current_url:
                logger.info(f"New URL discovered for {self.index_key}: {latest_url}")
                self._url = latest_url  # Update to latest URL
                return True
        except Exception as e:
            logger.warning(f"Could not check for URL updates for {self.index_key}: {e}")

        return False

    def check_for_updates(self) -> bool:
        """Check for updates with rate limiting."""
        # Rate limit discovery checks to once per day per instrument
        log_key = f"dynamic_discovery_{self.index_key}"
        last_check = access_log.get_timestamp(log_key)

        if last_check and not utils.is_older_than_hours(last_check, 24):
            logger.debug(
                f"URL discovery check for {self.index_key} performed recently, using cached URL"
            )
            return False

        logger.info(f"Checking for new URLs for {self.index_key}")
        access_log.set_timestamp(log_key, datetime.datetime.now())

        return self.update_available()

    def refresh_url(self):
        """Force refresh of the URL from discovery."""
        self._url = self.manager.discover_latest_url(self.index_key, force=True)

    def is_supported(self) -> bool:
        """Check if this instrument has dynamic URL support."""
        return self.instrument_key in DYNAMIC_URL_HANDLERS


# Default manager instance
_default_manager = None


def get_dynamic_manager() -> DynamicRemoteManager:
    """Get default dynamic index manager."""
    global _default_manager
    if _default_manager is None:
        _default_manager = DynamicRemoteManager()
    return _default_manager
