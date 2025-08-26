"""Logging handlers for PDS index access timestamps and URL discoveries."""

import datetime
import os
from pathlib import Path
from typing import Optional, Union

import tomlkit
from dateutil import parser
from dateutil.parser import ParserError
from loguru import logger


class IndexAccessLog:
    """Manage access timestamps for PDS indices.

    This class handles storing and retrieving timestamps of when PDS indices
    were last accessed or updated.
    """

    fname = "planetarypy_index_log.toml"
    path = Path(os.getenv("PLANETARYPY_INDEX_LOG", Path.home() / f".{fname}"))

    def __init__(self, log_path: str = None):
        """Initialize with optional custom log path."""
        if log_path is not None:
            self.path = Path(log_path)

        # Create empty log file if it doesn't exist
        if not self.path.exists():
            self._create_empty_log()

        self._read_log()

    def _create_empty_log(self):
        """Create an empty log file."""
        doc = tomlkit.document()
        doc.add(tomlkit.comment("PlanetaryPy Index Access Log"))
        doc.add(tomlkit.nl())
        doc.add(tomlkit.comment("Config-related timestamps are stored at root level"))
        doc.add(
            tomlkit.comment("Dynamic URL info is stored in [dynamic_urls] sections")
        )
        doc.add(tomlkit.nl())
        doc.add(tomlkit.nl())

        # Add empty dynamic_urls section
        dynamic_urls_table = tomlkit.table()
        doc["dynamic_urls"] = dynamic_urls_table

        self.path.write_text(tomlkit.dumps(doc))

    def _read_log(self):
        """Read the log file into memory."""
        try:
            self.tomldoc = tomlkit.loads(self.path.read_text())

            # Ensure required sections exist
            if "dynamic_urls" not in self.tomldoc:
                self.tomldoc["dynamic_urls"] = tomlkit.table()

        except tomlkit.exceptions.TOMLKitError:
            logger.warning(f"Could not parse log file {self.path}. Creating a new one.")
            self.tomldoc = tomlkit.document()
            self.tomldoc["dynamic_urls"] = tomlkit.table()
            self.save()

    def save(self):
        """Write log data to file."""
        self.path.write_text(tomlkit.dumps(self.tomldoc))

    def get_timestamp(self, key: str) -> Optional[datetime.datetime]:
        """Get timestamp for a specific key.

        Args:
            key: For config keys (config_download, config_update, config_check),
                looks at root level. For dynamic URLs, looks at last_checked.

        Returns:
            Datetime object or None if no timestamp exists
        """
        config_keys = ["config_download", "config_update", "config_check"]

        if key in config_keys:
            # Config timestamps are stored at root level
            if key in self.tomldoc:
                try:
                    return parser.parse(self.tomldoc[key])
                except ParserError:
                    return None
        else:
            # Dynamic URL timestamps are in their specific sections
            if key in self.tomldoc["dynamic_urls"]:
                entry = self.tomldoc["dynamic_urls"][key]
                if "last_checked" in entry:
                    try:
                        return parser.parse(entry["last_checked"])
                    except ParserError:
                        return None

        return None

    def set_timestamp(self, key: str, timestamp: Union[datetime.datetime, str]):
        """Set timestamp for a specific key.

        Args:
            key: For config keys, stored at root level.
                 For dynamic URLs, updates last_checked in their section.
            timestamp: Datetime object or ISO format string
        """
        if isinstance(timestamp, datetime.datetime):
            # Truncate to seconds precision (remove microseconds)
            timestamp_str = timestamp.replace(microsecond=0).isoformat()
        else:
            timestamp_str = timestamp

        config_keys = ["config_download", "config_update", "config_check"]

        if key in config_keys:
            # Store config timestamps at root level
            self.tomldoc[key] = timestamp_str
        else:
            # For dynamic URLs, update last_checked in their section
            if key not in self.tomldoc["dynamic_urls"]:
                # Create new entry if it doesn't exist
                self.tomldoc["dynamic_urls"][key] = tomlkit.table()

            self.tomldoc["dynamic_urls"][key]["last_checked"] = timestamp_str

        self.save()

    def log_url_discovery(
        self, key: str, url: str, is_update: bool = False, previous_url: str = None
    ):
        """Log the discovery of a URL for a dynamic index.

        Args:
            key: A dotted key format, e.g., 'mro.ctx.edr'
            url: The discovered URL
            is_update: Whether this is an update to an existing URL
            previous_url: The previous URL if this is an update
        """
        # Normalize key to use "missions." prefix for consistency with Index class
        if not key.startswith("missions."):
            key = "missions." + key

        now_str = datetime.datetime.now().replace(microsecond=0).isoformat()

        # Create or update the dynamic URL entry
        if key not in self.tomldoc["dynamic_urls"]:
            self.tomldoc["dynamic_urls"][key] = tomlkit.table()

        entry = self.tomldoc["dynamic_urls"][key]
        entry["url"] = url
        entry["last_checked"] = now_str
        entry["is_update"] = is_update

        # Set discovered_at only if this is a new entry or an actual update
        if "discovered_at" not in entry or is_update:
            entry["discovered_at"] = now_str

        if previous_url:
            entry["previous_url"] = previous_url
        elif "previous_url" in entry and not is_update:
            # Remove previous_url if this is not an update
            del entry["previous_url"]

        self.save()

        # Log the discovery
        if is_update:
            logger.info(
                f"Updated URL discovered for {key}: {url} (was: {previous_url})"
            )
        else:
            logger.info(f"URL checked for {key}: {url}")

    def get_discovery_info(self, key: str) -> Optional[dict]:
        """Get discovery information for a specific dynamic URL.

        Args:
            key: A dotted key format, e.g., 'mro.ctx.edr'

        Returns:
            Dictionary with discovery info or None if not found
        """
        if key in self.tomldoc["dynamic_urls"]:
            return dict(self.tomldoc["dynamic_urls"][key])
        return None

    def get_all_discoveries(self) -> dict:
        """Get all dynamic URL information.

        Returns:
            Dictionary of all dynamic URL entries
        """
        return dict(self.tomldoc["dynamic_urls"])

    def __repr__(self):
        return tomlkit.dumps(self.tomldoc)


# Create singleton instance
access_log = IndexAccessLog()
