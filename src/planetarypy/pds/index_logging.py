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
        doc.add(
            tomlkit.comment("This file tracks access timestamps and URL discoveries")
        )
        doc.add(tomlkit.nl())
        doc.add(tomlkit.nl())

        # Add empty timestamps section
        timestamps_table = tomlkit.table()
        doc["timestamps"] = timestamps_table
        doc.add(tomlkit.nl())

        # Add empty discoveries section
        discoveries_table = tomlkit.table()
        doc["discoveries"] = discoveries_table

        self.path.write_text(tomlkit.dumps(doc))

    def _read_log(self):
        """Read the log file into memory."""
        try:
            self.tomldoc = tomlkit.loads(self.path.read_text())

            # Ensure required sections exist
            if "timestamps" not in self.tomldoc:
                self.tomldoc["timestamps"] = tomlkit.table()
            if "discoveries" not in self.tomldoc:
                self.tomldoc["discoveries"] = tomlkit.table()

        except tomlkit.exceptions.TOMLKitError:
            logger.warning(f"Could not parse log file {self.path}. Creating a new one.")
            self.tomldoc = tomlkit.document()
            self.tomldoc["timestamps"] = tomlkit.table()
            self.tomldoc["discoveries"] = tomlkit.table()
            self.save()

    def save(self):
        """Write log data to file."""
        self.path.write_text(tomlkit.dumps(self.tomldoc))

    def get_timestamp(self, key: str) -> Optional[datetime.datetime]:
        """Get timestamp for a specific index.

        Args:
            key: A dotted key format, e.g., 'cassini.iss.ring_summary'

        Returns:
            Datetime object or None if no timestamp exists
        """
        if key in self.tomldoc["timestamps"]:
            try:
                return parser.parse(self.tomldoc["timestamps"][key])
            except ParserError:
                return None
        return None

    def set_timestamp(self, key: str, timestamp: Union[datetime.datetime, str]):
        """Set timestamp for a specific index.

        Args:
            key: A dotted key format, e.g., 'cassini.iss.ring_summary'
            timestamp: Datetime object or ISO format string
        """
        if isinstance(timestamp, datetime.datetime):
            # Truncate to seconds precision (remove microseconds)
            timestamp_str = timestamp.replace(microsecond=0).isoformat()
        else:
            timestamp_str = timestamp

        self.tomldoc["timestamps"][key] = timestamp_str
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
        discovery_data = tomlkit.table()
        discovery_data["url"] = url
        # Truncate to seconds precision (remove microseconds)
        discovery_data["discovered_at"] = (
            datetime.datetime.now().replace(microsecond=0).isoformat()
        )
        discovery_data["is_update"] = is_update
        if previous_url:
            discovery_data["previous_url"] = previous_url

        self.tomldoc["discoveries"][key] = discovery_data

        # Also update the regular access timestamp
        self.set_timestamp(key, datetime.datetime.now())

        # Log the discovery
        if is_update:
            logger.info(
                f"Updated URL discovered for {key}: {url} (was: {previous_url})"
            )
        else:
            logger.info(f"New URL discovered for {key}: {url}")

    def get_discovery_info(self, key: str) -> Optional[dict]:
        """Get discovery information for a specific index.

        Args:
            key: A dotted key format, e.g., 'mro.ctx.edr'

        Returns:
            Dictionary with discovery info or None if not found
        """
        if key in self.tomldoc["discoveries"]:
            return dict(self.tomldoc["discoveries"][key])
        return None

    def get_all_discoveries(self) -> dict:
        """Get all discovery information.

        Returns:
            Dictionary of all discovery entries
        """
        return dict(self.tomldoc["discoveries"])

    def __repr__(self):
        return tomlkit.dumps(self.tomldoc)


# Create singleton instance
access_log = IndexAccessLog()
