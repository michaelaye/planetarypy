"""Logging handlers for PDS index access timestamps and URL discoveries."""

from datetime import datetime as dt
from datetime import timedelta
from pathlib import Path

from loguru import logger

from planetarypy.utils import NestedTomlDict


class AccessLog(NestedTomlDict):
    """Handler for index log operations.

    The key should be a dotted key representing the index or configuration being logged, e.g. "config.indexes.static"
    or "mro.ctx.edr"
    """

    ONEDAY = timedelta(days=1)
    FILE_PATH = Path.home() / ".planetarypy_index_log.toml"

    def __init__(self, key: str | None = None):
        super().__init__(self.FILE_PATH)
        self.key = key

    def _log_time(self, time_type):
        """Log a timestamp for a given key and time type."""
        self.set(self.key, time_type, dt.now().replace(microsecond=0))
        self.save()
        logger.debug(
            f"Logged {time_type} for {self.key} at {self.get(self.key, time_type)}"
        )

    def log_check_time(self):
        self._log_time("last_checked")

    def log_update_time(self):
        self._log_time("last_updated")

    def log_current_url(self, url: str):
        """Log the URL of the currently cached/downloaded index."""
        self.set(self.key, "current_url", str(url))
        logger.debug(f"Logged current URL for {self.key}: {url}")

    def log_available_url(self, url: str):
        """Log the URL of an available update."""
        self.set(self.key, "available_url", str(url))
        self.log_update_available(True)
        self.log_check_time()
        logger.debug(f"Logged available update URL for {self.key}: {url}")

    def log_remote_timestamp(self, timestamp: dt):
        self.set(self.key, "remote_timestamp", timestamp.replace(microsecond=0))
        self.log_check_time()

    def _log_yesterday_check(self):
        """Set the last check time to yesterday to force a check on next access."""
        yesterday = dt.now() - self.ONEDAY - timedelta(minutes=1)
        self.set(self.key, "last_checked", yesterday.replace(microsecond=0))
        self.save()
    
    @property
    def current_url(self) -> str | None:
        """Get the URL of the currently cached index."""
        return self.get(self.key, "current_url")
    
    @property
    def available_url(self) -> str | None:
        """Get the URL of an available update, if any."""
        return self.get(self.key, "available_url")
    
    def log_update_available(self, available: bool):
        """Log whether an update is available for this key."""
        self.set(self.key, "update_available", available)
        # Persist immediately so flags don't linger after successful downloads
        self.save()

    @property
    def update_available(self) -> bool | None:
        """Get whether an update is available for this key."""
        return self.get(self.key, "update_available")
    
    @property
    def last_check(self):
        return self.get(self.key, "last_checked")

    @property
    def last_update(self):
        return self.get(self.key, "last_updated")

    @property
    def time_since_last_check(self) -> dt | None:
        """Return time delta since last check, or None if never checked."""
        last = self.last_check
        if last:
            return dt.now() - last
        return None

    @property
    def should_check(self) -> bool:
        """Determine if a check should be performed (if last check was over one day ago)."""
        last = self.last_check
        if last is None:
            return True
        return self.time_since_last_check > self.ONEDAY

    @property
    def _should_check_minute(self) -> bool:
        """Determine if a check should be performed (if last check was over one minute ago)."""
        last = self.last_check
        if last is None:
            return True
        return self.time_since_last_check > timedelta(minutes=1)

    def delete(self):
        """Delete the index log file."""
        if self.FILE_PATH.is_file():
            self.FILE_PATH.unlink()
            logger.info(f"Deleted index log file: {self.FILE_PATH}")
        else:
            logger.warning(f"Index log file does not exist: {self.FILE_PATH}")

    def __str__(self):
        if self.key is None:
            return f"{self.FILE_PATH.read_text()}"
        else:
            return self.dumps()

    def __repr__(self):
        return self.__str__()