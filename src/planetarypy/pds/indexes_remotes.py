"""Composition-based Index implementation using Remote classes.

This module provides a unified Index class that uses composition with Remote classes
to handle URL management, providing three speed modes for different use cases:
- fast: Skip remote operations if local file exists
- normal: Check for updates but don't auto-download
- thorough: Check for updates and auto-download when available
"""

__all__ = ["Index", "IndexSpeedMode"]

import enum
from pathlib import Path

from loguru import logger

from planetarypy.pds import utils

from .dynamic_remote import DYNAMIC_URL_HANDLERS, DynamicRemote, get_dynamic_manager
from .static_remote import StaticRemote, get_static_manager


class IndexSpeedMode(enum.Enum):
    """Speed modes for Index operations."""

    FAST = "fast"  # no checks done, just use the local file if it exists
    NORMAL = "normal"  # check for updates, but don't auto-download
    THOROUGH = "thorough"  # always check for updates and auto-download if available


class Index:
    """Unified Index class using composition with Remote classes.

    This class provides file management operations while delegating URL management
    to appropriate Remote classes based on the index configuration.
    """

    def __init__(
        self,
        index_key: str,
        local_dir: str | Path | None = None,
        speed_mode: str | IndexSpeedMode = IndexSpeedMode.NORMAL,
        check_and_update: bool = True,
    ):
        """Initialize Index with composition-based Remote handling.

        Args:
            index_key: Dotted key identifying the index (e.g., "mro.ctx.edr")
            local_dir: Local directory for index files
            speed_mode: Operation speed mode (fast/normal/thorough)
            check_and_update: Whether to check for updates
        """
        self.index_key = index_key
        self.local_dir = Path(local_dir) if local_dir else self._default_local_dir()
        self.speed_mode = (
            IndexSpeedMode(speed_mode) if isinstance(speed_mode, str) else speed_mode
        )
        self.check_and_update = check_and_update

        # Determine remote type and initialize appropriately
        self._remote = None
        self._remote_type = None
        self._determine_remote_type()

    def _default_local_dir(self) -> Path:
        """Get default local directory for this index."""
        # Use the same pattern as the original indexes.py
        from ..config import config

        return Path(config.storage_root) / "indexes" / self.index_key.replace(".", "/")

    def _determine_remote_type(self):
        """Determine if this index uses static or dynamic remote handling."""
        # Check if this index has dynamic URL support
        instrument_key = ".".join(self.index_key.split(".")[0:2])

        if instrument_key in DYNAMIC_URL_HANDLERS:
            self._remote_type = "dynamic"
            logger.debug(f"Index {self.index_key} will use dynamic remote handling")
        else:
            self._remote_type = "static"
            logger.debug(f"Index {self.index_key} will use static remote handling")

    @property
    def remote(self) -> StaticRemote | DynamicRemote:
        """Get the appropriate Remote instance for this index."""
        if self._remote is None:
            if self._remote_type == "dynamic":
                self._remote = DynamicRemote(
                    index_key=self.index_key, manager=get_dynamic_manager()
                )
            else:
                self._remote = StaticRemote(
                    index_key=self.index_key, manager=get_static_manager()
                )
        return self._remote

    @property
    def url(self) -> str:
        """Get the current URL for this index."""
        return self.remote.url

    @property
    def label_filename(self):
        """Get the label filename from URL."""
        if self.url:
            return Path(str(self.url).split("/")[-1])
        else:
            # Find label files using Path.glob()
            label_files = list(self.local_dir.glob("*.lbl")) + list(
                self.local_dir.glob("*.LBL")
            )
            if label_files:
                return Path(label_files[0].name)
            else:
                # Fallback to generic filename if no URL available
                return Path(f"{self.index_key.split('.')[-1]}.lbl")

    @property
    def isupper(self):
        """Check if filename uses uppercase extension."""
        return self.label_filename.suffix.isupper()

    @property
    def tab_extension(self):
        """Get the appropriate table extension."""
        return ".TAB" if self.isupper else ".tab"

    @property
    def table_filename(self):
        """Get the table filename."""
        return self.label_filename.with_suffix(self.tab_extension)

    @property
    def local_label_path(self) -> Path:
        """Get the local label file path."""
        return self.local_dir / self.label_filename

    @property
    def local_table_path(self) -> Path:
        """Get the local table file path."""
        return self.local_dir / self.table_filename

    @property
    def local_parq_path(self) -> Path:
        """Get the local parquet file path."""
        return self.local_table_path.with_suffix(".parq")

    @property
    def table_url(self) -> str:
        """Get the table URL from the label URL."""
        from yarl import URL

        return str(URL(self.url).with_suffix(".tab"))

    @property
    def files_downloaded(self) -> bool:
        """Check if index files exist locally."""
        if self.local_label_path.exists() and self.local_table_path.exists():
            return True
        else:
            return False

    def update_available(self) -> bool:
        """Check if an update is available."""
        if self.speed_mode == IndexSpeedMode.FAST and self.exists_locally():
            return False

        return self.remote.update_available()

    def check_for_updates(self) -> bool:
        """Check for updates with appropriate behavior based on speed mode."""
        if self.speed_mode == IndexSpeedMode.FAST:
            # Fast mode: skip checks if local file exists
            if self.exists_locally():
                logger.debug(
                    f"Fast mode: local file exists for {self.index_key}, skipping remote checks"
                )
                return False
            else:
                logger.info(
                    f"Fast mode: no local file for {self.index_key}, checking remote"
                )
                return self.remote.update_available()

        elif self.speed_mode == IndexSpeedMode.NORMAL:
            # Normal mode: check for updates but respect rate limiting
            if not self.check_and_update:
                return False
            return self.remote.check_for_updates()

        elif self.speed_mode == IndexSpeedMode.THOROUGH:
            # Thorough mode: always check for updates
            return self.remote.update_available()

        return False

    def download(self, force: bool = False, convert_to_parquet: bool = True) -> bool:
        """Download the index files from remote URL.

        Args:
            force: Force download even if local files exist
            convert_to_parquet: Whether to convert to parquet after download

        Returns:
            True if download was successful
        """
        if not force and self.files_downloaded:
            if self.speed_mode == IndexSpeedMode.FAST:
                logger.debug(
                    f"Fast mode: local files exist for {self.index_key}, skipping download"
                )
                return True

        try:
            url = self.remote.url
            if not url:
                logger.error(f"No URL available for {self.index_key}")
                return False

            # Ensure local directory exists
            self.local_dir.mkdir(parents=True, exist_ok=True)

            # Download label and table files
            logger.info(f"Downloading {self.index_key} label from {url}")
            from .. import utils

            # Download label file
            utils.url_retrieve(url, self.local_label_path)

            # Download table file
            table_url = self.table_url
            logger.info(f"Downloading {self.index_key} table from {table_url}")
            utils.url_retrieve(table_url, self.local_table_path)

            logger.info(f"Successfully downloaded {self.index_key} files")

            if convert_to_parquet:
                self.convert_to_parquet()

            return True

        except Exception as e:
            logger.error(f"Error downloading {self.index_key}: {e}")
            return False

    def convert_to_parquet(self):
        """Convert the downloaded index files to parquet format."""
        logger.info(f"Converting {self.index_key} to parquet format")
        # check for any required prefixes:
        if self.index_key == "go.ssi.edr":
            utils.simple_replace_in_file(self.local_table_path, '-23.629"', "-23.629,")
        try:
            df = self.read_index_data()
            logger.info(f"Storing {self.index_key} as parquet")
            df.to_parquet(self.local_parq_path)
            logger.info(f"Finished converting {self.index_key} to parquet")
        except Exception as e:
            logger.error(f"Error converting {self.index_key} to parquet: {e}")

    def read_index_data(self, do_convert_times: bool = True):
        """Read the index data from label and table files."""
        if not self.local_label_path.exists():
            raise FileNotFoundError(f"Label file not found: {self.local_label_path}")
        if not self.local_table_path.exists():
            raise FileNotFoundError(f"Table file not found: {self.local_table_path}")

        from .index_labels import IndexLabel

        label = IndexLabel(self.local_label_path)
        return label.read_index_data(do_convert_times=do_convert_times)

    @property
    def parquet(self):
        """Get the index data as a pandas DataFrame from parquet cache."""
        import pandas as pd

        if not self.local_parq_path.exists():
            # Try to download and convert if not available
            if self.download():
                return pd.read_parquet(self.local_parq_path)
            else:
                raise FileNotFoundError(
                    f"Could not load or download index data for {self.index_key}"
                )
        return pd.read_parquet(self.local_parq_path)

    def get_index(self):
        """Get the index data, downloading if necessary (convenience method)."""
        return self.parquet

    def sync(self) -> bool:
        """Synchronize index with remote (check and download if needed).

        Behavior depends on speed mode:
        - fast: Only download if no local file exists
        - normal: Check for updates, download if user requested
        - thorough: Always check and auto-download if updates available

        Returns:
            True if index is up to date after sync
        """
        logger.debug(f"Syncing {self.index_key} in {self.speed_mode.value} mode")

        if self.speed_mode == IndexSpeedMode.FAST:
            if self.exists_locally():
                logger.debug(
                    f"Fast mode: {self.index_key} exists locally, sync complete"
                )
                return True
            else:
                return self.download()

        elif self.speed_mode == IndexSpeedMode.NORMAL:
            if self.check_for_updates():
                logger.info(f"Updates available for {self.index_key}")
                if self.check_and_update:
                    return self.download()
                else:
                    logger.info(
                        f"check_and_update=False, not downloading {self.index_key}"
                    )
                    return self.exists_locally()
            else:
                logger.debug(f"No updates needed for {self.index_key}")
                return self.exists_locally()

        elif self.speed_mode == IndexSpeedMode.THOROUGH:
            if self.check_for_updates():
                logger.info(f"Thorough mode: downloading updates for {self.index_key}")
                return self.download(force=True)
            else:
                logger.debug(f"Thorough mode: no updates for {self.index_key}")
                return self.exists_locally()

        return False

    def refresh_remote(self):
        """Force refresh of remote URL information."""
        if hasattr(self.remote, "refresh_url"):
            self.remote.refresh_url()
        elif hasattr(self.remote, "refresh_config"):
            self.remote.refresh_config()

    def is_dynamic(self) -> bool:
        """Check if this index uses dynamic URL discovery."""
        return self._remote_type == "dynamic"

    def is_static(self) -> bool:
        """Check if this index uses static configuration."""
        return self._remote_type == "static"

    def get_info(self) -> dict:
        """Get information about this index."""
        return {
            "index_key": self.index_key,
            "local_dir": str(self.local_dir),
            "local_label_path": str(self.local_label_path),
            "local_table_path": str(self.local_table_path),
            "local_parq_path": str(self.local_parq_path),
            "exists_locally": self.exists_locally(),
            "remote_type": self._remote_type,
            "speed_mode": self.speed_mode.value,
            "url": self.url if self._remote else None,
            "table_url": self.table_url if self._remote else None,
            "check_and_update": self.check_and_update,
        }

    def __repr__(self) -> str:
        return (
            f"Index(key='{self.index_key}', "
            f"mode={self.speed_mode.value}, "
            f"remote={self._remote_type})"
        )


def create_index(
    index_key: str, speed_mode: str | IndexSpeedMode = IndexSpeedMode.NORMAL, **kwargs
) -> Index:
    """Convenience function to create an Index instance.

    Args:
        index_key: Dotted key identifying the index
        speed_mode: Operation speed mode
        **kwargs: Additional arguments passed to Index constructor

    Returns:
        Configured Index instance
    """
    return Index(index_key=index_key, speed_mode=speed_mode, **kwargs)
