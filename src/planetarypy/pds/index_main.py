"""Composition-based Index implementation using Remote classes.

This module provides a unified Index class that uses composition with Remote classes
to handle URL management."""

__all__ = ["Index", "InventoryIndex"]

import csv
from pathlib import Path

import pandas as pd
from loguru import logger
from yarl import URL

from ..config import config
from ..utils import have_internet, url_retrieve
from .dynamic_index import (
    DYNAMIC_URL_HANDLERS,  # registry of dynamic index handlers
    DynamicRemoteHandler,
)
from .index_labels import IndexLabel
from .static_index import StaticRemoteHandler


class Index:
    """Unified Index class using composition with Remote classes.
    
    This class provides file management operations while delegating URL management
    to appropriate Remote classes based on the index configuration.

    Parameters
    ----------
    index_key : str
        Dotted key identifying the index (e.g., "mro.ctx.edr")
    local_dir : str | Path | None, optional
        Local directory for index files. If None, a default path is used.
    force_config_update : bool, optional
        Whether to force update of static index configuration.
    """
    def __init__(
        self, 
        index_key: str, 
        local_dir: str | Path | None = None, 
        force_config_update: bool = False
    ):
        """Initialize Index with composition-based Remote handling.

        Args:
            index_key: Dotted key identifying the index (e.g., "mro.ctx.edr")
            local_dir: Local directory for index files
        """
        self.index_key = index_key
        self.mission, self.instrument, self.indexname = index_key.split(".")
        self._local_dir = Path(local_dir) if local_dir else self._default_local_dir()

        self._remote = None
        self._remote_type = None
        self._force_config_update = force_config_update
        self._determine_remote_type()

    def _default_local_dir(self) -> Path:
        """Get default local directory for this index."""
        return Path(config.storage_root) / f"{self.mission}/{self.instrument}/indexes/{self.indexname}"

    @property
    def local_dir(self) -> Path:
        """Get local directory for this index."""
        self._local_dir.mkdir(parents=True, exist_ok=True)
        return self._local_dir

    def _determine_remote_type(self):
        """Determine if this index uses static or dynamic remote handling."""
        if self.index_key in DYNAMIC_URL_HANDLERS:  # like 'mro.ctx'
            self._remote_type = "dynamic"
            self._remote = DynamicRemoteHandler(index_key=self.index_key)
            logger.debug(f"Index {self.index_key} will use dynamic remote handling")
        else:
            self._remote_type = "static"  # like 'go.ssi'
            self._remote = StaticRemoteHandler(
                index_key=self.index_key, force_config_update=self._force_config_update
            )
            logger.debug(f"Index {self.index_key} will use static remote handling")

    @property
    def remote_type(self) -> str:
        """Get the type of remote handling used ('static' or 'dynamic')."""
        return self._remote_type

    @property
    def remote(self) -> StaticRemoteHandler | DynamicRemoteHandler:
        """Get the appropriate Remote instance for this index."""
        return self._remote

    @property
    def update_available(self) -> bool:
        """Check if an update is available.

        Returns False if no internet connection is available.
        """
        if not have_internet():
            logger.debug(
                f"No internet connection; skipping update check for {self.index_key}"
            )
            return False
        return self.remote.update_available

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
        return str(URL(self.url).with_suffix(self.tab_extension))

    @property
    def files_downloaded(self) -> bool:
        """Check if index files exist locally."""
        if self.local_label_path.exists() and self.local_table_path.exists():
            return True
        else:
            return False

    def download(self, force: bool = False, convert_to_parquet: bool = True) -> bool:
        """Download the index files from remote URL.

        Args:
            force: Force download even if local files exist
            convert_to_parquet: Whether to convert to parquet after download

        Returns:
            True if download was successful
        """
        if not have_internet():
            logger.warning(f"No internet connection; cannot download {self.index_key}")
            return False

        url = self.url
        if not url:
            logger.error(f"No URL available for {self.index_key}")
            return False
        try:
            # Download label file
            logger.info(
                f"Downloading {self.index_key} label from {url} and related table."
            )
            url_retrieve(url, self.local_label_path)

            # Download table file
            logger.debug(f"Downloading {self.index_key} table from {self.table_url}")
            url_retrieve(self.table_url, self.local_table_path)

            logger.info(f"Successfully downloaded {self.index_key} files")

            if convert_to_parquet:
                self.convert_to_parquet()

            # Log the successful update
            self.remote.log.log_update_time()

            # For dynamic indexes, record the URL we just downloaded as current_url
            if self.remote_type == "dynamic":
                self.remote.log.log_current_url(url)

            # Clear the update_available flag since we just downloaded
            self.remote.log.log_update_available(False)

        except Exception as e:
            logger.error(f"Error downloading {self.index_key}: {e}")

    def ensure_parquet(self, force: bool = False) -> bool:
        """Ensure a parquet cache exists for this index.

        - If force is True, reconvert to parquet from existing label+table.
        - If parquet is missing, reconvert when label+table exist.
        - If label or table are missing, perform a clean download.

        Returns:
            True if a download was performed, False otherwise.
        """
        if force or not self.local_parq_path.is_file():
            if self.local_label_path.is_file() and self.local_table_path.is_file():
                logger.debug(
                    f"Ensuring parquet for {self.index_key}: converting existing label+table."
                )
                self.convert_to_parquet()
                return False
            else:
                logger.debug(
                    f"Ensuring parquet for {self.index_key}: label/table missing; downloading."
                )
                self.download()
                return True
        return False

    def convert_to_parquet(self):
        """Convert the downloaded index files to parquet format."""
        logger.info(f"Converting {self.index_key} to parquet format.")

        try:
            df = self.read_index_data()
            logger.debug(f"Storing {self.index_key} as parquet")
            df.to_parquet(self.local_parq_path)
            logger.info(f"Finished converting {self.index_key} to parquet format.")
        except Exception as e:
            logger.error(f"Error converting {self.index_key} to parquet: {e}")

    def read_index_data(self, convert_times: bool = True):
        """Read the index data from label and table files."""
        if not self.local_label_path.exists():
            raise FileNotFoundError(f"Label file not found: {self.local_label_path}")
        if not self.local_table_path.exists():
            raise FileNotFoundError(f"Table file not found: {self.local_table_path}")

        label = IndexLabel(self.local_label_path, index_key=self.index_key)
        return label.read_index_data(convert_times=convert_times)

    @property
    def dataframe(self):
        """Get the index data as a pandas DataFrame from parquet cache."""
        return pd.read_parquet(self.local_parq_path)

    def refresh_remote(self):
        """Force refresh of remote URL information."""
        if hasattr(self.remote, "refresh_url"):
            self.remote.refresh_url()
        elif hasattr(self.remote, "refresh_config"):
            self.remote.refresh_config()

    def __repr__(self) -> str:
        """Return a concise string representation of this Index."""
        return (
            f"<Index index_key={self.index_key!r} "
            f"remote_type={self._remote_type!r} "
            f"local_dir={str(self._local_dir)!r} "
            f"label={str(self.local_label_path)!r} "
            f"table={str(self.local_table_path)!r} "
            f"parq={str(self.local_parq_path)!r} "
            f"exists_locally={self.files_downloaded}>"
        )


class InventoryIndex(Index):
    """Index class for inventory-style CSV indexes.
    This class handles CSV files where:
    - First 3 columns are: volume, file_path, observation_id
    - Remaining columns contain comma-separated target names

    The data is exploded so each target gets its own row, then grouped back
    by observation_id with targets as lists for efficient querying.
    """

    @property
    def tab_extension(self):
        """Get the appropriate table extension."""
        return ".csv"

    def read_index_data(self, convert_times: bool = True):
        logger.debug("Using InventoryIndex for these csv tables.")
        rows = []
        with open(self.local_table_path, "r") as f:
            reader = csv.reader(f)
            for row in reader:
                volume, file_path, obs_id = row[:3]
                # All remaining fields are targets
                targets = [t.strip() for t in row[3:]]

                # Create one row per target
                for target in targets:
                    rows.append(
                        {
                            "volume": volume,
                            "file_path": file_path,
                            "observation_id": obs_id,
                            "target": target,
                        }
                    )

        self.target_per_row = pd.DataFrame(rows)
        logger.info(f"Read {len(self.target_per_row)} observations with target lists")
        return self.target_per_row

    @property
    def targets_per_obsid(self):
        obs_targets = (
            self.target_per_row.groupby("observation_id")
            .agg(
                {
                    "volume": "first",  # Take the first volume (they're all the same)
                    "file_path": "first",  # Take the first file_path
                    "target": list,  # Collect all targets into a list
                }
            )
            .reset_index()
        )
        return obs_targets
