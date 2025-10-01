"""Support tools to work with PDS index files."""

__all__ = ["logger", "storage_root", "dynamic_urls", "Index"]

import datetime  # Use absolute import instead of from datetime import datetime
from pathlib import Path
from urllib.request import URLError

import pandas as pd
from loguru import logger
from yarl import URL

from .. import utils
from ..config import config
from .ctx_index import CTXIndex
from .index_config import IndexConfigManager, access_log
from .index_labels import IndexLabel
from .lroc_index import LROCIndex

storage_root = Path(config.storage_root)

dynamic_urls = {"mro.ctx": CTXIndex, "lro.lroc": LROCIndex}


class Index:
    """Index manager class.

    This class manages one index, identified by a dotted key, e.g. `cassini.iss.ring_summary`
    """

    def __init__(
        self,
        key: str,  # Nested (dotted) key, e.g. cassini.iss.ring_summary
        url: str = None,  # URL to index. If not given, will be read from config object.
        check_update: bool = True,  # switch if dynamic URLS will be derived upon start
    ):
        self.key = key
        self.check_update = check_update

        self.set_url(url)

        # Try to get timestamp from access log
        self.timestamp = access_log.get_timestamp(self.key)

        # If not in log, try to get from file system
        if self.timestamp is None and self.local_label_path.exists():
            self.timestamp = datetime.datetime.fromtimestamp(
                self.local_label_path.stat().st_mtime
            )
            self.update_timestamp()

        self._remote_timestamp = None

    def set_url(self, url):
        """Set URL from having it dynamically determined (for non-static index URLs)."""
        manager = IndexConfigManager()
        self.url = manager.get_url(self.key) if url is None else url
        if not self.url and self.check_update and self.instrument_key in dynamic_urls:
            self.url = dynamic_urls[self.instrument_key]().latest_index_label_url
            if self.url:
                manager.set_url(self.key, str(self.url))

    @property
    def isotimestamp(self):
        return self.timestamp.isoformat() if self.timestamp else ""

    @property
    def remote_timestamp(self):
        if self._remote_timestamp:
            return (
                self._remote_timestamp
            )  # save the internet traffic if already checked before
        try:
            self._remote_timestamp = utils.get_remote_timestamp(self.url)
        except URLError:
            print("Warning: Could not get the remote timestamp for update check.")
        return self._remote_timestamp

    @property
    def key_tokens(self):
        return self.key.split(".")

    @property
    def mission(self):
        return self.key_tokens[0]

    @property
    def instrument(self):
        return self.key_tokens[1]

    @property
    def instrument_key(self):
        return ".".join(self.key_tokens[0:2])

    @property
    def index_name(self):
        """str: Examples: EDR, RDR, moon_summary"""
        return self.key_tokens[-1]  # Use the last token rather than a specific index

    @property
    def label_filename(self):
        if self.url:
            return Path(str(self.url).split("/")[-1])
        else:
            # Find label files using Path.glob()
            label_files = list(self.local_dir.glob("*.lbl")) + list(
                self.local_dir.glob("*.LBL")
            )
            if label_files:
                return label_files[0].name
            else:
                raise FileNotFoundError(f"No label files found in {self.local_dir}")

    @property
    def isupper(self):
        return self.label_filename.suffix.isupper()

    @property
    def tab_extension(self):
        return ".TAB" if self.isupper else ".tab"

    @property
    def table_filename(self):
        return self.label_filename.with_suffix(self.tab_extension)

    @property
    def table_url(self):
        if self.url:
            return str(URL(self.url).with_suffix(self.tab_extension))
        else:
            return ""

    @property
    def local_dir(self):
        # squeeze in an indexes subfolder
        key_path = Path(str(self.key).replace(".", "/"))
        indexes_path = key_path.parent / Path("indexes") / key_path.name
        p = storage_root / indexes_path
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def local_table_path(self):
        return self.local_dir / self.table_filename

    @property
    def local_label_path(self):
        return self.local_dir / self.label_filename

    @property
    def local_parq_path(self):
        return self.local_table_path.with_suffix(".parq")

    @property
    def parquet(self):
        return pd.read_parquet(self.local_parq_path)

    def update_timestamp(self):
        # Update the timestamp in the access log
        access_log.set_timestamp(self.key, self.timestamp)

    @property
    def label(self):
        return IndexLabel(self.local_label_path)

    def read_index_data(self, do_convert_times=True):
        df = self.label.read_index_data(do_convert_times=do_convert_times)
        return df

    def convert_to_parquet(self):
        print(
            "Reading index to memory for conversion to parquet. Will take up lots of memory for a bit."
        )
        df = self.read_index_data()
        df = df.convert_dtypes()
        print("Storing into parquet.")
        df.to_parquet(self.local_parq_path)
        print("Finished. Enjoy your freshly baked PDS Index. :")

    def __str__(self):
        s = f"Key: {self.key}\n"
        s += f"URL: {self.url}\n"
        s += f"Timestamp: {self.timestamp}\n"
        return s

    def __repr__(self):
        return self.__str__()

    def download(
        self,
        convert_to_parquet: bool = True,  # set to False if you just want download the files
    ):
        """Wrapping URLs for downloading PDS indices and their label files."""
        # check timestamp
        label_url = self.url
        logger.info("Downloading %s." % label_url)
        utils.url_retrieve(label_url, self.local_label_path)
        logger.info("Downloading %s.", self.table_url)
        utils.url_retrieve(self.table_url, self.local_table_path)
        print(f"Downloaded {self.local_label_path} and {self.local_table_path}")
        # if (
        #     self.key == "mro.hirise.indexes.edr"
        # ):  # HiRISE EDR index is broken on the PDS. Team knows.
        #     print("Fixing broken EDR index...")
        #     fix_hirise_edrcumindex(
        #         self.local_table_path, self.local_table_path.with_name("temp.tab")
        #     )
        #     self.local_table_path.with_name("temp.tab").rename(self.local_table_path)
        self.timestamp = self.remote_timestamp
        self.update_timestamp()
        if convert_to_parquet:
            self.convert_to_parquet()

    @property
    def update_available(self) -> bool:  # Boolean indicating if there's a new index
        "Property indicating if the index needs to be downloaded."
        if not self.timestamp:
            return True  # never downloaded
        return True if self.remote_timestamp > self.timestamp else False

    # Add a new method to provide user-friendly update notifications
    def check_for_updates(
        self,
        print_result: bool = True,  # Whether to print user-friendly messages
    ):
        """Check if updates are available for this index and optionally print notifications.

        Args:
            print_result: Whether to print user-friendly messages about available updates

        Returns:
            dict: Information about available updates
        """
        result = {"has_update": False, "details": {}}

        # Check if this is a dynamic URL that might have a newer index location
        if self.instrument_key in dynamic_urls:
            # Discover the latest URLs
            manager = IndexConfigManager()
            updates = manager.discover_dynamic_urls()
            # Find if this index has an update
            key = f"{self.instrument_key}.{self.index_name}"
            if key in updates and updates[key].get("is_update", False):
                result["has_update"] = True
                result["details"]["url_update"] = updates[key]

        # Check for timestamp update
        if self.update_available:
            result["has_update"] = True
            result["details"]["timestamp_update"] = {
                "current": self.timestamp.isoformat() if self.timestamp else None,
                "remote": self.remote_timestamp.isoformat()
                if self.remote_timestamp
                else None,
            }

            # Print notification about timestamp update
            if print_result and self.timestamp:
                print(f"Update available: Newer version of {self.key} index found!")
                print(f"  Current version: {self.timestamp.isoformat()}")
                print(f"  Available version: {self.remote_timestamp.isoformat()}")

        # Print if no updates were found
        if print_result and not result["has_update"]:
            print(f"No updates available for {self.key}")

        return result
