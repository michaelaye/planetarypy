"""Support tools to work with PDS index files."""

__all__ = ["logger", "storage_root", "dynamic_urls", "Index"]

import datetime  # Use absolute import instead of from datetime import datetime
import logging
from urllib.request import URLError

import pandas as pd
from fastcore.basics import patch  # better monkeypatcher
from fastcore.xtras import Path  # improved pathlib.Path
from yarl import URL

from .. import utils
from ..config import config
from .ctx_index import CTXIndex
from .index_config import access_log, discover_dynamic_urls, get_url
from .index_labels import IndexLabel
from .lroc_index import LROCIndex

logger = logging.getLogger(__name__)

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
        self.key = self.parse_key(key)
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

    def set_url(self, url):  # URL to index.
        """Set URL from having it dynamically determined (for non-static index URLs)."""
        self.url = get_url(self.key) if url is None else url
        if not self.url and self.check_update:  # empty ''
            self.url = dynamic_urls[self.instrument_key]().latest_index_label_url
            # Store the dynamically determined URL in the config
            if self.url:
                from .index_config import set_url as config_set_url

                config_set_url(self.key, self.url)

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
        return self.key_tokens[1]

    @property
    def mission_key(self):
        return ".".join(self.key_tokens[1:2])

    @property
    def instrument(self):
        return self.key_tokens[2]

    @property
    def instrument_key(self):
        return ".".join(self.key_tokens[1:3])

    @property
    def index_name(self):
        """str: Examples: EDR, RDR, moon_summary"""
        return self.key_tokens[-1]  # Use the last token rather than a specific index

    @property
    def label_filename(self):
        if self.url:
            return Path(str(self.url).split("/")[-1])
        else:
            return Path.ls(self.local_dir, file_exts=[".lbl", ".LBL"])[0]

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


@patch
def parse_key(
    self: Index,
    key: str,  # dotted key
):
    """Take care of different ways how the key could be structured.

    This is a simplified version that no longer adds the 'indexes' subkey,
    as we've simplified the structure for clarity.
    """
    # If key already starts with "missions", keep it as is
    if key.startswith("missions"):
        return key
    # Otherwise, add the "missions" prefix
    return "missions." + key


@patch
def download(
    self: Index,  # the Index object defined in this module
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
    #     self.key == "missions.mro.hirise.indexes.edr"
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


@patch(as_prop=True)
def update_available(self: Index) -> bool:  # Boolean indicating if there's a new index
    "Property indicating if the index needs to be downloaded."
    if not self.timestamp:
        return True  # never downloaded
    return True if self.remote_timestamp > self.timestamp else False


# Add a new patch method to provide user-friendly update notifications
@patch
def check_for_updates(
    self: Index,
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
        updates = discover_dynamic_urls()

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
