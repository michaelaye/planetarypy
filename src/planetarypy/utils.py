"""General utility functions for planetarypy."""
import datetime as dt
import email.utils as eut
import http.client as httplib
from pathlib import Path
from typing import Any
from urllib.request import urlopen

import pandas as pd
import requests
import tomlkit
from loguru import logger

try:
    from kalasiris.pysis import ProcessError
except (KeyError, ImportError):
    ISIS_AVAILABLE = False
else:
    ISIS_AVAILABLE = True

from requests.auth import HTTPBasicAuth
try:
    from tqdm.auto import tqdm
except ImportError:
    # Fallback: Just use the identity function if tqdm is not installed
    def tqdm(x, *args, **kwargs):
        return x
from yarl import URL

from planetarypy.datetime_format_converters import fromdoyformat

__all__ = [
    "replace_all_doy_times",
    "parse_http_date",
    "get_remote_timestamp",
    "check_url_exists",
    "url_retrieve",
    "have_internet",
    "file_variations",
    "catch_isis_error",
    "compare_remote_file",
    "calculate_hours_since_timestamp",
    "NestedTomlDict",
    "compare_remote_file",
]


class NestedTomlDict:
    """A wrapper around tomlkit documents that supports dotted key access.
    
    This class automatically creates nested table structures when you use
    dotted keys like "config.indexes.static".
    
    Example:
        >>> doc = NestedTomlDict(Path("config.toml"))
        >>> doc.set("config.indexes.static", "last_updated", "2025-10-20")
        >>> # Creates: [config.indexes.static]
        >>> #           last_updated = "2025-10-20"
        >>> doc.save()
    """
    
    def __init__(self, file_path: Path):
        """Initialize with a file path, loading existing content if available.
        
        Args:
            file_path: Path to the TOML file
        """
        self.file_path = file_path
        try:
            with self.file_path.open("r", encoding="utf-8") as f:
                self.doc = tomlkit.load(f)
        except FileNotFoundError:
            self.doc = tomlkit.document()
    
    def set(self, dotted_key: str, field: str, value: Any) -> None:
        """Set a value using a dotted key path.
        
        Args:
            dotted_key: A dot-separated path like "config.indexes.static"
            field: The field name to set in the final nested table
            value: The value to set
        """
        keys = dotted_key.split(".")
        current = self.doc
        
        # Navigate/create nested structure
        for k in keys:
            if k not in current:
                current[k] = tomlkit.table()
            current = current[k]
        
        # Set the value on the innermost table
        current[field] = value
    
    def get(self, dotted_key: str, field: str | None = None) -> Any:
        """Get a value using a dotted key path.
        
        Args:
            dotted_key: A dot-separated path like "config.indexes.static"
            field: Optional field name to get from the final nested table.
                   If None, returns the entire nested table.
        
        Returns:
            The value at the specified path, or None if not found
        """
        keys = dotted_key.split(".")
        current = self.doc
        
        # Navigate the nested structure
        for k in keys:
            if k not in current:
                return None
            current = current[k]
        
        # Return the field value or the entire table
        if field is not None:
            return current.get(field)
        return current
    
    def to_dict(self) -> dict:
        """Convert to a regular Python dict."""
        return dict(self.doc)
    
    def dumps(self) -> str:
        """Dump to TOML string."""
        return tomlkit.dumps(self.doc)
    
    def save(self) -> None:
        """Save to the TOML file."""
        with self.file_path.open("w", encoding="utf-8") as f:
            tomlkit.dump(self.doc, f)


def is_older_than_hours(timestamp: dt.datetime, hours: float) -> bool:
    """
    Return True if the timestamp is older than the given number of hours.
    """
    return calculate_hours_since_timestamp(timestamp) > hours


def calculate_hours_since_timestamp(timestamp: dt.datetime) -> float:
    """
    Calculate the number of hours since the given timestamp.
    """
    now = dt.datetime.now()
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=dt.timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=dt.timezone.utc)
    time_diff = now - timestamp
    return time_diff.total_seconds() / 3600


def replace_all_doy_times(df: pd.DataFrame, timecol: str = "TIME") -> pd.DataFrame:
    """
    Convert all detected DOY time columns in df to datetimes in place.

    All columns with timecol in the name will be converted and changes will be
    implemented on incoming dataframe in place (no returned dataframe)!
    """

    for col in [col for col in df.columns if timecol in col]:
        df[col] = df[col].map(fromdoyformat)


# Network and file handling
def parse_http_date(http_date: str) -> dt.datetime:
    """Parse date string retrieved via urllib.request."""
    return dt.datetime(*eut.parsedate(http_date)[:6])


def get_remote_timestamp(url: str) -> dt.datetime:
    """
    Return the timestamp (last-modified) of a remote file at a URL.

    Useful for checking if there's an updated file available.
    """
    with urlopen(str(url), timeout=10) as conn:
        t = parse_http_date(conn.headers["last-modified"])
    return t


def check_url_exists(url: str) -> bool:
    """Check if a URL exists."""
    response = requests.head(url)
    return response.status_code < 400


def compare_remote_file(
    remote_url: str, local_path: Path, timeout: int = 30
) -> dict:
    """
    Compare content from a remote URL with a local file, keeping a temp copy of remote.

    Args:
        remote_url: URL to fetch remote content from
        local_path: Path to local file to compare against
        timeout: Timeout in seconds for the HTTP request

    Returns:
        dict: Contains 'has_updates' (bool), 'remote_tmp_path' (Path or None),
            and 'error' (str or None)
    """
    try:
        response = requests.get(remote_url, timeout=timeout)
        response.raise_for_status()

        remote_content = response.text
        
        # Read local content
        try:
            local_content = local_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            local_content = ""
        
        has_updates = remote_content != local_content
        
        # Save remote content to temp file if different
        remote_tmp_path = None
        if has_updates:
            # Create temp file with similar name
            remote_tmp_path = local_path.with_suffix(f".remote_tmp{local_path.suffix}")
            remote_tmp_path.write_text(remote_content, encoding="utf-8")

        return {
            "has_updates": has_updates,
            "remote_tmp_path": remote_tmp_path,
            "error": None,
        }

    except (requests.RequestException, requests.Timeout) as e:
        return {"has_updates": False, "remote_tmp_path": None, "error": str(e)}


def url_retrieve(
    url: str | URL,
    outfile: str,
    chunk_size: int = 4096,
    user: str = None,
    passwd: str = None,
    leave_tqdm: bool = True,
    disable_tqdm: bool = False,
):
    """
    Downloads a file from url to outfile.

    Improved urlretrieve with progressbar, timeout and chunker.
    This downloader has built-in progress bar using tqdm and the `requests`
    package. Improves on standard `urllib` by adding time-out capability.

    Testing different chunk_sizes, 128 was usually fastest, YMMV.

    Inspired by https://stackoverflow.com/a/61575758/680232

    Parameters
    ----------
    url : str | yarl.URL
        The URL to download
    outfile : str
        The path where to store the downloaded file.
    chunk_size : int
        def chunk size for the request.iter_content call
    user : str
        if provided, create HTTPBasicAuth object
    passwd : str
        if provided, create HTTPBasicAuth object
    leave_tqdm : bool
        passed to tqdm to leave the progress bar after completion. In mass processing
        scenarios, you might want to set this to False. Default: True
    """
    url = str(url)

    if user:
        auth = HTTPBasicAuth(user, passwd)
    else:
        auth = None
    R = requests.get(url, stream=True, allow_redirects=True, auth=auth)
    if R.status_code != 200:
        raise ConnectionError(f"Could not download {url}\nError code: {R.status_code}")
    with tqdm.wrapattr(
        open(outfile, "wb"),
        "write",
        miniters=1,
        leave=leave_tqdm,
        disable=disable_tqdm,
        total=int(R.headers.get("content-length", 0)),
        desc=str(Path(outfile).name),
    ) as fd:
        for chunk in R.iter_content(chunk_size=chunk_size):
            fd.write(chunk)


def have_internet() -> bool:
    """
    Fast way to check for active internet connection.

    From https://stackoverflow.com/a/29854274/680232
    """
    conn = httplib.HTTPConnection("www.google.com", timeout=5)
    try:
        conn.request("HEAD", "/")
        return True
    except Exception:
        return False
    finally:
        conn.close()


def file_variations(filename: str | Path, extensions: list[str]) -> list[Path]:
    """
    Return list of variations of a file name based on possible extensions.

    Generate a list of variations on a filename by replacing the extension with
    the provided list.

    Adapted from T. Olsens `file_variations of the pysis module for using pathlib.

    Parameters
    ----------
    filename : str or Path
        The original filename to use as a base.
    extensions : list
        List of extensions to use for variations.

    Raises
    ------
    TypeError
        If extensions is not a list
    ValueError
        If any extension doesn't start with a dot
    """
    if not isinstance(extensions, list):
        raise TypeError("extensions must be a list")

    return [Path(filename).with_suffix(extension) for extension in extensions]


def catch_isis_error(func):
    """can be used as decorator for any ISIS function"""
    if not ISIS_AVAILABLE:
        logger.warning("ISIS not available.")
        return
    def inner(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ProcessError as err:
            print("Had ISIS error:")
            print(" ".join(err.cmd))
            print(err.stdout)
            print(err.stderr)

    return inner


# def read_config_carefully(path):
#     "every module that uses toml config files should use this function to read them."
#     try:
#         config = tomlkit.loads(path.read_text())
#     except tomlkit.exceptions.TOMLKitError as e:
#         print(f"Error parsing TOML file: {e}")
#         config = None
#     except FileNotFoundError:
#         raise FileNotFoundError(f"Configuration file not found at {path}")
#     else:
#         return config
