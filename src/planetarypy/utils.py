"""General utility functions for planetarypy."""

__all__ = [
    "logger",
    "replace_all_doy_times",
    "parse_http_date",
    "get_remote_timestamp",
    "check_url_exists",
    "url_retrieve",
    "have_internet",
    "file_variations",
]

import datetime as dt
import email.utils as eut
import http.client as httplib
import logging
from pathlib import Path
from typing import Union
from urllib.request import urlopen

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from tqdm.auto import tqdm

from planetarypy.datetime import fromdoyformat

logger = logging.getLogger(__name__)


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


def check_url_exists(url):
    """Check if a URL exists."""
    response = requests.head(url)
    return response.status_code < 400


def url_retrieve(
    url: str,
    outfile: str,
    chunk_size: int = 4096,
    user: str = None,
    passwd: str = None,
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
    url : str
        The URL to download
    outfile : str
        The path where to store the downloaded file.
    chunk_size : int
        def chunk size for the request.iter_content call
    user : str
        if provided, create HTTPBasicAuth object
    passwd : str
        if provided, create HTTPBasicAuth object
    """
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
        total=int(R.headers.get("content-length", 0)),
        desc=str(Path(outfile).name),
    ) as fd:
        for chunk in R.iter_content(chunk_size=chunk_size):
            fd.write(chunk)


def have_internet():
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


def file_variations(filename: Union[str, Path], extensions: list[str]) -> list[Path]:
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
