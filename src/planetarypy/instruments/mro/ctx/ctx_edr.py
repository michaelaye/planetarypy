"""Module for working with CTX EDR data.

The configuration file `planetarypy_mro_ctx.toml` enables you to configure where EDR is being searched for and where it is being stored.
See below for examples and some explanations.
"""

# import warnings
import os
from functools import cached_property
from pathlib import Path

import numpy as np
import warnings

import hvplot.pandas  # noqa: F401
import pooch

# pvl 1.3.2 self-triggers PendingDeprecationWarning; see pvl#109
with warnings.catch_warnings():
    warnings.filterwarnings("ignore", message=".*pvl.collections.Units.*")
    import pvl
import tomlkit
from loguru import logger
from yarl import URL

from planetarypy.config import config
from planetarypy.pds import get_index

# idea for later
# from planetarypy.instruments.base import Instrument

STORAGE_ROOT = Path(config["storage_root"])

CONFIGPATH = Path.home() / ".planetarypy_mro_ctx.toml"

with CONFIGPATH.open() as f:
    CTXCONFIG = tomlkit.load(f)

# warnings.filterwarnings("ignore", category=rasterio.errors.NotGeoreferencedWarning)

BASEURL = URL(CTXCONFIG["edr"]["url"])

# local mirror is a potentially read-only local data server that some workgroups may have.
# usually a user can't write on it, hence extra treatment for it.
# first lookup would be tried here, if it's set in config file.
EDR_LOCAL_MIRROR = CTXCONFIG["edr"]["local_mirror"]
MIRROR_READABLE = os.access(EDR_LOCAL_MIRROR, os.R_OK)
MIRROR_WRITEABLE = os.access(EDR_LOCAL_MIRROR, os.W_OK)

# Next is where we
# 1. lookup data if EDR_LOCAL_MIRROR is not set-up or not readable (e.g. currently unmounted drives)
# 2. store new data that isn't on the local mirror if it is not writeable.
EDR_LOCAL_STORAGE = CTXCONFIG["edr"]["local_storage"]

# consider different cases for EDR_LOCAL_STORAGE
if not EDR_LOCAL_STORAGE:  # empty string
    # this would be the default location for data retrieved by planetarypy
    EDR_LOCAL_STORAGE = STORAGE_ROOT / "mro/ctx"
else:
    # if the path given is not absolute, it will be attached to config.storage_root
    EDR_LOCAL_STORAGE = Path(EDR_LOCAL_STORAGE)
    if not EDR_LOCAL_STORAGE.is_absolute():
        EDR_LOCAL_STORAGE = STORAGE_ROOT / EDR_LOCAL_STORAGE

# make a cache for the index file to prevent repeated index loading
cache = dict()
# Track if we've already checked for updates this session to avoid repeated Index instantiations
_update_checked_this_session = False


def get_edr_index(allow_refresh=True):  # can afford refresh as it's cached
    "add some useful extra columns to the index."
    global _update_checked_this_session

    # Use cache if available
    # If cache exists and we've already checked for updates this session, use cache
    # This prevents repeated Index instantiations when multiple EDR instances
    # are created (e.g., in remove_bad_data())
    if "edrindex" in cache:
        if (not allow_refresh) or _update_checked_this_session:
            return cache["edrindex"]

    # Call get_index() with allow_refresh - it will only refresh if update is available
    # This creates an Index instance and checks for updates, but only once per session
    edrindex = get_index("mro.ctx.edr", allow_refresh=allow_refresh)
    edrindex["short_pid"] = edrindex.PRODUCT_ID.map(lambda x: x[:15])
    edrindex["month_col"] = edrindex.PRODUCT_ID.map(lambda x: x[:3])
    cache["edrindex"] = edrindex
    if allow_refresh:
        _update_checked_this_session = True
    return edrindex


def product_id_from_serial_number(serial_number, allow_refresh=True):
    """
    Given a serial_number like 'MRO/CTX/1234567890.123', return the matching PRODUCT_ID.
    """
    PREFIX = "MRO/CTX/"
    if not serial_number.startswith(PREFIX):
        raise ValueError("serial_number must start with 'MRO/CTX/'")
    count = serial_number[len(PREFIX) :]
    edrindex = get_edr_index(allow_refresh=allow_refresh)
    matches = edrindex[edrindex["SPACECRAFT_CLOCK_START_COUNT"].str.strip() == count]
    if matches.empty:
        raise ValueError(f"No PRODUCT_ID found for serial_number: {serial_number}")
    return matches.iloc[0]["PRODUCT_ID"]


class EDR:
    def __init__(self, pid: str, allow_refresh_index=True, prefer_mirror=True):
        self.pid = pid  # product_id
        self.allow_refresh_index = allow_refresh_index
        # should the PDS volumen become part of the path?
        self.with_volume = CTXCONFIG["edr"]["with_volume"]
        # should the product_id become part of the path?
        self.with_pid = CTXCONFIG["edr"]["with_pid"]
        self.prefer_mirror = prefer_mirror

    @property
    def pid(self):
        return self._pid

    @pid.setter
    def pid(self, value):
        if len(value) < 26:
            val = value[:15]  # use short_pid
            self.edrindex = get_edr_index()
            value = self.edrindex.query(f"short_pid=='{val}'").PRODUCT_ID.iloc[0]
        self._pid = value

    @property
    def short_pid(self):
        return self.pid[:15]

    @cached_property
    def meta(self):
        "get the metadata from the index table"
        edrindex = get_edr_index(allow_refresh=self.allow_refresh_index)
        s = edrindex.query("PRODUCT_ID == @self.pid").squeeze()
        s.name = f"Metadata for {self.pid}"
        s.index = s.index.str.lower()
        return s

    @property
    def image_time(self):
        return self.meta.image_time

    @property
    def serial_number(self):
        count = self.meta["SPACECRAFT_CLOCK_START_COUNT"].strip()
        return f"MRO/CTX/{count}"

    @property
    def data_ok(self):
        return True if self.meta.data_quality_desc.strip() == "OK" else False

    @property
    def volume(self):
        "get the PDS volume number for the current product id"
        return self.meta.volume_id.lower()

    def _check_and_add_sub_paths(self, base):
        base = Path(base) / self.volume if self.with_volume else base
        base = base / self.pid if self.with_pid else base
        return base

    @property
    def fname(self):
        return self.pid + ".IMG"

    @property
    def local_mirror_folder(self):
        if EDR_LOCAL_MIRROR and MIRROR_READABLE:
            return self._check_and_add_sub_paths(EDR_LOCAL_MIRROR)
        else:
            return None

    @property
    def local_storage_folder(self):
        return self._check_and_add_sub_paths(EDR_LOCAL_STORAGE)

    def _download(self, folder):
        return Path(
            pooch.retrieve(
                url=str(self.url),
                known_hash=None,
                fname=self.fname,
                path=folder,
                progressbar=True,
            )
        )

    @property
    def path(self):
        # easiest case
        if not MIRROR_READABLE:
            return self._download(self.local_storage_folder)
        # this checks the mirror always first for reading and writing
        # but falls back to local storage if things fail.
        if self.prefer_mirror:
            try:
                return self._download(self.local_mirror_folder)
            except Exception:
                logger.warning(
                    "You preferred to use local mirror, but I can't access it.\n"
                    "Using local_storage."
                )
        return self._download(self.local_storage_folder)

    @property
    def url(self):
        "Calculate URL from input dataframe row."
        url = BASEURL / self.meta.volume_id.lower() / "data" / (self.pid + ".IMG")
        return url

    PDS3_DTYPE_MAP = {
        ("UNSIGNED_INTEGER", 8): np.uint8,
        ("UNSIGNED_INTEGER", 16): np.dtype(">u2"),
        ("INTEGER", 16): np.dtype(">i2"),
        ("IEEE_REAL", 32): np.dtype(">f4"),
    }

    def quickview(self, stride=10):
        """Return a downsampled 2D numpy array by memory-mapping the raw .IMG.

        Uses np.memmap with stride slicing so only every `stride`-th pixel
        is read from disk — the full image is never loaded into memory.

        Parameters
        ----------
        stride : int
            Take every stride-th pixel in both dimensions. Default 10.

        Returns
        -------
        np.ndarray
            Downsampled 2D image array.
        """
        img_path = self.path
        label = pvl.load(str(img_path))
        image_obj = label["IMAGE"]

        lines = image_obj["LINES"]
        samples = image_obj["LINE_SAMPLES"]
        sample_type = image_obj["SAMPLE_TYPE"]
        sample_bits = image_obj["SAMPLE_BITS"]
        prefix_bytes = image_obj.get("LINE_PREFIX_BYTES", 0)
        suffix_bytes = image_obj.get("LINE_SUFFIX_BYTES", 0)

        dtype = self.PDS3_DTYPE_MAP.get((sample_type, sample_bits))
        if dtype is None:
            raise ValueError(
                f"Unsupported PDS3 sample format: {sample_type}, {sample_bits} bits"
            )
        dtype = np.dtype(dtype)

        record_bytes = label["RECORD_BYTES"]
        # ^IMAGE pointer is 1-based record number
        image_pointer = label["^IMAGE"]
        offset = (image_pointer - 1) * record_bytes

        row_bytes = prefix_bytes + samples * dtype.itemsize + suffix_bytes

        if prefix_bytes == 0 and suffix_bytes == 0:
            mm = np.memmap(
                str(img_path),
                dtype=dtype,
                mode="r",
                offset=offset,
                shape=(lines, samples),
            )
            return mm[::stride, ::stride].copy()

        # With prefix/suffix bytes we can't memmap as a simple 2D array,
        # so read row by row using a flat byte memmap.
        total_bytes = lines * row_bytes
        raw = np.memmap(str(img_path), dtype=np.uint8, mode="r",
                        offset=offset, shape=(total_bytes,))
        result_lines = range(0, lines, stride)
        result_samples = range(0, samples, stride)
        out = np.empty((len(result_lines), len(result_samples)), dtype=dtype)
        for i, line in enumerate(result_lines):
            row_start = line * row_bytes + prefix_bytes
            row_data = raw[row_start:row_start + samples * dtype.itemsize]
            row = np.frombuffer(row_data, dtype=dtype)
            out[i] = row[::stride]
        return out

    def __repr__(self):
        return f"EDR(pid='{self.pid}') # Volume: {self.volume}"

    def __str__(self):
        return self.__repr__()


# ── Product ID Cache (for tab completion) ─────────────────────────

def _pid_cache_path() -> Path:
    """Path to the cached CTX product ID list file."""
    return Path(config["storage_root"]) / "mro" / "ctx" / "product_ids.txt"


def rebuild_pid_cache() -> Path:
    """Rebuild the CTX product ID cache from the EDR index.

    Returns
    -------
    Path
        Path to the cache file.
    """
    cache = _pid_cache_path()
    cache.parent.mkdir(parents=True, exist_ok=True)
    df = get_edr_index(allow_refresh=False)
    pids = sorted(df["PRODUCT_ID"].unique())
    cache.write_text("\n".join(pids) + "\n")
    logger.info(f"Rebuilt CTX product ID cache: {len(pids)} entries at {cache}")
    return cache


def complete_ctx_pid(incomplete: str) -> list[str]:
    """Return CTX product IDs matching a prefix (for tab completion).

    Parameters
    ----------
    incomplete : str
        Prefix to match against.

    Returns
    -------
    list of str
        Matching product IDs.
    """
    cache = _pid_cache_path()
    if not cache.exists():
        try:
            rebuild_pid_cache()
        except Exception:
            return []

    prefix = incomplete.upper()
    matches = []
    with open(cache) as f:
        for line in f:
            pid = line.rstrip()
            if pid.startswith(prefix):
                matches.append(pid)
            elif matches:
                break
    return matches
