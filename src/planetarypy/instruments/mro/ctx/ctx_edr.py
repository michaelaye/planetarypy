"""Module for working with CTX EDR data.

The configuration file `planetarypy_mro_ctx.toml` enables you to configure where EDR is being searched for and where it is being stored.
See below for examples and some explanations.
"""

# import warnings
import os
from functools import cached_property
from pathlib import Path
from typing import Literal

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

CONFIGPATH = Path.home() / ".planetarypy_mro_ctx.toml"

with CONFIGPATH.open() as f:
    CTXCONFIG = tomlkit.load(f)

BASEURL = URL(CTXCONFIG["edr"]["url"])

Level = Literal["edr", "calib"]


def _storage_root():
    """Current ``config["storage_root"]`` as a Path (looked up on each call
    so user-level config edits take effect without re-importing this module).
    """
    return Path(config["storage_root"])


def _resolve_storage_root(raw, default_sub):
    """Normalize a CTXCONFIG storage-path value.

    Empty → ``{storage_root}/{default_sub}``. Relative → joined to
    ``storage_root``. Absolute → used as-is.
    """
    root = _storage_root()
    if not raw:
        return root / default_sub
    p = Path(raw)
    return p if p.is_absolute() else root / p


def _edr_local_mirror():
    """Configured read-mostly mirror path, or None if empty."""
    value = CTXCONFIG["edr"]["local_mirror"]
    return Path(value) if value else None


def _mirror_readable():
    mirror = _edr_local_mirror()
    return bool(mirror) and os.access(mirror, os.R_OK)


def _level_base(level: Level):
    if level == "edr":
        return _resolve_storage_root(
            CTXCONFIG["edr"]["local_storage"], "mro/ctx",
        )
    if level == "calib":
        return _resolve_storage_root(
            CTXCONFIG["calib"]["storage"], "mro/ctx",
        )
    raise ValueError(f"Unknown CTX data level: {level!r}")


def ctx_storage_folder(
    level: Level, volume=None, pid=None, base=None,
):
    """Build the CTX storage folder for a given data level.

    Single source of truth for EDR, Calib, and the catalog resolver
    (``plp fetch mro.ctx.edr``), so every entry point lays files out
    the same way.

    Parameters
    ----------
    level : {"edr", "calib"}
        Which ``CTXCONFIG`` section drives the ``with_volume`` / ``with_pid``
        toggles.
    volume : str | None
        PDS volume (e.g. ``"mrox_0123"``). Inserted only when
        ``[level].with_volume`` is true.
    pid : str | None
        Product ID. Inserted only when ``[level].with_pid`` is true.
    base : Path | str | None
        Override for the base directory. ``None`` uses the config-derived
        root for ``level``. Pass a mirror root here to apply the same
        volume/pid layout under a read-mostly mirror.
    """
    if level not in ("edr", "calib"):
        raise ValueError(f"Unknown CTX data level: {level!r}")
    section = CTXCONFIG[level]
    base = Path(base) if base is not None else _level_base(level)
    if section["with_volume"] and volume:
        base = base / volume
    if section["with_pid"] and pid:
        base = base / pid
    return base


# make a cache for the index file to prevent repeated index loading
cache = dict()
# Track if we've already checked for updates this session to avoid repeated
# Index instantiations.
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
        self.prefer_mirror = prefer_mirror

    @property
    def with_volume(self):
        """Whether the PDS volume segment is inserted into the storage path
        (read from ``CTXCONFIG["edr"]`` on each access)."""
        return CTXCONFIG["edr"]["with_volume"]

    @property
    def with_pid(self):
        """Whether the product_id segment is inserted into the storage path
        (read from ``CTXCONFIG["edr"]`` on each access)."""
        return CTXCONFIG["edr"]["with_pid"]

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

    @property
    def fname(self):
        return self.pid + ".IMG"

    @property
    def local_mirror_folder(self):
        mirror = _edr_local_mirror()
        if mirror and _mirror_readable():
            return ctx_storage_folder(
                "edr", volume=self.volume, pid=self.pid, base=mirror,
            )
        return None

    @property
    def local_storage_folder(self):
        return ctx_storage_folder("edr", volume=self.volume, pid=self.pid)

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
        if not _mirror_readable():
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
    return _level_base("edr") / "product_ids.txt"


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


# ── Catalog resolver registration ─────────────────────────────────
#
# Wired into planetarypy.catalog._resolver._STORAGE_RESOLVER_MODULES
# so `plp fetch mro.ctx.edr <pid>` lands in the same folder that
# `EDR(pid).local_storage_folder` resolves to.

def _ctx_local_product_dir(product_type, product_id):
    """Resolve the local directory for `plp fetch mro.ctx.<product_type>`.

    For ``product_type == "edr"`` this returns the same folder that
    ``EDR(product_id).local_storage_folder`` produces, keeping the CLI
    ``plp fetch`` and the programmatic ``EDR``/``ctxqv`` paths aligned.

    For other product types this falls back to the catalog's generic
    layout so registering the resolver doesn't accidentally change
    behavior for unrelated CTX products.
    """
    if product_type == "edr":
        # Skip the index lookup when volume isn't needed — avoids loading
        # the full CTX EDR parquet just to resolve a path.
        if not CTXCONFIG["edr"]["with_volume"]:
            return ctx_storage_folder("edr", pid=product_id)
        return EDR(product_id).local_storage_folder
    safe_pid = product_id.replace("/", "_").replace("\\", "_")
    return _storage_root() / "mro" / "ctx" / product_type / safe_pid
