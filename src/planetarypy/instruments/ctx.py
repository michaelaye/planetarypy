"""Module for dealing with CTX data."""

# import warnings
import os
import warnings
from functools import cached_property
from pathlib import Path

import pooch
import rasterio
import tomlkit
from loguru import logger
from planetarypy.config import config
from planetarypy.instruments import utils
from planetarypy.pds import get_index
from planetarypy.utils import catch_isis_error, file_variations
from yarl import URL

try:
    from kalasiris.pysis import (
        cam2map,
        ctxcal,
        ctxevenodd,
        mroctx2isis,
        spiceinit,
    )
except KeyError:
    warnings.warn("kalasiris has a problem initializing ISIS")
import rioxarray as rxr

# idea for later
# from planetarypy.instruments.base import Instrument
# import warnings

storage_root = Path(config["storage_root"])

configpath = Path.home() / ".planetarypy_mro_ctx.toml"

try:
    ctxconfig = tomlkit.loads(configpath.read_text())
except tomlkit.exceptions.TOMLKitError as e:
    print(f"Error parsing TOML file: {e}")
    ctxconfig = None
except FileNotFoundError:
    raise FileNotFoundError(f"Configuration file not found at {configpath}")
# warnings.filterwarnings("ignore", category=rasterio.errors.NotGeoreferencedWarning)

baseurl = URL(ctxconfig["raw"]["url"])

# local mirror is a potentially read-only local data server that many groups have.
# usually a user can't write on it, hence extra treatment for it.
# first lookup would be tried here, if it's set in config file.
raw_local_mirror = ctxconfig["raw"]["local_mirror"]
mirror_readable = os.access(raw_local_mirror, os.R_OK)
mirror_writeable = os.access(raw_local_mirror, os.W_OK)

# The next is where we
# 1. lookup data if raw_local_mirror is not set-up or not readable (like currently unmounted drive
# 2. store new data that isn't on the local mirror if it is not writeable.
raw_local_storage = ctxconfig["raw"]["local_storage"]

# consider different cases for raw_local_storage
if not raw_local_storage:  # empty string
    # this would be the default location for data retrieved by planetarypy
    raw_local_storage = storage_root / "missions/mro/ctx"
else:
    # if then path given is not absolute, it will be attached to config.storage_root
    raw_local_storage = Path(raw_local_storage)
    if not raw_local_storage.is_absolute():
        raw_local_storage = storage_root / raw_local_storage

# make a cache for the index file to prevent repeated index loading
cache = dict()


def get_edr_index(refresh=False):
    "add some useful extra columns to the index."
    if "edrindex" in cache and not refresh:
        return cache["edrindex"]
    else:
        edrindex = get_index("mro.ctx.edr", refresh=refresh)
        edrindex["short_pid"] = edrindex.PRODUCT_ID.map(lambda x: x[:15])
        edrindex["month_col"] = edrindex.PRODUCT_ID.map(lambda x: x[:3])
        edrindex.LINE_SAMPLES = edrindex.LINE_SAMPLES.astype(int)
        cache["edrindex"] = edrindex
        return edrindex


class Raw:
    def __init__(self, pid: str, refresh_index=False, prefer_mirror=True):
        self.pid = pid  # product_id
        self.refresh_index = refresh_index
        self.with_volume = ctxconfig["raw"]["with_volume"]
        self.with_pid = ctxconfig["raw"]["with_pid"]
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
        edrindex = get_edr_index(refresh=self.refresh_index)
        s = edrindex.query("PRODUCT_ID == @self.pid").squeeze()
        s.name = f"Metadata for {self.pid}"
        s.index = s.index.str.lower()
        return s

    @property
    def is_data_ok(self):
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
        if raw_local_mirror and mirror_readable:
            return self._check_and_add_sub_paths(raw_local_mirror)
        else:
            return None

    @property
    def local_storage_folder(self):
        return self._check_and_add_sub_paths(raw_local_storage)

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
        if not mirror_readable:
            return _download(self.local_storage_folder)
        # this checks the mirror always first for reading and writing
        # but falls back to local storage if things fail.
        if self.prefer_mirror:
            try:
                return self._download(self.local_mirror_folder)
            except Exception as e:
                logger.warning(
                    "You preferred to use local mirror, but I can't access it.\n"
                    "Using local_storage."
                )
        return self._download(self.local_storage_folder)

    @property
    def url(self):
        "Calculate URL from input dataframe row."
        url = baseurl / self.meta.volume_id.lower() / "data" / (self.pid + ".IMG")
        return url

    def __repr__(self):
        return f"Raw(pid='{self.pid}')"

    def __str__(self):
        return self.__repr__()


class Calib:
    "Manage processing of raw PDS files."

    def __init__(
        self,
        pid,  # CTX product_id
        destripe_to_calib=True,  # if to copy destriped files as calib files or leave extra
    ):
        self.pid = pid
        self.destripe_to_calib = destripe_to_calib
        self.raw = Raw(pid)
        (self.cub_name, self.cal_name, self.destripe_name, self.map_name) = (
            file_variations(
                self.raw.path.name,
                [
                    ".cub",
                    f"{ctxconfig['calib']['calibrated_ext']}.cub",
                    ".dst.cal.cub",
                    f"{ctxconfig['calib']['mapped_ext']}.cub",
                ],
            )
        )
        self.with_volume = ctxconfig["calib"]["with_volume"]
        self.with_pid = ctxconfig["calib"]["with_pid"]
        self.spice_done = False

    def _check_and_add_sub_paths(self, base):
        base = Path(base) / self.raw.volume if self.with_volume else base
        base = base / self.pid if self.with_pid else base
        return base

    @property
    def storage_folder(self):
        if folder := ctxconfig["calib"]["storage"]:
            return self._check_and_add_sub_paths(folder)
        else:
            return self._check_and_add_sub_paths(
                Path(config["storage_root"]) / "missions/mro/ctx"
            )

    @property
    def cub_path(self):
        return self.storage_folder / self.cub_name

    @property
    def cal_path(self):
        return self.storage_folder / self.cal_name

    @property
    def destripe_path(self):
        return self.storage_folder / self.destripe_name

    @property
    def map_path(self):
        return self.storage_folder / self.map_name

    @catch_isis_error
    def isis_import(self, refresh=False) -> None:
        "Import EDR data into ISIS cube."
        if not refresh and self.cub_path.is_file():
            return self.cub_path
        self.cub_path.parent.mkdir(exist_ok=True, parents=True)
        mroctx2isis(from_=self.raw.path, to=self.cub_path, _cwd=self.cub_path.parent)
        return self.cub_path

    @catch_isis_error
    def spice_init(self, web="yes") -> None:
        "Perform `spiceinit.`"
        if not self.spice_done:
            spiceinit(from_=self.cub_path, web=web, _cwd=self.cub_path.parent)
        self.spice_done = True

    @catch_isis_error
    def calibrate(self, refresh=False) -> None:
        "Do ISIS `ctxcal`."
        if self.cal_path.is_file() and not refresh:
            return self.cal_path
        ctxcal(from_=self.cub_path, to=self.cal_path, _cwd=self.cub_path.parent)
        return self.cal_path

    @property
    def spatial_summing(self) -> int:
        "Get the spatial summing value from the index file."
        return int(self.raw.meta["spatial_summing"])

    @catch_isis_error
    def destripe(self) -> None:
        "Do destriping via `ctxevenodd` if allowed by summing status."
        if self.spatial_summing != 2:
            ctxevenodd(
                from_=self.cal_path, 
                to=self.destripe_path, 
                _cwd=self.cub_path.parent
            )
            if self.destripe_to_calib:
                self.destripe_path.rename(self.cal_path)

    @catch_isis_error
    def map_project(self, mpp=6.25) -> None:
        "Perform map projection."
        cal_path = self.cal_path if self.destripe_to_calib else self.destripe_path
        cam2map(
            from_=cal_path, 
            to=self.map_path, 
            pixres="mpp", 
            resolution=mpp,
            _cwd=self.cub_path.parent
        )

    def plot_any(self, path):
        "returns re-usable holoviews plot object"
        da = utils.read_image(path)
        return da.hvplot(rasterize=True, aspect="equal", cmap="gray")

    def pipeline(self, project=False):
        logger.info("Importing...")
        self.isis_import()
        logger.info("Spiceinit...")
        self.spice_init()
        logger.info("Calibrating...")
        self.calibrate()
        logger.info("Destriping (if spatial summing allows...")
        self.destripe()
        if project:
            logger.info("Map projecting..")
            self.map_project()
            return self.map_path
        elif self.destripe_to_calib:
            return self.cal_path
        else:
            return self.destripe_path


class CTXCollection:
    def __init__(self, list_of_pids):
        self.pids = list_of_pids