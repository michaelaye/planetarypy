"""Module for dealing with CTX data."""

# import warnings
import os
import random
import warnings
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from functools import cached_property
from pathlib import Path
from subprocess import CalledProcessError

import geopandas as gpd
import hvplot.pandas  # noqa: F401
import pandas as pd
import pooch
from loguru import logger
from tqdm.auto import tqdm
from yarl import URL

from planetarypy.config import config
from planetarypy.instruments import utils
from planetarypy.pds import get_index
from planetarypy.utils import catch_isis_error, file_variations, read_config_carefully

try:
    from kalasiris import (
        cam2map,
        ctxcal,
        ctxevenodd,
        findimageoverlaps,
        footprintinit,
        fromlist,
        getkey,
        getkey_k,
        isis2gml,
        mroctx2isis,
        overlapstats,
        spiceinit,
    )
except KeyError:
    warnings.warn("kalasiris has a problem initializing ISIS")

# idea for later
# from planetarypy.instruments.base import Instrument
# import warnings

storage_root = Path(config["storage_root"])

configpath = Path.home() / ".planetarypy_mro_ctx.toml"

ctxconfig = read_config_carefully(configpath)
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


def product_id_from_serial_number(serial_number, refresh=False):
    """
    Given a serial_number like 'MRO/CTX/1234567890.123', return the matching PRODUCT_ID.
    """
    prefix = "MRO/CTX/"
    if not serial_number.startswith(prefix):
        raise ValueError("serial_number must start with 'MRO/CTX/'")
    count = serial_number[len(prefix) :]
    edrindex = get_edr_index(refresh=refresh)
    matches = edrindex[edrindex["SPACECRAFT_CLOCK_START_COUNT"].str.strip() == count]
    if matches.empty:
        raise ValueError(f"No PRODUCT_ID found for serial_number: {serial_number}")
    return matches.iloc[0]["PRODUCT_ID"]


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
        url = baseurl / self.meta.volume_id.lower() / "data" / (self.pid + ".IMG")
        return url

    def __repr__(self):
        return f"Raw(pid='{self.pid}')"

    def __str__(self):
        return self.__repr__()


class Calib:
    "Manage processing of raw PDS files using ISIS tools."

    def __init__(
        self,
        pid,  # CTX product_id
        destripe_to_calib=True,  # if to copy destriped files as calib files or leave extra
        prefer_mirror=True,
    ):
        self.pid = pid
        self.destripe_to_calib = destripe_to_calib
        self.raw = Raw(pid, prefer_mirror=prefer_mirror)
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

    @property
    def shape_path(self):
        return self.cal_path.with_suffix(".gml")

    @catch_isis_error
    def isis_import(self, refresh=False) -> None:
        "Import EDR data into ISIS cube."
        if not refresh and self.cub_path.is_file():
            return self.cub_path
        self.cub_path.parent.mkdir(exist_ok=True, parents=True)
        mroctx2isis(from_=self.raw.path, to=self.cub_path, _cwd=self.cub_path.parent)
        return self.cub_path

    @property
    def has_spiceinit(self):
        try:
            _ = getkey_k(self.cub_path, "Kernels", "LeapSecond")
        except CalledProcessError:
            return False
        else:
            return True

    @catch_isis_error
    def spiceinit(self, web="yes", refresh=False) -> None:
        "Perform `spiceinit.`"
        if self.has_spiceinit and not refresh:
            return
        spiceinit(from_=self.cub_path, web=web, _cwd=self.cub_path.parent)

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
                from_=self.cal_path, to=self.destripe_path, _cwd=self.cub_path.parent
            )
            if self.destripe_to_calib:
                self.destripe_path.replace(self.cal_path)
                return self.cal_path
            else:
                return self.destripe_path

    @catch_isis_error
    def map_project(self, mpp=6.25, refresh=False) -> None:
        "Perform map projection."
        cal_path = self.cal_path if self.destripe_to_calib else self.destripe_path
        if self.map_path.is_file() and not refresh:
            return self.map_path
        cam2map(
            from_=cal_path,
            to=self.map_path,
            pixres="mpp",
            resolution=mpp,
            _cwd=self.cub_path.parent,
        )
        return self.map_path

    @property
    def has_footprint(self):
        try:
            name = getkey(
                self.cal_path, objname="Polygon", keyword="Name"
            ).stdout.strip()
        except CalledProcessError:
            return False
        else:
            if name == "Footprint":
                return True
            else:
                logger.warning(
                    f"Footprint is not found in {self.cub_path} Polygon object, but {name} is found."
                )
                return False

    @catch_isis_error
    def footprintinit(self, refresh=False, **kwargs) -> None:
        """Initialize footprint for the calibrated cube.

        If map is None, mapping data will be taken
        """
        if not self.has_footprint or refresh:
            footprintinit(from_=self.cal_path, _cwd=self.cub_path.parent, **kwargs)
            logger.info("")
        if not self.shape_path.is_file() or refresh:
            isis2gml(
                from_=self.cal_path,
                to=self.shape_path,
                _cwd=self.cub_path.parent,
            )

    def plot_any(self, path):
        "returns re-usable holoviews plot object"
        da = utils.read_image(path)
        return da.hvplot(rasterize=True, aspect="equal", cmap="gray")

    def pipeline(self, project=False, refresh=False):
        logger.info("Importing...")
        self.isis_import(refresh=refresh)
        logger.info("Spiceinit...")
        self.spiceinit(refresh=refresh)
        logger.info("Calibrating...")
        self.calibrate(refresh=refresh)
        logger.info("Destriping (if spatial summing allows...")
        self.destripe()
        if project:
            logger.info("Map projecting..")
            self.map_project(refresh=refresh)
            return self.map_path
        elif self.destripe_to_calib:
            return self.cal_path
        else:
            return self.destripe_path


def calibrate_pid(pid, refresh=False):
    d = dict(pid=pid)
    try:
        Calib(pid).pipeline(refresh=refresh)
    except Exception as e:
        logger.error(e)
        d["success"] = False
    else:
        d["success"] = True
    return d


def download_pid(pid, refresh=False):
    d = dict(pid=pid)
    try:
        Calib(pid).isis_import(refresh=refresh)
    except Exception as e:
        logger.error(e)
        d["success"] = False
    else:
        d["success"] = True
    return d


def do_footprintinit(pid, refresh=False):
    d = dict(pid=pid)
    try:
        Calib(pid).footprintinit(refresh=refresh)
    except Exception as e:
        logger.error(e)
        d["success"] = False
    else:
        d["success"] = True
    return d


def process_parallel(Executor, task, pids, map=None, refresh=None):
    "Use ProcessPoolExecutor for CPU-bound tasks, and ThreadPoolExecutor for I/O-bound"
    argslist = []
    for pid in pids:
        args = [pid]
        kwargs = {}
        if map is not None:
            kwargs["map"] = map
        if refresh is not None:
            kwargs["refresh"] = refresh
        argslist.append((args, kwargs))
    with Executor() as executor:
        futures = [executor.submit(task, *args, **kwargs) for args, kwargs in argslist]

        results = []
        for future in tqdm(as_completed(futures), total=len(futures)):
            results.append(future.result())
    return results


class CTXCollection:
    """These are common ISIS tasks required to coregister CTX data.

    Args:
        list_of_pids (list):
            List of CTX product IDs to process.
        workdir (str or Path, optional):
            Working directory for output files. Defaults to current directory (".").
        remove_bad (bool, optional):
            If True, remove product IDs marked as bad data based on metadata, warning will be
            given. Defaults to True.
    """

    level_to_name = {
        0: "cubpaths",
        1: "calpaths",
        2: "mappaths",
    }

    def __init__(self, list_of_pids, workdir=".", remove_bad=True):
        self.pids = list_of_pids
        self.workdir = Path(workdir)
        if remove_bad:
            self.remove_bad_data()

    def remove_bad_data(self):
        "use meta data marker to remove bad pids"
        newlist = [pid for pid in self.pids if Raw(pid).data_ok]
        if (diff := len(self.pids) - len(newlist)) > 0:
            removed = set(self.pids) - set(newlist)
            logger.warning(
                f"Removed {diff} pids for being marked as bad data: {removed}"
            )
            self.pids = newlist

    @cached_property
    def calibs(self):
        return [Calib(pid) for pid in self.pids]

    @property
    def cubpaths(self):
        "get raw import cubes"
        return [cal.cub_path for cal in self.calibs]

    @property
    def calpaths(self):
        return [cal.cal_path for cal in self.calibs]

    @property
    def mappaths(self):
        return [cal.map_path for cal in self.calibs]

    @property
    def geodataframe_path(self):
        "get the path to the GeoDataFrame with all footprints"
        return self.workdir / "footprints.gdf"

    def check_paths(self, level=1):
        paths = getattr(self, self.level_to_name[level])
        for path in paths:
            if not path.exists():
                logger.info(f"{path} does not exist.")

    @property
    def image_list_lev1_path(self):
        "get the path to the image list file"
        return self.workdir / "image_list_lev1.lis"

    @property
    def image_list_lev2_path(self):
        "get the path to the image list file"
        return self.workdir / "image_list_lev2.lis"

    def create_image_list(self, workdir=None, level=1):
        workdir = self.workdir if workdir is None else Path(workdir)
        savepath = workdir / getattr(self, f"image_list_lev{level}_path").name
        match level:
            case 0:
                paths = self.cubpaths
            case 1:
                paths = self.calpaths
            case 2:
                paths = self.mappaths
        text = "\n".join([str(p) for p in paths])
        text += "\n"
        savepath.write_text(text)
        return savepath

    def download(self, refresh=False):
        "I/O-bound task, so using ThreadPool"
        return process_parallel(
            ThreadPoolExecutor, download_pid, self.pids, refresh=refresh
        )

    def calibrate(self, sample: int = None, refresh=False):
        """perform the whole import to calibrated level 1 pipeline.

        sample: How many samples of self.pids to process
        """
        todo = self.pids if sample is None else random.sample(self.pids, sample)
        return process_parallel(
            ProcessPoolExecutor, calibrate_pid, todo, refresh=refresh
        )

    def footprintinit(self, map=None, refresh=False):
        "perform ISIS footprint init on the list of files"
        return process_parallel(
            ProcessPoolExecutor, do_footprintinit, self.pids, map=None, refresh=refresh
        )

    @property
    def overlap_list_path(self):
        return self.workdir / "overlap_list.lis"

    def findimageoverlaps(self):
        with fromlist.temp(self.calpaths) as f:
            findimageoverlaps(fromlist=f, overlaplist=self.overlap_list_path)
        return self.overlap_list_path

    @property
    def overlap_stats_path(self):
        return self.workdir / "overlap_stats.csv"

    def overlap_stats(self):
        """Calculate overlap statistics for the collection."""
        if not self.overlap_list_path.is_file():
            logger.warning(
                "Overlap list not found. Run `findimageoverlaps()` first to create it."
            )
            return None
        overlapstats(
            fromlist=self.image_list_lev1_path,
            overlaplist=self.overlap_list_path,
            to=self.overlap_stats_path,
            table="csv",
            detail="full",
            singleline=False,
            _cwd=self.workdir,
        )
        df = pd.read_csv(self.overlap_stats_path)
        df.ffill(inplace=True)
        cols_to_int = [
            "Overlap ID",
            "Image Count",
        ]
        df[cols_to_int] = df[cols_to_int].astype(int)
        return df

    def read_gml_to_gdf(self, refresh=False):
        """Read the GML files from the collection and return a GeoDataFrame."""

        if self.geodataframe_path.is_file() and not refresh:
            logger.info(
                f"Found existing geodf at {self.geodataframe_path}, refresh with `refresh=True`."
            )
            return gpd.read_parquet(self.geodataframe_path)
        gdfs = []
        for cal in tqdm(self.calibs):
            if cal.shape_path.is_file():
                gdf = gpd.read_file(cal.shape_path, columns=[])  # read only geometry
                gdf["pid"] = cal.pid
                gdfs.append(gdf)
        gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))
        gdf.to_parquet(self.geodataframe_path, index=False)
        return gdf

    def autoseed(self, algo="strip"):
        # there's a module for it in my nbcoreg package
        # also covering pointreg and jigsaw
        pass

    def __str__(self):
        s = "CTXCollection()\n"
        s += f"{self.workdir=}\n"
        s += f"Len of pids: {len(self.pids)}"
        return s

    def __repr__(self):
        return self.__str__()
