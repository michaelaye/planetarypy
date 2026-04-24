"""Module for dealing with CTX calibrated data (ISIS pipeline)."""

import random
import warnings
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from functools import cached_property
from pathlib import Path
from subprocess import CalledProcessError

import geopandas as gpd
import hvplot.pandas  # noqa: F401
import pandas as pd
from loguru import logger
from tqdm.auto import tqdm

from planetarypy.instruments import utils
from planetarypy.instruments.mro.ctx.ctx_edr import (
    CTXCONFIG,
    EDR,
    _calib_folder,
)
from planetarypy.utils import catch_isis_error, file_variations

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


class Calib:
    "Manage processing of EDR PDS files using ISIS tools."

    def __init__(
        self,
        pid,  # CTX product_id
        destripe_to_calib=True,  # if to copy destriped files as calib files or leave extra
        prefer_mirror=True,
        workdir=None,  # override for storage_folder; if set, all derived paths live here (no volume/pid sub-paths applied)
    ):
        self.destripe_to_calib = destripe_to_calib
        self.edr = EDR(pid, prefer_mirror=prefer_mirror)
        self.pid = self.edr.pid
        (self.cub_name, self.cal_name, self.destripe_name, self.map_name) = (
            file_variations(
                self.edr.path.name,
                [
                    ".cub",
                    f"{CTXCONFIG['calib']['calibrated_ext']}.cub",
                    ".dst.cal.cub",
                    f"{CTXCONFIG['calib']['mapped_ext']}.cub",
                ],
            )
        )
        self._workdir_override = Path(workdir) if workdir is not None else None

    @property
    def with_volume(self):
        """Whether the PDS volume segment is inserted into the calib path
        (read from ``CTXCONFIG["calib"]`` on each access)."""
        return CTXCONFIG["calib"]["with_volume"]

    @property
    def with_pid(self):
        """Whether the product_id segment is inserted into the calib path
        (read from ``CTXCONFIG["calib"]`` on each access)."""
        return CTXCONFIG["calib"]["with_pid"]

    @property
    def storage_folder(self):
        # User-supplied workdir overrides everything; no volume/pid sub-paths applied.
        # Callers are responsible for passing the final directory they want derived
        # files (.cub, .lev1.cub, .dst.cal.cub, .lev2.cub, .gml, …) to live in.
        # Useful for experimental runs that need to isolate calibration state from
        # the canonical planetarypy_data archive (e.g. GapPipeline rundirs).
        if self._workdir_override is not None:
            return self._workdir_override
        return _calib_folder(volume=self.edr.volume, pid=self.pid)

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
        mroctx2isis(from_=self.edr.path, to=self.cub_path, _cwd=self.cub_path.parent)
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
        return int(self.edr.meta["spatial_summing"])

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
        """Initialize footprint for the calibrated cube."""
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

    def quickview(self, stride=10):
        """Return a downsampled quickview from the best available cube file.

        Tries map_path, then cal_path, then cub_path, using rioxarray
        with dask chunks and stride slicing.

        Parameters
        ----------
        stride : int
            Take every stride-th pixel in both dimensions. Default 10.

        Returns
        -------
        tuple of (np.ndarray, Path, str)
            Downsampled 2D array, the path used, and a label for the level.
        """
        candidates = [
            (self.map_path, "map"),
            (self.cal_path, "cal"),
            (self.cub_path, "cub"),
        ]
        for path, level in candidates:
            if path.is_file():
                da = utils.read_image(path)
                arr = da.values[::stride, ::stride]
                return arr, path, level
        return None, None, None

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


def process_parallel(Executor, task, pids, refresh=None):
    "Use ProcessPoolExecutor for CPU-bound tasks, and ThreadPoolExecutor for I/O-bound"
    argslist = []
    for pid in pids:
        args = [pid]
        kwargs = {}
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

    def __init__(self, list_of_pids, workdir=".", remove_bad=True, calib_workdir=None):
        """
        list_of_pids : iterable of CTX product-IDs
        workdir : where collection-level artefacts (image lists, footprints.gdf, …) go.
        remove_bad : filter pids flagged as bad data via EDR metadata.
        calib_workdir : optional override for per-Calib storage_folder. When set, each
            Calib(pid, workdir=calib_workdir) — so .cub / .lev1.cub / .dst.cal.cub / .gml
            land in a flat directory at `calib_workdir` rather than the canonical
            `storage_root/mro/ctx/mrox_XXX/{pid}/`. Useful for experimental
            runs that need calibration state isolated from the canonical archive.
            None (default) keeps legacy behavior (canonical storage).
        """
        self.pids = list_of_pids
        self.workdir = Path(workdir)
        self.calib_workdir = Path(calib_workdir) if calib_workdir is not None else None
        if remove_bad:
            self.remove_bad_data()

    def remove_bad_data(self):
        "use meta data marker to remove bad pids"
        newlist = [pid for pid in self.pids if EDR(pid).data_ok]
        if (diff := len(self.pids) - len(newlist)) > 0:
            removed = set(self.pids) - set(newlist)
            logger.warning(
                f"Removed {diff} pids for being marked as bad data: {removed}"
            )
            self.pids = newlist

    @cached_property
    def calibs(self):
        return [Calib(pid, workdir=self.calib_workdir) for pid in self.pids]

    @cached_property
    def metadata(self):
        """Merged metadata DataFrame from all Calib EDR objects."""
        return pd.DataFrame([cal.edr.meta for cal in self.calibs])

    @property
    def cub_paths(self):
        "get EDR cubes"
        return [cal.cub_path for cal in self.calibs]

    @property
    def cal_paths(self):
        return [cal.cal_path for cal in self.calibs]

    @property
    def map_paths(self):
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
    def image_list_lev0_path(self):
        "get the path to the image list file"
        return self.workdir / "image_list_lev0.lis"

    @property
    def image_list_lev1_path(self):
        "get the path to the image list file"
        return self.workdir / "image_list_lev1.lis"

    @property
    def image_list_lev2_path(self):
        "get the path to the image list file"
        return self.workdir / "image_list_lev2.lis"

    def write_isis_filelist(self, workdir=None, fname=None, level=1):
        workdir = self.workdir if workdir is None else Path(workdir)
        logger.debug(f"{self.workdir=}")
        savepath = (
            workdir / f"image_list_lev{level}.lis" if fname is None else workdir / fname
        )
        match level:
            case 0:
                paths = self.cub_paths
            case 1:
                paths = self.cal_paths
            case 2:
                paths = self.map_paths
        text = "\n".join([str(p) for p in paths])
        text += "\n"
        savepath.write_text(text)
        logger.info(f"Created image list at {savepath}")
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

    def footprintinit(self, refresh=False):
        "perform ISIS footprint init on the list of files"
        return process_parallel(
            ProcessPoolExecutor, do_footprintinit, self.pids, refresh=refresh
        )

    @property
    def overlap_list_path(self):
        return self.workdir / "overlap_list.bin"

    def findimageoverlaps(self):
        with fromlist.temp(self.cal_paths) as f:
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
