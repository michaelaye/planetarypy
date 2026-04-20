"""Module for supporting projected ISIS cube files."""

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
from planetarypy.utils import catch_isis_error

try:
    from kalasiris import (
        findimageoverlaps,
        footprintinit,
        fromlist,
        getkey,
        getkey_k,
        isis2gml,
        overlapstats,
        spiceinit,
    )
except KeyError:
    warnings.warn("kalasiris has a problem initializing ISIS")


class IsisCube:
    def __init__(self, fpath):
        self.fpath = Path(fpath)

    @property
    def is_projected(self):
        """Check if this cube has been map-projected (Level 2)."""
        try:
            _ = getkey_k(self.fpath, "Mapping", "ProjectionName")
            return True
        except CalledProcessError:
            return False

    @property
    def shape_path(self):
        return self.fpath.with_suffix(".gml")

    @property
    def has_spiceinit(self):
        try:
            _ = getkey_k(self.fpath, "Kernels", "LeapSecond")
        except CalledProcessError:
            return False
        else:
            return True

    @catch_isis_error
    def spiceinit(self, web="yes", refresh=False) -> None:
        "Perform `spiceinit.`"
        if self.has_spiceinit and not refresh:
            return
        spiceinit(from_=self.fpath, web=web, _cwd=self.cub_path.parent)

    @property
    def has_footprint(self):
        try:
            name = getkey(
                self.fpath, objname="Polygon", keyword="Name"
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
        cwd = self.fpath.parent
        fpath = self.fpath
        if not self.has_footprint or refresh:
            footprintinit(from_=fpath, _cwd=cwd, **kwargs)
            logger.info("")
        if not self.shape_path.is_file() or refresh:
            isis2gml(
                from_=fpath,
                to=self.shape_path,
                _cwd=cwd,
            )

    @property
    def shape_as_geoseries(self):
        return gpd.read_file(self.shape_path)["geometry"]
    
    @property
    def shape_as_polygon(self):
        

    def plot(self):
        "returns re-usable holoviews plot object"
        da = utils.read_image(self.fpath)
        return da.hvplot(rasterize=True, aspect="equal", cmap="gray")


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


class IsisCubes:
    """IsisCube tasks on lists of files.

    Parameters
    ----------
    list_of_paths: list
        List of projected Isis cubes to process.
    workdir: str | Path, optional
        Working directory for output files. Defaults to current directory (".").
    """

    level_to_name = {
        0: "cubpaths",
        1: "calpaths",
        2: "mappaths",
    }

    def __init__(self, list_of_paths, workdir="."):
        self.paths = list_of_paths
        self.workdir = Path(workdir)

    @property
    def cubpaths(self):
        "List of level 0 cube paths"
        return [Path(p) for p in self.paths]
    
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

    def write_isis_filelist(self, workdir=None, level=1):
        workdir = self.workdir if workdir is None else Path(workdir)
        savepath = workdir / f"image_list_lev{level}.lis"
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
