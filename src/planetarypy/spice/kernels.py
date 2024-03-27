"""SPICE kernels management."""

__all__ = ['KERNEL_STORAGE', 'dataset_ids', 'df', 'df2', 'datasets', 'NAIF_URL', 'BASE_URL', 'GENERIC_STORAGE', 'GENERIC_URL',
           'generic_kernel_names', 'generic_kernel_paths', 'is_start_valid', 'is_stop_valid', 'download_one_url',
           'Subsetter', 'get_metakernel_and_files', 'list_kernels_for_day', 'download_generic_kernels',
           'load_generic_kernels', 'show_loaded_kernels']

import zipfile
from datetime import timedelta
from io import BytesIO
from itertools import repeat
from multiprocessing import cpu_count
from pathlib import Path

import requests
import spiceypy as spice
from astropy.time import Time
from dask.distributed import Client
from tqdm.auto import tqdm
from tqdm.contrib.concurrent import process_map
from yarl import URL

import pandas as pd
from ..config import config
from ..utils import nasa_time_to_iso, url_retrieve


KERNEL_STORAGE = config.storage_root / "spice_kernels"
KERNEL_STORAGE.mkdir(exist_ok=True, parents=True)


dataset_ids = {
    "bc": "bc/bc_spice",
    "cassini": "co-s_j_e_v-spice-6-v1.0/cosp_1000",
    "clementine": "clem1-l-spice-6-v1.0/clsp_1000",
    "dart": "dart/dart_spice",
    "dawn": "dawn-m_a-spice-6-v1.0/dawnsp_1000",
    "di": "di-c-spice-6-v1.0/disp_1000",
    "ds1": "ds1-a_c-spice-6-v1.0/ds1sp_1000",
    "epoxi": "dif-c_e_x-spice-6-v1.0/epxsp_1000",
    "em16": "em16/em16_spice",
    "grail": "grail-l-spice-6-v1.0/grlsp_1000",
    "hayabusa": "hay-a-spice-6-v1.0/haysp_1000",
    "insight": "insight/insight_spice",
    "juno": "jno-j_e_ss-spice-6-v1.0/jnosp_1000",
    "ladee": "ladee/ladee_spice",
    "lro": "lro-l-spice-6-v1.0/lrosp_1000",
    "maven": "maven/maven_spice",
    "opportunity": "mer1-m-spice-6-v1.0/mer1sp_1000",
    "spirit": "mer2-m-spice-6-v1.0/mer2sp_1000",
    "messenger": "mess-e_v_h-spice-6-v1.0/messsp_1000",
    "mars2020": "mars2020/mars2020_spice",
    "mex": "mex-e_m-spice-6-v2.0/mexsp_2000",
    "mgs": "mgs-m-spice-6-v1.0/mgsp_1000",
    "ody": "ody-m-spice-6-v1.0/odsp_1000",
    "mro": "mro-m-spice-6-v1.0/mrosp_1000",
    "msl": "msl-m-spice-6-v1.0/mslsp_1000",
    "near": "near-a-spice-6-v1.0/nearsp_1000",
    "nh": "nh-j_p_ss-spice-6-v1.0/nhsp_1000",
    "orex": "orex/orex_spice",
    "rosetta": "ro_rl-e_m_a_c-spice-6-v1.0/rossp_1000",
    "stardust": "sdu-c-spice-6-v1.0/sdsp_1000",
    "venus_climate_orbiter": "vco/vco_spice",
    "vex": "vex-e_v-spice-6-v2.0/vexsp_2000",
    "vo": "vo1_vo2-m-spice-6-v1.0/vosp_1000",
}


## Identifying and downloading kernel sets
# One repeating task for SPICE calculations is the identification and 
# retrieval of all SPICE kernels for a mission for a given time interval.
# The folks at NAIF offer a "Subset" feature at their servers. Here we set up 
# a table of the currently supported datasets:
df = pd.DataFrame({"shorthand": dataset_ids.keys(), "path": dataset_ids.values()})
df2 = pd.read_html("https://naif.jpl.nasa.gov/naif/data_archived.html")[6]
df2.columns = df2.iloc[0]
df2 = df2.drop(0).reset_index(drop=True)
df2 = df2.drop(["Archive Readme", "Archive Link", "Subset Link"], axis=1)
df = df.join(df2)

# Now available as `from planetarypy.spice.kernerls import datasets`
datasets = df.set_index("shorthand")  


## Validation helpers
def is_start_valid(mission: str, start: Time) -> bool:
    """
    Check if the start time is valid for a given mission.

    Parameters
    ----------
    mission : str
        Mission shorthand label of datasets dataframe.
    start : astropy.Time
        Start time in astropy.Time format.
    """
    return Time(datasets.at[mission, "Start Time"]) <= start


def is_stop_valid(mission: str, stop: Time) -> bool:
    """
    Check if the stop time is valid for a given mission.

    Parameters
    ----------
    mission : str
        Mission shorthand label of datasets dataframe.
    start : astropy.Time
        Start time in astropy.Time format.
    """   
    return Time(datasets.at[mission, "Stop Time"]) >= stop


# To access subsets of datasets, wrap the NAIF server's subsetds.pl script.
# The Perl script subsetds.pl (see BASE_URL below) requires as input:
# - the dataset name
# - start and stop of the time interval
# - a constant named "Subset" to identify the action for this Perl script
# We can assemble these parameters into a payload dictionary for the 
# requests.get call and we manage different potential actions on the zipfile 
# with a Subsetter class, that only requires the mission identifier, start and 
# stop as parameters.
NAIF_URL = URL("https://naif.jpl.nasa.gov")
BASE_URL = NAIF_URL / "cgi-bin/subsetds.pl"


def download_one_url(url, local_path, overwrite: bool = False):
    if local_path.exists() and not overwrite:
        return
    local_path.parent.mkdir(exist_ok=True, parents=True)
    url_retrieve(url, local_path)


class Subsetter:
    """
    Class to manage retrieving subset SPICE kernel lists.

    
    Attributes
    ----------
    kernel_names: List of names of kernels for the given time range.

    Methods
    -------
    download_kernels():
        Download SPICE kernels.
    get_metakernel():
        Get metakernel file from NAIF and adjust paths to match local storage.

    """
    def __init__(self, mission: str,  start: str, stop=None, save_location=None):
        """
        Initialize the Subsetter object.

        Parameters
        ----------
        mission : str
            Mission shorthand in datasets dataframe.
        start : str
            Start time in either ISO or yyyy-jjj format.
        stop : str, optional
            Stop time in either ISO or yyyy-jjj format. Defaults to None.
        save_location : str, optional
            Overwrite default storing in planetarypy archive. Defaults to None.
        """
        self.mission = mission
        self.start = start
        self.stop = stop
        self.save_location = save_location
        self.initialize()

    def initialize(self):
        r = self.r
        if r.ok:
            z = zipfile.ZipFile(BytesIO(r.content))
        else:
            raise IOError("SPICE Server request returned status code: {r.status_code}")
        self.z = z
        # these files only exist "virtually" in the zip object, but are needed to
        # extract them:
        self.urls_file = [n for n in z.namelist() if n.startswith("urls_")][0]
        self.metakernel_file = [n for n in z.namelist() if n.lower().endswith(".tm")][0]
        with self.z.open(self.urls_file) as f:
            self.kernel_urls = f.read().decode().split()

    @property
    def r(self):
        return requests.get(BASE_URL, params=self.payload, stream=True)

    @property
    def start(self):
        return self._start

    @start.setter
    def start(self, value):
        try:
            self._start = Time(value)
        except ValueError:
            self._start = Time(nasa_time_to_iso(value))

    @property
    def stop(self):
        return self._stop

    @stop.setter
    def stop(self, value):
        if not value:
            self._stop = self.start + timedelta(days=1)
        else:
            try:
                self._stop = Time(value)
            except ValueError:
                self._stop = Time(nasa_time_to_iso(value))

    @property
    def payload(self):
        """Put payload together while checking for working time format.

        If Time(self.start) doesn't work, then we assume that the date must be in the
        Time-unsupported yyyy-jjj format, which can be converted by `nasa_time_to_iso`
        from `planetarypy.utils`.
        """
        if not (is_start_valid(self.mission, self.start) and is_stop_valid(self.mission, self.stop)):
            raise ValueError("One of start/stop is outside the supported date-range. See `datasets`.")
        p = {
            "dataset": dataset_ids[self.mission],
            "start": self.start.iso,
            "stop": self.stop.iso,
            "action": "Subset",
        }
        return p

    @property
    def kernel_names(self):
        "Return list of names of kernels for the given time range."
        return [str(Path(URL(url).parent.name) / URL(url).name) for url in self.kernel_urls]

    def get_local_path(self, url) -> Path:
        """
        Return local storage path from Kernel URL. 
        
        Uses self.save_location if given.

        Parameters
        ----------
        url : str
            URL of the kernel file.        
        """
        u = URL(url)
        basepath = (
            KERNEL_STORAGE / self.mission if not self.save_location else self.save_location
        )
        return basepath / u.parent.name / u.name

    def _non_blocking_download(self, overwrite: bool = False):
        with Client() as client:
            futures = []
            for url in tqdm(self.kernel_urls, desc="Kernels downloaded"):
                local_path = self.get_local_path(url)
                if local_path.exists() and not overwrite:
                    print(local_path.parent.name, local_path.name, "locally available.")
                    continue
                local_path.parent.mkdir(exist_ok=True, parents=True)
                futures.append(client.submit(url_retrieve, url, local_path))
            return [f.result() for f in futures]

    def _concurrent_download(self, overwrite: bool = False):
        paths = [self.get_local_path(url) for url in self.kernel_urls]
        args = zip(self.kernel_urls, paths, repeat(overwrite))
        results = process_map(download_one_url, args, max_workers=cpu_count() - 2)

    def download_kernels(
        self,
        overwrite: bool = False, non_blocking: bool = False,
        quiet: bool = False,
    ):
        """
        Download SPICE kernels.

        Parameters
        ----------
        overwrite : bool, optional
            Overwrite existing kernels. Defaults to False.
        non_blocking : bool, optional
            Use Dask client for parallel download. Defaults to False.
        quiet : bool, optional
            Suppress name and path of downloaded kernels. Defaults to False.
        
        """
        if non_blocking:
            return self._non_blocking_download(overwrite)
        # sequential download
        for url in tqdm(self.kernel_urls, desc="Kernels downloaded"):
            local_path = self.get_local_path(url)
            if local_path.exists() and not overwrite:
                if not quiet:
                    print(local_path.parent.name, local_path.name, "locally available.")
                continue
            local_path.parent.mkdir(exist_ok=True, parents=True)
            url_retrieve(url, local_path)

    def get_metakernel(self) -> Path:
        """
        Download metakernel file from NAIF, adapt paths to match local storage
        and return local path to metakernel file.

        Uses self.save_location if given, otherwise `planetarypy` archive.
        """
        basepath = (KERNEL_STORAGE / self.mission if not self.save_location else self.save_location)
        savepath = basepath / self.metakernel_file
        with open(savepath, "w") as outfile, self.z.open(self.metakernel_file) as infile:
            for line in infile:
                linestr = line.decode()
                if "'./data'" in linestr:
                    linestr = linestr.replace("'./data'", f"'{savepath.parent}'")
                outfile.write(linestr)
        return savepath


def get_metakernel_and_files(
        mission: str,
        start: str,
        stop: str,
        save_location: str = None,
        quiet: bool = False
) -> Path:
    """
    For a given mission and start/stop times, download the kernels and get metakernel path.
    
    Parameters
    ----------
    mission : str
        Mission shorthand in datasets dataframe.
    start : str
        Start time in either ISO or yyyy-jjj format.
    stop : str
        Stop time in either ISO or yyyy-jjj format.
    save_location : str, optional
        Overwrite default storing in planetarypy archive. Defaults to None.
    quiet : bool, optional
        Suppress download feedback. Defaults to False.
    """
    
    subset = Subsetter(mission, start, stop, save_location)
    subset.download_kernels(non_blocking=True, quiet=quiet)
    return subset.get_metakernel()


def list_kernels_for_day(mission: str, start: str, stop: str = "") -> list:
    """
    List all kernels for a given time range of a mission.

    Parameters
    ----------
    mission : str
        Mission shorthand in datasets dataframe.
    start : str
        Start time in either ISO or yyyy-jjj format.
    stop : str, optional
        Stop time in either ISO or yyyy-jjj format. Defaults to None.
    """
    subset = Subsetter(mission, start, stop)
    return subset.kernel_names


## Generic kernel management
# These are a few generic kernels that are required for basic illumination 
# calculations as supported by this package.
GENERIC_STORAGE = KERNEL_STORAGE / "generic"
GENERIC_STORAGE.mkdir(exist_ok=True, parents=True)
GENERIC_URL = NAIF_URL / "pub/naif/generic_kernels/"

generic_kernel_names = [
    "lsk/naif0012.tls",
    "pck/pck00010.tpc",
    "pck/de-403-masses.tpc",
    "spk/planets/de430.bsp",
    "spk/satellites/mar097.bsp",
]
generic_kernel_paths = [GENERIC_STORAGE.joinpath(i) for i in generic_kernel_names]


def download_generic_kernels(overwrite=False):
    "Download all kernels as required by generic_kernel_list."
    dl_urls = [GENERIC_URL / i for i in generic_kernel_names]
    for dl_url, savepath in zip(dl_urls, generic_kernel_paths):
        if savepath.exists() and not overwrite:
            print(savepath.name, "already downloaded. Use `overwrite=True` to download again.")
            continue
        savepath.parent.mkdir(exist_ok=True, parents=True)
        url_retrieve(dl_url, savepath)


def load_generic_kernels():
    """Load all kernels in generic_kernels list.

    Loads pure planetary bodies meta-kernel without spacecraft data.

    Downloads any missing generic kernels.
    """
    if any([not p.exists() for p in generic_kernel_paths]):
        download_generic_kernels()
    for kernel in generic_kernel_paths:
        spice.furnsh(str(kernel))


def show_loaded_kernels():
    "Print overview of loaded kernels."
    count = spice.ktotal("all")
    if count == 0:
        print("No kernels loaded at this time.")
    else:
        print("The loaded files are:\n(paths relative to kernels.KERNEL_STORAGE)\n")
    for which in range(count):
        out = spice.kdata(which, "all", 100, 100, 100)
        print("Position:", which)
        p = Path(out[0])
        print("Path", p.relative_to(KERNEL_STORAGE))
        print("Type:", out[1])
        print("Source:", out[2])
        print("Handle:", out[3])
        # print("Found:", out[4])
