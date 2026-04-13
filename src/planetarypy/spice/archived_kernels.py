"""SPICE kernels management for archived datasets.

To access subsets of datasets, wrap the NAIF server's subsetds.pl script.
The Perl script subsetds.pl (see BASE_URL below) requires as input:
- the dataset name
- start and stop of the time interval
- a constant named "Subset" to identify the action for this Perl script
We can assemble these parameters into a payload dictionary for the
requests.get call and we manage different potential actions on the zipfile
with a Subsetter class, that only requires the mission identifier, start and
stop as parameters.
"""

__all__ = [
    "datasets",
    "download_one_url",
    "Subsetter",
    "get_metakernel_and_files",
    "list_kernels_for_day",
]

import zipfile
from datetime import timedelta
from io import BytesIO
from itertools import repeat
from multiprocessing import cpu_count
from pathlib import Path

import pandas as pd
import requests
from astropy.time import Time
from loguru import logger
from tqdm.auto import tqdm
from tqdm.contrib.concurrent import process_map
from yarl import URL

from ..datetime_format_converters import fromdoyformat
from ..pds.index_logging import AccessLog
from ..utils import url_retrieve
from .config import BASE_URL, KERNEL_STORAGE, NAIF_URL

ARCHIVE_URL = NAIF_URL / "naif/data_archived.html"
# Cache location for the parsed datasets table; reused across sessions
DATASETS_CACHE = Path.home() / ".planetarypy_cache" / "archived_spice_datasets.csv"

shorthands = {
    "bc": "BepiColombo",
    "clps": "CLPS",
    "cassini": "Cassini Orbiter",
    "clementine": "Clementine",
    "dart": "DART",
    "dawn": "DAWN",
    "di": "Deep Impact",
    "ds1": "Deep Space 1",
    "epoxi": "EPOXI",
    "em16": "ExoMars TGO 2016",
    "grail": "GRAIL",
    "hayabusa": "Hayabusa",
    "hayabusa2": "Hayabusa2",
    "insight": "InSight",
    "juno": "JUNO",
    "ladee": "LADEE",
    "lucy": "Lucy",
    "lro": "Lunar Reconnaissance Orbiter",
    "maven": "MAVEN",
    "opportunity": "MER 1 (Opportunity)",
    "mer1": "MER 1 (Opportunity)",
    "spirit": "MER 2 (Spirit)",
    "mer2": "MER 2 (Spirit)",
    "messenger": "MESSENGER",
    "mars2020": "Mars 2020",
    "mex": "Mars Express",
    "mgs": "Mars Global Surveyor",
    "ody": "Mars Odyssey",
    "mro": "Mars Reconnaissance Orbiter",
    "msl": "Mars Science Laboratory",
    "near": "NEAR",
    "nh": "New Horizons",
    "orex": "OSIRIS-REx",
    "psyche": "Psyche",
    "rosetta": "Rosetta",
    "stardust": "Stardust",
    "venus_climate_orbiter": "Venus Climate Orbiter",
    "vex": "Venus Express",
    "vo": "Viking Orbiter",
}

shorthands = pd.Series(shorthands)
shorthands.name = "Mission Name"
shorthands.index.name = "shorthand"


def last_part(path, n):
    """Show only the last n parts from a pathlib.Path object.

    URL derives from pathlib.Path, so that works as well.
    """
    # only the last 2 path parts are part of the perl script's payload
    return Path(*path.parts[-n:]) if n > 0 else path


def _resolve_mission(mission_input: str) -> tuple[str, str | None]:
    """Resolve a mission identifier to (label, shorthand).

    Returns a tuple (mission_name, mission_code_or_None).

    - If mission_input is a shorthand key (e.g. 'mro'), returns (long name, shorthand).
    - If mission_input is a long name present in the datasets index or in the
      shorthands mapping values, returns (long name, shorthand or None).
    - Matching is case-insensitive for long names and shorthands.
    """
    if not mission_input:
        raise ValueError("mission_input must be a non-empty string")

    # direct shorthand match
    if mission_input in shorthands:
        return shorthands[mission_input], mission_input

    lower = mission_input.lower()
    # direct long-name exact match among shorthand values
    for code, label in shorthands.items():
        if label.lower() == lower:
            return label, code

    # If datasets already loaded, check index case-insensitively
    try:
        labels = [str(x) for x in datasets.index]
    except Exception:
        labels = []

    for lab in labels:
        if lab.lower() == lower:
            # attempt to find shorthand for this label
            for code, label in shorthands.items():
                if label.lower() == lab.lower():
                    return lab, code
            return lab, None

    # No exact matches; try to fuzzy-match shorthand (case-insensitive)
    for code in shorthands:
        if code.lower() == lower:
            return shorthands[code], code

    # Last resort: accept the input as label (no shorthand)
    return mission_input, None


def get_datasets():
    """Retrieve the NAIF archived datasets table with a once-per-day cache.

    Uses the shared PDS AccessLog mechanism to avoid refetching more than once
    per day. The parsed table is cached to a CSV under ~/.planetarypy_cache.
    """
    # Use the same central log file mechanism as PDS indices (no new log file)
    log = AccessLog("spice.archived_kernels.datasets")
    logger.debug(f"Datasets cache path: {DATASETS_CACHE}")
    logger.debug(
        "Datasets last_checked: {} | should_check: {}".format(
            log.last_check, log.should_check
        )
    )
    # If we've checked within the last day and a cache exists, load and return it
    if DATASETS_CACHE.is_file() and not log.should_check:
        try:
            logger.info("Using cached SPICE datasets table")
            return pd.read_csv(DATASETS_CACHE, index_col=0)
        except Exception as e:
            # If cache is unreadable, fall through to refresh
            logger.warning(
                f"Failed to read cached datasets at {DATASETS_CACHE}: {e}; refetching"
            )

    # Ensure parent exists before writing cache later
    DATASETS_CACHE.parent.mkdir(parents=True, exist_ok=True)

    # Fetch and parse from the remote page
    logger.info(f"Fetching SPICE datasets table from {ARCHIVE_URL}")
    res = pd.read_html(str(ARCHIVE_URL), extract_links="all", header=0)[
        6
    ]  # table is at index 6

    # The extract_links causes a lot of (<text>, None) tuples.
    # Normalize link cells: replace (text, None) with plain text; keep (text, href) when a link exists
    def _normalize_link_cell(v):
        # v can be a string, a (text, href) tuple, or a list of such tuples
        if isinstance(v, list):
            # Prefer the first tuple that has a real href; otherwise fall back to the first text
            for t in v:
                if isinstance(t, tuple) and t[1]:
                    return t  # keep (text, href)
            # No hrefs found; return just the text of the first tuple if present
            return v[0][0] if v and isinstance(v[0], tuple) else v
        if isinstance(v, tuple):
            text, href = v
            return text if not href else (text, href)
        return v

    df = res.map(_normalize_link_cell)

    # clean up header names and set index
    df.columns = [_normalize_link_cell(col) for col in df.columns]
    df = df.set_index("Mission Name")

    for col in df.columns:
        # If any value in the column is a (text, href) tuple, split it
        if df[col].apply(lambda x: isinstance(x, tuple)).any():
            # df[col] = df[col].apply(lambda x: urljoin(url, x[1]) if isinstance(x, tuple) and x[1] else None)
            df[col] = df[col].apply(
                lambda x: x[1] if isinstance(x, tuple) and x[1] else None
            )
    logger.debug(
        f"Parsed datasets table: {df.shape[0]} missions x {df.shape[1]} columns"
    )
    # Write/update cache and log times
    try:
        df.to_csv(DATASETS_CACHE)
        logger.debug(f"Wrote datasets cache to {DATASETS_CACHE}")
    finally:
        # Record both last update and last check
        log.log_update_time()
        log.log_check_time()
        logger.debug("Updated datasets access log timestamps")
    return df


datasets = get_datasets()
datasets = datasets.merge(
    shorthands.to_frame().reset_index(), on="Mission Name"
).set_index("shorthand")


## Validation helpers
def _is_start_valid(mission: str, start: Time) -> bool:
    """
    Check if the start time is valid for a given mission.

    Parameters
    ----------
    mission : str
        Mission shorthand label of datasets dataframe, e.g. 'cassini'.
    start : astropy.Time
        Start time in astropy.Time format.
    """
    return Time(datasets.at[mission, "Start Time"]) <= start


def _is_stop_valid(mission: str, stop: Time) -> bool:
    """
    Check if the stop time is valid for a given mission.

    Parameters
    ----------
    mission : str
        Mission shorthand label of datasets dataframe.
    stop : astropy.Time
        Stop time in astropy.Time format.
    """
    return Time(datasets.at[mission, "Stop Time"]) >= stop


def download_one_url(url, local_path, overwrite: bool = False):
    """Download a single URL to a local path.

    Parameters
    ----------
    url : str
        URL to download
    local_path : Path
        Path where to save the file
    overwrite : bool, optional
        Whether to overwrite existing files, by default False
    """
    if local_path.exists() and not overwrite:
        logger.debug(f"Skipping download, exists: {local_path}")
        return
    local_path.parent.mkdir(exist_ok=True, parents=True)
    logger.info(f"Downloading kernel: {url} -> {local_path}")
    url_retrieve(url, local_path)


class Subsetter:
    """
    Class to manage retrieving subset SPICE kernel lists.


    Attributes
    ----------
    kernel_names : list[str]
        Names of kernels for the given time range.

    Notes
    -----
    The following methods are available on this class:

    - ``download_kernels()`` – Download SPICE kernels.
    - ``get_metakernel()`` – Get metakernel file from NAIF and adjust paths to match local storage.

    """

    def __init__(self, mission: str, start: str, stop=None, save_location=None):
        """
        Initialize the Subsetter object.

        This means that the object at initialization (via internal method below) receives all required
        metadata to query infos, but doesn't do downloading automatically.

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
        # Resolve mission input to canonical label and optional shorthand code
        self.mission_name, self.mission_code = _resolve_mission(mission)
        self.start = start
        self.stop = stop
        self.save_location = save_location
        self._initialize()

    def _initialize(self):
        "get metadata via self.r and unpack it."
        logger.debug(
            f"Requesting subset package for mission={self.mission_name} (code={self.mission_code}) start={self.start} stop={self.stop}"
        )
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
        logger.debug(f"Discovered {len(self.kernel_urls)} kernel URLs from subset")

    @property
    def r(self):
        """This is the main remote request, fired up at each access.

        It uses the property `payload` to create the required request's parameters.
        """
        return requests.get(BASE_URL, params=self.payload, stream=True)

    @property
    def start(self):
        return self._start

    @start.setter
    def start(self, value):
        try:
            self._start = Time(value)
        except ValueError:
            self._start = Time(fromdoyformat(value).isoformat())

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
                self._stop = Time(fromdoyformat(value).isoformat())

    @property
    def payload(self):
        """Put payload together while checking for working time format.

        If Time(self.start) doesn't work, then we assume that the date must be in the
        Time-unsupported yyyy-jjj format, which can be converted by `fromdoyformat`
        from `planetarypy.utils`.
        """
        if not (
            _is_start_valid(self.mission_code, self.start)
            and _is_stop_valid(self.mission_code, self.stop)
        ):
            raise ValueError(
                "One of start/stop is outside the supported date-range. See `datasets`."
            )
        p = {
            "dataset": last_part(
                URL(datasets.at[self.mission_code, "Archive Link"]), 2
            ),
            "start": self.start.iso,
            "stop": self.stop.iso,
            "action": "Subset",
        }
        return p

    @property
    def kernel_names(self):
        "Return list of names of kernels for the given time range."
        return [
            str(Path(URL(url).parent.name) / URL(url).name) for url in self.kernel_urls
        ]

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
        # prefer shorthand code for local storage directory; fall back to sanitized label
        if not self.save_location:
            if self.mission_code:
                mission_dir = self.mission_code
            else:
                mission_dir = self.mission_name.replace(" ", "_").lower()
            basepath = KERNEL_STORAGE / mission_dir
        else:
            basepath = Path(self.save_location)
        return basepath / u.parent.name / u.name

    def _non_blocking_download(self, overwrite: bool = False):
        "Use multiprocessing for parallel download."
        paths = [self.get_local_path(url) for url in self.kernel_urls]
        # Create individual argument lists for each parameter
        urls = self.kernel_urls
        overwrites = [overwrite] * len(urls)
        logger.info("Starting parallel kernel downloads")
        _ = process_map(
            download_one_url,
            urls,  # First argument (url)
            paths,  # Second argument (local_path)
            overwrites,  # Third argument (overwrite)
            max_workers=cpu_count() - 2,
            desc="Kernels downloaded",
        )

    def _concurrent_download(self, overwrite: bool = False):
        paths = [self.get_local_path(url) for url in self.kernel_urls]
        args = zip(self.kernel_urls, paths, repeat(overwrite))
        _ = process_map(download_one_url, args, max_workers=cpu_count() - 2)

    def download_kernels(
        self,
        overwrite: bool = False,
        non_blocking: bool = False,
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
        logger.info("Starting sequential kernel downloads")
        for url in tqdm(self.kernel_urls, desc="Kernels downloaded"):
            local_path = self.get_local_path(url)
            if local_path.exists() and not overwrite:
                if not quiet:
                    print(local_path.parent.name, local_path.name, "locally available.")
                logger.debug(f"Locally available: {local_path}")
                continue
            local_path.parent.mkdir(exist_ok=True, parents=True)
            logger.info(f"Downloading kernel: {url} -> {local_path}")
            url_retrieve(url, local_path)

    def get_metakernel(self) -> Path:
        """
        Download metakernel file from NAIF, adapt paths to match local storage
        and return local path to metakernel file.

        Uses self.save_location if given, otherwise `planetarypy` archive.
        """
        basepath = (
            KERNEL_STORAGE / self.mission_code
            if not self.save_location
            else self.save_location
        )
        savepath = basepath / self.metakernel_file
        with (
            open(savepath, "w") as outfile,
            self.z.open(self.metakernel_file) as infile,
        ):
            for line in infile:
                linestr = line.decode()
                if "'./data'" in linestr:
                    linestr = linestr.replace("'./data'", f"'{savepath.parent}'")
                outfile.write(linestr)
        return savepath


def get_metakernel_and_files(
    mission: str, start: str, stop: str, save_location: str = None, quiet: bool = False
) -> str:
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
    return str(subset.get_metakernel())


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
