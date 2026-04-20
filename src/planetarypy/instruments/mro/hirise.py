"""MRO HiRISE instrument support.

Provides browse image download, solar geometry lookup from PDS indexes,
and EDR source product management for ISIS processing pipelines.

Examples
--------
>>> from planetarypy.instruments.mro.hirise import get_browse, get_metadata
>>> path = get_browse("ESP_013807_2035_RED")
>>> meta = get_metadata("ESP_013807_2035_RED")
>>> meta["SUB_SOLAR_AZIMUTH"]
129.324

>>> from planetarypy.instruments.mro.hirise import RED_PRODUCT
>>> prod = RED_PRODUCT("ESP_013807_2035", ccdno=4, channel=0)
>>> prod.download()
"""

import warnings
from pathlib import Path

import tomlkit
from loguru import logger
from yarl import URL

from planetarypy.config import config
from planetarypy.utils import check_url_exists, url_retrieve

HIRISE_BASE = "https://hirise-pds.lpl.arizona.edu/PDS"
_HIRISE_URL = URL(HIRISE_BASE)

# ── HiRISE instrument config ─────────────────────────────────────

_HIRISE_CONFIG_PATH = Path.home() / ".planetarypy_mro_hirise.toml"


def _hirise_config() -> dict:
    """Load the HiRISE config file, returning empty sections if missing."""
    if _HIRISE_CONFIG_PATH.exists():
        return tomlkit.loads(_HIRISE_CONFIG_PATH.read_text())
    return {}


def _edr_config() -> dict:
    """Return the [edr] section of the HiRISE config."""
    return _hirise_config().get("edr", {})


def _rdr_config() -> dict:
    """Return the [rdr] section of the HiRISE config."""
    return _hirise_config().get("rdr", {})


def _orbit_range(orbit: int) -> str:
    """Derive orbit range directory: 3092 → ORB_003000_003099."""
    base = (orbit // 100) * 100
    return f"ORB_{base:06d}_{base + 99:06d}"


def _parse_pid(product_id: str) -> tuple[str, list[str], str]:
    """Parse and normalize a HiRISE product ID.

    Returns (normalized_pid, parts, data_level).
    """
    pid = product_id.upper().strip()
    parts = pid.split("_")

    if len(parts) == 4 and parts[3] in ("RED", "COLOR"):
        return pid, parts, "RDR"
    if len(parts) >= 5:
        return pid, parts, "EDR"
    if len(parts) == 3:
        # Bare observation ID — default to RDR RED
        pid = f"{pid}_RED"
        parts = pid.split("_")
        return pid, parts, "RDR"

    raise ValueError(f"Cannot parse HiRISE product ID: {product_id}")


def get_browse(product_id: str, annotated: bool = True,
               dest: Path | None = None, force: bool = False) -> Path:
    """Download a HiRISE browse JPEG and return its local path.

    Fetches from the EXTRAS directory at the University of Arizona
    HiRISE server. Cached locally after first download.

    Parameters
    ----------
    product_id : str
        HiRISE product ID, e.g. "ESP_013807_2035_RED",
        "PSP_003092_0985_COLOR", or bare observation "ESP_013807_2035"
        (defaults to RED).
    annotated : bool
        If True (default), fetch the annotated browse (``.abrowse.jpg``)
        which includes the observation ID and scale bar.
        If False, fetch the clean browse (``.browse.jpg``).
    dest : Path, optional
        Directory to save into. Defaults to planetarypy storage.
    force : bool
        Re-download even if cached.

    Returns
    -------
    Path
        Local path to the browse JPEG.
    """
    pid, parts, data_level = _parse_pid(product_id)
    prefix = parts[0]
    orbit = int(parts[1])
    obs_id = f"{parts[0]}_{parts[1]}_{parts[2]}"
    orbit_dir = _orbit_range(orbit)

    if data_level == "EDR":
        filename = f"{pid}.browse.jpg"
    elif annotated:
        filename = f"{pid}.abrowse.jpg"
    else:
        filename = f"{pid}.browse.jpg"

    url = f"{HIRISE_BASE}/EXTRAS/{data_level}/{prefix}/{orbit_dir}/{obs_id}/{filename}"

    if dest is not None:
        outpath = Path(dest) / filename
    else:
        outpath = (
            Path(config["storage_root"])
            / "mro" / "hirise" / "extras" / obs_id / filename
        )

    if outpath.exists() and not force:
        logger.debug(f"Already cached: {outpath}")
        return outpath

    outpath.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Downloading {filename}...")
    url_retrieve(url, str(outpath))
    return outpath


def get_metadata(product_id: str, index: str = "rdr") -> dict:
    """Look up HiRISE metadata from the PDS index.

    Parameters
    ----------
    product_id : str
        HiRISE product ID (e.g. "ESP_013807_2035_RED").
    index : str
        Which index to query: "rdr" or "edr".

    Returns
    -------
    dict
        Index row as a dictionary.
    """
    from planetarypy.pds import get_index

    pid, parts, data_level = _parse_pid(product_id)
    idx_key = f"mro.hirise.{index}"
    df = get_index(idx_key, allow_refresh=False)

    row = df[df["PRODUCT_ID"] == pid]
    if row.empty:
        raise ValueError(f"Product {pid} not found in {idx_key} index")

    return row.iloc[0].to_dict()


def sun_azimuth_from_top(product_id: str, index: str = "rdr") -> float:
    """Get solar azimuth converted to CW-from-top convention.

    HiRISE indexes store SUB_SOLAR_AZIMUTH as CW from 3 o'clock.
    This function converts to CW from image top, suitable for
    `planetarypy.plotting.add_sun_indicator`.

    Parameters
    ----------
    product_id : str
        HiRISE product ID.
    index : str
        Which index: "rdr" or "edr".

    Returns
    -------
    float
        Solar azimuth in degrees, CW from image top.
    """
    meta = get_metadata(product_id, index=index)
    hirise_az = meta["SUB_SOLAR_AZIMUTH"]
    return (hirise_az + 90) % 360


# ── Observation ID Cache (for tab completion) ─────────────────────

def _obsid_cache_path(index: str = "edr") -> Path:
    """Path to the cached obsid list file."""
    return Path(config["storage_root"]) / "mro" / "hirise" / f"obsids_{index}.txt"


def rebuild_obsid_cache(index: str = "edr") -> Path:
    """Rebuild the observation ID cache from a HiRISE index.

    Extracts unique OBSERVATION_IDs, sorts them, and writes to a text file.
    Called automatically when the cache is missing.

    Parameters
    ----------
    index : str
        Which index to use: ``"edr"`` or ``"rdr"``.

    Returns
    -------
    Path
        Path to the cache file.
    """
    from planetarypy.pds import get_index

    cache = _obsid_cache_path(index)
    cache.parent.mkdir(parents=True, exist_ok=True)
    df = get_index(f"mro.hirise.{index}", allow_refresh=False)
    obsids = sorted(df["OBSERVATION_ID"].unique())
    cache.write_text("\n".join(obsids) + "\n")
    logger.info(f"Rebuilt {index.upper()} obsid cache: {len(obsids)} entries at {cache}")
    return cache


def complete_obsid(incomplete: str, index: str = "edr") -> list[str]:
    """Return observation IDs matching a prefix (for tab completion).

    Reads from a cached text file for speed. Rebuilds the cache from
    the specified index if missing.

    Parameters
    ----------
    incomplete : str
        Prefix to match against.
    index : str
        Which index to use: ``"edr"`` (default, all observations) or
        ``"rdr"`` (only observations with processed RDR products).

    Returns
    -------
    list of str
        Matching observation IDs.
    """
    cache = _obsid_cache_path(index)
    if not cache.exists():
        try:
            rebuild_obsid_cache(index)
        except Exception:
            return []

    prefix = incomplete.upper()
    matches = []
    with open(cache) as f:
        for line in f:
            obsid = line.rstrip()
            if obsid.startswith(prefix):
                matches.append(obsid)
            # Since the file is sorted, we can stop after we pass the prefix
            elif matches:
                break
    return matches


# ── EDR Source Product Management ─────────────────────────────────


def _edr_storage() -> Path:
    """Resolve EDR local storage from config.

    Priority: edr.local_storage → edr.local_mirror → storage_root/mro/pds
    """
    cfg = _edr_config()
    for key in ("local_storage", "local_mirror"):
        val = cfg.get(key, "")
        if val:
            return Path(val)
    return Path(config["storage_root"]) / "mro" / "pds"


def _edr_base_url() -> URL:
    """Resolve EDR base URL from config."""
    cfg = _edr_config()
    url = cfg.get("url", "")
    if url:
        return URL(url)
    return URL(f"{HIRISE_BASE}/EDR/")


class SOURCE_PRODUCT:
    """Manage a HiRISE source product (EDR) by its product ID.

    Handles URL construction, local path management, and download
    for individual CCD channel EDR files (e.g. ``PSP_003092_0985_RED4_0``).

    Parameters
    ----------
    spid : str
        Full source product ID, e.g. ``"PSP_003092_0985_RED4_0"``.
    saveroot : Path, optional
        Override storage root. Defaults to ``~planetarypy_data/missions/mro/hirise/``.
    check_url : bool
        If True, warn when the constructed URL doesn't exist on the server.

    Examples
    --------
    >>> prod = SOURCE_PRODUCT("PSP_003092_0985_RED4_0")
    >>> prod.obsid
    'PSP_003092_0985'
    >>> prod.local_path
    PosixPath('.../PSP_003092_0985/PSP_003092_0985_RED4_0.IMG')
    """

    red_ccds = ["RED" + str(i) for i in range(10)]
    ir_ccds = ["IR10", "IR11"]
    bg_ccds = ["BG12", "BG13"]
    ccds = red_ccds + ir_ccds + bg_ccds

    def __init__(self, spid, saveroot=None, check_url=False):
        tokens = spid.split("_")
        self._obsid = "_".join(tokens[:3])
        self._ccd = tokens[3]
        self._channel = str(tokens[4])
        self._color = self._parse_color(self._ccd)
        self.check_url = check_url
        self.saveroot = _edr_storage() if saveroot is None else Path(saveroot)

    @staticmethod
    def _parse_color(ccd):
        """Extract color prefix from CCD name: 'RED4' → 'RED', 'IR10' → 'IR'."""
        if ccd[:2] in ("IR", "BG"):
            return ccd[:2]
        return ccd[:3]

    @property
    def obsid(self):
        return self._obsid

    @property
    def ccd(self):
        return self._ccd

    @property
    def channel(self):
        return self._channel

    @property
    def color(self):
        return self._color

    @property
    def ccdno(self):
        return self._ccd[len(self._color):]

    @property
    def phase(self):
        orbit = int(self._obsid.split("_")[1])
        return "PSP" if orbit < 11000 else "ESP"

    @property
    def spid(self):
        return f"{self._obsid}_{self._ccd}_{self._channel}"

    @property
    def fname(self):
        return self.spid + ".IMG"

    @property
    def _orbit_dir(self):
        orbit = int(self._obsid.split("_")[1])
        base = (orbit // 100) * 100
        return f"ORB_{base:06d}_{base + 99:06d}"

    @property
    def remote_path(self):
        phase = self.phase
        return Path("EDR") / phase / self._orbit_dir / self._obsid / self.fname

    @property
    def url(self):
        u = _HIRISE_URL / str(self.remote_path)
        if self.check_url:
            if not check_url_exists(str(u)):
                warnings.warn(f"{u} does not exist on the server.")
        return u

    @property
    def local_path(self):
        return self.saveroot / self._obsid / self.fname

    @property
    def local_cube(self):
        return self.local_path.with_suffix(".cub")

    @property
    def stitched_cube_name(self):
        return f"{self._obsid}_{self._ccd}.cub"

    @property
    def stitched_cube_path(self):
        return self.local_cube.with_name(self.stitched_cube_name)

    def download(self, overwrite=False, **tqdm_kwargs):
        """Download the EDR .IMG file from the HiRISE PDS archive.

        Parameters
        ----------
        overwrite : bool
            Re-download even if cached locally.
        **tqdm_kwargs
            Passed to ``url_retrieve`` (e.g. ``tqdm_position``, ``leave_tqdm``).
        """
        self.local_path.parent.mkdir(parents=True, exist_ok=True)
        if self.local_path.exists() and not overwrite:
            logger.debug(f"Already cached: {self.local_path}")
            return
        url_retrieve(str(self.url), str(self.local_path), **tqdm_kwargs)

    def __str__(self):
        return f"SOURCE_PRODUCT({self.spid})"

    def __repr__(self):
        return self.__str__()


class RED_PRODUCT(SOURCE_PRODUCT):
    """Convenience constructor for RED CCD source products.

    Parameters
    ----------
    obsid : str
        Observation ID, e.g. ``"ESP_013807_2035"``.
    ccdno : int
        CCD number (0-9 for RED).
    channel : int
        Channel number (0 or 1).

    Examples
    --------
    >>> prod = RED_PRODUCT("ESP_013807_2035", ccdno=4, channel=0)
    >>> prod.spid
    'ESP_013807_2035_RED4_0'
    """

    def __init__(self, obsid, ccdno, channel, **kwargs):
        self.ccds = self.red_ccds
        super().__init__(f"{obsid}_RED{ccdno}_{channel}", **kwargs)


# ── EDR Bulk Download ─────────────────────────────────────────────


def edr_products(
    obsid: str,
    colors: list[str] = None,
    ccds: list[int] = None,
    saveroot: Path | None = None,
) -> list[SOURCE_PRODUCT]:
    """Build a list of EDR SOURCE_PRODUCTs for an observation.

    Parameters
    ----------
    obsid : str
        Observation ID, e.g. ``"PSP_003092_0985"``.
    colors : list of str, optional
        Color groups to include: ``"red"``, ``"ir"``, ``"bg"``.
        Defaults to ``["red"]``.
    ccds : list of int, optional
        Limit to specific CCD numbers within the selected colors.
        Only applies to RED (e.g. ``[4, 5]`` for RED4+RED5).
    saveroot : Path, optional
        Override storage root.

    Returns
    -------
    list of SOURCE_PRODUCT
        Two products per CCD (channels 0 and 1).
    """
    if colors is None:
        colors = ["red"]
    colors = [c.lower() for c in colors]

    ccd_list = []
    if "red" in colors:
        if ccds is not None:
            ccd_list.extend([f"RED{n}" for n in ccds])
        else:
            ccd_list.extend(SOURCE_PRODUCT.red_ccds)
    if "ir" in colors:
        ccd_list.extend(SOURCE_PRODUCT.ir_ccds)
    if "bg" in colors:
        ccd_list.extend(SOURCE_PRODUCT.bg_ccds)

    kwargs = {"saveroot": saveroot} if saveroot else {}
    products = []
    for ccd in ccd_list:
        for channel in (0, 1):
            products.append(SOURCE_PRODUCT(f"{obsid}_{ccd}_{channel}", **kwargs))
    return products


def _download_with_rich_task(prod, task_id, progress, overwrite, session=None):
    """Download a single product, updating a rich progress task."""
    if session is None:
        import requests
        session = requests

    if prod.local_path.exists() and not overwrite:
        size = prod.local_path.stat().st_size
        progress.update(task_id, completed=size, total=size,
                        description=f"[dim]{prod.fname} (cached)")
        return prod.fname, None

    try:
        url = str(prod.url)
        R = session.get(url, stream=True, allow_redirects=True)
        if R.status_code != 200:
            raise ConnectionError(f"HTTP {R.status_code}")
        total = int(R.headers.get("content-length", 0))
        progress.update(task_id, total=total)

        part_file = prod.local_path.with_suffix(prod.local_path.suffix + ".part")
        with open(part_file, "wb") as f:
            for chunk in R.iter_content(chunk_size=32768):
                f.write(chunk)
                progress.advance(task_id, len(chunk))
        part_file.rename(prod.local_path)
        return prod.fname, None
    except Exception as e:
        return prod.fname, str(e)


def download_edr(
    obsid: str,
    colors: list[str] = None,
    ccds: list[int] = None,
    saveroot: Path | None = None,
    overwrite: bool = False,
    max_workers: int = 4,
) -> list[SOURCE_PRODUCT]:
    """Download HiRISE EDR channel files for an observation.

    Downloads in parallel using a thread pool (default 4 workers)
    with rich progress bars.

    Parameters
    ----------
    obsid : str
        Observation ID, e.g. ``"PSP_003092_0985"``.
    colors : list of str, optional
        Color groups: ``"red"``, ``"ir"``, ``"bg"``. Defaults to ``["red"]``.
    ccds : list of int, optional
        Limit to specific RED CCD numbers (e.g. ``[4, 5]``).
    saveroot : Path, optional
        Override storage root.
    overwrite : bool
        Re-download even if cached.
    max_workers : int
        Number of parallel downloads (default 4).

    Returns
    -------
    list of SOURCE_PRODUCT
        The downloaded products (with valid ``.local_path``).

    Raises
    ------
    RuntimeError
        If any downloads failed.
    """
    import requests as req_mod
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from rich.progress import Progress, BarColumn, DownloadColumn, TransferSpeedColumn

    products = edr_products(obsid, colors=colors, ccds=ccds, saveroot=saveroot)

    # Ensure parent dirs exist before parallel downloads
    for prod in products:
        prod.local_path.parent.mkdir(parents=True, exist_ok=True)

    failed = []
    # Shared session for connection pooling (reuses TCP/TLS across threads)
    session = req_mod.Session()
    adapter = req_mod.adapters.HTTPAdapter(
        pool_connections=max_workers, pool_maxsize=max_workers,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    with Progress(
        "[progress.description]{task.description}",
        BarColumn(),
        DownloadColumn(),
        TransferSpeedColumn(),
    ) as progress:
        tasks = {
            prod: progress.add_task(prod.fname, total=None)
            for prod in products
        }

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(
                    _download_with_rich_task, prod, tasks[prod], progress,
                    overwrite, session,
                ): prod
                for prod in products
            }
            for future in as_completed(futures):
                fname, error = future.result()
                if error:
                    logger.error(f"Failed: {fname}: {error}")
                    failed.append(fname)

    session.close()

    if failed:
        raise RuntimeError(
            f"{len(failed)}/{len(products)} downloads failed: {', '.join(failed)}"
        )
    return products


# ── ISIS Processing Chain ─────────────────────────────────────────
#
# Full HiRISE EDR-to-mosaic pipeline, based on the pymars pipeline:
#   hi2isis → spiceinit → hical → histitch(balance) → cubenorm
#       → cam2map → equalizer → automos
#
# Each step deletes its input to conserve disk space (HiRISE data
# is large). The extension chain tracks provenance:
#   .IMG → .cub → .cal.cub → .cal.norm.cub → .cal.norm.map.cub
#       → .cal.norm.map.equ.mos.cub

import subprocess

try:
    from kalasiris import (
        automos, cam2map, catlab, cubenorm, equalizer,
        getkey, handmos, hical, hi2isis, histitch, spiceinit,
    )
    _ISIS_AVAILABLE = True
except (ImportError, KeyError):
    _ISIS_AVAILABLE = False


def _require_isis():
    """Raise if ISIS/kalasiris is not available."""
    if not _ISIS_AVAILABLE:
        raise RuntimeError(
            "ISIS not available (kalasiris import failed). "
            "Is ISISROOT set?"
        )


def _ext(path: Path, new_suffix: str) -> Path:
    """Append a suffix to a path's stem: foo.cal.cub + '.norm' → foo.cal.norm.cub"""
    return path.with_suffix("").with_suffix(new_suffix + ".cub")


def ingest_edr(product: SOURCE_PRODUCT) -> Path:
    """Convert a HiRISE EDR .IMG to an ISIS cube and initialize SPICE.

    Runs ``hi2isis`` then ``spiceinit`` (web-based kernel fetch).

    Parameters
    ----------
    product : SOURCE_PRODUCT
        The EDR product to ingest. Must already be downloaded.

    Returns
    -------
    Path
        Path to the resulting .cub file.
    """
    _require_isis()
    img = product.local_path
    cub = product.local_cube
    if not img.exists():
        raise FileNotFoundError(f"EDR file not found: {img}")
    hi2isis(from_=str(img), to=str(cub))
    # spiceinit may segfault on exit (known ISIS quirk) but still writes
    # the kernel group to the cube label successfully. Tolerate exit != 0
    # and verify the label afterwards.
    try:
        spiceinit(
            from_=str(cub),
            web="true",
            url="https://astrogeology.usgs.gov/apis/ale/v0.9.1/spiceserver/",
        )
    except subprocess.CalledProcessError as e:
        # Check whether spiceinit succeeded despite the non-zero exit
        try:
            label = catlab(from_=str(cub)).stdout
        except Exception:
            raise e
        if "Group = Kernels" not in label:
            raise
        logger.debug(
            f"spiceinit exited non-zero on {cub.name} but label looks valid"
        )
    return cub


def calibrate_channel(cub: Path, cleanup: bool = True) -> Path:
    """Radiometrically calibrate a single HiRISE channel cube.

    Runs ``hical`` to apply radiometric calibration.

    Parameters
    ----------
    cub : Path
        Path to a .cub file (output of :func:`ingest_edr`).
    cleanup : bool
        Delete the uncalibrated input cube.

    Returns
    -------
    Path
        Path to the calibrated cube (``*.cal.cub``).
    """
    _require_isis()
    cal = _ext(cub, ".cal")
    hical(from_=str(cub), to=str(cal))
    if cleanup:
        cub.unlink(missing_ok=True)
    return cal


def stitch_channels(ch0_cal: Path, ch1_cal: Path,
                    obsid: str, ccd: str,
                    cleanup: bool = True) -> Path:
    """Stitch two calibrated CCD channels and cubenorm the result.

    Runs ``histitch`` with ``balance=true`` on channels 0 and 1,
    then ``cubenorm`` to normalize column-to-column variations.

    Parameters
    ----------
    ch0_cal, ch1_cal : Path
        Calibrated .cal.cub files for channels 0 and 1 of the same CCD.
    obsid : str
        Observation ID (for naming the output).
    ccd : str
        CCD identifier, e.g. ``"RED4"``.
    cleanup : bool
        Delete input and intermediate files.

    Returns
    -------
    Path
        Path to the normalized stitched cube (``*.cal.norm.cub``).
    """
    _require_isis()
    stitched = ch0_cal.parent / f"{obsid}_{ccd}.cal.cub"
    normed = _ext(stitched, ".cal.norm")
    histitch(
        from1=str(ch0_cal), from2=str(ch1_cal),
        to=str(stitched), balance="true",
    )
    cubenorm(from_=str(stitched), to=str(normed))
    if cleanup:
        ch0_cal.unlink(missing_ok=True)
        ch1_cal.unlink(missing_ok=True)
        stitched.unlink(missing_ok=True)
    return normed


def map_project(normed: Path, mapfile: str | Path | None = None,
                cleanup: bool = True) -> Path:
    """Map-project a calibrated, normalized CCD cube.

    Runs ``cam2map``. If no map file is provided, ISIS uses its
    default Sinusoidal projection.

    Parameters
    ----------
    normed : Path
        Path to a .cal.norm.cub file.
    mapfile : str or Path, optional
        ISIS map projection file (.map). If None, uses ISIS default.
    cleanup : bool
        Delete input cube.

    Returns
    -------
    Path
        Path to the map-projected cube (``*.cal.norm.map.cub``).
    """
    _require_isis()
    mapped = _ext(normed, ".cal.norm.map")
    kwargs = {"from_": str(normed), "to": str(mapped), "pixres": "MAP"}
    if mapfile is not None:
        kwargs["map"] = str(mapfile)
    cam2map(**kwargs)
    if cleanup:
        normed.unlink(missing_ok=True)
    return mapped


# CCD configuration per color
_COLOR_CCDS = {
    "red": (SOURCE_PRODUCT.red_ccds, "RED"),
    "ir":  (SOURCE_PRODUCT.ir_ccds, "IR"),
    "bg":  (SOURCE_PRODUCT.bg_ccds, "BG"),
}


def _smart_max_workers(n_tasks: int) -> int:
    """Calculate max parallel workers based on available memory.

    Each ISIS CCD processing step uses ~500 MB-1 GB of memory.
    Uses 80% of available RAM.
    """
    import os

    per_task_bytes = 1.0 * 1024**3  # ~1 GB per ISIS CCD task (conservative)
    try:
        import psutil
        available = psutil.virtual_memory().available
    except ImportError:
        available = 8 * 1024**3  # assume 8 GB if psutil unavailable

    budget = available * 0.8
    workers = max(1, int(budget / per_task_bytes))
    workers = min(workers, n_tasks, os.cpu_count() or 4)
    return workers


def _stitch_worker(args):
    """Picklable worker for parallel histitch + cubenorm."""
    ch0_cal, ch1_cal, obsid, ccd = args
    return stitch_channels(ch0_cal, ch1_cal, obsid=obsid, ccd=ccd)


def _project_worker(args):
    """Picklable worker for parallel cam2map."""
    normed, mapfile = args
    return map_project(normed, mapfile=mapfile)


def create_mosaic(
    obsid: str,
    color: str = "red",
    ccds: list[int] | None = None,
    mapfile: str | Path | None = None,
    overwrite: bool = False,
    saveroot: Path | None = None,
    download: bool = True,
    print_progress: bool = True,
    max_workers: int | None = None,
) -> Path:
    """Create a HiRISE CCD mosaic from EDR data.

    Full processing chain (pymars/HiRISE standard pipeline):

    Per channel:
        ``download → hi2isis → spiceinit → hical``

    Per CCD:
        ``histitch(balance) → cubenorm → cam2map``

    Mosaic:
        ``equalizer → automos(priority=beneath)``

    Steps 2-5 are parallelized using process-based parallelism.
    Intermediate files are deleted after each step to conserve disk.

    Parameters
    ----------
    obsid : str
        HiRISE observation ID, e.g. ``"ESP_013807_2035"``.
    color : str
        CCD color group: ``"red"`` (default), ``"ir"``, or ``"bg"``.
    ccds : list of int, optional
        Specific CCD numbers to include. If None, uses all CCDs for
        the color (RED: 0-9, IR: 10-11, BG: 12-13).
        For RED, a common choice is ``[4, 5]`` for the central nadir pair.
    mapfile : str or Path, optional
        ISIS map projection file for ``cam2map``. If None, uses ISIS
        default projection (Sinusoidal).
    overwrite : bool
        If True, re-download and reprocess even if the mosaic exists.
    saveroot : Path, optional
        Override local storage directory.
    download : bool
        If True (default), download EDR files if missing.
        Set to False if files are already available locally.
    print_progress : bool
        If True (default), print step-by-step progress to stdout.
    max_workers : int, optional
        Maximum number of parallel workers for steps 2-5.
        If None, auto-calculates based on available memory (80% of free RAM).

    Returns
    -------
    Path
        Path to the final mosaic cube.

    Examples
    --------
    >>> create_mosaic("PSP_003092_0985")                     # RED 0-9, full pipeline
    >>> create_mosaic("PSP_003092_0985", ccds=[4, 5])        # RED 4+5 central pair
    >>> create_mosaic("PSP_003092_0985", color="ir")         # IR mosaic
    >>> create_mosaic("PSP_003092_0985", color="bg")         # BG mosaic
    >>> create_mosaic("PSP_003092_0985", max_workers=4)      # limit parallelism
    """
    _require_isis()
    color = color.lower()
    if color not in _COLOR_CCDS:
        raise ValueError(f"color must be 'red', 'ir', or 'bg', got '{color}'")

    all_ccds, prefix = _COLOR_CCDS[color]

    def _log(msg):
        if print_progress:
            print(msg, flush=True)

    # Build the CCD list
    if ccds is not None:
        ccd_names = [f"{prefix}{n}" for n in sorted(ccds)]
    else:
        ccd_names = list(all_ccds)

    # Build products: 2 channels per CCD
    prod_kwargs = {"saveroot": saveroot} if saveroot else {}
    products = []
    for ccd in ccd_names:
        for channel in (0, 1):
            products.append(SOURCE_PRODUCT(f"{obsid}_{ccd}_{channel}", **prod_kwargs))

    # Output naming
    if ccds is not None:
        ccd_label = prefix + "".join(str(c) for c in sorted(ccds))
    else:
        ccd_label = prefix
    out_dir = products[0].local_path.parent
    mosaic_path = out_dir / f"{obsid}_{ccd_label}.mos.cub"

    if mosaic_path.exists() and not overwrite:
        _log(f"Mosaic exists: {mosaic_path}")
        return mosaic_path

    n_channels = len(products)
    n_ccds = len(ccd_names)
    channel_names = " ".join(p.spid.split("_", 3)[-1] for p in products)
    ccd_name_str = " ".join(ccd_names)

    # ── Step 1: Download ──
    if download:
        _log(f"[1/6] Downloading {n_channels} channels...")
        download_edr(obsid, colors=[color], ccds=ccds, saveroot=saveroot,
                     overwrite=overwrite)

    # Determine parallelism
    from concurrent.futures import ProcessPoolExecutor

    if max_workers is None:
        n_workers = _smart_max_workers(n_channels)
    else:
        n_workers = max_workers

    # ── Step 2: hi2isis + spiceinit (process-parallel, SPICE is not thread-safe) ──
    _log(f"[2/6] hi2isis + spiceinit: {channel_names} ({n_workers} workers)")
    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        list(executor.map(ingest_edr, products))

    # ── Step 3: hical ──
    _log(f"[3/6] hical: {channel_names} ({n_workers} workers)")
    cube_paths = [p.local_cube for p in products]
    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        cal_paths = list(executor.map(calibrate_channel, cube_paths))

    # ── Step 4: histitch + cubenorm ──
    _log(f"[4/6] histitch + cubenorm: {ccd_name_str} ({n_workers} workers)")
    stitch_args = [
        (cal_paths[i * 2], cal_paths[i * 2 + 1], obsid, ccd_names[i])
        for i in range(n_ccds)
    ]
    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        normed_paths = list(executor.map(_stitch_worker, stitch_args))

    # ── Step 5: cam2map ──
    _log(f"[5/6] cam2map: {ccd_name_str} ({n_workers} workers)")
    project_args = [(normed, mapfile) for normed in normed_paths]
    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        mapped_paths = list(executor.map(_project_worker, project_args))

    # ── Step 6: equalizer + automos ──
    _log(f"[6/6] equalizer + automos → {mosaic_path.name}")
    if len(mapped_paths) == 1:
        mapped_paths[0].rename(mosaic_path)
    else:
        stem = mosaic_path.stem
        listfile = out_dir / f"{stem}.lis"
        listfile.write_text("\n".join(str(p) for p in mapped_paths) + "\n")

        holdfile = out_dir / f"{stem}_hold.lis"
        holdfile.write_text(str(mapped_paths[0]) + "\n")
        stats_out = out_dir / f"{stem}.equstats.pvl"

        equalizer(
            fromlist=str(listfile),
            outstats=str(stats_out),
            holdlist=str(holdfile),
        )
        automos(
            fromlist=str(listfile),
            mosaic=str(mosaic_path),
            priority="beneath",
        )

        for p in mapped_paths:
            p.unlink(missing_ok=True)
        listfile.unlink(missing_ok=True)
        holdfile.unlink(missing_ok=True)
        stats_out.unlink(missing_ok=True)

    _log(f"Done: {mosaic_path}")
    return mosaic_path


def create_red_mosaic(obsid: str, ccds: list[int] = (4, 5), **kwargs) -> Path:
    """Convenience wrapper for ``create_mosaic(color='red')``.

    See :func:`create_mosaic` for full parameter documentation.
    """
    return create_mosaic(obsid, color="red", ccds=ccds, **kwargs)
