"""MRO HiRISE instrument support.

Provides browse image download and solar geometry lookup from PDS indexes.

Examples
--------
>>> from planetarypy.instruments.mro.hirise import get_browse, get_metadata
>>> path = get_browse("ESP_013807_2035_RED")
>>> meta = get_metadata("ESP_013807_2035_RED")
>>> meta["SUB_SOLAR_AZIMUTH"]
129.324
"""

from pathlib import Path

from loguru import logger

from planetarypy.config import config
from planetarypy.utils import url_retrieve

HIRISE_BASE = "https://hirise-pds.lpl.arizona.edu/PDS"


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
