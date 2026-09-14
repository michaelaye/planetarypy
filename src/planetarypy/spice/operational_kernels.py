"""Spacecraft trajectories (SPKs) from NAIF's operational kernel server.

Most active NASA missions publish no metakernel on
https://naif.jpl.nasa.gov/pub/naif/ (PSYCHE, LUCY, MRO, MAVEN, … have only
``kernels/spk/``), and the PDS archive lags months behind. For a current
position a single trajectory SPK is enough. :func:`fetch_spk` downloads the
newest one covering a time into
``{storage_root}/spice_kernels/operational/<MISSION>/spk/``, and
:func:`find_local_spk` finds it again without touching the network.

Examples
--------
>>> from planetarypy.spice.operational_kernels import fetch_spk
>>> fetch_spk("psyche", "2026-09-14")
PosixPath('.../operational/PSYCHE/spk/psyche_sc-eph_260517-261201_260811_v1.bsp')
"""

from __future__ import annotations

import re
from pathlib import Path

from ..utils import url_retrieve
from ._deps import spice
from .config import KERNEL_STORAGE, NAIF_URL

OPERATIONAL_URL = NAIF_URL / "pub/naif/"
OPERATIONAL_STORAGE = KERNEL_STORAGE / "operational"

_LISTING_ROW = re.compile(
    r'<a href="(?P<name>[^"/]+\.bsp)">[^<]*</a>\s+'
    r"(?P<modified>\d{4}-\d{2}-\d{2} \d{2}:\d{2})\s+(?P<size>\S+)"
)


_SIZE_UNITS = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3}


def _bytes(size: str) -> float:
    """Apache's human-readable size column (``928K``, ``4.6M``, ``2.1G``) in bytes."""
    unit = size[-1] if size[-1] in "KMG" else ""
    return float(size.rstrip("KMG")) * _SIZE_UNITS[unit]


def spk_folder(mission: str) -> Path:
    return OPERATIONAL_STORAGE / mission.upper() / "spk"


def list_spks(mission: str) -> list[dict]:
    """SPK files on the operational server for ``mission``, newest first.

    Each dict has ``name``, ``url``, ``modified`` (``YYYY-MM-DD HH:MM``) and
    ``size`` as the server prints it (e.g. ``4.6M``).

    Raises
    ------
    LookupError
        NAIF has no ``kernels/spk/`` folder for that mission.
    """
    import requests

    url = OPERATIONAL_URL / f"{mission.upper()}/kernels/spk/"
    response = requests.get(str(url), timeout=60)
    if response.status_code == 404:
        raise LookupError(
            f"NAIF has no operational SPK folder for {mission!r} ({url}). "
            f"The mission folders are listed at {OPERATIONAL_URL}"
        )
    response.raise_for_status()
    rows = [
        {**m.groupdict(), "url": str(url / m["name"])}
        for m in _LISTING_ROW.finditer(response.text)
    ]
    return sorted(rows, key=lambda r: r["modified"], reverse=True)


def _covers(spk_path: Path, body_id: int, et: float) -> bool:
    cover = spice.stypes.SPICEDOUBLE_CELL(2000)
    spice.spkcov(str(spk_path), body_id, cover)
    return any(
        start <= et <= end
        for start, end in (spice.wnfetd(cover, i) for i in range(spice.wncard(cover)))
    )


def _spacecraft_id(mission: str, spacecraft: str | None) -> int:
    from .mission_kernels import spacecraft_for_mission

    name = spacecraft or spacecraft_for_mission(mission)
    if name is None:
        raise ValueError(
            f"Can't tell which spacecraft belongs to {mission!r}; pass its SPICE "
            "name or NAIF ID as spacecraft="
        )
    return spice.bods2c(name)


def find_local_spk(spacecraft: str, time=None) -> Path | None:
    """An already downloaded operational SPK covering ``spacecraft`` at ``time``."""
    from .spicer import _to_et

    if not OPERATIONAL_STORAGE.is_dir():
        return None
    body_id = spice.bods2c(spacecraft)
    et = _to_et(time)
    newest_first = sorted(OPERATIONAL_STORAGE.glob("*/spk/*.bsp"),
                          key=lambda p: p.stat().st_mtime, reverse=True)
    return next((p for p in newest_first if _covers(p, body_id, et)), None)


def fetch_spk(mission: str, time=None, spacecraft: str | None = None,
              max_downloads: int = 5, max_size_mb: float = 200) -> Path:
    """Download the newest operational SPK that covers the spacecraft at ``time``.

    A file already on disk that covers the time is returned without any
    download. Otherwise the server's SPKs are tried newest first, skipping
    files over ``max_size_mb`` (MAVEN's newest is 2.1 GB); files that turn
    out not to cover the time are deleted again, and the search gives up
    after ``max_downloads`` of them.

    Parameters
    ----------
    mission : str
        NAIF mission folder name, e.g. ``"psyche"`` (case-insensitive).
    time : str or datetime, optional
        UTC time (default: now).
    spacecraft : str, optional
        SPICE name or NAIF ID; default: derived from the mission name
        (``psyche`` → ``PSYC``).

    Raises
    ------
    LookupError
        No folder for the mission, or no SPK within ``max_downloads`` covers
        the time.
    ValueError
        The spacecraft can't be derived from the mission name.
    """
    from .spicer import _to_et

    body_id = _spacecraft_id(mission, spacecraft)
    et = _to_et(time)
    folder = spk_folder(mission)
    for local in sorted(folder.glob("*.bsp"), key=lambda p: p.stat().st_mtime, reverse=True):
        if _covers(local, body_id, et):
            return local

    folder.mkdir(parents=True, exist_ok=True)
    tried, too_big = [], []
    for row in list_spks(mission):
        target = folder / row["name"]
        if target.exists():
            continue
        if _bytes(row["size"]) > max_size_mb * 1024**2:
            too_big.append(f"{row['name']} ({row['size']})")
            continue
        if len(tried) == max_downloads:
            break
        url_retrieve(row["url"], str(target))
        if _covers(target, body_id, et):
            return target
        target.unlink()
        tried.append(row["name"])
    when = spice.et2utc(et, "ISOC", 0)
    message = (
        f"None of the newest {len(tried)} operational SPKs for {mission.upper()} "
        f"cover {spice.bodc2n(body_id)} at {when}: {', '.join(tried) or 'none new'}."
    )
    if too_big:
        message += (f" Skipped as larger than {max_size_mb:g} MB: "
                    f"{', '.join(too_big[:3])}{' …' if len(too_big) > 3 else ''}.")
    raise LookupError(message)
