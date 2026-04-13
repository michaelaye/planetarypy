"""URL rewrite rules for broken PDS archive URLs.

The USGS Imaging Node (pdsimage2.wr.usgs.gov) is completely broken (all 404).
This module rewrites those URLs to working mirrors:

1. SETI Rings Node (pds-rings.seti.org) — Cassini ISS, Galileo SSI, Juno JunoCam
2. JPL Planetary Data (planetarydata.jpl.nasa.gov) — most other missions
"""

import re
from loguru import logger

USGS_PREFIX = "http://pdsimage2.wr.usgs.gov/Missions/"

# Map from USGS path prefix to JPL path.
# USGS pattern: /Missions/{Mission}/{Instrument}/{volume}/{rest}
# JPL pattern:  /img/data/{jpl_mission}/{jpl_subdir}/{volume}/{rest}
_JPL_MISSION_MAP: dict[str, str] = {
    "Cassini/RADAR/": "cassini/cassini_orbiter/",
    "Phoenix/": "phoenix/",
    "Mars_Pathfinder/mpim_": "mpf/imp/mpim_",
    "Mars_Pathfinder/mprv_": "mpf/rover/mprv_",
    "Mars_Reconnaissance_Orbiter/CTX/": "mro/ctx/",
    "Mars_Reconnaissance_Orbiter/MARCI/": "mro/marci/",
    "Mars_Global_Surveyor/MOC/mgsc_": "mgs/moc/mgsc_",
    "Mars_Global_Surveyor/MOC/mgs_": "mgs/moc/mgs_",
    "Lunar_Orbiter/": "lo/",
    "Magellan/": "magellan/",
}

# SETI Rings volume patterns: volume_id -> group directory
# Pattern: uppercase volume, replace last 3 digits with 'xxx'
_SETI_VOLUME_RE = re.compile(
    r"(Cassini/ISS/(coiss_\d+)|Galileo/SSI/(go_0\d+)|Juno/JunoCam/(JNOJNC_\d+))"
)


def _seti_group(volume_id: str) -> str:
    """Derive SETI volume group from volume ID.

    E.g. 'coiss_2022' -> 'COISS_2xxx', 'go_0001' -> 'GO_0xxx'.
    """
    upper = volume_id.upper()
    return upper[:-3] + "xxx"


def rewrite_usgs_url(url_stem: str) -> str | None:
    """Rewrite a broken USGS Imaging Node URL to a working mirror.

    Parameters
    ----------
    url_stem : str
        Original URL stem starting with pdsimage2.wr.usgs.gov

    Returns
    -------
    str or None
        Rewritten URL, or None if no rewrite rule matches
    """
    if not url_stem.startswith(USGS_PREFIX):
        return None

    path = url_stem[len(USGS_PREFIX):]  # e.g. "Cassini/ISS/coiss_2022/data/..."

    # Try SETI Rings first (higher quality mirror for supported missions)
    m = _SETI_VOLUME_RE.match(path)
    if m:
        # Extract volume_id from whichever group matched
        volume_id = m.group(2) or m.group(3) or m.group(4)
        group = _seti_group(volume_id)
        # Path after the mission/instrument prefix is: {volume_id}/{rest}
        # Find where the volume starts in the original path
        vol_start = path.index(volume_id)
        rest = path[vol_start:]  # e.g. "coiss_2022/data/..."
        # SETI uses uppercase volume names
        rest_upper = volume_id.upper() + rest[len(volume_id):]
        return f"https://pds-rings.seti.org/viewmaster/volumes/{group}/{rest_upper}"

    # Try JPL Planetary Data
    for usgs_prefix, jpl_prefix in _JPL_MISSION_MAP.items():
        if path.startswith(usgs_prefix):
            rest = path[len(usgs_prefix):]
            return f"https://planetarydata.jpl.nasa.gov/img/data/{jpl_prefix}{rest}"

    return None


def rewrite_catalog_urls(storage_root) -> dict[str, int]:
    """Rewrite all broken USGS URLs in the catalog database.

    Returns
    -------
    dict[str, int]
        Counts: 'rewritten' (successfully mapped), 'no_rule' (no rewrite available)
    """
    from planetarypy.catalog._schema import get_connection

    con = get_connection(storage_root)
    rows = con.execute(
        "SELECT rowid, url_stem FROM products WHERE url_stem LIKE ?",
        [f"{USGS_PREFIX}%"],
    ).fetchall()

    if not rows:
        logger.info("No USGS URLs found in catalog.")
        con.close()
        return {"rewritten": 0, "no_rule": 0}

    counts = {"rewritten": 0, "no_rule": 0}
    no_rule_urls: list[str] = []

    for rowid, url_stem in rows:
        new_url = rewrite_usgs_url(url_stem)
        if new_url:
            con.execute(
                "UPDATE products SET url_stem = ?, url_status = 'rewritten' WHERE rowid = ?",
                [new_url, rowid],
            )
            counts["rewritten"] += 1
        else:
            counts["no_rule"] += 1
            no_rule_urls.append(url_stem)

    con.close()

    logger.info(
        f"URL rewrite: {counts['rewritten']} rewritten, {counts['no_rule']} without rules"
    )
    if no_rule_urls:
        unique = sorted(set(no_rule_urls))
        logger.warning(f"No rewrite rule for {len(unique)} unique URL patterns:")
        for url in unique[:10]:
            logger.warning(f"  {url}")

    return counts
