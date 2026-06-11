"""Resolve and download ESA PSA products via the Planetary Science Archive TAP.

ESA's PSA exposes an IVOA **EPN-TAP** table, ``psa.epn_core``, in which every
granule's ``access_url`` is a *direct download* endpoint. A PDS product id
appears inside the granule's ``granule_uid``
(``DATA_SET_ID:DATA:PRODUCT_ID::version``), so a substring match resolves a
product id straight to a download URL — no per-instrument rules, no harvest.

Implements the resolver contract ``resolve(product_id) -> access_url | None``.
Needs only ``requests`` (a core dependency); no IVOA/pyVO stack.
"""

from __future__ import annotations

import zipfile
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import requests

if TYPE_CHECKING:
    import pandas as pd

__all__ = [
    "query",
    "missions",
    "instruments",
    "product_types",
    "examples",
    "resolve",
    "resolve_all",
    "fetch_psa_product",
    "PSA_TAP_SYNC",
]

# ESA PSA EPN-TAP synchronous query endpoint.
PSA_TAP_SYNC = "https://psa.esa.int/psa-tap/tap/sync"


def query(adql: str, *, timeout: int = 60) -> list[dict]:
    """Run an ADQL query against the PSA TAP service; return rows as dicts."""
    resp = requests.get(
        PSA_TAP_SYNC,
        params={"REQUEST": "doQuery", "LANG": "ADQL", "FORMAT": "json", "QUERY": adql},
        timeout=timeout,
    )
    resp.raise_for_status()
    payload = resp.json()
    cols = [c["name"] for c in payload.get("metadata", [])]
    return [dict(zip(cols, row)) for row in payload.get("data", [])]


def missions() -> "pd.DataFrame":
    """List the PSA missions (instrument hosts) with their product counts.

    Busiest first. ``products`` is the number of individually downloadable data
    products the PSA holds for that mission (one PSA "granule" = one product).
    The ``catalog`` column is a **best-effort** hint: the planetarypy catalog
    mission code (the prefix of a ``mission.instrument.product_type`` key, e.g.
    ``mex``), found by matching the PSA host name against the curated
    ``MISSION_FULL_NAMES`` display strings. It is blank when the PSA name doesn't
    match (different spelling, or the mission isn't in the catalog) — the match
    is coincidental, not contractual. The reliable way to find a key is
    catalog-first: ``plp catalog list`` → ``plp catalog list <mission>`` →
    ``plp catalog list <mission>.<instrument>``. The fetch/examples paths do not
    depend on this column.
    """
    import pandas as pd

    from planetarypy.catalog._mission_map import MISSION_FULL_NAMES

    code = {full: short for short, full in MISSION_FULL_NAMES.items()}
    rows = query(
        "SELECT instrument_host_name AS mission, COUNT(*) AS products "
        "FROM psa.epn_core GROUP BY instrument_host_name ORDER BY products DESC"
    )
    for r in rows:
        r["catalog"] = code.get(r["mission"], "")
    return pd.DataFrame(rows, columns=["mission", "catalog", "products"])


def instruments(mission: Optional[str] = None) -> "pd.DataFrame":
    """List PSA instruments with their product counts, optionally for one mission.

    ``products`` is the number of downloadable data products. ``mission`` is
    matched as a case-sensitive substring of the instrument host name (e.g.
    ``"Mars Express"``, ``"Rosetta"``).
    """
    import pandas as pd

    where = ""
    if mission:
        m = mission.replace("'", "''")
        where = f"WHERE instrument_host_name LIKE '%{m}%' "
    rows = query(
        "SELECT instrument_host_name AS mission, instrument_name AS instrument, "
        f"COUNT(*) AS products FROM psa.epn_core {where}"
        "GROUP BY instrument_host_name, instrument_name "
        "ORDER BY mission, products DESC"
    )
    return pd.DataFrame(rows, columns=["mission", "instrument", "products"])


def product_types(mission: str, instrument: Optional[str] = None) -> "pd.DataFrame":
    """List PSA product types (PDS3 datasets) for a mission and optional instrument.

    Each row is a ``dataset_id`` (e.g. ``MEX-M-ASPERA3-2-EDR-IMA-EXT4-V1.0``) with
    its product count, busiest first. ``mission`` and ``instrument`` are matched as
    case-sensitive substrings of the PSA host/instrument names exactly as listed by
    :func:`missions` / :func:`instruments`. A returned ``dataset_id`` can be handed
    straight to :func:`examples` — so the PSA browse chain stays entirely in PSA's
    own vocabulary, no catalog key needed.
    """
    import pandas as pd

    clauses = ["instrument_host_name LIKE '%" + mission.replace("'", "''") + "%'"]
    if instrument:
        clauses.append("instrument_name LIKE '%" + instrument.replace("'", "''") + "%'")
    where = " AND ".join(clauses)
    rows = query(
        "SELECT granule_gid, COUNT(*) AS products FROM psa.epn_core "
        f"WHERE {where} GROUP BY granule_gid ORDER BY products DESC"
    )
    out = [
        {"dataset_id": r["granule_gid"].split(":")[0], "products": r["products"]}
        for r in rows
    ]
    return pd.DataFrame(out, columns=["dataset_id", "products"])


def _granule_product_id(granule_uid: str) -> str:
    """Extract the PDS product id from a PSA granule_uid.

    ``DATA_SET_ID:DATA:PRODUCT_ID::version`` → ``PRODUCT_ID``.
    """
    return granule_uid.rsplit("::", 1)[0].rsplit(":", 1)[-1]


def examples(key: str, n: int = 5) -> "pd.DataFrame":
    """Return up to ``n`` example PSA products for a product type.

    ``key`` may be either:

    - a **PSA dataset id** (e.g. ``"MEX-M-ASPERA3-2-EDR-IMA-EXT4-V1.0"``, as listed
      by :func:`product_types`) — used directly, fully within PSA's vocabulary; or
    - a **catalog key** ``mission.instrument.product_type`` (e.g.
      ``"mex.aspera.els_edr_high"``) — the catalog maps it to a seed product whose
      granule identifier reveals the PSA dataset id, so no mission-name translation
      is needed.

    Returns a ``DataFrame`` with ``product_id``, ``granule_uid`` and ``access_url``.
    Empty if the key isn't resolvable to a PSA dataset.
    """
    import pandas as pd

    cols = ["product_id", "granule_uid", "access_url"]

    if "-" in key or ":" in key:
        # Already a PSA dataset id (or granule_gid) — catalog keys are dotted and
        # never contain '-' or ':'.
        dsid = key.split(":")[0]
    else:
        from planetarypy.catalog import example_products

        try:
            seeds = example_products(key)["product_id"].dropna().tolist()
        except Exception:
            return pd.DataFrame(columns=cols)
        dsid = None
        for pid in seeds:
            rows = resolve_all(pid, limit=1)
            if rows:
                dsid = rows[0]["granule_uid"].split(":")[0]
                break
        if dsid is None:
            return pd.DataFrame(columns=cols)

    esc = dsid.replace("'", "''")
    rows = query(
        f"SELECT TOP {int(n)} granule_uid, access_url "
        f"FROM psa.epn_core WHERE granule_uid LIKE '{esc}:%'"
    )
    out = [
        {
            "product_id": _granule_product_id(r["granule_uid"]),
            "granule_uid": r["granule_uid"],
            "access_url": r["access_url"],
        }
        for r in rows
    ]
    return pd.DataFrame(out, columns=cols)


def resolve_all(product_id: str, *, limit: int = 20) -> list[dict]:
    """Every PSA granule whose ``granule_uid`` contains ``product_id``.

    Each row has at least ``granule_uid`` and ``access_url`` (a direct download
    URL). Returns an empty list when nothing matches. ADQL ``LIKE`` does
    substring matching, so the bare product id is enough.
    """
    pid = product_id.replace("'", "''")  # ADQL single-quote escape
    return query(
        f"SELECT TOP {int(limit)} granule_uid, access_url, access_format "
        f"FROM psa.epn_core WHERE granule_uid LIKE '%{pid}%'"
    )


def resolve(product_id: str) -> Optional[str]:
    """Resolve a product id to its PSA download URL, or ``None`` if not found.

    The resolver contract: identity → ``access_url`` | ``None`` (NotResolvable).
    When the id matches several granules, the first match's URL is returned —
    use :func:`resolve_all` to inspect all matches.
    """
    rows = resolve_all(product_id, limit=1)
    return rows[0]["access_url"] if rows else None


def fetch_psa_product(
    product_id: str,
    dest: Optional[Path] = None,
    *,
    key: Optional[str] = None,
    extract: bool = True,
    skip_online_check: bool = False,
) -> list[Path]:
    """Download an ESA PSA product by id; return the local file paths.

    The PSA returns the product as a zip (label + data). With ``extract=True``
    (default) the zip is unpacked and the extracted paths returned; otherwise the
    zip path is returned. The files open with :func:`planetarypy.open`.

    Storage location:

    - ``dest`` given → used verbatim.
    - ``key`` given (the catalog ``mission.instrument.product_type``) → the
      standard catalog layout ``{storage_root}/{mission}/{instrument}/
      {product_type}/<product_id>/`` (same as ``plp fetch``).
    - neither → ``{storage_root}/psa/<product_id>/``.
    """
    from planetarypy.catalog import OfflineError
    from planetarypy.utils import have_internet, url_retrieve

    if not skip_online_check and not have_internet():
        raise OfflineError(
            "No internet connection detected — cannot reach the ESA PSA. "
            "Pass skip_online_check=True to override."
        )
    url = resolve(product_id)
    if url is None:
        raise ValueError(f"No PSA product found for {product_id!r}")

    safe = product_id.replace(":", "_").replace("/", "_")
    if dest is None:
        from planetarypy.config import config

        if key and key.count(".") >= 2:
            from planetarypy.catalog import default_product_dir

            mission, instrument, product_type = key.split(".", 2)
            dest = default_product_dir(mission, instrument, product_type, product_id)
        else:
            dest = Path(config.storage_root) / "psa" / safe
    dest = Path(dest)
    dest.mkdir(parents=True, exist_ok=True)

    zip_path = dest / f"{safe}.zip"
    if not zip_path.exists():
        url_retrieve(url, str(zip_path))
    if not extract:
        return [zip_path]
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(dest)
        names = zf.namelist()
    return [dest / n for n in names if not n.endswith("/")]
