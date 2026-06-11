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

import re
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
    "datasets",
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


def _ci_like(column: str, value: str) -> str:
    """Case-insensitive ADQL substring match: ``UPPER(col) LIKE UPPER('%value%')``."""
    esc = value.replace("'", "''")
    return f"UPPER({column}) LIKE UPPER('%{esc}%')"


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
    matched as a case-insensitive substring of the instrument host name (e.g.
    ``"Mars Express"``, ``"rosetta"``).
    """
    import pandas as pd

    where = ""
    if mission:
        where = "WHERE " + _ci_like("instrument_host_name", mission) + " "
    rows = query(
        "SELECT instrument_host_name AS mission, instrument_name AS instrument, "
        f"COUNT(*) AS products FROM psa.epn_core {where}"
        "GROUP BY instrument_host_name, instrument_name "
        "ORDER BY mission, products DESC"
    )
    return pd.DataFrame(rows, columns=["mission", "instrument", "products"])


def datasets(mission: str, instrument: Optional[str] = None) -> "pd.DataFrame":
    """List PSA datasets for a mission and optional instrument.

    A PSA *dataset* is the archival grouping that holds many products — a PDS3
    **data set** (``DATA_SET_ID``, e.g. ``MEX-M-ASPERA3-2-EDR-IMA-EXT4-V1.0:DATA``)
    or a PDS4 **collection** (LID, e.g. ``urn:esa:psa:bc_mpo_berm:data_calibrated``).
    Each row is a ``dataset`` with its product count, busiest first.

    ``mission`` and ``instrument`` are matched as case-insensitive substrings of the
    PSA host/instrument names listed by :func:`missions` / :func:`instruments`. A
    returned ``dataset`` can be handed straight to :func:`examples` — so the PSA
    browse chain stays entirely in PSA's own vocabulary, no catalog key needed.
    """
    import pandas as pd

    clauses = [_ci_like("instrument_host_name", mission)]
    if instrument:
        clauses.append(_ci_like("instrument_name", instrument))
    where = " AND ".join(clauses)
    rows = query(
        "SELECT granule_gid, COUNT(*) AS products FROM psa.epn_core "
        f"WHERE {where} GROUP BY granule_gid ORDER BY products DESC"
    )
    out = [{"dataset": r["granule_gid"], "products": r["products"]} for r in rows]
    return pd.DataFrame(out, columns=["dataset", "products"])


def _granule_gid(granule_uid: str) -> str:
    """The dataset/collection group id from a granule_uid (drop product + version).

    Works for PDS3 (``DATA_SET_ID:DATA:PRODUCT::ver`` → ``DATA_SET_ID:DATA``) and
    PDS4 (``urn:…:collection:product::ver`` → ``urn:…:collection``).
    """
    return granule_uid.rsplit("::", 1)[0].rsplit(":", 1)[0]


def _granule_product_id(granule_uid: str) -> str:
    """Extract the product id from a PSA granule_uid (the last ``:`` segment)."""
    return granule_uid.rsplit("::", 1)[0].rsplit(":", 1)[-1]


def examples(key: str, n: int = 5) -> "pd.DataFrame":
    """Return up to ``n`` example PSA products for a product type.

    ``key`` may be either:

    - a **PSA dataset** (a PDS3 ``DATA_SET_ID`` or PDS4 collection LID, as listed by
      :func:`datasets`) — used directly, fully within PSA's vocabulary; or
    - a **catalog key** ``mission.instrument.product_type`` (e.g.
      ``"mex.aspera.els_edr_high"``) — the catalog maps it to a seed product whose
      granule identifier reveals the PSA dataset, so no mission-name translation is
      needed.

    Returns a ``DataFrame`` with ``product_id``, ``granule_uid`` and ``access_url``.
    Empty if the key isn't resolvable to a PSA dataset.
    """
    import pandas as pd

    cols = ["product_id", "granule_uid", "access_url"]

    if "-" in key or ":" in key:
        # Already a PSA dataset (DATA_SET_ID or collection LID) — catalog keys are
        # dotted and never contain '-' or ':'. Use it whole as a granule_uid prefix.
        gid = key
    else:
        from planetarypy.catalog import example_products

        try:
            seeds = example_products(key)["product_id"].dropna().tolist()
        except Exception:
            return pd.DataFrame(columns=cols)
        gid = None
        for pid in seeds:
            rows = resolve_all(pid, limit=1)
            if rows:
                gid = _granule_gid(rows[0]["granule_uid"])
                break
        if gid is None:
            return pd.DataFrame(columns=cols)

    esc = gid.replace("'", "''")
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

    Each row has ``granule_uid``, ``access_url`` (the zip-bundle download),
    ``access_format``, and ``label_url`` (the direct file URL on the PSA FTP
    archive — the product's PDS label, which for an attached-label product *is*
    the data file). Returns an empty list when nothing matches. ADQL ``LIKE``
    does substring matching, so the bare product id is enough. Results are
    ordered by ``granule_uid`` so the same id resolves deterministically — a
    filename can occur in several datasets (e.g. across processing levels), and a
    stable order makes :func:`resolve` and :func:`fetch_psa_product` reproducible.
    """
    pid = product_id.replace("'", "''")  # ADQL single-quote escape
    return query(
        f"SELECT TOP {int(limit)} granule_uid, access_url, access_format, label_url "
        f"FROM psa.epn_core WHERE granule_uid LIKE '%{pid}%' "
        "ORDER BY granule_uid"
    )


def resolve(product_id: str) -> Optional[str]:
    """Resolve a product id to its PSA download URL, or ``None`` if not found.

    The resolver contract: identity → ``access_url`` | ``None`` (NotResolvable).
    When the id matches several granules, the first match's URL is returned —
    use :func:`resolve_all` to inspect all matches.
    """
    rows = resolve_all(product_id, limit=1)
    return rows[0]["access_url"] if rows else None


_FTP_ROOT_MARKER = "/psa/ftp/"
# ^POINTER = "file"  or  ^POINTER = ("file", 3)  — a quoted filename means a
# detached data file; an unquoted record number (^IMAGE = 13) is an attached
# label pointing inside its own file and is correctly skipped.
_LABEL_POINTER_RE = re.compile(r'\^\w+\s*=\s*\(?\s*"([^"]+)"')


def _label_data_pointers(label_text: str) -> list[str]:
    """Data file names referenced by ``^POINTER = "file"`` entries in a PDS3 label.

    Only quoted pointers (detached data files) are returned; ``.LBL`` self-
    references and unquoted record-offset pointers (attached labels) are skipped.
    """
    out = []
    for fname in _LABEL_POINTER_RE.findall(label_text):
        fname = fname.split("/")[-1]
        if fname and not fname.upper().endswith(".LBL") and fname not in out:
            out.append(fname)
    return out


def _ftp_relpath(label_url: str, dataset_id: str) -> str:
    """Local path (relative to ``psa/``) for an FTP file URL: from DATA_SET_ID on.

    Mirrors the zip layout ``<DATA_SET_ID>/DATA/<volume>/<file>`` by dropping the
    archive's ``<MISSION>/<INSTRUMENT>/`` prefix.
    """
    ftp = label_url.split(_FTP_ROOT_MARKER, 1)[-1]
    i = ftp.find(dataset_id)
    return ftp[i:] if i != -1 else "/".join(ftp.split("/")[2:])


def fetch_psa_product(
    product_id: str,
    dest: Optional[Path] = None,
    *,
    direct: bool = True,
    extract: bool = True,
    skip_online_check: bool = False,
) -> list[Path]:
    """Download an ESA PSA product by id; return the local file paths.

    Two delivery routes, both landing in the same faithful tree under
    ``{storage_root}/psa/<DATA_SET_ID>/DATA/<volume>/<files>`` (volume sharding
    preserved, real ``DATA_SET_ID`` as the folder). The files open with
    :func:`planetarypy.open`.

    - **Direct FTP file** (default, ``direct=True``): fetch the product's own
      file(s) straight from the PSA archive via the granule's ``label_url`` — no
      zip, no redundant volume manifest. For an attached-label product that URL
      *is* the data file; for a detached-label one it is the ``.LBL`` whose
      ``^POINTER`` names the co-located data file, which is fetched alongside it.
    - **Zip bundle** (``direct=False``, or automatic fallback when the granule
      has no ``label_url`` — e.g. PDS4 missions): download the PSA product zip and
      unpack it faithfully. Dataset-level docs (``VOLDESC.CAT``/``AAREADME.TXT``)
      are written once; the zip's own ``inventory.txt`` manifest is dropped.
      ``extract=False`` keeps the raw zip instead.

    A per-product marker under ``psa/.fetched/`` records the fetched paths, so a
    repeat fetch returns them without re-downloading. Pass ``dest`` to use a
    different root.
    """
    from planetarypy.catalog import OfflineError
    from planetarypy.config import config
    from planetarypy.utils import have_internet

    if not skip_online_check and not have_internet():
        raise OfflineError(
            "No internet connection detected — cannot reach the ESA PSA. "
            "Pass skip_online_check=True to override."
        )
    matches = resolve_all(product_id, limit=1)
    if not matches:
        raise ValueError(f"No PSA product found for {product_id!r}")
    gid = _granule_gid(matches[0]["granule_uid"])

    root = Path(dest) if dest is not None else Path(config.storage_root) / "psa"
    safe_pid = product_id.replace(":", "_").replace("/", "_")
    safe_gid = gid.replace(":", "_").replace("/", "_")
    marker = root / ".fetched" / f"{safe_gid}__{safe_pid}"

    use_direct = direct and extract and bool(matches[0].get("label_url"))
    if (extract or use_direct) and marker.exists():
        cached = [root / line for line in marker.read_text().splitlines() if line]
        if cached and all(p.exists() for p in cached):
            return cached
    marker.parent.mkdir(parents=True, exist_ok=True)

    if use_direct:
        dataset_id = gid.rsplit(":", 1)[0]
        return _fetch_direct(matches[0]["label_url"], dataset_id, root, marker)
    return _fetch_zip(matches[0]["access_url"], root, marker, extract=extract)


def _fetch_direct(
    label_url: str, dataset_id: str, root: Path, marker: Path
) -> list[Path]:
    """Fetch a product's files directly from the PSA FTP archive (no zip)."""
    from planetarypy.utils import url_retrieve

    jobs = [(label_url, _ftp_relpath(label_url, dataset_id))]
    if label_url.upper().endswith(".LBL"):
        label_text = requests.get(label_url, timeout=60).text
        base_url = label_url.rsplit("/", 1)[0]
        for fname in _label_data_pointers(label_text):
            data_url = f"{base_url}/{fname}"
            jobs.append((data_url, _ftp_relpath(data_url, dataset_id)))

    paths = []
    for url, rel in jobs:
        target = root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            url_retrieve(url, str(target))
        paths.append(target)
    marker.write_text("\n".join(str(p.relative_to(root)) for p in paths))
    return paths


def _fetch_zip(url: str, root: Path, marker: Path, *, extract: bool) -> list[Path]:
    """Download the PSA product zip and unpack it faithfully under ``root``."""
    import shutil

    from planetarypy.utils import url_retrieve

    zip_path = marker.with_suffix(".zip")
    if not zip_path.exists():
        url_retrieve(url, str(zip_path))
    if not extract:
        return [zip_path]

    extracted: list[Path] = []
    with zipfile.ZipFile(zip_path) as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            name = info.filename
            if "/" not in name:
                # Loose file at the archive root = the zip's own manifest
                # (inventory.txt). Not part of the dataset tree — drop it.
                continue
            target = root / name
            is_dataset_doc = name.count("/") == 1  # <DATA_SET_ID>/<FILE>
            if is_dataset_doc and target.exists():
                extracted.append(target)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(info) as src, open(target, "wb") as dst:
                shutil.copyfileobj(src, dst)
            extracted.append(target)

    marker.write_text("\n".join(str(p.relative_to(root)) for p in extracted))
    zip_path.unlink(missing_ok=True)
    return extracted
