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
from typing import Optional

import requests

__all__ = ["query", "resolve", "resolve_all", "fetch_psa_product", "PSA_TAP_SYNC"]

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
    extract: bool = True,
    skip_online_check: bool = False,
) -> list[Path]:
    """Download an ESA PSA product by id; return the local file paths.

    The PSA returns the product as a zip (label + data). Files land under
    ``{storage_root}/psa/<product_id>/``. With ``extract=True`` (default) the
    zip is unpacked and the extracted paths returned; otherwise the zip path is
    returned. The files open with :func:`planetarypy.open`.
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
