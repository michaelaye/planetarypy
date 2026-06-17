"""PDS-wide product discovery via the NASA PDS Registry search API.

Thin wrapper over NASA's ``pds.api-client`` (the PDS Engineering Node's REST
client for the registry-wide search API at https://pds.nasa.gov/api/search/1,
80M+ products). Build a query from keyword filters, get back a
:class:`pandas.DataFrame`, and download any product's files by LIDVID.

Requires the optional ``[search]`` extra::

    pip install "planetarypy[search]"

Scope: this is the **NASA** PDS4 registry. It covers Cassini, Voyager,
Magellan, MRO, LRO, … (including products planetarypy's catalog can't
otherwise resolve), but **not** non-NASA national archives such as Chang'e
(CNSA) or Chandrayaan-2/3 (ISRO).
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union

if TYPE_CHECKING:
    import datetime as _dt

    import pandas as pd

__all__ = [
    "search_products",
    "get_product",
    "product_file_urls",
    "fetch_pds_product",
]

# Canonical public endpoint. It 307-redirects to the backend host; the
# client follows the redirect transparently.
_DEFAULT_HOST = "https://pds.nasa.gov/api/search/1"

# Registry properties holding the actual downloadable URLs.
_FILE_REF_KEYS = (
    "ops:Data_File_Info.ops:file_ref",
    "ops:Label_File_Info.ops:file_ref",
)

_SEARCH_EXTRA_MISSING_MSG = (
    "PDS registry search requires the [search] extra: "
    '`pip install "planetarypy[search]"` '
    "(or `conda install -c conda-forge pds.api-client`). "
    "See docs/howto/pds_search.qmd."
)


def _api(host: str = _DEFAULT_HOST):
    """Build an ``AllProductsApi`` client, importing the extra lazily."""
    try:
        from pds.api_client import AllProductsApi, ApiClient, Configuration
    except ImportError as exc:  # pragma: no cover - exercised via stub in tests
        raise ImportError(_SEARCH_EXTRA_MISSING_MSG) from exc
    return AllProductsApi(ApiClient(Configuration(host=host)))


def _as_iso(value: "Union[str, _dt.datetime]") -> str:
    """Normalize a date/datetime (or ISO string) to an ISO-8601 string."""
    import datetime as dt

    if isinstance(value, dt.datetime):
        return value.isoformat()
    from dateutil import parser as _dtparser

    return _dtparser.parse(value).isoformat()


def _build_q(
    *,
    target: Optional[str],
    instrument: Optional[str],
    instrument_host: Optional[str],
    investigation: Optional[str],
    processing_level: Optional[str],
    before: "Optional[Union[str, _dt.datetime]]",
    after: "Optional[Union[str, _dt.datetime]]",
    observationals: bool,
    lidvid: Optional[str],
    query: Optional[str],
) -> Optional[str]:
    """Translate keyword filters into a PDS API ``q`` clause string.

    Clause syntax mirrors ``pds.peppi``'s query builder. Multiple filters are
    AND-combined. Returns ``None`` when no filter is given (matches all).
    """
    clauses: list[str] = []
    if target:
        clauses.append(f'ref_lid_target eq "{target}"')
    if instrument:
        clauses.append(f'ref_lid_instrument eq "{instrument}"')
    if instrument_host:
        clauses.append(f'ref_lid_instrument_host eq "{instrument_host}"')
    if investigation:
        clauses.append(f'ref_lid_investigation eq "{investigation}"')
    if processing_level:
        clauses.append(
            "pds:Primary_Result_Summary.pds:processing_level "
            f'eq "{processing_level.title()}"'
        )
    if after is not None:
        clauses.append(
            f'pds:Time_Coordinates.pds:stop_date_time ge "{_as_iso(after)}"'
        )
    if before is not None:
        clauses.append(
            f'pds:Time_Coordinates.pds:start_date_time le "{_as_iso(before)}"'
        )
    if observationals:
        clauses.append('product_class eq "Product_Observational"')
    if lidvid:
        field = "lidvid" if "::" in lidvid else "lid"
        clauses.append(f'{field} eq "{lidvid}"')
    if query:
        clauses.append(query)
    if not clauses:
        return None
    # The PDS registry API requires the whole q wrapped in outer parentheses:
    # a bare ``A and B`` returns HTTP 400 (UnparsableQParamException), while
    # ``(A and B)`` parses. (peppi does the same in its ResultSet before sending.)
    # Wrapping a single clause is harmless.
    return "(" + " and ".join(clauses) + ")"


def _flatten(value: Any) -> Any:
    """Reduce single-element list cells to scalars; empty lists to ``None``.

    The registry returns every property as a list. One-element lists are the
    common case and read better as scalars; empty lists carry no value. (This
    is the bug that crashes ``pds.peppi``'s ``as_dataframe`` — it indexes
    ``[0]`` without guarding the empty case.)
    """
    if isinstance(value, (list, tuple)):
        if not value:
            return None
        if len(value) == 1:
            return value[0]
        return list(value)
    return value


def search_products(
    *,
    target: Optional[str] = None,
    instrument: Optional[str] = None,
    instrument_host: Optional[str] = None,
    investigation: Optional[str] = None,
    processing_level: Optional[str] = None,
    before: "Optional[Union[str, _dt.datetime]]" = None,
    after: "Optional[Union[str, _dt.datetime]]" = None,
    observationals: bool = False,
    lidvid: Optional[str] = None,
    query: Optional[str] = None,
    fields: Optional[list[str]] = None,
    limit: int = 100,
    host: str = _DEFAULT_HOST,
) -> "pd.DataFrame":
    """Search the NASA PDS registry; return one row per matching product.

    Parameters
    ----------
    target, instrument, instrument_host, investigation : str, optional
        Filter by the LID of a context object (e.g. ``target="urn:nasa:pds:
        context:target:planet.saturn"``).
    processing_level : str, optional
        One of ``"telemetry"``, ``"raw"``, ``"partially-processed"``,
        ``"calibrated"``, ``"derived"``.
    before, after : str | datetime, optional
        Restrict to products whose observation window starts before / ends
        after the given time.
    observationals : bool
        If ``True``, restrict to ``Product_Observational`` products.
    lidvid : str, optional
        Match a single product by LIDVID (``urn:…::1.0``) or LID.
    query : str, optional
        Raw PDS API query clause, AND-combined with the other filters — the
        escape hatch for anything the keyword filters don't cover (e.g.
        ``query='lid like "urn:nasa:pds:cassini_iss_saturn*"'``).
    fields : list[str], optional
        Restrict the returned columns to these registry property names.
    limit : int
        Maximum number of products to return (a single API request). Default
        100. Very large result sets are not auto-paginated in this release.

    Returns
    -------
    pandas.DataFrame
        Columns are the registry's property names (e.g.
        ``ops:Data_File_Info.ops:file_ref``); the index is the product LIDVID.
        Single-element list cells are flattened to scalars.
    """
    import pandas as pd

    q = _build_q(
        target=target,
        instrument=instrument,
        instrument_host=instrument_host,
        investigation=investigation,
        processing_level=processing_level,
        before=before,
        after=after,
        observationals=observationals,
        lidvid=lidvid,
        query=query,
    )
    resp = _api(host).product_list(q=q, fields=fields, limit=limit)
    rows = [
        {k: _flatten(v) for k, v in product.properties.items()}
        for product in resp.data
    ]
    index = [product.id for product in resp.data]
    df = pd.DataFrame.from_records(rows, index=index)
    df.index.name = "lidvid"
    return df


def get_product(lidvid: str, *, host: str = _DEFAULT_HOST) -> dict:
    """Return one product's registry properties by LIDVID (or LID).

    Raises :class:`KeyError` if no product matches.
    """
    q = _build_q(
        target=None,
        instrument=None,
        instrument_host=None,
        investigation=None,
        processing_level=None,
        before=None,
        after=None,
        observationals=False,
        lidvid=lidvid,
        query=None,
    )
    resp = _api(host).product_list(q=q, limit=1)
    if not resp.data:
        raise KeyError(f"No PDS product found for {lidvid!r}")
    return dict(resp.data[0].properties)


def product_file_urls(product_or_props: Any) -> list[str]:
    """Extract data + label download URLs from a product.

    Accepts a product object (with a ``.properties`` mapping) or a properties
    ``dict`` (as returned by :func:`get_product` or a DataFrame row). Skips the
    registry's ``"null"`` sentinel for absent files.
    """
    props = getattr(product_or_props, "properties", product_or_props)
    urls: list[str] = []
    for key in _FILE_REF_KEYS:
        value = props.get(key)
        if not value:
            continue
        refs = value if isinstance(value, (list, tuple)) else [value]
        urls += [u for u in refs if u and u != "null"]
    return urls


def fetch_pds_product(
    lidvid: str,
    dest: Optional[Path] = None,
    *,
    skip_online_check: bool = False,
    host: str = _DEFAULT_HOST,
) -> list[Path]:
    """Download every file (data + label) for one PDS product by LIDVID.

    Files land under ``{storage_root}/pds_search/<sanitized-lidvid>/`` unless
    ``dest`` is given. Existing files are skipped. Returns the local paths.
    The downloaded files can be opened with :func:`planetarypy.open`.
    """
    from planetarypy.catalog import OfflineError
    from planetarypy.utils import have_internet, url_retrieve

    if not skip_online_check and not have_internet():
        raise OfflineError(
            "No internet connection detected — cannot reach the PDS registry. "
            "Pass skip_online_check=True to override."
        )
    props = get_product(lidvid, host=host)
    urls = product_file_urls(props)
    if not urls:
        raise ValueError(f"No downloadable file URLs found for {lidvid!r}")

    if dest is None:
        from planetarypy.config import config

        safe = lidvid.replace(":", "_").replace("/", "_")
        dest = Path(config.storage_root) / "pds_search" / safe
    dest = Path(dest)
    dest.mkdir(parents=True, exist_ok=True)

    paths: list[Path] = []
    for url in urls:
        outfile = dest / url.rsplit("/", 1)[-1]
        if not outfile.exists():
            url_retrieve(url, str(outfile))
        paths.append(outfile)
    return paths
