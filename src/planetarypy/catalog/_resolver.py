"""Product download and URL resolution for the PDS catalog.

Resolution strategy (chain of responsibility):

1. **Catalog lookup**: If the product_id matches a sample product in the DB,
   use its url_stem + files directly. Works for ~1,948 sample products.

2. **Index lookup**: For product types with a registered PDS cumulative
   index, look up the product_id to get volume/path info. Authoritative
   for instruments spanning many volumes (CTX, HiRISE, Cassini ISS, etc.).

3. **Pattern-based**: For types WITHOUT an index, if catalog samples all
   share the same url_stem, derive the URL from the fixed stem + product_id.
   Skipped when an index exists (single-sample types can look "fixed" when
   they actually span many volumes).

4. **Fail with guidance**: Raise ProductNotFoundError explaining what's
   needed (variable URL, no index available, etc.).
"""

import json
from dataclasses import dataclass, field
from pathlib import Path

from loguru import logger

from planetarypy.config import config
from planetarypy.utils import url_retrieve


class ProductNotFoundError(Exception):
    """Product ID not found in catalog or any available index."""


class DownloadError(Exception):
    """Failed to download one or more product files."""


@dataclass
class ResolvedProduct:
    """A product resolved to downloadable URLs."""

    product_id: str
    url_stem: str
    files: list[str]
    label_file: str | None
    source: str  # 'catalog', 'index', 'constructed'

    @property
    def file_urls(self) -> dict[str, str]:
        """Map of filename -> full URL for each file."""
        base = self.url_stem.rstrip("/")
        return {f: f"{base}/{f}" for f in self.files}

    @property
    def label_url(self) -> str | None:
        """Full URL for the label file."""
        if self.label_file:
            return f"{self.url_stem.rstrip('/')}/{self.label_file}"
        return None


# ── Storage resolver registry ─────────────────────────────────────
#
# Instrument modules can register custom storage resolvers so that
# `plp fetch` stores products in the same layout as instrument-specific
# commands (hiedr, ctxfetch, etc.) and respects per-instrument config files.
#
# Register with:  register_storage_resolver("mro.hirise", my_func)
# The function signature is:  (product_type, product_id) -> Path

_STORAGE_RESOLVERS: dict[str, callable] = {}


def register_storage_resolver(key: str, resolver: callable):
    """Register an instrument-specific storage path resolver.

    Parameters
    ----------
    key : str
        Dotted mission.instrument key, e.g. ``"mro.hirise"``.
    resolver : callable
        Function ``(product_type: str, product_id: str) -> Path``
        returning the local directory for a product.
    """
    _STORAGE_RESOLVERS[key] = resolver


# Lazy-loaded resolver paths: maps key → (module, function_name)
_STORAGE_RESOLVER_MODULES = {
    "mro.hirise": ("planetarypy.instruments.mro.hirise", "_hirise_local_product_dir"),
    "mro.ctx": ("planetarypy.instruments.mro.ctx.ctx_edr", "_ctx_local_product_dir"),
}


def _local_product_dir(
    mission: str, instrument: str, product_type: str, product_id: str,
) -> Path:
    """Build local storage path for a product.

    Checks for a registered instrument-specific resolver first (either
    eagerly registered or lazy-loaded from ``_STORAGE_RESOLVER_MODULES``),
    then falls back to the default layout:
    ``{storage_root}/{mission}/{instrument}/{product_type}/{product_id}/``
    """
    key = f"{mission}.{instrument}"

    # Check eager registrations first
    if key in _STORAGE_RESOLVERS:
        return _STORAGE_RESOLVERS[key](product_type, product_id)

    # Try lazy-loading
    if key in _STORAGE_RESOLVER_MODULES:
        mod_path, func_name = _STORAGE_RESOLVER_MODULES[key]
        try:
            import importlib
            mod = importlib.import_module(mod_path)
            resolver = getattr(mod, func_name)
            _STORAGE_RESOLVERS[key] = resolver  # cache for next call
            return resolver(product_type, product_id)
        except (ImportError, AttributeError):
            pass

    # Default layout
    safe_pid = product_id.replace("/", "_").replace("\\", "_")
    return (
        config.storage_root / mission / instrument / product_type / safe_pid
    )


def resolve_product(
    mission: str,
    instrument: str,
    product_key: str,
    product_id: str,
) -> ResolvedProduct:
    """Resolve a product to downloadable URLs.

    Tries catalog lookup first, then index lookup.

    Parameters
    ----------
    mission, instrument, product_key : str
        The mission.instrument.product_type triplet
    product_id : str
        Product identifier to resolve

    Returns
    -------
    ResolvedProduct

    Raises
    ------
    ProductNotFoundError
        If the product cannot be resolved
    """
    # Tier 1: Catalog lookup
    resolved = _resolve_from_catalog(mission, instrument, product_key, product_id)
    if resolved:
        return resolved

    # Tier 2: Index lookup (authoritative — covers variable url_stems)
    from planetarypy.catalog._index_resolver import resolve_from_index, has_index

    resolved = resolve_from_index(mission, instrument, product_key, product_id)
    if resolved:
        return resolved

    # Tier 3: Pattern-based (for types with fixed url_stem, no index needed)
    # Only used when fetchability analysis confirms the url_stem is fixed.
    from planetarypy.catalog._pattern_resolver import resolve_by_pattern
    from planetarypy.catalog._pattern_resolver import classify_product_type

    if not has_index(mission, instrument, product_key):
        fetchability = classify_product_type(mission, instrument, product_key)

        if fetchability.status == "fixed":
            resolved = resolve_by_pattern(
                mission, instrument, product_key, product_id,
            )
            if resolved:
                return resolved

    # Build a helpful error message
    if has_index(mission, instrument, product_key):
        hint = (
            "The product was not found in either the catalog samples or "
            "the PDS index. Check the product_id spelling."
        )
    else:
        fetchability = classify_product_type(mission, instrument, product_key)
        if fetchability.status == "unfetchable":
            hint = (
                f"This product type has variable URL paths "
                f"({fetchability.reason}) and no PDS index is available. "
                f"Only the sample products in the catalog can be fetched. "
                f"Use example_products('{mission}.{instrument}.{product_key}') "
                f"to see available samples."
            )
        else:
            hint = (
                f"No URL pattern or PDS index is available for "
                f"{mission}.{instrument}.{product_key}. "
                f"Only sample products from the catalog are available."
            )

    raise ProductNotFoundError(
        f"Product '{product_id}' not found for "
        f"{mission}.{instrument}.{product_key}. {hint}"
    )


def _resolve_from_catalog(
    mission: str, instrument: str, product_key: str, product_id: str,
) -> ResolvedProduct | None:
    """Look up product in the catalog DB sample products."""
    from planetarypy.catalog import get_catalog

    con = get_catalog()
    row = con.execute(
        """SELECT p.url_stem, p.files, p.label_file
           FROM products p
           JOIN product_types pt USING (folder_name, product_key)
           JOIN instruments i USING (folder_name)
           WHERE i.mission = ? AND i.instrument = ?
             AND (pt.normalized_type = ? OR pt.product_key = ?)
             AND p.product_id = ?""",
        [mission, instrument, product_key, product_key, product_id],
    ).fetchone()

    if row is None:
        return None

    url_stem, files_json, label_file = row

    if not url_stem:
        return None

    # Parse files JSON array
    try:
        files = json.loads(files_json) if files_json else []
    except (json.JSONDecodeError, TypeError):
        files = []

    # If no files list but we have a label_file, use that
    if not files and label_file:
        files = [label_file]

    return ResolvedProduct(
        product_id=product_id,
        url_stem=url_stem,
        files=files,
        label_file=label_file,
        source="catalog",
    )


def download_product(
    resolved: ResolvedProduct,
    local_dir: Path,
    files: list[str] | None = None,
    label_only: bool = False,
    force: bool = False,
) -> list[Path]:
    """Download product files to local_dir.

    Parameters
    ----------
    resolved : ResolvedProduct
        The resolved product with URLs
    local_dir : Path
        Local directory to download into
    files : list[str] | None
        Specific filenames to download. None = all files.
    label_only : bool
        If True, download only the label file.
    force : bool
        If True, re-download even if files exist locally.

    Returns
    -------
    list[Path]
        Paths to downloaded files
    """
    local_dir.mkdir(parents=True, exist_ok=True)

    if label_only:
        if not resolved.label_file:
            raise DownloadError("No label file defined for this product")
        to_download = [resolved.label_file]
    elif files:
        to_download = files
    else:
        to_download = resolved.files

    if not to_download:
        raise DownloadError("No files to download for this product")

    downloaded: list[Path] = []
    urls = resolved.file_urls

    for filename in to_download:
        local_path = local_dir / filename
        if local_path.exists() and not force:
            logger.debug(f"Already exists: {local_path}")
            downloaded.append(local_path)
            continue

        url = urls.get(filename)
        if not url:
            url = f"{resolved.url_stem.rstrip('/')}/{filename}"

        logger.info(f"Downloading {filename}")
        try:
            url_retrieve(url, str(local_path))
            downloaded.append(local_path)
        except Exception as e:
            raise DownloadError(f"Failed to download {url}: {e}") from e

    return downloaded
