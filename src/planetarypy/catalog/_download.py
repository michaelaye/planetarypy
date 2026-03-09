"""Product download and URL resolution for the PDS catalog.

Resolution strategy (chain of responsibility):

1. **Catalog lookup**: If the product_id matches a sample product in the DB,
   use its url_stem + files directly. Works for ~1,948 sample products.

2. **Index lookup**: If a PDS index exists for this mission.instrument,
   look up the product_id in the index to get volume/path info, then
   construct the URL. Requires an IndexBridge configuration.

3. **Fail with guidance**: Raise ProductNotFoundError explaining what's
   needed (index not available, product not in catalog, etc.).

URL construction patterns vary across the PDS:
- Static: same base URL for all products, just append filename
- Volume-based: URL includes volume ID (e.g., coiss_2022) → need index
- Date/orbit-based: URL includes date path (e.g., 2018193/WAC) → need index
- For sample products, url_stem + filename always works regardless of pattern.
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


def _local_product_dir(
    mission: str, instrument: str, product_type: str, product_id: str,
) -> Path:
    """Build local storage path for a product.

    Layout: {storage_root}/{mission}/{instrument}/{product_type}/{product_id}/
    """
    # Sanitize product_id for filesystem use
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

    # Tier 2: Index lookup (future)
    # resolved = _resolve_from_index(mission, instrument, product_key, product_id)
    # if resolved:
    #     return resolved

    raise ProductNotFoundError(
        f"Product '{product_id}' not found for {mission}.{instrument}.{product_key}. "
        f"Currently only sample products from the catalog are supported. "
        f"For arbitrary product IDs, a PDS index is needed for this instrument."
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
             AND pt.product_key = ? AND p.product_id = ?""",
        [mission, instrument, product_key, product_id],
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
