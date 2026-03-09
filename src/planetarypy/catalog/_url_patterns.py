"""Pattern-based URL resolution from catalog sample products.

For product types where the url_stem is the same for all products (the
vast majority), we can resolve ANY product_id by:
1. Looking up the fixed url_stem from a sample product in the catalog
2. Deriving the label filename from the product_id
3. Constructing the full URL

This covers ~92% of product types (1,606 of 1,740). The remaining ~8%
have variable url_stems (volume-based, date-based paths) and need either:
- A PDS cumulative index (handled by _index_bridge.py)
- An instrument-specific URL derivation rule (future work)

This module is Tier 2 in the resolution chain:
  Tier 1: Catalog exact match (sample products in DB)
  Tier 2: Pattern-based (this module — fixed url_stem + derived filename)
  Tier 3: Index lookup (_index_bridge.py — for variable url_stems)
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import PurePosixPath
from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from planetarypy.catalog._download import ResolvedProduct


def resolve_by_pattern(
    mission: str,
    instrument: str,
    product_key: str,
    product_id: str,
) -> ResolvedProduct | None:
    """Resolve a product using URL patterns learned from catalog samples.

    Queries the catalog for sample products of this type, checks whether
    the url_stem is constant, and if so, constructs a URL for the new
    product_id.

    Returns None if:
    - No samples exist for this product type
    - The url_stem varies across samples (needs index instead)
    - The label filename cannot be derived
    """
    from planetarypy.catalog._download import ResolvedProduct

    pattern = _get_url_pattern(mission, instrument, product_key)
    if pattern is None:
        return None

    url_stem, label_rule = pattern
    label_file = _derive_label_file(product_id, label_rule)
    files = _build_file_list(label_file)

    return ResolvedProduct(
        product_id=product_id,
        url_stem=url_stem,
        files=files,
        label_file=label_file,
        source="pattern",
    )


def has_pattern(mission: str, instrument: str, product_key: str) -> bool:
    """Check if a URL pattern can be learned for this product type."""
    return _get_url_pattern(mission, instrument, product_key) is not None


# ── Internal helpers ──────────────────────────────────────────────────


@lru_cache(maxsize=256)
def _get_url_pattern(
    mission: str, instrument: str, product_key: str,
) -> tuple[str, str] | None:
    """Learn the URL pattern for a product type from catalog samples.

    Returns (url_stem, label_rule) or None.

    The result is cached since patterns don't change within a session.
    """
    from planetarypy.catalog import get_catalog

    con = get_catalog()
    rows = con.execute(
        """SELECT DISTINCT p.url_stem, p.product_id, p.label_file
           FROM products p
           JOIN instruments i USING (folder_name)
           JOIN product_types pt USING (folder_name, product_key)
           WHERE i.mission = ? AND i.instrument = ?
             AND pt.product_key = ?
             AND p.url_stem IS NOT NULL
           LIMIT 20""",
        [mission, instrument, product_key],
    ).fetchall()
    con.close()

    if not rows:
        logger.debug(
            f"No samples for {mission}.{instrument}.{product_key}"
        )
        return None

    # Check if url_stem is constant across samples
    url_stems = {r[0] for r in rows}
    if len(url_stems) > 1:
        logger.debug(
            f"Variable url_stem for {mission}.{instrument}.{product_key} "
            f"({len(url_stems)} distinct) — needs index or custom resolver"
        )
        return None

    url_stem = rows[0][0]
    label_rule = _detect_label_rule(rows)

    return url_stem, label_rule


def _detect_label_rule(
    rows: list[tuple[str, str, str | None]],
) -> str:
    """Detect how label_file relates to product_id from sample data.

    Returns a rule string used by _derive_label_file().
    """
    for _url, pid, label in rows:
        if not pid or not label:
            continue

        ext = label.rsplit(".", 1)[-1] if "." in label else ""
        stem = label.rsplit(".", 1)[0] if "." in label else label

        # PDS4 URN-style: urn:esa:psa:...:last_segment → last_segment.xml
        if pid.startswith("urn:") and ext.lower() == "xml":
            return "urn_xml"

        # Exact match: product_id + .EXT
        if stem == pid:
            return ext

        # Case-insensitive match
        if stem.lower() == pid.lower():
            return f"ci_{ext}"

        # Product_id has a prefix that gets stripped
        # e.g. "CIRS-HSK06072016" → "HSK06072016.LBL"
        if "-" in pid:
            _, suffix = pid.split("-", 1)
            if stem.lower() == suffix.lower():
                return f"strip_dash.{ext}"

        # Product_id starts with label stem (version/suffix appended to pid)
        # e.g. "A17_TRASVERSE...-201111" → "a17_traverse...LBL"
        # This is too unpredictable — skip

    # Default: assume product_id.LBL (most common PDS3 pattern)
    return "LBL"


def _derive_label_file(product_id: str, rule: str) -> str:
    """Derive the label filename from a product_id using the detected rule."""
    # PDS4 URN: extract last segment after final colon
    if rule == "urn_xml":
        segment = product_id.rsplit(":", 1)[-1] if ":" in product_id else product_id
        return f"{segment}.xml"

    # Strip dash prefix: "CIRS-HSK06072016" → "HSK06072016.LBL"
    if rule.startswith("strip_dash."):
        ext = rule.split(".", 1)[1]
        if "-" in product_id:
            _, stem = product_id.split("-", 1)
            return f"{stem}.{ext}"
        return f"{product_id}.{ext}"

    # Case-insensitive: use product_id as-is with detected extension
    if rule.startswith("ci_"):
        ext = rule[3:]
        return f"{product_id}.{ext}"

    # Standard: product_id + .extension
    return f"{product_id}.{rule}"


def _build_file_list(label_file: str) -> list[str]:
    """Build the expected file list from a label file.

    PDS products typically have a label + companion data file.
    """
    files = [label_file]

    ext_upper = PurePosixPath(label_file).suffix.upper()
    stem = PurePosixPath(label_file).stem

    if ext_upper == ".LBL":
        # PDS3: add most common data companion
        files.append(f"{stem}.IMG")
    elif ext_upper == ".XML":
        # PDS4: data file could be various formats
        files.append(f"{stem}.dat")

    return files
