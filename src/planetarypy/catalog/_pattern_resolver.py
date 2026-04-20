"""Pattern-based URL resolution and fetchability classification.

For product types where the url_stem is the same for all products, we can
resolve ANY product_id by looking up the fixed url_stem from a sample
product and deriving the filename.

Also provides fetchability analysis: classifies each product type as
'fixed', 'indexed', or 'unfetchable' based on URL stem variability.
"""

from __future__ import annotations

import json as _json
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import PurePosixPath
from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from planetarypy.catalog._resolver import ResolvedProduct


# ── File inference from catalog metadata ─────────────────────────────


def get_product_type_file_info(
    mission: str, instrument: str, product_key: str,
) -> tuple[str, str | None, str | None]:
    """Look up file extension and label convention for a product type.

    Uses fn_must_contain, fn_ends_with, and label_type from the catalog DB
    to determine what files a product consists of.

    Returns
    -------
    tuple[str, str | None, str | None]
        (label_type, data_extension, label_extension)
        - label_type: 'D' (detached), 'A' (attached), 'NA', or list-like string
        - data_extension: e.g. '.IMG', '.TAB', '.DAT' (from fn_must_contain/fn_ends_with)
        - label_extension: e.g. '.LBL', '.xml' (derived from label_type)
    """
    from planetarypy.catalog import get_catalog

    con = get_catalog()
    row = con.execute(
        """SELECT pt.label_type, pt.fn_must_contain, pt.fn_ends_with
           FROM product_types pt
           JOIN instruments i USING (folder_name)
           WHERE i.mission = ? AND i.instrument = ?
             AND (pt.normalized_type = ? OR pt.product_key = ?)
           LIMIT 1""",
        [mission, instrument, product_key, product_key],
    ).fetchone()

    if row is None:
        return "D", None, ".LBL"

    label_type, fn_must_contain_json, fn_ends_with_json = row

    # Extract data file extension from fn_ends_with or fn_must_contain
    data_ext = None
    if fn_ends_with_json:
        try:
            ends_with = _json.loads(fn_ends_with_json)
            if ends_with:
                data_ext = ends_with[0]
        except (ValueError, TypeError):
            pass

    if data_ext is None and fn_must_contain_json:
        try:
            must_contain = _json.loads(fn_must_contain_json)
            for item in must_contain:
                if item.startswith("."):
                    data_ext = item
                    break
        except (ValueError, TypeError):
            pass

    # Determine label extension from label_type
    label_ext = None
    if label_type == "D":
        label_ext = ".LBL"
    elif label_type == "A":
        label_ext = None
    elif label_type == "NA":
        label_ext = None
    elif label_type.startswith("["):
        try:
            parts = _json.loads(label_type.replace("\\\\", "\\"))
        except (ValueError, TypeError):
            inner = label_type.strip("[]")
            parts = [p.strip() for p in inner.split(",")]
        if len(parts) >= 2:
            label_ext = parts[-1].strip()
            if data_ext is None:
                data_ext = parts[0].strip()

    return label_type, data_ext, label_ext


def build_file_list_from_type(
    product_id: str,
    mission: str,
    instrument: str,
    product_key: str,
    file_spec_name: str | None = None,
) -> tuple[list[str], str | None]:
    """Build the file list for a product using catalog product type metadata.

    Uses label_type and fn_must_contain/fn_ends_with from the DB to determine
    the correct file extensions instead of guessing.

    Parameters
    ----------
    product_id : str
        The product identifier
    mission, instrument, product_key : str
        Product type coordinates
    file_spec_name : str | None
        If available (from an index), the FILE_SPECIFICATION_NAME — used
        to extract the actual filename stem which may differ from product_id.

    Returns
    -------
    tuple[list[str], str | None]
        (files, label_file) — list of filenames and the label filename
    """
    label_type, data_ext, label_ext = get_product_type_file_info(
        mission, instrument, product_key,
    )

    if file_spec_name:
        stem = PurePosixPath(file_spec_name).stem
    elif product_id.startswith("urn:"):
        stem = product_id.rsplit(":", 1)[-1]
    else:
        stem = product_id

    files = []
    label_file = None

    if label_type == "A":
        if data_ext:
            files.append(f"{stem}{data_ext}")
        else:
            files.append(f"{stem}.LBL")
    elif label_type == "NA":
        if data_ext:
            files.append(f"{stem}{data_ext}")
    elif label_type.startswith("["):
        if data_ext:
            files.append(f"{stem}{data_ext}")
        if label_ext:
            label_file = f"{stem}{label_ext}"
            files.append(label_file)
    else:
        if data_ext:
            files.append(f"{stem}{data_ext}")
        if label_ext:
            label_file = f"{stem}{label_ext}"
            files.append(label_file)

    return files, label_file


# ── Pattern-based resolution (Tier 3) ───────────────────────────────


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

    Returns None if no samples exist, the url_stem varies, or the
    label filename cannot be derived.
    """
    from planetarypy.catalog._resolver import ResolvedProduct

    pattern = _get_url_pattern(mission, instrument, product_key)
    if pattern is None:
        return None

    url_stem = pattern[0]
    files, label_file = build_file_list_from_type(
        product_id, mission, instrument, product_key,
    )

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


@lru_cache(maxsize=256)
def _get_url_pattern(
    mission: str, instrument: str, product_key: str,
) -> tuple[str, str] | None:
    """Learn the URL pattern for a product type from catalog samples.

    Returns (url_stem, label_rule) or None.
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
    """Detect how label_file relates to product_id from sample data."""
    for _url, pid, label in rows:
        if not pid or not label:
            continue

        ext = label.rsplit(".", 1)[-1] if "." in label else ""
        stem = label.rsplit(".", 1)[0] if "." in label else label

        if pid.startswith("urn:") and ext.lower() == "xml":
            return "urn_xml"
        if stem == pid:
            return ext
        if stem.lower() == pid.lower():
            return f"ci_{ext}"
        if "-" in pid:
            _, suffix = pid.split("-", 1)
            if stem.lower() == suffix.lower():
                return f"strip_dash.{ext}"

    return "LBL"


def _derive_label_file(product_id: str, rule: str) -> str:
    """Derive the label filename from a product_id using the detected rule."""
    if rule == "urn_xml":
        segment = product_id.rsplit(":", 1)[-1] if ":" in product_id else product_id
        return f"{segment}.xml"

    if rule.startswith("strip_dash."):
        ext = rule.split(".", 1)[1]
        if "-" in product_id:
            _, stem = product_id.split("-", 1)
            return f"{stem}.{ext}"
        return f"{product_id}.{ext}"

    if rule.startswith("ci_"):
        ext = rule[3:]
        return f"{product_id}.{ext}"

    return f"{product_id}.{rule}"


def _build_file_list(label_file: str) -> list[str]:
    """Build the expected file list from a label file."""
    files = [label_file]
    ext_upper = PurePosixPath(label_file).suffix.upper()
    stem = PurePosixPath(label_file).stem

    if ext_upper == ".LBL":
        files.append(f"{stem}.IMG")
    elif ext_upper == ".XML":
        files.append(f"{stem}.dat")

    return files


# ── Fetchability classification ──────────────────────────────────────

# Patterns that indicate a path segment is variable (volume, date, orbit)

_VOLUME_RE = re.compile(
    r"^(?:"
    r"CO[A-Z]{2,4}_\d{4}"
    r"|LROL[A-Z]{2}_\d{4}"
    r"|[a-z]{2,6}[-_]\d{4}[a-z]?"
    r"|mg[-_]\d{4}"
    r"|go[-_]\d{4}"
    r"|vl[-_]\d{4}"
    r"|sl9[-_]\d{4}"
    r"|VOL[-_]?\d{3,}"
    r")$",
    re.IGNORECASE,
)

_DATE_RE = re.compile(
    r"^(?:"
    r"sol\d{3,5}"
    r"|\d{8}"
    r"|\d{4}\d{3}(?!\d)"
    r"|\d{7,}_\d{7,}"
    r"|\d{6}_\d{6}"
    r")$",
    re.IGNORECASE,
)

_ORBIT_RE = re.compile(
    r"^(?:"
    r"ORB[-_]\d{3,}"
    r"|ORB[-_]\d+[-_]\d+"
    r"|orb\d{3,}x?"
    r"|eid\d{4,}"
    r")$",
    re.IGNORECASE,
)

_NUMBERED_DIR_RE = re.compile(
    r"^(?:"
    r"[a-z]\d{5,}"
    r"|[a-z]\d{4}[a-z]"
    r")$",
    re.IGNORECASE,
)


@dataclass
class ProductTypeFetchability:
    """Classification of a product type's URL resolution capability."""

    mission: str
    instrument: str
    product_key: str
    status: str  # 'fixed', 'indexed', 'unfetchable'
    n_samples: int
    n_stems: int
    has_index: bool
    variable_segments: list[str]
    pid_contains_variable: bool
    reason: str


def classify_product_type(
    mission: str, instrument: str, product_key: str,
) -> ProductTypeFetchability:
    """Classify a single product type's fetchability.

    Returns
    -------
    ProductTypeFetchability
        Status is one of 'fixed', 'indexed', 'unfetchable'.
    """
    from planetarypy.catalog import get_catalog
    from planetarypy.catalog._index_resolver import has_index as _has_index

    con = get_catalog()
    rows = con.execute(
        """SELECT p.url_stem, p.product_id
           FROM products p
           JOIN product_types pt USING (folder_name, product_key)
           JOIN instruments i USING (folder_name)
           WHERE i.mission = ? AND i.instrument = ?
             AND (pt.normalized_type = ? OR pt.product_key = ?)
             AND p.url_stem IS NOT NULL AND p.url_stem <> ''
           LIMIT 50""",
        [mission, instrument, product_key, product_key],
    ).fetchall()

    has_idx = _has_index(mission, instrument, product_key)
    n_samples = len(rows)

    if n_samples == 0:
        return ProductTypeFetchability(
            mission=mission, instrument=instrument, product_key=product_key,
            status="unfetchable", n_samples=0, n_stems=0,
            has_index=has_idx, variable_segments=[], pid_contains_variable=False,
            reason="No sample products with URLs in catalog",
        )

    stems = list({r[0] for r in rows})
    n_stems = len(stems)

    if n_samples > 1 and n_stems == 1:
        return ProductTypeFetchability(
            mission=mission, instrument=instrument, product_key=product_key,
            status="fixed", n_samples=n_samples, n_stems=1,
            has_index=has_idx, variable_segments=[], pid_contains_variable=False,
            reason="All samples share the same url_stem",
        )

    if n_stems > 1:
        var_segs = _find_variable_segments(stems)
        pid_has_var = _pid_contains_variable_info(rows, stems)

        if has_idx:
            return ProductTypeFetchability(
                mission=mission, instrument=instrument, product_key=product_key,
                status="indexed", n_samples=n_samples, n_stems=n_stems,
                has_index=True, variable_segments=var_segs,
                pid_contains_variable=pid_has_var,
                reason="Variable url_stem but PDS index is registered",
            )

        return ProductTypeFetchability(
            mission=mission, instrument=instrument, product_key=product_key,
            status="unfetchable", n_samples=n_samples, n_stems=n_stems,
            has_index=False, variable_segments=var_segs,
            pid_contains_variable=pid_has_var,
            reason=f"Variable url_stem ({n_stems} distinct), no index available",
        )

    # Single sample: heuristic analysis
    stem = stems[0]
    suspicious_segs = _detect_variable_segments_in_path(stem)

    if not suspicious_segs:
        return ProductTypeFetchability(
            mission=mission, instrument=instrument, product_key=product_key,
            status="fixed", n_samples=1, n_stems=1,
            has_index=has_idx, variable_segments=[], pid_contains_variable=False,
            reason="Single sample, no variable-looking path segments detected",
        )

    pid = rows[0][1]
    pid_has_var = any(
        _segment_appears_in_pid(seg, pid) for seg in suspicious_segs
    )

    if has_idx:
        return ProductTypeFetchability(
            mission=mission, instrument=instrument, product_key=product_key,
            status="indexed", n_samples=1, n_stems=1,
            has_index=True, variable_segments=suspicious_segs,
            pid_contains_variable=pid_has_var,
            reason="Single sample with variable-looking path, but PDS index registered",
        )

    return ProductTypeFetchability(
        mission=mission, instrument=instrument, product_key=product_key,
        status="unfetchable", n_samples=1, n_stems=1,
        has_index=False, variable_segments=suspicious_segs,
        pid_contains_variable=pid_has_var,
        reason=(
            f"Single sample, path contains variable-looking segments "
            f"({', '.join(suspicious_segs)}), no index available"
        ),
    )


def classify_all() -> list[ProductTypeFetchability]:
    """Classify all product types in the catalog."""
    from planetarypy.catalog import get_catalog

    con = get_catalog()
    types = con.execute("""
        SELECT DISTINCT i.mission, i.instrument, pt.product_key
        FROM product_types pt
        JOIN instruments i USING (folder_name)
        ORDER BY i.mission, i.instrument, pt.product_key
    """).fetchall()

    results = []
    for mission, instrument, product_key in types:
        results.append(classify_product_type(mission, instrument, product_key))

    order = {"fixed": 0, "indexed": 1, "unfetchable": 2}
    results.sort(key=lambda r: (order.get(r.status, 9), r.mission, r.instrument))
    return results


def fetchability_summary() -> dict[str, int]:
    """Return counts of product types by fetchability status."""
    results = classify_all()
    counts: dict[str, int] = {}
    for r in results:
        counts[r.status] = counts.get(r.status, 0) + 1
    return counts


# ── Internal helpers ─────────────────────────────────────────────────


def _find_variable_segments(stems: list[str]) -> list[str]:
    """Find which path segments differ across multiple url_stems."""
    parts_list = [s.split("/") for s in stems]
    min_len = min(len(p) for p in parts_list)

    variable = []
    for i in range(min_len):
        vals = {p[i] for p in parts_list}
        if len(vals) > 1:
            examples = sorted(vals)[:3]
            variable.append(f"seg[{i}]: {', '.join(examples)}")
    return variable


def _detect_variable_segments_in_path(url_stem: str) -> list[str]:
    """Heuristically detect variable-looking segments in a single URL path."""
    from urllib.parse import urlparse
    parsed = urlparse(url_stem)
    segments = [s for s in parsed.path.split("/") if s]

    suspicious = []
    for seg in segments:
        if _VOLUME_RE.match(seg):
            suspicious.append(f"volume:{seg}")
        elif _DATE_RE.match(seg):
            suspicious.append(f"date:{seg}")
        elif _ORBIT_RE.match(seg):
            suspicious.append(f"orbit:{seg}")
        elif _NUMBERED_DIR_RE.match(seg):
            suspicious.append(f"numbered:{seg}")

    return suspicious


def _pid_contains_variable_info(
    rows: list[tuple[str, str]], stems: list[str],
) -> bool:
    """Check if product IDs contain information matching variable path segments."""
    parts_list = [s.split("/") for s in stems]
    min_len = min(len(p) for p in parts_list)

    variable_indices = []
    for i in range(min_len):
        vals = {p[i] for p in parts_list}
        if len(vals) > 1:
            if all(_VOLUME_RE.match(v) for v in vals):
                continue
            variable_indices.append(i)

    if not variable_indices:
        return False

    for row_stem, pid in rows[:5]:
        row_parts = row_stem.split("/")
        for i in variable_indices:
            if i < len(row_parts):
                if _segment_appears_in_pid(row_parts[i], pid):
                    return True
    return False


def _segment_appears_in_pid(segment: str, product_id: str) -> bool:
    """Check if a path segment's content appears in the product_id."""
    seg_lower = segment.lower()
    pid_lower = product_id.lower()

    if _VOLUME_RE.match(segment):
        return False

    if len(seg_lower) >= 4 and seg_lower in pid_lower:
        return True
    if len(pid_lower) >= 4 and pid_lower in seg_lower:
        return True

    date_match = _DATE_RE.match(segment)
    if date_match:
        core_nums = re.findall(r"\d{4,}", segment)
        pid_nums = re.findall(r"\d{4,}", product_id)
        if core_nums and pid_nums and set(core_nums) & set(pid_nums):
            return True

    return False
