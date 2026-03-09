"""Bridge between PDS catalog and PDS index system for Tier 2 resolution.

For instruments that have registered PDS cumulative indexes, this module
enables resolving arbitrary product IDs (not just the ~1,948 sample
products in the catalog DB).

Flow:
    1. Look up (mission, instrument, product_key) in INDEX_REGISTRY → IndexConfig
    2. Load the index DataFrame via planetarypy.pds.get_index()
    3. Search for product_id in the configured column
    4. Construct download URL from volume_id + file_spec + archive base
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import TYPE_CHECKING

import pandas as pd
from loguru import logger

if TYPE_CHECKING:
    from planetarypy.catalog._download import ResolvedProduct

SETI_VOLUMES_BASE = "https://pds-rings.seti.org/holdings/volumes"


@dataclass(frozen=True)
class IndexConfig:
    """Configuration for resolving products via a PDS cumulative index."""

    index_key: str
    """PDS index dotted key (e.g. 'mro.ctx.edr')."""

    product_id_col: str = "PRODUCT_ID"
    """Column to match the requested product_id against."""

    file_spec_col: str = "FILE_SPECIFICATION_NAME"
    """Column with the file path relative to volume root."""

    volume_id_col: str = "VOLUME_ID"
    """Column with the volume identifier."""

    archive_url: str = ""
    """Base URL for standard archives.

    Product URL = {archive_url}/{volume_id}/{file_spec_dir}
    Leave empty for SETI Rings archives (use seti_volume_group instead).
    """

    seti_volume_group: str = ""
    """For SETI Rings archives: volume group (e.g. 'COISS_2xxx').

    Product URL = https://pds-rings.seti.org/holdings/volumes/{group}/{volume}/{dir}
    """


# ── Registry ──────────────────────────────────────────────────────────
# Maps catalog (mission, instrument, product_key) → IndexConfig.
# Every entry is explicit — no defaults, no fallbacks.

INDEX_REGISTRY: dict[tuple[str, str, str], IndexConfig] = {
    # ── MRO ──
    ("mro", "ctx", "edr"): IndexConfig(
        index_key="mro.ctx.edr",
        archive_url="https://planetarydata.jpl.nasa.gov/img/data/mro/ctx",
    ),
    ("mro", "hirise", "edr"): IndexConfig(
        index_key="mro.hirise.edr",
        archive_url="https://hirise-pds.lpl.arizona.edu/PDS",
    ),
    ("mro", "hirise", "rdr"): IndexConfig(
        index_key="mro.hirise.rdr",
        archive_url="https://hirise-pds.lpl.arizona.edu/PDS",
    ),
    ("mro", "hirise", "dtm"): IndexConfig(
        index_key="mro.hirise.dtm",
        archive_url="https://hirise-pds.lpl.arizona.edu/PDS",
    ),
    ("mro", "crism", "mtrdr"): IndexConfig(
        index_key="mro.crism.mtrdr",
        archive_url=(
            "https://pds-geosciences.wustl.edu/mro/"
            "mro-m-crism-5-rdr-mptargeted-v1"
        ),
    ),
    # ── LRO ──
    ("lro", "lroc", "edr"): IndexConfig(
        index_key="lro.lroc.edr",
        archive_url="https://pds.lroc.asu.edu/data/LRO-L-LROC-2-EDR-V1.0",
    ),
    ("lro", "diviner", "edr"): IndexConfig(
        index_key="lro.diviner.edr1",
        archive_url=(
            "https://pds-geosciences.wustl.edu/lro/lro-l-dlre-2-edr-v1"
        ),
    ),
    ("lro", "diviner", "rdr"): IndexConfig(
        index_key="lro.diviner.rdr1",
        archive_url=(
            "https://pds-geosciences.wustl.edu/lro/lro-l-dlre-4-rdr-v1"
        ),
    ),
    ("lro", "lola", "edr"): IndexConfig(
        index_key="lro.lola.edr",
        archive_url=(
            "https://pds-geosciences.wustl.edu/lro/lro-l-lola-2-edr-v1"
        ),
    ),
    ("lro", "lola", "rdr"): IndexConfig(
        index_key="lro.lola.rdr",
        archive_url=(
            "https://pds-geosciences.wustl.edu/lro/lro-l-lola-3-rdr-v1"
        ),
    ),
    # ── Cassini (SETI Rings) ──
    ("cassini", "iss", "edr_sat"): IndexConfig(
        index_key="cassini.iss.index",
        seti_volume_group="COISS_2xxx",
    ),
    ("cassini", "uvis", "edr"): IndexConfig(
        index_key="cassini.uvis.index",
        seti_volume_group="COUVIS_0xxx",
    ),
    # ── Galileo (catalog uses 'galileo', index uses 'go') ──
    ("galileo", "ssi", "edr"): IndexConfig(
        index_key="go.ssi.index",
        seti_volume_group="GO_0xxx",
    ),
    # ── MER ──
    ("mer_spirit", "pancam", "rdr"): IndexConfig(
        index_key="mer.spirit.pancam_rdr",
        archive_url=(
            "https://pds-geosciences.wustl.edu/mer/"
            "mer2-m-pancam-3-radcal-sci-v2"
        ),
    ),
    ("mer_opportunity", "pancam", "rdr"): IndexConfig(
        index_key="mer.opportunity.pancam_rdr",
        archive_url=(
            "https://pds-geosciences.wustl.edu/mer/"
            "mer1-m-pancam-3-radcal-sci-v2"
        ),
    ),
}


# ── Public API ────────────────────────────────────────────────────────


def get_index_config(
    mission: str,
    instrument: str,
    product_key: str,
) -> IndexConfig | None:
    """Look up the IndexConfig for a specific product type."""
    return INDEX_REGISTRY.get((mission, instrument, product_key))


def has_index(mission: str, instrument: str, product_key: str) -> bool:
    """Check if a PDS index is registered for this product type."""
    return (mission, instrument, product_key) in INDEX_REGISTRY


def list_indexed_products() -> list[tuple[str, str, str, str]]:
    """List all product types with registered PDS indexes.

    Returns list of (mission, instrument, product_key, index_key) tuples.
    """
    return [
        (m, i, pk, cfg.index_key)
        for (m, i, pk), cfg in INDEX_REGISTRY.items()
    ]


def resolve_from_index(
    mission: str,
    instrument: str,
    product_key: str,
    product_id: str,
) -> ResolvedProduct | None:
    """Resolve a product via PDS index lookup (Tier 2).

    Returns None if no index is available, not downloaded, or the product
    isn't found. Never raises — failures are logged and None returned.
    """
    from planetarypy.catalog._download import ResolvedProduct

    config = get_index_config(mission, instrument, product_key)
    if not config:
        return None

    try:
        df = _load_index_df(config)
    except Exception as e:
        logger.warning(f"Could not load index {config.index_key}: {e}")
        return None

    row = _find_product_in_index(df, product_id, config)
    if row is None:
        return None

    url_stem = _build_url_stem(row, config)
    files, label_file = _extract_files(row, config)

    if not url_stem or not files:
        logger.warning(
            f"Could not construct URL for '{product_id}' "
            f"from index {config.index_key}"
        )
        return None

    return ResolvedProduct(
        product_id=product_id,
        url_stem=url_stem,
        files=files,
        label_file=label_file,
        source="index",
    )


# ── Internal helpers ──────────────────────────────────────────────────


def _load_index_df(config: IndexConfig) -> pd.DataFrame:
    """Load the index DataFrame, downloading if necessary."""
    from planetarypy.pds import get_index

    return get_index(config.index_key)


def _find_product_in_index(
    df: pd.DataFrame,
    product_id: str,
    config: IndexConfig,
) -> pd.Series | None:
    """Search for a product in the index DataFrame.

    Tries the configured column first, then common fallbacks.
    Handles PDS whitespace-padded strings and case differences.
    """
    pid = product_id.strip()

    # Build ordered list of columns to try
    cols = [config.product_id_col]
    for fallback in ("PRODUCT_ID", "FILE_NAME", "IMAGE_ID", "OBSERVATION_ID"):
        if fallback not in cols and fallback in df.columns:
            cols.append(fallback)

    for col in cols:
        if col not in df.columns:
            continue

        series = df[col].astype(str).str.strip()

        # Exact match
        mask = series == pid
        if mask.any():
            return df.loc[mask].iloc[-1]

        # Case-insensitive match
        mask = series.str.upper() == pid.upper()
        if mask.any():
            return df.loc[mask].iloc[-1]

    return None


def _build_url_stem(row: pd.Series, config: IndexConfig) -> str:
    """Construct the URL directory for a product from its index row."""
    volume_id = _get_column(row, config.volume_id_col, "VOLUME_ID")
    file_spec = _get_column(
        row, config.file_spec_col, "FILE_SPECIFICATION_NAME", "FILE_NAME"
    )

    if not volume_id or not file_spec:
        return ""

    # Directory containing the product file
    file_dir = str(PurePosixPath(file_spec).parent)
    if file_dir == ".":
        file_dir = ""

    if config.seti_volume_group:
        parts = [
            SETI_VOLUMES_BASE,
            config.seti_volume_group,
            volume_id,
        ]
        if file_dir:
            parts.append(file_dir)
        return "/".join(parts)
    else:
        parts = [config.archive_url.rstrip("/"), volume_id]
        if file_dir:
            parts.append(file_dir)
        return "/".join(parts)


def _extract_files(
    row: pd.Series, config: IndexConfig
) -> tuple[list[str], str | None]:
    """Extract file list and label file from an index row.

    PDS products typically have a label (.LBL) and data file(s).
    The index FILE_SPECIFICATION_NAME points to one of them.
    """
    file_spec = _get_column(
        row, config.file_spec_col, "FILE_SPECIFICATION_NAME", "FILE_NAME"
    )
    if not file_spec:
        return [], None

    main_file = PurePosixPath(file_spec).name
    ext_upper = PurePosixPath(main_file).suffix.upper()
    stem = PurePosixPath(main_file).stem

    files = [main_file]
    label_file = None

    if ext_upper in (".LBL", ".XML"):
        # Label file — look for companion data file
        label_file = main_file
        for data_ext in (".IMG", ".DAT", ".TAB", ".FIT", ".JP2"):
            companion = f"{stem}{data_ext}"
            files.append(companion)
            break  # add the most common one
    elif ext_upper in (".IMG", ".DAT", ".FIT", ".JP2"):
        # Data file — add companion label
        label_candidate = f"{stem}.LBL"
        files.append(label_candidate)
        label_file = label_candidate

    return files, label_file


def _get_column(row: pd.Series, *col_names: str) -> str:
    """Get the first available column value from a row."""
    for col in col_names:
        if col in row.index:
            val = str(row[col]).strip()
            if val and val.lower() != "nan":
                return val
    return ""
