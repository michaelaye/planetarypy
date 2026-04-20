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
    from planetarypy.catalog._resolver import ResolvedProduct

SETI_VOLUMES_BASE = "https://pds-rings.seti.org/holdings/volumes"


@dataclass(frozen=True)
class IndexConfig:
    """Configuration for resolving products via a PDS cumulative index."""

    index_key: str
    """Primary PDS index dotted key (e.g. 'mro.ctx.edr')."""

    extra_index_keys: tuple[str, ...] = ()
    """Additional index keys to concatenate with the primary.

    Used when an archive splits the same data set across multiple volumes
    with separate index files (e.g. Diviner edr1 + edr2).
    """

    product_id_col: str = "PRODUCT_ID"
    """Column to match the requested product_id against."""

    file_spec_col: str = "FILE_SPECIFICATION_NAME"
    """Column with the file path relative to volume root."""

    volume_id_col: str = "VOLUME_ID"
    """Column with the volume identifier."""

    path_name_col: str = ""
    """Column with the subdirectory path (e.g. 'DATA/SOL073/').

    Used when the index has separate FILE_NAME and PATH_NAME columns
    instead of a single FILE_SPECIFICATION_NAME with the full relative path.
    """

    archive_url: str = ""
    """Base URL for standard archives.

    Product URL = {archive_url}/{volume_id}/{file_spec_dir}
    Leave empty for SETI Rings archives (use seti_volume_group instead).
    """

    seti_volume_group: str = ""
    """For SETI Rings archives: volume group (e.g. 'COISS_2xxx').

    Set to 'auto' to derive from the volume ID (e.g. COCIRS_1002 →
    COCIRS_1xxx).  Explicit values like 'COISS_2xxx' are used as-is.

    Product URL = https://pds-rings.seti.org/holdings/volumes/{group}/{volume}/{dir}
    """

    lowercase_paths: bool = True
    """Lowercase volume_id and file path components in URLs.

    Most PDS archive servers expect lowercase paths.  Set to False for
    servers that require the original case (e.g. SETI Rings).
    """

    lowercase_files: bool = False
    """Lowercase filenames in URLs.

    A few archives (e.g. MGS MOC on JPL) have all-lowercase filenames
    even though the index records them in uppercase.
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
        file_spec_col="FILE_NAME_SPECIFICATION",
        volume_id_col="",
        lowercase_paths=False,
    ),
    ("mro", "hirise", "rdr"): IndexConfig(
        index_key="mro.hirise.rdr",
        archive_url="https://hirise-pds.lpl.arizona.edu/PDS",
        file_spec_col="FILE_NAME_SPECIFICATION",
        volume_id_col="",
        lowercase_paths=False,
    ),
    ("mro", "hirise", "dtm"): IndexConfig(
        index_key="mro.hirise.dtm",
        archive_url="https://hirise-pds.lpl.arizona.edu/PDS",
        file_spec_col="FILE_NAME_SPECIFICATION",
        volume_id_col="",
        lowercase_paths=False,
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
        archive_url="https://pds.lroc.asu.edu/data",
        volume_id_col="",
        lowercase_paths=False,
    ),
    ("lro", "diviner", "edr"): IndexConfig(
        index_key="lro.diviner.edr1",
        extra_index_keys=("lro.diviner.edr2",),
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://pds-geosciences.wustl.edu/lro/lro-l-dlre-2-edr-v1"
        ),
    ),
    ("lro", "diviner", "rdr"): IndexConfig(
        index_key="lro.diviner.rdr1",
        extra_index_keys=("lro.diviner.rdr2",),
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://pds-geosciences.wustl.edu/lro/lro-l-dlre-4-rdr-v1"
        ),
    ),
    # LAMP indexes lack VOLUME_ID — the archive uses per-volume directories
    # (LROLAM_1001, LROLAM_1002, …) that can't be derived from the index.
    # Commented out until volume mapping is available.
    # ("lro", "lamp", "edr"): IndexConfig(
    #     index_key="lro.lamp.edr",
    #     ...
    # ),
    # ("lro", "lamp", "rdr"): IndexConfig(
    #     index_key="lro.lamp.rdr",
    #     ...
    # ),
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
    ("cassini", "iss", "edr_evj"): IndexConfig(
        index_key="cassini.iss_cruise.index",
        seti_volume_group="COISS_1xxx",
    ),
    ("cassini", "cirs", "jupiter"): IndexConfig(
        index_key="cassini.cirs.cube_point_index",
        seti_volume_group="auto",
    ),
    ("cassini", "occultation", "rss"): IndexConfig(
        index_key="cassini.rss.index",
        seti_volume_group="CORSS_8xxx",
    ),
    ("cassini", "uvis", "edr"): IndexConfig(
        index_key="cassini.uvis.index",
        product_id_col="FILE_NAME",
        file_spec_col="FILE_NAME",
        volume_id_col="",
        seti_volume_group="COUVIS_0xxx",
    ),
    ("cassini", "vims", "edr"): IndexConfig(
        index_key="cassini.vims.index",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        seti_volume_group="COVIMS_0xxx",
    ),
    # ── Galileo (catalog uses 'galileo', index uses 'go') ──
    ("galileo", "ssi", "edr"): IndexConfig(
        index_key="go.ssi.index",
        seti_volume_group="GO_0xxx",
    ),
    # ── Voyager (SETI Rings) ──
    ("voyager1", "iss", "edr"): IndexConfig(
        index_key="voyager1.iss.index",
        volume_id_col="VOLUME_NAME",
        seti_volume_group="VGISS_5xxx",
    ),
    ("voyager2", "iss", "edr"): IndexConfig(
        index_key="voyager2.iss.index",
        volume_id_col="VOLUME_NAME",
        seti_volume_group="auto",
    ),
    # ── Juno (SETI Rings) ──
    ("juno", "junocam", "edr"): IndexConfig(
        index_key="juno.junocam.index",
        seti_volume_group="JNOJNC_0xxx",
    ),
    # ── New Horizons (SETI Rings) ──
    ("new_horizons", "lorri", "edr"): IndexConfig(
        index_key="new_horizons.lorri.edr",
        seti_volume_group="NHxxLO_xxxx",
    ),
    ("new_horizons", "lorri", "rdr"): IndexConfig(
        index_key="new_horizons.lorri.rdr",
        seti_volume_group="NHxxLO_xxxx",
    ),
    # ── MGS ──
    ("mgs", "moc", "edr"): IndexConfig(
        index_key="mgs.moc.edr",
        archive_url="https://planetarydata.jpl.nasa.gov/img/data/mgs/moc",
        lowercase_files=True,
    ),
    ("mgs", "moc", "rdr"): IndexConfig(
        index_key="mgs.moc.rdr",
        archive_url="https://planetarydata.jpl.nasa.gov/img/data/mgs/moc/rdr",
        lowercase_files=True,
    ),
    # ── Viking Orbiter ──
    # vo_1064 cumindex covers both VO1 and VO2 (~50,500 images)
    ("viking", "vis", "edr"): IndexConfig(
        index_key="viking.vis.edr",
        archive_url=(
            "https://planetarydata.jpl.nasa.gov/img/data/viking/viking_orbiter"
        ),
    ),
    # ── MESSENGER ──
    ("messenger", "mdis", "edr"): IndexConfig(
        index_key="messenger.mdis.edr",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://planetarydata.jpl.nasa.gov/img/data/messenger"
        ),
        lowercase_paths=False,
    ),
    ("messenger", "mdis", "cdr"): IndexConfig(
        index_key="messenger.mdis.cdr",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://planetarydata.jpl.nasa.gov/img/data/messenger"
        ),
        lowercase_paths=False,
    ),
    # ── Phoenix (MECA instruments share one index, filter by INSTRUMENT_ID) ──
    ("phoenix", "wcl", "edr"): IndexConfig(
        index_key="phoenix.meca.edr",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://pds-geosciences.wustl.edu/phx/"
            "phx-m-meca-2-niedr-v1"
        ),
    ),
    ("phoenix", "wcl", "rdr"): IndexConfig(
        index_key="phoenix.meca.rdr",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://pds-geosciences.wustl.edu/phx/"
            "phx-m-meca-4-nirdr-v1"
        ),
    ),
    ("phoenix", "tecp", "rdr"): IndexConfig(
        index_key="phoenix.meca.rdr",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://pds-geosciences.wustl.edu/phx/"
            "phx-m-meca-4-nirdr-v1"
        ),
    ),
    ("phoenix", "elec", "edr"): IndexConfig(
        index_key="phoenix.meca.edr",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://pds-geosciences.wustl.edu/phx/"
            "phx-m-meca-2-niedr-v1"
        ),
    ),
    ("phoenix", "afm", "edr"): IndexConfig(
        index_key="phoenix.meca.edr",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://pds-geosciences.wustl.edu/phx/"
            "phx-m-meca-2-niedr-v1"
        ),
    ),
    ("phoenix", "afm", "rdr"): IndexConfig(
        index_key="phoenix.meca.rdr",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://pds-geosciences.wustl.edu/phx/"
            "phx-m-meca-4-nirdr-v1"
        ),
    ),
    # ── MER ──
    # ── MSL (single-volume archives — index.tab IS the cumulative index) ──
    # All WUSTL-hosted archives use FILE_NAME + PATH_NAME columns.
    # APXS
    ("msl", "apxs", "APXS_SCIENCE_EDR"): IndexConfig(
        index_key="msl.apxs.edr",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://pds-geosciences.wustl.edu/msl/msl-m-apxs-2-edr-v1"
        ),
    ),
    ("msl", "apxs", "APXS_OXIDE_RDR"): IndexConfig(
        index_key="msl.apxs.rdr",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://pds-geosciences.wustl.edu/msl/msl-m-apxs-4_5-rdr-v1"
        ),
    ),
    ("msl", "apxs", "APXS_SPECTRUM_RDR"): IndexConfig(
        index_key="msl.apxs.rdr",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://pds-geosciences.wustl.edu/msl/msl-m-apxs-4_5-rdr-v1"
        ),
    ),
    # ChemCam EDR (shared index covers LIBS+RMI+SOH)
    ("msl", "ccam", "CCAM_LIBS_EDR"): IndexConfig(
        index_key="msl.ccam.edr",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://pds-geosciences.wustl.edu/msl/"
            "msl-m-chemcam-libs-2-edr-v1"
        ),
    ),
    ("msl", "ccam", "CCAM_RMI_EDR"): IndexConfig(
        index_key="msl.ccam.edr",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://pds-geosciences.wustl.edu/msl/"
            "msl-m-chemcam-libs-2-edr-v1"
        ),
    ),
    ("msl", "ccam", "CCAM_SOH_EDR"): IndexConfig(
        index_key="msl.ccam.edr",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://pds-geosciences.wustl.edu/msl/"
            "msl-m-chemcam-libs-2-edr-v1"
        ),
    ),
    # ChemCam RDR (separate LIBS and RMI indexes)
    ("msl", "ccam", "CCAM_LIBS_L1B"): IndexConfig(
        index_key="msl.ccam.libs_rdr",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://pds-geosciences.wustl.edu/msl/"
            "msl-m-chemcam-libs-4_5-rdr-v1"
        ),
    ),
    ("msl", "ccam", "CCAM_LIBS_L2"): IndexConfig(
        index_key="msl.ccam.libs_rdr",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://pds-geosciences.wustl.edu/msl/"
            "msl-m-chemcam-libs-4_5-rdr-v1"
        ),
    ),
    ("msl", "ccam", "CCAM_RMI_RDR"): IndexConfig(
        index_key="msl.ccam.rmi_rdr",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://pds-geosciences.wustl.edu/msl/"
            "msl-m-chemcam-libs-4_5-rdr-v1"
        ),
    ),
    # CheMin EDR (all EDR types share one index)
    ("msl", "cmn", "CCD_FRAME"): IndexConfig(
        index_key="msl.cmn.edr",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://pds-geosciences.wustl.edu/msl/msl-m-chemin-2-edr-v1"
        ),
    ),
    ("msl", "cmn", "DIFFRACTION_SINGLE"): IndexConfig(
        index_key="msl.cmn.edr",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://pds-geosciences.wustl.edu/msl/msl-m-chemin-2-edr-v1"
        ),
    ),
    ("msl", "cmn", "DIFFRACTION_SPLIT"): IndexConfig(
        index_key="msl.cmn.edr",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://pds-geosciences.wustl.edu/msl/msl-m-chemin-2-edr-v1"
        ),
    ),
    ("msl", "cmn", "DIFFRACTION_ALL"): IndexConfig(
        index_key="msl.cmn.edr",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://pds-geosciences.wustl.edu/msl/msl-m-chemin-2-edr-v1"
        ),
    ),
    ("msl", "cmn", "ENERGY_SINGLE"): IndexConfig(
        index_key="msl.cmn.edr",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://pds-geosciences.wustl.edu/msl/msl-m-chemin-2-edr-v1"
        ),
    ),
    ("msl", "cmn", "ENERGY_SPLIT"): IndexConfig(
        index_key="msl.cmn.edr",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://pds-geosciences.wustl.edu/msl/msl-m-chemin-2-edr-v1"
        ),
    ),
    ("msl", "cmn", "ENERGY_ALL"): IndexConfig(
        index_key="msl.cmn.edr",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://pds-geosciences.wustl.edu/msl/msl-m-chemin-2-edr-v1"
        ),
    ),
    ("msl", "cmn", "FILM"): IndexConfig(
        index_key="msl.cmn.edr",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://pds-geosciences.wustl.edu/msl/msl-m-chemin-2-edr-v1"
        ),
    ),
    ("msl", "cmn", "HOUSEKEEPING"): IndexConfig(
        index_key="msl.cmn.edr",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://pds-geosciences.wustl.edu/msl/msl-m-chemin-2-edr-v1"
        ),
    ),
    ("msl", "cmn", "TRANSMIT_RAW"): IndexConfig(
        index_key="msl.cmn.edr",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://pds-geosciences.wustl.edu/msl/msl-m-chemin-2-edr-v1"
        ),
    ),
    # CheMin RDR
    ("msl", "cmn", "DIFFRACTION_ALL_RDR"): IndexConfig(
        index_key="msl.cmn.rdr",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://pds-geosciences.wustl.edu/msl/msl-m-chemin-4-rdr-v1"
        ),
    ),
    ("msl", "cmn", "ENERGY_SINGLE_RDR"): IndexConfig(
        index_key="msl.cmn.rdr",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://pds-geosciences.wustl.edu/msl/msl-m-chemin-4-rdr-v1"
        ),
    ),
    ("msl", "cmn", "MINERAL_TABLES"): IndexConfig(
        index_key="msl.cmn.rdr",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://pds-geosciences.wustl.edu/msl/msl-m-chemin-4-rdr-v1"
        ),
    ),
    # SAM (per-level indexes)
    ("msl", "sam", "l2_qms"): IndexConfig(
        index_key="msl.sam.l2",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://pds-geosciences.wustl.edu/msl/msl-m-sam-2-rdr-l0-v1"
        ),
    ),
    ("mer_spirit", "pancam", "rdr"): IndexConfig(
        index_key="mer_spirit.pancam.rdr",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
        archive_url=(
            "https://pds-geosciences.wustl.edu/mer/"
            "mer2-m-pancam-3-radcal-sci-v2"
        ),
    ),
    ("mer_opportunity", "pancam", "rdr"): IndexConfig(
        index_key="mer_opportunity.pancam.rdr",
        file_spec_col="FILE_NAME",
        path_name_col="PATH_NAME",
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
    from planetarypy.catalog._resolver import ResolvedProduct

    config = get_index_config(mission, instrument, product_key)
    if not config:
        return None

    try:
        df = _load_index_df(config)
    except Exception as e:
        logger.warning(f"Could not load index {config.index_key}: {e}")
        return None

    try:
        row = _find_product_in_index(df, product_id, config)
    except MultipleProductsError:
        raise  # Let caller handle ambiguous matches
    if row is None:
        return None

    # Use the canonical product_id from the index, not the user's input
    canonical_pid = _get_column(row, config.product_id_col, "PRODUCT_ID")
    if canonical_pid:
        canonical_pid = canonical_pid.strip()
    else:
        canonical_pid = product_id

    url_stem = _build_url_stem(row, config)

    # Use the actual filename from the index rather than guessing extensions.
    file_spec = _get_column(
        row, config.file_spec_col, "FILE_SPECIFICATION_NAME", "FILE_NAME"
    )
    if file_spec:
        main_file = PurePosixPath(file_spec).name
        if config.lowercase_files:
            main_file = main_file.lower()
        files = [main_file]
        label_file = main_file if main_file.upper().endswith(".LBL") else None
    else:
        files = []
        label_file = None

    if not url_stem or not files:
        logger.warning(
            f"Could not construct URL for '{product_id}' "
            f"from index {config.index_key}"
        )
        return None

    return ResolvedProduct(
        product_id=canonical_pid,
        url_stem=url_stem,
        files=files,
        label_file=label_file,
        source="index",
    )


# ── Internal helpers ──────────────────────────────────────────────────


def _load_index_df(config: IndexConfig) -> pd.DataFrame:
    """Load the index DataFrame, downloading if necessary.

    When extra_index_keys are set, loads all indexes and concatenates them.
    """
    from planetarypy.pds import get_index

    df = get_index(config.index_key)
    if config.extra_index_keys:
        dfs = [df]
        for key in config.extra_index_keys:
            dfs.append(get_index(key))
        df = pd.concat(dfs, ignore_index=True)
    return df


class MultipleProductsError(Exception):
    """Multiple products match the given identifier."""


def _find_product_in_index(
    df: pd.DataFrame,
    product_id: str,
    config: IndexConfig,
) -> pd.Series | None:
    """Search for a product in the index DataFrame.

    Tries the configured column first, then common fallbacks.
    Handles PDS whitespace-padded strings and case differences.

    Raises MultipleProductsError if an ambiguous match is found
    (e.g. observation ID matching multiple product variants).
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
            if mask.sum() == 1:
                return df.loc[mask].iloc[0]
            return df.loc[mask].iloc[-1]

        # Case-insensitive match
        mask = series.str.upper() == pid.upper()
        if mask.any():
            matches = df.loc[mask]
            if len(matches) == 1:
                return matches.iloc[0]
            # Multiple matches — check if they're distinct products
            pid_col = config.product_id_col
            if pid_col in df.columns:
                distinct_pids = matches[pid_col].str.strip().unique()
                if len(distinct_pids) > 1:
                    pid_list = ", ".join(sorted(distinct_pids))
                    raise MultipleProductsError(
                        f"'{product_id}' matches {len(distinct_pids)} products "
                        f"(matched via {col}): {pid_list}"
                    )
            return matches.iloc[-1]

    return None


def _build_url_stem(row: pd.Series, config: IndexConfig) -> str:
    """Construct the URL directory for a product from its index row."""
    if config.volume_id_col:
        volume_id = _get_column(row, config.volume_id_col, "VOLUME_ID")
    else:
        volume_id = ""
    file_spec = _get_column(
        row, config.file_spec_col, "FILE_SPECIFICATION_NAME", "FILE_NAME"
    )

    if not file_spec:
        return ""

    # Determine the subdirectory containing the product file.
    # Some indexes split path and filename into separate columns.
    if config.path_name_col:
        path_name = _get_column(row, config.path_name_col)
        file_dir = path_name.strip("/") if path_name else ""
    else:
        file_dir = str(PurePosixPath(file_spec).parent)
        if file_dir == ".":
            file_dir = ""

    if config.seti_volume_group:
        # If volume_id is empty, try to extract it from file_spec
        # (e.g. UVIS: FILE_NAME = '/COUVIS_0027/DATA/D2009_135/file.LBL')
        if not volume_id and file_spec.startswith("/"):
            path_parts = file_spec.strip("/").split("/")
            volume_id = path_parts[0]
            file_dir = "/".join(path_parts[1:-1])

        if not volume_id:
            return ""

        # Determine the volume group for SETI Rings archives
        if config.seti_volume_group == "auto":
            group = _derive_seti_volume_group(volume_id)
        else:
            group = config.seti_volume_group

        parts = [SETI_VOLUMES_BASE, group, volume_id]
        if file_dir:
            parts.append(file_dir)
        return "/".join(parts)
    else:
        lc = config.lowercase_paths
        parts = [config.archive_url.rstrip("/")]
        if volume_id:
            parts.append(volume_id.lower() if lc else volume_id)
        if file_dir:
            parts.append(file_dir.lower() if lc else file_dir)
        return "/".join(parts)


def _derive_seti_volume_group(volume_id: str) -> str:
    """Derive SETI volume group from a volume ID.

    Examples: COCIRS_1002 → COCIRS_1xxx, COISS_2011 → COISS_2xxx
    """
    if not volume_id or len(volume_id) < 2:
        return volume_id
    # Find the boundary between the prefix and numeric suffix
    # e.g. COCIRS_1002 → prefix=COCIRS_, first_digit=1, rest=002
    idx = 0
    for i, ch in enumerate(volume_id):
        if ch.isdigit():
            idx = i
            break
    if idx == 0:
        return volume_id
    prefix = volume_id[:idx]       # "COCIRS_" or "NHKCLO_"
    first_digit = volume_id[idx]   # "1"
    return f"{prefix}{first_digit}xxx"


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
        if not col:
            continue
        if col in row.index:
            val = str(row[col]).strip()
            if val and val.lower() != "nan":
                return val
    return ""
