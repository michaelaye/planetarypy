"""Internal helpers for ``planetarypy.constants.nssdc``.

Loads the parsed NSSDC fact-sheet archive (produced by
``scripts/parse_nssdc_archive.py``), provides date-indexed lookups, and
coerces NSSDC raw values + unit strings into astropy Quantities suitable
for wrapping in :class:`planetarypy.constants.base.Constant`.

The parsed archive is auto-downloaded from Zenodo on first use when not
present locally — same lazy-cache pattern as PDS index parquets.
"""

from __future__ import annotations

import gzip
import json
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Optional

import astropy.units as u

# ── Versioning ─────────────────────────────────────────────────────────
#
# Two independent knobs control archive identity and freshness:
#
#   EXPECTED_ARCHIVE_VERSION — JSON *schema* version. Bump only when the
#       parsed_archive.json.gz shape changes in a way that breaks existing
#       consumers (field rename, field removal, type change). Additive
#       optional keys (e.g. the v0.64 ``uncertainty`` / ``range`` keys)
#       do not require a bump.
#
#   ZENODO_RECORD_ID — Per-version Zenodo record ID of the deposit users
#       should download. Bump on every new Zenodo upload, including pure
#       data refreshes. This ID is also baked into the local cache
#       filename, so bumping it invalidates the cache and forces a
#       redownload — which is how data-only updates actually reach end
#       users after a planetarypy release.
#
# DOI of the current deposit (per-version DOI — distinct from the
# concept DOI 10.5281/zenodo.20122986 used in the README citation):
#   https://doi.org/10.5281/zenodo.20426712 (record 20426712, deposit v1.1.0)

EXPECTED_ARCHIVE_VERSION = "1"
ZENODO_RECORD_ID: Optional[str] = "20426712"

# Filename inside the Zenodo deposit (unversioned — the cache keeps a
# versioned copy keyed on both knobs above).
_ZENODO_FILENAME = "parsed_archive.json.gz"

# In-repo location during development. After Zenodo deposit, the runtime
# also accepts the file at {storage_root}/constants/nssdc/.
_PACKAGED_PATH = Path(__file__).parent / "parsed_archive.json.gz"


def _local_archive_path() -> Path:
    """Return the local cache path under the user's planetarypy storage root.

    Filename is keyed on both ``EXPECTED_ARCHIVE_VERSION`` (schema) and
    ``ZENODO_RECORD_ID`` (data revision) so that bumping either constant
    in a future planetarypy release forces a re-download on next use.
    """
    from planetarypy.config import config

    return (
        Path(config.storage_root)
        / "constants"
        / "nssdc"
        / f"parsed_archive_v{EXPECTED_ARCHIVE_VERSION}_z{ZENODO_RECORD_ID}.json.gz"
    )


def _download_from_zenodo(dest: Path) -> None:
    """Lazy download from the pinned Zenodo record to ``dest``.

    Raises if no record id is configured yet (early-development state).
    """
    if ZENODO_RECORD_ID is None:
        raise RuntimeError(
            "NSSDC archive not found locally and no Zenodo record id is "
            "configured yet. Either drop the parsed_archive.json.gz under "
            f"{dest.parent} or wait for the v1 Zenodo deposit (planned)."
        )
    from planetarypy import utils as plp_utils

    url = (
        f"https://zenodo.org/records/{ZENODO_RECORD_ID}"
        f"/files/{_ZENODO_FILENAME}"
    )
    dest.parent.mkdir(parents=True, exist_ok=True)
    plp_utils.url_retrieve(url, str(dest))


@lru_cache(maxsize=1)
def load_archive() -> dict:
    """Load the parsed NSSDC archive, downloading on first use if needed.

    Resolution order:
      1. Bundled in-repo ``parsed_archive.json.gz`` (development mode).
      2. User cache under ``{storage_root}/constants/nssdc/``.
      3. Auto-download from Zenodo to user cache, then load.
    """
    if _PACKAGED_PATH.is_file():
        path = _PACKAGED_PATH
    else:
        path = _local_archive_path()
        if not path.is_file():
            _download_from_zenodo(path)
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return json.load(f)


# ── Unit coercion ──────────────────────────────────────────────────────

# Map raw NSSDC unit strings to (astropy_unit, scale_multiplier).
# NSSDC writes values like "0.6419" with unit "10^24 kg" — we multiply
# the raw value by 10^24 and assign u.kg.
_UNIT_MAP: dict[str, tuple[u.UnitBase, float]] = {
    # bare units
    "km": (u.km, 1.0),
    "kg": (u.kg, 1.0),
    "kg/m^3": (u.kg / u.m ** 3, 1.0),
    "m/s^2": (u.m / u.s ** 2, 1.0),
    "km/s": (u.km / u.s, 1.0),
    "deg": (u.deg, 1.0),
    "degrees": (u.deg, 1.0),
    "K": (u.K, 1.0),
    "hrs": (u.hour, 1.0),
    "hours": (u.hour, 1.0),
    "days": (u.day, 1.0),
    "g/mole": (u.g / u.mol, 1.0),
    "mb": (u.mbar, 1.0),
    "bar": (u.bar, 1.0),
    "bars": (u.bar, 1.0),
    "Pa": (u.Pa, 1.0),
    "atm": (u.bar, 1.01325),
    "AU": (u.au, 1.0),
    "W/m^2": (u.W / u.m ** 2, 1.0),
    # scaled — NSSDC factored-out exponents
    "10^24 kg": (u.kg, 1e24),
    "10^15 kg": (u.kg, 1e15),
    "10^10 km^3": (u.km ** 3, 1e10),
    "10^12 km^3": (u.km ** 3, 1e12),
    "10^6 km": (u.km, 1e6),
    "10^6 km^3/s^2": (u.km ** 3 / u.s ** 2, 1e6),
    "10^9 km^3/s^2": (u.km ** 3 / u.s ** 2, 1e9),
    "10^20 kg": (u.kg, 1e20),
    "10^21 kg": (u.kg, 1e21),
    "10^22 kg": (u.kg, 1e22),
    "10^23 kg": (u.kg, 1e23),
    "10^25 kg": (u.kg, 1e25),
    "10^26 kg": (u.kg, 1e26),
    "10^27 kg": (u.kg, 1e27),
    "10^29 kg": (u.kg, 1e29),
    "10^30 kg": (u.kg, 1e30),
    # dimensionless
    None: (u.dimensionless_unscaled, 1.0),
    "": (u.dimensionless_unscaled, 1.0),
    "ppm": (u.dimensionless_unscaled, 1e-6),
    "10^-6": (u.dimensionless_unscaled, 1e-6),  # NSSDC's "x 10^-6" — used for J2
}


def _normalize_unit(raw_unit: Optional[str]) -> Optional[str]:
    """Normalize NSSDC's unit-string quirks before _UNIT_MAP lookup.

    NSSDC writes ``GM (x 10^6 km^3/s^2)`` — that leading 'x ' marks the
    factored-out scale. Normalize away so it matches ``"10^6 km^3/s^2"``
    in the lookup table.
    """
    if raw_unit is None:
        return None
    s = raw_unit.strip()
    s = re.sub(r"^x\s+", "", s)        # "x 10^6 km" -> "10^6 km"
    s = re.sub(r"\s+", " ", s)
    return s


# Conventional unit for fields NSSDC sometimes publishes without one.
# Applied only when the raw unit is ``None`` or empty AND the field name
# appears here — explicit units on the source always win. Documented
# per-entry so the assumption is auditable.
_FIELD_DEFAULT_UNITS: dict[str, u.UnitBase] = {
    # Earth captures publish "Mean molecular weight: 28.97" with no
    # unit; other bodies write "g/mole" explicitly. Atmospheric-chemistry
    # convention is g/mol (≡ Da, ≡ atomic mass units). Without this
    # default, ~338 captures load as dimensionless while ~171 load as
    # g/mol, producing an inconsistent type across bodies.
    "mean_molecular_weight": u.g / u.mol,
}


def coerce_value(
    raw_value: float,
    raw_unit: Optional[str],
    field_name: Optional[str] = None,
) -> Optional[u.Quantity]:
    """Turn ``(raw_value, raw_unit_str)`` into an astropy Quantity.

    When ``raw_unit`` is absent (``None`` or empty) AND ``field_name`` is
    a key in :data:`_FIELD_DEFAULT_UNITS`, the field's conventional unit
    is applied. Explicit units on the source always take precedence;
    fields without a default entry fall back to ``dimensionless_unscaled``
    via ``_UNIT_MAP[None]``.

    Returns ``None`` for non-numeric values (e.g. "Yes", "No") or unit
    strings we don't recognize — caller decides how to handle missing.
    """
    if raw_value is None:
        return None
    key = _normalize_unit(raw_unit)
    if (key is None or key == "") and field_name in _FIELD_DEFAULT_UNITS:
        return raw_value * _FIELD_DEFAULT_UNITS[field_name]
    if key in _UNIT_MAP:
        unit, scale = _UNIT_MAP[key]
        return (raw_value * scale) * unit
    # Best-effort fall-through: try astropy parser on the raw string.
    try:
        unit = u.Unit(key)
        return raw_value * unit
    except Exception:
        return None


# ── Date-indexed lookup ────────────────────────────────────────────────


@dataclass(frozen=True)
class NSSDCRecord:
    """One field-value reading from a single NSSDC capture.

    ``value`` is ``None`` for range-typed entries (NSSDC published bounds
    rather than a central estimate). ``uncertainty`` is the ± value for
    rows like ``X +/- Y unit``. ``range_min/range_max`` are populated for
    rows like ``X - Y unit``. The error-info fields are mutually exclusive
    in practice though the schema doesn't enforce it.
    """
    body: str
    field: str
    value: Optional[float]
    unit: Optional[str]
    page_date: Optional[str]    # ISO YYYY-MM-DD
    wayback_timestamp: str      # YYYYMMDDhhmmss
    wayback_url: str
    uncertainty: Optional[float] = None
    range_min: Optional[float] = None
    range_max: Optional[float] = None

    @property
    def best_date(self) -> str:
        """ISO date used for time-indexing — page_date if present, else
        the YYYY-MM-DD prefix of the Wayback timestamp."""
        return self.page_date or (
            f"{self.wayback_timestamp[0:4]}-"
            f"{self.wayback_timestamp[4:6]}-"
            f"{self.wayback_timestamp[6:8]}"
        )

    def source_string(self) -> str:
        """Human-readable provenance for the ``Constant.source`` attribute.

        Uses "updated" when the date comes from NSSDC's own "Last Updated"
        footer stamp, and "crawled" when the date is the Wayback Machine's
        capture timestamp fallback (used only for pre-2003 captures where
        NSSDC hadn't yet added the in-page footer).
        """
        verb = "updated" if self.page_date else "crawled"
        return f"NSSDC {self.body}fact.html {verb} {self.best_date}"


@lru_cache(maxsize=None)
def _per_field_history(body: str, field: str) -> list[NSSDCRecord]:
    """All NSSDCRecord entries for one (body, field), sorted by best_date."""
    archive = load_archive()
    sheet = archive["fact_sheet"].get(body)
    if not sheet:
        return []
    out: list[NSSDCRecord] = []
    for cap in sheet["captures"]:
        f = cap["fields"].get(field)
        if not f:
            continue
        rng = f.get("range") or {}
        has_value = f.get("value") is not None
        has_range = rng.get("min") is not None and rng.get("max") is not None
        if not has_value and not has_range:
            continue
        rec = NSSDCRecord(
            body=body,
            field=field,
            value=f.get("value"),
            unit=f.get("unit"),
            page_date=cap.get("page_date"),
            wayback_timestamp=cap["wayback_timestamp"],
            wayback_url=cap["wayback_url"],
            uncertainty=f.get("uncertainty"),
            range_min=rng.get("min"),
            range_max=rng.get("max"),
        )
        out.append(rec)
    out.sort(key=lambda r: r.best_date)
    return out


def latest(body: str, field: str) -> Optional[NSSDCRecord]:
    """Most recent NSSDC reading for ``(body, field)``. ``None`` if absent."""
    history = _per_field_history(body, field)
    return history[-1] if history else None


def at_date(body: str, field: str, date_iso: str) -> Optional[NSSDCRecord]:
    """NSSDC reading current as of ``date_iso`` (YYYY-MM-DD).

    Returns the latest reading whose ``best_date`` is ``<= date_iso``. If
    the requested date precedes the earliest capture, returns ``None``.
    """
    history = _per_field_history(body, field)
    if not history:
        return None
    # Linear scan from the end — histories are short (~hundred entries
    # per field, max).
    for rec in reversed(history):
        if rec.best_date <= date_iso:
            return rec
    return None


def history(body: str, field: str) -> list[NSSDCRecord]:
    """Full NSSDC time series for one (body, field), oldest first."""
    return list(_per_field_history(body, field))


def normalize_date(date: str) -> str:
    """Normalize a user-supplied date to ISO YYYY-MM-DD.

    Accepts ``"2001"``, ``"2001-06"``, ``"2001-06-15"``. Year-only and
    year-month resolve to the END of that period (Dec 31, last day of
    month) so a 'show me what was current in 2001' query naturally picks
    the latest 2001 capture rather than the first.
    """
    if re.fullmatch(r"\d{4}", date):
        return f"{date}-12-31"
    if re.fullmatch(r"\d{4}-\d{1,2}", date):
        y, m = date.split("-")
        # Last day of month — over-approximate as 31, the comparison is
        # string-wise so "-31" sorts after any real day.
        return f"{int(y):04d}-{int(m):02d}-31"
    return date


def known_bodies() -> list[str]:
    """Bodies present in the loaded archive."""
    return sorted(load_archive()["fact_sheet"].keys())


def known_fields(body: str) -> set[str]:
    """All field names that appear at least once in any capture for ``body``."""
    archive = load_archive()
    sheet = archive["fact_sheet"].get(body)
    if not sheet:
        return set()
    out: set[str] = set()
    for cap in sheet["captures"]:
        out.update(cap["fields"].keys())
    return out
