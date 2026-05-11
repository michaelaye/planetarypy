"""NSSDC Planetary Fact Sheet values, time-indexed.

NSSDC = NASA's National Space Science Data Center at Goddard Space Flight
Center, maintainer of the canonical per-body planetary fact sheets
(D. R. Williams, since 1996). See
https://nssdc.gsfc.nasa.gov/planetary/factsheet/ for the upstream pages.


Optional, opt-in namespace for users who want NSSDC-only data deliberately.
The default top-level :mod:`planetarypy.constants` namespace already merges
NSSDC fields transparently into each Body (filling in fields PCK doesn't
provide); reach for ``planetarypy.constants.nssdc`` only when you want:

- the NSSDC value for a field PCK *also* has (e.g. ``nssdc.Mars.GM``
  vs the PCK value at top-level ``Mars.GM``);
- date-indexed lookup against NSSDC's own publication history;
- the full per-field timeline for archival / drift-tracking work.

Example::

    from planetarypy.constants import Mars                  # default merged
    Mars.bond_albedo                                         # NSSDC latest
    Mars.GM                                                  # PCK / IAU 2015

    from planetarypy.constants.nssdc import Mars             # NSSDC-only view
    Mars.GM                                                  # NSSDC GM, latest

    from planetarypy.constants import nssdc
    nssdc.at_time("mars", "bond_albedo", "2001")             # 0.16 (1996-era)
    nssdc.history("mars", "bond_albedo")                     # full timeline

The parsed archive is loaded lazily on first import; if not present in
the user's local cache, it auto-downloads from Zenodo (record id pinned
in ``_loader.ZENODO_RECORD_ID``).
"""

from __future__ import annotations

from typing import Optional

from planetarypy.constants.base import (
    Body,
    BodyRegistry,
    Constant,
)
from planetarypy.constants.nssdc import _loader

# ── NAIF id mapping for the bodies that have NSSDC fact sheets ──────────
#
# NSSDC fact sheets cover only the major bodies. NAIF ids are assigned to
# match the SPICE conventions so the NSSDC-built Body objects compose
# cleanly with the rest of planetarypy.constants.

_NSSDC_NAIF: dict[str, tuple[int, str, str]] = {
    # body_name (matches archive key) → (naif_id, canonical_name, body_class)
    "sun":      (10,  "SUN",     "sun"),
    "mercury":  (199, "MERCURY", "planet"),
    "venus":    (299, "VENUS",   "planet"),
    "earth":    (399, "EARTH",   "planet"),
    "moon":     (301, "MOON",    "moon"),
    "mars":     (499, "MARS",    "planet"),
    "jupiter":  (599, "JUPITER", "planet"),
    "saturn":   (699, "SATURN",  "planet"),
    "uranus":   (799, "URANUS",  "planet"),
    "neptune":  (899, "NEPTUNE", "planet"),
    "pluto":    (999, "PLUTO",   "planet"),
    # asteroid / comet summary sheets are aggregate, not per-body —
    # exposing them as Body would be misleading; skipped for the namespace.
}

REFERENCE = (
    "NASA NSSDC Planetary Fact Sheets, Williams D.R. (NSSDC, GSFC), "
    "https://nssdc.gsfc.nasa.gov/planetary/factsheet/"
)


def _make_constant(rec: _loader.NSSDCRecord, body_label: str) -> Optional[Constant]:
    qty = _loader.coerce_value(rec.value, rec.unit)
    if qty is None:
        return None
    return Constant(
        qty,
        name=rec.field,
        body=body_label,
        description=f"NSSDC field '{rec.field}' (raw='{rec.value}', unit='{rec.unit}')",
        reference=REFERENCE,
        iau_year=0,           # NSSDC values aren't IAU-edition-versioned
        source=rec.source_string(),
    )


def _build_body_from_capture(body: str, capture: dict) -> Body:
    """Construct a Body populated from one NSSDC capture's field set."""
    naif_id, canonical_name, body_class = _NSSDC_NAIF[body]
    parent = None
    if body_class == "moon":
        # Earth's Moon is the only moon with an NSSDC fact sheet.
        parent = 399

    body_kwargs: dict = {}
    for field_name, field_data in capture["fields"].items():
        if field_data.get("value") is None:
            continue
        # Synthesize a transient NSSDCRecord for the source string.
        rec = _loader.NSSDCRecord(
            body=body,
            field=field_name,
            value=field_data["value"],
            unit=field_data.get("unit"),
            page_date=capture.get("page_date"),
            wayback_timestamp=capture["wayback_timestamp"],
            wayback_url=capture["wayback_url"],
        )
        constant = _make_constant(rec, canonical_name.capitalize())
        if constant is None:
            continue
        # Only assign attributes the Body dataclass actually has.
        if hasattr(Body, "__dataclass_fields__") \
                and field_name in Body.__dataclass_fields__:
            body_kwargs[field_name] = constant

    return Body(
        name=canonical_name,
        naif_id=naif_id,
        body_class=body_class,
        parent=parent,
        dwarf_planet=(naif_id == 999),
        mission_visited=False,   # NSSDC doesn't track this; PCK side does
        **body_kwargs,
    )


def _build_latest_bodies() -> dict[int, Body]:
    """Construct every NSSDC body from its most-recent capture."""
    archive = _loader.load_archive()
    out: dict[int, Body] = {}
    for body, sheet in archive["fact_sheet"].items():
        if body not in _NSSDC_NAIF:
            continue
        if not sheet["captures"]:
            continue
        latest_cap = sheet["captures"][-1]   # captures are sorted by best_date
        body_obj = _build_body_from_capture(body, latest_cap)
        out[body_obj.naif_id] = body_obj
    return out


# ── Public API ─────────────────────────────────────────────────────────


# Eager build of the latest-snapshot Body instances. Cheap (<<1s) and
# makes attribute access (`nssdc.Mars`) work without a side-effect on
# first read.
bodies: BodyRegistry = BodyRegistry(_build_latest_bodies())


# Hoist named bodies into the module namespace as direct attributes —
# `from planetarypy.constants.nssdc import Mars` then works.
def _hoist() -> None:
    for body in bodies.values():
        # `Mars`, `Saturn`, etc. (title case)
        py_name = body.name.title()
        globals()[py_name] = body


_hoist()


# ── Date-indexed lookup helpers ────────────────────────────────────────


def at_time(body: str, field: str, date) -> Optional[Constant]:
    """Return the NSSDC value of ``body.field`` current as of ``date``.

    ``date`` may be a year (``"2001"``), a year-month (``"2001-06"``),
    or an ISO date (``"2001-06-15"``). Year/year-month resolve to the
    end of that period (e.g. ``"2001"`` → ``"2001-12-31"``), so a
    'show me what was current in 2001' query picks the latest 2001
    capture.
    """
    iso = _loader.normalize_date(str(date))
    rec = _loader.at_date(body.lower(), field, iso)
    if rec is None:
        return None
    return _make_constant(
        rec,
        body_label=_NSSDC_NAIF.get(body.lower(), (0, body.upper(), ""))[1].capitalize(),
    )


def history(body: str, field: str) -> list[tuple[str, Constant, str]]:
    """Full NSSDC time series for ``(body, field)``.

    Returns a list of ``(date, Constant, wayback_url)`` tuples sorted
    oldest-first. Useful for drift-tracking studies.
    """
    body_l = body.lower()
    label = _NSSDC_NAIF.get(body_l, (0, body.upper(), ""))[1].capitalize()
    out: list[tuple[str, Constant, str]] = []
    for rec in _loader.history(body_l, field):
        constant = _make_constant(rec, body_label=label)
        if constant is not None:
            out.append((rec.best_date, constant, rec.wayback_url))
    return out


def known_bodies() -> list[str]:
    """Bodies present in the NSSDC archive."""
    return _loader.known_bodies()


__all__ = [
    "bodies",
    "at_time",
    "history",
    "known_bodies",
    "REFERENCE",
]
