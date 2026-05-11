"""Parse the NSSDC raw HTML archive into a clean time-indexed JSON archive.

Reads ``src/planetarypy/constants/nssdc/_raw/<body>/*.html.gz`` (populated by
``scripts/fetch_nssdc_archive.py``) and emits a normalized per-body history
suitable for the runtime lookup layer in
``src/planetarypy/constants/nssdc/_loader.py``.

The fact-sheet format drifted across the 30-year archival window:

  - **1996–2003**: ``<pre>``-wrapped fixed-width tables, three columns
    (target body, Earth, ratio).
  - **2004–2010**: same content, all whitespace stripped to a single line.
  - **2017+**: HTML ``<table>`` markup; same logical content.

Both styles are normalized into the same row shape::

    (label_with_units, value_for_target_body)

Time index uses the in-page "Last Updated: DD MONTH YYYY" stamp from the
``<h6>`` footer when available; falls back to the Wayback capture timestamp
otherwise. Dates earlier than 2003 are typically Wayback-only since the
"Last Updated" footer wasn't in the page yet.

Output: ``parsed_archive.json.gz`` with structure::

    {
      "version": "1",
      "generated_at": "2026-05-11T...",
      "fact_sheet": {
        "mars": {
          "captures": [
            {
              "wayback_timestamp": "19961219032509",
              "page_date": "1996-12-19" | null,
              "wayback_url": "...",
              "fields": {
                "mass_kg": {"value": 6.419e23, "raw": "0.6419", "unit": "10^24 kg"},
                ...
              }
            },
            ...
          ]
        },
        ...
      }
    }

Usage::

    python scripts/parse_nssdc_archive.py        # parse all bodies in _raw/
    python scripts/parse_nssdc_archive.py mars   # parse one body
"""

from __future__ import annotations

import datetime as _dt
import gzip
import html
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = REPO_ROOT / "src/planetarypy/constants/nssdc/_raw"
OUT_PATH = REPO_ROOT / "src/planetarypy/constants/nssdc/parsed_archive.json.gz"

ARCHIVE_VERSION = "1"


# ── Field-name normalization ─────────────────────────────────────────────
#
# NSSDC label (as it appears in the page) → Python-attribute-style name.
# Fields not in this map are skipped (currently asteroids/comets/satellites
# need their own additions in phase 2).

FIELD_MAP: dict[str, str] = {
    # Bulk parameters
    "Mass": "mass",
    "Volume": "volume",
    "Equatorial radius": "equatorial_radius",
    "Polar radius": "polar_radius",
    "Volumetric mean radius": "volumetric_mean_radius",
    "Core radius": "core_radius",
    "Ellipticity": "ellipticity",
    "Ellipticity (Flattening)": "flattening",
    "Mean density": "mean_density",
    "Surface gravity": "surface_gravity",
    "Surface gravity (mean)": "surface_gravity",
    "Surface acceleration": "surface_acceleration",
    "Surface acceleration (eq)": "surface_acceleration_eq",
    "Surface acceleration (pole)": "surface_acceleration_pole",
    "Escape velocity": "escape_velocity",
    "GM": "GM",
    "Bond albedo": "bond_albedo",
    "Geometric albedo": "geometric_albedo",
    "Visual geometric albedo": "geometric_albedo",
    "V-band magnitude V(1,0)": "v_band_magnitude",
    "Visual magnitude V(1,0)": "v_band_magnitude",
    "Solar irradiance": "solar_irradiance",
    "Black-body temperature": "black_body_temperature",
    "Topographic range": "topographic_range",
    "Moment of inertia (I/MR2)": "moment_of_inertia",
    "Moment of inertia (I/MR^2)": "moment_of_inertia",
    "J2": "J2",
    "Number of natural satellites": "number_of_satellites",
    "Planetary ring system": "has_ring_system",  # Yes/No textual
    # Orbital parameters
    "Semimajor axis": "semimajor_axis",
    "Sidereal orbit period": "sidereal_orbit_period",
    "Tropical orbit period": "tropical_orbit_period",
    "Perihelion": "perihelion",
    "Aphelion": "aphelion",
    "Synodic period": "synodic_period",
    "Mean orbital velocity": "mean_orbital_velocity",
    "Max. orbital velocity": "max_orbital_velocity",
    "Min. orbital velocity": "min_orbital_velocity",
    "Orbit inclination": "orbit_inclination",
    "Orbit eccentricity": "orbit_eccentricity",
    "Sidereal rotation period": "sidereal_rotation_period",
    "Length of day": "length_of_day",
    "Obliquity to orbit": "obliquity_to_orbit",
    "Inclination of equator": "inclination_of_equator",
    # Atmospheric (extracted from free-text section as best-effort)
    "Surface pressure": "surface_pressure",
    "Surface density": "surface_density",
    "Scale height": "scale_height",
    "Average temperature": "average_temperature",
    "Mean molecular weight": "mean_molecular_weight",
}


# ── Section detection / parsing ──────────────────────────────────────────


SECTION_HEADER_RE = re.compile(
    r"<h3[^>]*>\s*(.+?)\s*</h3>", re.IGNORECASE | re.DOTALL,
)

# A label-then-numbers row in the pre/table-stripped text. Captures:
#   group 1: label (text up to the first paren or sequence of two+ spaces)
#   group 2: rest of the line (which contains the numbers)
ROW_SPLIT_RE = re.compile(r"^\s*(.+?)\s{2,}(.+)$")

NUMERIC_RE = re.compile(r"-?\d+\.?\d*(?:[eE][+\-]?\d+)?")
# Same shape, but also accepts thousand-separator commas inside the
# integer part: ``1,989,100`` → ``1989100``. Used after stripping commas
# from the input string.
_COMMA_DIGITS_RE = re.compile(r"\d{1,3}(?:,\d{3})+")


def _strip_thousand_commas(token: str) -> str:
    """Remove thousand-separator commas inside numeric tokens.

    ``"1,989,100"`` → ``"1989100"``; non-thousands commas (``"1,2"``) and
    decimal commas left alone since NSSDC uses ``.`` as decimal point.
    """
    return _COMMA_DIGITS_RE.sub(lambda m: m.group(0).replace(",", ""), token)

# In-page "Last Updated" stamp. Variants seen:
#   "Last Updated: 27 October 2003, DRW"
#   "Last Updated: 16 September 2010, DRW"
#   "Last Updated: 04 December 2018"
LAST_UPDATED_RE = re.compile(
    r"Last\s+Updated[:\s]*([0-9]{1,2})\s+(\w+)\s+(\d{4})",
    re.IGNORECASE,
)

MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June",
     "July", "August", "September", "October", "November", "December"], 1)}


def parse_page_date(html_text: str) -> Optional[str]:
    """Return ISO-format YYYY-MM-DD from the in-page 'Last Updated' stamp,
    or None if absent / unparseable."""
    m = LAST_UPDATED_RE.search(html_text)
    if not m:
        return None
    day, month_name, year = m.groups()
    month_num = MONTHS.get(month_name.lower())
    if not month_num:
        return None
    try:
        return _dt.date(int(year), month_num, int(day)).isoformat()
    except ValueError:
        return None


def _strip_html(s: str) -> str:
    """Strip HTML tags, decode entities, normalize whitespace.

    Preserves explicit numeric exponents (e.g. ``<sup>24</sup>``) by
    inserting a ``^`` marker so a unit like ``10<sup>24</sup>`` becomes
    ``10^24`` after stripping. Collapses NBSP and other unicode spaces
    into a regular space so labels like ``"Visual magnitude V(1,&nbsp;0)"``
    match FIELD_MAP keys without literal NBSP characters.
    """
    # Mark superscripts before stripping tags so unit parsing keeps the exponent
    s = re.sub(r"<sup[^>]*>([^<]+)</sup>", r"^\1", s, flags=re.IGNORECASE)
    s = re.sub(r"<sub[^>]*>([^<]+)</sub>", r"_\1", s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", " ", s)
    s = html.unescape(s)
    s = s.replace("\xa0", " ")            # NBSP → regular space
    return s


def _split_label_unit(label_text: str) -> tuple[str, Optional[str]]:
    """Split 'Mass (10^24 kg)' into ('Mass', '10^24 kg').

    Takes the **last** balanced ``(...)`` group at end of string as the
    unit so labels like ``"Surface gravity (eq.) (m/s^2)"`` correctly
    yield label=``"Surface gravity (eq.)"`` and unit=``"m/s^2"``. Caller
    is responsible for any further label normalization.

    Returns (label, unit) where unit is None if no parenthesized suffix.
    """
    s = label_text.strip()
    if not s.endswith(")"):
        return s, None
    # Walk backward counting parens; find the matching '(' for the final ')'.
    depth = 0
    for i in range(len(s) - 1, -1, -1):
        if s[i] == ")":
            depth += 1
        elif s[i] == "(":
            depth -= 1
            if depth == 0:
                label = s[:i].rstrip()
                unit = s[i + 1:-1].strip()
                return label, unit
    return s, None


@dataclass
class ParsedRow:
    attr: str             # Python attribute name (the FIELD_MAP value)
    unit: Optional[str]   # raw unit string from page (e.g. '10^24 kg', 'km')
    raw: str              # raw value text as it appeared (e.g. '0.6419', '3396.2')
    value: Optional[float]  # coerced to float; None for non-numeric ('Yes', 'No', etc.)


def _match_field(label: str) -> Optional[str]:
    """FIELD_MAP lookup tolerant of trailing qualifier parens.

    NSSDC writes ``"Surface gravity (eq.)"`` for the Sun where Mars writes
    just ``"Surface gravity"``. After _split_label_unit pulled the
    *unit* paren off, any remaining ``(...)`` is a qualifier that we
    treat as cosmetic for FIELD_MAP matching purposes.
    """
    if label in FIELD_MAP:
        return FIELD_MAP[label]
    stripped = re.sub(r"\s*\([^()]*\)\s*$", "", label).strip()
    return FIELD_MAP.get(stripped)


def _parse_pre_block(text: str) -> list[ParsedRow]:
    """Parse rows from the plain-text body of a single section.

    Each line should have shape ``LABEL [whitespace] VALUE [other columns]``.
    We take the first non-empty numeric-or-textual token after the label
    as the target-body value (matches the column ordering used since
    1996: target column comes first after the label).
    """
    rows: list[ParsedRow] = []
    for line in text.splitlines():
        line = line.rstrip()
        if not line.strip():
            continue
        m = ROW_SPLIT_RE.match(line)
        if not m:
            continue
        label_text, rest = m.group(1), m.group(2)
        label, unit = _split_label_unit(label_text)
        attr = _match_field(label)
        if attr is None:
            continue

        # Pull the first whitespace-separated token from `rest` as the
        # target-body value. NSSDC tables put numbers like 1,989,100 with
        # thousand-separator commas — preserve as one token by collapsing
        # commas inside digit runs before splitting.
        rest_clean = _strip_thousand_commas(rest.strip())
        first_token = rest_clean.split()[0]
        token = first_token.rstrip(",;").rstrip()
        num = NUMERIC_RE.match(token)
        value = float(num.group(0)) if num else None
        rows.append(ParsedRow(attr=attr, unit=unit, raw=token, value=value))
    return rows


def _parse_table(table_html: str) -> list[ParsedRow]:
    """Parse rows from an HTML <table>.

    Modern (2017+) NSSDC layout: each data row is a <tr> with the label
    in a left-aligned <th> cell, followed by three <td> cells (target body,
    Earth, ratio). Header-only rows have multiple <th> and zero <td>.

    Older table-based variants (rare) use <td> for the label too — we
    accept that by treating the first cell of *any* type as label.
    """
    rows: list[ParsedRow] = []
    tr_iter = re.finditer(
        r"<tr[^>]*>(.*?)</tr>", table_html, re.IGNORECASE | re.DOTALL,
    )
    for tr in tr_iter:
        # Walk cells in document order: <th> or <td>. First cell is label,
        # second is the target-body value.
        cells = re.findall(
            r"<(?:t[hd])[^>]*>(.*?)</(?:t[hd])>",
            tr.group(1), re.IGNORECASE | re.DOTALL,
        )
        if len(cells) < 2:
            continue
        label_text = _strip_html(cells[0]).strip()
        if not label_text or label_text in ("", "Mars", "Earth", "Ratio"):
            continue  # header-only row
        label, unit = _split_label_unit(label_text)
        attr = _match_field(label)
        if attr is None:
            continue
        value_text = _strip_thousand_commas(_strip_html(cells[1]).strip())
        first_token = value_text.split()[0] if value_text.split() else ""
        token = first_token.rstrip(",;")
        num = NUMERIC_RE.match(token)
        value = float(num.group(0)) if num else None
        rows.append(ParsedRow(attr=attr, unit=unit, raw=token, value=value))
    return rows


def parse_capture(raw_html: str) -> dict:
    """Parse one capture's HTML into a {field_name: {value, raw, unit}} dict
    plus the page_date.

    Strategy: split HTML into sections by <h3>...</h3> markers. For each
    section, detect whether it's a <pre>-style or <table>-style block, and
    parse accordingly.
    """
    page_date = parse_page_date(raw_html)
    fields: dict[str, dict] = {}

    # Find all section boundaries: positions where <h3> markers appear.
    # Then for each section, look between this <h3> and the next one
    # (or the end of body).
    headers = list(SECTION_HEADER_RE.finditer(raw_html))
    boundaries = [m.end() for m in headers] + [len(raw_html)]

    for i, header in enumerate(headers):
        section_name = _strip_html(header.group(1)).strip()
        # We only parse known sections to limit noise. Satellite-table /
        # observational-parameters sections need separate handling later.
        if "Bulk parameters" not in section_name \
                and "Orbital parameters" not in section_name \
                and "Atmosphere" not in section_name:
            continue
        section_body = raw_html[header.end():boundaries[i + 1]]

        # Prefer table parsing if a <table> appears in this section.
        if re.search(r"<table[^>]*>", section_body, re.IGNORECASE):
            for row in _parse_table(section_body):
                # Only insert first occurrence (some sections list the
                # same label in moon sub-tables we want to skip).
                if row.attr not in fields:
                    fields[row.attr] = {
                        "value": row.value, "raw": row.raw, "unit": row.unit,
                    }
        else:
            # Pre-block style. Strip HTML to get text, parse rows.
            section_text = _strip_html(section_body)
            for row in _parse_pre_block(section_text):
                if row.attr not in fields:
                    fields[row.attr] = {
                        "value": row.value, "raw": row.raw, "unit": row.unit,
                    }

    # Atmospheric section is free-text; pull a few canonical labels from it
    # via simple regex.
    atmo = re.search(
        r"<h3[^>]*>[^<]*Atmosphere[^<]*</h3>(.*?)(?=<h3|<hr)",
        raw_html, re.IGNORECASE | re.DOTALL,
    )
    if atmo:
        atext = _strip_html(atmo.group(1))
        for label in ("Surface pressure", "Surface density",
                      "Scale height", "Average temperature",
                      "Mean molecular weight"):
            attr = FIELD_MAP[label]
            if attr in fields:
                continue
            m = re.search(
                rf"{re.escape(label)}\s*[:\s]\s*~?\s*({NUMERIC_RE.pattern})\s*([^\n,;]*)",
                atext,
            )
            if m:
                value = float(m.group(1))
                unit = m.group(2).strip().split()[0] if m.group(2).strip() else None
                fields[attr] = {"value": value, "raw": m.group(1), "unit": unit}

    return {"page_date": page_date, "fields": fields}


# ── Per-body driver ──────────────────────────────────────────────────────


def parse_body(body: str) -> dict:
    """Parse all cached captures for one body into a per-body history dict."""
    body_dir = RAW_DIR / body
    if not body_dir.is_dir():
        raise FileNotFoundError(f"No raw cache for body {body!r} at {body_dir}")

    captures: list[dict] = []
    for raw_file in sorted(body_dir.glob(f"{body}fact_*.html.gz")):
        # Filename: marsfact_YYYYMMDDhhmmss.html.gz
        m = re.match(rf"{body}fact_(\d{{14}})\.html\.gz", raw_file.name)
        if not m:
            continue
        wayback_ts = m.group(1)
        with gzip.open(raw_file, "rt", encoding="utf-8", errors="replace") as f:
            raw_html = f.read()
        try:
            parsed = parse_capture(raw_html)
        except Exception as e:
            print(f"  ! parse failed for {raw_file.name}: "
                  f"{type(e).__name__}: {e}", file=sys.stderr)
            continue
        captures.append({
            "wayback_timestamp": wayback_ts,
            "page_date": parsed["page_date"],
            "wayback_url": (
                f"https://web.archive.org/web/{wayback_ts}id_/"
                f"https://nssdc.gsfc.nasa.gov/planetary/factsheet/"
                f"{body}fact.html"
            ),
            "fields": parsed["fields"],
        })

    # Sort by best-available date (page_date if present, else Wayback ts).
    def sort_key(c: dict) -> str:
        return c["page_date"] or c["wayback_timestamp"][:8]
    captures.sort(key=sort_key)
    return {"captures": captures}


def main() -> None:
    args = sys.argv[1:]
    if args:
        bodies = args
    else:
        bodies = sorted(d.name for d in RAW_DIR.iterdir()
                        if d.is_dir() and not d.name.startswith("_"))

    archive: dict = {
        "version": ARCHIVE_VERSION,
        "generated_at": _dt.datetime.utcnow().isoformat() + "Z",
        "fact_sheet": {},
    }
    for body in bodies:
        print(f"\n== {body} ==", file=sys.stderr)
        try:
            body_history = parse_body(body)
        except FileNotFoundError as e:
            print(f"  skip: {e}", file=sys.stderr)
            continue
        n_captures = len(body_history["captures"])
        n_with_date = sum(1 for c in body_history["captures"] if c["page_date"])
        n_fields_total = sum(len(c["fields"]) for c in body_history["captures"])
        print(
            f"  {n_captures} captures parsed; "
            f"{n_with_date} have in-page date; "
            f"{n_fields_total} field-values total",
            file=sys.stderr,
        )
        archive["fact_sheet"][body] = body_history

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(OUT_PATH, "wt", encoding="utf-8") as f:
        json.dump(archive, f, indent=1, sort_keys=False)
    print(f"\nWrote {OUT_PATH} "
          f"({OUT_PATH.stat().st_size / 1024:.1f} KB compressed)",
          file=sys.stderr)


if __name__ == "__main__":
    main()
