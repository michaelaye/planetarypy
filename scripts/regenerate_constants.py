"""Regenerate ``src/planetarypy/constants/iauNNNN.py`` from a PCK kernel.

Usage::

    # Regenerate one specific edition:
    python scripts/regenerate_constants.py iau2009
    python scripts/regenerate_constants.py iau2015

    # Or both at once (equivalent to no arguments):
    python scripts/regenerate_constants.py
    python scripts/regenerate_constants.py iau2009 iau2015

This is a **maintainer-only build-time script**. It uses ``spiceypy`` as
an authoritative parser for the PCK kernel-pool format, but the runtime
``planetarypy.constants`` package never imports spiceypy — only the
generated ``iauNNNN.py`` files (immutable Python data) ship with the
package.

When NAIF (or a mission archive) releases a new kernel:

1. Add an entry to ``scripts/constants_kernels.toml``.
2. Drop the new ``.tpc`` file in
   ``src/planetarypy/constants/_constants_sources/``.
3. Run this script.
4. Commit the new source kernel + the new ``iauNNNN.py`` together.

CI re-runs this script on every PR that touches ``_constants_sources/``
or this file, and fails if the output drifts from what's committed —
catches "edited the manifest but forgot to regenerate".

See ``docs/explanation/constants_design.qmd`` for the design rationale.
"""

from __future__ import annotations

import gzip
import json
import re
import sys
import tomllib
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import spiceypy as sp


# ── Layout ──────────────────────────────────────────────────────────────

REPO_ROOT = Path(__file__).resolve().parent.parent
SOURCES_DIR = REPO_ROOT / "src/planetarypy/constants/_constants_sources"
OUT_DIR = REPO_ROOT / "src/planetarypy/constants"
MANIFEST_PATH = REPO_ROOT / "scripts/constants_kernels.toml"
NSSDC_ARCHIVE_PATH = REPO_ROOT / "src/planetarypy/constants/nssdc/parsed_archive.json.gz"

# NAIF id → NSSDC archive body key. Mirrors _NSSDC_NAIF in
# nssdc/__init__.py so the merge knows which PCK bodies have NSSDC data.
_NSSDC_BY_NAIF: dict[int, str] = {
    10:  "sun",
    199: "mercury",
    299: "venus",
    399: "earth",
    301: "moon",
    499: "mars",
    599: "jupiter",
    699: "saturn",
    799: "uranus",
    899: "neptune",
    999: "pluto",
}

# Fields populated from PCK; NSSDC values for these are NOT merged into
# the top-level Body (PCK wins for cartographic/orientation). NSSDC
# values for these fields remain accessible via Mars.at_time(date) for
# pre-PCK dates and via the explicit nssdc submodule.
_PCK_DERIVED_FIELDS: frozenset[str] = frozenset({
    "radii", "GM", "long_axis",
    "pole_ra", "pole_dec", "pm", "rotation_rate",
    "pole_ra_coeffs", "pole_dec_coeffs", "pm_coeffs",
    "mean_radius", "volume_radius", "flattening",
})

# NSSDC unit string (after _normalize_unit) → astropy expression to emit
# in the generated Python source. Mirrors _UNIT_MAP in nssdc/_loader.py
# but for code-emission rather than runtime coercion.
_NSSDC_UNIT_EMIT: dict[Optional[str], tuple[str, float]] = {
    "km": ("u.km", 1.0),
    "kg": ("u.kg", 1.0),
    "kg/m^3": ("u.kg / u.m ** 3", 1.0),
    "m/s^2": ("u.m / u.s ** 2", 1.0),
    "km/s": ("u.km / u.s", 1.0),
    "deg": ("u.deg", 1.0),
    "degrees": ("u.deg", 1.0),
    "K": ("u.K", 1.0),
    "hrs": ("u.hour", 1.0),
    "hours": ("u.hour", 1.0),
    "days": ("u.day", 1.0),
    "g/mole": ("u.g / u.mol", 1.0),
    "mb": ("u.mbar", 1.0),
    "bar": ("u.bar", 1.0),
    "bars": ("u.bar", 1.0),
    "Pa": ("u.Pa", 1.0),
    "atm": ("u.bar", 1.01325),
    "AU": ("u.au", 1.0),
    "W/m^2": ("u.W / u.m ** 2", 1.0),
    "10^24 kg": ("u.kg", 1e24),
    "10^15 kg": ("u.kg", 1e15),
    "10^10 km^3": ("u.km ** 3", 1e10),
    "10^12 km^3": ("u.km ** 3", 1e12),
    "10^6 km": ("u.km", 1e6),
    "10^6 km^3/s^2": ("u.km ** 3 / u.s ** 2", 1e6),
    "10^9 km^3/s^2": ("u.km ** 3 / u.s ** 2", 1e9),
    "10^20 kg": ("u.kg", 1e20),
    "10^21 kg": ("u.kg", 1e21),
    "10^22 kg": ("u.kg", 1e22),
    "10^23 kg": ("u.kg", 1e23),
    "10^25 kg": ("u.kg", 1e25),
    "10^26 kg": ("u.kg", 1e26),
    "10^27 kg": ("u.kg", 1e27),
    "10^29 kg": ("u.kg", 1e29),
    "10^30 kg": ("u.kg", 1e30),
    None: ("u.dimensionless_unscaled", 1.0),
    "": ("u.dimensionless_unscaled", 1.0),
    "ppm": ("u.dimensionless_unscaled", 1e-6),
    "10^-6": ("u.dimensionless_unscaled", 1e-6),  # NSSDC's "x 10^-6" — used for J2
}


def _normalize_nssdc_unit(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    s = raw.strip()
    s = re.sub(r"^x\s+", "", s)
    s = re.sub(r"\s+", " ", s)
    return s


def load_nssdc_lookup() -> dict[int, dict]:
    """NAIF id → {'page_date', 'wayback_url', 'fields'} for the latest NSSDC
    capture of each body present in the archive. Empty dict if the archive
    isn't built yet (regenerator continues, just no NSSDC merge)."""
    if not NSSDC_ARCHIVE_PATH.is_file():
        print(f"  (no NSSDC archive at {NSSDC_ARCHIVE_PATH}; skipping merge)",
              file=sys.stderr)
        return {}
    with gzip.open(NSSDC_ARCHIVE_PATH, "rt", encoding="utf-8") as f:
        archive = json.load(f)
    out: dict[int, dict] = {}
    for body, sheet in archive["fact_sheet"].items():
        if not sheet["captures"]:
            continue
        # Find the matching NAIF id
        naif_id = next(
            (nid for nid, name in _NSSDC_BY_NAIF.items() if name == body),
            None,
        )
        if naif_id is None:
            continue
        out[naif_id] = sheet["captures"][-1]   # latest capture
    return out


# ── Manifest ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class KernelEntry:
    file: str
    url: str
    purpose: str


@dataclass(frozen=True)
class Edition:
    iau_year: int
    pck: str
    url: str
    reference: str


def _load_manifest() -> tuple[list[KernelEntry], dict[int, Edition]]:
    """Read scripts/constants_kernels.toml; return (common_kernels, editions)."""
    if not MANIFEST_PATH.is_file():
        raise FileNotFoundError(f"Missing manifest: {MANIFEST_PATH}")
    data = tomllib.loads(MANIFEST_PATH.read_text())
    common = [
        KernelEntry(file=k["file"], url=k["url"], purpose=k.get("purpose", ""))
        for k in data.get("common", [])
    ]
    editions: dict[int, Edition] = {}
    for key, ed in data.get("edition", {}).items():
        # key is like "iau2015"; extract year
        year = int(key.replace("iau", ""))
        editions[year] = Edition(
            iau_year=year,
            pck=ed["pck"],
            url=ed["url"],
            reference=ed["reference"],
        )
    return common, editions


# ── Curated overlays (single source of truth for these flags) ───────────

DWARF_PLANET_NAIF_IDS: frozenset[int] = frozenset({
    999,        # Pluto
    2000001,    # Ceres
    # Eris, Makemake, Haumea, Sedna would be added here when/if NAIF
    # ships PCK data for them.
})

MISSION_VISITED_NAIF_IDS: frozenset[int] = frozenset({
    2000001,    # Ceres            (Dawn)
    2000004,    # Vesta            (Dawn)
    2000021,    # Lutetia          (Rosetta flyby)
    2000243,    # Ida              (Galileo flyby)
    2000253,    # Mathilde         (NEAR flyby)
    2000433,    # Eros             (NEAR Shoemaker)
    2000951,    # Gaspra           (Galileo flyby)
    2002867,    # Steins           (Rosetta flyby)
    2025143,    # Itokawa          (Hayabusa)
    2101955,    # Bennu            (OSIRIS-REx)
    2162173,    # Ryugu            (Hayabusa2)
    1000005,    # Halley           (Giotto, Vega 1+2)
    1000012,    # 67P/Churyumov-Gerasimenko (Rosetta)
    1000036,    # Hartley 2        (Deep Impact / EPOXI)
    1000041,    # 19P/Borrelly     (Deep Space 1)
    1000093,    # Tempel 1         (Deep Impact, Stardust)
    1000107,    # Wild 2           (Stardust)
})


# ── PCK pool-variable schema ────────────────────────────────────────────

@dataclass(frozen=True)
class Field:
    pool_var: str       # PCK suffix; full var = f"BODY{naif_id}_{pool_var}"
    attr: str           # python attr on Body
    n: int              # number of floats to read
    unit: str           # python expr for the astropy unit
    desc: str           # human description


# Static, single-value or fixed-tuple fields.
STATIC_FIELDS: list[Field] = [
    Field("RADII",      "radii",     3, "u.km",
          "Triaxial body radii (a, b, c)"),
    Field("GM",         "GM",        1, "u.km**3 / u.s**2",
          "Gravitational parameter"),
    Field("LONG_AXIS",  "long_axis", 1, "u.deg",
          "Offset of longest axis from prime meridian"),
]

# Polynomial-coefficient fields: (J2000_value, rate, quadratic).
# We extract the full triple as POLE_RA_COEFFS / POLE_DEC_COEFFS / PM_COEFFS
# AND emit the J2000 scalar on its own as POLE_RA / POLE_DEC / PM for
# the convenience case ("what's the angle right now").
POLY_FIELDS: list[Field] = [
    Field("POLE_RA",  "pole_ra",  3, "u.deg",
          "Right ascension of north pole"),
    Field("POLE_DEC", "pole_dec", 3, "u.deg",
          "Declination of north pole"),
    Field("PM",       "pm",       3, "u.deg",
          "Prime meridian angle"),
]

# Astropy unit expressions for the polynomial coefficients
# (J2000_value, rate, quadratic). PCK rates are per Julian century
# (POLE_RA/DEC, PM quadratic) or per day (PM linear).
#
# Astropy's ``u.cy`` is the "cycle" (rotation) unit, not "century" — and
# astropy reduces ``100 * u.year`` numerically to ``0.01 * yr`` on
# display, scaling the value (e.g. 0.106 deg/century becomes
# 0.00106 deg/yr). Both wrong for our use case. We define a named
# ``century`` unit at the top of each generated module (see
# format_module's MODULE_HEADER) so coefficients display as
# ``deg / century`` with their PCK-stated numeric values.
POLY_COEFF_UNITS = {
    "POLE_RA":  ("u.deg", "u.deg / _century",  "u.deg / _century**2"),
    "POLE_DEC": ("u.deg", "u.deg / _century",  "u.deg / _century**2"),
    "PM":       ("u.deg", "u.deg / u.day",     "u.deg / _century**2"),
}


# ── Classification (must match base.py exactly) ─────────────────────────

def classify(naif_id: int) -> str:
    if naif_id == 10:
        return "sun"
    if 1 <= naif_id <= 9:
        return "barycenter"
    if 100 <= naif_id <= 999 and naif_id % 100 == 99:
        return "planet"
    if 100 <= naif_id <= 999:
        return "moon"
    if 1_000_000 <= naif_id < 2_000_000:
        return "comet"
    # Asteroids: the canonical 2_NNNNNN range plus the auxiliary ranges
    # JPL uses for alternate / mission-target IDs (Didymos at 20065803,
    # Dimorphos at 120065803, DART/Hera bodies at 9_NNNNNN, etc.).
    # Anything 2_000_000 ≤ id < 1_000_000_000 we treat as asteroid for v1.
    if 2_000_000 <= naif_id < 1_000_000_000:
        return "asteroid"
    return "other"


def parent_planet_id(naif_id: int) -> Optional[int]:
    if classify(naif_id) == "moon":
        return (naif_id // 100) * 100 + 99
    return None


# ── Body discovery ──────────────────────────────────────────────────────

def discover_naif_ids() -> set[int]:
    """All NAIF ids that have at least one BODY*_RADII or BODY*_GM in pool."""
    ids: set[int] = set()
    for pattern in ("BODY*_RADII", "BODY*_GM"):
        try:
            result = sp.gnpool(pattern, 0, 5000)
        except sp.utils.exceptions.SpiceyError:
            continue
        # gnpool's return shape varies by spiceypy version — sometimes
        # a (names, found_bool) tuple, sometimes just the names list.
        names = result[0] if isinstance(result, tuple) else result
        for name in names:
            m = re.match(r"BODY(-?\d+)_", name)
            if m:
                ids.add(int(m.group(1)))
    return ids


def body_name(naif_id: int) -> str:
    """spiceypy.bodc2s, falling back to ``BODY_<id>`` for unregistered ids."""
    try:
        return sp.bodc2s(naif_id).strip()
    except sp.utils.exceptions.SpiceyError:
        return f"BODY_{naif_id}"


# ── Field extraction ────────────────────────────────────────────────────

def _gdpool_safe(var: str, n: int) -> Optional[list[float]]:
    """Return the first ``n`` floats of ``var`` from the kernel pool, or None.

    Values are coerced to plain Python ``float`` so the generated module's
    literals don't carry ``np.float64(...)`` wrappers.
    """
    try:
        if not sp.expool(var):
            return None
        values = sp.gdpool(var, 0, n + 5)
    except sp.utils.exceptions.SpiceyError:
        return None
    return [float(v) for v in values[:n]]


def extract_body(naif_id: int) -> dict:
    body: dict = {
        "naif_id":         naif_id,
        "name":            body_name(naif_id),
        "body_class":      classify(naif_id),
        "parent":          parent_planet_id(naif_id),
        "dwarf_planet":    naif_id in DWARF_PLANET_NAIF_IDS,
        "mission_visited": naif_id in MISSION_VISITED_NAIF_IDS,
        "fields":          {},
    }
    # Static fields → one value (or 3-tuple for radii)
    for f in STATIC_FIELDS:
        vals = _gdpool_safe(f"BODY{naif_id}_{f.pool_var}", f.n)
        if vals is None:
            continue
        body["fields"][f.attr] = (vals, f.unit, f.desc)
    # Polynomial fields → store full coeffs and the J2000 scalar.
    # PCK is allowed to omit trailing zero terms; pad to the full
    # (J2000, rate, quad) triple so downstream formatting is uniform.
    for f in POLY_FIELDS:
        vals = _gdpool_safe(f"BODY{naif_id}_{f.pool_var}", f.n)
        if vals is None:
            continue
        vals = (list(vals) + [0.0, 0.0, 0.0])[:3]
        # Layer 2 scalar at J2000:
        body["fields"][f.attr] = ([vals[0]], f.unit,
                                  f"{f.desc} at J2000")
        # Layer 3 polynomial coeffs:
        body["fields"][f"{f.attr}_coeffs"] = (
            vals, "POLY", f"{f.desc} polynomial coefficients (J2000, rate, quad)"
        )
        # rotation_rate is the linear coefficient of PM (deg/day):
        if f.pool_var == "PM":
            body["fields"]["rotation_rate"] = (
                [vals[1]], "u.deg / u.day",
                "Sidereal rotation rate (linear PM coefficient)",
            )
    # Derived (build-time) — only if we have radii
    if "radii" in body["fields"]:
        a, b, c = body["fields"]["radii"][0]
        mean_r = (a + b + c) / 3.0
        vol_r = (a * b * c) ** (1.0 / 3.0)
        body["fields"]["mean_radius"] = (
            [mean_r], "u.km", "Mean radius (a + b + c) / 3"
        )
        body["fields"]["volume_radius"] = (
            [vol_r], "u.km", "Volume-equivalent radius (a * b * c)^(1/3)"
        )
        body["flattening"] = (a - c) / a if a > 0 else 0.0
    return body


def _body_dataclass_fields() -> set[str]:
    """Names of fields the Body dataclass actually accepts.

    Loads base.py directly via importlib so the regenerator works even
    when iau2009.py / iau2015.py are stale (e.g. the very thing this
    script is about to fix). Going through `planetarypy.constants` would
    transitively import the iauNNNN modules, which may not yet be
    consistent with the current base.py.
    """
    import importlib.util
    base_path = OUT_DIR / "base.py"
    mod_name = "_constants_base_isolated"
    spec = importlib.util.spec_from_file_location(mod_name, base_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = module        # dataclass needs this for FQN lookup
    try:
        spec.loader.exec_module(module)
        return set(module.Body.__dataclass_fields__.keys())
    finally:
        sys.modules.pop(mod_name, None)


def merge_nssdc(body: dict, nssdc_lookup: dict[int, dict],
                allowed_attrs: set[str]) -> None:
    """Add NSSDC fields to a body's `nssdc_fields` map (PCK fields untouched).

    Only adds fields that:
      - exist as attributes on the Body dataclass;
      - are not in PCK_DERIVED_FIELDS (PCK wins for those at build time);
      - have a non-None value in the NSSDC capture;
      - have a unit string the emitter can produce.
    """
    body["nssdc_fields"] = {}
    body["nssdc_source_date"] = None
    capture = nssdc_lookup.get(body["naif_id"])
    if capture is None:
        return
    # Prefer NSSDC's own "Last Updated" footer date over the Wayback
    # crawl timestamp; pick a verb that says which we're showing.
    page_date = capture.get("page_date")
    if page_date:
        date_str, verb = page_date, "updated"
    else:
        ts = capture["wayback_timestamp"]
        date_str, verb = f"{ts[0:4]}-{ts[4:6]}-{ts[6:8]}", "crawled"
    body["nssdc_source_date"] = date_str
    body_short = _NSSDC_BY_NAIF[body["naif_id"]]
    body["nssdc_source_string"] = (
        f"NSSDC {body_short}fact.html {verb} {date_str}"
    )
    for field_name, field_data in capture["fields"].items():
        if field_name not in allowed_attrs:
            continue   # NSSDC has fields Body doesn't model; skip silently
        if field_name in _PCK_DERIVED_FIELDS:
            continue
        if field_data.get("value") is None:
            continue
        unit = _normalize_nssdc_unit(field_data.get("unit"))
        emit = _NSSDC_UNIT_EMIT.get(unit)
        if emit is None:
            continue   # unknown unit, skip rather than emit broken code
        unit_expr, scale = emit
        scaled_value = float(field_data["value"]) * scale
        body["nssdc_fields"][field_name] = {
            "value": scaled_value,
            "unit_expr": unit_expr,
            "raw": field_data["value"],
            "raw_unit": field_data.get("unit"),
        }


# ── Python identifier generation ────────────────────────────────────────

def py_alias(name: str, taken: set[str], naif_id: int) -> str:
    """Convert a PCK body name to a Python-safe attribute name.

    - ``MARS`` → ``Mars``
    - ``CHURYUMOV-GERASIMENKO`` → ``Churyumov_Gerasimenko``
    - ``67P/CHURYUMOV-GERASIMENKO`` → ``_67P_Churyumov_Gerasimenko``
    - empty / unrecognized → ``BODY_<naif_id>``
    - collisions → suffixed with ``_<naif_id>``
    """
    safe = re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_")
    if not safe:
        safe = f"BODY_{naif_id}"
    elif safe[0].isdigit():
        safe = f"_{safe}"
    parts = safe.split("_")
    titled = "_".join(p.title() if p.isalpha() else p for p in parts)
    if titled in taken:
        titled = f"{titled}_{naif_id}"
    return titled


# ── Output formatting ───────────────────────────────────────────────────

def _fmt_quantity(values: list[float], unit_expr: str) -> str:
    """Format a Constant-friendly value × unit expression."""
    if len(values) == 1:
        return f"{values[0]!r} * {unit_expr}"
    inner = ", ".join(repr(v) for v in values)
    return f"[{inner}] * {unit_expr}"


def _fmt_constant_kwarg(attr: str, vals: list[float], unit_expr: str,
                        desc: str, body_name: str) -> str:
    """Format one ``attr=Constant(...)`` argument inside a Body() block."""
    qty = _fmt_quantity(vals, unit_expr)
    return (
        f"    {attr}=Constant(\n"
        f"        {qty},\n"
        f"        name={attr!r}, body={body_name!r},\n"
        f"        description={desc!r},\n"
        f"        **_meta,\n"
        f"    ),"
    )


def _fmt_poly_coeffs(attr: str, vals: list[float], pool_var: str) -> str:
    """Format a polynomial-coefficients tuple of three Quantities."""
    units = POLY_COEFF_UNITS[pool_var]
    parts = [f"{vals[i]!r} * {units[i]}" for i in range(3)]
    return f"    {attr}=({', '.join(parts)}),"


def format_body_block(body: dict, py_name: str) -> str:
    """Emit one ``Mars = Body(...)`` block."""
    lines = [
        f"{py_name} = Body(",
        f"    name={body['name']!r},",
        f"    naif_id={body['naif_id']},",
        f"    body_class={body['body_class']!r},",
        f"    parent={body['parent']!r},",
        f"    dwarf_planet={body['dwarf_planet']},",
        f"    mission_visited={body['mission_visited']},",
    ]
    fields = body["fields"]

    # PCK-derived fields only. GM lives in _gm_jpl.py (JPL DE440, not
    # IAU-versioned) and NSSDC fields live in constants.nssdc — both
    # are merged onto the top-level Body at import time. Keeping
    # iauNNNN.py PCK-only makes the module name honest about its
    # contents and prevents NSSDC/GM publication cadence from
    # triggering iau-module rewrites.
    for attr in (
        "radii", "long_axis",
        "pole_ra", "pole_dec", "pm", "rotation_rate",
        "mean_radius", "volume_radius",
    ):
        if attr not in fields:
            continue
        vals, unit_expr, desc = fields[attr]
        lines.append(_fmt_constant_kwarg(
            attr, vals, unit_expr, desc, py_name,
        ))

    # Layer-3 polynomial coefficients (3-tuple of Quantities, not Constant)
    for attr, pool_var in (("pole_ra_coeffs", "POLE_RA"),
                           ("pole_dec_coeffs", "POLE_DEC"),
                           ("pm_coeffs", "PM")):
        if attr not in fields:
            continue
        vals, _unit_marker, _desc = fields[attr]
        lines.append(_fmt_poly_coeffs(attr, vals, pool_var))

    # Flattening: bare float, no units
    if "flattening" in body and body["flattening"] is not None:
        lines.append(f"    flattening={body['flattening']!r},")

    lines.append(")")
    return "\n".join(lines)


def format_module(year: int, pck_filename: str, reference: str,
                  bodies: list[dict]) -> str:
    """Emit the full ``iauNNNN.py`` module text."""
    header = f'''"""planetarypy.constants.iau{year} — Generated from {pck_filename}.

DO NOT EDIT: this module is regenerated by ``scripts/regenerate_constants.py``
from the upstream NAIF PCK file.

This module is PCK-only by design: it contains exactly the
cartographic and orientation fields that the IAU PCK edition
publishes (radii, pole_ra/dec, pm, rotation_rate, polynomial coeffs,
derived mean/volume radius, flattening, long_axis). It does **not**
contain GMs (those live in :mod:`planetarypy.constants._gm_jpl`,
sourced from JPL DE440 and not IAU-edition-versioned) or NSSDC
fact-sheet values (those live in :mod:`planetarypy.constants.nssdc`
with their own time-indexed history). Users normally import from the
top-level :mod:`planetarypy.constants` namespace, which composes PCK
+ GM + NSSDC into one Body per body at import time.

Reference: {reference}
"""
import astropy.units as u

from .base import Body, BodyRegistry, Constant, century as _century

PCK_SOURCE = {pck_filename!r}
IAU_YEAR = {year}
REFERENCE = {reference!r}

_meta = dict(reference=REFERENCE, iau_year=IAU_YEAR, source=PCK_SOURCE)


'''
    blocks: list[str] = [header]

    # Assign Python aliases (deterministic by NAIF id)
    taken: set[str] = set()
    aliases: dict[int, str] = {}
    for body in bodies:
        alias = py_alias(body["name"], taken, body["naif_id"])
        aliases[body["naif_id"]] = alias
        taken.add(alias)

    # Body blocks, one per body, in NAIF-id order
    for body in bodies:
        blocks.append(format_body_block(body, aliases[body["naif_id"]]))
        blocks.append("")  # blank line between bodies

    # Registry: NAIF id → Body
    blocks.append("\nbodies = BodyRegistry({")
    for body in bodies:
        blocks.append(f"    {body['naif_id']}: {aliases[body['naif_id']]},")
    blocks.append("})\n")

    return "\n".join(blocks)


# ── Main ────────────────────────────────────────────────────────────────

def regenerate(iau_year: int, common: list[KernelEntry], edition: Edition) -> None:
    """Generate iau{year}.py by furnshing the per-edition PCK + every
    common kernel listed in the manifest, then walking the kernel pool.
    """
    pck_path = SOURCES_DIR / edition.pck
    out_path = OUT_DIR / f"iau{iau_year}.py"

    if not pck_path.is_file():
        raise FileNotFoundError(
            f"Missing PCK: {pck_path}\n"
            f"  Download from: {edition.url}\n"
            f"  Drop in _sources/ before running."
        )
    missing_common = [
        k for k in common if not (SOURCES_DIR / k.file).is_file()
    ]
    if missing_common:
        details = "\n".join(
            f"  - {k.file}\n      url: {k.url}\n      purpose: {k.purpose}"
            for k in missing_common
        )
        raise FileNotFoundError(
            f"Missing {len(missing_common)} common kernel(s) declared in "
            f"manifest:\n{details}"
        )

    # Furnsh order matters: the per-edition PCK first (so its broad
    # definitions get established), then mission-specific kernels (which
    # can override / augment for their target body), and gm_de440 last
    # so its GMs win over any older GM values.
    kernels_to_furnsh: list[Path] = [pck_path]
    kernels_to_furnsh += [SOURCES_DIR / k.file for k in common]

    for path in kernels_to_furnsh:
        sp.furnsh(str(path))
    try:
        ids = discover_naif_ids()
        bodies = [extract_body(i) for i in sorted(ids)]
    finally:
        for path in reversed(kernels_to_furnsh):
            sp.unload(str(path))

    # Drop bodies that have NO usable fields (frame-only / metadata-only entries)
    bodies = [b for b in bodies if b["fields"]]
    # Drop barycenters: they're orbital centers, not physical bodies. The
    # GM kernel (gm_de440.tpc) defines GMs for planet barycenters but
    # users want `Mars.GM` to be the planet, not its barycenter.
    bodies = [b for b in bodies if b["body_class"] != "barycenter"]

    # NSSDC values and GMs are no longer merged into iauNNNN.py — they
    # live in constants.nssdc and constants._gm_jpl respectively and are
    # composed onto the user-facing Body at import time. See the module
    # header docstring for the rationale.

    text = format_module(
        iau_year, edition.pck, edition.reference, bodies,
    )
    out_path.write_text(text)

    _print_summary(iau_year, bodies, out_path)


def _print_summary(iau_year: int, bodies: list[dict], out_path: Path) -> None:
    counts = Counter(b["body_class"] for b in bodies)
    print(f"iau{iau_year} → {out_path.relative_to(REPO_ROOT)}  "
          f"({len(bodies)} bodies)")
    for cls in ("sun", "planet", "moon", "asteroid", "comet",
                "barycenter", "other"):
        if counts[cls]:
            print(f"  {cls:11s} {counts[cls]:>4}")
    others = [b for b in bodies if b["body_class"] == "other"]
    if others:
        print("  ↑ 'other' deserves a classification update — review:")
        for b in others:
            print(f"     {b['naif_id']:>10}  {b['name']}")


GM_OUT_PATH = OUT_DIR / "_gm_jpl.py"
GM_KERNEL_FILENAME = "gm_de440.tpc"
GM_SOURCE_LABEL = "JPL DE440 planetary ephemeris (gm_de440.tpc, 2020)"


def regenerate_gm(common: list[KernelEntry]) -> None:
    """Emit ``constants/_gm_jpl.py`` from the JPL GM kernel alone.

    GMs aren't IAU-edition-versioned — they track JPL DE-series
    ephemeris releases instead — so they live in their own module and
    get composed onto the user-facing Body at import time, regardless
    of which IAU edition's PCK supplies the cartographic fields.
    """
    gm_kernel = next(
        (k for k in common if k.file == GM_KERNEL_FILENAME), None,
    )
    if gm_kernel is None:
        raise RuntimeError(
            f"Manifest is missing the GM kernel entry ({GM_KERNEL_FILENAME})."
        )
    gm_path = SOURCES_DIR / gm_kernel.file
    if not gm_path.is_file():
        raise FileNotFoundError(
            f"Missing GM kernel: {gm_path}\n  url: {gm_kernel.url}"
        )

    sp.furnsh(str(gm_path))
    try:
        gms: dict[int, float] = {}
        for name in sp.gnpool("BODY*_GM", 0, 5000):
            m = re.match(r"BODY(-?\d+)_GM$", name)
            if not m:
                continue
            naif_id = int(m.group(1))
            vals = _gdpool_safe(name, 1)
            if vals is None:
                continue
            gms[naif_id] = vals[0]
    finally:
        sp.unload(str(gm_path))

    # Emit a single dict of {naif_id: Constant} so composition is a
    # plain lookup. Per-body Constants carry the JPL source string in
    # their provenance metadata.
    header = f'''"""planetarypy.constants._gm_jpl — Generated from {GM_KERNEL_FILENAME}.

DO NOT EDIT: regenerated by ``scripts/regenerate_constants.py``.

GMs (gravitational parameters) are not IAU-edition-versioned — they
track JPL DE-series ephemeris releases. This module is the single
source of truth for GM; it's composed onto every Body in
:mod:`planetarypy.constants` at import time, regardless of which IAU
PCK edition supplied the cartographic fields.
"""
import astropy.units as u

from .base import Constant

GM_SOURCE = {GM_KERNEL_FILENAME!r}
GM_REFERENCE = {GM_SOURCE_LABEL!r}

_meta = dict(
    reference=GM_REFERENCE,
    source=GM_SOURCE,
    iau_year=0,   # GMs aren't IAU-versioned
)

'''
    # body=''  is intentional — the composition step in
    # constants/__init__.py copies each GM Constant onto its PCK Body
    # and stamps the body name there. Leaving it empty here keeps
    # _gm_jpl.py independent of how PCK names its bodies.
    lines = [header, "gms: dict[int, Constant] = {"]
    for naif_id in sorted(gms):
        lines.append(
            f"    {naif_id}: Constant("
            f"{gms[naif_id]!r} * u.km**3 / u.s**2, "
            f"name='GM', body='', "
            f"description='Gravitational parameter', "
            f"**_meta),"
        )
    lines.append("}")
    lines.append("")
    GM_OUT_PATH.write_text("\n".join(lines))
    print(f"_gm_jpl → {GM_OUT_PATH.relative_to(REPO_ROOT)}  "
          f"({len(gms)} GMs)")


def main() -> None:
    common, editions = _load_manifest()
    args = sys.argv[1:] or (
        [f"iau{y}" for y in sorted(editions)] + ["_gm_jpl"]
    )
    for arg in args:
        if arg == "_gm_jpl":
            regenerate_gm(common)
            continue
        try:
            year = int(arg.replace("iau", ""))
        except ValueError:
            print(f"Skipping unrecognized arg: {arg!r}", file=sys.stderr)
            continue
        if year not in editions:
            print(
                f"Unknown IAU year {year}; manifest declares: "
                f"{sorted(editions)}",
                file=sys.stderr,
            )
            continue
        regenerate(year, common, editions[year])


if __name__ == "__main__":
    main()
