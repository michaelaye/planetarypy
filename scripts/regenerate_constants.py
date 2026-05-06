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

    # Static + Layer-2 scalars + derived (each as a Constant)
    for attr in (
        "radii", "GM", "long_axis",
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
from the upstream NAIF PCK file plus the auxiliary kernels listed in
``scripts/constants_kernels.toml``. To update: edit the manifest, drop
the new file(s) in ``src/planetarypy/constants/_pck_sources/``, and
rerun the script.

Reference: {reference}
"""
import astropy.units as u

from .base import Body, BodyRegistry, Constant, century as _century

PCK_SOURCE = {pck_filename!r}
IAU_YEAR = {year}
REFERENCE = {reference!r}

_meta = dict(reference=REFERENCE, iau_year=IAU_YEAR, pck_source=PCK_SOURCE)


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
            f"  Drop in _pck_sources/ before running."
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


def main() -> None:
    common, editions = _load_manifest()
    args = sys.argv[1:] or [f"iau{y}" for y in sorted(editions)]
    for arg in args:
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
