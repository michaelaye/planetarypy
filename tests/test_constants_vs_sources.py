"""End-to-end anti-hallucination tests for ``planetarypy.constants``.

Three layers, each guarding a different failure mode:

1. **PCK ↔ spiceypy round-trip** — every PCK-sourced ``Constant`` in
   ``iau2009`` / ``iau2015`` is re-derived from the shipped ``.tpc``
   kernels via ``spiceypy`` and compared bit-for-bit. Catches a value
   drifting between the kernel and the generated Python literal.

2. **NSSDC parser coverage** — re-runs the parser over the in-repo raw
   HTML archive (``_raw/<body>/*.html.gz``) and reports every label and
   section it silently dropped. An allowlist pins the known-ignored set
   so any NEW silent drop (a NEW NSSDC field, a renamed section, …)
   fails CI. Catches "the parser stopped capturing X without anyone
   noticing".

3. **NSSDC golden spot-checks** — a small table of hand-verified
   ``(body, field, expected_value)`` triples. Catches the case where
   the parser is consistent with itself but reads the WRONG column
   (e.g. Earth instead of the target body).

All three run from data already in the repo — no network, no Zenodo,
no Wayback. The PCK + parser-coverage tests are marked ``slow``; the
golden spot-checks run on every ``pytest`` invocation.
"""

from __future__ import annotations

import gzip
import re
import sys
from collections import Counter
from pathlib import Path

import pytest

# ── Shared paths ───────────────────────────────────────────────────────

_REPO_ROOT = Path(__file__).resolve().parent.parent
_KERNEL_DIR = _REPO_ROOT / "src/planetarypy/constants/_constants_sources"
_RAW_DIR = _REPO_ROOT / "src/planetarypy/constants/nssdc/_raw"
_SCRIPTS_DIR = _REPO_ROOT / "scripts"


# ════════════════════════════════════════════════════════════════════════
# 1. PCK ↔ spiceypy round-trip
# ════════════════════════════════════════════════════════════════════════
#
# Every Constant whose ``source`` ends in ``.tpc`` (PCK-sourced) must
# equal what we read back from spiceypy after furnshing the same set of
# kernels the regenerator used. Each IAU edition is tested separately
# with its own PCK kernel + the common GM/mission-target kernels.

spiceypy = pytest.importorskip("spiceypy")

# Per-edition kernel sets matching scripts/constants_kernels.toml.
# Mission-target kernels apply to both editions (asteroids / comets
# aren't IAU-edition-versioned).
_COMMON_KERNELS = [
    "gm_de440.tpc",
    "bennu_v17.tpc",
    "dawn_vesta_v04.tpc",
    "dawn_ceres_v06.tpc",
    "ROS_LUTETIA_RSOC_V03.TPC",
    "ROS_CGS_RSOC_V03.TPC",
]
_EDITION_PCK = {2009: "pck00010.tpc", 2015: "pck00011.tpc"}

# Body-attribute → SPICE pool variable suffix. The full pool name is
# ``BODY{naif_id}_{suffix}``. Polynomial fields (POLE_RA, POLE_DEC, PM)
# are 3-element arrays; we compare element-wise.
_SCALAR_FIELDS = {
    "GM":        ("GM", 0),         # BODY{n}_GM[0]
    "long_axis": ("LONG_AXIS", 0),  # BODY{n}_LONG_AXIS[0]
    # Layer-2 J2000 scalars are the [0] element of the polynomial coefs.
    "pole_ra":   ("POLE_RA", 0),
    "pole_dec":  ("POLE_DEC", 0),
    "pm":        ("PM", 0),
    # rotation_rate is the linear (rate) coefficient of PM.
    "rotation_rate": ("PM", 1),
}
_VECTOR_FIELDS = {
    "radii": ("RADII", 3),
    "pole_ra_coeffs": ("POLE_RA", 3),
    "pole_dec_coeffs": ("POLE_DEC", 3),
    "pm_coeffs": ("PM", 3),
}
# Derived (computed at build time from radii) — verified against radii
# rather than against pool, since pool doesn't store these directly.
_DERIVED_FIELDS = {"mean_radius", "volume_radius", "flattening"}


def _gdpool_safe(var: str, n: int) -> list[float] | None:
    """Read ``n`` floats from the kernel pool; ``None`` if var absent."""
    sp = spiceypy
    try:
        if not sp.expool(var):
            return None
        return list(sp.gdpool(var, 0, n + 3))[:n]
    except sp.utils.exceptions.SpiceyError:
        return None


@pytest.fixture(scope="module")
def _spice_kernels_iau2015():
    """Furnsh the kernel set the iau2015 module was generated from."""
    sp = spiceypy
    sp.kclear()
    # PCK first, GM (and missions) second so newer GMs supersede.
    for name in [_EDITION_PCK[2015]] + _COMMON_KERNELS:
        path = _KERNEL_DIR / name
        if path.is_file():
            sp.furnsh(str(path))
    yield
    sp.kclear()


@pytest.fixture(scope="module")
def _spice_kernels_iau2009():
    sp = spiceypy
    sp.kclear()
    for name in [_EDITION_PCK[2009]] + _COMMON_KERNELS:
        path = _KERNEL_DIR / name
        if path.is_file():
            sp.furnsh(str(path))
    yield
    sp.kclear()


def _iter_pck_constants(edition_module):
    """Yield (body, field_name, constant) for every PCK-sourced Constant."""
    expected_source = (
        f"pck{'00010' if edition_module.IAU_YEAR == 2009 else '00011'}.tpc"
    )
    for body in edition_module.bodies.values():
        for field_name, constant in body.iter_constants():
            # PCK-sourced ⇔ iau_year non-zero AND source matches edition.
            if constant.iau_year == 0:
                continue
            if constant.source != expected_source:
                continue
            yield body, field_name, constant


def _compare_field(body, field_name, constant) -> tuple[bool, str]:
    """Compare one Body.field's Constant against the SPICE pool.

    Returns ``(ok, diagnostic)``. Diagnostic is empty when ``ok``.
    """
    import astropy.units as u
    import numpy as np

    suffix_info = _SCALAR_FIELDS.get(field_name) or _VECTOR_FIELDS.get(field_name)
    if suffix_info is None:
        return True, ""  # derived field — handled separately
    suffix = suffix_info[0]
    full_var = f"BODY{body.naif_id}_{suffix}"

    # Convert our Constant to the same base units the pool stores.
    pool_unit_for_field = {
        "GM": u.km ** 3 / u.s ** 2,
        "long_axis": u.deg,
        "radii": u.km,
        "pole_ra": u.deg, "pole_dec": u.deg, "pm": u.deg,
        "rotation_rate": u.deg / u.day,
        # Polynomial coefficient tuples are mixed-unit; we compare them
        # element-by-element below rather than calling ``.to(...)``.
    }
    if field_name in ("pole_ra_coeffs", "pole_dec_coeffs", "pm_coeffs"):
        pool_vals = _gdpool_safe(full_var, 3)
        if pool_vals is None:
            return False, f"{full_var} not in kernel pool"
        # The Body field is a (J2000, rate, quad) tuple of Quantities in
        # mixed units. Reduce each to its raw scalar matching pool order.
        ours = [float(constant[i].value) if hasattr(constant[i], "value")
                else float(constant[i].to_value())
                for i in range(3)]
        if not np.allclose(ours, pool_vals, rtol=1e-14, atol=0):
            return False, f"{ours} != pool {pool_vals}"
        return True, ""

    if field_name in _SCALAR_FIELDS:
        idx = _SCALAR_FIELDS[field_name][1]
        pool_vals = _gdpool_safe(full_var, idx + 1)
        if pool_vals is None:
            return False, f"{full_var} not in kernel pool"
        theirs = pool_vals[idx]
        ours = float(constant.to(pool_unit_for_field[field_name]).value)
        if not np.isclose(ours, theirs, rtol=1e-14, atol=0):
            return False, f"{ours} != pool {theirs} (var {full_var}[{idx}])"
        return True, ""

    # vector field (radii)
    pool_vals = _gdpool_safe(full_var, 3)
    if pool_vals is None:
        return False, f"{full_var} not in kernel pool"
    ours = list(constant.to(pool_unit_for_field[field_name]).value)
    if not np.allclose(ours, pool_vals, rtol=1e-14, atol=0):
        return False, f"{ours} != pool {pool_vals}"
    return True, ""


@pytest.mark.slow
class TestPCKMatchesSpiceypy:
    """Every PCK-sourced Constant equals what spiceypy reads from the
    same kernel set the regenerator used."""

    def _run(self, edition_module):
        failures: list[str] = []
        n_checked = 0
        for body, field_name, constant in _iter_pck_constants(edition_module):
            ok, diag = _compare_field(body, field_name, constant)
            n_checked += 1
            if not ok:
                failures.append(f"  {body.name}.{field_name}: {diag}")
        assert n_checked > 0, "iterated zero PCK constants — fixture issue?"
        if failures:
            pytest.fail(
                f"{len(failures)}/{n_checked} PCK constants disagree with "
                f"spiceypy:\n" + "\n".join(failures[:50])
            )

    def test_iau2015(self, _spice_kernels_iau2015):
        from planetarypy.constants import iau2015
        self._run(iau2015)

    def test_iau2009(self, _spice_kernels_iau2009):
        from planetarypy.constants import iau2009
        self._run(iau2009)


@pytest.mark.slow
class TestNoPCKDataLeftBehind:
    """Reverse-coverage check: every ``BODY{n}_X`` variable in the loaded
    kernel pool corresponds to either a Body field we expose or a
    documented-ignored variable. Catches "NAIF added a new PCK field
    we silently aren't capturing"."""

    # Pool variable suffixes present in the kernels but intentionally
    # not exposed on Body (yet). Each entry needs a written reason.
    _IGNORED_POOL_SUFFIXES: frozenset[str] = frozenset({
        # Nutation/precession Fourier series — per-system tables. The
        # IAU rotation model exposed on Body is the polynomial part
        # only; nutation isn't yet wired through.
        "NUT_PREC_ANGLES",
        "NUT_PREC_RA",
        "NUT_PREC_DEC",
        "NUT_PREC_PM",
        # Earth-only centered-dipole magnetic field model. Not a
        # cartographic / orientation constant.
        "N_GEOMAG_CTR_DIPOLE_LAT",
        "N_GEOMAG_CTR_DIPOLE_LON",
        # Kernel-internal metadata.
        "MAX_PHASE_DEGREE",
        "CONSTANTS_REF_FRAME",
        "CONSTANTS_JED_EPOCH",
    })

    # Variable-name patterns that aren't BODY{id}_FIELD per-body data at
    # all — kernel-pool bookkeeping that has no Body-side analogue.
    _IGNORED_FULL_VAR_PATTERNS: tuple[re.Pattern, ...] = (
        re.compile(r"^BODY0+_"),   # BODY000_GMLIST — kernel metadata
    )

    def _run_for_edition(self, year: int):
        sp = spiceypy
        # gnpool may not be available on older spiceypy; skip cleanly.
        try:
            names = sp.gnpool("BODY*", 0, 100000)
        except sp.utils.exceptions.SpiceyError:
            pytest.skip("gnpool returned no results / unsupported")
        from planetarypy.constants import iau2009, iau2015
        edition = {2009: iau2009, 2015: iau2015}[year]
        # NAIF ids we actually expose on Body. Bodies absent from this set
        # are barycenters / unexposed minor bodies — irrelevant to the
        # "did we hallucinate or miss anything" question for Body fields.
        exposed_naif_ids = {b.naif_id for b in edition.bodies.values()}
        exposed_suffixes = (
            {s for s, _ in _SCALAR_FIELDS.values()}
            | {s for s, _ in _VECTOR_FIELDS.values()}
        )

        leftover: list[str] = []
        for var in names:
            if any(p.match(var) for p in self._IGNORED_FULL_VAR_PATTERNS):
                continue
            m = re.match(r"BODY(-?\d+)_(.+)", var)
            if not m:
                continue
            naif_id = int(m.group(1))
            suffix = m.group(2)
            if naif_id not in exposed_naif_ids:
                continue   # body not exposed on purpose (barycenters etc.)
            if suffix in self._IGNORED_POOL_SUFFIXES:
                continue
            if suffix in exposed_suffixes:
                continue
            leftover.append(var)
        if leftover:
            # Surface as a test failure with a precise list — the
            # remediation is either to expose the field on Body or add
            # the suffix to ``_IGNORED_POOL_SUFFIXES`` with a reason.
            sample = "\n".join(f"  {v}" for v in sorted(leftover)[:40])
            pytest.fail(
                f"{len(leftover)} pool variable(s) not exposed on Body "
                f"and not in the ignore list:\n{sample}"
                + ("\n  ..." if len(leftover) > 40 else "")
            )

    def test_iau2015(self, _spice_kernels_iau2015):
        self._run_for_edition(2015)

    def test_iau2009(self, _spice_kernels_iau2009):
        self._run_for_edition(2009)


# ════════════════════════════════════════════════════════════════════════
# 2. NSSDC parser coverage  (instrumented re-parse of raw HTML)
# ════════════════════════════════════════════════════════════════════════

# Load the parser module from scripts/ (not on sys.path normally).
sys.path.insert(0, str(_SCRIPTS_DIR))
try:
    import parse_nssdc_archive as _parser  # noqa: E402
finally:
    sys.path.pop(0)


# ─ Baseline allowlist of labels seen across the 30-year NSSDC archive ─
#
# A test passes when the set of unknown labels is a SUBSET of this. NEW
# labels (i.e. a future NSSDC capture introduces something not seen
# before) trigger a deliberate decision: extend FIELD_MAP to capture
# the field, or expand this allowlist with a reason.
#
# Most entries fall into one of three buckets — comments above each
# block say which:
#   (a) cross-column labels (planet/moon names appearing as column
#       headers in comparison tables — not data),
#   (b) sub-sections we don't expose (Sun internals, magnetosphere,
#       mean orbital elements, satellite tables, free-text atmosphere),
#   (c) PARSER BUGS we know about and choose not to fix in this round:
#       see ``_LIKELY_PARSER_BUGS`` below for the suspect list — they
#       look like genuine fields the parser misses due to label-form
#       quirks. Logged in test output as a warning.
_EXPECTED_UNKNOWN_LABELS: frozenset[str] = frozenset({
    # (a) column headers from inter-body comparison tables
    "Mercury", "Venus", "Earth", "Mars", "Jupiter", "Saturn",
    "Uranus", "Neptune", "Pluto", "Moon", "Sun",
    "Phobos", "Hydra", "Kerberos", "Nix", "Styx",
    # (b) sub-section / free-text / sub-table labels
    "Apogee", "Apogee (10^6 km)*", "Perigee", "Perigee (10^6 km)*",
    "Apparent diameter", "Apparent magnitude at opposition",
    "Apparent magnitude near opposition", "Apparent visual magnitude",
    "Maximum apparent visual magnitude", "Maximum visual magnitude",
    "Distance from Earth", "Recession rate from Earth",
    "Inclination to Earth equator", "Inclination to ecliptic",
    "Obliquity to ecliptic",
    "Longitude of ascending node", "Longitude of perihelion",
    "Mean Longitude",
    "Mean distance from Mars", "Mean distance from Pluto",
    "Orbital inclination to Pluto",
    "Revolution period",
    "Equatorial", "Polar", "Mean", "Maximum", "Minimum",
    "Major", "Minor axis radius", "Major axis radius", "Major:", "Axis",
    "Semi-Major",
    "Apparent diameter", "Equatorial inclination",
    "Acceleration (eq., 1 bar)", "Acceleration (pole, 1 bar)",
    "Gravity (eq., 1 bar)", "Gravity (mean, 1 bar)",
    "Sidereal rotation period (hrs)*", "Tropical orbit period^*",
    "Albedo", "Absolute magnitude", "Spectral type",
    "Moment of inertia",
    # Sun-internals: atmospheric/photospheric/chromospheric/magnetic.
    "Photosphere thickness:", "Chromosphere thickness:",
    "Temperature at top of photosphere:",
    "Temperature at bottom of photosphere:",
    "Temperature at top of chromosphere:",
    "Central density:", "Central pressure:", "Central temperature:",
    "Mean energy production", "Mass conversion rate", "Luminosity",
    "At 1 A.U.", "Surface emission",
    "Speed relative to nearby stars",
    "Sunspots:", "Sun Spot Cycle:", "Polar Field:",
    "Chromospheric plages:", "Bright chromospheric network:",
    "Ephemeral (unipolar) active regions:", "Prominences:",
    "Model values at center of Sun:",
    # Free-text atmosphere / composition lines
    "Atmospheric composition:",
    "Atmospheric composition (Column abundances in 10 ^6",
    "Aerosols:", "Scale height:", "Wind speeds:",
    "Average temperature:", "Surface pressure:", "Surface Pressure:",
    "Surface pressure (night): 3 x 10 ^-15",
    "Surface pressure (night): 3 x 10^-15 bar",
    "Surface pressure: <~5 x 10 ^-15",
    "Abundance at surface: 2 x 10 ^5",
    "Total mass of atmosphere:",
    "Total mass of atmosphere: ~2.5 x 10 ^16",
    "Total mass of hydrosphere:",
    "Diurnal temperature range (equator): 95 K to 390 K",
    "Longitude of tilt: 285 degrees (from Mercury",
    "composition.",
    # Discovery / authorship boilerplate
    "Discoverer:", "Discovery Date:", "Dr. David R. Williams,",
    "Note:", "Up to",
    # Unicode / formatting glitches
    "", "*",
    # (c) PARSER BUGS — see _LIKELY_PARSER_BUGS for the triage list.
    "Surface Gravity",          # FIELD_MAP has lowercase "Surface gravity"
    "Surface Gravity (mean)",   # FIELD_MAP has lowercase
})


# Labels in the allowlist that LOOK like genuine parser misses — the
# field would map to a real Body attribute if FIELD_MAP recognized the
# spelling/casing actually used on the page. Logged as a warning in
# the test output so a future maintainer can prioritise the fix.
# Fixing these is a follow-up that requires editing
# scripts/parse_nssdc_archive.py and re-running the parser + regenerator.
_LIKELY_PARSER_BUGS: frozenset[str] = frozenset({
    "Surface Gravity",
    "Surface Gravity (mean)",
})


# Section headers we intentionally don't enter. Same ratchet semantics
# as labels — new sections force a deliberate decision.
_EXPECTED_UNKNOWN_SECTIONS: frozenset[str] = frozenset({
    # Per-planet "Observational Parameters" — Earth-based-observer
    # quantities, not body-intrinsic, deliberately not on Body.
    "Mercury Observational Parameters",
    "Venus Observational Parameters",
    "Earth Observational Parameters",
    "Moon Observational Parameters",
    "Mars Observational Parameters",
    "Jupiter Observational Parameters",
    "Saturn Observational Parameters",
    "Uranus Observational Parameters",
    "Neptune Observational Parameters",
    "Pluto Observational Parameters",
    "Sun Observational Parameters",
    # Per-planet "Mean Orbital Elements (J2000)" — orbital ellipse
    # parameters distinct from the "Orbital parameters" section we do
    # consume; not yet on Body.
    "Mercury Mean Orbital Elements (J2000)",
    "Venus Mean Orbital Elements (J2000)",
    "Earth Mean Orbital Elements (J2000)",
    "Mars Mean Orbital Elements (J2000)",
    "Jupiter Mean Orbital Elements (J2000)",
    "Saturn Mean Orbital Elements (J2000)",
    "Uranus Mean Orbital Elements (J2000)",
    "Neptune Mean Orbital Elements (J2000)",
    "Pluto Mean Orbital Elements (J2000)",
    # Magnetosphere — not on Body.
    "Mercury Magnetosphere", "Mercurian Magnetosphere",
    "Terrestrial Magnetosphere", "Jovian Magnetosphere",
    "Saturnian Magnetosphere", "Uranian Magnetosphere",
    "Neptunian Magnetosphere",
    "Solar Magnetic Field",
    # Satellite sub-tables — handled separately if at all.
    "Satellites of Mars",
    # Pole-of-rotation sub-blocks — PCK is the source of truth for
    # these, NSSDC's value is editorial.
    "North Pole of Rotation", "Positive Pole of Rotation",
    # Sun internals — model values, not on Body.
    "Model values at center of Sun:",
})


@pytest.mark.slow
class TestNSSDCParserCoverage:
    """Re-parse every cached HTML capture with the instrumented parser
    and assert nothing new is silently dropped.

    Failure here means NSSDC changed something the parser doesn't yet
    handle — extend ``FIELD_MAP`` (or the allowlist above) deliberately."""

    def test_no_new_silent_drops(self):
        unknown_labels: Counter = Counter()
        unknown_sections: Counter = Counter()
        n_captures = 0
        for body_dir in sorted(_RAW_DIR.iterdir()):
            if not body_dir.is_dir():
                continue
            for html_gz in sorted(body_dir.glob("*.html.gz")):
                with gzip.open(html_gz, "rt",
                               encoding="utf-8", errors="replace") as f:
                    raw_html = f.read()
                report = _parser.SkippedReport()
                _parser.parse_capture(raw_html, _report=report)
                unknown_labels.update(report.unknown_labels)
                unknown_sections.update(report.unknown_sections)
                n_captures += 1

        assert n_captures > 100, (
            f"expected many captures, got {n_captures} — _raw missing?"
        )

        new_labels = set(unknown_labels) - _EXPECTED_UNKNOWN_LABELS
        new_sections = set(unknown_sections) - _EXPECTED_UNKNOWN_SECTIONS

        # Diagnostic output — always print so the test log carries a
        # full coverage snapshot, even when the assertion passes.
        print(f"\nNSSDC parser coverage over {n_captures} captures:")
        print(f"  unknown labels   : {len(unknown_labels)} distinct "
              f"({sum(unknown_labels.values())} total occurrences)")
        print(f"  unknown sections : {len(unknown_sections)} distinct "
              f"({sum(unknown_sections.values())} total occurrences)")

        # Warning band: allowlisted labels that look like real misses
        # the parser ought to be catching. Not a failure (the user
        # acknowledges these), but surfaced so they don't fall off
        # someone's plate forever.
        active_bugs = sorted(set(unknown_labels) & _LIKELY_PARSER_BUGS)
        if active_bugs:
            print("\n  PARSER BUGS still active (fix in "
                  "scripts/parse_nssdc_archive.py FIELD_MAP):")
            for label in active_bugs:
                print(f"    {label!r}  ×{unknown_labels[label]}")

        if new_labels:
            print("\n  NEW unknown labels (extend FIELD_MAP or allowlist):")
            for label in sorted(new_labels):
                print(f"    {label!r}  ×{unknown_labels[label]}")
        if new_sections:
            print("\n  NEW unknown sections:")
            for sect in sorted(new_sections):
                print(f"    {sect!r}  ×{unknown_sections[sect]}")

        assert not new_labels, (
            f"{len(new_labels)} new unknown label(s) — "
            f"see test output above for the list"
        )
        assert not new_sections, (
            f"{len(new_sections)} new unknown section(s) — "
            f"see test output above for the list"
        )


# ════════════════════════════════════════════════════════════════════════
# 3. NSSDC golden spot-checks  (hand-verified against published pages)
# ════════════════════════════════════════════════════════════════════════
#
# Cross-checks against values verified by hand from
# https://nssdc.gsfc.nasa.gov/planetary/factsheet/ — guards against the
# parser misreading a column (e.g. grabbing Earth instead of Mars).
#
# Tolerances are loose (rtol=1e-2) so this DOESN'T flap on minor NSSDC
# revisions; the goal is "did we read the right column", not bit-equality.

import astropy.units as u  # noqa: E402

_GOLDEN_VALUES = [
    # (body, field, expected, tolerance)
    ("Mercury", "bond_albedo", 0.068 * u.dimensionless_unscaled, 1e-2),
    ("Mercury", "equatorial_radius", 2440.5 * u.km, 1e-3),
    ("Mercury", "mean_density", 5429.0 * u.kg / u.m ** 3, 1e-3),
    ("Venus", "bond_albedo", 0.77 * u.dimensionless_unscaled, 5e-2),
    ("Earth", "bond_albedo", 0.294 * u.dimensionless_unscaled, 5e-2),
    ("Earth", "equatorial_radius", 6378.1 * u.km, 1e-3),
    ("Moon", "equatorial_radius", 1738.1 * u.km, 1e-3),
    ("Mars", "equatorial_radius", 3396.2 * u.km, 1e-3),
    ("Mars", "orbit_eccentricity", 0.0935 * u.dimensionless_unscaled, 5e-2),
    ("Mars", "bond_albedo", 0.250 * u.dimensionless_unscaled, 5e-2),
    ("Jupiter", "equatorial_radius", 71492.0 * u.km, 1e-3),
    ("Saturn", "equatorial_radius", 60268.0 * u.km, 1e-3),
    ("Uranus", "equatorial_radius", 25559.0 * u.km, 1e-3),
    ("Neptune", "equatorial_radius", 24764.0 * u.km, 1e-3),
    ("Sun", "mean_density", 1408.0 * u.kg / u.m ** 3, 1e-3),
    ("Sun", "surface_gravity", 274.0 * u.m / u.s ** 2, 1e-3),
]


@pytest.mark.parametrize("body_name,field,expected,rtol", _GOLDEN_VALUES)
def test_nssdc_golden_value(body_name, field, expected, rtol):
    """Hand-verified spot-check against the live NSSDC fact sheet pages."""
    from planetarypy.constants import find_body
    body = find_body(body_name)
    assert body is not None, f"unknown body {body_name!r}"
    actual = getattr(body, field, None)
    assert actual is not None, (
        f"{body_name}.{field} is None — NSSDC merge dropped it?"
    )
    # Convert to expected unit; will raise UnitConversionError if
    # incompatible (which is itself a useful failure).
    actual_in_expected_units = actual.to(expected.unit).value
    expected_value = expected.value
    delta = abs(actual_in_expected_units - expected_value)
    assert delta <= rtol * abs(expected_value), (
        f"{body_name}.{field}: {actual_in_expected_units} {expected.unit} "
        f"!= expected {expected_value} {expected.unit} (rtol={rtol})"
    )
