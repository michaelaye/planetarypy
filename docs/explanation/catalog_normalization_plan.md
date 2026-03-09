# Catalog Product Key Normalization — Future Work

## Current State

BODY_MAP decomposes product keys like `edr_saturn` into `(type=edr, phase=saturn)`.
As of 2026-03-09, the following phases are recognized:

**Planets/bodies:** saturn, jupiter, neptune, uranus, earth, pluto, ceres, vesta,
gaspra, ida, halley, phobos, arrokoth

**Mission phases:** cruise, launch, kem_cruise, early_mission, late_mission,
pre_jupiter, earth_venus_jupiter

## Debatable Cases (not yet added)

These product keys contain body-like substrings, but adding them to BODY_MAP
risks false matches elsewhere. Each needs case-by-case evaluation.

### `flyby` as a phase (juno.jiram)

| product_key | would become |
|---|---|
| `flyby_img_edr` | type=`img_edr` phase=`flyby` |
| `flyby_logs` | type=`logs` phase=`flyby` |
| `flyby_spe_edr` | type=`spe_edr` phase=`flyby` |

**Risk:** `flyby` is generic. `mariner._misc.pos_flyby` would decompose to
`type=pos` phase=`flyby` — possibly correct but unverified. Any future product
key ending in `_flyby` would also match.

**Recommendation:** If added, also audit all `*flyby*` product keys first.

### `orbit` as a phase (rosetta.consert, mgs.mag_er, near.grs)

| mission | product_key | would become |
|---|---|---|
| rosetta.consert | `l2_orbit` | type=`l2` phase=`orbit` |
| rosetta.consert | `l3_orbit` | type=`l3` phase=`orbit` |
| rosetta.consert | `l3_orbit_fss` | type=`l3_orbit_fss` (won't match — fss suffix) |
| rosetta.consert | `l4_orbit` | type=`l4` phase=`orbit` |
| rosetta.consert | `l4_orbit_grnd` | type=`l4_orbit_grnd` (won't match) |
| mgs.mag_er | `orbit_info` | type=`info` phase=`orbit` |
| near.grs | `orbit` | standalone → type=`orbit` phase=`orbit` |

**Risk:** `orbit` is extremely generic. `orbit_info` at MGS is metadata about
orbits, not data from an "orbit phase". NEAR GRS `orbit` is the orbital phase
but treating it as standalone body key conflates it with actual targets.

**Recommendation:** Do not add globally. Consider mission-specific overrides if
the Rosetta CONSERT use case becomes important.

## Instrument-Level Target Names (not decomposable)

These product keys use target body names at the instrument level — the body name
is part of what the instrument *is*, not a phase modifier on the data type.

| mission | instrument | product_keys | notes |
|---|---|---|---|
| hst | mars_cube, mars_image | `mars_cube`, `mars_image` | Instrument is defined by target |
| iue | comet_extracted, comet_image, comet_raw | `comet_*` | Instrument is defined by target |
| pre_magellan | eb_mars_img, eb_venus, eb_moon, etc. | various | Dataset names, not phase splits |

These should remain as-is. The body name is structural, not a decomposable phase.

## Edge Cases

### `photom_halley_addenda` (ihw.irsn)

Body `halley` is in the middle of the key, not at suffix/prefix position.
Current normalization only strips bodies from prefix or suffix. This key
stays as `photom_halley_addenda` with no phase. Low priority — single product.

### `cassini.hp/rss.solar` and `voyager.mag/nh.swap.solar_wind`

`solar` appears in `solar_wind` (physical phenomenon) and as Cassini solar
occultation data. Both are in NORMALIZATION_EXCEPTIONS. Correct behavior.

### `mex.pfs.ATM_cruise_dupes`

`cruise` is in BODY_MAP but this key has prefix `ATM_` and suffix `_dupes`,
so the body stripping doesn't match. This is correct — the key describes
a special duplicate dataset, not a cruise-phase product.
