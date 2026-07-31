# Overnight work report — cross-archive Mars tutorial + sun-indicator debug

*Prepared autonomously overnight 2026-06-16. **Nothing is committed.** All changes
are in the working tree for your review. Scratch experiments live in `/tmp/plp_debug/`.*

---

## 1. Sun-vector feature — marked experimental **and** a real bug fixed

**File: `src/planetarypy/plotting.py` (uncommitted).**

You were right that it doesn't fully work. Empirically reproduced (see
`/tmp/plp_debug/sun_sweep_upperright.png`, before/after):

**Bug (confirmed, not by inspection — by rendering):** `add_sun_indicator` drew the
sun glyph in **data coordinates** anchored in a corner with the arrow pointing
*outward*. For any azimuth pointing off that corner (e.g. az=0/45/90/135 for "upper
right" — i.e. the *typical* sun-up cases), `ax.plot(sx, sy)` of an out-of-bounds
point **triggered matplotlib autoscale**, expanding the axes limits (e.g. ylim top
went `-0.5 → -62.5`), adding whitespace and pushing the sun ball *outside* the
image. It also assumed `origin='upper'`, so it'd mis-place on projected/`origin='lower'` plots.

**Fix applied:** redraw the glyph in **axes-fraction coordinates** with an inset
anchor (`inset = length + 0.06`) so the arrow stays inside the frame for *any*
azimuth, and drawing no longer touches the data limits (verified: limits now
constant across all 8 azimuths). This also removes the `origin` assumption.

**Marked experimental:** docstrings of `add_sun_indicator` + `imshow_with_sun` now
carry an experimental `.. warning::`, and `add_sun_indicator` emits a one-shot
`UserWarning`. The warning specifically flags the **unresolved** issue (below).

**Still genuinely experimental — the convention handoff is the real open question:**
- `add_sun_indicator` expects **azimuth CW-from-image-top** (PDS `SUB_SOLAR_AZIMUTH`).
- `Spicer.solar_azimuth_at` returns **azimuth CW-from-north** (geographic).
- These agree only when image-north points up. Feeding Spicer's value straight in
  is wrong for any rotated/projected image. This needs a documented conversion
  (subtract the image's north azimuth) before I'd call the feature stable.

**Regression test added:** `tests/test_plotting.py` (uncommitted) asserts
`add_sun_indicator` leaves `ax.get_xlim()/ylim()` unchanged across all 8 azimuths ×
4 corners, plus a check that the experimental `UserWarning` fires. 33 tests, green,
ruff-clean.

---

## 2. The cross-archive tutorial — obstacles found while actually doing it

Goal: "different datasets over the same Mars surface area" using **`planetarypy.search`
(NASA PDS)** + **`planetarypy.psa` (ESA PSA)**. I ran both for real against Jezero
Crater (77.4°E, 18.4°N). Findings, worst-first:

### ✅ CORRECTED 2026-06-17 — my original NASA findings here were wrong
The user supplied a working `pds.peppi` spatial example, which forced a re-investigation.
The corrections, all re-verified live against `pds.nasa.gov/api/search/1`:

- **Real bug (now FIXED in the working tree):** the registry requires the *whole* `q`
  wrapped in **outer parentheses** — `A and B` → HTTP 400, `(A and B)` → 200. peppi
  does this in `result_set.py` (`q = f"({query_string})"`); planetarypy's `_build_q`
  joined with ` and ` and omitted the wrap, so **every multi-filter query 400'd**.
  Fix applied (`search.py`): `return "(" + " and ".join(clauses) + ")"`. Verified —
  a target+observationals+cart-bbox query now returns correct Mars-in-box products.
- **Spatial search DOES work** (I was wrong it didn't): filter on
  `cart:Bounding_Coordinates.cart:{south,north,west,east}_bounding_coordinate` with
  `ge`/`le`. **Caveat (user):** these fields are only populated when the
  instrument/archive team did the work — common for derived/calibrated, often absent
  for raw/EDR. My overnight "no spatial" came from probing the *wrong* field name
  (`pds:…westernmost_longitude`) and a sample of products that lacked cart fields.
- **`target=` filter works correctly — and the results are NOT mislabeled** (I twice
  got this wrong). The Hayabusa2 `hyb2_nirs3` products returned for `target=Mars` are
  *genuinely* Mars-targeted: their own PDS4 label declares
  `pds:Target_Identification = Mars (Planet)`, mission phase "Transfer Phase"
  (cruise), 2016-05-31, with real SPICE Sun-target geometry — i.e. remote cruise
  observations of Mars (plausibly NIRS3 calibration; Hayabusa2 had no Mars flyby,
  only the 2015 Earth gravity assist), harvested by the PDS Small Bodies Node. So
  `target=Mars` correctly returns *every* Mars-targeted product across all
  missions/nodes; these spectra just sort ahead of MRO/TGO imagery. Narrow with an
  `instrument`/`lid` constraint to get the dataset you want. (JAXA = agency/namespace;
  Hayabusa2 = the mission.)

**Net:** the only planetarypy defect was the missing outer parens, and it's fixed.
peppi is adding a `within_bbox()` helper — worth mirroring in `planetarypy.search`
(see follow-ups).

### 🟠 Spatial search exists but isn't surfaced in either Python API (MEDIUM)
- **NASA:** there is **no dedicated spatial parameter/endpoint** in pds.api-client
  1.7.0, BUT (correction, see above) you *can* filter spatially through the `q`
  escape hatch on `cart:Bounding_Coordinates.cart:*_bounding_coordinate` — once the
  outer-parens bug is fixed. `planetarypy.search` exposes this only via the raw
  `query=` string today; a first-class bbox parameter (like peppi's `within_bbox`)
  would make it discoverable. Works only where the cart fields are populated.
- The high-level PSA API (`missions/instruments/datasets/examples`) is also
  non-spatial (spatial only via raw `psa.query` ADQL).
- **Workaround that works:** PSA's EPN-TAP table *is* spatially queryable via the raw
  `psa.query(adql)` escape hatch — `epn_core` exposes `c1min/c1max` (lon),
  `c2min/c2max` (lat), `s_region`. A bounding-box `WHERE` clause finds coverage. This
  is the strong, working showcase for the PSA side, but a user has to *know* to drop
  to raw ADQL — the spatial capability isn't surfaced in the Python API.

### 🟠 PSA bounding boxes degenerate for polar/long-track swaths (MEDIUM, data-quality)
Querying HRSC granules whose bbox contains Jezero returns many with
`lon[-180, 180] lat[-12, 90]` — **full-longitude, pole-touching boxes** from
orbital images that cross high latitudes. A naive `c1min ≤ lon ≤ c1max AND …` gives
**false positives**. Mitigations to teach: exclude wrap-around boxes
(`(c1max - c1min) < 90`), or use the per-product `psa.geometry_index()` (CENTER_LAT/LON),
or `s_region` polygons. (Nice tie-in to planetarypy's own
`geo.split_at_antimeridian` / `normalise_lon_bounds`.)

### 🟡 Smaller UX friction (LOW)
- **PSA longitude convention is −180..180**, undocumented in the API — a user will
  guess 0–360 (Jezero "77°E") and may get it wrong near the antimeridian.
- **NASA target/host filters require exact context LIDs** (`urn:nasa:pds:context:
  target:planet.mars`) with no name→LID helper.
- **NASA results use raw registry property names** as columns
  (`ops:Data_File_Info.ops:file_ref`) — not friendly; needs a rename/curation layer.
- `search_products(limit=…)` has **no pagination**; large areas silently truncate.
- **`geo.overlaps(gdf)` requires the id column to be named exactly `id`** — otherwise
  it raises a cryptic `KeyError: 'id_left'` (from the internal sjoin). It *is*
  documented to take `footprints_to_gdf` output, but a hand-built gdf with a
  differently-named id column fails opaquely. Minor; a clearer error would help.

### Reliable path the tutorial actually uses
- **PSA side:** `psa.query()` ADQL bbox spatial filter (+ degeneracy mitigation) →
  `psa.fetch_psa_product` → `planetarypy.open`.
- **NASA side:** planetarypy's **own PDS index system** (`pds.get_index` with corner
  lat/lon, e.g. `mro.ctx.edr` / `mro.hirise.rdr`) for reliable spatial selection —
  this is what planetarypy is genuinely good at. `search` shown only for
  registry-wide *existence* discovery, with the limitations above flagged inline.
- **Glue features exercised:** `constants` (Mars body/radii), `crs`
  (`local_crs` at Jezero), `geo` (footprints, `overlaps`, anti-meridian),
  `spicer` (illumination + the convention trap), `plotting` (the fixed, experimental
  sun indicator), `catalog`/`indexes`.

---

## 3. Tutorial deliverable
`docs/tutorials/cross_archive_mars_tutorial.qmd` (uncommitted, draft). Written against
the **verified** working paths above; network-heavy/large-download cells are marked
so it doesn't pull hundreds of MB on render. Not fully rendered overnight (would
download data); code is based on what I actually ran.

## 4. Suggested priorities for the morning
1. **Decide on the `search` filter bug** — it's the biggest issue; either fix the
   `q`/field handling or clearly mark `search_products`' keyword filters as
   unreliable in the docstring until fixed.
2. Review the sun-indicator fix + experimental marking (and whether to add the
   regression test).
3. Review the tutorial framing (esp. the PSA-spatial-via-raw-ADQL story and whether
   to promote a spatial helper into `psa`).
