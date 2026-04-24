# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.53.2] - 2026-04-24

### Fixed
- **CTX storage path mismatch between `plp fetch` and `ctxqv`/`EDR`.** `plp fetch mro.ctx.edr <pid>` previously wrote to `{storage_root}/mro/ctx/edr/<pid>/` (the catalog's generic layout), while `EDR(pid).local_storage_folder` (used by `plp ctxqv` and programmatic access) wrote to `{storage_root}/mro/ctx/<volume>/…` per `~/.planetarypy_mro_ctx.toml`. Downloads from one code path were invisible to the other, causing redundant re-downloads. Both paths now resolve through the single `ctx_storage_folder(level, volume, pid)` helper and land in the same directory.

### Changed
- CTX storage layout for `plp fetch mro.ctx.edr` now follows `~/.planetarypy_mro_ctx.toml` (`[edr].local_storage`, `[edr].with_volume`, `[edr].with_pid`) instead of the generic catalog fallback. A new `_ctx_local_product_dir` resolver is registered in `planetarypy.catalog._resolver._STORAGE_RESOLVER_MODULES`.
- CTX config (`CTXCONFIG`, mirror reachability) is now read lazily on each access rather than snapshotted at import — mounting or unmounting the local mirror mid-session is reflected immediately, and `with_volume` / `with_pid` config edits take effect without re-importing.
- `EDR.with_volume`, `EDR.with_pid`, `Calib.with_volume`, `Calib.with_pid` are now `@property` reads of `CTXCONFIG` (previously snapshotted in `__init__`).

### Removed
- Shadow duplicate `EDR` class in `planetarypy.instruments.mro.ctx.ctx_calib` (leftover from the original module split). `EDR` is now defined once in `ctx_edr.py` and imported from there.
- Module-level globals `STORAGE_ROOT`, `EDR_LOCAL_STORAGE`, `EDR_LOCAL_MIRROR`, `MIRROR_READABLE`, `MIRROR_WRITEABLE` in `ctx_edr.py` and their lowercase counterparts in `ctx_calib.py`. Replaced by lazy accessors (`_storage_root`, `_edr_local_mirror`, `_mirror_readable`, `_level_base`).

### Migration notes
- If you previously ran `plp fetch mro.ctx.edr <pid>` and have existing downloads under `{storage_root}/mro/ctx/edr/<pid>/`, they will **not** be picked up by the new layout. Either move them to `{storage_root}/mro/ctx/<volume>/[<pid>/]` (per your `[edr].with_volume` / `[edr].with_pid` toggles), delete them, or re-run `plp fetch` to download into the new location.

## [0.52.2] - 2026-04-13

### Fixed
- Only show satellite ephemeris download message when actually downloading (not when loading from cache)

## [0.52.1] - 2026-04-13

### Fixed
- Spicer now works for outer solar system bodies (Jupiter, Saturn, Neptune, Pluto systems) — satellite ephemeris SPKs are downloaded on demand when needed
- Graceful fallback in CLI when SPICE ephemeris data is missing (shows what it can instead of crashing)
- Suppress pvl PendingDeprecationWarning (pvl#109)

## [0.52.0] - 2026-04-13

> *Dedicated to the memory of Candice J. Hansen — scientist, mentor, and friend. This release was built in a single long push fueled by the urgency that reminds us our time to contribute is finite.*

### Added
- **Spicer class** (`planetarypy.spice.spicer`): surface illumination calculator for any solar system body
  - `Spicer("MARS").illumination(lon, lat, time)` — solar incidence, flux, L_s, local time
  - `slope` and `aspect` parameters for tilted surface flux (south-facing slopes etc.)
  - `solar_azimuth_at(lon, lat, time)` — SPICE-computed solar azimuth, validated to <1° against HiRISE index
  - `.Ls` property for current solar longitude
  - `sun_direction_at()` for azimuth calculation via Point class
  - `illumination_at(point)` integration with `planetarypy.geo.Point`
  - `supported_bodies()` — discover all bodies available from loaded kernels (79 from generic PCK)
  - `units=True` toggle for astropy Quantity output
  - Rotation via scipy (matching SPICE right-hand convention)
- **CLI**: `plp spicer Mars` — current L_s, subsolar point, solar constant; add `--lon --lat` for surface illumination
- Spicer tutorial with diurnal flux curve, slope/aspect comparison, multi-body demo, and HiRISE index validation

## [0.51.0] - 2026-04-13

### Added
- **Geospatial module** (`planetarypy.geo`): GDAL-free coordinate transforms built on rasterio + pyproj
  - `pixel_to_xy`, `xy_to_pixel`, `pixel_to_lonlat`, `lonlat_to_pixel`, `xy_to_lonlat`, `lonlat_to_xy`
  - `is_within(source, lon, lat)`: check if coordinates fall within an image
  - `image_azimuth`: clockwise from north (standard planetary science)
  - `image_azimuth_cw_from_right`: clockwise from 3 o'clock (HiRISE convention)
  - `pixel_resolution`: pixel size from affine transform
  - Works with IAU 2015 planetary CRS codes (Mars, Moon, any solar system body)
- **Point class** (`planetarypy.geo.Point`): CRS-aware geographic point
  - Create from lon/lat, pixel coordinates, or projected coordinates
  - `.to_xy()`, `.to_pixel()`, `.is_within()`, `.azimuth_to()`, `.to_shapely()`
  - Auto-resolves pixel↔lonlat when source DataArray is provided
- **Plotting module** (`planetarypy.plotting`): visualization helpers
  - `imshow_gray`: grayscale image display with percentile stretch
  - `percentile_stretch`: reusable stretch calculation
  - `add_sun_indicator`: sun direction overlay on any axes
  - `imshow_with_sun`: combined image display + sun indicator
- **HiRISE instrument module** (`planetarypy.instruments.mro.hirise`)
  - `get_browse(pid, annotated=True)`: download browse JPEG (annotated or clean)
  - `get_metadata(pid)`: look up index metadata
  - `sun_azimuth_from_top(pid)`: convert HiRISE CW-from-right to CW-from-top
- **CLI**: `plp hibrowse --annotated/--clean` option for browse variant
- **Geospatial tutorial**: pixel↔lonlat transforms, IAU CRS codes, Point class, sun indicator verification with real HiRISE data (ESP_013807_2035)
- `rasterio`, `pyproj`, `rioxarray` added to core dependencies

### Changed
- `plp hibrowse` and `plp hifetch` now use `planetarypy.instruments.mro.hirise` module instead of inline CLI helpers
- `plp ctxqv` uses `planetarypy.plotting.imshow_gray` instead of duplicating stretch logic

## [0.50.1] - 2026-04-13

### Fixed
- Move duckdb from optional `[catalog]` extra to core dependency (was breaking installs)
- Pandas 2.x compatibility: datetime64 resolution (ns→us) and string dtype (object→StringDtype) in test assertions
- Mock catalog DB in tests that hit `get_catalog()` (fixes CI without a built catalog)
- Stale docs: rewrite pds_index_config explanation, update landing page, fix tutorial tier numbering

### Added
- Logo in docs navbar and browser favicon

## [0.50.0] - 2026-04-13

> **Breaking change:** This is a ground-up rewrite with a new API.
> The previous version (0.32.x) based on nbplanetary remains at
> [github.com/michaelaye/nbplanetary](https://github.com/michaelaye/nbplanetary).

### Added
- **PDS Catalog module** (`planetarypy.catalog`): comprehensive index of ~2000 product types across 200+ instruments from the entire PDS archive, built from MillionConcepts pdr-tests repository into a local DuckDB database
  - `build_catalog()` to clone pdr-tests and populate the database
  - Query API: `list_missions()`, `list_instruments()`, `list_products()`, `example_products()`, `search()`, `summary()`
  - Dotted key access: `list_products("mro.hirise")`, `example_products("cassini.iss.edr_sat")`
  - 150+ manual mission/instrument mappings for pdr-tests folder names
  - AST-based parser for selection_rules.py (no code execution)
  - Multi-instrument folder splitting: folders like `mro` correctly split into `ctx`, `hirise`, `marci`, `mcs` instruments based on product key prefixes; infix matching for Rosetta-style `EDR_instrument` keys
  - Product key normalization: decompose keys like `edr_sat`, `sat_rdr_asc` into 3 dimensions — `normalized_type` (data type), `phase` (target body/mission phase), `format` (ascii/binary/coordinate system). Recognized phases include planets (saturn, jupiter, neptune, uranus, earth, pluto), minor bodies (ceres, vesta, gaspra, ida, arrokoth, phobos, halley), and mission phases (cruise, launch, kem_cruise)
  - `Mission` and `Instrument` objects with human-readable full names for all 65 missions and ~180 instruments (e.g. `Mission("mro").full_name` → "Mars Reconnaissance Orbiter", `mro["ctx"]` → "Context Camera")
  - Rosetta Lander (Philae) as separate mission entry with 8 properly split instruments
  - Voyager POS ephemeris override: standalone body keys normalized to `"ephemeris"` for pre-SPICE position data
  - Generic instrument groupings (spectrometers, particles, plasma, probe, dust, lander) decomposed into real instrument names across Galileo, NEAR, Rosetta, Voyager, Dawn, Deep Impact
  - `_misc` instruments hidden from `list_instruments()` by default; accessible via `include_misc=True` or `Mission.misc`
  - URL rewrite for broken USGS Imaging Node URLs (60 of 69 rewritten to SETI Rings and JPL Planetary Data mirrors)
  - Product download API: `fetch_product("mission.instrument.type", product_id)` downloads files and returns local path, `get_product_url()` returns remote URL, `list_product_files()` returns file-to-URL mapping
  - Index-backed resolution (Tier 2): 58 product types across 29 instruments on 15 missions — arbitrary product IDs resolved via PDS cumulative indexes for CTX, HiRISE, Cassini ISS, Galileo SSI, LROC, Diviner, CRISM, LOLA, Cassini UVIS/VIMS/CIRS, Voyager 1&2 ISS, Juno JunoCam, New Horizons LORRI, MER Pancam, MGS MOC, Viking VIS, MESSENGER MDIS, Cassini RSS, Phoenix MECA instruments (WCL/AFM/TECP/ELEC), and MSL (APXS, ChemCam, CheMin, SAM)
  - Pattern-based URL resolution (Tier 3): for product types with fixed `url_stem`, resolve arbitrary product IDs without needing a PDS index
  - Per-archive URL construction: `IndexConfig` supports `path_name_col` (for FILE_NAME + PATH_NAME split indexes), `lowercase_paths`/`lowercase_files` flags, `volume_id_col=""` to skip volume in URL, and `seti_volume_group="auto"` to derive SETI volume groups dynamically
  - Verified HTTP 200 for all 50 testable registry entries (7 indexes not yet downloaded, 1 removed)
  - Explanation doc: `docs/explanation/product_url_resolution.qmd` with full direct data access status table
  - Tutorial notebook in `docs/tutorials/pds_catalog_tutorial.ipynb`
- **Unified CLI** (`plp`): single entry point built on typer
  - `plp fetch mro.ctx.edr PRODUCT_ID` — download any product by dotted key
  - `plp fetch --here` — download into current directory
  - `plp hibrowse PSP_003092_0985_RED` — fetch HiRISE browse JPEG from EXTRAS, opens in Preview on macOS
  - `plp hifetch PSP_003092_0985_RED` — fetch full HiRISE data product
  - `plp ctxqv J05_046771_1950` — CTX quickview with strided memmap
  - `plp catalog build` — build/rebuild the catalog database
- **CTX quickview** (`EDR.quickview`, `Calib.quickview`): memory-mapped strided reads for fast previews
- Dynamic URL handlers for LRO LAMP EDR and RDR indexes (volume-based URLs at JPL)
- Backup URL fallback for CTX index (pdsimage2.wr.usgs.gov)
- Quarto documentation with Diátaxis framework structure
- New how-to guides: CTX calibration, CTX EDR, ISIS autoseed
- Comprehensive API reference documentation
- `slow` pytest marker for test filtering
- SPICE datasets daily caching system
- Case-insensitive mission name resolution for SPICE

### Changed
- Renamed `list_product_types()` → `list_products()` in catalog API
- Restructured catalog internals: `_download.py` → `_resolver.py`, `_index_bridge.py` → `_index_resolver.py`, `_url_patterns.py` + `_url_examiner.py` → `_pattern_resolver.py`
- Replaced click with typer for CLI; single `plp` entry point replaces `plp_update_indexes`, `plp_build_catalog`, `ctxqv`
- Migrated documentation from Sphinx to Quarto
- Split ctx.py into separate EDR and calibration modules
- Renamed HISTORY.md to CHANGELOG.md
- Adopted Keep a Changelog format

### Removed
- Dead code: `exceptions.py` (unused), `pds/cli.py` (broken imports), `scripts/` package, old Click-based CLIs

### Fixed
- Index bridge URL construction: correct case handling per archive server (lowercase paths for JPL/WUSTL, preserve case for SETI Rings/HiRISE), proper volume_id/path_name column support, and use actual filenames from index instead of guessing extensions
- Ambiguous product resolution: raises `MultipleProductsError` when an ID matches multiple products (e.g. HiRISE observation matching both RED and COLOR)
- Canonical PID casing: local storage paths use the index's canonical case, not the user's input
- Removed LRO LAMP from index registry (index lacks volume mapping needed for URL construction)
- Cloudflare 403 errors via User-Agent header in remote timestamp checks
- Repeated Index instantiations in dynamic handler
- Test failures from outdated DataFrame schema

## [0.41.2] - 2025-01-26

### Added
- pooch dependency for data downloads

## [0.41.1] - 2025-01-26

### Fixed
- Repeated Index instantiations in dynamic index handler

## [0.1.0] - 2020-10-20

### Added
- First release on PyPI
- PDS index management system
- SPICE kernel utilities
- Basic configuration management

---

## Historical Versions (Previous Project Incarnations)

> **Note:** The following versions are from predecessor projects that evolved into
> the current planetarypy. They are included for historical reference and are not
> installable from this repository.

### nbplanetary era (2021-2025)

Using nbdev (notebook-driven development) for a complete rewrite.

- **v0.27.0** (2023-06) - BepiColombo SPICE kernels, PDW2023 tutorials
- **v0.26.0** (2023-03) - CRISM and LROC index handlers
- **v0.25.0** (2023-02) - Spicer class, ISIS integration via kalasiris
- **v0.21.x** (2022-02) - Full instrument modules: CTX, HiRISE, UVIS, CISS, Diviner

### Original planetarypy era (2015-2020)

Private collection of planetary science tools, inspired by astropy's organization.

- **v0.9.0** (2020-07) - North azimuth and sun angle calculations
- **v0.8.0** (2020-07) - GeoTools module from pymars (coordinate transforms)
- **v0.7.0** (2020-06) - url_retrieve with progress bar and timeout
- **v0.6.0** (2020-05) - Powerful Index class, config system
- **v0.5.0** (2019-07) - Renamed to planetarypy (from planetpy)
- **v0.4.0** (2019-03) - PDS index download CLI, Cassini ISS indices
- **v0.1** (2015-04) - NASA factsheet planetary constants parser
