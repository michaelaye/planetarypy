# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **PDS Catalog module** (`planetarypy.catalog`): comprehensive index of ~2000 product types across 200+ instruments from the entire PDS archive, built from MillionConcepts pdr-tests repository into a local DuckDB database
  - `build_catalog()` to clone pdr-tests and populate the database
  - Query API: `list_missions()`, `list_instruments()`, `list_product_types()`, `example_products()`, `search()`, `summary()`
  - Dotted key access: `list_product_types("mro.hirise")`, `example_products("cassini.iss.edr_sat")`
  - 150+ manual mission/instrument mappings for pdr-tests folder names
  - AST-based parser for selection_rules.py (no code execution)
  - Multi-instrument folder splitting: folders like `mro` correctly split into `ctx`, `hirise`, `marci`, `mcs` instruments based on product key prefixes; infix matching for Rosetta-style `EDR_instrument` keys
  - Product key normalization: decompose keys like `edr_sat`, `sat_rdr_asc` into 3 dimensions — `normalized_type` (data type), `phase` (target body/mission phase), `format` (ascii/binary/coordinate system)
  - `Mission` and `Instrument` objects with human-readable full names for all 65 missions and ~180 instruments (e.g. `Mission("mro").full_name` → "Mars Reconnaissance Orbiter", `mro["ctx"]` → "Context Camera")
  - Rosetta Lander (Philae) as separate mission entry with 8 properly split instruments
  - Voyager POS ephemeris override: standalone body keys normalized to `"ephemeris"` for pre-SPICE position data
  - Generic instrument groupings (spectrometers, particles, plasma, probe, dust, lander) decomposed into real instrument names across Galileo, NEAR, Rosetta, Voyager, Dawn, Deep Impact
  - `_misc` instruments hidden from `list_instruments()` by default; accessible via `include_misc=True` or `Mission.misc`
  - URL rewrite for broken USGS Imaging Node URLs (60 of 69 rewritten to SETI Rings and JPL Planetary Data mirrors)
  - Product download API: `fetch_product("mission.instrument.type", product_id)` downloads files and returns local path, `get_product_url()` returns remote URL, `list_product_files()` returns file-to-URL mapping
  - Index-backed resolution (Tier 2): arbitrary product IDs resolved via PDS cumulative indexes for CTX, HiRISE, Cassini ISS, Galileo SSI, LROC, Diviner, CRISM, LOLA, Cassini UVIS, and MER Pancam
  - Tutorial notebook in `docs/tutorials/pds_catalog_tutorial.ipynb`
  - CLI command `plp_build_catalog`
  - Optional dependency: `pip install planetarypy[catalog]`
- Dynamic URL handlers for LRO LAMP EDR and RDR indexes (volume-based URLs at JPL)
- Backup URL fallback for CTX index (pdsimage2.wr.usgs.gov)
- Quarto documentation with Diátaxis framework structure
- New how-to guides: CTX calibration, CTX EDR, ISIS autoseed
- Comprehensive API reference documentation
- `slow` pytest marker for test filtering
- SPICE datasets daily caching system
- Case-insensitive mission name resolution for SPICE

### Changed
- Migrated documentation from Sphinx to Quarto
- Split ctx.py into separate EDR and calibration modules
- Renamed HISTORY.md to CHANGELOG.md
- Adopted Keep a Changelog format

### Fixed
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
