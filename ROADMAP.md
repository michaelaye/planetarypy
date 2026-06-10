# PlanetaryPy Roadmap

Development priorities as of v0.52.1.

## High Priority

- **Verify EDR sun azimuth convention**: RDR is verified (CW from 3 o'clock, convert with `(az + 90) % 360`), but EDR may differ since images are unprojected.
- **Terrain module**: `SlopeAspect.from_raster("dtm.tif")` to extract slope/aspect from a DTM (via GDAL or richdem as optional dep), then `mars.flux_on_terrain(sa, time)` for spatially varying flux maps.
- **MCP Server**: expose planetarypy as tools for Claude and other LLMs
  - `pds_query`: spatial/temporal search across PDS indexes ("find CTX images covering this lat/lon")
  - `spicer`: illumination calculations as a tool ("solar incidence at Jezero right now")
  - `browse_image`: fetch and return HiRISE/CTX browse images for inline display
  - `catalog_search`: query the catalog DB ("what instruments observed Enceladus?")

## Medium Priority

- **Christian Tai Udovicic CRS module**: planned integration for easy access to all major solar system CRSs via IAU codes.
- **Automatic sun indicator from index**: `imshow_with_sun` should accept a product ID and auto-fetch the solar azimuth from the PDS index, handling the convention conversion internally.
- **CTX sun indicator**: verify and document CTX `SUB_SOLAR_AZIMUTH` convention.
- **Manifest-based URL resolution**: if pdr-tests manifest parquets become available, build compressed lookup tables for non-indexed product types (currently only ~58 of 2042 product types support direct download).
- **Claude Code skills**: `/mars`, `/pds <key>`, `/hibrowse <pid>` slash commands.

## Low Priority

- **Analytical orbital formulas**: Keplerian approximations for L_s, subsolar point without SPICE kernels (e.g. Allison & McEwen 2000 for Mars). ~0.1° accuracy vs SPICE ~0.001°. Useful as fallback when kernels aren't available.
- **North arrow indicator**: add to plotting module alongside sun indicator.
- **HiRISE EXTRAS**: more product types (IRB, NOMAP, anaglyphs, DTM hillshade).
- **Uranus system**: no small satellite SPK available from NAIF currently.

## Recently Completed (v0.50–0.52)

- PDS Catalog with 65 missions, 2042 product types, direct data access for 58 types
- Unified CLI (`plp fetch`, `hibrowse`, `hifetch`, `ctxqv`, `spicer`, `catalog build`)
- Spicer illumination calculator with slope/aspect, solar azimuth validated against HiRISE
- Geo module with Point class, IAU CRS support, coordinate transforms
- Plotting module with sun indicator
- HiRISE instrument API (`get_browse`, `get_metadata`, `sun_azimuth_from_top`)
- On-demand satellite ephemeris for Jupiter, Saturn, Neptune, Pluto systems
- Quarto documentation site with tutorials
