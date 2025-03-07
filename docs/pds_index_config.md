# PDS Index Configuration

## Overview

The PDS (Planetary Data System) index configuration in PlanetaryPy has been redesigned to improve clarity and collaboration. The configuration is now split into separate files:

1. **General Configuration** (`~/.planetarypy_config.toml`): Stores general settings like `storage_root`
2. **URL Configuration** (`~/.planetarypy_index_urls.toml`): Stores URLs for PDS indices with a clear structure
3. **Access Log** (`~/.planetarypy_index.log`): Records timestamps of when indices were last accessed

This separation makes each file focused on a specific purpose, making the system easier to understand and maintain.

## URL Configuration File

The URL configuration file uses TOML format with a clean, organized hierarchy grouped by mission and instrument:

```toml
# PlanetaryPy Index URLs Configuration
# This file contains URLs for PDS indices, organized by mission and instrument

[missions]
# Cassini Mission
[missions.cassini]
# ISS Instrument
[missions.cassini.iss]
ring_summary = "https://pds-rings.seti.org/holdings/metadata/COISS_2xxx/COISS_2999/COISS_2999_ring_summary.lbl"
saturn_summary = "https://pds-rings.seti.org/holdings/metadata/COISS_2xxx/COISS_2999/COISS_2999_saturn_summary.lbl"
moon_summary = "https://pds-rings.seti.org/holdings/metadata/COISS_2xxx/COISS_2999/COISS_2999_moon_summary.lbl"

# UVIS Instrument
[missions.cassini.uvis]
ring_summary = "https://pds-rings.seti.org/holdings/metadata/COUVIS_0xxx/COUVIS_0999/COUVIS_0999_ring_summary.lbl"

# MRO Mission
[missions.mro]
# HiRISE Instrument
[missions.mro.hirise]
edr = "https://hirise-pds.lpl.arizona.edu/PDS/INDEX/EDRCUMINDEX.LBL"

# CTX Instrument
# Latest URL discovered on 2023-07-15
[missions.mro.ctx]
edr = "https://planetarydata.jpl.nasa.gov/img/data/mro/ctx/mrox_2103/index/cumindex.lbl"

# LRO Mission
[missions.lro]
# LROC Instrument
# Latest URL discovered on 2023-07-15
[missions.lro.lroc]
edr = "https://pds.lroc.asu.edu/data/LRO-L-LROC-2-EDR-V1.0/LROLRC_2101/INDEX/CUMINDEX.LBL"
```

This structure makes it easy to:

- Understand which mission and instrument an index belongs to
- Add new indices in a collaborative manner
- Share only the relevant URLs without timestamp or other data
- Visually organize related information into logical groups
- See the latest discovered URLs for dynamic indices like CTX and LROC

### Dynamic URLs

For some missions/instruments (like MRO/CTX and LRO/LROC), the URLs for the latest indices are determined dynamically by examining the mission data repository. The system will:

1. Automatically discover and store these URLs when creating a new configuration
2. Include a comment showing when the URL was discovered
3. Update the URLs when requested by the user

This ensures users always know where the latest index data is coming from, even before first accessing it.

## Access Log File

The access log uses JSON format and stores timestamps for each index:

```json
{
  "missions.cassini.iss.ring_summary": "2023-03-15T14:30:22.123456",
  "missions.mro.hirise.edr": "2023-03-10T09:45:12.654321"
}
```

This separation ensures that:

- Timestamps are kept private to each user
- The URL configuration can be shared without sharing access history
- The configuration is clearer and more focused

## Migration from Old Configuration

The old configuration system combined all settings in a single file, including both general settings and PDS index information. To migrate to the new system, run:

```
planetarypy-migrate-config
```

This command:

1. Extracts PDS index URLs and timestamps from the general configuration file
2. Creates the new URL configuration and access log files
3. Populates them with the extracted PDS index data
4. Leaves other general settings (like `storage_root`) in the original config file

For verbose output during migration:

```
planetarypy-migrate-config --verbose
```

To also refresh dynamic URLs during migration:

```
planetarypy-migrate-config --refresh-urls
```

## Checking for Updates

The system provides ways to check for updated index files, both for dynamic URLs (CTX, LROC) and for timestamps on existing indices.

### Command-line Update Checking

To check all known indices for updates:

```
planetarypy-migrate-config --check-updates
```

Or with the standalone script:

```
python run_migration.py --check-updates
```

This will:
1. Check for new dynamic URLs for CTX and LROC indices
2. Check for updated timestamps on all known indices
3. Print user-friendly notifications about any available updates

Example output:
```
Checking for updates to all known indices...
Checking for updates to dynamic index URLs...
Update available: New mro.ctx.edr index found!
  Previous: https://planetarydata.jpl.nasa.gov/img/data/mro/ctx/mrox_2101/index/cumindex.lbl
  New: https://planetarydata.jpl.nasa.gov/img/data/mro/ctx/mrox_2103/index/cumindex.lbl

Checking individual indices for updates...
Update available: Newer version of missions.cassini.iss.ring_summary index found!
  Current version: 2023-03-15T14:30:22.123456
  Available version: 2023-07-22T10:15:45.789012

Update check complete.
```

### Programmatic Update Checking

You can also check for updates programmatically:

```python
from planetarypy.pds import Index

# Create an index
idx = Index("cassini.iss.ring_summary")

# Check for updates (will print notifications)
result = idx.check_for_updates()

# Check silently and handle the result
result = idx.check_for_updates(print_result=False)
if result["has_update"]:
    print(f"Update found for {idx.key}!")
    
    if "url_update" in result["details"]:
        print(f"New URL available: {result['details']['url_update']['url']}")
        
    if "timestamp_update" in result["details"]:
        print(f"New timestamp: {result['details']['timestamp_update']['remote']}")
```

## Refreshing Dynamic URLs

You can manually refresh the dynamic URLs to get the latest index locations:

```
planetarypy-migrate-config --refresh-urls
```

Or using the standalone script:

```
python run_migration.py --refresh-urls
```

This will:
1. Check the mission repositories for the latest index files
2. Update the URLs in the configuration file
3. Add a timestamp comment showing when the URL was discovered
4. Print notifications when new URLs are found

## Programmatic Access

You can access the configuration programmatically:

```python
# For PDS index URLs and timestamps
from planetarypy.pds import urls_config, access_log

# Get URL for an index
url = urls_config.get_url("cassini.iss.ring_summary")

# Set URL for an index
urls_config.set_url("cassini.iss.ring_summary", "https://example.com/path/to/index.lbl")

# Get timestamp for an index
timestamp = access_log.get_timestamp("cassini.iss.ring_summary")

# Set timestamp for an index
from datetime import datetime
access_log.set_timestamp("cassini.iss.ring_summary", datetime.now())

# Discover and store the latest dynamic URLs
updates = urls_config.discover_dynamic_urls()
for key, info in updates.items():
    if info.get("is_update", False):
        print(f"New URL found for {key}")

# For general configuration (storage_root, etc.)
from planetarypy.config import config

# Get the storage root
storage_path = config.storage_root

# Set a general configuration value
config.set_value("custom_setting", "value")
```

## Benefits of the New System

1. **Separation of Concerns**: Each configuration file has a single, clear purpose
2. **Organization**: The TOML file is structured into logical groups by mission and instrument
3. **Simplicity**: Direct mapping of index name to URL without redundant nesting
4. **Discoverability**: Dynamic URLs are proactively discovered and stored for reference
5. **Update Notifications**: Users are informed when new index files are available
6. **Collaboration**: Easier to share and contribute URL configurations
7. **Privacy**: Access timestamps are kept separate
8. **Maintainability**: Simpler structure makes it easier to manage and update
9. **Extensibility**: Easy to add new indices and missions 