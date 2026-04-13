# PlanetaryPy CLI: plp_update_indexes

## Overview
Added a command-line interface for forcing update checks of PDS index configuration and dynamic URL discovery.

## Files Created/Modified

### 1. New CLI Module: `src/planetarypy/pds/cli.py`
- **Function**: `plp_update_indexes()`
- **Features**:
  - Force update check for main configuration URLs
  - Force discovery of dynamic URLs (CTX and LROC indices)
  - Verbose output with detailed progress
  - Selective updates with `--config-only` or `--dynamic-only` flags
  - Proper error handling and user-friendly messages

### 2. Updated Dependencies: `pyproject.toml`
- **Added**: `click` to core dependencies for CLI functionality
- **Added**: Script entry point `plp_update_indexes = "planetarypy.pds.cli:plp_update_indexes"`

### 3. Test Script: `test_cli.py`
- Validates CLI help functionality
- Tests CLI structure and argument parsing
- Provides usage examples

## CLI Usage

### Basic Commands
```bash
# Show help
plp_update_indexes --help

# Update everything with verbose output
plp_update_indexes --verbose

# Update only configuration URLs
plp_update_indexes --config-only

# Update only dynamic URLs
plp_update_indexes --dynamic-only
```

### Command Options
- `--verbose, -v`: Enable detailed progress output
- `--config-only`: Only check/update main configuration URLs
- `--dynamic-only`: Only check/update dynamic URLs (CTX, LROC)
- `--help`: Show usage information

## Functionality

### What It Does
1. **Configuration Update**: Forces check of remote configuration file for updates
2. **Dynamic URL Discovery**: Forces discovery of latest CTX and LROC index URLs
3. **Verbose Reporting**: Shows detailed progress and results
4. **Error Handling**: Graceful handling of network errors and other issues

### Output Examples
```
✅ Successfully checked configuration URLs and dynamic URLs for updates.

With --verbose:
Checking configuration URLs for updates...
Checking dynamic URLs for updates...
  ✅ mro.ctx.edr: Updated to https://planetarydata.jpl.nasa.gov/img/data/mro/ctx/mrox_5100//index/cumindex.lbl
  ℹ️  lro.lroc.edr: No change - https://pds.lroc.asu.edu/data/LRO-L-LROC-2-EDR-V1.0/LROLRC_0063A//INDEX/CUMINDEX.LBL
```

## Integration
- Integrates with existing `IndexURLsConfig` class
- Uses the same update mechanisms as automatic daily checks
- Respects all existing logging and timestamp tracking
- Works with the new access log structure (config timestamps at root, dynamic URLs in sections)

## Installation
After installing the package, the CLI will be available system-wide as `plp_update_indexes`.

## Dependencies
- **click**: For command-line interface framework
- All existing planetarypy dependencies (requests, tomlkit, loguru, etc.)

## Use Cases
1. **Manual Updates**: Force immediate update check without waiting for daily automatic checks
2. **Debugging**: Verbose mode helps troubleshoot update issues
3. **Selective Updates**: Update only specific components when needed
4. **CI/CD Integration**: Can be used in automated scripts and deployment pipelines
