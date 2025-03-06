# Package Modernization

This project has been modernized to use `pyproject.toml` as the primary configuration file, following PEP 517 and PEP 518 standards.

## Changes Made

1. Updated `pyproject.toml` to include:
   - All project metadata
   - Dependencies from requirements.txt
   - Development dependencies from requirements-dev.txt
   - Package build configuration
   - Tool configurations from setup.cfg (pytest, flake8, etc.)
   - Bumped version settings

2. Updated version in `src/planetarypy/__init__.py` to match `pyproject.toml`

## Files That Can Be Removed

The following files are now redundant and can be safely removed:

- `setup.py` - All configuration now in pyproject.toml
- `setup.cfg` - All tool configurations moved to pyproject.toml under appropriate [tool.*] sections
- `requirements.txt` - Dependencies now in pyproject.toml
- `requirements-dev.txt` - Development dependencies now in pyproject.toml
- `requirements_dev.txt` - Redundant with requirements-dev.txt

## Installation

With this modernization, you can install the package using:

```bash
# For development installation with all dev dependencies
pip install -e ".[dev]"

# For regular installation
pip install .
```

## Future Improvements

1. Consider using standardized tools for version management like `hatch` instead of `bumpversion`
2. Update documentation to reflect the new project structure
3. Update CI/CD pipelines to use the new configuration 