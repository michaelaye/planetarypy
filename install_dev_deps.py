#!/usr/bin/env python
"""Install the dev environment's dependencies with conda, pip for the rest.

Stdlib only, on purpose: this is the script that *creates* the environment, so
it has to run in a bare one. It used to import ``sh`` and ``tomlkit``, so you
needed those installed before you could run the installer — a chicken-and-egg
that CI hid behind its own `install sh tomlkit` step and that nothing told a
human about.

``tomllib`` here is not a rejection of ``tomlkit``: the rest of the package uses
tomlkit deliberately, because it round-trips a TOML file without flattening its
comments and structure, and ``config.py`` / ``utils.py`` write user config files
back. This script only *reads* three dependency lists out of pyproject and
discards the document, so that fidelity buys nothing — and being stdlib is worth
something, because this is the bootstrap. Both packages are still installed
normally: tomlkit is a core dependency, sh is in the dev extra.

conda rather than mamba: conda ships the libmamba solver, so the separate mamba
binary is no longer needed.
"""
import logging
import subprocess
import sys
import tomllib
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)

# Define packages that should be installed via pip instead of conda
PIP_PACKAGES = {"build", "pip-tools", "planets"}

# Define packages that should NEVER be installed via pip
CONDA_ONLY_PACKAGES = {"gdal"}

# Core packages that must be installed first
CORE_PACKAGES = {"gdal"}


def get_package_name(dep):
    """Extract the package name from a dependency string, removing version specifiers."""
    return (
        dep.split(">=")[0]
        .split("==")[0]
        .split("<=")[0]
        .split("~=")[0]
        .split("!=")[0]
        .strip()
    )


def run(cmd, what, packages=()):
    """Run an installer, streaming its output; exit(1) with context on failure."""
    if packages:
        logger.info("Packages: %s", " ".join(packages))
    try:
        subprocess.run(cmd, check=True)
    except FileNotFoundError:
        logger.error("Error installing %s: %r not found on PATH.", what, cmd[0])
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        logger.error("Error installing %s!", what)
        logger.error("Exit code: %s", e.returncode)
        logger.error("Command: %s", " ".join(cmd))
        sys.exit(1)
    logger.info("%s installed successfully!", what.capitalize())


def conda_install(packages):
    return ["conda", "install", "-y", "-c", "conda-forge", *packages]


def pip_install(packages):
    # sys.executable rather than a bare `pip`, so packages land in the
    # interpreter running this script rather than whichever pip is on PATH.
    return [sys.executable, "-m", "pip", "install", *packages]


def install_deps():
    logger.info("\n=== Starting install_dev_deps.py ===")

    logger.info("\nInstalling core dependencies via conda...")
    run(conda_install(sorted(CORE_PACKAGES)), "core dependencies", sorted(CORE_PACKAGES))

    logger.info("\nReading pyproject.toml...")
    with open(Path("pyproject.toml"), "rb") as f:
        pyproject = tomllib.load(f)

    logger.info("\nCollecting dependencies from pyproject.toml...")
    main_deps = pyproject["project"]["dependencies"]
    dev_deps = pyproject["project"]["optional-dependencies"]["dev"]
    spice_deps = pyproject["project"]["optional-dependencies"]["spice"]
    all_deps = main_deps + dev_deps + spice_deps

    logger.info("Found %d main dependencies", len(main_deps))
    logger.info("Found %d dev dependencies", len(dev_deps))
    logger.info("Found %d spice dependencies", len(spice_deps))
    logger.info("Total dependencies to process: %d", len(all_deps))

    logger.info("\nSplitting dependencies between conda and pip...")
    conda_deps = [
        dep
        for dep in all_deps
        if get_package_name(dep) not in PIP_PACKAGES
        and get_package_name(dep) not in CORE_PACKAGES
    ]
    pip_deps = [
        dep
        for dep in all_deps
        if get_package_name(dep) in PIP_PACKAGES
        and get_package_name(dep).lower() not in CONDA_ONLY_PACKAGES
    ]

    logger.info("Packages to install via conda: %d", len(conda_deps))
    logger.info("Packages to install via pip: %d", len(pip_deps))

    if conda_deps:
        logger.info("\nInstalling conda packages...")
        run(conda_install(conda_deps), "conda packages", conda_deps)

    if pip_deps:
        logger.info("\nInstalling pip packages...")
        run(pip_install(pip_deps), "pip packages", pip_deps)

    logger.info("\n=== install_dev_deps.py completed successfully ===")


if __name__ == "__main__":
    install_deps()
