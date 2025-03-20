#!/usr/bin/env python
import logging
import sys
from pathlib import Path

import sh
import tomlkit

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)

# Define packages that should be installed via pip instead of conda
PIP_PACKAGES = {"build", "pip-tools", "planets"}

# Define packages that should NEVER be installed via pip
CONDA_ONLY_PACKAGES = {"gdal"}

# Core packages that must be installed first
CORE_PACKAGES = {"gdal"}


def install_deps():
    logger.info("\n=== Starting install_dev_deps.py ===")

    # Install core packages first
    logger.info("\nInstalling core dependencies via mamba...")
    try:
        sh.mamba(
            "install",
            "-y",
            "-c",
            "conda-forge",
            *CORE_PACKAGES,
            _err=sys.stderr,
            _out=sys.stdout,
        )
        logger.info("Core dependencies installed successfully!")
    except sh.ErrorReturnCode as e:
        logger.error("Error installing core packages!")
        logger.error("Exit code: %s", e.exit_code)
        logger.error("Stdout: %s", e.stdout.decode() if e.stdout else "No stdout")
        logger.error("Stderr: %s", e.stderr.decode() if e.stderr else "No stderr")
        sys.exit(1)

    # Read pyproject.toml
    logger.info("\nReading pyproject.toml...")
    pyproject_path = Path("pyproject.toml")
    with open(pyproject_path) as f:
        pyproject = tomlkit.load(f)

    # Get main, spice, and dev dependencies
    logger.info("\nCollecting dependencies from pyproject.toml...")
    main_deps = pyproject["project"]["dependencies"]
    dev_deps = pyproject["project"]["optional-dependencies"]["dev"]
    spice_deps = pyproject["project"]["optional-dependencies"]["spice"]
    all_deps = main_deps + dev_deps + spice_deps

    logger.info("Found %d main dependencies", len(main_deps))
    logger.info("Found %d dev dependencies", len(dev_deps))
    logger.info("Found %d spice dependencies", len(spice_deps))
    logger.info("Total dependencies to process: %d", len(all_deps))

    # Split dependencies into conda and pip packages
    logger.info("\nSplitting dependencies between conda and pip...")
    # Remove core packages and pip-only packages from conda installation
    conda_deps = [
        dep for dep in all_deps if dep not in PIP_PACKAGES and dep not in CORE_PACKAGES
    ]
    pip_deps = [
        dep
        for dep in all_deps
        if dep in PIP_PACKAGES and dep.lower() not in CONDA_ONLY_PACKAGES
    ]

    logger.info("Packages to install via conda: %d", len(conda_deps))
    logger.info("Packages to install via pip: %d", len(pip_deps))

    # Install conda packages
    if conda_deps:
        try:
            logger.info("\nInstalling conda packages...")
            logger.info("Packages: %s", " ".join(conda_deps))
            sh.mamba(
                "install",
                "-y",
                "-c",
                "conda-forge",
                *conda_deps,
                _err=sys.stderr,
                _out=sys.stdout,
            )
            logger.info("Conda installation completed successfully!")
        except sh.ErrorReturnCode as e:
            logger.error("Error installing conda packages!")
            logger.error("Exit code: %s", e.exit_code)
            logger.error("Stdout: %s", e.stdout.decode() if e.stdout else "No stdout")
            logger.error("Stderr: %s", e.stderr.decode() if e.stderr else "No stderr")
            sys.exit(1)

    # Install pip packages
    if pip_deps:
        try:
            logger.info("\nInstalling pip packages...")
            logger.info("Packages: %s", " ".join(pip_deps))
            sh.pip("install", *pip_deps, _err=sys.stderr, _out=sys.stdout)
            logger.info("Pip installation completed successfully!")
        except sh.ErrorReturnCode as e:
            logger.error("Error installing pip packages!")
            logger.error("Exit code: %s", e.exit_code)
            logger.error("Stdout: %s", e.stdout.decode() if e.stdout else "No stdout")
            logger.error("Stderr: %s", e.stderr.decode() if e.stderr else "No stderr")
            sys.exit(1)

    logger.info("\n=== install_dev_deps.py completed successfully ===")


if __name__ == "__main__":
    install_deps()
