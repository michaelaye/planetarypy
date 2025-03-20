#!/usr/bin/env python
import sys
from pathlib import Path

import sh
import toml

# Define packages that should be installed via pip instead of conda
PIP_PACKAGES = {"build", "pip-tools", "planets"}

# Define packages that should NEVER be installed via pip
CONDA_ONLY_PACKAGES = {"gdal"}

# Core packages that must be installed first
CORE_PACKAGES = {"gdal"}


def install_deps():
    print("\n=== Starting install_dev_deps.py ===")

    # Install core packages first
    print("\nInstalling core dependencies via mamba...")
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
        print("Core dependencies installed successfully!")
    except sh.ErrorReturnCode as e:
        print("Error installing core packages!")
        print("Exit code:", e.exit_code)
        print("Stdout:", e.stdout.decode() if e.stdout else "No stdout")
        print("Stderr:", e.stderr.decode() if e.stderr else "No stderr")
        sys.exit(1)

    # Read pyproject.toml
    print("\nReading pyproject.toml...")
    pyproject_path = Path("pyproject.toml")
    with open(pyproject_path) as f:
        pyproject = toml.load(f)

    # Get main, spice, and dev dependencies
    print("\nCollecting dependencies from pyproject.toml...")
    main_deps = pyproject["project"]["dependencies"]
    dev_deps = pyproject["project"]["optional-dependencies"]["dev"]
    spice_deps = pyproject["project"]["optional-dependencies"]["spice"]
    all_deps = main_deps + dev_deps + spice_deps

    print(f"Found {len(main_deps)} main dependencies")
    print(f"Found {len(dev_deps)} dev dependencies")
    print(f"Found {len(spice_deps)} spice dependencies")
    print(f"Total dependencies to process: {len(all_deps)}")

    # Split dependencies into conda and pip packages
    print("\nSplitting dependencies between conda and pip...")
    # Remove core packages and pip-only packages from conda installation
    conda_deps = [
        dep for dep in all_deps if dep not in PIP_PACKAGES and dep not in CORE_PACKAGES
    ]
    pip_deps = [
        dep
        for dep in all_deps
        if dep in PIP_PACKAGES and dep.lower() not in CONDA_ONLY_PACKAGES
    ]

    print(f"Packages to install via conda: {len(conda_deps)}")
    print(f"Packages to install via pip: {len(pip_deps)}")

    # Install conda packages
    if conda_deps:
        try:
            print("\nInstalling conda packages...")
            print("Packages:", " ".join(conda_deps))
            sh.mamba(
                "install",
                "-y",
                "-c",
                "conda-forge",
                *conda_deps,
                _err=sys.stderr,
                _out=sys.stdout,
            )
            print("Conda installation completed successfully!")
        except sh.ErrorReturnCode as e:
            print("Error installing conda packages!")
            print("Exit code:", e.exit_code)
            print("Stdout:", e.stdout.decode() if e.stdout else "No stdout")
            print("Stderr:", e.stderr.decode() if e.stderr else "No stderr")
            sys.exit(1)

    # Install pip packages
    if pip_deps:
        try:
            print("\nInstalling pip packages...")
            print("Packages:", " ".join(pip_deps))
            sh.pip("install", *pip_deps, _err=sys.stderr, _out=sys.stdout)
            print("Pip installation completed successfully!")
        except sh.ErrorReturnCode as e:
            print("Error installing pip packages!")
            print("Exit code:", e.exit_code)
            print("Stdout:", e.stdout.decode() if e.stdout else "No stdout")
            print("Stderr:", e.stderr.decode() if e.stderr else "No stderr")
            sys.exit(1)

    print("\n=== install_dev_deps.py completed successfully ===")


if __name__ == "__main__":
    install_deps()
