"""Clone and update the pdr-tests repository."""

import datetime as dt
import subprocess
from pathlib import Path

from loguru import logger

REPO_URL = "https://github.com/MillionConcepts/pdr-tests.git"
REPO_DIR_NAME = "pdr-tests"


def _repo_path(storage_root: Path) -> Path:
    """Return the path where the pdr-tests repo should be cloned."""
    return storage_root / "catalog" / REPO_DIR_NAME


def _timestamp_path(storage_root: Path) -> Path:
    """Return path to the file tracking last fetch timestamp."""
    return storage_root / "catalog" / ".pdr_tests_last_fetch"


def _read_timestamp(storage_root: Path) -> dt.datetime | None:
    """Read the last fetch timestamp, or None if not found."""
    ts_path = _timestamp_path(storage_root)
    if ts_path.exists():
        try:
            return dt.datetime.fromisoformat(ts_path.read_text().strip())
        except (ValueError, OSError):
            return None
    return None


def _write_timestamp(storage_root: Path) -> None:
    """Write the current time as the last fetch timestamp."""
    ts_path = _timestamp_path(storage_root)
    ts_path.parent.mkdir(parents=True, exist_ok=True)
    ts_path.write_text(dt.datetime.now(dt.timezone.utc).isoformat())


def ensure_repo(storage_root: Path, force_update: bool = False) -> Path:
    """Ensure the pdr-tests repo is cloned and reasonably up-to-date.

    Performs a shallow clone if not present. Pulls updates if the last
    fetch was more than 24 hours ago (or if force_update is True).

    Parameters
    ----------
    storage_root : Path
        The planetarypy storage root directory
    force_update : bool
        If True, force a git pull regardless of last fetch time

    Returns
    -------
    Path
        Path to the cloned repository root
    """
    repo = _repo_path(storage_root)
    catalog_dir = storage_root / "catalog"
    catalog_dir.mkdir(parents=True, exist_ok=True)

    if not (repo / ".git").exists():
        logger.info(f"Cloning pdr-tests repo to {repo}...")
        # Sparse checkout: only clone the definitions directory
        subprocess.run(
            [
                "git", "clone", "--depth", "1",
                "--filter=blob:none", "--sparse",
                REPO_URL, str(repo),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        subprocess.run(
            ["git", "sparse-checkout", "set", "pdr_tests/definitions"],
            cwd=str(repo),
            check=True,
            capture_output=True,
            text=True,
        )
        _write_timestamp(storage_root)
        logger.info("pdr-tests repo cloned successfully.")
        return repo

    # Repo exists — check if we should update
    last_fetch = _read_timestamp(storage_root)
    hours_since = None
    if last_fetch is not None:
        now = dt.datetime.now(dt.timezone.utc)
        if last_fetch.tzinfo is None:
            last_fetch = last_fetch.replace(tzinfo=dt.timezone.utc)
        hours_since = (now - last_fetch).total_seconds() / 3600

    if force_update or hours_since is None or hours_since > 24:
        logger.info("Updating pdr-tests repo...")
        try:
            subprocess.run(
                ["git", "pull", "--depth", "1"],
                cwd=str(repo),
                check=True,
                capture_output=True,
                text=True,
            )
            _write_timestamp(storage_root)
            logger.info("pdr-tests repo updated.")
        except subprocess.CalledProcessError as e:
            logger.warning(f"git pull failed: {e.stderr}. Using existing checkout.")
    else:
        logger.debug(
            f"pdr-tests repo is recent ({hours_since:.1f}h old), skipping update."
        )

    return repo
