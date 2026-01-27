"""Tests for SPICE kernel management.

Test Strategy:
- URL validation tests (fast): Verify kernel URLs are accessible via HEAD requests
- Unit tests (fast): Test path construction, naming, configuration
- Integration tests (slow): Actually download and load kernels

This separation ensures CI catches URL changes (e.g., kernel version updates at NAIF)
without needing to download gigabytes of data.
"""

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from planetarypy.spice import generic_kernels
from planetarypy.spice.archived_kernels import BASE_URL, Subsetter, datasets
from planetarypy.spice.config import KERNEL_STORAGE
from planetarypy.spice.generic_kernels import (
    GENERIC_STORAGE,
    GENERIC_URL,
    download_generic_kernels,
    generic_kernel_names,
    generic_kernel_paths,
    load_generic_kernels,
    show_loaded_kernels,
)


# =============================================================================
# URL Validation Tests (Fast) - Catch broken URLs without downloading
# =============================================================================


def test_generic_kernel_urls_are_accessible():
    """Verify all generic kernel URLs return 200 OK via HEAD request.

    This test catches issues like kernel files being renamed or removed at NAIF
    (e.g., mar097.bsp -> mar099.bsp) without needing to download the actual files.
    """
    for name in generic_kernel_names:
        url = str(GENERIC_URL / name)
        response = requests.head(url, timeout=10)
        assert response.ok, (
            f"Generic kernel URL not accessible: {url}\n"
            f"Status: {response.status_code}\n"
            f"This may indicate the kernel was renamed or removed at NAIF."
        )


@pytest.mark.slow
def test_archived_kernel_urls_are_valid():
    """Test that all Archive Link URLs in the datasets are valid."""
    for mission in datasets.index:
        url = datasets.at[mission, "Archive Link"]
        response = requests.head(url, timeout=10)
        assert response.ok, f"Archive Link for {mission} is not accessible: {url}"


# =============================================================================
# Unit Tests (Fast) - Test logic without network/disk I/O
# =============================================================================


def test_generic_kernel_paths():
    """Test that generic kernel paths are correctly defined."""
    assert isinstance(generic_kernel_paths, list)
    assert all(isinstance(path, Path) for path in generic_kernel_paths)
    assert all(GENERIC_STORAGE in path.parents for path in generic_kernel_paths)


def test_generic_kernel_names():
    """Test that generic kernel names are correctly defined."""
    assert isinstance(generic_kernel_names, list)
    assert all(isinstance(name, str) for name in generic_kernel_names)
    assert len(generic_kernel_names) == len(generic_kernel_paths)
    # Check that each name corresponds to a path
    for name, path in zip(generic_kernel_names, generic_kernel_paths):
        assert name in str(path), f"Kernel name {name} not found in path {path}"


def test_archived_kernel_paths():
    """Test that archived kernel paths are correctly defined."""
    subsetter = Subsetter("mex", "2004-01-01", "2004-01-02")
    paths = [subsetter.get_local_path(url) for url in subsetter.kernel_urls]
    assert all(isinstance(path, Path) for path in paths)
    assert all(path.parent.parent.parent == KERNEL_STORAGE for path in paths)
    assert all(path.parent.parent.name == "mex" for path in paths)


def test_archived_kernel_names():
    """Test that archived kernel names are correctly defined."""
    subsetter = Subsetter("mex", "2004-01-01", "2004-01-02")
    assert isinstance(subsetter.kernel_names, list)
    assert all(isinstance(name, str) for name in subsetter.kernel_names)


def test_storage_paths_exist():
    """Test that storage paths exist."""
    assert GENERIC_STORAGE.exists(), f"Expected {GENERIC_STORAGE} to exist"
    assert KERNEL_STORAGE.exists(), f"Expected {KERNEL_STORAGE} to exist"


def test_available_missions():
    """Test to verify available missions."""
    assert "mex" in datasets.index, "mex should be in the available missions"


def test_no_duplicate_kernels():
    """Test that there are no duplicate kernels between generic and archived."""
    # Create a test subsetter to get archived kernel names
    subsetter = Subsetter("mex", "2004-01-01", "2004-01-02")
    archived_names = subsetter.kernel_names
    generic_set = set(generic_kernel_names)
    archived_set = set(archived_names)
    assert not (generic_set & archived_set), (
        "Found duplicate kernels between generic and archived"
    )


def test_download_skips_existing_files(tmp_path, monkeypatch):
    """Test that download_generic_kernels skips files that already exist."""
    # Setup: Create fake existing kernel files
    test_storage = tmp_path / "generic"
    test_storage.mkdir(parents=True)

    # Patch the module's storage and paths
    monkeypatch.setattr(generic_kernels, "GENERIC_STORAGE", test_storage)

    fake_paths = [test_storage / name for name in generic_kernel_names]
    monkeypatch.setattr(generic_kernels, "generic_kernel_paths", fake_paths)

    # Create fake existing files
    for path in fake_paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("fake kernel content")

    # Mock url_retrieve to track if it's called
    mock_retrieve = MagicMock()
    monkeypatch.setattr(generic_kernels, "url_retrieve", mock_retrieve)

    # Act: Call download with overwrite=False (default)
    download_generic_kernels(overwrite=False)

    # Assert: url_retrieve should NOT have been called (files exist)
    mock_retrieve.assert_not_called()


def test_download_overwrites_when_requested(tmp_path, monkeypatch):
    """Test that download_generic_kernels overwrites files when overwrite=True."""
    # Setup: Create fake existing kernel files
    test_storage = tmp_path / "generic"
    test_storage.mkdir(parents=True)

    monkeypatch.setattr(generic_kernels, "GENERIC_STORAGE", test_storage)

    fake_paths = [test_storage / name for name in generic_kernel_names]
    monkeypatch.setattr(generic_kernels, "generic_kernel_paths", fake_paths)

    # Create fake existing files
    for path in fake_paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("fake kernel content")

    # Mock url_retrieve
    mock_retrieve = MagicMock()
    monkeypatch.setattr(generic_kernels, "url_retrieve", mock_retrieve)

    # Act: Call download with overwrite=True
    download_generic_kernels(overwrite=True)

    # Assert: url_retrieve SHOULD have been called for each kernel
    assert mock_retrieve.call_count == len(generic_kernel_names)


# =============================================================================
# Integration Tests (Slow) - Actually download and load kernels
# =============================================================================


@pytest.mark.slow
def test_download_generic_kernels_integration(tmp_path, monkeypatch):
    """Integration test: Actually download generic kernels to temp directory."""
    test_storage = tmp_path / "generic"
    test_storage.mkdir(parents=True)

    # Patch storage location to use temp directory
    monkeypatch.setattr(generic_kernels, "GENERIC_STORAGE", test_storage)
    test_paths = [test_storage / name for name in generic_kernel_names]
    monkeypatch.setattr(generic_kernels, "generic_kernel_paths", test_paths)

    # Actually download
    download_generic_kernels()

    # Verify files were downloaded
    for path in test_paths:
        assert path.exists(), f"Expected {path} to exist after download"
        assert path.stat().st_size > 0, f"Downloaded file {path} is empty"


@pytest.mark.slow
def test_load_generic_kernels_integration(tmp_path, monkeypatch):
    """Integration test: Download and load generic kernels, verify SPICE state."""
    import spiceypy as spice

    # Clear any previously loaded kernels
    spice.kclear()

    test_storage = tmp_path / "generic"
    test_storage.mkdir(parents=True)

    monkeypatch.setattr(generic_kernels, "GENERIC_STORAGE", test_storage)
    test_paths = [test_storage / name for name in generic_kernel_names]
    monkeypatch.setattr(generic_kernels, "generic_kernel_paths", test_paths)

    # Download and load
    load_generic_kernels()

    # Verify kernels are loaded in SPICE
    loaded = show_loaded_kernels()
    assert isinstance(loaded, list)
    assert len(loaded) == len(generic_kernel_names), (
        f"Expected {len(generic_kernel_names)} kernels loaded, got {len(loaded)}"
    )

    # Cleanup
    spice.kclear()


@pytest.mark.slow
def test_download_archived_kernels_integration(tmp_path):
    """Integration test: Download archived kernels for a mission."""
    subsetter = Subsetter(
        "mex", "2004-01-01", "2004-01-02",
        save_location=tmp_path / "archived_kernels"
    )
    subsetter.download_kernels(overwrite=True)

    # Check that files were downloaded
    for url in subsetter.kernel_urls:
        path = subsetter.get_local_path(url)
        assert path.exists(), f"Expected {path} to exist"
        assert path.stat().st_size > 0, f"Downloaded file {path} is empty"
