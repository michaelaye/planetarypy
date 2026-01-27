import os
from pathlib import Path

import pytest
import requests

from planetarypy.spice import generic_kernels
from planetarypy.spice.archived_kernels import BASE_URL, Subsetter, datasets
from planetarypy.spice.config import KERNEL_STORAGE
from planetarypy.spice.generic_kernels import (
    GENERIC_STORAGE,
    download_generic_kernels,
    generic_kernel_names,
    generic_kernel_paths,
    load_generic_kernels,
    show_loaded_kernels,
)


def test_generic_kernel_paths():
    """Test that generic kernel paths are correctly defined."""
    assert isinstance(generic_kernel_paths, list)
    assert all(isinstance(path, Path) for path in generic_kernel_paths)
    assert all(GENERIC_STORAGE in path.parents for path in generic_kernel_paths)


def test_archived_kernel_paths():
    """Test that archived kernel paths are correctly defined."""
    subsetter = Subsetter("mex", "2004-01-01", "2004-01-02")
    paths = [subsetter.get_local_path(url) for url in subsetter.kernel_urls]
    assert all(isinstance(path, Path) for path in paths)
    assert all(path.parent.parent.parent == KERNEL_STORAGE for path in paths)
    assert all(path.parent.parent.name == "mex" for path in paths)


def test_generic_kernel_names():
    """Test that generic kernel names are correctly defined."""
    assert isinstance(generic_kernel_names, list)
    assert all(isinstance(name, str) for name in generic_kernel_names)
    assert len(generic_kernel_names) == len(generic_kernel_paths)
    # Check that each name corresponds to a path
    for name, path in zip(generic_kernel_names, generic_kernel_paths):
        assert name in str(path), f"Kernel name {name} not found in path {path}"


def test_archived_kernel_names():
    """Test that archived kernel names are correctly defined."""
    subsetter = Subsetter("mex", "2004-01-01", "2004-01-02")
    assert isinstance(subsetter.kernel_names, list)
    assert all(isinstance(name, str) for name in subsetter.kernel_names)


@pytest.mark.skipif(
    os.environ.get("CI", "false") == "true",
    reason="Downloading kernels in CI environment is not necessary",
)
@pytest.mark.slow
def test_download_generic_kernels(tmp_path):
    """Test downloading generic kernels."""
    # Temporarily override GENERIC_STORAGE for testing
    original_storage = GENERIC_STORAGE
    generic_kernels.GENERIC_STORAGE = tmp_path / "generic_kernels"
    generic_kernels.GENERIC_STORAGE.mkdir(parents=True, exist_ok=True)

    try:
        download_generic_kernels()
        # Check that files were downloaded
        for path in generic_kernel_paths:
            assert path.exists(), f"Expected {path} to exist"
    finally:
        # Restore original storage path
        generic_kernels.GENERIC_STORAGE = original_storage


@pytest.mark.skipif(
    os.environ.get("CI", "false") == "true",
    reason="Downloading kernels in CI environment is not necessary",
)
@pytest.mark.slow
def test_download_archived_kernels(tmp_path):
    """Test downloading archived kernels."""
    # Create a test subsetter with temporary storage
    subsetter = Subsetter(
        "mex", "2004-01-01", "2004-01-02", save_location=tmp_path / "archived_kernels"
    )
    subsetter.download_kernels(overwrite=True)

    # Check that files were downloaded
    for path in [subsetter.get_local_path(url) for url in subsetter.kernel_urls]:
        assert path.exists(), f"Expected {path} to exist"


@pytest.fixture(scope="module")
def setup_generic_kernels():
    """Setup fixture to ensure generic kernels are downloaded and loaded."""
    download_generic_kernels()
    load_generic_kernels()


def test_load_generic_kernels(setup_generic_kernels):
    """Test loading generic kernels."""
    # Kernels should already be loaded by fixture
    loaded = show_loaded_kernels()
    assert isinstance(loaded, list)
    assert all(isinstance(kernel, str) for kernel in loaded)
    assert len(loaded) > 0, "No kernels were loaded"


def test_show_loaded_kernels(setup_generic_kernels):
    """Test showing loaded kernels."""
    loaded = show_loaded_kernels()
    assert isinstance(loaded, list)
    assert all(isinstance(kernel, str) for kernel in loaded)
    assert len(loaded) > 0, "No kernels were loaded"


def test_storage_paths_exist():
    """Test that storage paths exist."""
    assert GENERIC_STORAGE.exists(), f"Expected {GENERIC_STORAGE} to exist"
    assert KERNEL_STORAGE.exists(), f"Expected {KERNEL_STORAGE} to exist"


@pytest.mark.slow
def test_urls_are_valid():
    """Test that all Archive Link URLs in the datasets are valid."""
    for mission in datasets.index:
        url = datasets.at[mission, "Archive Link"]
        assert requests.head(url).ok, f"Archive Link for {mission} is not accessible"


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
