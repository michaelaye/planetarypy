"""Tests for PDS index configuration module."""

import datetime
import tempfile
from pathlib import Path

import pytest

try:
    import tomlkit

    DEPENDENCIES_AVAILABLE = True
except ImportError:
    DEPENDENCIES_AVAILABLE = False

from planetarypy.pds.index_config import IndexConfigManager, discover_dynamic_urls
from planetarypy.pds.index_logging import IndexAccessLog, access_log


def test_legacy_api_functions_exist():
    from planetarypy.pds import index_config

    assert hasattr(index_config, "discover_dynamic_urls"), (
        "discover_dynamic_urls missing from API"
    )
    assert hasattr(index_config, "discover_single_dynamic_url"), (
        "discover_single_dynamic_url missing from API"
    )
    assert hasattr(index_config, "check_and_update_dynamic_urls"), (
        "check_and_update_dynamic_urls missing from API"
    )
    assert callable(index_config.discover_dynamic_urls)
    assert callable(index_config.discover_single_dynamic_url)
    assert callable(index_config.check_and_update_dynamic_urls)


@pytest.mark.parametrize(
    "func_name",
    [
        "discover_dynamic_urls",
        "discover_single_dynamic_url",
        "check_and_update_dynamic_urls",
    ],
)
def test_legacy_api_functions_callable(func_name):
    from planetarypy.pds import index_config
    func = getattr(index_config, func_name)
    if func_name == "discover_single_dynamic_url":
        func("mro.ctx.edr")
    else:
        func()


def test_metadata_handling():
    if not DEPENDENCIES_AVAILABLE:
        pytest.skip("Dependencies not available, skipping test")
    with tempfile.TemporaryDirectory() as temp_dir:
        test_config_path = Path(temp_dir) / "test_metadata.toml"
        basic_doc = tomlkit.document()
        basic_doc["missions"] = tomlkit.table()
        test_config_path.write_text(tomlkit.dumps(basic_doc))
        config = IndexConfigManager(config_path=str(test_config_path))
        assert "metadata" in config.config_doc


def test_config_loading():
    manager = IndexConfigManager()
    config_doc = manager.config_doc
    assert isinstance(config_doc, dict)


def test_get_url():
    manager = IndexConfigManager()
    url = manager.get_url("cassini.iss.index")
    assert isinstance(url, str)
    # The config may be empty, so just check type and fallback
    url = manager.get_url("nonexistent.missing.index")
    assert url == ""


def test_set_url():
    with tempfile.TemporaryDirectory() as tmpdir:
        test_config_path = Path(tmpdir) / "test_index_urls.toml"
        manager = IndexConfigManager(str(test_config_path))
        test_url = "https://example.org/test/index.lbl"
        key = "test.instrument.index"
        manager.set_url(key, test_url)
        assert manager.get_url(key) == test_url
        config_doc = manager.config_doc
        assert key in config_doc


def test_flat_config_structure():
    manager = IndexConfigManager()
    config_doc = manager.config_doc
    # All keys should be flat dotted index keys
    for key in config_doc:
        assert isinstance(key, str)


def test_discover_dynamic_urls():
    with tempfile.TemporaryDirectory():
        results = discover_dynamic_urls()
        assert isinstance(results, dict)


def test_access_log_loading():
    """Test that the access log is loaded properly."""
    assert hasattr(access_log, "tomldoc")
    assert "dynamic_urls" in access_log.tomldoc


def test_timestamp_operations():
    """Test setting and getting timestamps."""
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_log = IndexAccessLog(Path(tmpdir) / "test_index.log")

        # Test setting and getting timestamp with datetime object
        test_key = "test.instrument.index"
        test_time = datetime.datetime.now().replace(
            microsecond=0
        )  # Remove microseconds for comparison
        temp_log.set_timestamp(test_key, test_time)

        retrieved_time = temp_log.get_timestamp(test_key)
        assert retrieved_time is not None
        assert retrieved_time.replace(microsecond=0) == test_time

        # Test setting and getting timestamp with string
        test_key2 = "test.instrument.index2"
        test_time_str = "2023-10-01T12:00:00"
        temp_log.set_timestamp(test_key2, test_time_str)

        retrieved_time2 = temp_log.get_timestamp(test_key2)
        assert retrieved_time2 is not None
        assert retrieved_time2.isoformat() == test_time_str


def test_nonexistent_timestamp():
    """Test getting a timestamp that doesn't exist."""
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_log = IndexAccessLog(Path(tmpdir) / "test_index.log")
        assert temp_log.get_timestamp("nonexistent.key") is None


def test_index_key_exists():
    manager = IndexConfigManager()
    config_doc = manager.config_doc
    # Example: check for a known index key
    key = "cassini.iss.index"
    if key not in config_doc:
        pytest.skip(f"Index key {key} not in config")
    assert isinstance(config_doc[key], str)
