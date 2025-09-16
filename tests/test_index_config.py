import os
import sys
try:
    import tomlkit
    DEPENDENCIES_AVAILABLE = True
except ImportError:
    DEPENDENCIES_AVAILABLE = False
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

import pytest

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
def test_config_update():
    if not DEPENDENCIES_AVAILABLE:
        pytest.skip("Dependencies not available, skipping test")
    with tempfile.TemporaryDirectory() as temp_dir:
        test_config_path = Path(temp_dir) / "test_config.toml"
        config = IndexConfigManager(config_path=str(test_config_path))
        assert "metadata" in config.config_doc
        last_updated = config.config_doc["metadata"].get("last_updated")
        source_url = config.config_doc["metadata"].get("source_url", "N/A")
        assert last_updated is not None
        assert source_url is not None
        # Test update for old config
        old_doc = tomlkit.document()
        old_doc["metadata"] = tomlkit.table()
        old_timestamp = datetime.datetime.now() - datetime.timedelta(days=2)
        old_doc["metadata"]["last_updated"] = old_timestamp.isoformat()
        old_doc["metadata"]["source_url"] = "test"
        old_doc["missions"] = tomlkit.table()
        old_doc["missions"]["test"] = tomlkit.table()
        old_doc["missions"]["test"]["instrument"] = tomlkit.table()
        old_doc["missions"]["test"]["instrument"]["index"] = "old_url"
        test_config_path.write_text(tomlkit.dumps(old_doc))
        config2 = IndexConfigManager(config_path=str(test_config_path))
        new_timestamp = config2.config_doc["metadata"].get("last_updated")
        assert new_timestamp is not None
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
"""Tests for PDS index configuration module."""

import datetime
import tempfile
from pathlib import Path

import pytest

from planetarypy.pds.index_config import IndexConfigManager, discover_dynamic_urls
from planetarypy.pds.index_logging import IndexAccessLog, access_log


def test_config_loading():
    manager = IndexConfigManager()
    config_doc = manager.config_doc
    assert isinstance(config_doc, dict)
    assert "missions" in config_doc


def test_get_url():
    manager = IndexConfigManager()
    url = manager.get_url("cassini.iss.index")
    assert isinstance(url, str)
    assert "pds-rings.seti.org" in url or url == ""
    url = manager.get_url("nonexistent.missing.index")
    assert url == ""


def test_set_url():
    with tempfile.TemporaryDirectory() as tmpdir:
        test_config_path = Path(tmpdir) / "test_index_urls.toml"
        manager = IndexConfigManager(str(test_config_path))
        test_url = "https://example.org/test/index.lbl"
        manager.set_url("test.instrument.index", test_url)
        assert manager.get_url("test.instrument.index") == test_url
        config_doc = manager.config_doc
        assert "test" in config_doc["missions"]
        assert "instrument" in config_doc["missions"]["test"]


def test_mission_structure():
    manager = IndexConfigManager()
    config_doc = manager.config_doc
    for mission in config_doc["missions"]:
        assert isinstance(config_doc["missions"][mission], dict)
        for instrument in config_doc["missions"][mission]:
            assert isinstance(config_doc["missions"][mission][instrument], dict)


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


@pytest.mark.parametrize(
    "mission,expected_instrument",
    [
        ("cassini", "iss"),
        ("mro", "ctx"),
    ],
)
def test_mission_instrument_structure(mission, expected_instrument):
    manager = IndexConfigManager()
    config_doc = manager.config_doc
    if mission not in config_doc["missions"]:
        pytest.skip(f"Mission {mission} not in config")
    assert expected_instrument in config_doc["missions"][mission]
