"""Tests for PDS index configuration module."""

import datetime
import tempfile
from pathlib import Path

import pytest

from planetarypy.pds.index_config import (
    discover_dynamic_urls,
    get_url,
    load_config,
    set_url,
)
from planetarypy.pds.index_logging import IndexAccessLog, access_log


def test_config_loading():
    """Test that the configuration is properly loaded."""
    config_doc = load_config()
    assert isinstance(config_doc, dict)
    assert "missions" in config_doc


def test_get_url():
    """Test retrieving URLs with mission, instrument, index structure."""
    # Test with a known index
    url = get_url("cassini.iss.index")
    assert isinstance(url, str)
    assert (
        "pds-rings.seti.org" in url or url == ""
    )  # Either contains URL or empty if not in config

    # Test with an invalid index
    url = get_url("nonexistent.missing.index")
    assert url == ""


def test_set_url():
    """Test setting URLs with appropriate structure."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Load config with custom path
        test_config_path = Path(tmpdir) / "test_index_urls.toml"
        load_config(str(test_config_path))

        # Test setting a new URL
        test_url = "https://example.org/test/index.lbl"
        set_url("test.instrument.index", test_url)
        assert get_url("test.instrument.index") == test_url

        # Test that mission structure was created properly
        config_doc = load_config()
        assert "test" in config_doc["missions"]
        assert "instrument" in config_doc["missions"]["test"]


def test_mission_structure():
    """Test that the mission structure is properly organized."""
    config_doc = load_config()
    for mission in config_doc["missions"]:
        assert isinstance(config_doc["missions"][mission], dict)
        for instrument in config_doc["missions"][mission]:
            assert isinstance(config_doc["missions"][mission][instrument], dict)


def test_discover_dynamic_urls():
    """Test that dynamic URL discovery doesn't error (actual discovery depends on network)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Load config with custom path
        test_config_path = Path(tmpdir) / "test_index_urls.toml"
        load_config(str(test_config_path))

        # Just ensure it runs without error (actual URLs depend on network connectivity)
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
        # Add more mission/instrument pairs as needed
    ],
)
def test_mission_instrument_structure(mission, expected_instrument):
    """Test that expected missions have the expected instruments."""
    config_doc = load_config()

    # Skip if mission isn't in the config
    if mission not in config_doc["missions"]:
        pytest.skip(f"Mission {mission} not in config")

    # Check that the expected instrument exists for this mission
    assert expected_instrument in config_doc["missions"][mission]
