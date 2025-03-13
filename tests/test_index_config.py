"""Tests for PDS index configuration module."""

import datetime
import tempfile
from pathlib import Path

import pytest

from planetarypy.pds.index_config import (
    IndexAccessLog,
    IndexURLsConfig,
    access_log,
    urls_config,
)


def test_urls_config_loading():
    """Test that the URLs config is properly loaded."""
    assert hasattr(urls_config, "tomldoc")
    assert "missions" in urls_config.tomldoc


def test_get_url():
    """Test retrieving URLs with mission, instrument, index structure."""
    # Test with a known index
    url = urls_config.get_url("cassini.iss.index")
    assert isinstance(url, str)
    assert (
        "pds-rings.seti.org" in url or url == ""
    )  # Either contains URL or empty if not in config

    # Test with an invalid index
    url = urls_config.get_url("nonexistent.missing.index")
    assert url == ""


def test_set_url():
    """Test setting URLs with appropriate structure."""
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_config = IndexURLsConfig(Path(tmpdir) / "test_index_urls.toml")

        # Test setting a new URL
        test_url = "https://example.org/test/index.lbl"
        temp_config.set_url("test.instrument.index", test_url)
        assert temp_config.get_url("test.instrument.index") == test_url

        # Test that mission structure was created properly
        assert "test" in temp_config.tomldoc["missions"]
        assert "instrument" in temp_config.tomldoc["missions"]["test"]


def test_mission_structure():
    """Test that the mission structure is properly organized."""
    for mission in urls_config.tomldoc["missions"]:
        assert isinstance(urls_config.tomldoc["missions"][mission], dict)
        for instrument in urls_config.tomldoc["missions"][mission]:
            assert isinstance(
                urls_config.tomldoc["missions"][mission][instrument], dict
            )


def test_discover_dynamic_urls():
    """Test that dynamic URL discovery doesn't error (actual discovery depends on network)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_config = IndexURLsConfig(Path(tmpdir) / "test_index_urls.toml")
        # Just ensure it runs without error (actual URLs depend on network connectivity)
        results = temp_config.discover_dynamic_urls()
        assert isinstance(results, dict)


def test_access_log_loading():
    """Test that the access log is loaded properly."""
    assert hasattr(access_log, "log_data")
    assert isinstance(access_log.log_data, dict)


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
    # Skip if mission isn't in the config
    if mission not in urls_config.tomldoc["missions"]:
        pytest.skip(f"Mission {mission} not in config")

    # Check that the expected instrument exists for this mission
    assert expected_instrument in urls_config.tomldoc["missions"][mission]
