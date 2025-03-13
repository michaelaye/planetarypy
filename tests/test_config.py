"""Tests for basic configuration module."""

import tempfile
from pathlib import Path

from planetarypy.config import Config, config


def test_config_loading():
    """Test that the config is properly loaded."""
    assert isinstance(config.d, dict)
    assert isinstance(config.storage_root, Path)


def test_config_getitem():
    """Test dictionary-like access to config values."""
    result = config["storage_root"]
    assert isinstance(result, str)
    assert len(result) > 0


def test_config_missing_key():
    """Test that missing keys return empty string."""
    assert config.get_value("nonexistent.key") == ""


def test_config_get_value():
    """Test retrieving values with get_value method."""
    # Test with valid keys
    assert config.get_value("storage_root") != ""
    # Test with invalid key
    assert config.get_value("nonexistent.key") == ""


def test_config_set_value():
    """Test setting values in the config."""
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_config = Config(Path(tmpdir) / "test_config.toml")

        # Test setting a new value
        temp_config.set_value("test.nested.value", "test_value")
        assert temp_config.get_value("test.nested.value") == "test_value"

        # Test overwriting an existing value
        temp_config.set_value("test.nested.value", "new_value")
        assert temp_config.get_value("test.nested.value") == "new_value"


def test_config_storage_root():
    """Test that storage_root is properly configured."""
    assert config.storage_root.is_dir()
    assert isinstance(config.storage_root, Path)


def test_config_custom_path():
    """Test creating a config with custom path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        custom_path = Path(tmpdir) / "custom_config.toml"
        custom_config = Config(custom_path)
        assert custom_config.path == custom_path
        assert custom_path.exists()
