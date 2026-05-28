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


def test_default_config_includes_filter_deprecation_warnings():
    """Fresh configs ship with ``filter_deprecation_warnings = true`` so
    end users see the knob exists when they open the file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        fresh = Config(Path(tmpdir) / "fresh.toml")
        assert fresh.get_value("filter_deprecation_warnings") is True
        # Comment block must accompany the key — discoverability matters.
        text = fresh.path.read_text()
        assert "filter_deprecation_warnings = true" in text
        assert "DeprecationWarning" in text  # explanatory comment


def test_existing_config_gets_key_backfilled():
    """Configs written before the key existed get it backfilled on read,
    so users opening the file later see the new knob."""
    import tomlkit
    with tempfile.TemporaryDirectory() as tmpdir:
        legacy_path = Path(tmpdir) / "legacy.toml"
        # Pre-existing config with only storage_root (the old shape).
        doc = tomlkit.document()
        doc["storage_root"] = str(Path(tmpdir) / "data")
        legacy_path.write_text(tomlkit.dumps(doc))
        # Reading via Config should backfill the new key.
        cfg = Config(legacy_path)
        assert cfg.get_value("filter_deprecation_warnings") is True
        # …and persist that backfill to disk.
        text = legacy_path.read_text()
        assert "filter_deprecation_warnings = true" in text


def test_default_config_includes_max_table_rows():
    """Fresh configs ship with ``max_table_rows = 4`` so the table-vs-CSV
    threshold used by ``plp indexes select`` (and future row-display
    commands) is discoverable in the config file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        fresh = Config(Path(tmpdir) / "fresh.toml")
        assert fresh.get_value("max_table_rows") == 4
        text = fresh.path.read_text()
        assert "max_table_rows = 4" in text
        assert "transposed" in text  # explanatory comment


def test_max_table_rows_backfilled_into_legacy_config():
    """Configs written before the key existed get it backfilled on read."""
    import tomlkit
    with tempfile.TemporaryDirectory() as tmpdir:
        legacy_path = Path(tmpdir) / "legacy.toml"
        doc = tomlkit.document()
        doc["storage_root"] = str(Path(tmpdir) / "data")
        legacy_path.write_text(tomlkit.dumps(doc))
        cfg = Config(legacy_path)
        assert cfg.get_value("max_table_rows") == 4
        text = legacy_path.read_text()
        assert "max_table_rows = 4" in text


def test_existing_max_table_rows_value_is_preserved():
    """A user who customized ``max_table_rows`` must keep their value."""
    import tomlkit
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "narrow.toml"
        doc = tomlkit.document()
        doc["storage_root"] = str(Path(tmpdir) / "data")
        doc["max_table_rows"] = 10
        path.write_text(tomlkit.dumps(doc))
        cfg = Config(path)
        assert cfg.get_value("max_table_rows") == 10


def test_existing_explicit_false_is_preserved():
    """A user who set ``filter_deprecation_warnings = false`` must not
    have their choice silently flipped to true on the next read."""
    import tomlkit
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "dev.toml"
        doc = tomlkit.document()
        doc["storage_root"] = str(Path(tmpdir) / "data")
        doc["filter_deprecation_warnings"] = False
        path.write_text(tomlkit.dumps(doc))
        cfg = Config(path)
        # Backfill must use ``in`` not ``get()`` so falsy-but-present
        # values aren't overwritten.
        assert cfg.get_value("filter_deprecation_warnings") is False
