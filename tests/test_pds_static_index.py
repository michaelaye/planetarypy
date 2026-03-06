"""Tests for planetarypy.pds.static_index module."""

import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import tomlkit
from yarl import URL

from planetarypy.pds.static_index import ConfigHandler, StaticRemoteHandler
from planetarypy.pds.index_logging import AccessLog


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_TOML = """\
[mro]
[mro.ctx]
edr = "https://example.com/mro/ctx/edr_index.lbl"
[mro.hirise]
edr = "https://example.com/mro/hirise/edr_index.lbl"

[cassini]
[cassini.iss]
ring_summary = "https://example.com/cassini/iss/ring_summary.lbl"
"""


@pytest.fixture()
def config_env(tmp_path, monkeypatch):
    """Set up a temp config file and log file so nothing touches the real home dir."""
    config_path = tmp_path / ".planetarypy_index_urls.toml"
    config_path.write_text(SAMPLE_TOML, encoding="utf-8")

    log_path = tmp_path / ".planetarypy_index_log.toml"

    monkeypatch.setattr(ConfigHandler, "CONFIG_PATH", config_path)
    monkeypatch.setattr(AccessLog, "FILE_PATH", log_path)

    return {"config_path": config_path, "log_path": log_path, "tmp_path": tmp_path}


# ---------------------------------------------------------------------------
# ConfigHandler tests
# ---------------------------------------------------------------------------


class TestConfigHandler:
    """Tests for ConfigHandler."""

    def test_loads_existing_config(self, config_env):
        """ConfigHandler reads an existing TOML file without downloading."""
        with patch.object(ConfigHandler, "should_update", new_callable=lambda: property(lambda self: False)):
            handler = ConfigHandler()
        assert handler.path == config_env["config_path"]

    def test_get_url_returns_correct_url(self, config_env):
        """get_url returns a yarl.URL for a dotted key."""
        with patch.object(ConfigHandler, "should_update", new_callable=lambda: property(lambda self: False)):
            handler = ConfigHandler()
        url = handler.get_url("mro.ctx.edr")
        assert isinstance(url, URL)
        assert str(url) == "https://example.com/mro/ctx/edr_index.lbl"

    def test_get_url_different_key(self, config_env):
        """get_url works for different dotted keys."""
        with patch.object(ConfigHandler, "should_update", new_callable=lambda: property(lambda self: False)):
            handler = ConfigHandler()
        url = handler.get_url("cassini.iss.ring_summary")
        assert str(url) == "https://example.com/cassini/iss/ring_summary.lbl"

    def test_get_all_keys_flattens_nested_dict(self, config_env):
        """_get_all_keys returns all leaf dotted keys from a nested dict."""
        with patch.object(ConfigHandler, "should_update", new_callable=lambda: property(lambda self: False)):
            handler = ConfigHandler()
        keys = handler._get_all_keys(handler.to_dict())
        assert keys == {
            "mro.ctx.edr",
            "mro.hirise.edr",
            "cassini.iss.ring_summary",
        }

    def test_get_all_keys_with_parent_key(self, config_env):
        """_get_all_keys respects the parent_key argument."""
        with patch.object(ConfigHandler, "should_update", new_callable=lambda: property(lambda self: False)):
            handler = ConfigHandler()
        keys = handler._get_all_keys({"a": {"b": 1}}, parent_key="root")
        assert keys == {"root.a.b"}

    def test_get_all_keys_empty_dict(self, config_env):
        """_get_all_keys returns empty set for empty dict."""
        with patch.object(ConfigHandler, "should_update", new_callable=lambda: property(lambda self: False)):
            handler = ConfigHandler()
        assert handler._get_all_keys({}) == set()

    def test_should_update_true_when_never_updated(self, config_env):
        """should_update is True when no update has ever been logged."""
        handler = ConfigHandler.__new__(ConfigHandler)
        handler.path = config_env["config_path"]
        handler.log = AccessLog("indexes.static.config")
        # log has no last_update → should_update is True
        assert handler.should_update is True

    def test_should_update_false_when_recently_updated(self, config_env):
        """should_update is False when last update was less than a day ago."""
        handler = ConfigHandler.__new__(ConfigHandler)
        handler.path = config_env["config_path"]
        handler.log = AccessLog("indexes.static.config")
        handler.log.log_update_time()  # sets last_updated to now
        assert handler.should_update is False

    def test_should_update_true_when_old(self, config_env):
        """should_update is True when last update was more than a day ago."""
        handler = ConfigHandler.__new__(ConfigHandler)
        handler.path = config_env["config_path"]
        handler.log = AccessLog("indexes.static.config")
        # Manually set last_updated to 2 days ago
        two_days_ago = datetime.datetime.now() - datetime.timedelta(days=2)
        handler.log.set("indexes.static.config", "last_updated", two_days_ago)
        handler.log.save()
        assert handler.should_update is True

    def test_downloads_config_when_missing(self, config_env, monkeypatch):
        """When config file does not exist, it is downloaded."""
        config_env["config_path"].unlink()
        assert not config_env["config_path"].exists()

        def fake_url_retrieve(url, path, **kwargs):
            Path(path).write_text(SAMPLE_TOML, encoding="utf-8")

        monkeypatch.setattr("planetarypy.pds.static_index.utils.url_retrieve", fake_url_retrieve)
        handler = ConfigHandler()
        assert config_env["config_path"].exists()
        assert str(handler.get_url("mro.ctx.edr")) == "https://example.com/mro/ctx/edr_index.lbl"

    def test_check_and_update_config_with_updates(self, config_env, monkeypatch):
        """_check_and_update_config replaces the file when remote differs."""
        updated_toml = SAMPLE_TOML + '\n[go]\n[go.ssi]\nraw = "https://example.com/go/ssi/raw.lbl"\n'
        remote_tmp = config_env["tmp_path"] / "remote_tmp.toml"
        remote_tmp.write_text(updated_toml, encoding="utf-8")

        monkeypatch.setattr(
            "planetarypy.pds.static_index.utils.compare_remote_file",
            lambda *a, **kw: {"has_updates": True, "remote_tmp_path": remote_tmp, "error": None},
        )

        with patch.object(ConfigHandler, "should_update", new_callable=lambda: property(lambda self: False)):
            handler = ConfigHandler()

        handler._check_and_update_config()
        # The config should now contain the new key
        reloaded = tomlkit.loads(config_env["config_path"].read_text())
        assert reloaded["go"]["ssi"]["raw"] == "https://example.com/go/ssi/raw.lbl"

    def test_check_and_update_config_no_updates(self, config_env, monkeypatch):
        """_check_and_update_config logs check time when no updates."""
        monkeypatch.setattr(
            "planetarypy.pds.static_index.utils.compare_remote_file",
            lambda *a, **kw: {"has_updates": False, "remote_tmp_path": None, "error": None},
        )
        with patch.object(ConfigHandler, "should_update", new_callable=lambda: property(lambda self: False)):
            handler = ConfigHandler()
        handler._check_and_update_config()
        # Should have logged a check time
        assert handler.log.last_check is not None

    def test_check_and_update_config_error(self, config_env, monkeypatch):
        """_check_and_update_config handles errors gracefully."""
        monkeypatch.setattr(
            "planetarypy.pds.static_index.utils.compare_remote_file",
            lambda *a, **kw: {"has_updates": False, "remote_tmp_path": None, "error": "Network error"},
        )
        with patch.object(ConfigHandler, "should_update", new_callable=lambda: property(lambda self: False)):
            handler = ConfigHandler()
        # Should not raise
        handler._check_and_update_config()

    def test_delete(self, config_env):
        """_delete removes the config file."""
        with patch.object(ConfigHandler, "should_update", new_callable=lambda: property(lambda self: False)):
            handler = ConfigHandler()
        assert handler.path.exists()
        handler._delete()
        assert not handler.path.exists()

    def test_local_path_override(self, config_env, tmp_path):
        """ConfigHandler accepts a local_path override."""
        alt_path = tmp_path / "alt_config.toml"
        alt_path.write_text(SAMPLE_TOML, encoding="utf-8")
        with patch.object(ConfigHandler, "should_update", new_callable=lambda: property(lambda self: False)):
            handler = ConfigHandler(local_path=str(alt_path))
        assert handler.path == alt_path


# ---------------------------------------------------------------------------
# StaticRemoteHandler tests
# ---------------------------------------------------------------------------


class TestStaticRemoteHandler:
    """Tests for StaticRemoteHandler."""

    def _make_handler(self, config_env, monkeypatch, should_check=False):
        """Helper to build a StaticRemoteHandler without network calls."""
        # Prevent should_check from triggering get_remote_timestamp during __init__
        monkeypatch.setattr(AccessLog, "should_check", property(lambda self: should_check))
        with patch.object(ConfigHandler, "should_update", new_callable=lambda: property(lambda self: False)):
            handler = StaticRemoteHandler("mro.ctx.edr")
        return handler

    def test_url_property(self, config_env, monkeypatch):
        """url property returns the correct URL from config."""
        handler = self._make_handler(config_env, monkeypatch)
        assert isinstance(handler.url, URL)
        assert str(handler.url) == "https://example.com/mro/ctx/edr_index.lbl"

    def test_should_check_delegates_to_access_log(self, config_env, monkeypatch):
        """should_check returns whatever AccessLog.should_check says."""
        monkeypatch.setattr(AccessLog, "should_check", property(lambda self: False))
        with patch.object(ConfigHandler, "should_update", new_callable=lambda: property(lambda self: False)):
            handler = StaticRemoteHandler.__new__(StaticRemoteHandler)
            handler.index_key = "mro.ctx.edr"
            handler.config = ConfigHandler()
            handler.log = AccessLog("mro.ctx.edr")
            handler._remote_timestamp = None
        assert handler.should_check is False

        monkeypatch.setattr(AccessLog, "should_check", property(lambda self: True))
        assert handler.should_check is True

    def test_get_remote_timestamp_success(self, config_env, monkeypatch):
        """get_remote_timestamp returns and stores the remote timestamp."""
        handler = self._make_handler(config_env, monkeypatch)
        fake_ts = datetime.datetime(2025, 6, 15, 12, 0, 0)
        monkeypatch.setattr(
            "planetarypy.pds.static_index.utils.get_remote_timestamp",
            lambda url: fake_ts,
        )
        result = handler.get_remote_timestamp()
        assert result == fake_ts
        assert handler._remote_timestamp == fake_ts

    def test_get_remote_timestamp_url_error(self, config_env, monkeypatch):
        """get_remote_timestamp returns None on URLError."""
        from urllib.request import URLError

        handler = self._make_handler(config_env, monkeypatch)
        monkeypatch.setattr(
            "planetarypy.pds.static_index.utils.get_remote_timestamp",
            MagicMock(side_effect=URLError("timeout")),
        )
        result = handler.get_remote_timestamp()
        assert result is None
        assert handler._remote_timestamp is None

    def test_get_remote_timestamp_called_during_init_when_should_check(self, config_env, monkeypatch):
        """When should_check is True, __init__ calls get_remote_timestamp."""
        fake_ts = datetime.datetime(2025, 6, 15, 12, 0, 0)
        monkeypatch.setattr(
            "planetarypy.pds.static_index.utils.get_remote_timestamp",
            lambda url: fake_ts,
        )
        monkeypatch.setattr(AccessLog, "should_check", property(lambda self: True))
        with patch.object(ConfigHandler, "should_update", new_callable=lambda: property(lambda self: False)):
            handler = StaticRemoteHandler("mro.ctx.edr")
        assert handler._remote_timestamp == fake_ts

    def test_update_available_true_when_log_says_so(self, config_env, monkeypatch):
        """update_available returns True if log.update_available is already True."""
        handler = self._make_handler(config_env, monkeypatch)
        handler.log.log_update_available(True)
        assert handler.update_available is True

    def test_update_available_false_when_should_not_check(self, config_env, monkeypatch):
        """update_available returns False when should_check is False and no flag."""
        handler = self._make_handler(config_env, monkeypatch, should_check=False)
        handler.log.log_update_available(False)
        assert handler.update_available is False

    def test_update_available_true_when_remote_newer(self, config_env, monkeypatch):
        """update_available returns True when remote timestamp is newer than last update."""
        monkeypatch.setattr(AccessLog, "should_check", property(lambda self: True))
        with patch.object(ConfigHandler, "should_update", new_callable=lambda: property(lambda self: False)):
            handler = StaticRemoteHandler.__new__(StaticRemoteHandler)
            handler.index_key = "mro.ctx.edr"
            handler.config = ConfigHandler()
            handler.log = AccessLog("mro.ctx.edr")
            handler._remote_timestamp = None

        # Log a last_update in the past
        old_time = datetime.datetime(2025, 1, 1, 0, 0, 0)
        handler.log.set("mro.ctx.edr", "last_updated", old_time)
        handler.log.save()

        # Set a newer remote timestamp in the log
        new_time = datetime.datetime(2025, 6, 15, 12, 0, 0)
        handler.log.set("mro.ctx.edr", "remote_timestamp", new_time)
        handler.log.save()
        # Clear any prior update_available flag
        handler.log.log_update_available(False)

        assert handler.update_available is True

    def test_update_available_false_when_remote_older(self, config_env, monkeypatch):
        """update_available returns False when remote timestamp is older than last update."""
        monkeypatch.setattr(AccessLog, "should_check", property(lambda self: True))
        with patch.object(ConfigHandler, "should_update", new_callable=lambda: property(lambda self: False)):
            handler = StaticRemoteHandler.__new__(StaticRemoteHandler)
            handler.index_key = "mro.ctx.edr"
            handler.config = ConfigHandler()
            handler.log = AccessLog("mro.ctx.edr")
            handler._remote_timestamp = None

        # Last update is recent
        recent = datetime.datetime(2025, 6, 20, 0, 0, 0)
        handler.log.set("mro.ctx.edr", "last_updated", recent)
        handler.log.save()

        # Remote is older
        old_remote = datetime.datetime(2025, 6, 10, 0, 0, 0)
        handler.log.set("mro.ctx.edr", "remote_timestamp", old_remote)
        handler.log.save()
        handler.log.log_update_available(False)

        assert handler.update_available is False

    def test_update_available_true_when_never_updated(self, config_env, monkeypatch):
        """update_available returns True when there's no prior update logged."""
        monkeypatch.setattr(AccessLog, "should_check", property(lambda self: True))
        with patch.object(ConfigHandler, "should_update", new_callable=lambda: property(lambda self: False)):
            handler = StaticRemoteHandler.__new__(StaticRemoteHandler)
            handler.index_key = "mro.ctx.edr"
            handler.config = ConfigHandler()
            handler.log = AccessLog("mro.ctx.edr")
            handler._remote_timestamp = None

        # Set a remote timestamp but no last_updated
        handler.log.set("mro.ctx.edr", "remote_timestamp", datetime.datetime(2025, 6, 15))
        handler.log.save()
        handler.log.log_update_available(False)

        assert handler.update_available is True
