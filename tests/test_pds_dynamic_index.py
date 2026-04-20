"""Tests for dynamic_index.py and dynamic_url_handlers.py.

Tests use monkeypatching to avoid network calls and tmp_path for log files.
"""

import pandas as pd
import pytest
from yarl import URL

from planetarypy.pds.dynamic_index import DYNAMIC_URL_HANDLERS, DynamicRemoteHandler
from planetarypy.pds.dynamic_url_handlers import CTXIndex, LROCIndex, LAMPEDRIndex, LAMPRDRIndex
from planetarypy.pds.index_logging import AccessLog


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _isolate_log(tmp_path, monkeypatch):
    """Redirect AccessLog to a temp file so tests never touch the real log."""
    monkeypatch.setattr(AccessLog, "FILE_PATH", tmp_path / "test_log.toml")


def _make_volumes_df(folders: list[str]) -> pd.DataFrame:
    """Build a DataFrame that mimics pd.read_html output from a PDS directory listing.

    Real PDS pages produce tables with multiple columns (Name, Last modified, Size, Description).
    The source code uses ``.dropna(how='all', axis=1)`` and ``.iloc[..., :-1]`` which drops the
    last column, so we need at least two columns for the fixture to survive that trimming.
    """
    return pd.DataFrame({
        "Name": folders,
        "Last modified": ["2025-01-01"] * len(folders),
    })


# ---------------------------------------------------------------------------
# CTXIndex tests
# ---------------------------------------------------------------------------


class TestCTXIndex:
    """Tests for the CTXIndex handler class."""

    FOLDERS = ["mrox_1230/", "mrox_1231/", "mrox_1232/", "Parent Directory"]

    def _patch_read_html(self, monkeypatch, folders=None):
        folders = folders or self.FOLDERS
        df = _make_volumes_df(folders)
        monkeypatch.setattr(pd, "read_html", lambda url: [df])
        return df

    def test_volumes_table_caches(self, monkeypatch):
        """volumes_table should be computed once and cached."""
        call_count = 0
        original_folders = self.FOLDERS

        def counting_read_html(url):
            nonlocal call_count
            call_count += 1
            return [_make_volumes_df(original_folders)]

        monkeypatch.setattr(pd, "read_html", counting_read_html)

        idx = CTXIndex()
        _ = idx.volumes_table
        _ = idx.volumes_table
        assert call_count == 1

    def test_latest_release_folder(self, monkeypatch):
        """latest_release_folder returns second-to-last row."""
        self._patch_read_html(monkeypatch)
        idx = CTXIndex()
        # After iloc[1:, :-1] the df starts at row index 1 (skipping row 0).
        # iloc[-2, 0] on that slice is the second-to-last row of the trimmed df.
        # Rows after trim: ["mrox_1231/", "mrox_1232/", "Parent Directory"]
        # iloc[-2] => "mrox_1232/"
        assert idx.latest_release_folder == "mrox_1232/"

    def test_latest_release_number(self, monkeypatch):
        self._patch_read_html(monkeypatch)
        idx = CTXIndex()
        assert idx.latest_release_number == "1232"

    def test_latest_index_label_url(self, monkeypatch):
        self._patch_read_html(monkeypatch)
        idx = CTXIndex()
        result = idx.latest_index_label_url
        assert isinstance(result, URL)
        assert str(result).endswith("mrox_1232/index/cumindex.lbl")
        # Should use the primary URL as the base
        assert str(result).startswith(CTXIndex.url)

    def test_backup_url_used_on_primary_failure(self, monkeypatch):
        """If primary URL fails, backup should be tried."""
        calls = []

        def failing_primary(url):
            calls.append(url)
            if url == CTXIndex.url:
                raise ConnectionError("Primary down")
            return [_make_volumes_df(self.FOLDERS)]

        monkeypatch.setattr(pd, "read_html", failing_primary)

        idx = CTXIndex()
        _ = idx.volumes_table
        assert len(calls) == 2
        assert calls[0] == CTXIndex.url
        assert calls[1] == CTXIndex.backup_url
        assert idx._successful_url == CTXIndex.backup_url

    def test_both_urls_fail_raises(self, monkeypatch):
        """If both primary and backup fail, should raise."""
        monkeypatch.setattr(
            pd, "read_html", lambda url: (_ for _ in ()).throw(ConnectionError("down"))
        )
        idx = CTXIndex()
        with pytest.raises(ConnectionError):
            _ = idx.volumes_table


# ---------------------------------------------------------------------------
# LROCIndex tests
# ---------------------------------------------------------------------------


class TestLROCIndex:
    """Tests for the LROCIndex handler class."""

    FOLDERS = ["LROLRC_0001/", "LROLRC_0048/", "LROLRC_0049/", "Parent Directory"]

    def _patch_read_html(self, monkeypatch, folders=None):
        folders = folders or self.FOLDERS
        df = _make_volumes_df(folders)
        monkeypatch.setattr(pd, "read_html", lambda url: [df])

    def test_latest_release_folder(self, monkeypatch):
        self._patch_read_html(monkeypatch)
        idx = LROCIndex()
        # After iloc[1:-1, :-1]: rows = ["LROLRC_0048/", "LROLRC_0049/"]
        # iloc[-1, 0] => "LROLRC_0049/"
        assert idx.latest_release_folder == "LROLRC_0049/"

    def test_latest_release_number(self, monkeypatch):
        self._patch_read_html(monkeypatch)
        idx = LROCIndex()
        assert idx.latest_release_number == "0049"

    def test_latest_index_label_url(self, monkeypatch):
        self._patch_read_html(monkeypatch)
        idx = LROCIndex()
        result = idx.latest_index_label_url
        assert isinstance(result, URL)
        assert str(result).endswith("LROLRC_0049/INDEX/CUMINDEX.LBL")
        assert str(result).startswith(LROCIndex.edr_url)


# ---------------------------------------------------------------------------
# LAMPEDRIndex / LAMPRDRIndex tests
# ---------------------------------------------------------------------------


def _make_lamp_tables(volume_folders: list[str]) -> list[pd.DataFrame]:
    """Build the 4-table structure that JPL planetarydata pages return.

    Table 0: header ("PDS Imaging Node: Data Archive")
    Table 1: file listing with Name, Last modified, Size, Description columns
    Table 2/3: footer tables
    """
    rows = [{"Name": "Parent Directory", "Last modified": None, "Size": "-", "Description": None}]
    for folder in volume_folders:
        rows.append({"Name": folder, "Last modified": "2025-01-01", "Size": "-", "Description": None})
        md5_name = folder.rstrip("/") + "_md5.txt"
        rows.append({"Name": md5_name, "Last modified": "2025-01-01", "Size": "400K", "Description": None})

    table0 = pd.DataFrame({"0": ["PDS Imaging Node: Data Archive"]})
    table1 = pd.DataFrame(rows)
    table2 = pd.DataFrame({"0": [None], "4": ["footer"]})
    table3 = pd.DataFrame({"0": ["NASA Officials:"], "1": ["Someone"]})
    return [table0, table1, table2, table3]


class TestLAMPEDRIndex:
    """Tests for the LAMPEDRIndex handler class."""

    FOLDERS = ["LROLAM_0060/", "LROLAM_0061/", "LROLAM_0062/"]

    def _patch_read_html(self, monkeypatch, folders=None):
        folders = folders or self.FOLDERS
        tables = _make_lamp_tables(folders)
        monkeypatch.setattr(pd, "read_html", lambda url: tables)

    def test_volumes_table_filters_to_dirs_only(self, monkeypatch):
        self._patch_read_html(monkeypatch)
        idx = LAMPEDRIndex()
        assert len(idx.volumes_table) == 3
        assert all(idx.volumes_table["Name"].str.match(r"LROLAM_\d{4}/"))

    def test_volumes_table_caches(self, monkeypatch):
        call_count = 0

        def counting_read_html(url):
            nonlocal call_count
            call_count += 1
            return _make_lamp_tables(self.FOLDERS)

        monkeypatch.setattr(pd, "read_html", counting_read_html)
        idx = LAMPEDRIndex()
        _ = idx.volumes_table
        _ = idx.volumes_table
        assert call_count == 1

    def test_latest_release_folder(self, monkeypatch):
        self._patch_read_html(monkeypatch)
        idx = LAMPEDRIndex()
        assert idx.latest_release_folder == "LROLAM_0062/"

    def test_latest_release_number(self, monkeypatch):
        self._patch_read_html(monkeypatch)
        idx = LAMPEDRIndex()
        assert idx.latest_release_number == "0062"

    def test_latest_index_label_url(self, monkeypatch):
        self._patch_read_html(monkeypatch)
        idx = LAMPEDRIndex()
        result = idx.latest_index_label_url
        assert isinstance(result, URL)
        assert str(result).endswith("LROLAM_0062/INDEX/CUMINDEX.LBL")
        assert str(result).startswith(LAMPEDRIndex.url)


class TestLAMPRDRIndex:
    """Tests for the LAMPRDRIndex handler class."""

    FOLDERS = ["LROLAM_1060/", "LROLAM_1061/", "LROLAM_1062/"]

    def _patch_read_html(self, monkeypatch, folders=None):
        folders = folders or self.FOLDERS
        tables = _make_lamp_tables(folders)
        monkeypatch.setattr(pd, "read_html", lambda url: tables)

    def test_latest_release_folder(self, monkeypatch):
        self._patch_read_html(monkeypatch)
        idx = LAMPRDRIndex()
        assert idx.latest_release_folder == "LROLAM_1062/"

    def test_latest_release_number(self, monkeypatch):
        self._patch_read_html(monkeypatch)
        idx = LAMPRDRIndex()
        assert idx.latest_release_number == "1062"

    def test_latest_index_label_url(self, monkeypatch):
        self._patch_read_html(monkeypatch)
        idx = LAMPRDRIndex()
        result = idx.latest_index_label_url
        assert isinstance(result, URL)
        assert str(result).endswith("LROLAM_1062/INDEX/CUMINDEX.LBL")
        assert str(result).startswith(LAMPRDRIndex.url)

    def test_uses_rdr_base_url(self, monkeypatch):
        self._patch_read_html(monkeypatch)
        idx = LAMPRDRIndex()
        assert "lamp/rdr/" in str(idx.latest_index_label_url)


# ---------------------------------------------------------------------------
# DYNAMIC_URL_HANDLERS registry tests
# ---------------------------------------------------------------------------


class TestRegistry:
    def test_ctx_registered(self):
        assert "mro.ctx.edr" in DYNAMIC_URL_HANDLERS
        assert DYNAMIC_URL_HANDLERS["mro.ctx.edr"] is CTXIndex

    def test_lroc_registered(self):
        assert "lro.lroc.edr" in DYNAMIC_URL_HANDLERS
        assert DYNAMIC_URL_HANDLERS["lro.lroc.edr"] is LROCIndex

    def test_lamp_edr_registered(self):
        assert "lro.lamp.edr" in DYNAMIC_URL_HANDLERS
        assert DYNAMIC_URL_HANDLERS["lro.lamp.edr"] is LAMPEDRIndex

    def test_lamp_rdr_registered(self):
        assert "lro.lamp.rdr" in DYNAMIC_URL_HANDLERS
        assert DYNAMIC_URL_HANDLERS["lro.lamp.rdr"] is LAMPRDRIndex


# ---------------------------------------------------------------------------
# DynamicRemoteHandler tests
# ---------------------------------------------------------------------------


class _FakeHandler:
    """Minimal stub that mimics CTXIndex / LROCIndex."""

    fake_url = "https://example.com/fake_volume/index/cumindex.lbl"

    @property
    def latest_index_label_url(self):
        return self.fake_url


class TestDynamicRemoteHandler:
    """Tests for DynamicRemoteHandler with a mock handler class."""

    KEY = "test.fake.edr"

    @pytest.fixture(autouse=True)
    def _register_fake(self, monkeypatch):
        """Temporarily register _FakeHandler in the registry."""
        monkeypatch.setitem(DYNAMIC_URL_HANDLERS, self.KEY, _FakeHandler)

    def test_handler_class_lookup(self):
        handler = DynamicRemoteHandler(self.KEY)
        assert handler.handler_class is _FakeHandler

    def test_handler_class_missing(self):
        handler = DynamicRemoteHandler.__new__(DynamicRemoteHandler)
        handler.key = "nonexistent.key"
        handler.log = AccessLog(key="nonexistent.key")
        assert handler.handler_class is None

    def test_discover_latest_url(self):
        handler = DynamicRemoteHandler(self.KEY)
        url = handler.discover_latest_url()
        assert url == _FakeHandler.fake_url

    def test_discover_latest_url_no_handler(self):
        handler = DynamicRemoteHandler.__new__(DynamicRemoteHandler)
        handler.key = "nonexistent.key"
        handler.log = AccessLog(key="nonexistent.key")
        with pytest.raises(ValueError, match="No dynamic handler"):
            handler.discover_latest_url()

    def test_first_time_sets_available_url(self):
        """On first run (no log), available_url should be set."""
        handler = DynamicRemoteHandler(self.KEY)
        assert handler.log.available_url == _FakeHandler.fake_url
        assert handler.log.update_available is True

    def test_url_returns_available_when_update_exists(self):
        handler = DynamicRemoteHandler(self.KEY)
        assert handler.url == _FakeHandler.fake_url

    def test_url_returns_current_when_no_update(self):
        """If current_url matches latest, url returns current_url."""
        log = AccessLog(key=self.KEY)
        log.log_current_url(_FakeHandler.fake_url)
        log.log_update_available(False)
        log.log_check_time()
        log.save()

        handler = DynamicRemoteHandler(self.KEY)
        # should_check is False (just checked), no available_url, returns current
        assert handler.url == _FakeHandler.fake_url

    def test_update_available_true_when_new_url(self):
        """update_available should be True when available_url differs from current."""
        log = AccessLog(key=self.KEY)
        log.log_current_url("https://example.com/old/cumindex.lbl")
        log.save()

        handler = DynamicRemoteHandler(self.KEY)
        # __init__ checks and finds new URL != current
        assert handler.update_available is True
        assert handler.log.available_url == _FakeHandler.fake_url

    def test_update_available_false_when_same_url(self):
        """update_available should be False when URLs match and check just happened."""
        log = AccessLog(key=self.KEY)
        log.log_current_url(_FakeHandler.fake_url)
        log.log_check_time()
        log.log_update_available(False)
        log.save()

        handler = DynamicRemoteHandler(self.KEY)
        assert handler.update_available is False

    def test_should_check_delegates_to_log(self):
        """should_check reflects the AccessLog state."""
        # Never checked -> should be True
        handler = DynamicRemoteHandler(self.KEY)
        # After __init__ runs _check_for_updates, log_check_time is called,
        # so should_check becomes False.
        assert handler.should_check is False

    def test_should_check_true_when_never_checked(self, tmp_path, monkeypatch):
        """should_check is True when no log entry exists."""
        # Use a fresh log file
        fresh_log_path = tmp_path / "fresh_log.toml"
        monkeypatch.setattr(AccessLog, "FILE_PATH", fresh_log_path)
        log = AccessLog(key=self.KEY)
        assert log.should_check is True

    def test_url_none_when_no_handler_and_no_log(self):
        """url returns None when there's no handler and no logged URLs."""
        key = "missing.handler.key"
        handler = DynamicRemoteHandler.__new__(DynamicRemoteHandler)
        handler.key = key
        handler.log = AccessLog(key=key)
        # No URLs in log, should_check is True but discover will raise,
        # so let's set should_check to False
        handler.log.log_check_time()
        handler.log.save()
        assert handler.url is None

    def test_check_for_updates_logs_same_url(self):
        """When discovered URL matches current, only check time is updated."""
        log = AccessLog(key=self.KEY)
        log.log_current_url(_FakeHandler.fake_url)
        log.save()

        handler = DynamicRemoteHandler(self.KEY)
        # available_url should NOT be set (URLs match)
        assert handler.log.available_url is None
        # But check time should have been updated
        assert handler.log.last_check is not None


class TestDynamicRemoteHandlerDiscoverError:
    """Test error handling in discover_latest_url."""

    KEY = "test.error.edr"

    def test_discover_returns_none_on_exception(self, monkeypatch):
        class _BrokenHandler:
            @property
            def latest_index_label_url(self):
                raise RuntimeError("scrape failed")

        monkeypatch.setitem(DYNAMIC_URL_HANDLERS, self.KEY, _BrokenHandler)

        handler = DynamicRemoteHandler(self.KEY)
        result = handler.discover_latest_url()
        assert result is None
