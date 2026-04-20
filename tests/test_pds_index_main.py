"""Tests for planetarypy.pds.index_main (Index and InventoryIndex)."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from planetarypy.pds.index_main import Index, InventoryIndex


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _patch_storage_root(tmp_path, monkeypatch):
    """Redirect config.storage_root to a temp directory for every test."""
    monkeypatch.setattr(
        "planetarypy.pds.index_main.config.storage_root", tmp_path
    )


@pytest.fixture()
def _static_key(monkeypatch):
    """Ensure 'go.ssi.raw' is NOT in DYNAMIC_URL_HANDLERS (static route)."""
    monkeypatch.setattr(
        "planetarypy.pds.index_main.DYNAMIC_URL_HANDLERS", {}
    )


@pytest.fixture()
def _dynamic_key(monkeypatch):
    """Put 'mro.ctx.edr' in DYNAMIC_URL_HANDLERS so it routes dynamically."""
    monkeypatch.setattr(
        "planetarypy.pds.index_main.DYNAMIC_URL_HANDLERS",
        {"mro.ctx.edr": MagicMock},
    )


def _make_index(key="go.ssi.raw", monkeypatch=None, url="https://example.com/data/cumindex.lbl"):
    """Helper: build an Index with both Remote constructors stubbed out."""
    mock_remote = MagicMock()
    mock_remote.url = url

    with patch.object(Index, "_determine_remote_type") as det:
        idx = Index(key)
        # Manually wire up remote after skipping real detection
        idx._remote = mock_remote
        idx._remote_type = "static"
    return idx


@pytest.fixture()
def static_index(monkeypatch):
    """An Index whose remote.url points to a lowercase .lbl URL."""
    monkeypatch.setattr(
        "planetarypy.pds.index_main.DYNAMIC_URL_HANDLERS", {}
    )
    return _make_index("go.ssi.raw", url="https://pds.example.com/go/ssi/cumindex.lbl")


@pytest.fixture()
def upper_index(monkeypatch):
    """An Index whose remote.url points to an uppercase .LBL URL."""
    monkeypatch.setattr(
        "planetarypy.pds.index_main.DYNAMIC_URL_HANDLERS", {}
    )
    return _make_index("go.ssi.raw", url="https://pds.example.com/go/ssi/CUMINDEX.LBL")


# ---------------------------------------------------------------------------
# Remote-type detection
# ---------------------------------------------------------------------------

class TestRemoteTypeDetection:
    """Index should pick static or dynamic based on DYNAMIC_URL_HANDLERS."""

    @patch("planetarypy.pds.index_main.StaticRemoteHandler")
    def test_static_route(self, mock_static_cls, _static_key):
        idx = Index("go.ssi.raw")
        assert idx.remote_type == "static"
        mock_static_cls.assert_called_once()

    @patch("planetarypy.pds.index_main.DynamicRemoteHandler")
    def test_dynamic_route(self, mock_dyn_cls, _dynamic_key):
        idx = Index("mro.ctx.edr")
        assert idx.remote_type == "dynamic"
        mock_dyn_cls.assert_called_once()


# ---------------------------------------------------------------------------
# URL-derived filename properties
# ---------------------------------------------------------------------------

class TestFilenameProperties:

    def test_label_filename_lowercase(self, static_index):
        assert static_index.label_filename == Path("cumindex.lbl")

    def test_label_filename_uppercase(self, upper_index):
        assert upper_index.label_filename == Path("CUMINDEX.LBL")

    def test_isupper_false(self, static_index):
        assert static_index.isupper is False

    def test_isupper_true(self, upper_index):
        assert upper_index.isupper is True

    def test_tab_extension_lowercase(self, static_index):
        assert static_index.tab_extension == ".tab"

    def test_tab_extension_uppercase(self, upper_index):
        assert upper_index.tab_extension == ".TAB"

    def test_table_filename_lowercase(self, static_index):
        assert static_index.table_filename == Path("cumindex.tab")

    def test_table_filename_uppercase(self, upper_index):
        assert upper_index.table_filename == Path("CUMINDEX.TAB")

    def test_table_url_lowercase(self, static_index):
        assert static_index.table_url == "https://pds.example.com/go/ssi/cumindex.tab"

    def test_table_url_uppercase(self, upper_index):
        assert upper_index.table_url == "https://pds.example.com/go/ssi/CUMINDEX.TAB"


# ---------------------------------------------------------------------------
# Local path computation
# ---------------------------------------------------------------------------

class TestLocalPaths:

    def test_default_local_dir(self, tmp_path, static_index):
        expected = tmp_path / "go/ssi/indexes/raw"
        assert static_index._local_dir == expected

    def test_local_label_path(self, static_index):
        assert static_index.local_label_path == static_index.local_dir / "cumindex.lbl"

    def test_local_table_path(self, static_index):
        assert static_index.local_table_path == static_index.local_dir / "cumindex.tab"

    def test_local_parq_path(self, static_index):
        assert static_index.local_parq_path.suffix == ".parq"
        assert static_index.local_parq_path.stem == "cumindex"

    def test_local_dir_created_on_access(self, static_index):
        """Accessing .local_dir should mkdir the directory."""
        d = static_index.local_dir
        assert d.is_dir()

    def test_custom_local_dir(self, tmp_path, monkeypatch):
        """Passing local_dir= overrides the default."""
        monkeypatch.setattr(
            "planetarypy.pds.index_main.DYNAMIC_URL_HANDLERS", {}
        )
        custom = tmp_path / "custom"
        with patch.object(Index, "_determine_remote_type"):
            idx = Index("go.ssi.raw", local_dir=custom)
        assert idx._local_dir == custom


# ---------------------------------------------------------------------------
# files_downloaded
# ---------------------------------------------------------------------------

class TestFilesDownloaded:

    def test_false_when_nothing_exists(self, static_index):
        assert static_index.files_downloaded is False

    def test_true_when_both_exist(self, static_index):
        static_index.local_dir.mkdir(parents=True, exist_ok=True)
        static_index.local_label_path.touch()
        static_index.local_table_path.touch()
        assert static_index.files_downloaded is True

    def test_false_when_only_label(self, static_index):
        static_index.local_dir.mkdir(parents=True, exist_ok=True)
        static_index.local_label_path.touch()
        assert static_index.files_downloaded is False

    def test_false_when_only_table(self, static_index):
        static_index.local_dir.mkdir(parents=True, exist_ok=True)
        static_index.local_table_path.touch()
        assert static_index.files_downloaded is False


# ---------------------------------------------------------------------------
# download
# ---------------------------------------------------------------------------

class TestDownload:

    @patch("planetarypy.pds.index_main.url_retrieve")
    @patch("planetarypy.pds.index_main.have_internet", return_value=True)
    def test_download_calls_url_retrieve_twice(self, _inet, mock_retrieve, static_index):
        static_index._remote.log = MagicMock()

        # Stub convert_to_parquet so it doesn't try to read real files
        with patch.object(Index, "convert_to_parquet"):
            static_index.download()

        assert mock_retrieve.call_count == 2
        # First call: label URL
        assert "cumindex.lbl" in str(mock_retrieve.call_args_list[0])
        # Second call: table URL
        assert "cumindex.tab" in str(mock_retrieve.call_args_list[1])

    @patch("planetarypy.pds.index_main.url_retrieve")
    @patch("planetarypy.pds.index_main.have_internet", return_value=False)
    def test_download_returns_false_without_internet(self, _inet, _retr, static_index):
        result = static_index.download()
        assert result is False
        _retr.assert_not_called()

    @patch("planetarypy.pds.index_main.url_retrieve")
    @patch("planetarypy.pds.index_main.have_internet", return_value=True)
    def test_download_returns_false_when_no_url(self, _inet, _retr, static_index):
        static_index._remote.url = None
        result = static_index.download()
        assert result is False


# ---------------------------------------------------------------------------
# dataframe / parquet
# ---------------------------------------------------------------------------

class TestDataframe:

    def test_dataframe_reads_parquet(self, static_index):
        """dataframe property should read from local_parq_path."""
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        static_index.local_dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(static_index.local_parq_path)

        result = static_index.dataframe
        pd.testing.assert_frame_equal(result, df)


# ---------------------------------------------------------------------------
# ensure_parquet
# ---------------------------------------------------------------------------

class TestEnsureParquet:

    def test_returns_false_when_parquet_exists(self, static_index):
        static_index.local_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"x": [1]}).to_parquet(static_index.local_parq_path)
        assert static_index.ensure_parquet() is False

    @patch.object(Index, "convert_to_parquet")
    def test_converts_when_label_and_table_exist(self, mock_convert, static_index):
        d = static_index.local_dir
        d.mkdir(parents=True, exist_ok=True)
        static_index.local_label_path.touch()
        static_index.local_table_path.touch()
        result = static_index.ensure_parquet()
        assert result is False
        mock_convert.assert_called_once()

    @patch.object(Index, "download")
    def test_downloads_when_files_missing(self, mock_dl, static_index):
        # No files on disk, no parquet
        result = static_index.ensure_parquet()
        assert result is True
        mock_dl.assert_called_once()

    @patch.object(Index, "convert_to_parquet")
    def test_force_reconverts(self, mock_convert, static_index):
        d = static_index.local_dir
        d.mkdir(parents=True, exist_ok=True)
        # Parquet exists
        pd.DataFrame({"x": [1]}).to_parquet(static_index.local_parq_path)
        # But label+table also exist
        static_index.local_label_path.touch()
        static_index.local_table_path.touch()
        result = static_index.ensure_parquet(force=True)
        assert result is False
        mock_convert.assert_called_once()


# ---------------------------------------------------------------------------
# label_filename fallback (no URL)
# ---------------------------------------------------------------------------

class TestLabelFilenameFallback:

    def test_fallback_to_local_lbl_file(self, static_index):
        """When URL is None, label_filename globs for local .lbl files."""
        static_index._remote.url = None
        d = static_index.local_dir
        d.mkdir(parents=True, exist_ok=True)
        (d / "myindex.lbl").touch()
        assert static_index.label_filename == Path("myindex.lbl")

    def test_fallback_to_generic_name(self, static_index):
        """When URL is None and no local files, use indexname.lbl."""
        static_index._remote.url = None
        static_index.local_dir.mkdir(parents=True, exist_ok=True)
        assert static_index.label_filename == Path("raw.lbl")


# ---------------------------------------------------------------------------
# repr
# ---------------------------------------------------------------------------

class TestRepr:

    def test_repr_contains_key_info(self, static_index):
        r = repr(static_index)
        assert "go.ssi.raw" in r
        assert "static" in r


# ---------------------------------------------------------------------------
# InventoryIndex
# ---------------------------------------------------------------------------

class TestInventoryIndex:

    def test_tab_extension_is_csv(self, monkeypatch):
        monkeypatch.setattr(
            "planetarypy.pds.index_main.DYNAMIC_URL_HANDLERS", {}
        )
        with patch.object(Index, "_determine_remote_type"):
            inv = InventoryIndex("go.ssi.inventory")
            inv._remote = MagicMock()
            inv._remote.url = "https://example.com/data/inventory.lbl"
        assert inv.tab_extension == ".csv"

    def test_table_filename_uses_csv(self, monkeypatch):
        monkeypatch.setattr(
            "planetarypy.pds.index_main.DYNAMIC_URL_HANDLERS", {}
        )
        with patch.object(Index, "_determine_remote_type"):
            inv = InventoryIndex("go.ssi.inventory")
            inv._remote = MagicMock()
            inv._remote.url = "https://example.com/data/inventory.lbl"
        assert inv.table_filename == Path("inventory.csv")

    def test_read_index_data(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "planetarypy.pds.index_main.DYNAMIC_URL_HANDLERS", {}
        )
        with patch.object(Index, "_determine_remote_type"):
            inv = InventoryIndex("go.ssi.inventory", local_dir=tmp_path)
            inv._remote = MagicMock()
            inv._remote.url = "https://example.com/data/inventory.lbl"

        # Create a small CSV fixture
        csv_path = tmp_path / "inventory.csv"
        csv_path.write_text(
            "VOL1,path/to/file1.dat,OBS001,MARS,PHOBOS\n"
            "VOL1,path/to/file2.dat,OBS002,JUPITER\n"
        )

        df = inv.read_index_data()
        # OBS001 has 2 targets, OBS002 has 1 => 3 rows total
        assert len(df) == 3
        assert set(df.columns) == {"volume", "file_path", "observation_id", "target"}
        assert list(df[df.observation_id == "OBS001"]["target"]) == ["MARS", "PHOBOS"]

    def test_targets_per_obsid(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "planetarypy.pds.index_main.DYNAMIC_URL_HANDLERS", {}
        )
        with patch.object(Index, "_determine_remote_type"):
            inv = InventoryIndex("go.ssi.inventory", local_dir=tmp_path)
            inv._remote = MagicMock()
            inv._remote.url = "https://example.com/data/inventory.lbl"

        csv_path = tmp_path / "inventory.csv"
        csv_path.write_text(
            "VOL1,path/to/file1.dat,OBS001,MARS,PHOBOS\n"
            "VOL1,path/to/file2.dat,OBS002,JUPITER\n"
        )
        inv.read_index_data()
        grouped = inv.targets_per_obsid
        assert len(grouped) == 2
        row = grouped[grouped.observation_id == "OBS001"].iloc[0]
        assert row["target"] == ["MARS", "PHOBOS"]
