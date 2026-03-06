"""Tests for planetarypy.pds.index_labels module.

Covers PVLColumn, _convert_times, find_mixed_type_cols, IndexLabel, index_to_df,
and decode_line using both pure unit tests and synthetic PDS fixture files.
"""

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from planetarypy.pds.index_labels import (
    IndexLabel,
    PVLColumn,
    _convert_times,
    decode_line,
    find_mixed_type_cols,
    index_to_df,
)

DATA_DIR = Path(__file__).parent / "data"
LABEL_PATH = DATA_DIR / "test_index.lbl"
TABLE_PATH = DATA_DIR / "test_index.tab"


# ---------------------------------------------------------------------------
# Helpers to build lightweight PVL-like column objects
# ---------------------------------------------------------------------------

def _make_pvlobj(name, start_byte, nbytes, items=None, item_bytes=None, item_offset=None):
    """Return a dict that quacks enough like a PVL column object for PVLColumn."""
    d = {"NAME": name, "START_BYTE": start_byte, "BYTES": nbytes}
    if items is not None:
        d["ITEMS"] = items
    if item_bytes is not None:
        d["ITEM_BYTES"] = item_bytes
    if item_offset is not None:
        d["ITEM_OFFSET"] = item_offset
    return d


# ===================================================================
# PVLColumn tests
# ===================================================================

class TestPVLColumnScalar:
    """Tests for a simple (non-array) column."""

    @pytest.fixture()
    def col(self):
        return PVLColumn(_make_pvlobj("VOLUME_ID", start_byte=1, nbytes=10))

    def test_name(self, col):
        assert col.name == "VOLUME_ID"

    def test_name_as_list_single(self, col):
        assert col.name_as_list == ["VOLUME_ID"]

    def test_start_is_zero_indexed(self, col):
        # START_BYTE=1 in PDS is index 0 in Python
        assert col.start == 0

    def test_stop(self, col):
        assert col.stop == 10  # start(0) + BYTES(10)

    def test_items_is_none(self, col):
        assert col.items is None

    def test_item_bytes_is_none(self, col):
        assert col.item_bytes is None

    def test_item_offset_is_none(self, col):
        assert col.item_offset is None

    def test_colspecs_tuple(self, col):
        assert col.colspecs == (0, 10)

    def test_decode_scalar(self, col):
        line = "ABCDEFGHIJ" + "extra"
        assert col.decode(line) == "ABCDEFGHIJ"

    def test_repr_delegates(self, col):
        # Just verify repr doesn't crash
        assert isinstance(repr(col), str)


class TestPVLColumnArray:
    """Tests for an array (multi-item) column."""

    @pytest.fixture()
    def col(self):
        # 3 items, each 4 bytes, offset 5 bytes apart, starting at byte 11 (0-indexed: 10)
        return PVLColumn(
            _make_pvlobj(
                "CORNER_LAT",
                start_byte=11,
                nbytes=19,
                items=3,
                item_bytes=4,
                item_offset=5,
            )
        )

    def test_name(self, col):
        assert col.name == "CORNER_LAT"

    def test_name_as_list_multiple(self, col):
        assert col.name_as_list == ["CORNER_LAT_1", "CORNER_LAT_2", "CORNER_LAT_3"]

    def test_items(self, col):
        assert col.items == 3

    def test_colspecs_list(self, col):
        specs = col.colspecs
        assert isinstance(specs, list)
        assert len(specs) == 3
        # First item starts at index 10
        assert specs[0] == (10, 14)
        # Second item: offset=5 → 10+5=15
        assert specs[1] == (15, 19)
        # Third item: offset=10 → 10+10=20
        assert specs[2] == (20, 24)

    def test_decode_array(self, col):
        # Build a line long enough
        line = "." * 10 + "AAAAxBBBBxCCCCx" + "." * 10
        result = col.decode(line)
        assert result == ["AAAA", "BBBB", "CCCC"]


class TestPVLColumnEdgeCases:
    """Edge cases for PVLColumn."""

    def test_start_byte_equals_one(self):
        """START_BYTE=1 should map to Python index 0."""
        col = PVLColumn(_make_pvlobj("X", start_byte=1, nbytes=5))
        assert col.start == 0

    def test_large_start_byte(self):
        col = PVLColumn(_make_pvlobj("X", start_byte=101, nbytes=8))
        assert col.start == 100
        assert col.stop == 108

    def test_single_item_array(self):
        """An array column with items=1 still returns a list from name_as_list and colspecs."""
        col = PVLColumn(
            _make_pvlobj("VAL", start_byte=1, nbytes=4, items=1, item_bytes=4, item_offset=4)
        )
        assert col.name_as_list == ["VAL_1"]
        assert isinstance(col.colspecs, list)
        assert len(col.colspecs) == 1


# ===================================================================
# _convert_times tests
# ===================================================================

class TestConvertTimes:
    def test_valid_datetime_column(self):
        df = pd.DataFrame({"START_TIME": ["2020-01-01T00:00:00", "2021-06-15T12:30:00"]})
        result = _convert_times(df)
        assert pd.api.types.is_datetime64_any_dtype(result["START_TIME"])

    def test_unk_replaced(self):
        df = pd.DataFrame({"START_TIME": ["2020-01-01", "UNK"]})
        result = _convert_times(df)
        assert pd.api.types.is_datetime64_any_dtype(result["START_TIME"])
        assert pd.isna(result["START_TIME"].iloc[1])

    def test_unk_with_whitespace(self):
        df = pd.DataFrame({"START_TIME": ["2020-01-01", "UNK   "]})
        result = _convert_times(df)
        assert pd.isna(result["START_TIME"].iloc[1])

    def test_null_replaced(self):
        df = pd.DataFrame({"STOP_TIME": ["2020-01-01", "NULL"]})
        result = _convert_times(df)
        assert pd.isna(result["STOP_TIME"].iloc[1])

    def test_na_replaced(self):
        df = pd.DataFrame({"STOP_TIME": ["2020-01-01", "N/A"]})
        result = _convert_times(df)
        assert pd.isna(result["STOP_TIME"].iloc[1])

    def test_none_string_replaced(self):
        df = pd.DataFrame({"STOP_TIME": ["2020-01-01", "NONE"]})
        result = _convert_times(df)
        assert pd.isna(result["STOP_TIME"].iloc[1])

    def test_na_string_replaced(self):
        df = pd.DataFrame({"STOP_TIME": ["2020-01-01", "NA"]})
        result = _convert_times(df)
        assert pd.isna(result["STOP_TIME"].iloc[1])

    def test_local_time_skipped(self):
        """LOCAL_TIME columns should not be converted."""
        df = pd.DataFrame({"LOCAL_TIME": ["not-a-date", "also-not"]})
        result = _convert_times(df)
        assert result["LOCAL_TIME"].dtype == object

    def test_dwell_time_skipped(self):
        df = pd.DataFrame({"DWELL_TIME": ["123", "456"]})
        result = _convert_times(df)
        assert result["DWELL_TIME"].dtype == object

    def test_ntv_prefix_skipped(self):
        df = pd.DataFrame({"NTV_TIME_FROM_CLOSEST_APPROACH": ["1.0", "2.0"]})
        result = _convert_times(df)
        # Should remain unconverted
        assert not pd.api.types.is_datetime64_any_dtype(
            result["NTV_TIME_FROM_CLOSEST_APPROACH"]
        )

    def test_non_time_columns_untouched(self):
        df = pd.DataFrame({
            "PRODUCT_ID": ["abc", "def"],
            "START_TIME": ["2020-01-01", "2021-01-01"],
        })
        result = _convert_times(df)
        assert result["PRODUCT_ID"].dtype == object
        assert pd.api.types.is_datetime64_any_dtype(result["START_TIME"])

    def test_all_unk_column(self):
        df = pd.DataFrame({"START_TIME": ["UNK", "UNK"]})
        result = _convert_times(df)
        assert result["START_TIME"].isna().all()


# ===================================================================
# find_mixed_type_cols tests
# ===================================================================

class TestFindMixedTypeCols:
    def test_no_mixed_cols(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        result = find_mixed_type_cols(df, fix=False)
        assert result == []

    def test_detects_mixed_col(self):
        df = pd.DataFrame({"a": [1, "two", 3]})
        result = find_mixed_type_cols(df, fix=False)
        assert "a" in result

    def test_fix_converts_to_str(self):
        df = pd.DataFrame({"a": [1, "two", 3], "b": ["x", "y", "z"]})
        result = find_mixed_type_cols(df, fix=True)
        assert "a" in result
        # After fix, column should be all strings
        assert df["a"].dtype == object
        assert all(isinstance(v, str) for v in df["a"])

    def test_fix_false_leaves_original(self):
        df = pd.DataFrame({"a": [1, "two", 3]})
        find_mixed_type_cols(df, fix=False)
        # Original mixed types preserved
        assert isinstance(df["a"].iloc[0], int)
        assert isinstance(df["a"].iloc[1], str)

    def test_returns_list(self):
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        result = find_mixed_type_cols(df, fix=False)
        assert isinstance(result, list)

    def test_multiple_mixed_cols(self):
        df = pd.DataFrame({"a": [1, "x"], "b": [1.0, "y"], "c": ["a", "b"]})
        result = find_mixed_type_cols(df, fix=False)
        assert "a" in result
        assert "b" in result
        assert "c" not in result


# ===================================================================
# IndexLabel tests (synthetic PDS fixture files)
# ===================================================================

class TestIndexLabel:
    """Tests using synthetic test_index.lbl + test_index.tab fixtures."""

    @pytest.fixture()
    def label(self):
        return IndexLabel(LABEL_PATH)

    def test_tablename(self, label):
        assert label.tablename == "INDEX_TABLE"

    def test_index_name(self, label):
        assert label.index_name == "test_index.tab"

    def test_pvl_lbl_loads(self, label):
        lbl = label.pvl_lbl
        assert lbl["PDS_VERSION_ID"] == "PDS3"

    def test_colnames(self, label):
        assert label.colnames == ["VOLUME_ID", "FILE_NAME", "IMAGE_TIME", "EXPOSURE"]

    def test_colspecs(self, label):
        specs = label.colspecs
        assert len(specs) == 4
        # START_BYTE=2 → index 1, BYTES=10 → (1, 11)
        assert specs[0] == (1, 11)
        assert specs[1] == (14, 34)
        assert specs[2] == (37, 60)
        assert specs[3] == (62, 72)

    def test_columns_dic(self, label):
        d = label.columns_dic
        assert set(d.keys()) == {"VOLUME_ID", "FILE_NAME", "IMAGE_TIME", "EXPOSURE"}

    def test_index_path(self, label):
        assert label.index_path == TABLE_PATH

    def test_pvl_columns_count(self, label):
        assert len(label.pvl_columns) == 4

    def test_read_index_data_shape(self, label):
        df = label.read_index_data(convert_times=True)
        assert len(df) == 5
        assert list(df.columns) == ["VOLUME_ID", "FILE_NAME", "IMAGE_TIME", "EXPOSURE"]

    def test_read_index_data_datetime_conversion(self, label):
        df = label.read_index_data(convert_times=True)
        assert pd.api.types.is_datetime64_any_dtype(df["IMAGE_TIME"])

    def test_read_index_data_unk_is_nat(self, label):
        df = label.read_index_data(convert_times=True)
        assert pd.isna(df["IMAGE_TIME"].iloc[2])

    def test_read_index_data_values(self, label):
        df = label.read_index_data(convert_times=True)
        assert df["VOLUME_ID"].iloc[0] == "VOL_001"
        assert df["FILE_NAME"].iloc[0] == "img_00001.fits"
        assert df["EXPOSURE"].iloc[0] == pytest.approx(12.5)

    def test_read_index_data_no_time_conversion(self, label):
        df = label.read_index_data(convert_times=False)
        assert not pd.api.types.is_datetime64_any_dtype(df["IMAGE_TIME"])


# ===================================================================
# index_to_df tests (synthetic fixture files)
# ===================================================================

class TestIndexToDf:
    @pytest.fixture()
    def label(self):
        return IndexLabel(LABEL_PATH)

    def test_returns_dataframe(self, label):
        df = index_to_df(TABLE_PATH, label, convert_times=False)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 5

    def test_convert_times_true(self, label):
        df = index_to_df(TABLE_PATH, label, convert_times=True)
        assert pd.api.types.is_datetime64_any_dtype(df["IMAGE_TIME"])
        # Row 2 has UNK → NaT
        assert pd.isna(df["IMAGE_TIME"].iloc[2])
        # Row 0 has a valid time
        assert not pd.isna(df["IMAGE_TIME"].iloc[0])

    def test_convert_times_false(self, label):
        df = index_to_df(TABLE_PATH, label, convert_times=False)
        assert not pd.api.types.is_datetime64_any_dtype(df["IMAGE_TIME"])

    def test_strings_are_stripped(self, label):
        df = index_to_df(TABLE_PATH, label, convert_times=False)
        # Fixture has padded strings like "VOL_001   " which should be stripped
        assert df["VOLUME_ID"].iloc[0] == "VOL_001"
        assert df["FILE_NAME"].iloc[3] == "img_00004.fits"


# ===================================================================
# decode_line test (synthetic fixture)
# ===================================================================

class TestDecodeLine:
    def test_decode_line_prints_columns(self, capsys):
        with open(TABLE_PATH, "r") as f:
            line = f.readline().rstrip("\r\n")
        decode_line(line, LABEL_PATH)
        captured = capsys.readouterr()
        assert "VOLUME_ID" in captured.out
        assert "FILE_NAME" in captured.out
        assert "IMAGE_TIME" in captured.out
        assert "EXPOSURE" in captured.out
