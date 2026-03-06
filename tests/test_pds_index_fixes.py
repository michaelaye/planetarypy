"""Tests for planetarypy.pds.index_fixes module."""

import numpy as np
import pandas as pd
import pytest

from planetarypy.pds.index_fixes import (
    apply_file_fixer,
    apply_pre_time_df_fixer,
    fix_go_ssi_file,
    fix_lro_lola_rdr_df,
    fix_mer_rdr_df,
    replace_in_dataframe,
    replace_in_file,
)


# ---------------------------------------------------------------------------
# replace_in_dataframe
# ---------------------------------------------------------------------------

class TestReplaceInDataFrame:
    def _sample_df(self):
        return pd.DataFrame({
            "A": ["foo bar", "baz foo", "hello"],
            "B": ["foo", "world", "foo"],
            "C": [1, 2, 3],
        })

    def test_replaces_all_string_columns_by_default(self):
        df = self._sample_df()
        result = replace_in_dataframe(df, "foo", "XXX")
        assert result["A"].tolist() == ["XXX bar", "baz XXX", "hello"]
        assert result["B"].tolist() == ["XXX", "world", "XXX"]

    def test_does_not_modify_original_when_not_inplace(self):
        df = self._sample_df()
        replace_in_dataframe(df, "foo", "XXX")
        assert df["A"].tolist() == ["foo bar", "baz foo", "hello"]

    def test_inplace_modifies_original(self):
        df = self._sample_df()
        result = replace_in_dataframe(df, "foo", "XXX", inplace=True)
        assert result is df
        assert df["A"].tolist() == ["XXX bar", "baz XXX", "hello"]

    def test_restrict_to_specific_columns(self):
        df = self._sample_df()
        result = replace_in_dataframe(df, "foo", "XXX", columns=["B"])
        # A should be untouched
        assert result["A"].tolist() == ["foo bar", "baz foo", "hello"]
        assert result["B"].tolist() == ["XXX", "world", "XXX"]

    def test_regex_replacement(self):
        df = pd.DataFrame({"A": ["abc123", "def456", "ghi"]})
        result = replace_in_dataframe(df, r"\d+", "NUM", regex=True)
        assert result["A"].tolist() == ["abcNUM", "defNUM", "ghi"]

    def test_no_matches(self):
        df = self._sample_df()
        result = replace_in_dataframe(df, "NOMATCH", "XXX")
        assert result["A"].tolist() == ["foo bar", "baz foo", "hello"]

    def test_empty_dataframe(self):
        df = pd.DataFrame({"A": pd.Series([], dtype="object")})
        result = replace_in_dataframe(df, "foo", "bar")
        assert len(result) == 0


# ---------------------------------------------------------------------------
# replace_in_file
# ---------------------------------------------------------------------------

class TestReplaceInFile:
    def test_replaces_text_in_file(self, tmp_path):
        p = tmp_path / "data.txt"
        p.write_text("hello world\nhello again\n")
        replace_in_file(p, "hello", "hi")
        assert p.read_text() == "hi world\nhi again\n"

    def test_no_match_leaves_file_unchanged(self, tmp_path):
        p = tmp_path / "data.txt"
        original = "nothing to see here\n"
        p.write_text(original)
        replace_in_file(p, "MISSING", "X")
        assert p.read_text() == original

    def test_accepts_string_path(self, tmp_path):
        p = tmp_path / "data.txt"
        p.write_text("aaa")
        replace_in_file(str(p), "aaa", "bbb")
        assert p.read_text() == "bbb"


# ---------------------------------------------------------------------------
# fix_go_ssi_file
# ---------------------------------------------------------------------------

class TestFixGoSsiFile:
    def test_replaces_malformed_quote(self, tmp_path):
        p = tmp_path / "table.tab"
        p.write_text('some data,-23.629",more data\n')
        fix_go_ssi_file(p)
        assert p.read_text() == "some data,-23.629,,more data\n"

    def test_no_malformed_quote_leaves_file_unchanged(self, tmp_path):
        p = tmp_path / "table.tab"
        original = "clean data,-23.629,more\n"
        p.write_text(original)
        fix_go_ssi_file(p)
        assert p.read_text() == original


# ---------------------------------------------------------------------------
# fix_mer_rdr_df
# ---------------------------------------------------------------------------

class TestFixMerRdrDf:
    def test_appends_z_to_time_columns(self):
        df = pd.DataFrame({
            "START_TIME": ["2004-01-05T12:00:00", "2004-01-06T13:00:00Z"],
            "STOP_TIME": ["2004-02-01T00:00:00", "2004-02-02T00:00:00"],
            "OTHER": ["not a time", "also not"],
        })
        result = fix_mer_rdr_df(df)
        assert result["START_TIME"].tolist() == [
            "2004-01-05T12:00:00Z",
            "2004-01-06T13:00:00Z",
        ]
        assert result["STOP_TIME"].tolist() == [
            "2004-02-01T00:00:00Z",
            "2004-02-02T00:00:00Z",
        ]
        # Non-TIME column untouched
        assert result["OTHER"].tolist() == ["not a time", "also not"]

    def test_coerces_release_id_to_numeric(self):
        df = pd.DataFrame({
            "RELEASE_ID": ["1", "TBD", "3"],
        })
        result = fix_mer_rdr_df(df)
        assert result["RELEASE_ID"].iloc[0] == 1.0
        assert pd.isna(result["RELEASE_ID"].iloc[1])
        assert result["RELEASE_ID"].iloc[2] == 3.0

    def test_no_time_columns_no_error(self):
        df = pd.DataFrame({"A": ["hello"], "B": [1]})
        result = fix_mer_rdr_df(df)
        assert result["A"].tolist() == ["hello"]

    def test_does_not_modify_original(self):
        df = pd.DataFrame({"START_TIME": ["2004-01-05T12:00:00"]})
        fix_mer_rdr_df(df)
        assert df["START_TIME"].iloc[0] == "2004-01-05T12:00:00"


# ---------------------------------------------------------------------------
# fix_lro_lola_rdr_df
# ---------------------------------------------------------------------------

class TestFixLroLolaRdrDf:
    def test_date_only_gets_time_appended(self):
        df = pd.DataFrame({
            "PRODUCT_CREATION_TIME": [
                "2010-06-15",
                "2010-07-20T10:30:00",
                "2011-01-01",
            ],
        })
        result = fix_lro_lola_rdr_df(df)
        assert result["PRODUCT_CREATION_TIME"].dtype == "datetime64[ns]"
        assert result["PRODUCT_CREATION_TIME"].iloc[0] == pd.Timestamp("2010-06-15")
        assert result["PRODUCT_CREATION_TIME"].iloc[1] == pd.Timestamp("2010-07-20 10:30:00")

    def test_nan_values_stay_nat(self):
        df = pd.DataFrame({
            "PRODUCT_CREATION_TIME": [np.nan, "2020-01-01T00:00:00"],
        })
        result = fix_lro_lola_rdr_df(df)
        assert pd.isna(result["PRODUCT_CREATION_TIME"].iloc[0])

    def test_empty_string_becomes_nat(self):
        df = pd.DataFrame({
            "PRODUCT_CREATION_TIME": ["", "2020-05-05T12:00:00"],
        })
        result = fix_lro_lola_rdr_df(df)
        assert pd.isna(result["PRODUCT_CREATION_TIME"].iloc[0])

    def test_missing_column_returns_unchanged(self):
        df = pd.DataFrame({"OTHER_COL": ["hello"]})
        result = fix_lro_lola_rdr_df(df)
        assert "OTHER_COL" in result.columns
        assert result["OTHER_COL"].iloc[0] == "hello"

    def test_does_not_modify_original(self):
        df = pd.DataFrame({"PRODUCT_CREATION_TIME": ["2020-01-01"]})
        fix_lro_lola_rdr_df(df)
        assert df["PRODUCT_CREATION_TIME"].iloc[0] == "2020-01-01"


# ---------------------------------------------------------------------------
# apply_file_fixer
# ---------------------------------------------------------------------------

class TestApplyFileFixer:
    def test_dispatches_go_ssi(self, tmp_path):
        p = tmp_path / "table.tab"
        p.write_text('val,-23.629",rest\n')
        apply_file_fixer("go.ssi.index", p)
        assert p.read_text() == "val,-23.629,,rest\n"

    def test_unknown_key_is_noop(self, tmp_path):
        p = tmp_path / "table.tab"
        p.write_text("unchanged")
        apply_file_fixer("unknown.key.here", p)
        assert p.read_text() == "unchanged"


# ---------------------------------------------------------------------------
# apply_pre_time_df_fixer
# ---------------------------------------------------------------------------

class TestApplyPreTimeDfFixer:
    def test_dispatches_mer_opportunity(self):
        df = pd.DataFrame({"START_TIME": ["2004-01-05T12:00:00"]})
        result = apply_pre_time_df_fixer("mer.opportunity.pancam_rdr", df)
        assert result["START_TIME"].iloc[0] == "2004-01-05T12:00:00Z"

    def test_dispatches_mer_spirit(self):
        df = pd.DataFrame({"STOP_TIME": ["2004-01-05T12:00:00"]})
        result = apply_pre_time_df_fixer("mer.spirit.pancam_rdr", df)
        assert result["STOP_TIME"].iloc[0] == "2004-01-05T12:00:00Z"

    def test_dispatches_lro_lola_rdr(self):
        df = pd.DataFrame({"PRODUCT_CREATION_TIME": ["2020-01-01"]})
        result = apply_pre_time_df_fixer("lro.lola.rdr", df)
        assert result["PRODUCT_CREATION_TIME"].dtype == "datetime64[ns]"

    def test_unknown_key_returns_df_unchanged(self):
        df = pd.DataFrame({"X": [1, 2]})
        result = apply_pre_time_df_fixer("no.such.key", df)
        assert result is df
