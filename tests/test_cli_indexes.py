"""Tests for the ``plp indexes`` sub-app, focused on the ``last`` verb.

``get_index`` is patched so tests don't need real PDS data on disk —
the table-rendering and time-column logic are what we're exercising.
"""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
from typer.testing import CliRunner

from planetarypy.cli import app

runner = CliRunner()


def _fake_df(time_col: str | None = "START_TIME") -> pd.DataFrame:
    """Synthetic 5-row index DataFrame. ``time_col`` controls which
    canonical time column (if any) the frame carries — used to drive
    the ``--sort`` auto-detection path."""
    rows = {
        "PRODUCT_ID": ["P_001", "P_002", "P_003", "P_004", "P_005"],
        "FILE_NAME": [f"file_{i}.IMG" for i in range(1, 6)],
        # File-order: oldest first. Sorted by START_TIME ascending would
        # give the same order — but if the file order is REVERSED, the
        # sort path picks differently. We test both.
    }
    if time_col is not None:
        rows[time_col] = [
            "2024-01-01T00:00:00",
            "2024-02-01T00:00:00",
            "2024-03-01T00:00:00",
            "2024-04-01T00:00:00",
            "2024-05-01T00:00:00",
        ]
    return pd.DataFrame(rows)


# ── plp indexes last ────────────────────────────────────────────────────


class TestIndexesLastBasic:

    def test_unknown_key_exits_with_error(self):
        with patch("planetarypy.pds.utils._all_dotted_index_keys",
                   return_value={"mro.ctx.edr"}):
            result = runner.invoke(
                app, ["indexes", "last", "definitely.not.a.key"]
            )
        assert result.exit_code == 1
        # Error message goes to stderr — typer.echo(err=True). Typer's
        # CliRunner merges them by default into ``output``.
        assert "Unknown index key" in result.output

    def test_default_shows_three_trailing_rows(self):
        df = _fake_df()
        with patch("planetarypy.pds.utils._all_dotted_index_keys",
                   return_value={"mro.ctx.edr"}), \
             patch("planetarypy.pds.get_index", return_value=df):
            result = runner.invoke(app, ["indexes", "last", "mro.ctx.edr"])
        assert result.exit_code == 0
        # 3 trailing rows are the LAST 3 product ids (file order).
        assert "P_003" in result.output
        assert "P_004" in result.output
        assert "P_005" in result.output
        # The first two should NOT appear in the rendered table.
        assert "P_001" not in result.output
        assert "P_002" not in result.output

    def test_rows_flag_limits_output(self):
        df = _fake_df()
        with patch("planetarypy.pds.utils._all_dotted_index_keys",
                   return_value={"mro.ctx.edr"}), \
             patch("planetarypy.pds.get_index", return_value=df):
            result = runner.invoke(
                app, ["indexes", "last", "mro.ctx.edr", "-n", "1"]
            )
        assert result.exit_code == 0
        assert "P_005" in result.output
        # Only the last row should be in the output.
        assert "P_004" not in result.output

    def test_title_shows_total_rows_and_cols(self):
        df = _fake_df()
        with patch("planetarypy.pds.utils._all_dotted_index_keys",
                   return_value={"mro.ctx.edr"}), \
             patch("planetarypy.pds.get_index", return_value=df):
            result = runner.invoke(app, ["indexes", "last", "mro.ctx.edr"])
        # Title block carries "{rows:,} rows × {cols} cols".
        assert "5 rows" in result.output
        assert "3 cols" in result.output


# ── --sort flag ─────────────────────────────────────────────────────────


class TestIndexesLastSort:

    def test_sort_picks_time_column_and_uses_it(self):
        # File order is reversed from chronological — sort must
        # reorder so the latest START_TIME wins, not the last file row.
        df = _fake_df()
        df_reversed = df.iloc[::-1].reset_index(drop=True)
        with patch("planetarypy.pds.utils._all_dotted_index_keys",
                   return_value={"mro.ctx.edr"}), \
             patch("planetarypy.pds.get_index", return_value=df_reversed):
            result = runner.invoke(
                app, ["indexes", "last", "mro.ctx.edr", "-n", "1", "--sort"]
            )
        assert result.exit_code == 0
        # After sort by START_TIME ascending, the last row is the
        # newest — P_005, not the last file row (P_001).
        assert "P_005" in result.output
        assert "P_001" not in result.output
        # Title should mention the chosen sort column.
        assert "by START_TIME" in result.output

    def test_sort_falls_back_when_no_time_column(self):
        # DataFrame with no canonical time column at all.
        df = _fake_df(time_col=None)
        with patch("planetarypy.pds.utils._all_dotted_index_keys",
                   return_value={"mro.ctx.edr"}), \
             patch("planetarypy.pds.get_index", return_value=df):
            result = runner.invoke(
                app, ["indexes", "last", "mro.ctx.edr", "-n", "1", "--sort"]
            )
        # Graceful fallback: warn to stderr, still emit the file-order tail.
        assert result.exit_code == 0
        assert "no time column found" in result.output.lower()
        assert "P_005" in result.output


# ── peek still works (regression guard for the helper refactor) ─────────


class TestIndexesPeekStillWorks:
    """The helper extraction shouldn't break the existing peek command."""

    def test_peek_renders_three_random_rows(self):
        df = _fake_df()
        with patch("planetarypy.pds.utils._all_dotted_index_keys",
                   return_value={"mro.ctx.edr"}), \
             patch("planetarypy.pds.get_index", return_value=df):
            result = runner.invoke(app, ["indexes", "peek", "mro.ctx.edr"])
        assert result.exit_code == 0
        assert "5 rows" in result.output
        assert "showing 3 random" in result.output
        # Some product id MUST appear — which exact one is random.
        assert any(f"P_00{i}" in result.output for i in range(1, 6))
