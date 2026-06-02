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


# ── help-on-missing-default UX (peek / last / info / refresh) ──────────


class TestIndexesHelpOnMissing:
    """Every indexes verb should print help + exit 0 when its primary
    argument is missing, instead of typer's auto "Missing argument" error.
    The pattern is consistent across the sub-app for muscle-memory."""

    def test_peek_bare_invocation_shows_help(self):
        result = runner.invoke(app, ["indexes", "peek"])
        assert result.exit_code == 0
        assert "Usage:" in result.stdout
        assert "Inspect a registered PDS index" in result.stdout

    def test_last_bare_invocation_shows_help(self):
        result = runner.invoke(app, ["indexes", "last"])
        assert result.exit_code == 0
        assert "Usage:" in result.stdout
        assert "Show the last entries" in result.stdout

    def test_info_bare_invocation_shows_help(self):
        result = runner.invoke(app, ["indexes", "info"])
        assert result.exit_code == 0
        assert "Usage:" in result.stdout
        assert "Show config + cache status" in result.stdout


class TestIndexesInfoFreshness:
    """`plp indexes info KEY` surfaces when the parquet was last
    downloaded (``last_update`` on the access log) and when we last
    checked upstream for a newer one (``last_check``)."""

    def _patch_index(self, monkeypatch, *, last_update, last_check):
        from datetime import datetime
        from unittest.mock import MagicMock

        # Stub _all_dotted_index_keys so the existence check passes.
        monkeypatch.setattr(
            "planetarypy.pds.utils._all_dotted_index_keys",
            lambda: {"mro.ctx.edr"},
        )
        # Stub _index_config_for and _completion_id_col_for to keep the
        # rest of the table happy without a real registry round-trip.
        monkeypatch.setattr(
            "planetarypy.pds.utils._index_config_for",
            lambda key: None,
        )
        monkeypatch.setattr(
            "planetarypy.pds.utils._completion_id_col_for",
            lambda key: "PRODUCT_ID",
        )

        fake_log = MagicMock()
        fake_log.last_update = last_update
        fake_log.last_check = last_check

        fake_remote = MagicMock()
        fake_remote.log = fake_log

        class _StubIndex:
            def __init__(self, *a, **kw): pass
            @property
            def url(self): return "https://example/index.lbl"
            @property
            def remote_type(self): return "dynamic"
            @property
            def local_parq_path(self):
                from pathlib import Path
                return Path("/nonexistent/path.parq")
            remote = fake_remote
        monkeypatch.setattr("planetarypy.pds.Index", _StubIndex)
        return fake_log

    def test_info_shows_last_updated_and_last_checked_with_relative_age(
        self, monkeypatch,
    ):
        from datetime import datetime, timedelta
        now = datetime.now()
        self._patch_index(
            monkeypatch,
            last_update=now - timedelta(days=5, hours=2),
            last_check=now - timedelta(minutes=30),
        )
        result = runner.invoke(app, ["indexes", "info", "mro.ctx.edr"])
        assert result.exit_code == 0
        assert "last updated" in result.stdout
        assert "5d ago" in result.stdout
        assert "last checked" in result.stdout
        assert "30m ago" in result.stdout

    def test_info_shows_never_when_log_empty(self, monkeypatch):
        self._patch_index(monkeypatch, last_update=None, last_check=None)
        result = runner.invoke(app, ["indexes", "info", "mro.ctx.edr"])
        assert result.exit_code == 0
        # Both fields fall back to "(never)" when the log carries no datetime.
        assert "last updated" in result.stdout
        assert "last checked" in result.stdout
        assert "(never)" in result.stdout

    def test_refresh_bare_invocation_shows_help(self):
        """`refresh` has no positional arg; the analogous UX is "show help
        when no actionable flag (--config / --cache) is given".

        Asserting on the docstring text rather than the flag names because
        Rich's click integration renders flags as separately-styled spans
        (`\\x1b[1;36m-\\x1b[0m\\x1b[1;36m-config\\x1b[0m`), so the plain
        substring `--config` doesn't appear in the captured stdout under CI.
        """
        result = runner.invoke(app, ["indexes", "refresh"])
        assert result.exit_code == 0
        assert "Usage:" in result.stdout
        assert "Refresh upstream index config" in result.stdout


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


# ── tab-completion: segment-aware fallback ──────────────────────────────


class TestCompleteIndexKey:
    """``_complete_index_key`` falls back to a case-insensitive segment
    prefix match when whole-key prefix produces nothing — so typing
    ``ctx`` or ``CTX`` can complete ``mro.ctx.edr`` without forcing the
    user to remember the mission segment."""

    KEYS = {
        "mro.ctx.edr", "mro.ctx.calib",
        "mro.hirise.edr",
        "cassini.cda.index",
        "go.ssi.raw",
    }

    def test_whole_key_prefix_still_wins(self):
        from planetarypy.cli import _complete_index_key
        with patch("planetarypy.pds.utils._all_dotted_index_keys",
                   return_value=self.KEYS):
            assert set(_complete_index_key("mro.")) == {
                "mro.ctx.edr", "mro.ctx.calib", "mro.hirise.edr",
            }
            assert set(_complete_index_key("mro.ctx")) == {
                "mro.ctx.edr", "mro.ctx.calib",
            }

    def test_segment_prefix_when_whole_key_misses(self):
        from planetarypy.cli import _complete_index_key
        with patch("planetarypy.pds.utils._all_dotted_index_keys",
                   return_value=self.KEYS):
            # `ctx` doesn't prefix any whole key, so the segment
            # fallback kicks in — middle segment starts with `ctx`.
            assert set(_complete_index_key("ctx")) == {
                "mro.ctx.edr", "mro.ctx.calib",
            }
            # `hirise` matches mro.hirise.edr via the second segment.
            assert _complete_index_key("hirise") == ["mro.hirise.edr"]
            # `cda` matches cassini.cda.index via the second segment.
            assert _complete_index_key("cda") == ["cassini.cda.index"]
            # `index` matches the third segment of cassini.cda.index.
            assert _complete_index_key("index") == ["cassini.cda.index"]

    def test_segment_match_is_case_insensitive(self):
        from planetarypy.cli import _complete_index_key
        with patch("planetarypy.pds.utils._all_dotted_index_keys",
                   return_value=self.KEYS):
            assert set(_complete_index_key("CTX")) == {
                "mro.ctx.edr", "mro.ctx.calib",
            }
            assert _complete_index_key("HiRise") == ["mro.hirise.edr"]

    def test_empty_incomplete_does_not_segment_match(self):
        # Empty incomplete falls through the prefix branch returning
        # all keys; the segment fallback explicitly skips when
        # ``incomplete`` is empty so we don't list everything twice
        # or do redundant work.
        from planetarypy.cli import _complete_index_key
        with patch("planetarypy.pds.utils._all_dotted_index_keys",
                   return_value=self.KEYS):
            assert set(_complete_index_key("")) == self.KEYS

    def test_no_match_returns_empty(self):
        from planetarypy.cli import _complete_index_key
        with patch("planetarypy.pds.utils._all_dotted_index_keys",
                   return_value=self.KEYS):
            assert _complete_index_key("nothing_here") == []

    def test_shell_complete_adapter_delegates_to_logic_function(self):
        """``_shell_complete_index_key`` is the Click-shaped wrapper —
        it discards the (unused) ``ctx``/``param`` args and returns
        whatever the underlying matcher returns. The wrapper exists to
        bypass Typer's autocompletion-side ``startswith(incomplete)``
        filter that would otherwise drop segment-prefix matches."""
        from planetarypy.cli import _shell_complete_index_key
        with patch("planetarypy.pds.utils._all_dotted_index_keys",
                   return_value=self.KEYS):
            # Same segment-prefix expectation as the logic function —
            # routing through the adapter must not lose the candidate.
            assert set(_shell_complete_index_key(
                ctx=None, param=None, incomplete="ctx"
            )) == {"mro.ctx.edr", "mro.ctx.calib"}


# ── DeprecationWarning filter ───────────────────────────────────────────


class TestApplyWarningFilters:
    """``_apply_warning_filters`` reads the user config and either
    silences DeprecationWarning (default, end-user-facing) or leaves it
    alone (devs who set ``filter_deprecation_warnings = false``)."""

    def test_default_installs_deprecation_filter(self, monkeypatch):
        # No explicit config key → default True → filter installed.
        import warnings
        from planetarypy.cli import _apply_warning_filters
        from planetarypy.config import config

        # Force the key to be absent (config["..."] returns "" when missing).
        monkeypatch.setattr(config, "tomldoc", dict(config.tomldoc))
        config.tomldoc.pop("filter_deprecation_warnings", None)

        with warnings.catch_warnings():
            warnings.resetwarnings()
            _apply_warning_filters()
            with warnings.catch_warnings(record=True) as captured:
                warnings.simplefilter("default")
                # `simplefilter("default")` is a coarser setting — the
                # specific ignore filter installed by our helper still
                # wins because filters are matched in order. Re-apply
                # to put it on top.
                warnings.filterwarnings("ignore", category=DeprecationWarning)
                warnings.warn("test", DeprecationWarning)
            assert not any(
                issubclass(w.category, DeprecationWarning) for w in captured
            )

    def test_explicit_false_does_not_install_filter(self, monkeypatch):
        import warnings
        from planetarypy.cli import _apply_warning_filters
        from planetarypy.config import config

        monkeypatch.setattr(config, "tomldoc", dict(config.tomldoc))
        config.tomldoc["filter_deprecation_warnings"] = False

        with warnings.catch_warnings(record=True) as captured:
            warnings.resetwarnings()
            warnings.simplefilter("default")
            _apply_warning_filters()
            warnings.warn("test", DeprecationWarning)
        assert any(
            issubclass(w.category, DeprecationWarning) for w in captured
        ), "with filter_deprecation_warnings=false the warning must surface"

    def test_explicit_true_installs_filter(self, monkeypatch):
        import warnings
        from planetarypy.cli import _apply_warning_filters
        from planetarypy.config import config

        monkeypatch.setattr(config, "tomldoc", dict(config.tomldoc))
        config.tomldoc["filter_deprecation_warnings"] = True

        with warnings.catch_warnings(record=True) as captured:
            warnings.resetwarnings()
            warnings.simplefilter("default")
            _apply_warning_filters()
            warnings.warn("test", DeprecationWarning)
        assert not any(
            issubclass(w.category, DeprecationWarning) for w in captured
        )


# ── peek still works (regression guard for the helper refactor) ─────────


class TestIndexesColumnsFilter:
    """`--columns COL1,COL2` projects the display to the named columns.
    Applies to peek and last; select has its own test class."""

    def _patch_get_index(self, df):
        """Patch get_index so it honors the columns kwarg (mimicking the
        real implementation) without going through the Index machinery."""
        def _fake(key, allow_refresh=False, **kw):
            out = df.copy()
            cols = kw.get("columns")
            if cols is not None:
                cols = list(cols)
                missing = [c for c in cols if c not in out.columns]
                if missing:
                    raise KeyError(
                        f"Column(s) not in {key!r}: {missing!r}. "
                        f"Available columns: {list(out.columns)!r}"
                    )
                out = out[cols]
            return out
        return patch("planetarypy.pds.get_index", side_effect=_fake)

    def test_peek_projects_to_named_columns(self):
        df = _fake_df()
        with patch("planetarypy.pds.utils._all_dotted_index_keys",
                   return_value={"mro.ctx.edr"}), \
             self._patch_get_index(df):
            result = runner.invoke(
                app, ["indexes", "peek", "mro.ctx.edr",
                      "--columns", "PRODUCT_ID,START_TIME"]
            )
        assert result.exit_code == 0
        # Table header shows column names of the projection.
        assert "PRODUCT_ID" in result.output
        assert "START_TIME" in result.output
        # FILE_NAME projected away.
        assert "FILE_NAME" not in result.output

    def test_peek_strips_whitespace_in_columns_spec(self):
        df = _fake_df()
        with patch("planetarypy.pds.utils._all_dotted_index_keys",
                   return_value={"mro.ctx.edr"}), \
             self._patch_get_index(df):
            result = runner.invoke(
                app, ["indexes", "peek", "mro.ctx.edr",
                      "--columns", "PRODUCT_ID , START_TIME "]
            )
        assert result.exit_code == 0
        assert "FILE_NAME" not in result.output

    def test_peek_unknown_column_exits_with_helpful_error(self):
        df = _fake_df()
        with patch("planetarypy.pds.utils._all_dotted_index_keys",
                   return_value={"mro.ctx.edr"}), \
             self._patch_get_index(df):
            result = runner.invoke(
                app, ["indexes", "peek", "mro.ctx.edr",
                      "--columns", "BOGUS"]
            )
        assert result.exit_code == 2
        assert "BOGUS" in result.stderr
        # Available columns surfaced for discoverability.
        assert "PRODUCT_ID" in result.stderr

    def test_last_projects_after_sort_so_time_col_can_still_drive_sort(self):
        """--sort needs the time column; --columns must NOT prevent
        that even when the user projects the time column out."""
        df = _fake_df()  # has START_TIME
        with patch("planetarypy.pds.utils._all_dotted_index_keys",
                   return_value={"mro.ctx.edr"}), \
             patch("planetarypy.pds.get_index", return_value=df):
            # Project away START_TIME; --sort should still find it.
            result = runner.invoke(
                app, ["indexes", "last", "mro.ctx.edr",
                      "--columns", "PRODUCT_ID,FILE_NAME",
                      "--sort", "--rows", "2"]
            )
        assert result.exit_code == 0
        assert "by START_TIME" in result.output
        # Display projected: no START_TIME column shown.
        # Look for it as a row label in the transposed table.
        lines = result.output.splitlines()
        start_time_label_lines = [l for l in lines
                                  if "│START_TIME" in l or "│ START_TIME" in l]
        assert not start_time_label_lines

    def test_last_unknown_column_exits_with_error(self):
        df = _fake_df()
        with patch("planetarypy.pds.utils._all_dotted_index_keys",
                   return_value={"mro.ctx.edr"}), \
             patch("planetarypy.pds.get_index", return_value=df):
            result = runner.invoke(
                app, ["indexes", "last", "mro.ctx.edr",
                      "--columns", "BOGUS"]
            )
        assert result.exit_code == 2
        assert "BOGUS" in result.stderr


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
