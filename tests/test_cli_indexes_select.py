"""Tests for the ``plp indexes select`` verb (Option B batch filter)."""
from __future__ import annotations

import json
from unittest.mock import patch

import pandas as pd
from typer.testing import CliRunner

from planetarypy.cli import app

runner = CliRunner()


def _fake_df() -> pd.DataFrame:
    return pd.DataFrame({
        "PRODUCT_ID": ["P_001", "P_002", "P_003", "P_004", "P_005"],
        "FILE_NAME": [f"file_{i}.IMG" for i in range(1, 6)],
        "START_TIME": [f"2024-{m:02d}-01T00:00:00" for m in range(1, 6)],
    })


def _patch_index(df: pd.DataFrame):
    """Context-manager-style: patch _all_dotted_index_keys + Index.

    Returns the two patch objects so callers can stack them with ``with``.
    """
    keys_patch = patch(
        "planetarypy.pds.utils._all_dotted_index_keys",
        return_value={"mro.ctx.edr"},
    )
    class _StubIndex:
        def __init__(self, *a, **kw): pass
        def ensure_parquet(self, force=False): return False
        @property
        def update_available(self): return False
        @property
        def dataframe(self): return df
    index_patch = patch("planetarypy.pds.Index", _StubIndex)
    return keys_patch, index_patch


class TestIndexesSelectInputHandling:

    def test_bare_invocation_shows_help_and_exits_zero(self):
        """Missing KEY → print help, exit 0 (user's preferred UX scheme)."""
        result = runner.invoke(app, ["indexes", "select"])
        assert result.exit_code == 0
        assert "Filter a registered PDS index" in result.stdout
        assert "Usage:" in result.stdout

    def test_unknown_key_exits_with_error(self):
        with patch("planetarypy.pds.utils._all_dotted_index_keys",
                   return_value={"mro.ctx.edr"}):
            result = runner.invoke(
                app, ["indexes", "select", "no.such.key", "P_001"]
            )
        assert result.exit_code == 1
        assert "Unknown index key" in result.stderr

    def test_no_pids_at_all_is_error(self):
        with patch("planetarypy.pds.utils._all_dotted_index_keys",
                   return_value={"mro.ctx.edr"}):
            result = runner.invoke(app, ["indexes", "select", "mro.ctx.edr"])
        assert result.exit_code == 2
        assert "provide at least one PID" in result.stderr

    def test_positional_and_pids_from_mutually_exclusive(self, tmp_path):
        f = tmp_path / "pids.txt"
        f.write_text("P_001\n")
        with patch("planetarypy.pds.utils._all_dotted_index_keys",
                   return_value={"mro.ctx.edr"}):
            result = runner.invoke(
                app, ["indexes", "select", "mro.ctx.edr",
                      "P_001", "--pids-from", str(f)]
            )
        assert result.exit_code == 2
        assert "mutually exclusive" in result.stderr


class TestIndexesSelectFormats:

    def test_auto_uses_table_for_few_rows(self):
        df = _fake_df()
        keys_p, idx_p = _patch_index(df)
        with keys_p, idx_p:
            result = runner.invoke(
                app, ["indexes", "select", "mro.ctx.edr", "P_002"]
            )
        assert result.exit_code == 0
        # Rich table output includes the field-name column.
        assert "PRODUCT_ID" in result.stdout
        assert "P_002" in result.stdout

    def test_auto_switches_to_csv_above_threshold(self):
        df = _fake_df()
        keys_p, idx_p = _patch_index(df)
        with keys_p, idx_p:
            # 4 PIDs, default --max-table-rows=4 → switch to CSV.
            result = runner.invoke(
                app, ["indexes", "select", "mro.ctx.edr",
                      "P_001", "P_002", "P_003", "P_004"]
            )
        assert result.exit_code == 0
        lines = result.stdout.strip().splitlines()
        # First line is CSV header.
        assert lines[0].startswith("PRODUCT_ID,FILE_NAME,START_TIME")
        # 4 data rows after header.
        assert len(lines) == 5

    def test_explicit_table_overrides_auto(self):
        df = _fake_df()
        keys_p, idx_p = _patch_index(df)
        with keys_p, idx_p:
            result = runner.invoke(
                app, ["indexes", "select", "mro.ctx.edr",
                      "P_001", "P_002", "P_003", "P_004",
                      "--format", "table"]
            )
        assert result.exit_code == 0
        # Table format has field-name labels, not a CSV header.
        assert "PRODUCT_ID,FILE_NAME" not in result.stdout

    def test_explicit_csv_overrides_auto(self):
        df = _fake_df()
        keys_p, idx_p = _patch_index(df)
        with keys_p, idx_p:
            result = runner.invoke(
                app, ["indexes", "select", "mro.ctx.edr",
                      "P_002", "--format", "csv"]
            )
        assert result.exit_code == 0
        lines = result.stdout.strip().splitlines()
        assert lines[0].startswith("PRODUCT_ID,")
        assert any("P_002" in line for line in lines[1:])

    def test_jsonl_output_is_parseable(self):
        df = _fake_df()
        keys_p, idx_p = _patch_index(df)
        with keys_p, idx_p:
            result = runner.invoke(
                app, ["indexes", "select", "mro.ctx.edr",
                      "P_001", "P_003", "--format", "jsonl"]
            )
        assert result.exit_code == 0
        rows = [json.loads(line) for line in result.stdout.strip().splitlines()]
        assert len(rows) == 2
        assert {r["PRODUCT_ID"] for r in rows} == {"P_001", "P_003"}

    def test_max_table_rows_threshold_configurable_via_flag(self):
        df = _fake_df()
        keys_p, idx_p = _patch_index(df)
        with keys_p, idx_p:
            # Bump threshold so 4 rows stays in table mode.
            result = runner.invoke(
                app, ["indexes", "select", "mro.ctx.edr",
                      "P_001", "P_002", "P_003", "P_004",
                      "--max-table-rows", "10"]
            )
        assert result.exit_code == 0
        # No CSV header line when table mode is used.
        assert "PRODUCT_ID,FILE_NAME" not in result.stdout

    def test_max_table_rows_threshold_from_config(self, monkeypatch):
        """Without --max-table-rows on the CLI, the value comes from the
        main config (default 4; user can bump or lower)."""
        df = _fake_df()
        # Stub the config lookup to return a custom threshold.
        import planetarypy.cli as cli_mod
        class _CfgStub:
            def __getitem__(self, key):
                return 10 if key == "max_table_rows" else ""
        monkeypatch.setattr("planetarypy.config.config", _CfgStub())

        keys_p, idx_p = _patch_index(df)
        with keys_p, idx_p:
            # 4 PIDs would normally hit the hardcoded default of 4 and
            # switch to CSV. With config-set 10, stay in table.
            result = runner.invoke(
                app, ["indexes", "select", "mro.ctx.edr",
                      "P_001", "P_002", "P_003", "P_004"]
            )
        assert result.exit_code == 0
        assert "PRODUCT_ID,FILE_NAME" not in result.stdout

    def test_cli_flag_overrides_config_value(self, monkeypatch):
        """CLI ``--max-table-rows`` wins over the config value."""
        df = _fake_df()
        class _CfgStub:
            def __getitem__(self, key):
                # Config says "always use table" — but CLI flag forces CSV.
                return 100 if key == "max_table_rows" else ""
        monkeypatch.setattr("planetarypy.config.config", _CfgStub())

        keys_p, idx_p = _patch_index(df)
        with keys_p, idx_p:
            result = runner.invoke(
                app, ["indexes", "select", "mro.ctx.edr",
                      "P_001", "P_002", "P_003", "P_004",
                      "--max-table-rows", "2"]
            )
        assert result.exit_code == 0
        # 4 rows >= 2 → CSV mode despite generous config.
        assert "PRODUCT_ID,FILE_NAME,START_TIME" in result.stdout


class TestIndexesSelectMissingReporting:

    def test_missing_pids_summary_default(self):
        df = _fake_df()
        keys_p, idx_p = _patch_index(df)
        with keys_p, idx_p:
            result = runner.invoke(
                app, ["indexes", "select", "mro.ctx.edr",
                      "P_001", "GHOST_A", "GHOST_B"]
            )
        # Exit nonzero because at least one missing.
        assert result.exit_code == 1
        # Default errors-only mode: summary only, not each PID.
        assert "2 not found" in result.stderr
        assert "MISS GHOST_A" not in result.stderr

    def test_missing_pids_full_lists_each(self):
        df = _fake_df()
        keys_p, idx_p = _patch_index(df)
        with keys_p, idx_p:
            result = runner.invoke(
                app, ["indexes", "select", "mro.ctx.edr",
                      "P_001", "GHOST_A", "GHOST_B",
                      "--report", "full"]
            )
        assert result.exit_code == 1
        assert "MISS GHOST_A" in result.stderr
        assert "MISS GHOST_B" in result.stderr

    def test_all_found_exits_zero(self):
        df = _fake_df()
        keys_p, idx_p = _patch_index(df)
        with keys_p, idx_p:
            result = runner.invoke(
                app, ["indexes", "select", "mro.ctx.edr", "P_001", "P_002"]
            )
        assert result.exit_code == 0
        assert "all found" in result.stderr

    def test_pids_from_file(self, tmp_path):
        df = _fake_df()
        f = tmp_path / "pids.txt"
        f.write_text("# header\nP_002\n\nP_004\n")
        keys_p, idx_p = _patch_index(df)
        with keys_p, idx_p:
            result = runner.invoke(
                app, ["indexes", "select", "mro.ctx.edr",
                      "--pids-from", str(f)]
            )
        assert result.exit_code == 0
        assert "P_002" in result.stdout
        assert "P_004" in result.stdout
