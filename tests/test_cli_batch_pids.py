"""Tests for the batch-PID extensions to ``plp fetch``.

The catalog layer (`fetch_products`) and the utils layer (`parallel_map`,
`read_pids`) have their own tests. Here we exercise only the CLI wiring:
positional-variadic vs `--pids-from` argument handling, mutual exclusion,
and the four `--report` output modes.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

from typer.testing import CliRunner

import planetarypy.catalog as catalog_mod
from planetarypy.cli import app

runner = CliRunner()


def _fake_downloaded(pid: str) -> MagicMock:
    dp = MagicMock()
    dp.product_id = pid
    dp.local_dir = Path(f"/tmp/{pid}")
    dp.files = [Path(f"/tmp/{pid}/{pid}.IMG")]
    dp.label_file = None
    return dp


# ── Input handling ──────────────────────────────────────────────────────


class TestFetchInputHandling:

    def test_single_positional_pid_uses_single_path(self, monkeypatch):
        # Single-PID path goes through fetch_product, NOT fetch_products.
        called = {"single": 0, "batch": 0}
        def _single(key, pid, **kw):
            called["single"] += 1
            return _fake_downloaded(pid)
        def _batch(*a, **kw):
            called["batch"] += 1
            return []
        monkeypatch.setattr(catalog_mod, "fetch_product", _single)
        monkeypatch.setattr(catalog_mod, "fetch_products", _batch)
        monkeypatch.setattr(catalog_mod, "get_product_urls",
                            lambda k, p, **kw: {"data": "http://example/x"})

        result = runner.invoke(app, ["fetch", "mro.ctx.edr", "P_A"])
        assert result.exit_code == 0
        assert called["single"] == 1
        assert called["batch"] == 0

    def test_multiple_positional_pids_uses_batch_path(self, monkeypatch):
        import planetarypy.utils as utils_mod
        monkeypatch.setattr(utils_mod, "have_internet", lambda: True)
        monkeypatch.setattr(
            catalog_mod, "fetch_product",
            lambda key, pid, **kw: _fake_downloaded(pid),
        )

        result = runner.invoke(
            app, ["fetch", "mro.ctx.edr", "P_A", "P_B", "P_C"]
        )
        assert result.exit_code == 0
        # Summary line goes to stderr.
        assert "3/3 OK" in result.stderr

    def test_pids_from_file_uses_batch_path(self, monkeypatch, tmp_path):
        import planetarypy.utils as utils_mod
        monkeypatch.setattr(utils_mod, "have_internet", lambda: True)
        monkeypatch.setattr(
            catalog_mod, "fetch_product",
            lambda key, pid, **kw: _fake_downloaded(pid),
        )
        f = tmp_path / "pids.txt"
        f.write_text("# header\nP_A\nP_B\n\nP_C\n")

        result = runner.invoke(
            app, ["fetch", "mro.ctx.edr", "--pids-from", str(f)]
        )
        assert result.exit_code == 0
        assert "3/3 OK" in result.stderr

    def test_pids_from_and_positional_together_is_error(self, monkeypatch, tmp_path):
        f = tmp_path / "pids.txt"
        f.write_text("P_A\n")
        result = runner.invoke(
            app, ["fetch", "mro.ctx.edr", "P_X", "--pids-from", str(f)]
        )
        assert result.exit_code == 2
        assert "mutually exclusive" in result.stderr

    def test_no_pids_at_all_is_error(self):
        result = runner.invoke(app, ["fetch", "mro.ctx.edr"])
        assert result.exit_code == 2
        assert "provide at least one PID" in result.stderr

    def test_pids_from_csv_auto_detected_column(self, monkeypatch, tmp_path):
        """When --pids-from is a CSV, the PID column is auto-detected
        via the index registry (PRODUCT_ID for mro.ctx.edr)."""
        import pandas as pd
        import planetarypy.utils as utils_mod
        monkeypatch.setattr(utils_mod, "have_internet", lambda: True)
        monkeypatch.setattr(
            catalog_mod, "fetch_product",
            lambda key, pid, **kw: _fake_downloaded(pid),
        )
        f = tmp_path / "targets.csv"
        pd.DataFrame({
            "PRODUCT_ID": ["P_A", "P_B"],
            "DATE": ["2024-01", "2024-02"],
        }).to_csv(f, index=False)

        result = runner.invoke(
            app, ["fetch", "mro.ctx.edr", "--pids-from", str(f)]
        )
        assert result.exit_code == 0
        assert "2/2 OK" in result.stderr

    def test_pids_from_csv_with_explicit_pid_key(self, monkeypatch, tmp_path):
        import pandas as pd
        import planetarypy.utils as utils_mod
        monkeypatch.setattr(utils_mod, "have_internet", lambda: True)
        monkeypatch.setattr(
            catalog_mod, "fetch_product",
            lambda key, pid, **kw: _fake_downloaded(pid),
        )
        f = tmp_path / "weird.csv"
        pd.DataFrame({
            "my_pid": ["P_A", "P_B"],
            "extras": [1, 2],
        }).to_csv(f, index=False)

        result = runner.invoke(
            app, ["fetch", "mro.ctx.edr",
                  "--pids-from", str(f), "--pid-key", "my_pid"]
        )
        assert result.exit_code == 0
        assert "2/2 OK" in result.stderr

    def test_pid_suffix_appended_to_each_pid(self, monkeypatch, tmp_path):
        """--pid-suffix _RED turns each obsid into PSP_xxxxx_yyyy_RED
        before the fetcher sees it."""
        import planetarypy.utils as utils_mod
        monkeypatch.setattr(utils_mod, "have_internet", lambda: True)

        seen_pids: list[str] = []
        def _capture(key, pid, **kw):
            seen_pids.append(pid)
            return _fake_downloaded(pid)
        monkeypatch.setattr(catalog_mod, "fetch_product", _capture)

        f = tmp_path / "obsids.txt"
        f.write_text("PSP_001\nPSP_002\n")

        result = runner.invoke(
            app, ["fetch", "mro.hirise.rdr",
                  "--pids-from", str(f), "--pid-suffix", "_RED"]
        )
        assert result.exit_code == 0
        assert seen_pids == ["PSP_001_RED", "PSP_002_RED"]

    def test_pids_from_csv_with_ambiguous_columns_errors_helpfully(self, tmp_path):
        """CSV with no auto-detectable column should error and tell the
        user about --pid-key."""
        import pandas as pd
        f = tmp_path / "weird.csv"
        pd.DataFrame({"alpha": ["x"], "beta": ["y"]}).to_csv(f, index=False)

        result = runner.invoke(
            app, ["fetch", "mro.ctx.edr", "--pids-from", str(f)]
        )
        assert result.exit_code == 2
        assert "Cannot auto-detect" in result.stderr
        assert "pid_key" in result.stderr

    def test_missing_pids_file_is_error(self, tmp_path):
        result = runner.invoke(
            app, ["fetch", "mro.ctx.edr",
                  "--pids-from", str(tmp_path / "nope.txt")]
        )
        assert result.exit_code == 2

    def test_folder_flag_rejected_in_batch_mode(self, monkeypatch):
        import planetarypy.utils as utils_mod
        monkeypatch.setattr(utils_mod, "have_internet", lambda: True)
        result = runner.invoke(
            app, ["fetch", "mro.ctx.edr", "--folder", "P_A", "P_B"]
        )
        assert result.exit_code == 2
        assert "--folder is only valid with a single PID" in result.stderr


# ── --report modes ─────────────────────────────────────────────────────


class TestFetchReportModes:

    def _setup(self, monkeypatch, bad_pid: str | None = "BAD"):
        import planetarypy.utils as utils_mod
        monkeypatch.setattr(utils_mod, "have_internet", lambda: True)
        def _stub(key, pid, **kw):
            if pid == bad_pid:
                raise RuntimeError(f"no such product {pid}")
            return _fake_downloaded(pid)
        monkeypatch.setattr(catalog_mod, "fetch_product", _stub)

    def test_errors_only_default_silent_on_success(self, monkeypatch):
        self._setup(monkeypatch, bad_pid=None)
        result = runner.invoke(
            app, ["fetch", "mro.ctx.edr", "P_A", "P_B"]
        )
        assert result.exit_code == 0
        # No FAIL lines on stdout (errors-only writes failures to stderr).
        assert "FAIL" not in result.stdout
        # Summary appears on stderr.
        assert "2/2 OK" in result.stderr

    def test_errors_only_surfaces_failures_on_stderr(self, monkeypatch):
        self._setup(monkeypatch, bad_pid="BAD")
        result = runner.invoke(
            app, ["fetch", "mro.ctx.edr", "P_A", "BAD", "P_C"]
        )
        assert result.exit_code == 1
        # New format: "FAIL  BAD" on its own line, error indented below.
        assert "FAIL  BAD" in result.stderr
        assert "└" in result.stderr  # error-marker for the indented body
        assert "2/3 OK, 1 failed" in result.stderr

    def test_full_report_lists_every_pid(self, monkeypatch):
        self._setup(monkeypatch, bad_pid="BAD")
        result = runner.invoke(
            app, ["fetch", "mro.ctx.edr", "P_A", "BAD", "--report", "full"]
        )
        assert result.exit_code == 1
        # New format: "OK    P_A" (4-space gap to align with FAIL block)
        assert "OK    P_A" in result.stdout
        assert "FAIL  BAD" in result.stdout
        assert "└" in result.stdout  # indented error body

    def test_jsonl_report_is_machine_parseable(self, monkeypatch):
        self._setup(monkeypatch, bad_pid="BAD")
        result = runner.invoke(
            app, ["fetch", "mro.ctx.edr", "P_A", "BAD",
                  "--report", "jsonl"]
        )
        # jsonl mode always exits 0; consumers should parse the per-row ok.
        assert result.exit_code == 0
        lines = [json.loads(line) for line in result.stdout.strip().splitlines()]
        assert len(lines) == 2
        assert lines[0]["product_id"] == "P_A"
        assert lines[0]["ok"] is True
        assert "files" in lines[0]
        assert lines[1]["product_id"] == "BAD"
        assert lines[1]["ok"] is False
        assert "RuntimeError" in lines[1]["error"]

    def test_csv_report_has_header_and_one_row_per_pid(self, monkeypatch):
        self._setup(monkeypatch, bad_pid="BAD")
        result = runner.invoke(
            app, ["fetch", "mro.ctx.edr", "P_A", "BAD",
                  "--report", "csv"]
        )
        assert result.exit_code == 0
        lines = result.stdout.strip().splitlines()
        assert lines[0].startswith("product_id,ok,error,files")
        assert any(line.startswith("P_A,true,") for line in lines[1:])
        assert any(line.startswith("BAD,false,") for line in lines[1:])

    def test_fail_block_format_pid_on_own_line_error_indented(self, monkeypatch):
        """The new report format: PID on one line, error indented and
        wrapped on the lines below — easier to scan than the old
        `FAIL PID: ErrorType: msg` one-liner.
        (Batch path only; ≥2 PIDs needed to engage fetch_products.)"""
        self._setup(monkeypatch, bad_pid="BAD")
        result = runner.invoke(
            app, ["fetch", "mro.ctx.edr", "P_A", "BAD"]
        )
        # PID line is bare; error is indented under a └ marker.
        assert "FAIL  BAD\n" in result.stderr  # newline after PID
        assert "      └ " in result.stderr  # indented body marker
        assert "RuntimeError" in result.stderr

    def test_long_pid_is_truncated_with_middle_ellipsis(self, monkeypatch):
        """PIDs over 60 chars get middle-ellipsis truncation so the
        report doesn't wrap horizontally on the user's terminal.
        (Batch path only — single-PID call uses the older codepath.)"""
        long_pid = "X" * 100  # 100 chars, well over the 60-char cap
        short_pid = "Y_SHORT"
        def _fail(key, pid, **kw):
            raise RuntimeError("nope")
        monkeypatch.setattr(catalog_mod, "fetch_product", _fail)
        import planetarypy.utils as utils_mod
        monkeypatch.setattr(utils_mod, "have_internet", lambda: True)

        # 2 PIDs → batch path → _format_fail_block engages.
        result = runner.invoke(app, ["fetch", "mro.ctx.edr",
                                     long_pid, short_pid])
        assert "…" in result.stderr  # ellipsis present in the long-PID line
        # Truncated long-PID line should now fit a reasonable terminal.
        for line in result.stderr.splitlines():
            if line.startswith("FAIL") and "…" in line:
                assert len(line) < 80

    def test_full_mode_separates_entries_with_blank_lines(self, monkeypatch):
        """Visual separation: blank line between every entry so failures
        don't pile into a wall of text."""
        self._setup(monkeypatch, bad_pid="BAD")
        result = runner.invoke(
            app, ["fetch", "mro.ctx.edr",
                  "P_A", "BAD", "P_C", "--report", "full"]
        )
        # \n\n appears between OK lines and FAIL blocks.
        assert "\n\n" in result.stdout

    def test_unknown_report_mode_rejected(self, monkeypatch):
        self._setup(monkeypatch, bad_pid=None)
        result = runner.invoke(
            app, ["fetch", "mro.ctx.edr", "P_A", "P_B",
                  "--report", "bogus"]
        )
        assert result.exit_code == 2
        assert "unknown --report mode" in result.stderr


# ── Offline preflight ───────────────────────────────────────────────────


class TestFetchOfflinePreflight:

    def test_offline_aborts_batch_with_nonzero_exit(self, monkeypatch):
        import planetarypy.utils as utils_mod
        monkeypatch.setattr(utils_mod, "have_internet", lambda: False)

        called = {"n": 0}
        def _spy(key, pid, **kw):
            called["n"] += 1
            return _fake_downloaded(pid)
        monkeypatch.setattr(catalog_mod, "fetch_product", _spy)

        result = runner.invoke(
            app, ["fetch", "mro.ctx.edr", "P_A", "P_B"]
        )
        assert result.exit_code == 1
        assert called["n"] == 0
        assert "No internet connection" in result.stderr
