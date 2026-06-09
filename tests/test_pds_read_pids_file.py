"""Tests for ``planetarypy.pds.read_pids_file``.

Covers the CSV-aware reader that backs the ``--pids-from`` CLI option:
extension dispatch, ``pid_column`` auto-detection, explicit ``pid_key``
override, and error paths for ambiguous CSVs.
"""
from __future__ import annotations

import io

import pandas as pd
import pytest

from planetarypy.pds import read_pids_file


# ── Plain text passthrough ─────────────────────────────────────────────


class TestTextPassthrough:

    def test_txt_file_one_pid_per_line(self, tmp_path):
        f = tmp_path / "targets.txt"
        f.write_text("PID_A\nPID_B\nPID_C\n")
        assert read_pids_file(f) == ["PID_A", "PID_B", "PID_C"]

    def test_no_extension_treated_as_text(self, tmp_path):
        f = tmp_path / "targets"
        f.write_text("PID_A\n# comment\nPID_B\n")
        assert read_pids_file(f) == ["PID_A", "PID_B"]

    def test_stdin_dash_always_plain_text(self, monkeypatch):
        monkeypatch.setattr("sys.stdin", io.StringIO("PID_X\nPID_Y\n"))
        assert read_pids_file("-") == ["PID_X", "PID_Y"]

    def test_index_key_ignored_for_plain_text(self, tmp_path):
        # index_key is only consulted for CSV. Plain text always passes through.
        f = tmp_path / "targets.txt"
        f.write_text("PID_A\nPID_B\n")
        assert read_pids_file(f, index_key="mro.ctx.edr") == ["PID_A", "PID_B"]


# ── CSV auto-detection via pid_column ──────────────────────────────────


class TestCsvAutoDetect:

    def test_csv_with_product_id_column(self, tmp_path):
        f = tmp_path / "targets.csv"
        pd.DataFrame({
            "PRODUCT_ID": ["P_001", "P_002", "P_003"],
            "DATE": ["2024-01-01", "2024-02-01", "2024-03-01"],
        }).to_csv(f, index=False)
        out = read_pids_file(f, index_key="mro.ctx.edr")
        assert out == ["P_001", "P_002", "P_003"]

    def test_csv_with_file_name_only(self, tmp_path):
        # cassini.uvis.index is registered with product_id_col=FILE_NAME
        f = tmp_path / "targets.csv"
        pd.DataFrame({
            "FILE_NAME": ["A.LBL", "B.LBL"],
            "SCLK": [1, 2],
        }).to_csv(f, index=False)
        out = read_pids_file(f, index_key="cassini.uvis.index")
        assert out == ["A.LBL", "B.LBL"]

    def test_csv_with_no_recognizable_column_raises_valueerror(self, tmp_path):
        f = tmp_path / "weird.csv"
        pd.DataFrame({
            "wibble": ["x1", "x2"],
            "wobble": ["y1", "y2"],
        }).to_csv(f, index=False)
        with pytest.raises(ValueError, match="Cannot auto-detect"):
            read_pids_file(f, index_key="mro.ctx.edr")

    def test_error_message_lists_columns_and_suggests_pid_key(self, tmp_path):
        f = tmp_path / "weird.csv"
        pd.DataFrame({"alpha": [1], "beta": [2]}).to_csv(f, index=False)
        with pytest.raises(ValueError) as exc:
            read_pids_file(f, index_key="mro.ctx.edr")
        msg = str(exc.value)
        assert "alpha" in msg and "beta" in msg
        assert "pid_key" in msg

    def test_csv_without_index_key_and_without_pid_key_raises(self, tmp_path):
        # No index_key, no pid_key → no way to choose. Should raise even if
        # PRODUCT_ID is sitting right there: we don't guess without a hint.
        f = tmp_path / "targets.csv"
        pd.DataFrame({"PRODUCT_ID": ["P_A"]}).to_csv(f, index=False)
        with pytest.raises(ValueError, match="Cannot auto-detect"):
            read_pids_file(f)


# ── CSV with explicit pid_key ──────────────────────────────────────────


class TestCsvExplicitPidKey:

    def test_pid_key_picks_named_column(self, tmp_path):
        f = tmp_path / "weird.csv"
        pd.DataFrame({
            "my_pid": ["A", "B", "C"],
            "other": [1, 2, 3],
        }).to_csv(f, index=False)
        out = read_pids_file(f, pid_key="my_pid")
        assert out == ["A", "B", "C"]

    def test_pid_key_wins_over_auto_detection(self, tmp_path):
        # Both PRODUCT_ID and a custom column present; --pid-key should
        # win over the pid_column resolver.
        f = tmp_path / "both.csv"
        pd.DataFrame({
            "PRODUCT_ID": ["AUTO_A", "AUTO_B"],
            "OBSERVATION_TARGET": ["MARS_A", "MARS_B"],
        }).to_csv(f, index=False)
        out = read_pids_file(
            f, index_key="mro.ctx.edr", pid_key="OBSERVATION_TARGET",
        )
        assert out == ["MARS_A", "MARS_B"]

    def test_unknown_pid_key_raises_keyerror_with_columns_listed(self, tmp_path):
        f = tmp_path / "targets.csv"
        pd.DataFrame({"alpha": ["x"], "beta": ["y"]}).to_csv(f, index=False)
        with pytest.raises(KeyError) as exc:
            read_pids_file(f, pid_key="gamma")
        msg = str(exc.value)
        assert "alpha" in msg and "beta" in msg


# ── suffix kwarg ───────────────────────────────────────────────────────


class TestSuffix:

    def test_suffix_appended_to_text_pids(self, tmp_path):
        f = tmp_path / "obsids.txt"
        f.write_text("PSP_001\nPSP_002\n")
        assert read_pids_file(f, suffix="_RED") == ["PSP_001_RED", "PSP_002_RED"]

    def test_suffix_appended_to_csv_pids(self, tmp_path):
        f = tmp_path / "obsids.csv"
        pd.DataFrame({"PRODUCT_ID": ["PSP_001", "PSP_002"]}).to_csv(f, index=False)
        out = read_pids_file(f, index_key="mro.ctx.edr", suffix="_RED")
        assert out == ["PSP_001_RED", "PSP_002_RED"]

    def test_suffix_with_explicit_pid_key(self, tmp_path):
        f = tmp_path / "alt.csv"
        pd.DataFrame({"obsid": ["A", "B"]}).to_csv(f, index=False)
        assert read_pids_file(f, pid_key="obsid", suffix="_X") == ["A_X", "B_X"]

    def test_suffix_none_is_noop(self, tmp_path):
        f = tmp_path / "obsids.txt"
        f.write_text("A\nB\n")
        assert read_pids_file(f, suffix=None) == ["A", "B"]

    def test_empty_suffix_is_noop(self, tmp_path):
        f = tmp_path / "obsids.txt"
        f.write_text("A\nB\n")
        # Empty string is falsy → treated as no suffix (avoids "A" → "A").
        assert read_pids_file(f, suffix="") == ["A", "B"]


# ── pid_key forces CSV parsing on stdin / non-csv paths ───────────────


class TestPidKeyForcesCsv:

    def test_pid_key_parses_stdin_as_csv(self, monkeypatch):
        """`head file.csv | plp fetch ... --pid-key obsid` should work
        the same as if the file had been passed directly."""
        csv_text = "obsid,date\nP_A,2024-01\nP_B,2024-02\n"
        monkeypatch.setattr("sys.stdin", io.StringIO(csv_text))
        assert read_pids_file("-", pid_key="obsid") == ["P_A", "P_B"]

    def test_pid_key_parses_txt_as_csv(self, tmp_path):
        """A user with CSV content saved as .txt can still opt in by
        passing pid_key — the flag is the user's CSV declaration."""
        f = tmp_path / "data.txt"
        f.write_text("obsid,date\nP_A,2024-01\nP_B,2024-02\n")
        assert read_pids_file(f, pid_key="obsid") == ["P_A", "P_B"]

    def test_pid_key_on_stdin_combines_with_suffix(self, monkeypatch):
        csv_text = "obsid\nPSP_001\nPSP_002\n"
        monkeypatch.setattr("sys.stdin", io.StringIO(csv_text))
        assert read_pids_file(
            "-", pid_key="obsid", suffix="_RED",
        ) == ["PSP_001_RED", "PSP_002_RED"]

    def test_pid_key_on_stdin_with_missing_column_raises(self, monkeypatch):
        csv_text = "alpha,beta\nx,y\n"
        monkeypatch.setattr("sys.stdin", io.StringIO(csv_text))
        with pytest.raises(KeyError) as exc:
            read_pids_file("-", pid_key="obsid")
        assert "obsid" in str(exc.value)
        assert "alpha" in str(exc.value) and "beta" in str(exc.value)

    def test_stdin_without_pid_key_stays_plain_text(self, monkeypatch):
        """Sanity: the existing stdin-as-text behavior is preserved
        when pid_key is absent. CSV-on-stdin requires opting in."""
        monkeypatch.setattr("sys.stdin", io.StringIO("P_A\nP_B\n"))
        assert read_pids_file("-") == ["P_A", "P_B"]


# ── Stdin auto-detection via first-line comma sniff ───────────────────


class TestStdinAutoDetect:

    def test_stdin_csv_auto_detected_via_comma_sniff(self, monkeypatch):
        """`head file.csv | plp fetch KEY --pids-from -` works without
        --pid-key: the first-line comma trips the sniff, pid_column
        finds PRODUCT_ID, and the column is read."""
        csv_text = "PRODUCT_ID,DATE\nP_A,2024-01\nP_B,2024-02\n"
        monkeypatch.setattr("sys.stdin", io.StringIO(csv_text))
        assert read_pids_file("-", index_key="mro.ctx.edr") == ["P_A", "P_B"]

    def test_stdin_csv_with_suffix(self, monkeypatch):
        csv_text = "PRODUCT_ID\nPSP_001\nPSP_002\n"
        monkeypatch.setattr("sys.stdin", io.StringIO(csv_text))
        # Single-column CSV with PRODUCT_ID header — no comma in the
        # body lines, but the header has none either, so the sniff
        # says plain text. Document the limitation.
        # (When the user wants this case to work, --pid-key PRODUCT_ID
        # forces CSV regardless.)
        out = read_pids_file("-", index_key="mro.ctx.edr", suffix="_RED")
        # Plain-text path: every line is a PID, including "PRODUCT_ID".
        assert out == ["PRODUCT_ID_RED", "PSP_001_RED", "PSP_002_RED"]

    def test_stdin_csv_no_recognizable_column_errors_with_hint(self, monkeypatch):
        """Comma trips the sniff, CSV is parsed, but the column names
        don't match the registry → ValueError pointing at --pid-key."""
        csv_text = "alpha,beta\nx,y\nm,n\n"
        monkeypatch.setattr("sys.stdin", io.StringIO(csv_text))
        with pytest.raises(ValueError, match="Cannot auto-detect"):
            read_pids_file("-", index_key="mro.ctx.edr")

    def test_stdin_plain_text_no_comma_stays_plain(self, monkeypatch):
        """No comma anywhere → plain text path; pid_column never runs."""
        monkeypatch.setattr("sys.stdin", io.StringIO("P_A\nP_B\nP_C\n"))
        assert read_pids_file("-", index_key="mro.ctx.edr") == [
            "P_A", "P_B", "P_C",
        ]

    def test_stdin_plain_text_with_comment_and_blank(self, monkeypatch):
        """Buffered-stdin path still strips blanks + #-comments
        identically to utils.read_pids."""
        monkeypatch.setattr("sys.stdin", io.StringIO(
            "# header\nP_A\n\n  # indented\nP_B\n"
        ))
        assert read_pids_file("-") == ["P_A", "P_B"]


# ── Round-trip with the CLI csv output format ──────────────────────────


class TestRoundTripWithCliCsvOutput:

    def test_csv_emitted_by_indexes_select_can_be_read_back(self, tmp_path):
        # Mirrors the output shape of `plp indexes select --format csv`:
        # standard PDS columns including PRODUCT_ID.
        f = tmp_path / "select_output.csv"
        pd.DataFrame({
            "PRODUCT_ID": ["P_001", "P_002", "P_003"],
            "FILE_NAME": ["f1.IMG", "f2.IMG", "f3.IMG"],
            "START_TIME": ["2024-01", "2024-02", "2024-03"],
        }).to_csv(f, index=False)
        assert read_pids_file(f, index_key="mro.ctx.edr") == [
            "P_001", "P_002", "P_003",
        ]


# ── TSV / auto-delimiter ───────────────────────────────────────────────


class TestTsvAutoDelimiter:

    def test_tsv_extension_with_index_key(self, tmp_path):
        f = tmp_path / "targets.tsv"
        pd.DataFrame({
            "PRODUCT_ID": ["P_001", "P_002"],
            "DATE": ["2024-01-01", "2024-02-01"],
        }).to_csv(f, sep="\t", index=False)
        assert read_pids_file(f, index_key="mro.ctx.edr") == ["P_001", "P_002"]

    def test_tsv_extension_with_pid_key(self, tmp_path):
        f = tmp_path / "targets.tsv"
        pd.DataFrame({
            "observation_id": ["ESP_1", "ESP_2"],
            "lat": [1.0, 2.0],
        }).to_csv(f, sep="\t", index=False)
        assert read_pids_file(f, pid_key="observation_id") == ["ESP_1", "ESP_2"]

    def test_tab_extension(self, tmp_path):
        f = tmp_path / "targets.tab"
        pd.DataFrame({"PRODUCT_ID": ["P_A"]}).to_csv(f, sep="\t", index=False)
        assert read_pids_file(f, index_key="mro.ctx.edr") == ["P_A"]

    def test_tab_delimited_csv_extension_with_pid_key(self, tmp_path):
        """The reported bug: a tab-separated file named .csv with
        --pid-key. The delimiter must auto-detect as tab so the named
        column is found, instead of the whole header collapsing into a
        single column (which produced 'is not a column')."""
        f = tmp_path / "seasonal.csv"
        f.write_text(
            'observation_id\t"roll"\t"Status"\n'
            'ESP_075205_0930\t-5\tCOMPLETED\n'
            'ESP_075205_0931\t3\tPLANNED\n'
        )
        out = read_pids_file(f, pid_key="observation_id")
        assert out == ["ESP_075205_0930", "ESP_075205_0931"]

    def test_tab_delimited_csv_extension_auto_detect_column(self, tmp_path):
        f = tmp_path / "seasonal.csv"
        pd.DataFrame({
            "PRODUCT_ID": ["P_001", "P_002"],
            "extra": ["a", "b"],
        }).to_csv(f, sep="\t", index=False)
        assert read_pids_file(f, index_key="mro.ctx.edr") == ["P_001", "P_002"]

    def test_stdin_tab_sniff_routes_to_csv_without_pid_key(self, monkeypatch):
        # A tab in the first line alone must route stdin to CSV mode (not
        # plain text); index_key then auto-detects the column.
        monkeypatch.setattr(
            "sys.stdin",
            io.StringIO("PRODUCT_ID\tDATE\nP_1\t2024\nP_2\t2024\n"),
        )
        assert read_pids_file("-", index_key="mro.ctx.edr") == ["P_1", "P_2"]

    def test_stdin_tab_with_pid_key(self, monkeypatch):
        monkeypatch.setattr(
            "sys.stdin",
            io.StringIO("observation_id\tlat\nESP_1\t1\nESP_2\t2\n"),
        )
        assert read_pids_file("-", pid_key="observation_id") == ["ESP_1", "ESP_2"]

    def test_comma_csv_unaffected_by_tab_detection(self, tmp_path):
        # Guard: ordinary comma CSV must still parse (no tab → comma).
        f = tmp_path / "targets.csv"
        pd.DataFrame({"PRODUCT_ID": ["P_1", "P_2"]}).to_csv(f, index=False)
        assert read_pids_file(f, index_key="mro.ctx.edr") == ["P_1", "P_2"]

    def test_tsv_suffix_appended(self, tmp_path):
        f = tmp_path / "obsids.tsv"
        pd.DataFrame({"observation_id": ["PSP_1", "PSP_2"]}).to_csv(
            f, sep="\t", index=False)
        out = read_pids_file(f, pid_key="observation_id", suffix="_RED")
        assert out == ["PSP_1_RED", "PSP_2_RED"]
