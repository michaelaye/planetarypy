"""Tests for ``planetarypy.utils.read_pids``."""
from __future__ import annotations

import io

import pytest

from planetarypy.utils import read_pids


def test_read_pids_one_per_line(tmp_path):
    f = tmp_path / "pids.txt"
    f.write_text("PID_A\nPID_B\nPID_C\n")
    assert read_pids(f) == ["PID_A", "PID_B", "PID_C"]


def test_read_pids_strips_blank_lines(tmp_path):
    f = tmp_path / "pids.txt"
    f.write_text("PID_A\n\n   \nPID_B\n\nPID_C\n")
    assert read_pids(f) == ["PID_A", "PID_B", "PID_C"]


def test_read_pids_strips_hash_comments(tmp_path):
    f = tmp_path / "pids.txt"
    f.write_text(
        "# header comment\n"
        "PID_A\n"
        "   # indented comment\n"
        "PID_B\n"
        "# trailing comment\n"
    )
    assert read_pids(f) == ["PID_A", "PID_B"]


def test_read_pids_preserves_order_no_dedup(tmp_path):
    """Order matters and duplicates are NOT removed — caller decides."""
    f = tmp_path / "pids.txt"
    f.write_text("PID_B\nPID_A\nPID_B\nPID_A\n")
    assert read_pids(f) == ["PID_B", "PID_A", "PID_B", "PID_A"]


def test_read_pids_rstrips_trailing_whitespace(tmp_path):
    f = tmp_path / "pids.txt"
    f.write_text("PID_A   \nPID_B\t\nPID_C\n")
    assert read_pids(f) == ["PID_A", "PID_B", "PID_C"]


def test_read_pids_accepts_str_path(tmp_path):
    f = tmp_path / "pids.txt"
    f.write_text("PID_A\nPID_B\n")
    assert read_pids(str(f)) == ["PID_A", "PID_B"]


def test_read_pids_from_stdin(monkeypatch):
    monkeypatch.setattr("sys.stdin", io.StringIO("PID_X\n# comment\nPID_Y\n"))
    assert read_pids("-") == ["PID_X", "PID_Y"]


def test_read_pids_empty_file_returns_empty_list(tmp_path):
    f = tmp_path / "empty.txt"
    f.write_text("")
    assert read_pids(f) == []


def test_read_pids_only_comments_returns_empty_list(tmp_path):
    f = tmp_path / "comments.txt"
    f.write_text("# just a header\n# nothing real here\n")
    assert read_pids(f) == []


def test_read_pids_missing_file_raises_filenotfound(tmp_path):
    with pytest.raises(FileNotFoundError):
        read_pids(tmp_path / "does_not_exist.txt")
