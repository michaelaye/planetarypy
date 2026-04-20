"""Tests for the catalog AST parser."""

import csv
import textwrap
from pathlib import Path

import pytest

from planetarypy.catalog._parser import (
    parse_selection_rules,
    parse_test_csv,
    find_test_csvs,
)


@pytest.fixture
def tmp_rules_file(tmp_path):
    """Create a mock selection_rules.py file."""
    content = textwrap.dedent('''\
        """Mock selection rules."""

        IMG_FILE = "img_usgs_cassini"
        ATM_FILE = "atm"

        file_information = {
            "edr": {
                "manifest": IMG_FILE,
                "fn_must_contain": [".IMG"],
                "url_must_contain": ["/data/"],
                "label": "A",
            },
            "calib": {
                "manifest": ATM_FILE,
                "fn_regex": [r"cal_.*\\\\.dat"],
                "label": "D",
                "support_np": True,
            },
            "rdr": {
                "manifest": IMG_FILE,
                "fn_ends_with": [".jp2"],
                "url_must_contain": ["/RDR/"],
                "label": "D",
            },
        }
    ''')
    p = tmp_path / "selection_rules.py"
    p.write_text(content)
    return p


@pytest.fixture
def tmp_csv_file(tmp_path):
    """Create a mock test CSV file."""
    p = tmp_path / "edr_test.csv"
    with p.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["label_file", "files", "product_id", "url_stem", "hash"])
        writer.writeheader()
        writer.writerow({
            "label_file": "N1234.LBL",
            "files": '["N1234.LBL", "N1234.IMG"]',
            "product_id": "1_N1234",
            "url_stem": "http://example.com/data/123",
            "hash": '{"IMAGE": "abc123"}',
        })
    return p


def test_parse_selection_rules_basic(tmp_rules_file):
    """Test that basic selection rules are parsed correctly."""
    result = parse_selection_rules(tmp_rules_file)
    assert "edr" in result
    assert "calib" in result
    assert "rdr" in result


def test_parse_selection_rules_variable_resolution(tmp_rules_file):
    """Test that manifest variable references are resolved."""
    result = parse_selection_rules(tmp_rules_file)
    assert result["edr"]["manifest"] == "img_usgs_cassini"
    assert result["calib"]["manifest"] == "atm"


def test_parse_selection_rules_list_fields(tmp_rules_file):
    """Test that list fields are parsed correctly."""
    result = parse_selection_rules(tmp_rules_file)
    assert result["edr"]["fn_must_contain"] == [".IMG"]
    assert result["edr"]["url_must_contain"] == ["/data/"]
    assert result["rdr"]["fn_ends_with"] == [".jp2"]


def test_parse_selection_rules_boolean_fields(tmp_rules_file):
    """Test that boolean fields are parsed correctly."""
    result = parse_selection_rules(tmp_rules_file)
    assert result["calib"]["support_np"] is True


def test_parse_selection_rules_label_types(tmp_rules_file):
    """Test that label types are parsed correctly."""
    result = parse_selection_rules(tmp_rules_file)
    assert result["edr"]["label"] == "A"
    assert result["calib"]["label"] == "D"


def test_parse_selection_rules_empty(tmp_path):
    """Test parsing an empty file_information dict."""
    p = tmp_path / "selection_rules.py"
    p.write_text('file_information = {}\n')
    result = parse_selection_rules(p)
    assert result == {}


def test_parse_selection_rules_syntax_error(tmp_path):
    """Test that syntax errors are handled gracefully."""
    p = tmp_path / "selection_rules.py"
    p.write_text('this is not valid python {{{')
    result = parse_selection_rules(p)
    assert result == {}


def test_parse_test_csv(tmp_csv_file):
    """Test CSV parsing."""
    rows = parse_test_csv(tmp_csv_file)
    assert len(rows) == 1
    assert rows[0]["label_file"] == "N1234.LBL"
    assert rows[0]["product_id"] == "1_N1234"
    assert rows[0]["url_stem"] == "http://example.com/data/123"


def test_find_test_csvs(tmp_path):
    """Test matching CSV files to product keys."""
    # Create CSV files
    (tmp_path / "edr_test.csv").write_text("label_file,files\n")
    (tmp_path / "calib_test.csv").write_text("label_file,files\n")

    matches = find_test_csvs(tmp_path, ["edr", "calib", "rdr"])
    assert "edr" in matches
    assert "calib" in matches
    assert "rdr" not in matches
