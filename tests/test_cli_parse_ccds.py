"""Tests for the ``_parse_ccds`` CLI helper.

The helper backs the dual-idiom ``--ccds`` flag on ``plp hiedr`` /
``plp himos``: repeated flag, comma-separated, or a mix all reduce to
the same ``list[int] | None``.
"""
from __future__ import annotations

import pytest
import typer

from planetarypy.cli import _parse_ccds


class TestParseCcds:

    def test_none_returns_none(self):
        assert _parse_ccds(None) is None

    def test_empty_list_returns_none(self):
        assert _parse_ccds([]) is None

    def test_single_string_comma_separated(self):
        assert _parse_ccds("4,5") == [4, 5]

    def test_repeated_flag_list_of_singles(self):
        assert _parse_ccds(["4", "5"]) == [4, 5]

    def test_mixed_repeated_and_comma(self):
        assert _parse_ccds(["4", "5,6"]) == [4, 5, 6]

    def test_whitespace_tolerated(self):
        assert _parse_ccds(["4 , 5", "6"]) == [4, 5, 6]

    def test_empty_strings_skipped(self):
        assert _parse_ccds(["", "4", ""]) == [4]
        assert _parse_ccds(["4,,5"]) == [4, 5]

    def test_non_integer_raises_badparameter(self):
        with pytest.raises(typer.BadParameter, match="expects integers"):
            _parse_ccds(["4", "abc"])

    def test_back_compat_with_string_only_callers(self):
        """Old call sites that pass a single string still work — that's
        the migration safety net for the pre-list-input world."""
        assert _parse_ccds("4,5,6") == [4, 5, 6]
