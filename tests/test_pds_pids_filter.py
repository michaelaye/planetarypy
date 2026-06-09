"""Tests for the PID-filter additions to ``planetarypy.pds``.

Covers ``pid_column`` resolution, ``get_index(pids=...)`` filtering, and
``missing_pids`` reporting. The ``Index`` class is stubbed so the tests
never touch disk or the network.
"""
from __future__ import annotations

import pandas as pd
import pytest

from planetarypy.pds import (
    get_index,
    missing_pids,
    pid_column,
    resolve_pids,
)


def _df_with_product_id() -> pd.DataFrame:
    return pd.DataFrame({
        "PRODUCT_ID": ["P_001", "P_002", "P_003", "P_004", "P_005"],
        "FILE_NAME": [f"file_{i}.IMG" for i in range(1, 6)],
        "START_TIME": [f"2024-{m:02d}-01T00:00:00" for m in range(1, 6)],
    })


def _df_with_obsid_products() -> pd.DataFrame:
    """Mirrors a per-CCD index: one obsid fans out into many PRODUCT_IDs."""
    return pd.DataFrame({
        "PRODUCT_ID": [
            "OBS_A_RED0", "OBS_A_RED1", "OBS_A_BG0",
            "OBS_B_RED0", "SOLO",
        ],
        "FILE_NAME": [f"f_{i}.IMG" for i in range(5)],
    })


def _df_with_file_name_only() -> pd.DataFrame:
    """Mirrors cassini.uvis.index, which lacks PRODUCT_ID."""
    return pd.DataFrame({
        "FILE_NAME": ["EUV1999_007_17_05.LBL", "EUV1999_008_03_22.LBL"],
        "SCLK_TIME": [1.0, 2.0],
    })


class _StubIndex:
    """Drop-in replacement for ``Index`` that exposes a canned DataFrame."""
    def __init__(self, df: pd.DataFrame):
        self._df = df

    def make(self):
        # Factory that ignores constructor args from get_index.
        outer = self
        class _I:
            def __init__(self, *a, **kw): pass
            def ensure_parquet(self, force=False): return False
            @property
            def update_available(self): return False
            @property
            def dataframe(self): return outer._df
        return _I


# ── pid_column ──────────────────────────────────────────────────────────


class TestPidColumn:

    def test_prefers_product_id_when_present(self):
        df = _df_with_product_id()
        assert pid_column("mro.ctx.edr", df) == "PRODUCT_ID"

    def test_falls_back_to_file_name_when_product_id_absent(self):
        df = _df_with_file_name_only()
        # cassini.uvis.index is registered with product_id_col=FILE_NAME
        assert pid_column("cassini.uvis.index", df) == "FILE_NAME"

    def test_falls_back_through_default_order_for_unknown_key(self):
        df = pd.DataFrame({
            "IMAGE_ID": ["IMG_1", "IMG_2"],
            "OTHER": [1, 2],
        })
        # PRODUCT_ID and FILE_NAME not present; IMAGE_ID wins next.
        assert pid_column("not.registered.key", df) == "IMAGE_ID"

    def test_raises_keyerror_when_no_candidate_present(self):
        df = pd.DataFrame({"X": [1], "Y": [2]})
        with pytest.raises(KeyError, match="No suitable product-id column"):
            pid_column("not.registered.key", df)


# ── get_index(pids=...) ─────────────────────────────────────────────────


class TestGetIndexPidsFilter:

    def test_filter_returns_only_matching_rows(self, monkeypatch):
        df = _df_with_product_id()
        monkeypatch.setattr("planetarypy.pds.Index",
                            _StubIndex(df).make())
        out = get_index("mro.ctx.edr", pids=["P_002", "P_004"])
        assert list(out["PRODUCT_ID"]) == ["P_002", "P_004"]

    def test_filter_unknown_pids_silently_dropped(self, monkeypatch):
        df = _df_with_product_id()
        monkeypatch.setattr("planetarypy.pds.Index",
                            _StubIndex(df).make())
        out = get_index("mro.ctx.edr", pids=["P_002", "DOES_NOT_EXIST"])
        assert list(out["PRODUCT_ID"]) == ["P_002"]

    def test_filter_empty_list_returns_empty_dataframe(self, monkeypatch):
        df = _df_with_product_id()
        monkeypatch.setattr("planetarypy.pds.Index",
                            _StubIndex(df).make())
        out = get_index("mro.ctx.edr", pids=[])
        assert len(out) == 0
        assert list(out.columns) == list(df.columns)

    def test_pids_none_returns_full_index(self, monkeypatch):
        df = _df_with_product_id()
        monkeypatch.setattr("planetarypy.pds.Index",
                            _StubIndex(df).make())
        out = get_index("mro.ctx.edr", pids=None)
        assert len(out) == len(df)

    def test_pids_accepts_any_iterable(self, monkeypatch):
        df = _df_with_product_id()
        monkeypatch.setattr("planetarypy.pds.Index",
                            _StubIndex(df).make())
        # Generator, tuple, set — all should work via map(str, ...).
        out_gen = get_index("mro.ctx.edr", pids=(p for p in ["P_001", "P_003"]))
        out_tup = get_index("mro.ctx.edr", pids=("P_001", "P_003"))
        out_set = get_index("mro.ctx.edr", pids={"P_001", "P_003"})
        assert set(out_gen["PRODUCT_ID"]) == {"P_001", "P_003"}
        assert set(out_tup["PRODUCT_ID"]) == {"P_001", "P_003"}
        assert set(out_set["PRODUCT_ID"]) == {"P_001", "P_003"}


# ── missing_pids ────────────────────────────────────────────────────────


class TestGetIndexColumnsFilter:

    def test_columns_projects_in_order(self, monkeypatch):
        df = _df_with_product_id()
        monkeypatch.setattr("planetarypy.pds.Index",
                            _StubIndex(df).make())
        out = get_index("mro.ctx.edr",
                        columns=["START_TIME", "PRODUCT_ID"])
        assert list(out.columns) == ["START_TIME", "PRODUCT_ID"]
        assert len(out) == len(df)

    def test_columns_with_pids_filter_compose(self, monkeypatch):
        df = _df_with_product_id()
        monkeypatch.setattr("planetarypy.pds.Index",
                            _StubIndex(df).make())
        out = get_index("mro.ctx.edr",
                        pids=["P_002", "P_004"],
                        columns=["PRODUCT_ID", "FILE_NAME"])
        assert list(out.columns) == ["PRODUCT_ID", "FILE_NAME"]
        assert list(out["PRODUCT_ID"]) == ["P_002", "P_004"]

    def test_unknown_column_raises_keyerror_with_available_listed(self, monkeypatch):
        df = _df_with_product_id()
        monkeypatch.setattr("planetarypy.pds.Index",
                            _StubIndex(df).make())
        with pytest.raises(KeyError) as exc:
            get_index("mro.ctx.edr", columns=["BOGUS", "PRODUCT_ID"])
        msg = str(exc.value)
        assert "BOGUS" in msg
        assert "PRODUCT_ID" in msg  # listed under available

    def test_columns_none_keeps_every_column(self, monkeypatch):
        df = _df_with_product_id()
        monkeypatch.setattr("planetarypy.pds.Index",
                            _StubIndex(df).make())
        out = get_index("mro.ctx.edr", columns=None)
        assert list(out.columns) == list(df.columns)


class TestMissingPids:

    def test_returns_pids_absent_from_dataframe(self):
        df = _df_with_product_id()
        out = missing_pids(df, "mro.ctx.edr",
                           ["P_002", "GHOST_A", "P_004", "GHOST_B"])
        assert out == ["GHOST_A", "GHOST_B"]

    def test_preserves_input_order(self):
        df = _df_with_product_id()
        out = missing_pids(df, "mro.ctx.edr", ["Z", "A", "M"])
        assert out == ["Z", "A", "M"]

    def test_does_not_dedup(self):
        df = _df_with_product_id()
        out = missing_pids(df, "mro.ctx.edr", ["X", "X", "Y", "Y"])
        assert out == ["X", "X", "Y", "Y"]

    def test_empty_pids_returns_empty_list(self):
        df = _df_with_product_id()
        assert missing_pids(df, "mro.ctx.edr", []) == []

    def test_all_present_returns_empty_list(self):
        df = _df_with_product_id()
        out = missing_pids(df, "mro.ctx.edr", ["P_001", "P_003"])
        assert out == []


# ── resolve_pids ─────────────────────────────────────────────────────────


class TestResolvePids:

    def test_exact_match_maps_to_itself(self):
        df = _df_with_obsid_products()
        out = resolve_pids("mro.hirise.edr", ["SOLO"], df, prefix=True)
        assert out == {"SOLO": ["SOLO"]}

    def test_prefix_expands_to_all_sorted(self):
        df = _df_with_obsid_products()
        out = resolve_pids("mro.hirise.edr", ["OBS_A"], df, prefix=True)
        assert out == {"OBS_A": ["OBS_A_BG0", "OBS_A_RED0", "OBS_A_RED1"]}

    def test_prefix_false_does_not_expand(self):
        df = _df_with_obsid_products()
        out = resolve_pids("mro.hirise.edr", ["OBS_A"], df, prefix=False)
        assert out == {"OBS_A": []}

    def test_exact_wins_over_prefix(self):
        """A value that is itself complete short-circuits expansion."""
        df = pd.DataFrame({"PRODUCT_ID": ["OBS_A", "OBS_A_RED0"]})
        out = resolve_pids("mro.hirise.edr", ["OBS_A"], df, prefix=True)
        assert out == {"OBS_A": ["OBS_A"]}

    def test_unmatched_pid_maps_to_empty(self):
        df = _df_with_obsid_products()
        out = resolve_pids("mro.hirise.edr", ["GHOST"], df, prefix=True)
        assert out == {"GHOST": []}

    def test_mixed_inputs_preserve_order(self):
        df = _df_with_obsid_products()
        out = resolve_pids(
            "mro.hirise.edr", ["SOLO", "OBS_B", "GHOST"], df, prefix=True,
        )
        assert list(out.keys()) == ["SOLO", "OBS_B", "GHOST"]
        assert out["OBS_B"] == ["OBS_B_RED0"]


# ── get_index(prefix=...) ────────────────────────────────────────────────


class TestGetIndexPrefixFilter:

    def test_prefix_true_expands_short_pid(self, monkeypatch):
        df = _df_with_obsid_products()
        monkeypatch.setattr("planetarypy.pds.Index",
                            _StubIndex(df).make())
        out = get_index("mro.hirise.edr", pids=["OBS_A"], prefix=True)
        assert set(out["PRODUCT_ID"]) == {
            "OBS_A_RED0", "OBS_A_RED1", "OBS_A_BG0",
        }

    def test_prefix_false_is_exact_only(self, monkeypatch):
        df = _df_with_obsid_products()
        monkeypatch.setattr("planetarypy.pds.Index",
                            _StubIndex(df).make())
        out = get_index("mro.hirise.edr", pids=["OBS_A"], prefix=False)
        assert len(out) == 0

    def test_prefix_mixes_exact_and_expanded(self, monkeypatch):
        df = _df_with_obsid_products()
        monkeypatch.setattr("planetarypy.pds.Index",
                            _StubIndex(df).make())
        out = get_index(
            "mro.hirise.edr", pids=["SOLO", "OBS_B"], prefix=True,
        )
        assert set(out["PRODUCT_ID"]) == {"SOLO", "OBS_B_RED0"}
