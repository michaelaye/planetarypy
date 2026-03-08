"""Tests for the catalog mission mapping."""

import pytest

from planetarypy.catalog._mission_map import resolve_mission_instrument, MANUAL_MISSION_MAP


def test_auto_split():
    """Test auto-splitting on first underscore."""
    m, i, status = resolve_mission_instrument("cassini_iss")
    assert m == "cassini"
    assert i == "iss"
    assert status == "auto"


def test_auto_split_multi_underscore():
    """Test auto-splitting preserves everything after first underscore."""
    m, i, status = resolve_mission_instrument("lro_mini_rf")
    assert m == "lro"
    assert i == "mini_rf"
    assert status == "auto"


def test_manual_map_diviner():
    """Test that diviner maps to lro."""
    m, i, status = resolve_mission_instrument("diviner")
    assert m == "lro"
    assert i == "diviner"
    assert status == "manual"


def test_manual_map_crism():
    """Test that crism maps to mro."""
    m, i, status = resolve_mission_instrument("crism")
    assert m == "mro"
    assert i == "crism"
    assert status == "manual"


def test_manual_map_galileo():
    """Test that gal_ssi maps to galileo."""
    m, i, status = resolve_mission_instrument("gal_ssi")
    assert m == "galileo"
    assert i == "ssi"
    assert status == "manual"


def test_manual_map_new_horizons():
    """Test that nh_ prefix maps to new_horizons."""
    m, i, status = resolve_mission_instrument("nh_lorri")
    assert m == "new_horizons"
    assert i == "lorri"
    assert status == "manual"


def test_manual_map_voyager():
    """Test that vg_ prefix maps to voyager."""
    m, i, status = resolve_mission_instrument("vg_iss")
    assert m == "voyager"
    assert i == "iss"
    assert status == "manual"


def test_manual_map_magellan():
    """Test that mgn_ prefix maps to magellan."""
    m, i, status = resolve_mission_instrument("mgn_image")
    assert m == "magellan"
    assert i == "image"
    assert status == "manual"


def test_manual_map_venus_express():
    """Test that vex_ prefix maps to venus_express."""
    m, i, status = resolve_mission_instrument("vex_virtis")
    assert m == "venus_express"
    assert i == "virtis"
    assert status == "manual"


def test_manual_map_lroc():
    """Test that lroc maps to lro."""
    m, i, status = resolve_mission_instrument("lroc")
    assert m == "lro"
    assert i == "lroc"
    assert status == "manual"


def test_manual_map_covers_all_known():
    """Verify all entries in MANUAL_MISSION_MAP return 'manual' status."""
    for folder_name in MANUAL_MISSION_MAP:
        _, _, status = resolve_mission_instrument(folder_name)
        assert status == "manual", f"{folder_name} should return 'manual' status"
