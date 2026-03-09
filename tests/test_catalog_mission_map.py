"""Tests for the catalog mission mapping."""

import pytest

from planetarypy.catalog._mission_map import (
    resolve_mission_instrument,
    split_product_key,
    MANUAL_MISSION_MAP,
    MULTI_INSTRUMENT_SPLIT,
)


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


def test_no_multi_in_manual_map():
    """No entry in MANUAL_MISSION_MAP should have 'multi' as instrument."""
    for folder_name, (mission, instrument) in MANUAL_MISSION_MAP.items():
        assert instrument != "multi", f"{folder_name} still mapped to 'multi'"


def test_split_folders_marked_as_split():
    """All folders in MULTI_INSTRUMENT_SPLIT should map to _split instrument."""
    for folder_name in MULTI_INSTRUMENT_SPLIT:
        _, instrument, _ = resolve_mission_instrument(folder_name)
        assert instrument == "_split", (
            f"{folder_name} should map to '_split', got '{instrument}'"
        )


# --- split_product_key tests ---


class TestSplitProductKey:
    def test_mro_ctx(self):
        instr, key = split_product_key("mro", "ctx_edr")
        assert instr == "ctx"
        assert key == "edr"

    def test_mro_hirise_multi_word(self):
        instr, key = split_product_key("mro", "hirise_rdr_roi")
        assert instr == "hirise"
        assert key == "rdr_roi"

    def test_phoenix_ssi(self):
        instr, key = split_product_key("phoenix", "ssi_edr")
        assert instr == "ssi"
        assert key == "edr"

    def test_dawn_fc(self):
        instr, key = split_product_key("dawn", "fc_edr_img")
        assert instr == "fc"
        assert key == "edr_img"

    def test_unmatched_goes_to_misc(self):
        instr, key = split_product_key("mro", "extras_support_np")
        assert instr == "_misc"
        assert key == "extras_support_np"

    def test_exact_match_instrument(self):
        """Product key that exactly matches a prefix (e.g. 'gas' for ulysses)."""
        instr, key = split_product_key("ulysses", "gas")
        assert instr == "gas"
        assert key == "gas"

    def test_empty_prefix_map(self):
        """Folders with empty split map: each key becomes its own instrument."""
        instr, key = split_product_key("hst", "mars_cube")
        assert instr == "mars_cube"
        assert key == "mars_cube"

    def test_non_split_folder(self):
        """Non-split folders return key as both instrument and key."""
        instr, key = split_product_key("cassini_iss", "edr_sat")
        assert instr == "edr_sat"
        assert key == "edr_sat"

    def test_longest_prefix_wins(self):
        """Longer prefixes should match before shorter ones."""
        instr, key = split_product_key("apollo", "A15_16_SMSS_extras")
        assert instr == "smss"
        assert key == "extras"

    def test_chandrayaan_m3(self):
        instr, key = split_product_key("ch1", "M3_L0")
        assert instr == "m3"
        assert key == "L0"
