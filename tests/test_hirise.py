"""Tests for HiRISE instrument module.

Fast tests: product ID parsing, path construction, CCD lists.
Slow tests: actual download and ISIS pipeline (marked @pytest.mark.slow).
"""

import pytest
from pathlib import Path


# ── Fast tests: no network, no ISIS ──────────────────────────────


class TestSourceProduct:
    def test_psp_obsid(self):
        from planetarypy.instruments.mro.hirise import SOURCE_PRODUCT

        prod = SOURCE_PRODUCT("PSP_003092_0985_RED4_0")
        assert prod.obsid == "PSP_003092_0985"
        assert prod.ccd == "RED4"
        assert prod.channel == "0"
        assert prod.color == "RED"
        assert prod.ccdno == "4"
        assert prod.phase == "PSP"
        assert prod.fname == "PSP_003092_0985_RED4_0.IMG"
        assert prod.spid == "PSP_003092_0985_RED4_0"

    def test_esp_obsid(self):
        from planetarypy.instruments.mro.hirise import SOURCE_PRODUCT

        prod = SOURCE_PRODUCT("ESP_050000_1234_RED5_1")
        assert prod.phase == "ESP"
        assert prod.obsid == "ESP_050000_1234"
        assert prod.ccd == "RED5"
        assert prod.channel == "1"

    def test_ir_ccd(self):
        from planetarypy.instruments.mro.hirise import SOURCE_PRODUCT

        prod = SOURCE_PRODUCT("PSP_003092_0985_IR10_0")
        assert prod.color == "IR"
        assert prod.ccdno == "10"
        assert prod.ccd == "IR10"

    def test_bg_ccd(self):
        from planetarypy.instruments.mro.hirise import SOURCE_PRODUCT

        prod = SOURCE_PRODUCT("PSP_003092_0985_BG12_1")
        assert prod.color == "BG"
        assert prod.ccdno == "12"

    def test_url_construction(self):
        from planetarypy.instruments.mro.hirise import SOURCE_PRODUCT

        prod = SOURCE_PRODUCT("PSP_003092_0985_RED4_0")
        url = str(prod.url)
        assert "hirise-pds.lpl.arizona.edu" in url
        assert "/EDR/PSP/" in url
        assert "ORB_003000_003099" in url
        assert "PSP_003092_0985_RED4_0.IMG" in url

    def test_local_path_has_obsid_dir(self):
        from planetarypy.instruments.mro.hirise import SOURCE_PRODUCT

        prod = SOURCE_PRODUCT("PSP_003092_0985_RED4_0")
        assert "PSP_003092_0985" in str(prod.local_path)
        assert prod.local_path.name == "PSP_003092_0985_RED4_0.IMG"

    def test_local_cube_suffix(self):
        from planetarypy.instruments.mro.hirise import SOURCE_PRODUCT

        prod = SOURCE_PRODUCT("PSP_003092_0985_RED4_0")
        assert prod.local_cube.suffix == ".cub"
        assert prod.local_cube.stem == "PSP_003092_0985_RED4_0"

    def test_stitched_cube_path(self):
        from planetarypy.instruments.mro.hirise import SOURCE_PRODUCT

        prod = SOURCE_PRODUCT("PSP_003092_0985_RED4_0")
        assert prod.stitched_cube_path.name == "PSP_003092_0985_RED4.cub"

    def test_saveroot_override(self, tmp_path):
        from planetarypy.instruments.mro.hirise import SOURCE_PRODUCT

        prod = SOURCE_PRODUCT("PSP_003092_0985_RED4_0", saveroot=tmp_path)
        assert str(tmp_path) in str(prod.local_path)

    def test_ccd_class_lists(self):
        from planetarypy.instruments.mro.hirise import SOURCE_PRODUCT

        assert len(SOURCE_PRODUCT.red_ccds) == 10
        assert len(SOURCE_PRODUCT.ir_ccds) == 2
        assert len(SOURCE_PRODUCT.bg_ccds) == 2
        assert len(SOURCE_PRODUCT.ccds) == 14


class TestRedProduct:
    def test_constructor(self):
        from planetarypy.instruments.mro.hirise import RED_PRODUCT

        prod = RED_PRODUCT("PSP_003092_0985", ccdno=4, channel=0)
        assert prod.spid == "PSP_003092_0985_RED4_0"
        assert prod.ccd == "RED4"
        assert prod.channel == "0"

    def test_all_red_ccds(self):
        from planetarypy.instruments.mro.hirise import RED_PRODUCT

        for ccdno in range(10):
            for ch in (0, 1):
                prod = RED_PRODUCT("ESP_013807_2035", ccdno=ccdno, channel=ch)
                assert prod.ccd == f"RED{ccdno}"
                assert prod.channel == str(ch)


class TestEdrProducts:
    def test_red_default(self):
        from planetarypy.instruments.mro.hirise import edr_products

        prods = edr_products("PSP_003092_0985")
        assert len(prods) == 20  # 10 CCDs × 2 channels

    def test_red_subset(self):
        from planetarypy.instruments.mro.hirise import edr_products

        prods = edr_products("PSP_003092_0985", ccds=[4, 5])
        assert len(prods) == 4
        spids = [p.spid for p in prods]
        assert "PSP_003092_0985_RED4_0" in spids
        assert "PSP_003092_0985_RED5_1" in spids

    def test_ir(self):
        from planetarypy.instruments.mro.hirise import edr_products

        prods = edr_products("PSP_003092_0985", colors=["ir"])
        assert len(prods) == 4  # 2 CCDs × 2 channels
        assert all("IR" in p.ccd for p in prods)

    def test_bg(self):
        from planetarypy.instruments.mro.hirise import edr_products

        prods = edr_products("PSP_003092_0985", colors=["bg"])
        assert len(prods) == 4
        assert all("BG" in p.ccd for p in prods)

    def test_all_colors(self):
        from planetarypy.instruments.mro.hirise import edr_products

        prods = edr_products("PSP_003092_0985", colors=["red", "ir", "bg"])
        assert len(prods) == 28  # (10 + 2 + 2) × 2


class TestParsePid:
    def test_rdr_red(self):
        from planetarypy.instruments.mro.hirise import _parse_pid

        pid, parts, level = _parse_pid("PSP_003092_0985_RED")
        assert level == "RDR"
        assert pid == "PSP_003092_0985_RED"

    def test_rdr_color(self):
        from planetarypy.instruments.mro.hirise import _parse_pid

        pid, parts, level = _parse_pid("ESP_013807_2035_COLOR")
        assert level == "RDR"

    def test_edr(self):
        from planetarypy.instruments.mro.hirise import _parse_pid

        pid, parts, level = _parse_pid("PSP_003092_0985_RED4_0")
        assert level == "EDR"

    def test_bare_obsid_defaults_to_rdr_red(self):
        from planetarypy.instruments.mro.hirise import _parse_pid

        pid, parts, level = _parse_pid("PSP_003092_0985")
        assert pid == "PSP_003092_0985_RED"
        assert level == "RDR"


class TestOrbitRange:
    def test_low_orbit(self):
        from planetarypy.instruments.mro.hirise import _orbit_range

        assert _orbit_range(3092) == "ORB_003000_003099"

    def test_high_orbit(self):
        from planetarypy.instruments.mro.hirise import _orbit_range

        assert _orbit_range(50000) == "ORB_050000_050099"

    def test_boundary(self):
        from planetarypy.instruments.mro.hirise import _orbit_range

        assert _orbit_range(11000) == "ORB_011000_011099"


class TestObsidCompletion:
    def test_complete_from_cache(self, tmp_path):
        """Test completion against a manually created cache file."""
        from planetarypy.instruments.mro import hirise

        # Create a small fake cache
        cache = tmp_path / "obsids.txt"
        cache.write_text("ESP_013800_1820\nESP_013801_1210\nPSP_003092_0985\nPSP_003092_1715\n")

        # Monkey-patch the cache path
        orig = hirise._obsid_cache_path
        hirise._obsid_cache_path = lambda index="edr": cache
        try:
            matches = hirise.complete_obsid("PSP_003092")
            assert matches == ["PSP_003092_0985", "PSP_003092_1715"]

            matches = hirise.complete_obsid("ESP_01380")
            assert len(matches) == 2

            matches = hirise.complete_obsid("XYZ")
            assert matches == []
        finally:
            hirise._obsid_cache_path = orig


class TestExtHelper:
    def test_append_suffix(self):
        from planetarypy.instruments.mro.hirise import _ext

        p = Path("/data/obs_RED4.cub")
        assert _ext(p, ".cal") == Path("/data/obs_RED4.cal.cub")

    def test_replace_suffix_chain(self):
        from planetarypy.instruments.mro.hirise import _ext

        p = Path("/data/obs_RED4.cal.cub")
        assert _ext(p, ".cal.norm") == Path("/data/obs_RED4.cal.norm.cub")


# ── Slow tests: network + ISIS required ─────────────────────────


@pytest.mark.slow
class TestDownloadEdr:
    """Test actual EDR download from PDS (network required)."""

    def test_download_single_channel(self, tmp_path):
        from planetarypy.instruments.mro.hirise import SOURCE_PRODUCT

        prod = SOURCE_PRODUCT("PSP_003092_0985_RED4_0", saveroot=tmp_path)
        prod.download()
        assert prod.local_path.exists()
        assert prod.local_path.stat().st_size > 40_000_000  # ~46 MB


@pytest.mark.slow
class TestUrlLiveness:
    """Verify that EDR URLs resolve correctly on the PDS server."""

    def test_psp_red_url_exists(self):
        import requests
        from planetarypy.instruments.mro.hirise import SOURCE_PRODUCT

        prod = SOURCE_PRODUCT("PSP_003092_0985_RED4_0")
        resp = requests.head(str(prod.url), timeout=30)
        assert resp.ok

    def test_esp_red_url_exists(self):
        import requests
        from planetarypy.instruments.mro.hirise import SOURCE_PRODUCT

        prod = SOURCE_PRODUCT("ESP_013807_2035_RED4_0")
        resp = requests.head(str(prod.url), timeout=30)
        assert resp.ok
