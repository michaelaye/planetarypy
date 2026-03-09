"""Tests for catalog URL pattern-based resolution."""

import pytest

from planetarypy.catalog._url_patterns import (
    _build_file_list,
    _derive_label_file,
    _detect_label_rule,
    _get_url_pattern,
    has_pattern,
    resolve_by_pattern,
)


class TestDetectLabelRule:
    def test_exact_lbl(self):
        rows = [("url", "PRODUCT_001", "PRODUCT_001.LBL")]
        assert _detect_label_rule(rows) == "LBL"

    def test_exact_lbl_lower(self):
        rows = [("url", "product_001", "product_001.lbl")]
        assert _detect_label_rule(rows) == "lbl"

    def test_exact_xml(self):
        rows = [("url", "my_product", "my_product.xml")]
        assert _detect_label_rule(rows) == "xml"

    def test_case_insensitive(self):
        rows = [("url", "PRODUCT_001", "product_001.LBL")]
        assert _detect_label_rule(rows) == "ci_LBL"

    def test_urn_xml(self):
        rows = [(
            "url",
            "urn:esa:psa:bc_mpo_mag:data:my_product_20200412",
            "my_product_20200412.xml",
        )]
        assert _detect_label_rule(rows) == "urn_xml"

    def test_strip_dash(self):
        rows = [("url", "CIRS-HSK06072016", "HSK06072016.LBL")]
        assert _detect_label_rule(rows) == "strip_dash.LBL"

    def test_default_fallback(self):
        rows = [("url", "WEIRD_ID_123", "completely_different.LBL")]
        assert _detect_label_rule(rows) == "LBL"

    def test_skips_none_label(self):
        rows = [("url", "PRODUCT", None), ("url", "OTHER", "OTHER.LBL")]
        assert _detect_label_rule(rows) == "LBL"


class TestDeriveLabelFile:
    def test_standard_lbl(self):
        assert _derive_label_file("PROD_001", "LBL") == "PROD_001.LBL"

    def test_lowercase_lbl(self):
        assert _derive_label_file("prod_001", "lbl") == "prod_001.lbl"

    def test_xml(self):
        assert _derive_label_file("my_product", "xml") == "my_product.xml"

    def test_case_insensitive(self):
        assert _derive_label_file("PROD_001", "ci_LBL") == "PROD_001.LBL"

    def test_urn_xml(self):
        result = _derive_label_file(
            "urn:esa:psa:bc_mpo_mag:data:my_product_20200412",
            "urn_xml",
        )
        assert result == "my_product_20200412.xml"

    def test_strip_dash(self):
        result = _derive_label_file("CIRS-FRV16123004", "strip_dash.LBL")
        assert result == "FRV16123004.LBL"

    def test_strip_dash_no_dash(self):
        result = _derive_label_file("NODASH", "strip_dash.LBL")
        assert result == "NODASH.LBL"


class TestBuildFileList:
    def test_lbl_adds_img(self):
        files = _build_file_list("PRODUCT.LBL")
        assert "PRODUCT.LBL" in files
        assert "PRODUCT.IMG" in files

    def test_xml_adds_dat(self):
        files = _build_file_list("product.xml")
        assert "product.xml" in files
        assert "product.dat" in files

    def test_unknown_ext(self):
        files = _build_file_list("product.TAB")
        assert files == ["product.TAB"]

    def test_lowercase_lbl(self):
        files = _build_file_list("product.lbl")
        assert "product.IMG" in files


class TestResolveByPattern:
    def test_unknown_type_returns_none(self):
        result = resolve_by_pattern("nonexistent", "inst", "edr", "PROD")
        assert result is None

    def test_with_mocked_pattern(self, monkeypatch):
        monkeypatch.setattr(
            "planetarypy.catalog._url_patterns._get_url_pattern",
            lambda m, i, pk: ("https://example.com/data", "LBL"),
        )
        result = resolve_by_pattern("test", "inst", "edr", "MY_PRODUCT_001")
        assert result is not None
        assert result.product_id == "MY_PRODUCT_001"
        assert result.url_stem == "https://example.com/data"
        assert result.label_file == "MY_PRODUCT_001.LBL"
        assert result.source == "pattern"
        assert "MY_PRODUCT_001.LBL" in result.files
        assert "MY_PRODUCT_001.IMG" in result.files

    def test_with_urn_pattern(self, monkeypatch):
        monkeypatch.setattr(
            "planetarypy.catalog._url_patterns._get_url_pattern",
            lambda m, i, pk: ("https://psa.esa.int/data", "urn_xml"),
        )
        result = resolve_by_pattern(
            "test", "inst", "raw",
            "urn:esa:psa:instrument:data:my_obs_20200101",
        )
        assert result is not None
        assert result.label_file == "my_obs_20200101.xml"

    def test_pattern_none_returns_none(self, monkeypatch):
        monkeypatch.setattr(
            "planetarypy.catalog._url_patterns._get_url_pattern",
            lambda m, i, pk: None,
        )
        result = resolve_by_pattern("test", "inst", "edr", "PROD")
        assert result is None


class TestHasPattern:
    def test_unknown_returns_false(self):
        assert has_pattern("nonexistent", "inst", "edr") is False

    def test_with_mocked_pattern(self, monkeypatch):
        monkeypatch.setattr(
            "planetarypy.catalog._url_patterns._get_url_pattern",
            lambda m, i, pk: ("https://example.com", "LBL"),
        )
        assert has_pattern("test", "inst", "edr") is True


class TestGetUrlPattern:
    def test_returns_none_for_unknown(self):
        # Clear LRU cache before test
        _get_url_pattern.cache_clear()
        result = _get_url_pattern("nonexistent_xyz", "inst", "edr")
        assert result is None

    def test_fixed_stem_from_db(self, monkeypatch):
        """Mock the DB query to return samples with fixed url_stem."""
        _get_url_pattern.cache_clear()

        class MockCon:
            def execute(self, query, params):
                return self

            def fetchall(self):
                return [
                    ("https://example.com/data", "PROD_A", "PROD_A.LBL"),
                    ("https://example.com/data", "PROD_B", "PROD_B.LBL"),
                ]

            def close(self):
                pass

        monkeypatch.setattr(
            "planetarypy.catalog.get_catalog",
            lambda: MockCon(),
        )

        result = _get_url_pattern("test_m", "test_i", "edr")
        assert result is not None
        assert result[0] == "https://example.com/data"
        assert result[1] == "LBL"

    def test_variable_stem_returns_none(self, monkeypatch):
        """Variable url_stems should return None (needs index)."""
        _get_url_pattern.cache_clear()

        class MockCon:
            def execute(self, query, params):
                return self

            def fetchall(self):
                return [
                    ("https://example.com/vol1/data", "PROD_A", "PROD_A.LBL"),
                    ("https://example.com/vol2/data", "PROD_B", "PROD_B.LBL"),
                ]

            def close(self):
                pass

        monkeypatch.setattr(
            "planetarypy.catalog.get_catalog",
            lambda: MockCon(),
        )

        result = _get_url_pattern("test_var", "test_i", "edr")
        assert result is None
