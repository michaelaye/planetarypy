"""Tests for the catalog index bridge module."""

import pytest
import pandas as pd

from planetarypy.catalog._index_bridge import (
    IndexConfig,
    INDEX_REGISTRY,
    get_index_config,
    has_index,
    list_indexed_products,
    resolve_from_index,
    _find_product_in_index,
    _build_url_stem,
    _extract_files,
)


class TestIndexConfig:
    def test_default_columns(self):
        cfg = IndexConfig(index_key="mro.ctx.edr")
        assert cfg.product_id_col == "PRODUCT_ID"
        assert cfg.file_spec_col == "FILE_SPECIFICATION_NAME"
        assert cfg.volume_id_col == "VOLUME_ID"

    def test_frozen(self):
        cfg = IndexConfig(index_key="mro.ctx.edr")
        with pytest.raises(AttributeError):
            cfg.index_key = "other"


class TestRegistry:
    def test_ctx_edr_registered(self):
        assert ("mro", "ctx", "edr") in INDEX_REGISTRY

    def test_hirise_edr_registered(self):
        assert ("mro", "hirise", "edr") in INDEX_REGISTRY

    def test_hirise_rdr_registered(self):
        assert ("mro", "hirise", "rdr") in INDEX_REGISTRY

    def test_cassini_iss_edr_sat_registered(self):
        assert ("cassini", "iss", "edr_sat") in INDEX_REGISTRY

    def test_galileo_ssi_uses_go_index(self):
        cfg = INDEX_REGISTRY[("galileo", "ssi", "edr")]
        assert cfg.index_key == "go.ssi.index"

    def test_all_configs_have_index_key(self):
        for key, cfg in INDEX_REGISTRY.items():
            assert cfg.index_key, f"{key} missing index_key"

    def test_seti_or_archive_url_set(self):
        for key, cfg in INDEX_REGISTRY.items():
            assert cfg.archive_url or cfg.seti_volume_group, (
                f"{key} needs either archive_url or seti_volume_group"
            )

    def test_mgs_moc_edr_registered(self):
        assert ("mgs", "moc", "edr") in INDEX_REGISTRY

    def test_mgs_moc_rdr_registered(self):
        assert ("mgs", "moc", "rdr") in INDEX_REGISTRY

    def test_viking_vis_edr_registered(self):
        assert ("viking", "vis", "edr") in INDEX_REGISTRY

    def test_messenger_mdis_edr_registered(self):
        assert ("messenger", "mdis", "edr") in INDEX_REGISTRY

    def test_messenger_mdis_cdr_registered(self):
        assert ("messenger", "mdis", "cdr") in INDEX_REGISTRY

    def test_no_instrument_level_keys(self):
        """Registry must use (mission, instrument, product_key) triples."""
        for key in INDEX_REGISTRY:
            assert len(key) == 3, f"Key {key} is not a triple"


class TestGetIndexConfig:
    def test_basic_lookup(self):
        cfg = get_index_config("mro", "ctx", "edr")
        assert cfg is not None
        assert cfg.index_key == "mro.ctx.edr"

    def test_hirise_rdr(self):
        cfg = get_index_config("mro", "hirise", "rdr")
        assert cfg is not None
        assert cfg.index_key == "mro.hirise.rdr"

    def test_hirise_edr(self):
        cfg = get_index_config("mro", "hirise", "edr")
        assert cfg is not None
        assert cfg.index_key == "mro.hirise.edr"

    def test_unknown_product_type(self):
        cfg = get_index_config("mro", "ctx", "nonexistent")
        assert cfg is None

    def test_unknown_instrument(self):
        cfg = get_index_config("unknown", "instrument", "edr")
        assert cfg is None


class TestHasIndex:
    def test_known(self):
        assert has_index("mro", "ctx", "edr") is True

    def test_unknown_product_key(self):
        assert has_index("mro", "ctx", "nonexistent") is False

    def test_unknown_instrument(self):
        assert has_index("unknown", "instrument", "edr") is False


class TestListIndexedProducts:
    def test_returns_tuples(self):
        result = list_indexed_products()
        assert len(result) > 0
        assert all(len(t) == 4 for t in result)

    def test_includes_ctx_edr(self):
        result = list_indexed_products()
        assert ("mro", "ctx", "edr", "mro.ctx.edr") in result

    def test_includes_hirise_rdr(self):
        result = list_indexed_products()
        assert ("mro", "hirise", "rdr", "mro.hirise.rdr") in result


class TestFindProductInIndex:
    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame({
            "PRODUCT_ID": [
                "B01_009942_1894_XI_09N202W",
                "P01_001503_1853_XI_05N213W",
                "D14_032794_1989_XN_18N282W",
            ],
            "VOLUME_ID": ["MROX_0042", "MROX_0001", "MROX_2601"],
            "FILE_SPECIFICATION_NAME": [
                "DATA/B01_009942_1894_XI_09N202W.IMG",
                "DATA/P01_001503_1853_XI_05N213W.IMG",
                "DATA/D14_032794_1989_XN_18N282W.IMG",
            ],
        })

    def test_exact_match(self, sample_df):
        cfg = IndexConfig(index_key="mro.ctx.edr")
        row = _find_product_in_index(
            sample_df, "B01_009942_1894_XI_09N202W", cfg
        )
        assert row is not None
        assert row["VOLUME_ID"] == "MROX_0042"

    def test_case_insensitive(self, sample_df):
        cfg = IndexConfig(index_key="mro.ctx.edr")
        row = _find_product_in_index(
            sample_df, "b01_009942_1894_xi_09n202w", cfg
        )
        assert row is not None

    def test_not_found(self, sample_df):
        cfg = IndexConfig(index_key="mro.ctx.edr")
        row = _find_product_in_index(sample_df, "NONEXISTENT", cfg)
        assert row is None

    def test_whitespace_padded(self):
        df = pd.DataFrame({
            "PRODUCT_ID": ["  B01_009942  ", "  P01_001503  "],
            "VOLUME_ID": ["MROX_0042", "MROX_0001"],
        })
        cfg = IndexConfig(index_key="mro.ctx.edr")
        row = _find_product_in_index(df, "B01_009942", cfg)
        assert row is not None

    def test_fallback_column(self):
        df = pd.DataFrame({
            "FILE_NAME": ["N1455098340_1.LBL", "N1455098341_1.LBL"],
            "VOLUME_ID": ["COISS_2001", "COISS_2001"],
        })
        cfg = IndexConfig(
            index_key="cassini.iss.index",
            product_id_col="FILE_NAME",
        )
        row = _find_product_in_index(df, "N1455098340_1.LBL", cfg)
        assert row is not None


class TestBuildUrlStem:
    def test_jpl_archive(self):
        row = pd.Series({
            "VOLUME_ID": "MROX_0042",
            "FILE_SPECIFICATION_NAME": "DATA/B01_009942_1894_XI_09N202W.IMG",
        })
        cfg = IndexConfig(
            index_key="mro.ctx.edr",
            archive_url="https://planetarydata.jpl.nasa.gov/img/data/mro/ctx",
        )
        url = _build_url_stem(row, cfg)
        assert url == (
            "https://planetarydata.jpl.nasa.gov/img/data/mro/ctx/"
            "MROX_0042/DATA"
        )

    def test_seti_archive(self):
        row = pd.Series({
            "VOLUME_ID": "COISS_2001",
            "FILE_SPECIFICATION_NAME": "data/N1455098340_1.LBL",
        })
        cfg = IndexConfig(
            index_key="cassini.iss.index",
            seti_volume_group="COISS_2xxx",
        )
        url = _build_url_stem(row, cfg)
        assert url == (
            "https://pds-rings.seti.org/holdings/volumes/"
            "COISS_2xxx/COISS_2001/data"
        )

    def test_no_subdirectory(self):
        row = pd.Series({
            "VOLUME_ID": "MROX_0042",
            "FILE_SPECIFICATION_NAME": "product.IMG",
        })
        cfg = IndexConfig(
            index_key="mro.ctx.edr",
            archive_url="https://example.com/data",
        )
        url = _build_url_stem(row, cfg)
        assert url == "https://example.com/data/MROX_0042"

    def test_trailing_slash_stripped(self):
        row = pd.Series({
            "VOLUME_ID": "VOL_001",
            "FILE_SPECIFICATION_NAME": "DATA/file.IMG",
        })
        cfg = IndexConfig(
            index_key="test.index",
            archive_url="https://example.com/data/",
        )
        url = _build_url_stem(row, cfg)
        assert "data//VOL_001" not in url


class TestExtractFiles:
    def test_img_file_adds_label(self):
        row = pd.Series({
            "FILE_SPECIFICATION_NAME": "DATA/B01_009942.IMG",
        })
        cfg = IndexConfig(index_key="mro.ctx.edr")
        files, label = _extract_files(row, cfg)
        assert "B01_009942.IMG" in files
        assert "B01_009942.LBL" in files
        assert label == "B01_009942.LBL"

    def test_lbl_file_adds_data(self):
        row = pd.Series({
            "FILE_SPECIFICATION_NAME": "data/N1455098340_1.LBL",
        })
        cfg = IndexConfig(index_key="cassini.iss.index")
        files, label = _extract_files(row, cfg)
        assert "N1455098340_1.LBL" in files
        assert "N1455098340_1.IMG" in files
        assert label == "N1455098340_1.LBL"

    def test_empty_spec(self):
        row = pd.Series({"FILE_SPECIFICATION_NAME": ""})
        cfg = IndexConfig(index_key="test")
        files, label = _extract_files(row, cfg)
        assert files == []
        assert label is None


class TestResolveFromIndex:
    def test_no_index_returns_none(self):
        result = resolve_from_index("unknown", "inst", "edr", "PROD_001")
        assert result is None

    def test_with_mocked_index(self, monkeypatch):
        mock_df = pd.DataFrame({
            "PRODUCT_ID": ["TEST_PRODUCT_001"],
            "VOLUME_ID": ["VOL_001"],
            "FILE_SPECIFICATION_NAME": ["DATA/TEST_PRODUCT_001.IMG"],
        })
        monkeypatch.setattr(
            "planetarypy.catalog._index_bridge._load_index_df",
            lambda config: mock_df,
        )
        # Register a test entry
        from planetarypy.catalog._index_bridge import INDEX_REGISTRY
        INDEX_REGISTRY[("test_mission", "test_instr", "edr")] = IndexConfig(
            index_key="test.instr.edr",
            archive_url="https://example.com/data",
        )
        try:
            result = resolve_from_index(
                "test_mission", "test_instr", "edr", "TEST_PRODUCT_001"
            )
            assert result is not None
            assert result.product_id == "TEST_PRODUCT_001"
            assert result.source == "index"
            assert "VOL_001" in result.url_stem
            assert "TEST_PRODUCT_001.IMG" in result.files
        finally:
            del INDEX_REGISTRY[("test_mission", "test_instr", "edr")]

    def test_product_not_in_index(self, monkeypatch):
        mock_df = pd.DataFrame({
            "PRODUCT_ID": ["OTHER_PRODUCT"],
            "VOLUME_ID": ["VOL_001"],
            "FILE_SPECIFICATION_NAME": ["DATA/OTHER.IMG"],
        })
        monkeypatch.setattr(
            "planetarypy.catalog._index_bridge._load_index_df",
            lambda config: mock_df,
        )
        from planetarypy.catalog._index_bridge import INDEX_REGISTRY
        INDEX_REGISTRY[("test_mission", "test_instr", "edr")] = IndexConfig(
            index_key="test.instr.edr",
            archive_url="https://example.com/data",
        )
        try:
            result = resolve_from_index(
                "test_mission", "test_instr", "edr", "NONEXISTENT"
            )
            assert result is None
        finally:
            del INDEX_REGISTRY[("test_mission", "test_instr", "edr")]


class TestExtraIndexKeys:
    def test_diviner_edr_has_extra_keys(self):
        cfg = INDEX_REGISTRY[("lro", "diviner", "edr")]
        assert cfg.extra_index_keys == ("lro.diviner.edr2",)

    def test_diviner_rdr_has_extra_keys(self):
        cfg = INDEX_REGISTRY[("lro", "diviner", "rdr")]
        assert cfg.extra_index_keys == ("lro.diviner.rdr2",)

    def test_normal_config_has_no_extra_keys(self):
        cfg = INDEX_REGISTRY[("mro", "ctx", "edr")]
        assert cfg.extra_index_keys == ()

    def test_load_concatenates_multiple_indexes(self, monkeypatch):
        """_load_index_df concatenates primary + extra index DataFrames."""
        from planetarypy.catalog._index_bridge import _load_index_df

        df1 = pd.DataFrame({
            "PRODUCT_ID": ["PROD_A"],
            "VOLUME_ID": ["VOL_001"],
        })
        df2 = pd.DataFrame({
            "PRODUCT_ID": ["PROD_B"],
            "VOLUME_ID": ["VOL_002"],
        })
        call_log = []

        def mock_get_index(key):
            call_log.append(key)
            return {"test.edr1": df1, "test.edr2": df2}[key]

        monkeypatch.setattr(
            "planetarypy.pds.get_index", mock_get_index
        )
        cfg = IndexConfig(
            index_key="test.edr1",
            extra_index_keys=("test.edr2",),
        )
        result = _load_index_df(cfg)
        assert len(result) == 2
        assert set(result["PRODUCT_ID"]) == {"PROD_A", "PROD_B"}
        assert call_log == ["test.edr1", "test.edr2"]
