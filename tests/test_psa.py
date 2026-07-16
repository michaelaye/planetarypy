"""Tests for ``planetarypy.psa`` (ESA PSA TAP resolver).

Deterministic tests mock the TAP layer; one ``slow`` test hits the live PSA.
"""

import zipfile

import pytest

from planetarypy import psa


# ── query parsing ────────────────────────────────────────────────────


def test_query_parses_metadata_and_data(monkeypatch):
    class _Resp:
        def raise_for_status(self):
            pass

        def json(self):
            return {
                "metadata": [{"name": "a"}, {"name": "b"}],
                "data": [[1, 2], [3, 4]],
            }

    monkeypatch.setattr(psa.requests, "get", lambda *a, **k: _Resp())
    assert psa.query("SELECT a,b FROM t") == [{"a": 1, "b": 2}, {"a": 3, "b": 4}]


# ── resolve / resolve_all ────────────────────────────────────────────


def test_resolve_returns_access_url(monkeypatch):
    captured = {}

    def fake_query(adql, **k):
        captured["adql"] = adql
        return [{"granule_uid": "DS:DATA:PID::1.0", "access_url": "https://x/dl"}]

    monkeypatch.setattr(psa, "query", fake_query)
    assert psa.resolve("PID") == "https://x/dl"
    assert "LIKE '%PID%'" in captured["adql"]
    assert "psa.epn_core" in captured["adql"]


def test_resolve_escapes_single_quote(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        psa, "query", lambda adql, **k: captured.update(adql=adql) or []
    )
    psa.resolve("O'Brien")
    assert "O''Brien" in captured["adql"]  # ADQL-escaped


def test_resolve_none_when_no_match(monkeypatch):
    monkeypatch.setattr(psa, "query", lambda adql, **k: [])
    assert psa.resolve("nope") is None


def test_resolve_all_returns_rows(monkeypatch):
    rows = [
        {"granule_uid": "a", "access_url": "u1"},
        {"granule_uid": "b", "access_url": "u2"},
    ]
    monkeypatch.setattr(psa, "query", lambda adql, **k: rows)
    assert psa.resolve_all("x") == rows


# ── missions / instruments ───────────────────────────────────────────


def test_missions_builds_dataframe(monkeypatch):
    captured = {}

    def fake_query(adql, **k):
        captured["adql"] = adql
        return [{"mission": "Rosetta", "products": 10}, {"mission": "MEX", "products": 5}]

    monkeypatch.setattr(psa, "query", fake_query)
    df = psa.missions()
    assert list(df.columns) == ["mission", "catalog", "products"]
    assert df.iloc[0]["mission"] == "Rosetta"
    # best-effort name bridge: PSA "Rosetta" -> catalog code "rosetta"
    assert df.iloc[0]["catalog"] == "rosetta"
    assert "GROUP BY instrument_host_name" in captured["adql"]


def test_instruments_filtered(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        psa, "query",
        lambda adql, **k: captured.update(adql=adql)
        or [{"mission": "Mars Express", "instrument": "HRSC", "products": 3}],
    )
    df = psa.instruments("Mars Express")
    assert list(df.columns) == ["mission", "instrument", "products"]
    assert "UPPER(instrument_host_name) LIKE UPPER('%Mars Express%')" in captured["adql"]


def test_instruments_unfiltered_has_no_where(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        psa, "query", lambda adql, **k: captured.update(adql=adql) or []
    )
    psa.instruments()
    assert "WHERE" not in captured["adql"]


# ── datasets ──────────────────────────────────────────────────────────


def test_datasets_keeps_full_granule_gid(monkeypatch):
    captured = {}

    def fake_query(adql, **k):
        captured["adql"] = adql
        return [
            # PDS3 data set and a PDS4 collection LID — both kept whole (no split)
            {"granule_gid": "MEX-M-ASPERA3-2-EDR-IMA-EXT4-V1.0:DATA", "products": 59040},
            {"granule_gid": "urn:esa:psa:bc_mpo_berm:data_calibrated", "products": 80276},
        ]

    monkeypatch.setattr(psa, "query", fake_query)
    df = psa.datasets("BepiColombo")
    assert list(df.columns) == ["dataset", "products"]
    assert df.iloc[0]["dataset"] == "MEX-M-ASPERA3-2-EDR-IMA-EXT4-V1.0:DATA"
    # the PDS4 collection LID is preserved (would have become "urn" with split)
    assert df.iloc[1]["dataset"] == "urn:esa:psa:bc_mpo_berm:data_calibrated"
    assert "GROUP BY granule_gid" in captured["adql"]


def test_datasets_no_instrument(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        psa, "query", lambda adql, **k: captured.update(adql=adql) or []
    )
    psa.datasets("Rosetta")
    assert "instrument_name" not in captured["adql"]


def test_granule_gid_pds3_and_pds4():
    assert psa._granule_gid("MEX-X:DATA:PROD::1.0") == "MEX-X:DATA"
    assert (psa._granule_gid("urn:esa:psa:bc_mpo_berm:data_calibrated:ber::0.1")
            == "urn:esa:psa:bc_mpo_berm:data_calibrated")


# ── geometry index: DATA_SET_ID containing a slash (e.g. VIRTIS-2/3) ──


def test_normalise_and_group_keep_slash_but_drop_data_suffix():
    assert psa._normalise_dataset("VEX-V-VIRTIS-2/3-V3.0:DATA") == "VEX-V-VIRTIS-2/3-V3.0"
    assert psa.dataset_group("VEX-V-VIRTIS-2/3-V3.0:DATA") == "VEX-V-VIRTIS-2/3-V3.0"


def test_index_dir_url_encodes_slash_as_dash(monkeypatch):
    # PSA FTP directory names render a DATA_SET_ID's "/" as "-"; splitting on the
    # raw slash never matched, leaving the whole deep label_url in place -> 404.
    label = ("https://archives.esac.esa.int/psa/ftp/VENUS-EXPRESS/VIRTIS/"
             "VEX-V-VIRTIS-2-3-V3.0/DATA/MTP001/VIR0023/RAW/VH0023_00.QUB/VH0023_00.LBL")
    monkeypatch.setattr(psa, "query", lambda adql, **k: [{"label_url": label}])
    url = psa._index_dir_url("VEX-V-VIRTIS-2/3-V3.0:DATA")
    assert url == ("https://archives.esac.esa.int/psa/ftp/VENUS-EXPRESS/VIRTIS/"
                   "VEX-V-VIRTIS-2-3-V3.0/INDEX/")
    assert "VIRTIS-2/3" not in url          # the raw slash never leaks into the path


# ── examples (catalog key -> DATA_SET_ID -> N products) ──────────────


def test_examples_accepts_dataset_directly(monkeypatch):
    # A PSA dataset (PDS3 or PDS4) must bypass the catalog and be used whole.
    captured = {}
    monkeypatch.setattr(
        psa, "query",
        lambda adql, **k: captured.update(adql=adql)
        or [{"granule_uid": "MEX-M-ASPERA3-2-EDR-IMA-EXT4-V1.0:DATA:P1::1.0",
             "access_url": "https://u"}],
    )
    df = psa.examples("MEX-M-ASPERA3-2-EDR-IMA-EXT4-V1.0", n=1)
    assert "LIKE 'MEX-M-ASPERA3-2-EDR-IMA-EXT4-V1.0:%'" in captured["adql"]
    assert df["product_id"].tolist() == ["P1"]


def test_examples_accepts_pds4_collection_lid(monkeypatch):
    # A PDS4 collection LID has internal ':' — it must be used whole, not split.
    captured = {}
    monkeypatch.setattr(
        psa, "query",
        lambda adql, **k: captured.update(adql=adql) or [],
    )
    psa.examples("urn:esa:psa:bc_mpo_berm:data_calibrated", n=3)
    assert "LIKE 'urn:esa:psa:bc_mpo_berm:data_calibrated:%'" in captured["adql"]


def test_granule_product_id():
    assert psa._granule_product_id("DS-1:DATA:ELSSCIH_X::21.0") == "ELSSCIH_X"


def test_examples_chains_catalog_to_dataset(monkeypatch):
    import pandas as pd

    import planetarypy.catalog as cat

    monkeypatch.setattr(
        cat, "example_products", lambda key: pd.DataFrame({"product_id": ["SEED1"]})
    )
    monkeypatch.setattr(
        psa, "resolve_all",
        lambda pid, **k: [{"granule_uid": "DS-1:DATA:SEED1::1.0", "access_url": "u"}],
    )
    captured = {}

    def fake_query(adql, **k):
        captured["adql"] = adql
        return [
            {"granule_uid": "DS-1:DATA:P1::1.0", "access_url": "https://a"},
            {"granule_uid": "DS-1:DATA:P2::1.0", "access_url": "https://b"},
        ]

    monkeypatch.setattr(psa, "query", fake_query)
    df = psa.examples("mex.aspera.els_edr_high", n=2)
    assert list(df.columns) == ["product_id", "granule_uid", "access_url"]
    assert df["product_id"].tolist() == ["P1", "P2"]
    # derived from the seed's granule group, no mission-name translation
    assert "LIKE 'DS-1:DATA:%'" in captured["adql"]


def test_examples_empty_when_key_unknown(monkeypatch):
    import planetarypy.catalog as cat

    def boom(key):
        raise KeyError("nope")

    monkeypatch.setattr(cat, "example_products", boom)
    df = psa.examples("x.y.z")
    assert df.empty
    assert list(df.columns) == ["product_id", "granule_uid", "access_url"]


# ── fetch_psa_product ────────────────────────────────────────────────


def _psa_zip_writer(dataset="MEX-DS-1", product="PROD.IMG", volume="0010"):
    """Return a fake url_retrieve that writes a realistic PSA mini-archive.

    Mirrors the real structure: a top folder named for the DATA_SET_ID holding
    a volume-sharded DATA/BROWSE tree and dataset-level docs at its root, plus
    the zip's own ``inventory.txt`` manifest loose at the archive root.
    """
    base = product.rsplit(".", 1)[0]

    def fake_retrieve(url, outfile):
        with zipfile.ZipFile(outfile, "w") as zf:
            zf.writestr(f"{dataset}/DATA/{volume}/{product}", "image")
            zf.writestr(f"{dataset}/BROWSE/{volume}/{base}.JPG", "browse")
            zf.writestr(f"{dataset}/VOLDESC.CAT", "voldesc")
            zf.writestr(f"{dataset}/AAREADME.TXT", "readme")
            zf.writestr("inventory.txt", "manifest")

    return fake_retrieve


def test_fetch_extracts_faithfully_under_psa_root(tmp_path, monkeypatch):
    from planetarypy.config import config
    import planetarypy.utils as utils

    monkeypatch.setattr(
        psa, "resolve_all",
        lambda pid, **k: [{"granule_uid": f"MEX-DS-1:DATA:{pid}::1.0",
                           "access_url": "https://x/product"}],
    )
    monkeypatch.setattr(utils, "have_internet", lambda: True)
    monkeypatch.setattr(config, "storage_root", tmp_path)
    monkeypatch.setattr(utils, "url_retrieve", _psa_zip_writer())

    paths = psa.fetch_psa_product("PROD.IMG")
    psa_root = tmp_path / "psa"
    # Dataset folder = the zip's own top folder (the real DATA_SET_ID).
    # Volume sharding (0010) is preserved; the image stays a file.
    img = psa_root / "MEX-DS-1" / "DATA" / "0010" / "PROD.IMG"
    assert img.is_file() and img.read_text() == "image"
    assert (psa_root / "MEX-DS-1" / "BROWSE" / "0010" / "PROD.JPG").is_file()
    # Dataset-level docs land at the dataset-folder root.
    assert (psa_root / "MEX-DS-1" / "VOLDESC.CAT").is_file()
    assert (psa_root / "MEX-DS-1" / "AAREADME.TXT").is_file()
    # The zip's own manifest is dropped, never written to the tree.
    assert not (psa_root / "inventory.txt").exists()
    # No file-named folder, no double-wrapped dataset tree.
    assert not (psa_root / "PROD.IMG").is_dir()
    assert img in paths


def test_fetch_drops_inventory_manifest(tmp_path, monkeypatch):
    from planetarypy.config import config
    import planetarypy.utils as utils

    monkeypatch.setattr(
        psa, "resolve_all",
        lambda pid, **k: [{"granule_uid": f"MEX-DS-1:DATA:{pid}::1.0",
                           "access_url": "u"}],
    )
    monkeypatch.setattr(utils, "have_internet", lambda: True)
    monkeypatch.setattr(config, "storage_root", tmp_path)
    monkeypatch.setattr(utils, "url_retrieve", _psa_zip_writer())

    paths = psa.fetch_psa_product("PROD.IMG")
    assert all(p.name != "inventory.txt" for p in paths)


def test_fetch_writes_dataset_docs_once(tmp_path, monkeypatch):
    from planetarypy.config import config
    import planetarypy.utils as utils

    monkeypatch.setattr(utils, "have_internet", lambda: True)
    monkeypatch.setattr(config, "storage_root", tmp_path)

    # Two products of the same dataset; the second ships an updated VOLDESC,
    # which must NOT overwrite the one already on disk.
    def resolve(pid, **k):
        return [{"granule_uid": f"MEX-DS-1:DATA:{pid}::1.0", "access_url": "u"}]

    monkeypatch.setattr(psa, "resolve_all", resolve)

    monkeypatch.setattr(utils, "url_retrieve", _psa_zip_writer(product="A.IMG"))
    psa.fetch_psa_product("A.IMG")
    voldesc = tmp_path / "psa" / "MEX-DS-1" / "VOLDESC.CAT"
    assert voldesc.read_text() == "voldesc"

    def writer2(url, outfile):
        with zipfile.ZipFile(outfile, "w") as zf:
            zf.writestr("MEX-DS-1/DATA/0011/B.IMG", "image-b")
            zf.writestr("MEX-DS-1/VOLDESC.CAT", "CHANGED")

    monkeypatch.setattr(utils, "url_retrieve", writer2)
    paths = psa.fetch_psa_product("B.IMG")
    assert (tmp_path / "psa" / "MEX-DS-1" / "DATA" / "0011" / "B.IMG").is_file()
    # Doc written once: the original content survives.
    assert voldesc.read_text() == "voldesc"
    assert voldesc in paths  # still reported as part of product B


def test_fetch_is_idempotent_via_marker(tmp_path, monkeypatch):
    from planetarypy.config import config
    import planetarypy.utils as utils

    monkeypatch.setattr(
        psa, "resolve_all",
        lambda pid, **k: [{"granule_uid": f"MEX-DS-1:DATA:{pid}::1.0",
                           "access_url": "u"}],
    )
    monkeypatch.setattr(utils, "have_internet", lambda: True)
    monkeypatch.setattr(config, "storage_root", tmp_path)

    calls = {"n": 0}
    writer = _psa_zip_writer()

    def counting_writer(url, outfile):
        calls["n"] += 1
        writer(url, outfile)

    monkeypatch.setattr(utils, "url_retrieve", counting_writer)
    first = psa.fetch_psa_product("PROD.IMG")
    second = psa.fetch_psa_product("PROD.IMG")
    assert calls["n"] == 1           # second fetch served from the marker
    assert first == second


def test_fetch_no_extract_returns_zip(tmp_path, monkeypatch):
    import planetarypy.utils as utils

    monkeypatch.setattr(
        psa, "resolve_all",
        lambda pid, **k: [{"granule_uid": f"DS-1:DATA:{pid}::1.0",
                           "access_url": "https://x/product"}],
    )
    monkeypatch.setattr(utils, "have_internet", lambda: True)
    monkeypatch.setattr(
        utils, "url_retrieve",
        lambda url, outfile: zipfile.ZipFile(outfile, "w").close(),
    )
    paths = psa.fetch_psa_product("PROD", dest=tmp_path, extract=False)
    assert len(paths) == 1 and paths[0].suffix == ".zip"


def test_fetch_not_found_raises(tmp_path, monkeypatch):
    import planetarypy.utils as utils

    monkeypatch.setattr(utils, "have_internet", lambda: True)
    monkeypatch.setattr(psa, "resolve_all", lambda pid, **k: [])
    with pytest.raises(ValueError, match="No PSA product"):
        psa.fetch_psa_product("PROD", dest=tmp_path)


def test_fetch_offline_raises(monkeypatch):
    from planetarypy.catalog import OfflineError
    import planetarypy.utils as utils

    monkeypatch.setattr(utils, "have_internet", lambda: False)
    with pytest.raises(OfflineError):
        psa.fetch_psa_product("PROD")


# ── direct FTP fetch (label_url) ─────────────────────────────────────


def test_label_data_pointers_parses_quoted_only():
    text = (
        '^SPREADSHEET = "PROD.CSV"\n'
        '^IMAGE       = 13\n'            # attached: record offset, not a file
        '^LABEL       = "PROD.LBL"\n'    # self-reference, skipped
        '^TABLE       = ("PROD.TAB", 1)\n'
    )
    assert psa._label_data_pointers(text) == ["PROD.CSV", "PROD.TAB"]


def test_ftp_relpath_strips_mission_instrument():
    url = ("https://archives.esac.esa.int/psa/ftp/MARS-EXPRESS/HRSC/"
           "MEX-M-HRSC-3-RDR-V4.0/DATA/0010/H0010_0000_GR3.IMG")
    assert (psa._ftp_relpath(url, "MEX-M-HRSC-3-RDR-V4.0")
            == "MEX-M-HRSC-3-RDR-V4.0/DATA/0010/H0010_0000_GR3.IMG")


def _file_writer(record):
    from pathlib import Path

    def fake(url, outfile):
        record.append(url.rsplit("/", 1)[-1])
        p = Path(outfile)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("bytes")

    return fake


def test_fetch_direct_attached_label(tmp_path, monkeypatch):
    import planetarypy.utils as utils

    lbl = ("https://archives.esac.esa.int/psa/ftp/MARS-EXPRESS/HRSC/"
           "MEX-DS-1/DATA/0010/PROD.IMG")
    monkeypatch.setattr(utils, "have_internet", lambda: True)
    monkeypatch.setattr(
        psa, "resolve_all",
        lambda pid, **k: [{"granule_uid": f"MEX-DS-1:DATA:{pid}::1.0",
                           "access_url": "zip://x", "label_url": lbl}],
    )
    seen = []
    monkeypatch.setattr(utils, "url_retrieve", _file_writer(seen))

    paths = psa.fetch_psa_product("PROD.IMG", dest=tmp_path)
    # attached label = self-contained: one direct file, no zip, no .LBL probe.
    assert seen == ["PROD.IMG"]
    assert paths == [tmp_path / "MEX-DS-1" / "DATA" / "0010" / "PROD.IMG"]
    assert not list(tmp_path.glob("**/*.zip"))


def test_fetch_direct_detached_label(tmp_path, monkeypatch):
    import planetarypy.utils as utils

    lbl = ("https://archives.esac.esa.int/psa/ftp/MARS-EXPRESS/ASPERA-3/"
           "MEX-DS-1/DATA/D1/PROD.LBL")
    monkeypatch.setattr(utils, "have_internet", lambda: True)
    monkeypatch.setattr(
        psa, "resolve_all",
        lambda pid, **k: [{"granule_uid": f"MEX-DS-1:DATA:{pid}::1.0",
                           "access_url": "zip://x", "label_url": lbl}],
    )

    class Resp:
        text = '^SPREADSHEET = "PROD.CSV"\n^LABEL = "PROD.LBL"\n'

    monkeypatch.setattr(psa.requests, "get", lambda url, **k: Resp())
    seen = []
    monkeypatch.setattr(utils, "url_retrieve", _file_writer(seen))

    paths = psa.fetch_psa_product("PROD", dest=tmp_path)
    # detached label: the .LBL plus the co-located data file it points at.
    assert {p.name for p in paths} == {"PROD.LBL", "PROD.CSV"}
    assert seen == ["PROD.LBL", "PROD.CSV"]
    assert (tmp_path / "MEX-DS-1" / "DATA" / "D1" / "PROD.CSV").is_file()


def test_fetch_direct_false_forces_zip(tmp_path, monkeypatch):
    import planetarypy.utils as utils

    lbl = ("https://archives.esac.esa.int/psa/ftp/M/I/"
           "MEX-DS-1/DATA/0010/PROD.IMG")
    monkeypatch.setattr(utils, "have_internet", lambda: True)
    monkeypatch.setattr(
        psa, "resolve_all",
        lambda pid, **k: [{"granule_uid": f"MEX-DS-1:DATA:{pid}::1.0",
                           "access_url": "zip://x", "label_url": lbl}],
    )
    monkeypatch.setattr(utils, "url_retrieve", _psa_zip_writer())

    psa.fetch_psa_product("PROD.IMG", dest=tmp_path, direct=False)
    # direct=False ignores label_url and unpacks the zip bundle.
    assert (tmp_path / "MEX-DS-1" / "DATA" / "0010" / "PROD.IMG").is_file()
    assert (tmp_path / "MEX-DS-1" / "VOLDESC.CAT").is_file()


# ── discovery: geometry index ────────────────────────────────────────


def test_dataset_group_strips_ext_keeps_version():
    assert psa.dataset_group("MEX-M-HRSC-3-RDR-V4.0") == "MEX-M-HRSC-3-RDR-V4.0"
    assert psa.dataset_group("MEX-M-HRSC-3-RDR-EXT4-V4.0:DATA") == "MEX-M-HRSC-3-RDR-V4.0"
    # different archive version stays a distinct group
    assert psa.dataset_group("MEX-M-HRSC-3-RDR-V3.0") == "MEX-M-HRSC-3-RDR-V3.0"


def test_group_members_filters_by_group(monkeypatch):
    def fake_query(adql, **k):
        if "instrument_host_name AS host" in adql:
            return [{"host": "MARS EXPRESS", "instr": "HRSC"}]
        return [
            {"granule_gid": "MEX-M-HRSC-3-RDR-V4.0:DATA"},
            {"granule_gid": "MEX-M-HRSC-3-RDR-EXT1-V4.0:DATA"},
            {"granule_gid": "MEX-M-HRSC-3-RDR-V3.0:DATA"},      # other version
            {"granule_gid": "MEX-M-HRSC-5-REFDR-MAPPROJECTED-V3.0:DATA"},  # other group
        ]

    monkeypatch.setattr(psa, "query", fake_query)
    members = psa.group_members("MEX-M-HRSC-3-RDR-V4.0")
    assert members == ["MEX-M-HRSC-3-RDR-EXT1-V4.0", "MEX-M-HRSC-3-RDR-V4.0"]


def test_geometry_index_aggregates_members(tmp_path, monkeypatch):
    import pandas as pd
    from planetarypy.config import config

    monkeypatch.setattr(config, "storage_root", tmp_path)
    monkeypatch.setattr(psa, "group_members", lambda d: ["DS-A", "DS-B"])

    def fake_member(ds, cache_dir, *, force):
        return pd.DataFrame({"PRODUCT_ID": [f"{ds}_1", f"{ds}_2"],
                             "INCIDENCE_ANGLE": [10, 80]})

    monkeypatch.setattr(psa, "_member_geometry_df", fake_member)
    df = psa.geometry_index("DS-A")
    assert list(df["PRODUCT_ID"]) == ["DS-A_1", "DS-A_2", "DS-B_1", "DS-B_2"]
    # filter handle works on the aggregated frame
    assert set(df[df.INCIDENCE_ANGLE < 50]["PRODUCT_ID"]) == {"DS-A_1", "DS-B_1"}


def test_geometry_index_single_dataset(tmp_path, monkeypatch):
    import pandas as pd
    from planetarypy.config import config

    monkeypatch.setattr(config, "storage_root", tmp_path)
    called = []

    def fake_member(ds, cache_dir, *, force):
        called.append(ds)
        return pd.DataFrame({"PRODUCT_ID": ["X"]})

    # aggregate=False must not enumerate the group
    monkeypatch.setattr(psa, "group_members",
                        lambda d: (_ for _ in ()).throw(AssertionError("enumerated")))
    monkeypatch.setattr(psa, "_member_geometry_df", fake_member)
    df = psa.geometry_index("MEX-DS-1:DATA", aggregate=False)
    assert called == ["MEX-DS-1"]  # normalised, single member
    assert list(df["PRODUCT_ID"]) == ["X"]


def test_member_geometry_df_reads_parquet_cache(tmp_path, monkeypatch):
    import pandas as pd

    cache_dir = tmp_path / ".indexes"
    cache_dir.mkdir()
    pd.DataFrame({"PRODUCT_ID": ["A"]}).to_parquet(cache_dir / "MEX-DS-1.parquet")
    # no network setup: a cache hit must return before any query/requests call
    monkeypatch.setattr(
        psa, "query",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("hit network")),
    )
    df = psa._member_geometry_df("MEX-DS-1", cache_dir, force=False)
    assert list(df["PRODUCT_ID"]) == ["A"]


# ── live ─────────────────────────────────────────────────────────────


@pytest.mark.slow
def test_live_resolve_mars_express():
    url = psa.resolve("IMA_AZ1120140041730C_ACCS01")
    assert url and url.startswith("https://")


@pytest.mark.slow
def test_live_geometry_index_hrsc():
    df = psa.geometry_index("MEX-M-HRSC-3-RDR-V4.0", aggregate=False)
    assert not df.empty
    assert {"PRODUCT_ID", "INCIDENCE_ANGLE", "SOLAR_LONGITUDE"} <= set(df.columns)
