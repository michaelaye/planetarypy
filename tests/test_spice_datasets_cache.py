import os
import pandas as pd
import pytest
from pathlib import Path

pytest.importorskip("spiceypy")


# Skip in parallel mode - this test needs fresh module imports
@pytest.mark.skipif(
    os.environ.get("PYTEST_XDIST_WORKER") is not None,
    reason="Test requires isolated module imports, skip in parallel mode"
)
def test_get_datasets_uses_cache_when_fresh(tmp_path, monkeypatch):
    """Test that get_datasets returns cached data when cache is fresh."""
    from planetarypy.pds.index_logging import AccessLog
    import planetarypy.spice.archived_kernels as ak_mod

    # Redirect cache and log file to tmp_path (module-level DATASETS_CACHE
    # is evaluated at import time, so we must patch it directly)
    cache_path = tmp_path / "archived_spice_datasets.csv"
    monkeypatch.setattr(ak_mod, "DATASETS_CACHE", cache_path)
    monkeypatch.setattr(AccessLog, "FILE_PATH", tmp_path / "index_log.toml")
    df = pd.DataFrame(
        {
            "Mission Name": ["Mars Reconnaissance Orbiter", "Mars Express"],
            "Archive Readme": [None, None],
            "Archive Link": [
                "https://naif.jpl.nasa.gov/pub/naif/pds/data/mro-m-spice-6-v1.0/",
                "https://naif.jpl.nasa.gov/pub/naif/pds/data/mex-e_m-spice-6-v1.0/",
            ],
            "PDS3 or PDS4": ["PDS3", "PDS3"],
            "Data Size (GB)": ["10", "5"],
            "Start Time": ["2006-01-01", "2004-01-01"],
            "Stop Time": ["2025-01-01", "2015-01-01"],
            "Subset Link": [
                "https://naif.jpl.nasa.gov/cgi-bin/subsetds.pl?dataset=mro",
                "https://naif.jpl.nasa.gov/cgi-bin/subsetds.pl?dataset=mex",
            ],
        }
    ).set_index("Mission Name")
    df.to_csv(cache_path)

    # Mark last check as now so should_check == False
    log = AccessLog("spice.archived_kernels.datasets")
    log._log_time("last_checked")

    # Ensure pd.read_html is NOT called (would fetch network)
    def fail_read_html(*args, **kwargs):
        raise AssertionError("read_html should not be called when cache is fresh")

    monkeypatch.setattr(pd, "read_html", fail_read_html)

    from planetarypy.spice.archived_kernels import get_datasets


    # Act
    out = get_datasets()

    # Assert the cache was used (same mission names, columns present)
    # Note: dtype may differ due to CSV round-trip (None -> NaN -> float64)
    assert list(out.index) == list(df.index), "Mission names should match"
    assert set(out.columns) == set(df.columns), "Columns should match"
    # If read_html was called, the assertion above would have failed


@pytest.mark.skipif(
    os.environ.get("PYTEST_XDIST_WORKER") is not None,
    reason="Test requires isolated module imports, skip in parallel mode"
)
def test_fresh_parse_coerces_data_size_to_numeric(tmp_path, monkeypatch):
    """Regression: the read_html path must return a numeric 'Data Size (GB)'.

    read_html yields every cell as a string; only the CSV cache round-trip
    coerced it to float, so the fresh-parse path used to leak strings and
    crash `plp spice`'s `:g` format. This pins both paths to numeric.
    """
    from planetarypy.pds.index_logging import AccessLog
    import planetarypy.spice.archived_kernels as ak_mod

    cache_path = tmp_path / "archived_spice_datasets.csv"
    monkeypatch.setattr(ak_mod, "DATASETS_CACHE", cache_path)
    monkeypatch.setattr(AccessLog, "FILE_PATH", tmp_path / "index_log.toml")

    # Mimic pd.read_html(extract_links="all"): every cell is a (text, href)
    # tuple, all text values are strings — including "Data Size (GB)".
    cols = [
        "Mission Name", "Archive Readme", "Archive Link", "PDS3 or PDS4",
        "Data Size (GB)", "Start Time", "Stop Time", "Subset Link",
    ]
    rows = [
        ("Cassini", "readme", ("link", "http://x/cas"), "PDS3",
         "73.5", "1997-10-15", "2017-09-15", ("sub", "http://x/cas")),
        ("Mars Express", "readme", ("link", "http://x/mex"), "PDS3",
         "0.8", "2004-01-01", "2015-01-01", ("sub", "http://x/mex")),
    ]
    table = pd.DataFrame(
        [[(str(c), None) if not isinstance(c, tuple) else c for c in r]
         for r in rows],
        columns=[(c, None) for c in cols],
    )
    monkeypatch.setattr(pd, "read_html", lambda *a, **k: [None] * 6 + [table])

    from planetarypy.spice.archived_kernels import get_datasets
    out = get_datasets()

    assert pd.api.types.is_numeric_dtype(out["Data Size (GB)"]), (
        "Data Size (GB) must be numeric on the fresh-parse path"
    )
    assert out.loc["Cassini", "Data Size (GB)"] == 73.5
