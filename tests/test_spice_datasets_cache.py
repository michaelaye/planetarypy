import os
import pandas as pd
import pytest
from pathlib import Path


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
