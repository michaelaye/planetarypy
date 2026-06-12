"""Always-run regression: a real (tiny) PDS index download + parquet round-trip.

Guards a declared-dependency contract that the unit-level parquet tests and the
import-only ``minimal-install`` smoke both miss: the PDS index cache writes and
reads parquet through pandas (``to_parquet``/``read_parquet``), which needs a
parquet engine (``pyarrow``) that pandas does not bundle. A clean
``pip install planetarypy`` must ship one — this is the regression guard for the
v0.76.1 fix, and it only bites on a *runtime* parquet path (importing the
package alone never triggers it).

Deliberately **not** ``slow``-marked: unlike the per-node canary in
``test_pds_smoke.py`` (which exists to alert on PDS outages), this runs in the
normal gate and in the pip ``minimal-install`` CI job so a missing engine or a
parse regression fails loudly there. It skips on genuine connectivity failures
so PDS downtime can't false-red the gate. ``msl.sam.l0`` is the smallest
registered node index (~0.24 MB) — affordable every run (the Cassini ISS
summary indexes are 150-266 MB).
"""
from __future__ import annotations

import pandas as pd
import pytest
import requests

from planetarypy.pds import get_index
from planetarypy.utils import have_internet

INDEX_KEY = "msl.sam.l0"


def test_index_download_and_parquet_roundtrip():
    if not have_internet():
        pytest.skip("no internet connection")
    try:
        # force_refresh re-downloads and re-converts, exercising both the
        # parquet write (to_parquet) and the read (read_parquet) paths.
        df = get_index(INDEX_KEY, force_refresh=True)
    except (requests.exceptions.RequestException, ConnectionError, TimeoutError) as exc:
        pytest.skip(f"{INDEX_KEY} unreachable: {exc}")
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
