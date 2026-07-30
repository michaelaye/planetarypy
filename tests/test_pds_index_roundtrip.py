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
parse regression fails loudly there.

What it needs is *any* small index, not a particular one — so it tries three,
on three different hosts, and the first that downloads wins. A single node
outage therefore cannot red the release gate: on 2026-07-30 a WUSTL permission
change 401'd the whole MSL SAM dataset and blocked releases through this test,
even though the contract it guards has nothing to do with WUSTL. Reporting that
outage is the job of the reachability probe and the smoke canary, not of this
test.

Candidates are ordered by measured ``.tab`` payload, smallest first (0.01 /
0.07 / 0.39 MB), so the happy path is cheap and the fallbacks are only paid for
during an outage.
"""
from __future__ import annotations

import pandas as pd
import pytest

from planetarypy.pds import get_index
from planetarypy.utils import have_internet

INDEX_KEYS = (
    "cassini.rss.profile_index",  # ~0.01 MB, pds-rings.seti.org
    "msl.cmn.rdr",                # ~0.07 MB, pds-geosciences.wustl.edu
    "mgs.moc.rdr",                # ~0.39 MB, planetarydata.jpl.nasa.gov
)


def test_index_download_and_parquet_roundtrip(is_transient_network_error):
    if not have_internet():
        pytest.skip("no internet connection")

    failures: list[tuple[str, Exception]] = []
    for key in INDEX_KEYS:
        try:
            # force_refresh re-downloads and re-converts, exercising both the
            # parquet write (to_parquet) and the read (read_parquet) paths.
            df = get_index(key, force_refresh=True)
        except Exception as exc:
            failures.append((key, exc))
            continue
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        return

    detail = ", ".join(f"{key}: {exc!r}" for key, exc in failures)
    # Every host unreachable is a connectivity problem, not our regression.
    if all(is_transient_network_error(exc) for _, exc in failures):
        pytest.skip(f"no index reachable (transient network errors) — {detail}")
    # Otherwise something non-transient reached us from all three independent
    # hosts at once: a missing parquet engine or a parse regression, which is
    # exactly what this test exists to catch. Surface the first such error.
    for _, exc in failures:
        if not is_transient_network_error(exc):
            raise exc
