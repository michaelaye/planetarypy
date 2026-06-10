"""Live PDS download smoke tests — a per-node availability canary.

These actually fetch a small index file from several different PDS nodes, so a
node outage or a download/parse regression surfaces. Data fetching is core to
what planetarypy does, and we want to know when a PDS node is failing.

Marked ``slow`` so they're EXCLUDED from the release gate (which runs
``-m "not slow"``) — a transient PDS blip must not false-red a release. The
scheduled ``PDS download smoke`` workflow runs them on a fresh runner as a
standalone canary that alerts (red) on PDS failures without blocking releases.
Run locally with ``pytest -m slow tests/test_pds_smoke.py``.
"""
from __future__ import annotations

import pytest

from planetarypy.pds import get_index

# One small index per distinct PDS node/server. ``force_refresh`` makes each
# run a real download (not a cache read), so the test genuinely exercises the
# node. Approx download sizes noted; the huge-only nodes (SBN ~30 MB,
# LROC/Cartography cumulative indexes 100 MB+) are intentionally omitted.
NODE_INDEXES = [
    ("pds-geosciences.wustl.edu", "msl.sam.l0"),       # ~0.24 MB
    ("pds-rings.seti.org", "go.ssi.body_summary"),     # ~10 MB
    ("hirise-pds.lpl.arizona.edu", "mro.hirise.dtm"),  # ~7 MB
]


@pytest.mark.slow
@pytest.mark.parametrize(
    "node,key", NODE_INDEXES, ids=[n for n, _ in NODE_INDEXES]
)
def test_pds_index_download(node, key):
    """Fetch a small index from ``node`` and confirm it parses to rows."""
    df = get_index(key, force_refresh=True)
    assert len(df) > 0, f"{node}: {key} downloaded but parsed to 0 rows"
