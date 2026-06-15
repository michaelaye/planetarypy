"""Live NAIF availability canary — checks the generic SPICE kernels are reachable.

The NAIF kernel server (``naif.jpl.nasa.gov``) is, from CI's vantage point, the
single most frequent source of transient outages. The main test gate no longer
depends on it (it uses cached kernels and skips the Spicer tests when they can't
be loaded — see ``tests/test_spicer.py``), so a NAIF blip can't red the gate or a
release. This is where NAIF problems are *meant* to surface instead — and the
signal is sharper for it: a failure here means exactly one thing.

It catches two distinct failure modes:
- **Outage** — a connection timeout / URL error fetching the kernels.
- **URL drift** — NAIF renaming or superseding a default kernel (e.g. a new LSK
  or PCK edition) shows up as a 404, telling us our pinned names need updating.

Mirrors ``tests/test_pds_smoke.py``. ``slow``-marked → excluded from the gate
(which runs ``-m "not slow"``); the scheduled ``NAIF download smoke`` workflow
runs it on a fresh runner as a standalone canary. Run locally with
``pytest -m slow tests/test_naif_smoke.py``.
"""
from __future__ import annotations

import urllib.error
import urllib.request

import pytest

from planetarypy.spice.generic_kernels import GENERIC_URL, generic_kernel_names

# The exact set the default Spicer setup needs, derived from the code so this
# canary stays in sync if the default kernel list changes.
KERNEL_URLS = [str(GENERIC_URL / name) for name in generic_kernel_names]


@pytest.mark.slow
@pytest.mark.parametrize("url", KERNEL_URLS, ids=generic_kernel_names)
def test_generic_kernel_reachable(url):
    """Each default generic kernel URL responds (catches NAIF outage + URL drift)."""
    # Range-GET a single byte: confirms reachability and that the file still
    # exists at this URL, without downloading the whole (up to ~10 MB) kernel.
    req = urllib.request.Request(url, headers={"Range": "bytes=0-0"})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            assert resp.status in (200, 206), f"{url} -> HTTP {resp.status}"
    except urllib.error.HTTPError as exc:  # e.g. 404 → kernel renamed/superseded
        pytest.fail(f"{url} -> HTTP {exc.code} (kernel moved/renamed at NAIF?)")
