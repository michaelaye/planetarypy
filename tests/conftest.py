"""Shared test helpers.

``is_transient_network_error`` — the single place that decides whether a failure
in a network-dependent test is a *transient connectivity* problem (a connect
timeout / refused / reset) that may legitimately be skipped, versus a real failure
that must surface.

This distinction is load-bearing for the cold-cache / clean-environment tests
(the NAIF kernel prefetch, the PDS index parquet round-trip). Those runs are
exactly where genuine regressions show up — a wrong or renamed URL, an SSL/cert
problem, a missing dependency, a parse regression — so the skip must be *narrow*.
A blanket ``except Exception: skip`` would re-create the "kitchen-sink" blind spot
by mislabeling a real bug as "server down". Only a genuine connection timeout is
tolerable; everything else fails loudly. When in doubt, we return False (fail).
"""
from __future__ import annotations

import socket
import urllib.error

import pytest
import requests.exceptions as _rexc


def _chain(exc: BaseException):
    """Yield exc and its ``__cause__``/``__context__`` chain (cycle-safe)."""
    seen: set[int] = set()
    cur: BaseException | None = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        yield cur
        cur = cur.__cause__ or cur.__context__


def _is_transient_network_error(exc: BaseException) -> bool:
    for e in _chain(exc):
        # --- definite, non-transient failures: classify FIRST ---
        # An HTTP status response (404/500/...) is a real answer from the server,
        # not a connectivity blip — e.g. a kernel renamed at NAIF, or a wrong URL.
        if isinstance(e, (urllib.error.HTTPError, _rexc.HTTPError, _rexc.SSLError)):
            return False
        # planetarypy.utils.url_retrieve raises a *builtin* ConnectionError
        # carrying "Error code: NNN" for any non-200 status — that's HTTP-status,
        # not connectivity.
        if type(e) is ConnectionError and "Error code:" in str(e):
            return False

        # --- genuine transient connectivity ---
        # requests connect/read timeouts and connection errors (SSLError, a
        # subclass of requests.ConnectionError, was already excluded above).
        if isinstance(e, (_rexc.Timeout, _rexc.ConnectionError)):
            return True
        # stdlib urllib: URLError wraps the real reason; HTTPError handled above.
        if isinstance(e, urllib.error.URLError):
            if isinstance(e.reason, (TimeoutError, socket.timeout, ConnectionError)):
                return True
        # builtins (connection refused/reset/aborted, socket timeout)
        if isinstance(e, (TimeoutError, socket.timeout, ConnectionError)):
            return True
    return False


@pytest.fixture(scope="session")
def is_transient_network_error():
    """Session fixture exposing the transient-network-error classifier."""
    return _is_transient_network_error


@pytest.fixture(autouse=True)
def _isolate_index_cache():
    """Clear the process-level index cache around every test.

    ``get_index`` memoizes the loaded frame per dotted key for the life of
    the process. Tests stub ``Index`` with different canned frames under the
    same key (e.g. ``mro.ctx.edr``), so without this the second test would
    read the first test's cached frame. Real callers are unaffected — same
    key means same index.
    """
    from planetarypy.pds import clear_index_cache

    clear_index_cache()
    yield
    clear_index_cache()
