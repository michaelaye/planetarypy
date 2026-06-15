"""Unit tests for the transient-network-error classifier (tests/conftest.py).

These pin the contract the cold-cache / clean-run tests rely on: a genuine
connection timeout (or refused/reset) may be skipped, but an HTTP-status error
(404/500), an SSL error, or any parse/code failure must NOT be — those are real
regressions a clean run is meant to surface, and a too-broad skip would mask them.
"""
import socket
import urllib.error

import requests.exceptions as rexc

# --- transient (skippable) ---
TRANSIENT = [
    rexc.ConnectTimeout("connect timed out"),
    rexc.ReadTimeout("read timed out"),
    rexc.Timeout("timeout"),
    rexc.ConnectionError("failed to establish a new connection"),
    TimeoutError("[Errno 110] Connection timed out"),
    socket.timeout("timed out"),
    ConnectionError("[Errno 111] Connection refused"),
    ConnectionResetError("[Errno 104] Connection reset by peer"),
    urllib.error.URLError(TimeoutError("[Errno 110] Connection timed out")),
    urllib.error.URLError(ConnectionRefusedError("refused")),
]

# --- NOT transient (must fail loudly) ---
HARD = [
    # url_retrieve raises a builtin ConnectionError carrying the HTTP status:
    ConnectionError("Could not download https://x/y\nError code: 404"),
    ConnectionError("Could not download https://x/y\nError code: 500"),
    rexc.HTTPError("404 Client Error"),
    rexc.SSLError("certificate verify failed"),
    urllib.error.HTTPError("https://x/y", 404, "Not Found", {}, None),
    urllib.error.URLError(ValueError("malformed")),  # non-connectivity reason
    ValueError("could not parse PDS label"),
    KeyError("PRODUCT_ID"),
    ImportError("Unable to find a usable engine; pyarrow"),
]


class TestTransientNetworkClassifier:
    def test_transient_cases_are_skippable(self, is_transient_network_error):
        for exc in TRANSIENT:
            assert is_transient_network_error(exc) is True, f"should be transient: {exc!r}"

    def test_hard_failures_are_not_skippable(self, is_transient_network_error):
        for exc in HARD:
            assert is_transient_network_error(exc) is False, f"must fail, not skip: {exc!r}"

    def test_timeout_wrapped_in_chain_is_transient(self, is_transient_network_error):
        # requests typically raises ConnectionError with the low-level timeout as
        # its __cause__/__context__.
        try:
            try:
                raise TimeoutError("[Errno 110] Connection timed out")
            except TimeoutError as cause:
                raise rexc.ConnectionError("conn failed") from cause
        except rexc.ConnectionError as exc:
            assert is_transient_network_error(exc) is True

    def test_http_status_masquerading_is_not_skipped(self, is_transient_network_error):
        # The exact shape planetarypy.utils.url_retrieve produces on a 404.
        exc = ConnectionError("Could not download https://naif/x.bsp\nError code: 404")
        assert is_transient_network_error(exc) is False
