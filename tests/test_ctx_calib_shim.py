"""Tests for the backward-compat ``process_parallel`` shim in
``planetarypy.instruments.mro.ctx.ctx_calib``.

The shim now delegates to :func:`planetarypy.utils.parallel_map`; these
tests pin its contract so the public API (signature + early-failure
semantics + result ordering) survives the refactor.

``ctx_calib`` (and its sibling ``ctx_edr``, pulled in via the package
``__init__``) imports ``hvplot.pandas`` at module top, which is not
installed in the minimal CI environment. The whole test module is
skipped when ``hvplot`` is absent so collection doesn't crash.
"""
from __future__ import annotations

import os

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import pytest

pytest.importorskip("hvplot")

from planetarypy.instruments.mro.ctx.ctx_calib import process_parallel  # noqa: E402


def _upcase(pid):
    return pid.upper()


def _upcase_with_refresh(pid, refresh=False):
    return f"{pid.upper()}:{refresh}"


def _bang_on_x(pid, refresh=False):
    if pid == "x":
        raise RuntimeError("nope")
    return pid.upper()


_UNDER_XDIST = "PYTEST_XDIST_WORKER" in os.environ


class TestProcessParallelShim:

    def test_thread_executor_returns_results_in_input_order(self):
        out = process_parallel(ThreadPoolExecutor, _upcase, ["a", "b", "c"])
        assert out == ["A", "B", "C"]

    def test_refresh_kwarg_is_forwarded(self):
        out = process_parallel(
            ThreadPoolExecutor, _upcase_with_refresh,
            ["a", "b"], refresh=True,
        )
        assert out == ["A:True", "B:True"]

    def test_refresh_none_skips_kwarg(self):
        # When refresh is None, task should be called without that kwarg
        # — matches the historical signature.
        out = process_parallel(ThreadPoolExecutor, _upcase, ["a"], refresh=None)
        assert out == ["A"]

    def test_first_exception_is_raised_preserving_old_semantics(self):
        with pytest.raises(RuntimeError, match="nope"):
            process_parallel(
                ThreadPoolExecutor, _bang_on_x,
                ["a", "x", "c"], refresh=False,
            )

    @pytest.mark.skipif(
        _UNDER_XDIST,
        reason="ProcessPoolExecutor doesn't nest cleanly inside pytest-xdist",
    )
    def test_process_executor_selector(self):
        out = process_parallel(ProcessPoolExecutor, _upcase, ["a", "b"])
        assert out == ["A", "B"]
