"""Tests for ``planetarypy.utils.parallel_map``."""
from __future__ import annotations

import os

import pytest

from planetarypy.utils import parallel_map


# pytest-xdist runs each worker in its own process; nesting a
# ProcessPoolExecutor inside that worker doesn't always survive
# the fork/spawn interplay. Skip the one process-pool test under
# xdist — the functionality is verified by the dedicated serial run.
_UNDER_XDIST = "PYTEST_XDIST_WORKER" in os.environ


def _square(x: int) -> int:
    return x * x


def _bang_on_three(x: int) -> int:
    if x == 3:
        raise ValueError(f"refusing to square {x}")
    return x * x


def _identity(x):
    return x


def test_parallel_map_returns_input_ordered_triples():
    out = parallel_map(_square, [1, 2, 3, 4], workers=2)
    assert [item for item, _, _ in out] == [1, 2, 3, 4]
    assert [result for _, result, _ in out] == [1, 4, 9, 16]
    assert all(exc is None for _, _, exc in out)


def test_parallel_map_captures_per_item_exceptions_without_failing_batch():
    out = parallel_map(_bang_on_three, [1, 2, 3, 4], workers=2)
    items, results, excs = zip(*out)
    assert items == (1, 2, 3, 4)
    assert results == (1, 4, None, 16)
    assert excs[0] is None
    assert excs[1] is None
    assert isinstance(excs[2], ValueError)
    assert "refusing to square 3" in str(excs[2])
    assert excs[3] is None


def test_parallel_map_empty_input_returns_empty_list():
    assert parallel_map(_square, [], workers=4) == []


def test_parallel_map_accepts_generator_input():
    gen = (i for i in range(3))
    out = parallel_map(_square, gen, workers=2)
    assert [r for _, r, _ in out] == [0, 1, 4]


def test_parallel_map_thread_executor_default():
    # Thread executor handles closures and lambdas — the default
    # 'thread' selector lets us pass lambdas without pickling issues.
    out = parallel_map(lambda x: x + 100, [1, 2, 3], workers=2)
    assert [r for _, r, _ in out] == [101, 102, 103]


@pytest.mark.skipif(
    _UNDER_XDIST,
    reason="ProcessPoolExecutor doesn't nest cleanly inside pytest-xdist workers",
)
def test_parallel_map_process_executor_selectable():
    # Process executor needs picklable callables (module-level functions).
    out = parallel_map(_square, [1, 2, 3], workers=2, executor="process")
    assert [r for _, r, _ in out] == [1, 4, 9]


def test_parallel_map_rejects_unknown_executor():
    with pytest.raises(ValueError, match="executor must be"):
        parallel_map(_square, [1, 2], executor="bogus")


def test_parallel_map_preserves_input_order_under_concurrency():
    # Even though futures complete in arbitrary order under threading,
    # the returned list must mirror input order so callers can index it.
    items = list(range(20))
    out = parallel_map(_identity, items, workers=8)
    assert [item for item, _, _ in out] == items
    assert [result for _, result, _ in out] == items
