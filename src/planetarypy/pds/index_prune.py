"""Find and remove index caches left behind by superseded storage layouts.

An index cache is an *orphan* when it sits in a directory that
:func:`planetarypy.pds.index_main.index_local_dir` does not compute for any
registered index key. That is a difference against the live set rather than a
list of the layouts planetarypy happens to have used, so directories stranded
by a future layout change are caught the same way as the historical ones, and
so are caches whose mission or index key was renamed upstream.
"""

__all__ = [
    "OrphanCache",
    "find_orphan_index_caches",
    "remove_orphan_index_caches",
]

from dataclasses import dataclass
from pathlib import Path

from ..config import config
from .index_main import index_local_dir
from .utils import IndexKeyError, _all_dotted_index_keys


@dataclass(frozen=True)
class OrphanCache:
    """Index files found in a directory the current layout never uses."""

    directory: Path
    files: tuple[Path, ...]
    n_bytes: int


def _live_index_dirs(storage_root: Path) -> set[Path]:
    """Resolved cache directories of every registered index key."""
    live = set()
    for key in _all_dotted_index_keys():
        try:
            live.add(index_local_dir(key, storage_root).resolve())
        except IndexKeyError:
            continue
    return live


def _candidate_dirs(root: Path):
    """Every directory at or below an ``indexes`` directory under ``root``."""
    for indexes_dir in root.rglob("indexes"):
        if not indexes_dir.is_dir():
            continue
        yield indexes_dir
        for sub in indexes_dir.rglob("*"):
            if sub.is_dir():
                yield sub


def find_orphan_index_caches(
    storage_root: str | Path | None = None,
) -> list[OrphanCache]:
    """Return the orphaned index caches under ``storage_root``.

    Parameters
    ----------
    storage_root : str or Path, optional
        Root to scan. Defaults to the configured ``storage_root``.

    Returns
    -------
    list of OrphanCache
        One entry per orphaned directory, sorted by path. Directories holding
        no files of their own are skipped, so a live cache nested inside an
        orphaned parent leaves the parent listed with only its own loose files.
    """
    root = Path(storage_root) if storage_root else Path(config.storage_root)
    if not root.is_dir():
        return []

    live = _live_index_dirs(root)
    found: dict[Path, OrphanCache] = {}
    for d in _candidate_dirs(root):
        resolved = d.resolve()
        if resolved in live or resolved in found:
            continue
        files = tuple(sorted(f for f in d.iterdir() if f.is_file()))
        if not files:
            continue
        found[resolved] = OrphanCache(
            directory=d,
            files=files,
            n_bytes=sum(f.stat().st_size for f in files),
        )
    return sorted(found.values(), key=lambda o: o.directory)


def _remove_empty_dirs(leaf: Path, stop: Path) -> None:
    d = leaf.resolve()
    stop = stop.resolve()
    while d != stop and d.is_relative_to(stop) and d.is_dir() and not any(d.iterdir()):
        d.rmdir()
        d = d.parent


def remove_orphan_index_caches(
    caches: list[OrphanCache],
    storage_root: str | Path | None = None,
) -> tuple[int, int]:
    """Delete the files in ``caches`` and return ``(n_files, n_bytes)``.

    Only the files listed on each :class:`OrphanCache` are removed, never the
    directory tree: an orphaned directory can contain a live cache in a
    subdirectory, which a recursive delete would take with it. Directories
    left empty afterwards are removed up to ``storage_root``.
    """
    root = Path(storage_root) if storage_root else Path(config.storage_root)
    n_files = 0
    n_bytes = 0
    for cache in caches:
        for f in cache.files:
            n_bytes += f.stat().st_size
            f.unlink()
            n_files += 1
        _remove_empty_dirs(cache.directory, root)
    return n_files, n_bytes
