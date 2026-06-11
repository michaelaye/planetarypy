"""Per-instrument display dispatchers for ``plp meta`` / ``get_meta``.

Most indexes use the generic two-column dump in :func:`get_meta`. A few
indexes (notably HiRISE EDR/RDR, where one observation explodes into
many channel rows) need custom behavior. Instrument packages register such
a handler via :func:`register_meta_handler`; the function must accept
``(index_key, product_id, *, long: bool)`` and return a display-ordered
``pandas.Series`` (the CLI does no further reordering).
"""

from __future__ import annotations

from typing import Callable, Optional

# index_key → display function, populated at runtime via register_meta_handler.
_META_HANDLERS: dict[str, Callable] = {}


def register_meta_handler(index_key: str, handler: Callable) -> None:
    """Register a custom ``plp meta`` display function for an index key.

    Public extension hook for instrument packages whose indexes need custom
    metadata rendering.
    """
    _META_HANDLERS[index_key] = handler


def get_handler(index_key: str) -> Optional[Callable]:
    """Return the per-index meta-display function, or None for the generic path."""
    if index_key in _META_HANDLERS:
        return _META_HANDLERS[index_key]
    # Transitional fallback (removed once HiRISE self-registers in the
    # instrument-extraction work): keep current behavior for the in-tree module.
    if index_key in ("mro.hirise.edr", "mro.hirise.rdr"):
        from planetarypy.instruments.mro.hirise import format_meta

        return format_meta
    return None
