"""Per-instrument display dispatchers for ``plp meta`` / ``get_meta``.

Most indexes use the generic two-column dump in :func:`get_meta`. A few
indexes (notably HiRISE EDR/RDR, where one observation explodes into
many channel rows) need custom behavior — those live in the instrument
module and this file routes the lookup.

To register a new instrument display: add a branch to
:func:`get_handler` that lazy-imports the per-instrument function. The
function must accept ``(index_key, product_id, *, long: bool)`` and
return a ``pandas.Series`` already in display order (the CLI does no
further reordering when a handler returns the result).
"""

from __future__ import annotations

from typing import Callable, Optional


def get_handler(index_key: str) -> Optional[Callable]:
    """Return the per-index meta-display function, or None for the generic path."""
    if index_key in ("mro.hirise.edr", "mro.hirise.rdr"):
        from planetarypy.instruments.mro.hirise import format_meta
        return format_meta
    return None
