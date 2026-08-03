"""Project-wide control over whether values carry astropy units.

planetarypy already speaks astropy units in places — :mod:`planetarypy.constants`
returns :class:`~astropy.units.Quantity` subclasses, and the SPICE layer wraps
selected results — but there was no way to say "units everywhere" or "plain
floats, please" once and have it hold. This module is that switch.

    >>> from planetarypy import units
    >>> units.set_units(False)                 # plain floats for this session
    >>> with units.use_units(True): ...        # ...except inside this block

Units are **on by default**, matching what ``constants`` already does. Turning
them off is for code that feeds values straight into libraries which choke on
``Quantity`` — numba kernels, some plotting paths, JSON serialisation.

Design mirrors :mod:`planetarypy.crs`'s target-CRS switch deliberately: same
setter/getter/context-manager trio, same ``ContextVar`` backing, so the two
session settings behave identically and nest the same way.
"""

from __future__ import annotations

import contextlib
import contextvars

__all__ = [
    "set_units",
    "get_units",
    "use_units",
    "maybe_quantity",
    "attach_units",
    "units_of",
]

# ContextVar rather than a module global, for the same reasons as the target
# CRS: the context manager becomes correct by construction, and threads or async
# tasks cannot overwrite each other's setting.
_USE_UNITS: contextvars.ContextVar = contextvars.ContextVar(
    "planetarypy_use_units", default=True
)


def set_units(enabled: bool) -> bool:
    """Turn astropy units on or off for the session. Returns the previous value."""
    previous = _USE_UNITS.get()
    _USE_UNITS.set(bool(enabled))
    return previous


def get_units() -> bool:
    """Whether values should currently carry units."""
    return _USE_UNITS.get()


@contextlib.contextmanager
def use_units(enabled: bool = True):
    """Context-manager form of :func:`set_units`.

    >>> with use_units(False):
    ...     plain = nomenclature.find("mars", "Jezero").diameter   # float
    >>> # previous setting restored here, even if the block raised

    Nests, and restores on exception.
    """
    token = _USE_UNITS.set(bool(enabled))
    try:
        yield _USE_UNITS.get()
    finally:
        _USE_UNITS.reset(token)


def maybe_quantity(value, unit):
    """``value * unit`` when units are enabled, otherwise ``value`` untouched.

    The single place the toggle is consulted, so callers never branch on it
    themselves and every module honours the setting identically.
    """
    if not _USE_UNITS.get() or unit is None:
        return value
    from astropy import units as u

    return value * u.Unit(unit)


def units_of(obj) -> dict:
    """The unit map a DataFrame carries in ``.attrs``, or ``{}``.

    Units are recorded on the frame regardless of the toggle — knowing that
    ``diameter`` is km costs nothing and stays true — while the toggle governs
    only whether individual values are wrapped.
    """
    return dict(getattr(obj, "attrs", {}).get("units", {}))


def attach_units(frame, unit_map: dict):
    """Record ``{column: unit}`` on a DataFrame's ``.attrs``. Returns the frame."""
    attrs = dict(getattr(frame, "attrs", {}))
    attrs["units"] = {**attrs.get("units", {}), **unit_map}
    frame.attrs = attrs
    return frame
