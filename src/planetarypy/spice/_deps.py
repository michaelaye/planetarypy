"""Friendly import guard for the optional SPICE dependency stack.

``spiceypy`` and ``scipy`` ship in the ``[spice]`` extra rather than core, so a
bare ``pip install planetarypy`` does not have them. Routing their imports
through this module turns an otherwise cryptic ``ModuleNotFoundError`` into an
actionable hint pointing at the extra.
"""

SPICE_INSTALL_HINT = (
    "planetarypy SPICE support requires the optional 'spice' dependencies "
    "(spiceypy, scipy), which are not installed. Install them with:\n\n"
    "    pip install 'planetarypy[spice]'\n\n"
    "(The conda package ships them by default.)"
)

try:
    import spiceypy as spice  # noqa: F401
except ImportError as exc:  # pragma: no cover - only without the [spice] extra
    raise ImportError(SPICE_INSTALL_HINT) from exc
