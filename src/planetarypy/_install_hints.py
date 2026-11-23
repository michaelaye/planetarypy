"""Install hints that must be importable without any optional dependency.

Top level on purpose: importing anything under ``planetarypy.spice`` runs that
package's ``__init__``, which needs spiceypy, and ``plp`` must import without
the ``[spice]`` extra (the minimal-install smoke test, #51).
"""

SKD_INSTALL_HINT = (
    "spice-kernel-db is not installed: it is the kernel store behind plp spice "
    "and the mission metakernels behind spacecraft positions. Install it with:\n\n"
    "    pip install 'planetarypy[spice]'"
)
