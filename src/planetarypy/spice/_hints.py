"""Install hints that must be importable without any optional dependency."""

SKD_INSTALL_HINT = (
    "spice-kernel-db is not installed: it is the kernel store behind plp spice "
    "and the mission metakernels behind spacecraft positions. Install it with:\n\n"
    "    pip install 'planetarypy[spice]'"
)
