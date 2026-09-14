"""Spacecraft ephemerides from metakernels tracked by spice-kernel-db.

planetarypy's generic kernels know where planets and moons are, not where a
spacecraft is. Mission metakernels that `spice-kernel-db
<https://github.com/michaelaye/spice-kernel-db>`_ already has on disk fill
that gap. Nothing is downloaded here: when no tracked metakernel covers the
requested time, the error says which ``spice-kernel-db`` command gets one.

Examples
--------
>>> from planetarypy.spice.mission_kernels import find_metakernel
>>> find_metakernel("MPO", "2027-06-01")
PosixPath('.../BEPICOLOMBO/mk/bc_plan.tm')
"""

from __future__ import annotations

import contextlib
import io
from pathlib import Path

from ._deps import spice

SKD_INSTALL_HINT = (
    "Spacecraft positions come from mission metakernels managed by "
    "spice-kernel-db, which is not installed. Install it with:\n\n"
    "    pip install 'planetarypy[skd]'"
)

# Reconstructed ("ops") trajectories beat planned ones when both cover a time;
# scenario metakernels (e.g. juice_crema_5_2.tm) come last.
_PREFERRED_SUFFIXES = ("_ops", "_plan")


def _skd():
    try:
        import spice_kernel_db
    except ImportError as exc:
        raise ImportError(SKD_INSTALL_HINT) from exc
    return spice_kernel_db


def tracked_metakernels() -> list[dict]:
    """Metakernels spice-kernel-db has on disk, without identical aliases.

    Each dict carries at least ``filename``, ``mission`` and ``mk_path``.
    Raises ``LookupError`` when the database is missing or locked by a
    running spice-kernel-db write.
    """
    import duckdb

    try:
        db = _skd().KernelDB(read_only=True)
    except duckdb.Error as exc:
        if "lock" in str(exc).lower():
            reason = ("spice-kernel-db's database is in use by another process "
                      "(a download running?). Try again when it has finished.")
        else:
            reason = ("spice-kernel-db has no readable database yet. Set up a "
                      "mission with `spice-kernel-db mission add`.")
        raise LookupError(reason) from exc
    try:
        # list_metakernels() also prints a Rich summary table
        with contextlib.redirect_stdout(io.StringIO()):
            rows = db.list_metakernels()
    finally:
        db.close()
    return [r for r in rows if not r.get("identical_to")]


def _spk_paths(mk_path) -> list[Path]:
    parsed = _skd().parse_metakernel(mk_path)
    paths = (Path(parsed.resolve(raw)) for raw in parsed.kernels)
    return [p for p in paths if p.suffix.lower() == ".bsp" and p.is_file()]


def _coverage(spk_path: Path, body_id: int) -> list[tuple[float, float]]:
    cover = spice.stypes.SPICEDOUBLE_CELL(2000)
    spice.spkcov(str(spk_path), body_id, cover)
    return [tuple(spice.wnfetd(cover, i)) for i in range(spice.wncard(cover))]


def _preference(filename: str) -> int:
    stem = Path(filename).stem.lower()
    for rank, suffix in enumerate(_PREFERRED_SUFFIXES):
        if stem.endswith(suffix):
            return rank
    return len(_PREFERRED_SUFFIXES)


def find_metakernel(spacecraft: str, time=None) -> Path:
    """The tracked metakernel whose SPKs cover ``spacecraft`` at ``time``.

    When several do, ``*_ops`` wins over ``*_plan``, which wins over any other
    (scenario) metakernel; ties go alphabetically.

    Parameters
    ----------
    spacecraft : str
        NAIF name or ID of the spacecraft, e.g. ``"MPO"`` or ``"-121"``.
    time : str or datetime, optional
        UTC time (default: now).

    Raises
    ------
    ImportError
        spice-kernel-db is not installed.
    LookupError
        No tracked metakernel covers the spacecraft at that time. The message
        names the ``spice-kernel-db`` commands that would get one.
    """
    from .spicer import _to_et

    body_id = spice.bods2c(spacecraft)
    et = _to_et(time)
    candidates = sorted(
        tracked_metakernels(),
        key=lambda r: (_preference(r["filename"]), r["filename"]),
    )
    missions_with_body = set()
    for row in candidates:
        intervals = [
            iv for spk in _spk_paths(row["mk_path"]) for iv in _coverage(spk, body_id)
        ]
        if intervals:
            missions_with_body.add(row["mission"])
        if any(start <= et <= end for start, end in intervals):
            return Path(row["mk_path"])

    when = spice.et2utc(et, "ISOC", 0)
    if missions_with_body:
        mission = sorted(missions_with_body)[0]
        hint = (
            f"Tracked {mission} metakernels don't reach {when}. See what else "
            f"exists with:\n\n    spice-kernel-db browse {mission}\n\n"
            "then fetch one with `spice-kernel-db get <name>.tm`."
        )
    else:
        hint = (
            "Add the mission and fetch a metakernel with:\n\n"
            "    spice-kernel-db mission add\n"
            "    spice-kernel-db get <name>.tm"
        )
    raise LookupError(
        f"No metakernel tracked by spice-kernel-db covers {spacecraft} at {when}. {hint}"
    )


def resolve_metakernel(name_or_path) -> Path:
    """A metakernel path, given a path on disk or a tracked metakernel's filename."""
    path = Path(name_or_path).expanduser()
    if path.is_file():
        return path
    for row in tracked_metakernels():
        if row["filename"] == str(name_or_path):
            return Path(row["mk_path"])
    raise LookupError(
        f"{name_or_path!r} is neither a file nor a metakernel tracked by "
        "spice-kernel-db. List the tracked ones with `spice-kernel-db metakernels`."
    )


def tracked_spacecraft() -> dict[str, list[str]]:
    """NAIF names of the spacecraft in each tracked mission's SPKs.

    E.g. ``{"BEPICOLOMBO": ["BEPICOLOMBO MMO", "BEPICOLOMBO MPO", ...]}``.
    """
    ids_by_mission: dict[str, set[int]] = {}
    for row in tracked_metakernels():
        ids = ids_by_mission.setdefault(row["mission"], set())
        for spk in _spk_paths(row["mk_path"]):
            ids.update(int(i) for i in spice.spkobj(str(spk)) if i < 0)
    result = {}
    for mission, ids in sorted(ids_by_mission.items()):
        names = set()
        for body_id in ids:
            with contextlib.suppress(Exception):
                names.add(spice.bodc2n(body_id))
        result[mission] = sorted(names)
    return result


def mission_spacecraft(mission: str) -> list[str]:
    """NAIF names of the spacecraft in a tracked mission's SPKs, e.g. BEPICOLOMBO → MPO."""
    for name, spacecraft in tracked_spacecraft().items():
        if name.lower() == mission.lower():
            return spacecraft
    return []


def _normalized(name: str) -> str:
    return "".join(ch for ch in name.upper() if ch.isalnum())


def spacecraft_for_mission(mission: str) -> str | None:
    """SPICE name of a NAIF-archived mission's spacecraft, when it can be told.

    Names can collide: SPICE reads ``PSYCHE`` as the asteroid, while the
    Psyche spacecraft is ``PSYC``. This looks ``mission`` up among NAIF's
    archived missions and matches it against SPICE's built-in spacecraft
    names, exactly first, then as the longest built-in name the mission
    name starts with. Returns None when ``mission`` isn't an archived
    mission or no spacecraft name matches.
    """
    try:
        from .archived_kernels import datasets
    except Exception:
        return None
    wanted = _normalized(mission)
    names = {wanted}
    for shorthand, full_name in datasets["Mission Name"].items():
        if wanted in (_normalized(shorthand), _normalized(full_name)):
            names = {_normalized(shorthand), _normalized(full_name)}
            break
    else:
        return None

    # some mission names are spacecraft names already (MRO, MAVEN, LUCY)
    for name in names:
        with contextlib.suppress(Exception):
            code = spice.bods2c(name)
            if code < 0:
                return spice.bodc2n(code)

    builtin = []
    for code in range(-1, -1000, -1):
        with contextlib.suppress(Exception):
            builtin.append(spice.bodc2n(code))
    for name in builtin:
        if _normalized(name) in names:
            return name
    prefixes = [name for name in builtin
                if len(_normalized(name)) >= 4
                and any(n.startswith(_normalized(name)) for n in names)]
    return max(prefixes, key=len) if prefixes else None
