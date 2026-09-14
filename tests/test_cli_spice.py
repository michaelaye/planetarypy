"""Tests for the ``plp spice`` sub-app.

Three verbs are CLI-ready against the existing
``planetarypy.spice.archived_kernels`` API:

  - ``plp spice missions`` — Rich table of all NAIF archive missions
  - ``plp spice info <mission>`` — date range + archive metadata for one
  - ``plp spice fetch <mission> --start --stop`` — wraps
    ``get_metakernel_and_files()``; the actual NAIF download is mocked
    in tests so the suite stays offline.

Tests use Typer's ``CliRunner`` to capture stdout + stderr per stream.
"""

import re
import sys
from unittest.mock import patch

import pytest
import typer
from typer.testing import CliRunner

from planetarypy import cli
from planetarypy.cli import app
from planetarypy.spice._deps import SPICE_INSTALL_HINT


runner = CliRunner()


class TestBareInvocation:
    def test_spice_bare_prints_help(self):
        """``plp spice`` (no subcommand) should show the sub-app help,
        same UX as ``plp catalog`` / ``plp indexes``. Typer's
        ``no_args_is_help`` on a sub-app exits 2 (treats "missing
        subcommand" as user error), but the help text is still shown —
        same convention all our sub-apps follow."""
        result = runner.invoke(app, ["spice"])
        assert "Usage:" in result.stdout
        assert "missions" in result.stdout
        assert "info" in result.stdout
        assert "fetch" in result.stdout


class TestMissions:
    def test_table_lists_known_missions(self):
        result = runner.invoke(app, ["spice", "missions"])
        assert result.exit_code == 0
        # Spot-check a few well-known shorthands (case-sensitive in the
        # archive index) and the title.
        for s in ("cassini", "mro", "dawn", "bc"):
            assert s in result.stdout
        assert "NAIF mission" in result.stdout


class TestInfo:
    def test_bare_invocation_prints_help(self):
        result = runner.invoke(app, ["spice", "info"])
        assert result.exit_code == 0
        assert "Usage:" in result.stdout

    def test_info_shows_date_range_and_metadata(self):
        result = runner.invoke(app, ["spice", "info", "cassini"])
        assert result.exit_code == 0
        # Title from the Mission Name column
        assert "Cassini" in result.stdout
        # Date range field is synthesized from Start Time / Stop Time
        assert "1997-10-15" in result.stdout
        assert "2017-09-15" in result.stdout
        # Archive URLs land in the table
        assert "naif.jpl.nasa.gov" in result.stdout

    def test_unknown_mission_suggests_close_match(self):
        result = runner.invoke(app, ["spice", "info", "cassiny"])
        assert result.exit_code == 1
        assert "Unknown mission" in result.stderr
        assert "cassini" in result.stderr


class TestFetch:
    def test_bare_invocation_prints_help(self):
        result = runner.invoke(app, ["spice", "fetch"])
        assert result.exit_code == 0
        assert "Usage:" in result.stdout

    def test_missing_start_or_stop_errors_clearly(self):
        # mission supplied but no dates → typer wouldn't catch this on
        # its own (both are typer.Options) so the command's own check
        # handles it.
        result = runner.invoke(app, ["spice", "fetch", "cassini"])
        assert result.exit_code == 2
        assert "--start" in result.stderr and "--stop" in result.stderr

    def test_unknown_mission_suggests_close_match(self):
        result = runner.invoke(app, [
            "spice", "fetch", "cassiny",
            "--start", "2006-06-01", "--stop", "2006-06-30",
        ])
        assert result.exit_code == 1
        assert "Unknown mission" in result.stderr
        assert "cassini" in result.stderr

    def test_fetch_prints_metakernel_path_to_stdout(self, tmp_path):
        """The actual NAIF download is mocked; we only verify the CLI
        threads through to the public function and emits the returned
        path to stdout (pipe-safe for shell composition)."""
        fake_metakernel = tmp_path / "cassini_2006_06.tm"
        fake_metakernel.touch()

        with patch(
            "planetarypy.spice.archived_kernels.get_metakernel_and_files",
            return_value=str(fake_metakernel),
        ) as mock_fetch:
            result = runner.invoke(app, [
                "spice", "fetch", "cassini",
                "--start", "2006-06-01", "--stop", "2006-06-30",
            ])

        assert result.exit_code == 0
        assert str(fake_metakernel) in result.stdout
        mock_fetch.assert_called_once_with(
            "cassini", "2006-06-01", "2006-06-30", save_location=None,
        )


class TestCached:
    """`plp spice cached` walks {storage_root}/spice_kernels/ and groups
    by top-level directory. Mocked here so the test doesn't depend on
    what kernels the dev happens to have on disk."""

    def test_empty_cache_prints_hint_and_exits_zero(self):
        with patch(
            "planetarypy.spice.archived_kernels.list_cached_kernels",
            return_value={},
        ):
            result = runner.invoke(app, ["spice", "cached"])
        assert result.exit_code == 0
        assert "No SPICE kernels cached" in result.stderr

    def test_populated_cache_renders_rich_table(self, tmp_path):
        # Build a synthetic cache view: two groups, a few files each.
        fake_files = {
            "generic": [
                tmp_path / "naif0012.tls",
                tmp_path / "pck00010.tpc",
            ],
            "cassini": [tmp_path / "metakernel.tm"],
        }
        for files in fake_files.values():
            for f in files:
                f.write_bytes(b"x" * 100)

        with patch(
            "planetarypy.spice.archived_kernels.list_cached_kernels",
            return_value=fake_files,
        ):
            result = runner.invoke(app, ["spice", "cached"])

        assert result.exit_code == 0
        for kernel in ("naif0012.tls", "pck00010.tpc", "metakernel.tm"):
            assert kernel in result.stdout
        assert "generic" in result.stdout
        assert "cassini" in result.stdout


class TestGeneric:
    def test_bare_invocation_prints_help(self):
        result = runner.invoke(app, ["spice", "generic"])
        assert result.exit_code == 0
        assert "Usage:" in result.stdout

    def test_known_alias_returns_cached_path_to_stdout(self, tmp_path):
        """If the kernel is already cached, the wrapper returns its path
        without re-downloading — CLI just echoes that to stdout."""
        fake_path = tmp_path / "naif0012.tls"
        fake_path.touch()
        with patch(
            "planetarypy.spice.generic_kernels.download_generic_kernel",
            return_value=fake_path,
        ) as mock_dl:
            result = runner.invoke(app, ["spice", "generic", "lsk"])
        assert result.exit_code == 0
        assert str(fake_path) in result.stdout
        mock_dl.assert_called_once_with("lsk", overwrite=False)

    def test_force_flag_threads_through(self, tmp_path):
        fake_path = tmp_path / "naif0012.tls"
        fake_path.touch()
        with patch(
            "planetarypy.spice.generic_kernels.download_generic_kernel",
            return_value=fake_path,
        ) as mock_dl:
            result = runner.invoke(
                app, ["spice", "generic", "lsk", "--force"],
            )
        assert result.exit_code == 0
        mock_dl.assert_called_once_with("lsk", overwrite=True)

    def test_unknown_alias_errors_nonzero(self):
        result = runner.invoke(app, ["spice", "generic", "xyz"])
        assert result.exit_code == 1
        assert "Unknown generic-kernel alias" in result.stderr


class TestTabCompletion:
    def setup_method(self):
        from planetarypy.cli import (
            _complete_generic_alias, _complete_spice_mission,
        )

        self._complete_mission = _complete_spice_mission
        self._complete_generic = _complete_generic_alias

    def test_mission_prefix_matches(self):
        out = self._complete_mission("cas")
        assert "cassini" in out

    def test_mission_case_insensitive(self):
        out = self._complete_mission("CAS")
        assert "cassini" in out

    def test_generic_alias_prefix_matches(self):
        out = self._complete_generic("d")
        assert "de430" in out

    def test_generic_alias_full_list_on_empty_prefix(self):
        out = self._complete_generic("")
        for alias in ("lsk", "pck", "de430", "mar099s", "masses"):
            assert alias in out


class TestSpicerObserver:
    """``--observer`` threads through to ``Spicer.light_time`` and names the body."""

    @staticmethod
    def row(stdout, label):
        """The Rich table row whose first cell is ``label``, cells stripped."""
        for line in stdout.splitlines():
            cells = [c.strip() for c in re.split("[│┃]", line) if c.strip()]
            if cells and cells[0] == label:
                return cells
        raise AssertionError(f"no row {label!r} in:\n{stdout}")

    @pytest.fixture()
    def fake_spicer(self, monkeypatch):
        pytest.importorskip("spiceypy")
        from planetarypy.spice import mission_kernels
        from planetarypy.spice import spicer as spicer_mod

        observers = []

        class FakeSpicer:
            body = "MARS"
            ref_frame = "IAU_MARS"
            radii = spicer_mod.Radii(3396.2, 3396.2, 3376.2)

            def __init__(self, body):
                self.has_shape = body.upper() != "PSYCHE"
                self.is_spacecraft = body.upper() in ("MPO", "-121")
                self.target_id = -121 if self.is_spacecraft else 499

            def solar_longitude(self, time):
                return 30.0

            def subsolar_point(self, time):
                return 41.0, 12.0

            def solar_constant(self, time):
                return 515.0

            def light_time(self, time, observer, metakernel=None):
                observers.append(observer)
                self.metakernels.append(metakernel)
                return 0.0112 if metakernel else 2000.0

        FakeSpicer.metakernels = []
        monkeypatch.setattr(spicer_mod, "Spicer", FakeSpicer)
        monkeypatch.setattr(mission_kernels, "mission_spacecraft", lambda mission: [])
        monkeypatch.setattr(mission_kernels, "spacecraft_for_mission", lambda mission: None)
        return observers

    def test_default_is_earth(self, fake_spicer):
        result = runner.invoke(app, ["spicer", "Mars"])
        assert result.exit_code == 0
        assert fake_spicer == ["EARTH"]
        assert self.row(result.stdout, "Light time from Earth") == [
            "Light time from Earth", "33 min 20.0 s (= 599.585 million km)"
        ]

    def test_naif_id_is_shown_by_name(self, fake_spicer):
        result = runner.invoke(app, ["spicer", "Mars", "--observer", "599"])
        assert result.exit_code == 0
        assert fake_spicer == ["599"]
        self.row(result.stdout, "Light time from Jupiter")

    def test_unknown_observer_errors(self, fake_spicer):
        result = runner.invoke(app, ["spicer", "Mars", "--observer", "Notabody"])
        assert result.exit_code == 1
        assert result.stderr.startswith("Error:")

    def test_mission_name_lists_its_spacecraft(self, fake_spicer, monkeypatch):
        from planetarypy.spice import mission_kernels

        monkeypatch.setattr(
            mission_kernels, "mission_spacecraft", lambda mission: ["BEPICOLOMBO MPO"]
        )
        result = runner.invoke(app, ["spicer", "Mercury", "--observer", "BepiColombo"])
        assert result.exit_code == 1
        assert "'BEPICOLOMBO MPO'" in result.stderr

    def test_spacecraft_uses_found_metakernel_and_names_it(
        self, fake_spicer, monkeypatch, tmp_path
    ):
        from planetarypy.spice import mission_kernels

        mk = tmp_path / "bc_plan.tm"
        monkeypatch.setattr(mission_kernels, "find_metakernel", lambda sc, time: mk)
        result = runner.invoke(app, ["spicer", "Mercury", "--observer", "MPO"])
        assert result.exit_code == 0
        assert self.row(result.stdout, "Light time from BEPICOLOMBO MPO")[1] == (
            "0.011 s (= 3,358 km)  (bc_plan.tm)"
        )

    def test_metakernel_option_overrides_the_search(self, fake_spicer, monkeypatch, tmp_path):
        from planetarypy.spice import mission_kernels

        mk = tmp_path / "bc_ops.tm"
        mk.touch()

        def must_not_search(*args):
            raise AssertionError("find_metakernel called despite --metakernel")

        monkeypatch.setattr(mission_kernels, "find_metakernel", must_not_search)
        result = runner.invoke(
            app, ["spicer", "Mercury", "--observer", "MPO", "--metakernel", str(mk)]
        )
        assert result.exit_code == 0
        assert "(bc_ops.tm)" in result.stdout

    def test_spacecraft_as_body_shows_light_time_only(self, fake_spicer, monkeypatch, tmp_path):
        from planetarypy.spice import mission_kernels

        monkeypatch.setattr(
            mission_kernels, "find_metakernel", lambda sc, time: tmp_path / "bc_plan.tm"
        )
        result = runner.invoke(app, ["spicer", "MPO", "--lon", "1", "--lat", "2"])
        assert result.exit_code == 0
        assert "BEPICOLOMBO MPO (spacecraft)" in result.stdout
        assert self.row(result.stdout, "Light time from Earth")[1] == (
            "0.011 s (= 3,358 km)  (bc_plan.tm)"
        )
        assert "Radii" not in result.stdout
        assert self.row(result.stdout, "Surface illumination")[1] == (
            "(a spacecraft has no surface)"
        )

    def test_known_name_without_shape_is_rejected_cleanly(self, fake_spicer):
        result = runner.invoke(app, ["spicer", "Psyche"])
        assert result.exit_code == 1
        assert "Traceback" not in result.output
        assert result.stderr.startswith("Error: SPICE knows 'PSYCHE'")
        assert "plp spicer --list" in result.stderr

    def test_shapeless_name_that_is_also_a_mission_points_at_its_spacecraft(
        self, fake_spicer, monkeypatch
    ):
        from planetarypy.spice import mission_kernels

        monkeypatch.setattr(mission_kernels, "spacecraft_for_mission", lambda name: "PSYC")
        result = runner.invoke(app, ["spicer", "Psyche"])
        assert result.exit_code == 1
        assert "There is also a PSYCHE mission" in result.stderr
        assert "try `plp spicer PSYC`" in result.stderr

    def test_list_shows_bodies_and_tracked_spacecraft(self, fake_spicer, monkeypatch):
        from planetarypy.spice import mission_kernels
        from planetarypy.spice import spicer as spicer_mod

        monkeypatch.setattr(
            spicer_mod.Spicer, "supported_bodies",
            staticmethod(lambda: [(499, "MARS", 3396.19), (2000001, "CERES", 487.3)]),
            raising=False,
        )
        monkeypatch.setattr(
            mission_kernels, "tracked_spacecraft",
            lambda: {"BEPICOLOMBO": ["BEPICOLOMBO MMO", "BEPICOLOMBO MPO"]},
        )
        result = runner.invoke(app, ["spicer", "--list"])
        assert result.exit_code == 0
        assert self.row(result.stdout, "Mars") == ["Mars", "3,396.2 km"]
        assert self.row(result.stdout, "Ceres")[1] == "487.3 km"
        assert self.row(result.stdout, "BEPICOLOMBO") == [
            "BEPICOLOMBO", "BEPICOLOMBO MMO, BEPICOLOMBO MPO"
        ]

    def test_list_without_skd_still_lists_bodies(self, fake_spicer, monkeypatch):
        from planetarypy.spice import spicer as spicer_mod

        monkeypatch.setattr(
            spicer_mod.Spicer, "supported_bodies",
            staticmethod(lambda: [(499, "MARS", 3396.19)]), raising=False,
        )
        monkeypatch.setitem(sys.modules, "spice_kernel_db", None)
        result = runner.invoke(app, ["spicer", "--list"])
        assert result.exit_code == 0
        self.row(result.stdout, "Mars")
        assert "planetarypy[skd]" in result.stdout

    def test_unknown_body_errors_like_an_unknown_observer(self, fake_spicer):
        result = runner.invoke(app, ["spicer", "Notabody"])
        assert result.exit_code == 1
        assert result.stderr.startswith("Error: BODY 'Notabody'")

    def test_no_covering_metakernel_prints_the_hint(self, fake_spicer, monkeypatch):
        from planetarypy.spice import mission_kernels

        def nothing_covers(sc, time):
            raise LookupError("No metakernel tracked by spice-kernel-db covers MPO.\n"
                              "    spice-kernel-db browse BEPICOLOMBO")

        monkeypatch.setattr(mission_kernels, "find_metakernel", nothing_covers)
        result = runner.invoke(app, ["spicer", "Mercury", "--observer", "MPO"])
        assert result.exit_code == 0
        assert self.row(result.stdout, "Light time from BEPICOLOMBO MPO")[1] == (
            "(no mission kernels)"
        )
        assert "spice-kernel-db browse BEPICOLOMBO" in result.stdout


class TestSpiceExtraMissing:
    """Without the ``[spice]`` extra, the CLI owes the user one actionable line.

    The import chain raises several frames deep inside ``spice/_deps.py``.
    Unhandled, typer renders that as ~70 lines of Rich traceback with the
    install hint at the very bottom — see ``cli._spice_deps``.
    """

    SPICE_MODULES = (
        "planetarypy.spice",
        "planetarypy.spice.archived_kernels",
        "planetarypy.spice.generic_kernels",
        "planetarypy.spice.config",
        "planetarypy.spice.spicer",
        "planetarypy.spice.mission_kernels",
    )

    def _without_spice(self, monkeypatch):
        """Make every ``from planetarypy.spice... import x`` raise ImportError."""
        for name in self.SPICE_MODULES:
            monkeypatch.setitem(sys.modules, name, None)

    @pytest.mark.parametrize(
        "argv",
        [
            ["spice", "missions"],
            ["spice", "info", "cassini"],
            ["spice", "cached"],
            ["spice", "generic", "lsk"],
            ["spice", "fetch", "cassini", "--start", "2006-06-01",
             "--stop", "2006-06-30"],
            ["spicer", "Mars"],
        ],
        ids=lambda a: " ".join(a[:2]),
    )
    def test_exits_one_without_a_traceback(self, monkeypatch, argv):
        self._without_spice(monkeypatch)
        result = runner.invoke(app, argv)
        assert result.exit_code == 1
        # the hint belongs on stderr, on the first line, with no traceback
        assert "Traceback" not in result.stderr
        assert result.stderr.lstrip().startswith("Error:")

    def test_hint_text_reaches_the_user(self, capsys):
        """``_spice_deps`` must echo the guard's message, not swallow it."""
        with pytest.raises(typer.Exit) as excinfo:
            with cli._spice_deps():
                raise ImportError(SPICE_INSTALL_HINT)
        assert excinfo.value.exit_code == 1
        assert "pip install 'planetarypy[spice]'" in capsys.readouterr().err

    def test_completion_degrades_to_empty(self, monkeypatch):
        """Tab completion must not explode on a missing extra."""
        self._without_spice(monkeypatch)
        assert cli._complete_spice_mission("ca") == []
        assert cli._complete_generic_alias("l") == []
