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

from unittest.mock import patch

from typer.testing import CliRunner

from planetarypy.cli import app


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
