"""Tests for ``plp constants``.

Uses Typer's ``CliRunner`` (which captures stdout and stderr separately)
to lock in the contract:

  - ``plp constants Mars.GM`` writes the value to stdout and the source
    line to stderr (so the value stays pipe-safe through ``awk``/``jq``).
  - ``plp constants Mars`` writes a Rich table summarising every scalar
    Constant attached to the body.
  - Body matching is case-insensitive; misspelt bodies and unknown fields
    each exit non-zero with a suggestion on stderr.
  - ``--at <date>`` snapshots the body before printing (verifies the CLI
    threads through to ``Body.at_time``).
"""

from typer.testing import CliRunner

from planetarypy.cli import app


runner = CliRunner()


class TestFieldForm:
    def test_value_to_stdout_source_to_stderr(self):
        result = runner.invoke(app, ["constants", "Mars.GM"])
        assert result.exit_code == 0
        assert "km3 / s2" in result.stdout
        # stderr carries the provenance lines (prefixed `# source:` /
        # `# reference:`); stdout must stay clean of them so consumers can
        # pipe the value into awk/jq/etc.
        assert "# source:" in result.stderr
        # GM is sourced from the JPL DE440 ephemeris, not the PCK.
        assert "gm_de440.tpc" in result.stderr
        assert "# source:" not in result.stdout

    def test_pck_field_source_is_the_pck(self):
        # Sibling check: a PCK-sourced field reports the PCK kernel.
        result = runner.invoke(app, ["constants", "Mars.radii"])
        assert result.exit_code == 0
        assert "pck00011.tpc" in result.stderr

    def test_case_insensitive_body_name(self):
        upper = runner.invoke(app, ["constants", "MARS.GM"]).stdout
        lower = runner.invoke(app, ["constants", "mars.GM"]).stdout
        title = runner.invoke(app, ["constants", "Mars.GM"]).stdout
        assert upper == lower == title

    def test_unknown_body_suggests_close_match(self):
        result = runner.invoke(app, ["constants", "jupier.GM"])
        assert result.exit_code == 1
        assert "Unknown body" in result.stderr
        assert "Jupiter" in result.stderr

    def test_unknown_field_suggests_close_match(self):
        result = runner.invoke(app, ["constants", "Mars.gravity"])
        assert result.exit_code == 1
        assert "no field 'gravity'" in result.stderr
        assert "surface_gravity" in result.stderr

    def test_at_time_snapshots_pck_edition(self):
        """Mars pole_dec moved ~1.5° between IAU 2009 and IAU 2015.
        --at 2012 must surface the older value via pck00010.tpc."""
        result = runner.invoke(app, ["constants", "Mars.pole_dec", "--at", "2012"])
        assert result.exit_code == 0
        assert result.stdout.startswith("52.8865")
        assert "pck00010.tpc" in result.stderr


class TestBodyForm:
    def test_table_contains_pck_and_nssdc_fields(self):
        result = runner.invoke(app, ["constants", "Mars"])
        assert result.exit_code == 0
        # Rich-rendered output; just check key field names appear.
        for field in ("GM", "radii", "bond_albedo", "semimajor_axis"):
            assert field in result.stdout
        # Sources surface in the third column.
        assert "pck00011.tpc" in result.stdout
        assert "NSSDC" in result.stdout

    def test_unknown_body_in_table_form_still_suggests(self):
        result = runner.invoke(app, ["constants", "marz"])
        assert result.exit_code == 1
        assert "Unknown body" in result.stderr
        assert "Mars" in result.stderr


class TestBareInvocation:
    """`plp constants` (no args) prints --help instead of erroring on the
    missing positional. Same UX as `plp` itself."""

    def test_no_args_prints_help_and_exits_zero(self):
        result = runner.invoke(app, ["constants"])
        assert result.exit_code == 0
        assert "Usage:" in result.stdout
        assert "constants" in result.stdout


class TestListForm:
    """`plp constants list` is a discovery surface over the body registry."""

    def test_bare_list_shows_category_menu(self):
        result = runner.invoke(app, ["constants", "list"])
        assert result.exit_code == 0
        for cat in ("planets", "moons", "asteroids", "comets",
                    "dwarf_planets", "mission_visited", "sun"):
            assert cat in result.stdout

    def test_category_lists_its_bodies(self):
        result = runner.invoke(app, ["constants", "list", "planets"])
        assert result.exit_code == 0
        for planet in ("Mars", "Jupiter", "Earth"):
            assert planet in result.stdout

    def test_moons_parent_restricts_to_that_planet(self):
        result = runner.invoke(app, ["constants", "list", "moons", "saturn"])
        assert result.exit_code == 0
        assert "Titan" in result.stdout
        assert "Enceladus" in result.stdout
        # A Mars moon must not leak into Saturn's list.
        assert "Phobos" not in result.stdout

    def test_dwarf_planets_is_flag_based_and_cross_class(self):
        result = runner.invoke(app, ["constants", "list", "dwarf_planets"])
        assert result.exit_code == 0
        assert "Pluto" in result.stdout
        assert "Ceres" in result.stdout

    def test_hyphenated_category_is_normalised(self):
        result = runner.invoke(app, ["constants", "list", "dwarf-planets"])
        assert result.exit_code == 0
        assert "Pluto" in result.stdout

    def test_unknown_category_suggests_close_match(self):
        result = runner.invoke(app, ["constants", "list", "moon"])
        assert result.exit_code == 1
        assert "Unknown category" in result.stderr
        assert "moons" in result.stderr

    def test_parent_on_non_moons_category_is_noted_and_ignored(self):
        result = runner.invoke(app, ["constants", "list", "planets", "earth"])
        assert result.exit_code == 0
        assert "only applies to 'moons'" in result.stderr
        assert "Mars" in result.stdout

    def test_list_does_not_shadow_a_body_query(self):
        # Back-compat guard: the bare body-query form is untouched.
        result = runner.invoke(app, ["constants", "Mars.GM"])
        assert result.exit_code == 0
        assert "km3 / s2" in result.stdout


class TestTabCompletion:
    """The CLI offers tab-completion on bodies *and* on per-body fields."""

    def setup_method(self):
        import click
        from planetarypy.cli import _complete_constants_query

        self._complete = _complete_constants_query
        self._ctx = click.Context(click.Command("x"))

    def test_body_prefix_completes_to_body_names(self):
        out = self._complete(self._ctx, [], "Mar")
        assert "Mars" in out

    def test_dot_lists_fields_for_that_body(self):
        out = self._complete(self._ctx, [], "Mars.")
        assert "Mars.GM" in out
        assert "Mars.bond_albedo" in out

    def test_field_prefix_narrows_completions(self):
        out = self._complete(self._ctx, [], "Mars.bo")
        assert out == ["Mars.bond_albedo"]

    def test_only_constant_bearing_fields_offered(self):
        """Metadata like ``body_class``, ``dwarf_planet``, ``naif_id`` are
        not valid arguments to ``plp constants`` — they must be filtered
        out so users don't get noise in the completion list."""
        out = self._complete(self._ctx, [], "Mars.")
        for noise in ("Mars.body_class", "Mars.naif_id", "Mars.dwarf_planet",
                      "Mars.parent", "Mars.mission_visited", "Mars.name"):
            assert noise not in out

    def test_completion_respects_None_fields(self):
        """Jupiter (gas giant) has no surface_* fields populated.
        Completion must reflect that — empty list rather than offering
        attribute names that resolve to None."""
        out = self._complete(self._ctx, [], "Jupiter.surface_")
        assert out == []

    def test_list_keyword_is_offered(self):
        out = self._complete(self._ctx, [], "li")
        assert "list" in out

    def test_category_completer_offers_categories(self):
        from planetarypy.cli import _complete_constants_category

        assert _complete_constants_category("m") == ["moons", "mission_visited"]
        assert _complete_constants_category("d") == ["dwarf_planets"]
