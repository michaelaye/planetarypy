"""Tests for ``plp hitif`` — HiRISE JP2 to IAU-referenced GeoTIFF.

The API layer is mocked; nothing here decodes a JP2 or shells out to
rio / kdu_expand.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from planetarypy.cli import app

runner = CliRunner()

_API = "planetarypy.instruments.mro.hirise.jp2_to_geotiff"


class TestHitifCli:

    def test_bare_invocation_prints_help(self):
        result = runner.invoke(app, ["hitif"])
        assert result.exit_code == 0
        assert "Usage:" in result.stdout

    def test_forwards_input_and_echoes_output_path(self):
        with patch(_API, return_value=Path("out.tif")) as api:
            result = runner.invoke(app, ["hitif", "in.JP2"])
        assert result.exit_code == 0
        assert "out.tif" in result.stdout
        assert api.call_args.args == ("in.JP2", None)

    def test_forwards_explicit_output(self):
        with patch(_API, return_value=Path("custom.tif")) as api:
            result = runner.invoke(app, ["hitif", "in.JP2", "custom.tif"])
        assert result.exit_code == 0
        assert api.call_args.args == ("in.JP2", "custom.tif")

    def test_defaults_match_measured_best_settings(self):
        with patch(_API, return_value=Path("out.tif")) as api:
            runner.invoke(app, ["hitif", "in.JP2"])
        kwargs = api.call_args.kwargs
        assert kwargs["compress"] == "deflate"
        assert kwargs["blocksize"] == 1024
        assert kwargs["iau_code"] is None
        assert kwargs["overviews"] is True
        assert kwargs["echo"] is True

    def test_quiet_disables_command_echo(self):
        with patch(_API, return_value=Path("out.tif")) as api:
            runner.invoke(app, ["hitif", "in.JP2", "--quiet"])
        assert api.call_args.kwargs["echo"] is False

    def test_flags_are_forwarded(self):
        with patch(_API, return_value=Path("out.tif")) as api:
            runner.invoke(
                app,
                ["hitif", "in.JP2", "--iau-code", "49935", "--overviews",
                 "--blocksize", "512", "--compress", "zstd", "--threads", "2", "-f"],
            )
        kwargs = api.call_args.kwargs
        assert kwargs["iau_code"] == 49935
        assert kwargs["overviews"] is True
        assert kwargs["blocksize"] == 512
        assert kwargs["compress"] == "zstd"
        assert kwargs["threads"] == 2
        assert kwargs["overwrite"] is True

    def test_api_error_exits_nonzero(self):
        with patch(_API, side_effect=FileExistsError("out.tif exists")):
            result = runner.invoke(app, ["hitif", "in.JP2"])
        assert result.exit_code == 1
        assert "out.tif exists" in result.output

    def test_no_overviews_opts_out(self):
        with patch(_API, return_value=Path("out.tif")) as api:
            runner.invoke(app, ["hitif", "in.JP2", "--no-overviews"])
        assert api.call_args.kwargs["overviews"] is False

    def test_nodata_defaults_to_zero(self):
        with patch(_API, return_value=Path("out.tif")) as api:
            runner.invoke(app, ["hitif", "in.JP2"])
        assert api.call_args.kwargs["nodata"] == 0.0

    def test_no_nodata_leaves_it_unset(self):
        with patch(_API, return_value=Path("out.tif")) as api:
            runner.invoke(app, ["hitif", "in.JP2", "--no-nodata"])
        assert api.call_args.kwargs["nodata"] is None

    def test_explicit_nodata_forwarded(self):
        with patch(_API, return_value=Path("out.tif")) as api:
            runner.invoke(app, ["hitif", "in.JP2", "--nodata", "65535"])
        assert api.call_args.kwargs["nodata"] == 65535.0
