"""CLI tests for ``plp search at`` — mock the search API layer (no network)."""

import pandas as pd
import pytest
from typer.testing import CliRunner

from planetarypy import cli
from planetarypy import search as search_mod

runner = CliRunner()


def test_search_at_help_on_missing():
    result = runner.invoke(cli.app, ["search", "at"])
    assert result.exit_code == 0
    assert "Usage:" in result.stdout


def test_search_at_count(monkeypatch):
    monkeypatch.setattr(search_mod, "count", lambda **kw: 42)
    result = runner.invoke(cli.app, ["search", "at", "Mars", "77.4", "18.4", "--count"])
    assert result.exit_code == 0
    assert "42" in result.stdout


def test_search_at_maps_body_and_builds_bbox(monkeypatch):
    captured = {}
    monkeypatch.setattr(search_mod, "count", lambda **kw: 2)

    def fake_search(**kw):
        captured.update(kw)
        return pd.DataFrame(
            {
                "ref_lid_instrument": ["urn:nasa:pds:context:instrument:tgo.cassis"],
                "pds:Time_Coordinates.pds:start_date_time": ["2020-04-23T17:38:25Z"],
            },
            index=pd.Index(["urn:esa:psa:em16_tgo_cas:x::1.0"], name="lidvid"),
        )

    monkeypatch.setattr(search_mod, "search_products", fake_search)
    result = runner.invoke(cli.app, ["search", "at", "Mars", "77.4", "18.4"])
    assert result.exit_code == 0
    # plain body name → target LID; default radius 1° → bbox; observationals forced
    assert captured["target"] == "urn:nasa:pds:context:target:planet.mars"
    assert captured["observationals"] is True
    assert captured["bbox"] == pytest.approx((76.4, 17.4, 78.4, 19.4))
    assert "cassis" in result.stdout  # short instrument form in the table


def test_search_at_full_lid_passthrough(monkeypatch):
    captured = {}
    monkeypatch.setattr(search_mod, "count", lambda **kw: captured.update(kw) or 0)
    result = runner.invoke(
        cli.app,
        ["search", "at", "urn:nasa:pds:context:target:planet.mercury", "0", "0", "--count"],
    )
    assert result.exit_code == 0
    assert captured["target"] == "urn:nasa:pds:context:target:planet.mercury"


def test_search_at_negative_coords(monkeypatch):
    captured = {}
    monkeypatch.setattr(search_mod, "count", lambda **kw: captured.update(kw) or 0)
    # negative lon AND lat must parse as positionals, not be mistaken for flags
    result = runner.invoke(cli.app, ["search", "at", "Mars", "-120", "-85", "--count"])
    assert result.exit_code == 0
    assert captured["bbox"] == pytest.approx((-121.0, -86.0, -119.0, -84.0))
