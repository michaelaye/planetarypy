"""Tests for the `plp` CLI plugin entry-point loader."""

import importlib.metadata as md

import typer

from planetarypy import cli


class _FakeEP:
    def __init__(self, name, obj):
        self.name = name
        self._obj = obj

    def load(self):
        return self._obj


def _patch_eps(monkeypatch, eps):
    monkeypatch.setattr(
        md, "entry_points",
        lambda group=None: eps if group == "planetarypy.cli_plugins" else [],
    )


def test_plugin_register_adds_command(monkeypatch):
    def register(app):
        @app.command("plugindemo")
        def _demo():
            typer.echo("hi")

    _patch_eps(monkeypatch, [_FakeEP("demo", register)])
    test_app = typer.Typer()
    cli._load_cli_plugins(test_app)
    names = [c.name for c in test_app.registered_commands]
    assert "plugindemo" in names


def test_plugin_can_add_subapp(monkeypatch):
    def register(app):
        sub = typer.Typer()

        @sub.command("ping")
        def _ping():
            typer.echo("pong")

        app.add_typer(sub, name="demoinstr")

    _patch_eps(monkeypatch, [_FakeEP("demo", register)])
    test_app = typer.Typer()
    cli._load_cli_plugins(test_app)
    assert any(g.name == "demoinstr" for g in test_app.registered_groups)


def test_broken_plugin_is_skipped(monkeypatch, capsys):
    class _BadEP:
        name = "bad"

        def load(self):
            raise RuntimeError("boom")

    _patch_eps(monkeypatch, [_BadEP()])
    test_app = typer.Typer()
    cli._load_cli_plugins(test_app)  # must not raise
    assert "bad" in capsys.readouterr().err


def test_no_plugins_is_noop(monkeypatch):
    _patch_eps(monkeypatch, [])
    cli._load_cli_plugins(typer.Typer())  # no error, nothing registered
