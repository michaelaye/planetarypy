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


# --- CONTRIBUTES manifest verification -------------------------------------
#
# The manifest exists because load success says nothing about whether a plugin
# did its job: planetarypy-ctx once registered its verbs while its package
# resolved to an empty namespace directory, so the storage resolver was never
# registered and nothing complained. These tests pin that down.


class _ManifestEP(_FakeEP):
    """An entry point whose manifest lives on a stub module in sys.modules."""

    def __init__(self, name, obj, manifest, module="fake_plugin_mod"):
        super().__init__(name, obj)
        self.module = module
        self._manifest = manifest

    def install(self, monkeypatch):
        import sys
        import types

        mod = types.ModuleType(self.module)
        if self._manifest is not None:
            mod.CONTRIBUTES = self._manifest
        monkeypatch.setitem(sys.modules, self.module, mod)
        return self


def _register_demo(app):
    @app.command("plugindemo")
    def _demo():
        typer.echo("hi")


def test_satisfied_manifest_is_silent(monkeypatch, capsys):
    ep = _ManifestEP("demo", _register_demo, {"commands": ["plugindemo"]})
    ep.install(monkeypatch)
    _patch_eps(monkeypatch, [ep])
    cli._load_cli_plugins(typer.Typer())
    assert capsys.readouterr().err == ""


def test_missing_command_is_reported(monkeypatch, capsys):
    ep = _ManifestEP("demo", _register_demo, {"commands": ["plugindemo", "ghost"]})
    ep.install(monkeypatch)
    _patch_eps(monkeypatch, [ep])
    cli._load_cli_plugins(typer.Typer())
    err = capsys.readouterr().err
    assert "ghost" in err and "never registered" in err
    assert "plugindemo" not in err  # the one that did land isn't complained about


def test_missing_storage_resolver_is_reported(monkeypatch, capsys):
    ep = _ManifestEP("demo", _register_demo, {"storage_resolvers": ["no.such.key"]})
    ep.install(monkeypatch)
    _patch_eps(monkeypatch, [ep])
    cli._load_cli_plugins(typer.Typer())
    err = capsys.readouterr().err
    assert "no.such.key" in err and "storage resolver" in err


def test_missing_meta_handler_is_reported(monkeypatch, capsys):
    ep = _ManifestEP("demo", _register_demo, {"meta_handlers": ["no.such.key"]})
    ep.install(monkeypatch)
    _patch_eps(monkeypatch, [ep])
    cli._load_cli_plugins(typer.Typer())
    err = capsys.readouterr().err
    assert "no.such.key" in err and "meta handler" in err


def test_registered_resolver_satisfies_manifest(monkeypatch, capsys):
    from planetarypy.catalog._resolver import _STORAGE_RESOLVERS

    monkeypatch.setitem(_STORAGE_RESOLVERS, "demo.key", lambda *a, **k: None)
    ep = _ManifestEP("demo", _register_demo, {"storage_resolvers": ["demo.key"]})
    ep.install(monkeypatch)
    _patch_eps(monkeypatch, [ep])
    cli._load_cli_plugins(typer.Typer())
    assert capsys.readouterr().err == ""


def test_plugin_without_manifest_is_silent(monkeypatch, capsys):
    ep = _ManifestEP("demo", _register_demo, None)
    ep.install(monkeypatch)
    _patch_eps(monkeypatch, [ep])
    cli._load_cli_plugins(typer.Typer())
    assert capsys.readouterr().err == ""


# --- help-panel seam --------------------------------------------------------


def test_plugin_commands_get_a_plugin_panel(monkeypatch):
    ep = _ManifestEP("demo", _register_demo, {"panel": "Demo"})
    ep.install(monkeypatch)
    _patch_eps(monkeypatch, [ep])
    app = typer.Typer()
    cli._load_cli_plugins(app)
    assert app.registered_commands[0].rich_help_panel == "Demo"


def test_plugin_panel_prefixes_rather_than_replaces(monkeypatch):
    """A plugin's own grouping survives inside its section of the help."""

    def register(app):
        @app.command("grouped", rich_help_panel="Fetch & download")
        def _g():
            pass

    ep = _ManifestEP("demo", register, {"panel": "Demo"})
    ep.install(monkeypatch)
    _patch_eps(monkeypatch, [ep])
    app = typer.Typer()
    cli._load_cli_plugins(app)
    assert app.registered_commands[0].rich_help_panel == "Demo · Fetch & download"


def test_panel_defaults_to_entry_point_name(monkeypatch):
    ep = _ManifestEP("nameless", _register_demo, {})
    ep.install(monkeypatch)
    _patch_eps(monkeypatch, [ep])
    app = typer.Typer()
    cli._load_cli_plugins(app)
    assert app.registered_commands[0].rich_help_panel == "nameless"


def test_core_commands_keep_their_panel(monkeypatch):
    """Re-panelling must touch only what the plugin just added."""
    app = typer.Typer()

    @app.command("corecmd", rich_help_panel="Inspect a product")
    def _core():
        pass

    ep = _ManifestEP("demo", _register_demo, {"panel": "Demo"})
    ep.install(monkeypatch)
    _patch_eps(monkeypatch, [ep])
    cli._load_cli_plugins(app)
    panels = {c.name: c.rich_help_panel for c in app.registered_commands}
    assert panels["corecmd"] == "Inspect a product"
    assert panels["plugindemo"] == "Demo"
