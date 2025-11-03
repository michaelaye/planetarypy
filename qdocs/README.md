# Quarto + quartodoc docs for PlanetaryPy

This folder contains an experimental Quarto documentation site that uses [quartodoc](https://github.com/machow/quartodoc) to generate API reference pages from Python docstrings. It's separate from the existing `docs/` to avoid disrupting current documentation workflows.

## Prerequisites

1) Install Quarto (outside Python):
   - macOS: download from https://quarto.org/docs/get-started/
   - Or via Homebrew: `brew install quarto`

2) Install quartodoc (Python):

```fish
# from the project root (or any virtualenv)
pip install quartodoc
 # optional but recommended so quartodoc can resolve imports cleanly
 pip install -e .
```

## Build and preview

Run the following from this `qdocs/` folder:

```fish
# 1) Generate API pages under qdocs/reference/
quartodoc build

# 2) Preview the Quarto website (auto-reloads on content changes)
quarto preview

# Optional: render the site to qdocs/_site without starting a server
quarto render
```

For auto-regeneration of API pages while editing Python docstrings, you can also watch for changes in another terminal:

```fish
quartodoc build --watch
```

## Configuration

- `_quarto.yml` configures the Quarto website and the `quartodoc` section.
- API pages are written to `reference/` with a generated `reference/index.qmd`.
- The navbar includes a link to the API Reference.

## Notes

- You can safely delete the generated `reference/` and `_site/` directories; they are build artifacts.
- If you add new modules or subpackages, update the `sections:` list in `_quarto.yml` so they appear in the API reference.
