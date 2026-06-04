# CLAUDE.md

Guidance for AI coding agents (Claude Code, Cursor with Claude, Copilot Chat, etc.) working in the planetarypy repository.

This file is the **working agreement** between AI agents and this codebase. It carries two kinds of guidance, both load-bearing:

- **Project knowledge** — where things live, how subsystems compose, which conventions are domain-specific. Skip this and you'll re-invent things that already exist.
- **Development principles** — how to make changes that survive review and don't drift the codebase. Skip these and contributions need rewriting.

The goal: any AI agent picking this up should produce contributions of the same quality, in the same style, as if they'd been working on the codebase for months. Read it before your first non-trivial edit.

---

## Build and Test

```bash
pip install -e ".[dev]"          # dev install
pip install -e ".[dev,spice]"    # with SPICE extras

pytest                                            # all tests (parallel, coverage, -xvs)
pytest tests/test_config.py -xvs                  # one file
pytest tests/test_config.py::test_name -xvs       # one test
pytest tests/test_config.py -xvs -p no:xdist      # serial (for debugging)
pytest -m "not slow"                              # skip slow

flake8 src/planetarypy tests                      # lint
```

Default pytest opts (pyproject): `-xvs --import-mode=importlib --cov=planetarypy -n auto`.

## Source Layout

`src/` layout, hatchling build. Tests in `tests/`. Docs in `docs/` (Quarto + quartodoc).

Top-level under `src/planetarypy/`:

| Module | Role |
|---|---|
| `config.py` | Singleton TOML config (`from planetarypy.config import config`) |
| `pds/` | PDS3 index download, parse, parquet cache |
| `catalog/` | DuckDB catalog of all PDS instruments/products (3-tier URL resolver) |
| `spice/` | SPICE kernel mgmt + `spicer.py` illumination calculator |
| `instruments/` | Mission-specific handlers (mro/, go/) |
| `constants/` | IAU 2009/2015 rotation/shape constants + NSSDC fact sheets |
| `isis/` | ISIS3 helpers (autoseed, projected) |
| `geo.py` | Coordinate transforms, `Point` class, IAU CRS resolution |
| `plotting.py` | `imshow_gray`, `add_sun_indicator`, `imshow_with_sun` |
| `cli.py` | Unified `plp` Typer CLI |
| `datetime_format_converters.py`, `utils.py` | Shared helpers |

## Configuration System

Two user-level TOML files, auto-managed:
- `~/.planetarypy_config.toml` — general; primarily `storage_root` (default `~/planetarypy_data/`). Override path with `PLANETARYPY_CONFIG` env var. Holds knobs like `filter_deprecation_warnings`, `max_table_rows`.
- `~/.planetarypy_index_urls.toml` — PDS index URLs, auto-fetched from the `planetarypy/planetarypy_configs` GitHub repo.

The config-handler pattern (lazy first-fetch, daily-check-via-AccessLog, force-refresh) is implemented in `pds/static_index.py:ConfigHandler` and is the template for any new remote-refreshed config.

## PDS Index System (`pds/`)

**Dotted key convention:** `{mission}.{instrument}.{indexname}` — e.g. `mro.ctx.edr`, `cassini.iss.ring_summary`, `go.ssi.raw`.

**Two handler types (composition):**
1. `StaticRemoteHandler` — fixed URLs from config (Cassini, Galileo, …).
2. `DynamicRemoteHandler` — URLs discovered by scraping. Handler classes registered in `DYNAMIC_URL_HANDLERS` in `dynamic_index.py` (`CTXIndex`, `LROCIndex`, `LAMPEDRIndex`, `LAMPRDRIndex` live in `dynamic_url_handlers.py`).

**Flow:** `Index(key)` → static/dynamic dispatch → download `.lbl` + `.tab` → `IndexLabel` parses PDS3 label → convert to parquet → cache at `{storage_root}/{mission}/{instrument}/indexes/{indexname}/`.

**`get_index(key, ...)`** (`pds/__init__.py`) is the user-facing reader. Supports row filtering via `pids=` (uses `pid_column(key, df)` to resolve the right column) and column projection via `columns=`. `missing_pids(df, key, pids)` returns the diff.

**`read_pids_file(source, *, index_key=None, pid_key=None, suffix=None)`** in `pds/__init__.py` is the canonical entry point for the `--pids-from` CLI option. Dispatch rule:

1. `pid_key` set → CSV mode (the explicit "this is tabular" signal).
2. File with `.csv` extension → CSV mode.
3. Stdin (`"-"`) whose first non-blank line contains a comma → CSV mode (small heuristic so `head file.csv | plp fetch ...` works without an explicit flag).
4. Otherwise → plain text via `planetarypy.utils.read_pids`.

Other notable files: `index_main.py` (entry `Index`), `index_logging.py` (`AccessLog` — atomic `log_remote_check` writes both `remote_timestamp` and `last_checked`), `index_fixes.py` (per-mission patches), `meta_display.py`.

## PDS Catalog (`catalog/`)

DuckDB-backed catalog of every known PDS instrument + product, built from the MillionConcepts `pdr-tests` repo. Surface API in `catalog/__init__.py`: `build_catalog`, `list_missions`, `list_instruments`, `list_products`, `example_products`, `ambiguous_mappings`, `search`, `fetch_product`, `fetch_products`, `BatchFetchResult`, `OfflineError`, `get_product_urls`.

**3-tier URL resolution** (in order):
1. `_resolver.py` — explicit pattern map
2. `_index_resolver.py` — derive from PDS index entry
3. `_pattern_resolver.py` — last-resort heuristic

Supporting files: `_mission_map.py`, `_parser.py`, `_repo.py`, `_schema.py`, `_url_rewrite.py`, `_validation.py`, `_objects.py`.

**`fetch_products(key, product_ids, ...)`** is the batch wrapper around `fetch_product`. Uses `planetarypy.utils.parallel_map` under the hood. Calls `have_internet()` upfront and raises `OfflineError` on preflight failure; `skip_online_check=True` bypasses. Returns `list[BatchFetchResult]` with `.product_id` / `.downloaded` / `.exception` / `.ok` fields per PID.

## SPICE (`spice/`) — optional `[spice]` extra

- `generic_kernels.py` — LSK, PCK
- `archived_kernels.py` — mission-specific kernels
- `pckernels.py` — planetary constants kernels
- `spicer.py` — **`Spicer` illumination calculator**, primary user-facing API
  - `Spicer(body).illumination(lon, lat, time)` — full illumination angles
  - `Spicer(body).solar_azimuth_at(lon, lat, time)` — CW-from-north sun azimuth
  - `Spicer(body).north_azimuth_at(lon, lat, time)` — image-plane north arrow
- `config.py` — SPICE-specific paths
- Kernels cached under `{storage_root}/spice_kernels/`

## Instruments (`instruments/`)

- `mro/ctx/` — `ctx_edr.py`, `ctx_calib.py`. The latter's `process_parallel` is a thin shim over `planetarypy.utils.parallel_map` (kept for backward compatibility).
- `mro/hirise.py` — `get_browse`, `get_metadata`, `sun_azimuth_from_top` (label-cheat: reads `SUB_SOLAR_AZIMUTH` + `NORTH_AZIMUTH` from the RDR cumulative index and converts the HiRISE CW-from-3-o'clock convention).
- `go/ssi.py` — Galileo SSI.

A duplicate of the `process_parallel` pattern still lives in `isis/projected.py`; treat it as out-of-scope debt until a real refactor cycle picks it up.

## Constants (`constants/`)

PCK polynomial fields + JPL DE-series GMs + NSSDC fact-sheet values composed into a single `Body` object per target. The composition happens at import time in `constants/__init__.py`.

Subsystems:
- `iau2009.py`, `iau2015.py` — PCK polynomial fields, generated from NAIF kernels.
- `_gm_jpl.py` — JPL DE440 GMs.
- `nssdc/` — NSSDC fact-sheet archive (parsed JSON + raw HTML captures). The `_loader.py` lazy-fetches `parsed_archive.json.gz` from Zenodo on first use (see Release Process → NSSDC Zenodo Dataset Updates below).
- `base.py` — `Constant` class, `Range` / `RangeWarning` for uncertain or naturally-varying quantities.

## CLI (`cli.py`) — `plp` entry point

Typer app with sub-apps grouped by Rich help panel:
- `plp catalog …` (Discovery), `plp indexes …` (Discovery), `plp spice …` (Discovery)
- Top-level commands by panel: Fetch & download, Visualize, Inspect a product, Science computations, Maintenance (`plp ctx-migrate`)

**Design philosophy: API first, CLI wraps thin.** Every `plp` verb is a thin wrapper over a public Python API. Build and test the library function first; the CLI command then forwards arguments and formats output. Useful logic — parsing PID lists, building catalogs, batching downloads, filtering indexes by PIDs, parallel execution — lives in `planetarypy.*` modules, not under `cli.py`. The reason is reuse: notebooks and downstream tooling should pick up new capabilities without screen-scraping or shelling out to `plp`. If you find substantial logic inside `cli.py`, that's a bug — factor it down to the API layer.

This is also documented (with a user-facing framing) in `docs/howto/cli.qmd`.

## Conventions

### Code style
- Line length 88 (Black-compatible); flake8 ignoring E203, E701.
- Python ≥ 3.11. Type hints throughout.
- No f-strings without substitutions (linter rule).
- Logging: loguru, **disabled by default** (library convention). Enable via `logger.enable("planetarypy")` or `planetarypy.enable_logging("DEBUG")`.

### Domain conventions
- **IAU 2015 CRS codes** preferred over raw proj4 strings.
- **HiRISE azimuths** (`SUB_SOLAR_AZIMUTH`, `NORTH_AZIMUTH`) are CW from 3 o'clock. Convert to CW-from-top with `(az + 90) % 360`.
- **SPICE azimuth convention**: `Spicer.solar_azimuth_at` returns CW-from-north (geographic). The plotting helper `add_sun_indicator` expects CW-from-top (PDS unprojected convention).

### CLI design patterns

These patterns are uniform across the existing `plp` verbs; new commands should follow them:

- **Help-on-missing default arg.** Every CLI command's primary positional argument defaults to `None`. When the user invokes the bare verb (no args), the function prints help and exits 0 — never typer's default "Missing argument 'X'" error. Pattern:
  ```python
  @app.command(...)
  def myverb(
      ctx: typer.Context,
      key: str = typer.Argument(None, help="..."),
      ...
  ):
      if key is None:
          typer.echo(ctx.get_help())
          raise typer.Exit()
      ...
  ```
  Add a regression test that asserts `result.exit_code == 0` and `"Usage:" in result.stdout` for the bare invocation.

- **Dual-idiom flags for list-valued options.** Options that accept a list of strings or ints support **both** comma-separated and repeated-flag forms (and any mix). Implementation: declare the option as `list[str]` (Typer's `multiple=True` is implicit) and pass through a parser that splits on commas and strips whitespace. See `_parse_columns` and `_parse_ccds` in `cli.py`.
  ```bash
  # All equivalent:
  --columns "PRODUCT_ID,IMAGE_TIME"
  -c PRODUCT_ID -c IMAGE_TIME
  -c PRODUCT_ID -c "IMAGE_TIME,EMISSION_ANGLE"
  ```

- **Variadic positionals for "main inputs".** When a command's primary subject is a list of identifiers (PIDs, obsids), use variadic positionals: `plp fetch KEY PID1 PID2 PID3`. Use `--pids-from PATH` for file input.

- **`--pids-from / --pid-key / --pid-suffix` family.** Any CLI verb that operates on a batch of PIDs offers the same three flags:
  - `--pids-from PATH | -` (file or stdin; CSV auto-detected via comma sniff or explicit `--pid-key`).
  - `--pid-key NAME` (CSV column name; forces CSV parsing of stdin/.txt).
  - `--pid-suffix STR` (appended to each PID read from `--pids-from`; **not applied to positional PIDs** — hand-typed PIDs are passed through verbatim because the user owns the full string).

- **Batch report format.** For batch-of-PIDs verbs, `--report errors-only|full|jsonl|csv`. Use the `_emit_batch_report` helper in `cli.py` so output stays consistent. FAIL blocks are multi-line, indented, with middle-ellipsis PID truncation at 60 chars; padded with leading/trailing blank lines so the report stays visually quarantined from any tqdm progress bar that came before.

### Testing conventions
- Optional dependencies: `pytest.importorskip("module")` at the top of test files that need them. See `tests/test_ctx_calib_shim.py` (importorskip on hvplot), `tests/test_spicer.py` (spiceypy), etc.
- Network-dependent tests: `@pytest.mark.slow`. CI runs `pytest -m "not slow"`. Tests that hit external servers (NAIF, PDS, etc.) belong here so transient failures don't block PRs.
- CLI tests use `typer.testing.CliRunner` and **mock the API layer** (e.g. `monkeypatch.setattr(catalog_mod, "fetch_product", ...)`). They should not run a subprocess or hit the network.
- ProcessPoolExecutor tests must skip under pytest-xdist (nested process pools don't survive worker fork/spawn). Pattern: `@pytest.mark.skipif("PYTEST_XDIST_WORKER" in os.environ, reason="...")`.
- Test class organization: group related tests in `class TestFoo:`. One test method per concrete scenario.
- **The dev-environment kitchen-sink problem.** Running pytest in a dev environment that has every dep installed (core + spice + isis + plotting tools) **cannot** validate the declared-dependency contract. Real users `pip install planetarypy` without those extras and hit `ModuleNotFoundError` on imports that worked locally. CI's `minimal-install` job (in `.github/workflows/test.yaml`) catches this by creating a fresh venv, `pip install .` with no extras, and exercising every public submodule + CLI verb. **Before changing anything in `[project.dependencies]` or `[project.optional-dependencies]`, mentally run that job first**: does my change break the no-extras smoke?

---

## Development Principles

These apply to every change. They're roughly in priority order — earlier ones override later ones in tension.

### Surgical fixes only

When debugging or fixing a problem, make precise, targeted corrections. Don't delete or rewrite existing components on the assumption that removing them solves the issue — those components were built for reasons that may not be visible in the surrounding code. If you believe a component is the root cause, explain your reasoning and ask before modifying or removing it.

Bad: hook throws → remove the hook. Build fails → delete and rewrite the config. Feature broken → rip out the module.
Correct: hook throws → read the hook, trace the error, fix the specific line. Build fails → read the error, fix the specific issue. Feature broken → isolate the defect, patch it surgically.

### Verify before asserting

Never claim something *is* a certain way unless you've verified it with your own tools. This applies to file contents, build results, image appearance, deployment status, EVERYTHING. If you haven't looked, you don't know — say so. After making changes, verify the result before claiming success. "Done!" or "It's X" without proof is not good enough.

### Don't add what the task doesn't need

Don't add features, refactor, or introduce abstractions beyond what the task requires. Three similar lines is better than a premature abstraction. A bug fix doesn't need surrounding cleanup; a one-shot operation doesn't need a helper. No half-finished implementations either — finish what you start or don't start.

Don't add error handling, fallbacks, or validation for scenarios that can't happen. Trust internal code and framework guarantees. Only validate at system boundaries (user input, external APIs). Don't use feature flags or backward-compatibility shims when you can just change the code.

### Default to writing no comments

Only add a comment when the WHY is non-obvious: a hidden constraint, a subtle invariant, a workaround for a specific bug, behavior that would surprise a reader. If removing the comment wouldn't confuse a future reader, don't write it. Don't explain WHAT the code does (well-named identifiers already do that). Don't reference the current task, fix, or callers ("used by X", "added for the Y flow", "handles the case from issue #123") — those belong in commit messages, not code.

### Don't auto-commit

Wait for the user to explicitly approve a commit. Reviewing the diff in their head is part of how they understand the change. Surfacing a ready-to-commit state and asking is better than committing and asking forgiveness.

Exception: a release cycle (when the user has explicitly invoked it) follows the documented Release Process and creates several commits along the way as part of that workflow.

### Scope-expansion is OK when announced

If you find a sibling-file bug or obvious cleanup adjacent to the work you're doing, do the cleanup but **announce it clearly** in your summary so the user can review the full scope. Don't bundle silently. Don't bundle work that should be a separate commit ("A, then B for later" requests should produce two commits, not one).

### Semver bump type

Any commit adding **API surface** — a new CLI verb, a new exported function, a new keyword on an existing function, a new config key — is **MINOR**, not PATCH. PATCH is for pure bug fixes that don't add surface. MAJOR is for breaking changes. This is a recurring failure mode for AI agents working on this codebase: the default impulse is to call everything PATCH; force yourself to look at the diff and ask "did I add any name a downstream user could reach?" If yes → MINOR.

### Partial answers ≠ full agreement

When you've posed multiple decision questions and the user answers only some, the silence on the others is **not** approval of your stated lean. Re-ask the unanswered ones — concisely, but explicitly — before declaring a plan locked or starting implementation. "Locked. Building now" on a partial answer is a known failure mode.

### Help-on-missing for every new plp verb

Every new `plp` command's primary positional argument defaults to `None`; missing → print help, exit 0 (not typer's auto "Missing argument" error). See CLI Design Patterns above for the implementation pattern. Bake this in on the first pass — don't wait to be reminded.

### State-machine bugs need explicit search

When reviewing or red-teaming code (especially in `pds/`, `catalog/`, `spice/`, anywhere with `__init__`-time state writes and timestamp-gated property reads): trace state writes across `__init__` → method side-effects → subsequent property reads. Function-by-function review doesn't catch bugs that span the lifecycle. Specific anti-patterns:

- **Getters / loggers that mutate multiple pieces of state.** A method named `log_X` that also writes `Y` is a smell — the caller at the use site doesn't see Y being written.
- **Tests that codify the wrong behavior.** Green tests are evidence that the author agreed with the code, not that the code is correct. Always ask: what scenarios does this test *not* cover?
- **Variable names that suggest the wrong mental model.** `should_check` reads as "should I poll the remote?" when it actually controls "should I run the comparison?" — those are different things.

When the user invokes a red team explicitly: include "find state-machine bugs where init writes state that silences a later read" as one of the explicit prompts.

---

## Release Process

Triggered by "do a release cycle":

1. CHANGELOG.md entry (date `YYYY-MM-DD`, prose bullets matching prior style).
2. Stage feature work; commit with a descriptive message.
3. `bump-my-version bump {patch|minor|major}` (auto-commits + tags). Pick MINOR if any API surface was added — see the semver rule above.
4. `git push && git push --tags`
5. **Gate: wait for the tag-push CI run to complete and pass.** PyPI + conda publishing must NOT happen against a red CI. Poll loop:
   ```bash
   TAG=v$(grep '^current_version' pyproject.toml | cut -d'"' -f2)
   until [ "$(gh run list --repo michaelaye/planetarypy --branch "$TAG" \
              --workflow='Run software tests' --limit 1 \
              --json status -q '.[0].status')" = "completed" ]; do
     sleep 30
   done
   CONCLUSION=$(gh run list --repo michaelaye/planetarypy --branch "$TAG" \
                --workflow='Run software tests' --limit 1 \
                --json conclusion -q '.[0].conclusion')
   test "$CONCLUSION" = "success" \
     || { echo "Tag-push CI failed; aborting release. Investigate, then retry."; exit 1; }
   ```
   If CI failed because of a transient external server (NAIF, PDS, etc.), the prefetch retries in `.github/workflows/test.yaml` should catch most cases; if a tag run still fails on a flake, manually rerun the workflow from the GH UI and re-poll. If it's a real test failure, fix it on `main`, delete the tag, re-bump.
6. `rm -rf dist/ && python -m build && python -m twine check dist/* && python -m twine upload dist/*`
7. **Create the GitHub Release** for the tag, using the CHANGELOG entry as the release notes (so the Releases tab stays in sync with PyPI + conda; without this the tag exists in git but no Release object appears in the GH UI):
   ```bash
   V=$(grep '^current_version' pyproject.toml | cut -d'"' -f2)
   awk -v ver="$V" '$0 ~ "^## \\[" ver "\\]" {f=1; next} /^## \[/{if(f)exit} f' CHANGELOG.md > /tmp/release_notes.md
   gh release create "v$V" \
     --repo michaelaye/planetarypy \
     --title "v$V — <short subject from the CHANGELOG opening paragraph>" \
     --notes-file /tmp/release_notes.md
   ```
8. Update `conda/meta.yaml`:
   - Bump `{% set version = "X.Y.Z" %}`.
   - Replace `sha256:` with the new sdist hash:
     `curl -s https://pypi.org/pypi/planetarypy/X.Y.Z/json | jq -r '.urls[] | select(.packagetype=="sdist") | .digests.sha256'`
9. `conda-build conda/ --output-folder /tmp/conda-output --no-anaconda-upload` (recipe is edited in place — no grayskull regeneration).
10. `anaconda --site anaconda upload /tmp/conda-output/noarch/planetarypy-X.Y.Z-*.conda` (glob the build string — hash suffix varies).

**Notes:**
- `--site anaconda` required (bypasses an interactive prompt that fails in non-TTY).
- `grayskull`, `conda-build`, `anaconda-client` live in the `py314` conda env.
- Dep name mapping: `duckdb` (pip) → `python-duckdb` (conda-forge); others 1:1.
- `noarch: python` — one build for all platforms.
- The CI gate at step 5 deliberately waits on the **tag** ref, not `main`. The same code can produce different results because external prefetch hits flaky upstreams; only the tag's run represents the artifact we're about to publish.
- **Four publishing surfaces — git tag, PyPI, anaconda.org, GitHub Releases — are one indivisible release.** None is "done" until all four are. If a release cycle is interrupted between `twine upload` and `gh release create`, treat it as incomplete; do the missing step retroactively before considering the cycle closed.

### NSSDC Zenodo Dataset Updates

The parsed NSSDC archive (`src/planetarypy/constants/nssdc/parsed_archive.json.gz`) is **not** shipped in the PyPI/conda wheel — `pyproject.toml`'s `[tool.hatch.build] include` whitelist excludes it. End users get the file via lazy download from Zenodo (concept DOI `10.5281/zenodo.20122986`) the first time they touch `planetarypy.constants.nssdc`; subsequent calls hit a local cache under `{storage_root}/constants/nssdc/`.

The cache filename is `parsed_archive_v{EXPECTED_ARCHIVE_VERSION}_z{ZENODO_RECORD_ID}.json.gz`. Both knobs live in `src/planetarypy/constants/nssdc/_loader.py`. Bumping either constant in a release invalidates existing caches and forces re-download.

When NSSDC parser changes regenerate the archive:

1. `python scripts/parse_nssdc_archive.py` — regenerate `parsed_archive.json.gz`.
2. `python scripts/build_nssdc_zenodo_bundle.py` — assemble `build/nssdc_archive_v<N>/`.
3. Upload to Zenodo as a **new version** of the existing deposit. Note the new per-version record ID Zenodo assigns.
4. Update `ZENODO_RECORD_ID` in `_loader.py` to the new ID (and the comment's DOI line).
5. Cut a planetarypy release (above) shipping the bumped constant — without a release, end users keep using their cached (old-ID) copy.

`EXPECTED_ARCHIVE_VERSION` / `ARCHIVE_VERSION` (`parse_nssdc_archive.py`) is independent: bump only on **breaking** JSON shape changes (field rename, removal, type change). Additive optional keys do not require a schema bump.

---

## When in doubt

Read the existing surrounding code and match its style. Read the existing tests for the area you're touching — they encode the contract. When you can't resolve a question from those alone, ask the human; don't guess.
