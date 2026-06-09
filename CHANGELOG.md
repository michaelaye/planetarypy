# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.68.3] - 2026-06-09

A one-bug patch. `plp spice missions` and `plp spice info` crashed for real users on a schedule — and the test suite couldn't see it because the cache was laundering the bug.

### Fixed

- **`plp spice missions` / `info` crashed with `ValueError: Unknown format code 'g' for object of type 'str'` whenever the once-per-day datasets cache refreshed.** `get_datasets()` parses the NAIF archive table with `pandas.read_html`, which yields every cell as a string — including `Data Size (GB)`. The CLI formats that column with `f"{...:g}"`, which only works on a number. The bug was invisible in normal use and in CI because the parsed table is cached to CSV, and the CSV round-trip silently coerces the column back to float; every process *after* a refresh read the float-typed cache and worked, while the one process that *did* the refresh held string-typed data and crashed. Fixed at the source: `get_datasets()` now coerces `Data Size (GB)` to numeric right after parsing, so the fresh-`read_html` path and the cached-CSV path return identical dtypes. A deterministic, offline regression test (`test_fresh_parse_coerces_data_size_to_numeric`) mocks `read_html` with string cells and pins the dtype contract — it fails against the pre-fix code.

## [0.68.2] - 2026-06-04

A "fix the install contract" release. Every change is shaped like a bug fix: the package declares what it actually needs, ships the docs to use it, and surfaces clear errors when something's missing. No new public-API capability — the heavy modules (ctx_calib, isis/projected) have always required geopandas + hvplot + kalasiris; this release just makes that fact visible to pip, deptry, and the user.

### Fixed

- **Undeclared runtime deps `geopandas` and `hvplot` are now properly declared.** Both modules `ctx_calib.py` and `isis/projected.py` did top-level `import geopandas as gpd` / `import hvplot.pandas` without those packages being in any dependency list. Users on a fresh `pip install planetarypy` hit a `ModuleNotFoundError` the moment they imported the CTX or ISIS submodules. The dev-env-kitchen-sink local tests couldn't see this because every dev already had those packages installed for other work.
- **The kalasiris try/except in ctx_calib.py and isis/projected.py was too narrow.** It caught `KeyError` (ISIS env vars not set) but not `ImportError` (kalasiris itself not installed). On a fresh install without the new `[isis]` extra, that meant a hard `ModuleNotFoundError` instead of the warning the code was trying to emit. Widened to `(KeyError, ImportError)` matching the pattern in `utils.py:18`.
- **`utils.catch_isis_error` silently returned `None` when ISIS wasn't available.** The decorated function then surfaced as `TypeError: 'NoneType' object is not callable` to the user, with no indication that the `[isis]` extra was the actual missing thing. Now raises a clear `ImportError` pointing at `pip install "planetarypy[isis]"` and the new ISIS docs page (the previous silent-None was the worst of both worlds).
- **`ctx_edr.py` crashed at import on a fresh system** because it did `with CONFIGPATH.open() as f` without checking whether `~/.planetarypy_mro_ctx.toml` existed. Mirrored the auto-create pattern used by `planetarypy.config.Config`: a sane default is written on first import (PDS public URL for downloads, empty mirror/local paths that fall back to `{storage_root}/mro/ctx`), users edit afterwards.
- **`ssi.py` had a stale import** — `from planetarypy.pds.utils import get_index` (the function lives in `planetarypy.pds`, not `pds.utils`). Broken for ages; only surfaced now because the new smoke job is the first thing that imports `ssi` without a kitchen-sink env masking the issue.

### Added

- **New `[isis]` optional extra: `kalasiris`, `geopandas`, `hvplot`.** Strictly a declaration of deps that the codebase has always needed for ISIS pipelines; no new capability. The extra installs the Python-side glue; ISIS itself remains a separately-managed heavy install (`conda create -n isis -c usgs-astrogeology isis` or the USGS docker image, with its `bin/` directory on `$PATH`). Same shape as the existing `[spice]` extra.
- **`scipy` added to the existing `[spice]` extra.** `spicer.py` uses `scipy.spatial.transform.Rotation` via a lazy import; the declaration was missing. Same "fix the contract" framing.
- **`rich` and `shapely` added to core deps.** Used directly by `cli.py` (rich tables in many sub-commands) and `geo.Point.to_shapely()` via lazy import. Both were previously relying on transitive availability (rich via typer, shapely via geopandas in dev envs); declaring them is the honest contract.
- **`docs/howto/isis_workflows.qmd`** documents the two-part ISIS install (USGS binaries + the `[isis]` Python extra), how to make ISIS binaries reachable on `$PATH`, and the three distinct failure modes users may see (no `[isis]` extra / ISIS not on PATH / ISIS subprocess error) with a specific remediation for each.
- **`minimal-install` CI job** runs in parallel with the matrixed test job. Creates a fresh venv, `pip install .` with no extras / no dev, imports every public submodule, runs `plp --help` on every sub-app. This is what would have caught every bug listed above before they ever shipped — it's the new permanent guardrail against dev-env-kitchen-sink blindspots. CLAUDE.md gains a "Testing conventions" note pointing at it.

### Changed

- **Lazy-imported `geopandas` and `hvplot` in the ISIS-pipeline modules.** Both moved from module-top to inside the functions that actually use them (`ctx_calib.read_gml_to_gdf`, `ctx_calib.plot_any`, `isis/projected.shape_as_geoseries`, `isis/projected.read_gml_to_gdf`, `isis/projected.plot`). Users on a core install can now `from planetarypy.instruments.mro.ctx import ctx_calib` cleanly — calling the heavy functions without the `[isis]` extra raises a clear ImportError at the call site instead of crashing the module import. Same lazy-import pattern that already existed for `scipy` in `spicer.py:67` and `psutil` in `hirise.py:940`.
- **`ctx_edr.py` no longer imports `hvplot.pandas`.** It was dead code — no `.hvplot()` call site in that file. The accessor registration happens wherever the actual caller imports.
- **`utils.catch_isis_error` re-raises `ProcessError` instead of swallowing it.** The previous catch-and-print swallowed real failures, leaving callers thinking operations had succeeded when they hadn't. Now logs (via loguru) and re-raises so callers can react.

### Removed

- **Stray top-level `spicer.py`** at the repo root. Pre-refactor orphan from the v0.52 era; superseded by `src/planetarypy/spice/spicer.py` long ago. Imported `planets` and `traitlets` neither of which we ship. Nothing referenced it.

### Internal

- **`deptry` configuration added to `pyproject.toml`** under `[tool.deptry]`: excludes docs/notebooks/scripts; maps `python-dateutil` → `dateutil`; documents the legitimate per-rule ignores (planetarypy self-imports, psutil's try/except gate, lxml's call-time use by pandas.read_html, dev-tooling CLI deps, click as a transitive of typer). `deptry .` now reports "Success! No dependency issues found" cleanly — a static-analysis companion to the runtime smoke job.

## [0.68.1] - 2026-06-04

A maintenance release: stale dependencies removed, one misclassified dependency moved into core where it belongs, and two pieces of documentation added for collaborators (AI or otherwise) working in the repo.

### Fixed
- **`matplotlib` promoted from `[spice]` extra to core dependencies.** `planetarypy.plotting` (`imshow_gray`, `add_sun_indicator`, `imshow_with_sun`) and the `plp` visualize verbs all import matplotlib unconditionally; none of that is SPICE-related. With matplotlib in `[spice]`, anyone running `pip install planetarypy` without the extra and then `from planetarypy.plotting import imshow_gray` saw a confusing ImportError that mentioned matplotlib but not anything SPICE-related. The fix makes the install correct for that workflow.

### Removed
- **`fastcore` from core dependencies.** Only use in the codebase was `from fastcore.utils import Path` in `src/planetarypy/pds/index_labels.py`; replaced with `from pathlib import Path` (semantically identical for our usage — all call sites pass strings to `Path(...)`).
- **`planets` from the `[spice]` extra.** Zero imports anywhere; `planetarypy.constants` (PCK + JPL DE440 + NSSDC composed in `constants/__init__.py`, added in v0.61.0 and overhauled in v0.64.0) provides everything `planets` was previously used for.

(Note: `lxml` was briefly removed in a pre-tag iteration of this release, but the tag-push CI caught that it's required at runtime by `pandas.read_html()` in `spice/archived_kernels.py:180` — kept in the dependency list with a comment documenting the transitive use.)

### Added
- **`CLAUDE.md` at the repo root**, promoted from a gitignored personal-doc to a committed shared resource. Comprehensive working agreement for AI coding agents (Claude Code, Cursor with Claude, Copilot Chat) operating in this repo: project map, code/CLI/testing conventions, development principles (surgical fixes, semver rule, partial-answers-aren't-agreement, state-machine bug review heuristics, etc.), full release process. The deliberately-excluded category is personal interaction infrastructure (output formatting headers, memory writing protocols) — those stay in maintainers' personal `~/.claude/CLAUDE.md`.
- **`docs/howto/planetaryimage_today.qmd`** — modern replacements for the (no-longer-maintained) `planetaryimage` package's functions. Direct translation table for `PDS3Image.open` / `CubeFile.open` / `img.data` / `plt.imshow(..., cmap='gray')` to the rasterio + `planetarypy.plotting` equivalents. Includes the modernized version of a widely-shared 2015 demo notebook and a section on when `pdr` is the better choice over rasterio. Auto-discovered by the existing `docs/howto/index.qmd` listing block.

### Internal
- 798 tests pass serially under the cleaned-up dep set. Verified the `fastcore` → `pathlib` swap is invisible via focused test runs on `tests/test_pds_static_index.py`, `tests/test_pds_pids_filter.py`, and `tests/test_cli_indexes.py`.

## [0.68.0] - 2026-06-03

Column projection across the `plp indexes` family, a dual-idiom (repeated-flag-or-comma) treatment for both `--columns` and `--ccds`, a more visible freshness state on `plp indexes info`, plus a real bug that had been silently telling users their HiRISE indexes were up to date when they weren't.

### Added

- **`plp indexes peek/last/select --columns/-c COL[,COL...]`** — project the displayed rows to the named columns, in the order given. Comma-separated, repeated-flag, or any mix all work:
  ```bash
  plp indexes last mro.ctx.edr --columns "PRODUCT_ID,IMAGE_TIME" --rows 2 --sort
  plp indexes peek mro.ctx.edr -c PRODUCT_ID -c IMAGE_TIME
  plp indexes select mro.ctx.edr P_A -c PRODUCT_ID -c "IMAGE_TIME,EMISSION_ANGLE"
  ```
  Unknown column names produce a clear error listing every available column. Two ordering subtleties handled: `plp indexes last --sort` runs sort before projection so the time column can drive the sort even if you project it away; `plp indexes select` computes `missing_pids` before projection so the PID column survives for the diff.
- **`planetarypy.pds.get_index(..., columns=None)`** — keyword-only parameter mirroring the CLI flag. Exact (case-sensitive) match; unknown names raise `KeyError` listing every available column. API-first per the project's CLI-thin / lib-fat discipline.
- **`plp hiedr --ccds` and `plp himos --ccds` accept the same dual idiom** as `--columns`: `--ccds 4,5`, `--ccds 4 --ccds 5`, or any mix. Non-integer tokens raise a clean `BadParameter` instead of crashing inside the CCD loop. New helper `_parse_ccds` mirrors `_parse_columns`; both old call sites now share it instead of inlining `[int(n) for n in ccds.split(",")] if ccds else None`.
- **`update available?` row on `plp indexes info`** — completes the freshness picture alongside the existing `last updated` and `last checked` rows. Renders `yes — run \`plp indexes refresh --cache KEY\`` or `no` (or `(check failed: ...)` if the remote HEAD couldn't be reached).

### Fixed

- **`StaticRemoteHandler.update_available` silently returning False on stale-but-cached state.** Real-world report: `plp indexes info mro.hirise.edr` claimed no update was available when the user had manually confirmed a newer index existed. Root cause: `Index(...)` instantiation calls `get_remote_timestamp()`, which writes both `remote_timestamp` and (via the bundled `log_remote_check` side-effect) `last_checked` to "now". That silences `should_check` for the next 24h. The next read of `update_available` then short-circuited on `not self.should_check` and never ran the `remote_time > last_update` comparison — leaving the flag stuck at whatever was previously logged (typically False after a long-past download). The fix removes the `should_check` gate from the comparison path: `should_check` now only gates whether to *fetch a fresh* remote timestamp; the comparison runs whenever a `remote_timestamp` is available (cached or fresh). Regression test pins the exact scenario (cached remote-ts > last_update + should_check False + flag unset → must be True). After upgrading, the very next invocation of any `plp indexes` verb correctly flips the flag for affected indexes.

### Changed

- **`AccessLog.log_remote_timestamp` renamed to `log_remote_check`** with a clarified docstring that spells out the two writes performed atomically (`remote_timestamp` from the server's `Last-Modified`, plus `last_checked` from our wall-clock). The previous name suggested a single-field setter, which was what made the bug above so hard to spot — readers two layers up couldn't see that calling it also bumped `last_checked`. Behavior unchanged; single internal caller updated.

### Internal

- 17 new tests across the touched layers: `test_pds_pids_filter.py` (`columns` projection API: 4 tests), `test_cli_indexes.py` (peek/last `--columns` + dual idiom + projection-after-sort: 5 tests), `test_cli_indexes_select.py` (select `--columns` across table/csv/jsonl formats + missing-PIDs-diff-survives-projection: 5 tests), `test_cli_parse_ccds.py` (the new helper, all forms + non-int error: 9 tests), `test_pds_static_index.py` (the regression for the silent-staleness bug: 1 test), `test_pds_index_logging.py` (renamed `test_log_remote_check`).

## [0.67.1] - 2026-06-02

A small UX improvement for `plp indexes info`: the local-cache row finally gets two siblings reporting when that cache was last downloaded and when we last asked upstream about updates. The data was already tracked on every index's `AccessLog`; this release just surfaces it.

### Changed
- **`plp indexes info KEY`** gains two new rows in the Rich-rendered table:
  ```
  last updated   2026-02-23 11:01:28  (99d ago)
  last checked   2026-06-02 22:08:37  (just now)
  ```
  Each renders with a compact relative-age suffix (`just now` / `30m ago` / `5h ago` / `99d ago`) alongside the absolute timestamp, or `(never)` when the access log carries no datetime yet (fresh installs, or indexes you haven't touched). At a glance you can now tell whether your cached parquet is stale and whether the once-per-day update check has fired recently.

### Internal
- New `_format_when(d)` helper in `cli.py`: renders a datetime with a relative-age suffix; handles tz-aware/naive mismatches so it's robust to whatever the log writer produces. 2 new regression tests in `TestIndexesInfoFreshness` (real-datetime → relative-age strings; `None` → `(never)`) — both stub `Index` + `AccessLog` to stay fast and offline.

## [0.67.0] - 2026-05-29

CSV inputs and a better failure report for the batch-PID workflow. Real-world driver: feeding a HiRISE observation-CSV through `head | plp fetch` to grab the first few RED products — which surfaced two design oversights (stdin always parsed as plain text; FAIL block was an unreadable wall of text) plus the discovery that the suffix idiom belongs in the API too.

### Added

- **`planetarypy.pds.read_pids_file(source, *, index_key=None, pid_key=None, suffix=None)`** — single entry point for "read PIDs from a file or stdin", with smart format dispatch:
  1. `pid_key` set → CSV (the explicit "this is tabular" signal).
  2. File with `.csv` extension → CSV.
  3. Stdin whose first non-blank line contains a comma → CSV (small heuristic so `head file.csv | plp fetch ...` Just Works).
  4. Otherwise → plain text via `planetarypy.utils.read_pids`.

  In CSV mode the PID column is resolved by `pid_key` (explicit) or `pid_column(index_key, df)` (auto-detect via the catalog registry). Failure raises `ValueError` listing the CSV's columns so the caller knows what to pass to `pid_key`. The optional `suffix` is appended to every returned PID; empty string is a no-op. Designed API-first so notebooks and scripts can ingest CSVs without going through the CLI.

- **`plp fetch --pid-key NAME` / `plp indexes select --pid-key NAME`** — name the CSV column to read PIDs from when auto-detection can't (or shouldn't) pick. Also forces CSV parsing on stdin / non-csv-extension paths, so `head file.csv | plp fetch KEY --pids-from - --pid-key NAME` works.
- **`plp fetch --pid-suffix STR` / `plp indexes select --pid-suffix STR`** — append a fixed string to every PID read from `--pids-from`. Motivated by HiRISE-style files that carry observation IDs (`PSP_xxxxx_yyyy`) when the downstream call needs a more specific product (`PSP_xxxxx_yyyy_RED`). Intentionally scoped to file/stdin input only; positional PIDs are passed through verbatim because hand-typed PIDs already carry their full identifier.
- **`docs/howto/hirise_rdr_red_batch.md`** — cheat sheet walking through the four ways to grab HiRISE RDR `_RED` products: single positional, variadic positional, full CSV file (with `--pid-key`), and `head`-pipe via stdin. Includes a flag-summary table for the four scenarios.

### Changed

- **Batch FAIL output is now multi-line and scannable.** The previous one-line-per-failure format (`FAIL PID: ErrorType: msg`) became a wall of unreadable text when error messages were long. New shape:
  ```
  FAIL  ESP_089803_2650_RED
        └ ProductNotFoundError: Product 'ESP_089803_2650_RED' not
          found for mro.hirise.rdr. Check the product_id spelling.

  FAIL  …
  ```
  PIDs over 60 chars get middle-ellipsis truncation (`X…Y`) so pathological cases (whole CSV rows that became pseudo-PIDs from a misconfigured input) don't overflow the terminal. Error message is indented under a `└` marker, soft-wrapped to terminal width (capped at 100). Blank line between entries. Report is padded with leading and trailing blank lines so it stays visually quarantined from `tqdm`'s progress bar (which leaves the cursor mid-line and would otherwise collide with the first FAIL). `OK` lines in `--report full` stay one line each. `jsonl` and `csv` modes are unchanged (their consumers don't need formatting).

### Internal

- 23 new tests across `tests/test_pds_read_pids_file.py` (covering `read_pids_file` dispatch, CSV auto-detection, explicit `pid_key`, suffix application, stdin sniff, plain-text fallback) and `tests/test_cli_batch_pids.py` (covering CLI `--pid-key`, `--pid-suffix`, new FAIL block formatting, long-PID truncation, blank-line separation). Two pre-existing tests updated for the new format strings.
- CI hygiene: `tests/test_ctx_calib_shim.py` adopts `pytest.importorskip("hvplot")` so collection survives in the minimal pip env; `test_generic_kernel_urls_are_accessible` in `test_kernels.py` marked `@pytest.mark.slow` so flaky NAIF HEAD requests stop blocking every-push CI (the test still runs locally; a scheduled-workflow home is left as a separate follow-up).

## [0.66.0] - 2026-05-29

Batch-PID cycle: API-first capability for "do this thing for a list of PIDs", surfaced as two new CLI verbs and a parallel batch helper that other commands can plug into. Plus a small UX consistency pass across the `plp indexes` sub-app and a duplicate-code cleanup in `instruments.mro.ctx`.

### Added

- **`planetarypy.utils.parallel_map(func, items, *, workers=4, executor='thread'|'process', desc=None)`** — canonical helper that returns `list[(item, result, exception)]`, preserving input order and continuing past per-item failures. Promotes the `process_parallel` pattern previously duplicated in `instruments/mro/ctx/ctx_calib.py` and `isis/projected.py` into a single shared utility; new code should reach for this directly instead of standing up its own `ThreadPoolExecutor` / `as_completed` plumbing.
- **`planetarypy.utils.read_pids(source)`** — read PIDs from a file (or `'-'` for stdin). One per line; blank lines and `#`-prefixed comments stripped. No deduplication — the caller decides whether duplicates matter. Designed to feed any of the new `pids=`/`product_ids=` APIs below.
- **`planetarypy.pds.get_index(..., pids=None)`** — when `pids` is given, returns the index DataFrame filtered to only those PIDs. Column resolution lives in the new public **`planetarypy.pds.pid_column(index_key, df)`** helper, which honors `IndexConfig.product_id_col` then falls back through `(PRODUCT_ID, FILE_NAME, IMAGE_ID, OBSERVATION_ID)`.
- **`planetarypy.pds.missing_pids(df, index_key, pids)`** — order-preserving diff helper: returns the input PIDs not present in the index's product-id column. Pairs naturally with `get_index(pids=...)` to surface "which IDs did the index not know about?" in batch workflows.
- **`planetarypy.catalog.fetch_products(key, product_ids, *, workers=4, ...)`** — parallel batch wrapper around `fetch_product`, using `parallel_map` under the hood. Returns `list[BatchFetchResult]` (also new) with `.product_id` / `.downloaded` / `.exception` / `.ok` fields per PID. Calls `have_internet()` upfront and raises the new `OfflineError` on preflight failure; `skip_online_check=True` bypasses for offline mirrors or captive networks.
- **`plp fetch KEY [PIDS...]`** — variadic positional PIDs. Single-PID call preserves the existing output contract (URL to stderr, files to stdout) so shell composition (`cd (plp fetch --folder ...)`, `qgis (plp fetch ...)`) keeps working bit-for-bit. Multi-PID calls go through `fetch_products` with `--workers N` (default 4), continue-on-error processing, and a `--report errors-only|full|jsonl|csv` (default `errors-only`) for the per-PID outcome view. Exit 0 if all OK, 1 if any failed; `jsonl`/`csv` modes always exit 0 since downstream consumers should parse the per-row `ok` flag. `--folder` is rejected in batch mode (only makes sense for `cd` composition).
- **`plp fetch --pids-from PATH`** — read PIDs from a file (or `-` for stdin). Mutually exclusive with the variadic positionals.
- **`plp indexes select KEY [PIDS...] [--pids-from PATH]`** — new verb dedicated to "filter to these specific PIDs". Kept separate from `peek` (random sample) and `last` (trailing rows) because the premises differ. `--format auto|table|csv|jsonl` defaults to `auto`: transposed Rich table when matched rows are at most `--max-table-rows` (default 3), otherwise CSV — small N for visual inspection, large N for piping. `--report errors-only|full` controls how missing PIDs are surfaced on stderr (stdout stays clean for the pipe consumer regardless of `--format`).
- **New main-config knob `max_table_rows`** (default `3`) in `~/.planetarypy_config.toml`. Threshold above which row-display commands switch from the transposed Rich table to CSV. Fresh configs ship with the key + an explanatory comment; legacy configs get it backfilled on next read. The CLI flag `--max-table-rows N` overrides it per call.

### Changed

- **Help-on-missing UX across the entire `plp indexes` sub-app.** Bare invocations of `plp indexes peek`, `plp indexes last`, `plp indexes info`, `plp indexes refresh`, and `plp indexes select` now print the command's help text and exit 0, instead of emitting Typer's auto "Missing argument 'KEY'" error. The pattern matches the existing `plp fetch` UX — calling a verb with no args should *teach you what it does*, not bark at you. (`plp indexes refresh` had no positional arg; its analogous case "neither `--config` nor `--cache` given" now also shows help.)
- **`planetarypy.instruments.mro.ctx.ctx_calib.process_parallel`** is now a thin backward-compatible shim over `planetarypy.utils.parallel_map`. Public signature (Executor class + task + pids + refresh kwarg) and the original early-failure semantics (raise the first exception) are preserved; the inline `ThreadPoolExecutor` / `as_completed` / `tqdm` orchestration is gone. The matching duplicate in `planetarypy.isis.projected` is *not* touched in this cycle — separate cleanup.
- **`docs/howto/cli.qmd`** gains a "Design philosophy" paragraph stating the API-first / CLI-wraps-thin discipline explicitly: every `plp` verb is a thin wrapper over a public Python API; useful logic lives in `planetarypy.*` modules, not in `cli.py`. Notebooks and downstream tooling should pick up new capabilities without screen-scraping or shelling out.

### Internal

- 73 new tests across 7 files: `test_utils_parallel.py` (8), `test_utils_read_pids.py` (10), `test_pds_pids_filter.py` (14), `test_catalog_fetch_products.py` (8), `test_cli_batch_pids.py` (14), `test_cli_indexes_select.py` (15), `test_ctx_calib_shim.py` (4); plus 4 new tests added to `test_cli_indexes.py` (help-on-missing for peek/last/info/refresh) and 3 new config-layer tests in `test_config.py` (fresh-config presence, legacy backfill, explicit value preservation for `max_table_rows`). One process-pool test is skipped under pytest-xdist (nested process pools don't survive the worker fork/spawn).

## [0.65.1] - 2026-05-28

A bugfix release that finally delivers the v0.64.0 NSSDC parser improvements to end users on PyPI/conda. The parsed fact-sheet archive isn't bundled in the wheel (it's lazy-downloaded from Zenodo on first use), and the local cache filename in `planetarypy.constants.nssdc._loader` was keyed only on the JSON schema version — which v0.64.0 didn't bump, because the schema didn't break. Result: anyone with a pre-v0.64.0 cache kept reading the old buggy data, and fresh installs pulled the original v1.0.0 Zenodo deposit which was also pre-fix. This release uploads the corrected archive as Zenodo deposit v1.1.0 (concept DOI [10.5281/zenodo.20122986](https://doi.org/10.5281/zenodo.20122986)) and refactors the loader's cache key so future data refreshes propagate automatically to existing users.

### Fixed
- **NSSDC parser fixes from v0.64.0 now reach end users.** The local cache filename is now `parsed_archive_v{EXPECTED_ARCHIVE_VERSION}_z{ZENODO_RECORD_ID}.json.gz` — keyed on both the JSON schema version and the per-version Zenodo record ID. Bumping `ZENODO_RECORD_ID` in a release invalidates existing caches and triggers a redownload, which is how data-only updates actually propagate. Previously only `EXPECTED_ARCHIVE_VERSION` was in the filename, so pure data refreshes (no schema change) were invisible to the cache layer.
- **`ZENODO_RECORD_ID` bumped `20122987` → `20426712`.** Points at the new Zenodo deposit v1.1.0, which contains the parsed archive with all v0.64.0 parser fixes baked in (J2 oblateness restored on 9 planets, range/uncertainty plumbed, unit aliases for `hours`/`g/mole`/`degrees`, `±` and `10^N` extraction, Title-Case `Surface Gravity`, NSSDC label-wording drift, unit-paren detector no longer eating non-unit qualifiers).

### Changed
- **Versioning policy in `_loader.py` documented and split.** Two independent knobs, explicitly commented: `EXPECTED_ARCHIVE_VERSION` (JSON schema — bump only on breaking shape changes; additive optional keys like the v0.64 `uncertainty`/`range` keys don't count) and `ZENODO_RECORD_ID` (data revision — bump every Zenodo upload). The previous single-knob "bump only on breaking schema changes" policy was technically defensible but incompatible with delivering data updates: without a freshness signal, no breaking schema → no cache miss → no propagation. New split makes that contract explicit.
- **Zenodo bundle script (`scripts/build_nssdc_zenodo_bundle.py`) plumbing.** New `DEPOSIT_VERSION = "1.1.0"` and `CONCEPT_DOI = "10.5281/zenodo.20122986"` constants. The generated `README.md` inside the bundle now cites the deposit version (in title and citation block), surfaces the JSON schema version separately in the schema reference section, and uses the concept DOI in the citation rather than a version-pinned record ID. Bundle output directory becomes `build/nssdc_archive_v{DEPOSIT_VERSION}/` (was schema-version-named).

### Note for downstream consumers
End-user constant *values* will change after this upgrade — the same data changes documented under v0.64.0's "Fixed" section now actually take effect (J2 populated on 9 planets, range/uncertainty plumbed through, ~1400 readings recovered from the unit-paren-eater fix, etc.). Strict semver permits this under PATCH because the pre-fix values were buggy, but pinned-equality tests against the old constants will see diffs.

## [0.65.0] - 2026-05-27

Two CLI quality-of-life additions for `plp indexes`, plus a small main-config knob to silence upstream deprecation noise that one of them provokes.

### Added
- **`plp indexes last KEY [-n N] [--sort]`** — show the trailing rows of a PDS index, transposed in the same layout as `plp indexes peek` (one output column per shown index row, field names down the left). Default 3 rows; `--rows`/`-n` adjusts.
  - Default order: file order. Most PDS indexes are appended chronologically as new products land, so the last row in the parquet IS the newest entry. Cheapest read — no sort.
  - `--sort` / `-s`: auto-detect a canonical time column (`START_TIME` / `OBSERVATION_TIME` / `IMAGE_TIME` / `TIME`, in order) and sort ascending before taking the tail. Useful when the parquet isn't actually chronologically ordered. Falls back gracefully with a stderr notice if no canonical time column is present.
  - Internal: extracted `_render_index_rows()` so `peek` and `last` share the table-rendering code; `peek`'s behavior is unchanged.
- **New main-config knob `filter_deprecation_warnings`** (default `true`) in `~/.planetarypy_config.toml`. Suppresses `DeprecationWarning` during `plp` execution so end users don't see upstream noise (notably Typer's `shell_complete=`-is-deprecated notice that the segment-aware completion below provokes). Devs working on planetarypy can set this to `false` to see the warnings as reminders. Standard Python `-W` flags and `PYTHONWARNINGS` env var still stack with this filter — it doesn't override an explicit user policy. The key is written into freshly-created configs with an explanatory comment block; legacy configs (existing users who installed before this version) get it backfilled on next read so they too see the knob exists.

### Changed
- **`plp indexes` tab completion is now segment-aware.** Pressing `<TAB>` after just an instrument or indexname segment — `ctx<TAB>`, `hirise<TAB>`, `CTX<TAB>` — now completes to the full dotted key (`mro.ctx.edr`, `mro.hirise.edr`, …). No more "did I have to remember the mission prefix?" friction. Whole-key prefix still wins (`mro.<TAB>` keeps working the obvious way); the segment-prefix branch only fires when whole-key prefix produces nothing. Case-insensitive. Applies to every command that resolves an index key (`peek`, `last`, `info`, `refresh`, `example_pid`, `meta`).

  Implementation: wired via Click's native `shell_complete=` rather than Typer's `autocompletion=` because Typer's wrapper post-filters returned candidates with `value.startswith(incomplete)` — which would have silently dropped every segment-prefix match. This is the reason for the new `filter_deprecation_warnings` knob; Typer emits a `DeprecationWarning` about `shell_complete=` going away in a future version. If/when that lands, the fix is either to revert to prefix-only completion under `autocompletion=`, or upstream a filter opt-out flag.

### Internal
- 18 new tests across `tests/test_cli_indexes.py` (15) and `tests/test_config.py` (3): completion paths (whole-key precedence, segment-prefix fallback, case-insensitivity, adapter wiring), `last` command behavior (default 3-row tail, `-n` limit, `--sort` picks time column, `--sort` falls back, peek regression guard), warning-filter behavior (default-on, explicit-true, explicit-false), config backfill (fresh has key, legacy gets backfilled, explicit-false survives backfill). `get_index` is patched so the suite stays offline.

## [0.64.0] - 2026-05-27

A `planetarypy.constants` cycle focused on (1) restoring the design contract that the `iauNNNN` modules contain *only* IAU data, (2) finding and fixing a cluster of "silently dropped NSSDC values" bugs that had been hiding in plain sight in the 30-year fact-sheet archive, and (3) plumbing through structured error information (measurement uncertainty, naturally-varying ranges) that the parser was previously throwing away.

35 new constants become available across the 11 NSSDC bodies at the latest-capture view, plus thousands more recovered in the time-indexed `nssdc.history(...)` API across the full archive. Public API stays backwards-compatible: `from planetarypy.constants import Mars` still resolves the same `Body` with the same fields.

### Added
- **`Range(min, max)` dataclass** with `midpoint` and `half_width` properties (`planetarypy.constants.Range`). Represents bounds for quantities NSSDC publishes as "X - Y" (seasonal/diurnal/spatial variation). Kept distinct from measurement uncertainty by design — see the explanation doc for the rationale.
- **`Constant.uncertainty: float` and `Constant.range: Optional[Range]`** — two orthogonal optional fields on every `Constant`. Default to absent (`0.0` / `None`) so every existing call site keeps working unchanged. `__repr__` shows `± X` inline and `(range X–Y)` after the value, or `[range X–Y]` in place of the value when value is NaN.
- **`RangeWarning(UserWarning)`** — issued once per process when a `Constant` with a populated `.range` returns the midpoint as its scalar value (only when the user has explicitly opted into the midpoint strategy via config). Standard `warnings.filterwarnings(...)` works; the config switch below silences it permanently.
- **Two new config keys** under `[constants]` in `~/.planetarypy_config.toml`:
  - `range_strategy = "nan" | "midpoint"` (default `"nan"`) — for range-typed entries, controls whether `Constant.value` is `NaN` (faithful, the default — `NaN` propagates through arithmetic as a visible signal) or the range midpoint (ergonomic, opt-in, emits `RangeWarning`).
  - `warn_on_range_midpoint = true | false` (default `true`) — suppress the `RangeWarning` even when in midpoint mode (for users who've made the trade and want quiet output).
- **`tests/test_constants_vs_sources.py`** (21 tests) — three-layer anti-hallucination suite: (1) PCK-sourced Constants round-trip against `spiceypy` bit-for-bit (both 2009/2015 editions) with reverse-coverage on the kernel pool; (2) NSSDC parser coverage ratchet over the 913 in-repo HTML captures with an allowlist of expected drops, so any new silent drop fails CI; (3) 16 hand-verified `(body, field, expected_value)` triples against the live NSSDC fact-sheet pages.
- **`tests/test_constants_nssdc_parser.py`** (17 tests) — focused parser unit tests on small synthetic HTML inputs: scientific notation (`10^N` / `X x 10^N` / `X × 10^N`), qualifier prefixes (`~`/`<`/`>`/`≈` and stacked combos), `+/-` and `±` uncertainty, range extraction with/without units, newline-doesn't-bleed-into-unit regression.
- **`tests/test_constants_nssdc_loader.py`** (12 tests) — loader unit-coercion surface: known-unit lookup, scaled-unit factor, `x`-prefix normalization, per-field default-unit fallback, precedence rules.
- **`tests/test_constants_range_strategy.py`** (10 tests) — runtime strategy plumbing: both NaN and midpoint strategies, once-per-process warning gate, config-driven suppression, uncertainty propagation, NaN-aware repr.

### Changed
- **`constants.iau2009` and `constants.iau2015` are now PCK-only.** Previously each generated module inlined IAU PCK polynomial fields, JPL DE440 GMs, and NSSDC fact-sheet values under a namespace named after just one of those sources — a contract violation. Now each source lives in its own module and is composed at import time in `constants/__init__.py` (`PCK + JPL GM + NSSDC`). The generated `iauNNNN.py` literals collapse from 2706 lines to ~12 lines each. Public API unchanged: `from planetarypy.constants import Mars` still resolves the same Body with the same fields, but each Constant now correctly reports the source it actually came from (PCK / JPL DE440 / NSSDC) rather than inheriting the iau module's PCK label.
- **New `constants._gm_jpl` module**, generated from `gm_de440.tpc` alone. Single source of truth for GMs, composed onto every Body regardless of which IAU edition's PCK supplies the cartographic fields. GMs aren't IAU-edition-versioned — they track JPL DE-series ephemeris releases — and the new structure makes that explicit.
- **NSSDC parser now captures range and uncertainty structure** instead of silently dropping the rows or extracting garbage as the unit. `X +/- Y unit` yields `{value, uncertainty, unit}`; `X - Y unit` yields `{range: {min, max}, unit}` with `value` omitted (interpretation-free). The runtime loader applies the user's `range_strategy` preference to decide how to surface the value. Atmosphere fallback regex also gains scientific-notation value matching (`10^-15`, `X x 10^N`, `X × 10^N`) and qualifier-prefix tolerance (`~`, `<`, `>`, `≈`).
- **Per-field default unit table** in the loader (`_FIELD_DEFAULT_UNITS`). NSSDC publishes a few fields with no unit because the discipline-conventional unit is implicit (Earth's `Mean molecular weight: 28.97`). Without a fallback, the same field loaded as `dimensionless` for some bodies and `g/mol` for others — a typed inconsistency. Current entry: `mean_molecular_weight → u.g/u.mol`. Auditable per-entry; easy to extend.
- **`docs/factsheets.qmd`** regenerated. New rows for J2, Flattening, V-band magnitude, Moment of inertia, Pluto surface gravity, Mercury surface pressure across the planets that publish them. Mars surface density now reads `0.016 ± 0.006 kg/m³` (uncertainty); Neptune scale height reads `19.1–20.3 km` (range). Unit display also fixed where the pre-fix archive had truncated forms (Venus surface density `kg/m` → `kg/m³`).
- **`docs/explanation/constants_design.qmd`** gains a "NSSDC error-info: uncertainty vs range" section documenting the two concepts, their distinct semantics, the `range_strategy` config knob, and how to suppress `RangeWarning`.

### Fixed
NSSDC parser fixes in this cycle. Each is source-only (didn't touch the in-repo `parsed_archive.json.gz` until the final regen commit), so the test allowlist tightens with each.

- **`J2` (oblateness coefficient) on all 9 NSSDC planets** — Mercury, Venus, Earth, Mars, Jupiter, Saturn, Uranus, Neptune, Pluto. Two-stage bug: (a) `<sub>2</sub>` was rewritten to the marker form `_2` (intentional, so unit strings like `m/s^2` survive tag stripping), but FIELD_MAP only had the key `"J2"`, so 687 readings across 30 years silently disappeared; (b) the regenerator's unit table had no entry for NSSDC's `"x 10^-6"` notation, so even after the parser fix the value was still dropped during unit coercion. Both gaps closed; loader-side `_UNIT_MAP` kept in sync.
- **Unit-paren detector no longer eats non-unit qualifiers.** `_split_label_unit()` greedily stripped any trailing `(...)` as a unit, truncating labels like `"Visual magnitude V(1,0)"` (where `(1,0)` is photometric notation, not a unit) and `"Ellipticity (Flattening)"` (where `(Flattening)` is a synonym qualifier, not a unit). FIELD_MAP entries for all three labels existed but were unreachable. Fix short-circuits the splitter when the full string is already a known FIELD_MAP key. Recovers ~1400 readings across the archive — 583 Visual magnitude, 227 V-band magnitude, 629 Flattening across nearly every NSSDC planet + Moon.
- **NSSDC label wording drift** — older captures used longer-form labels (`"Orbital inclination"`, `"Orbital eccentricity"`, `"Inclination to equator"`) that had no FIELD_MAP entry, only their shorter siblings (`"Orbit ..."`, `"Inclination of equator"`). Aliased each to the same target attr. Recovers 103 readings.
- **Title-Case `Surface Gravity` variants** aliased to `surface_gravity`. NSSDC alternated the casing across captures and only the lowercase form had an entry. Recovers 14 readings.
- **Three new NSSDC unit aliases** in both `_NSSDC_UNIT_EMIT` (regenerator) and `_UNIT_MAP` (loader): `"hours"` → `u.hour` (Jupiter/Neptune/Saturn/Uranus sidereal rotation period, 202 readings), `"g/mole"` → `u.g/u.mol` (mean molecular weight on 6 bodies, 123 readings), `"degrees"` → `u.deg` (4 outer-planet orbit inclinations).
- **`<sup>` / `<sub>` markers re-attached to their preceding word.** Modern NSSDC captures wrap `<sup>` in `<span>`, so after tag stripping `kg/m<span><sup>3</sup></span>` became `kg/m ^3` (exponent visibly disconnected). The atmosphere-fallback regex then dropped the `^3` and the row coerced as `kg/m` (no entry in the unit table). Collapsed the spacing artifact. Recovers Mars and Venus `surface_density` (64 readings) plus pre-empts ROW_SPLIT_RE misidentifying values containing exponents as labels.
- **NSSDC atmosphere fallback no longer eats newlines.** The `\s*` between value and unit-capture was greedy and consumed line breaks, allowing the next section header ("Atmospheric composition: ...") to bleed in as a fake unit. Switched to `[ \t]*` (horizontal whitespace only). Recovers 278 readings of `mean_molecular_weight` across 7 bodies that had `unit = "Atmospheric"` baked in.
- **NSSDC uncertainty and range entries** are now real values instead of being dropped or mislabeled. `X +/- Y unit` becomes a Constant with uncertainty; `X - Y unit` becomes a Constant with a `Range` (and, depending on `range_strategy`, either `NaN` or the midpoint as the scalar value). Examples baked into the regenerated archive: Mars `surface_density = 0.016 ± 0.006 kg/m³`, Neptune `scale_height = range [19.1, 20.3] km`, Neptune `mean_molecular_weight = range [2.53, 2.69]`, Pluto `mean_molecular_weight = range [16, 25] g/mole`.
- **Scientific-notation values in the atmosphere fallback** (`10^N` and `X x 10^N`). Mercury's `Surface pressure: ~10^-15 bar` now correctly loads as `1e-15 bar` instead of dropping the row (the regex previously matched only the `10`, leaving `^-15` as junk for unit coercion). Qualifier prefix widened to `[<>~≈]*` so stacks like `<~5 x 10^-15` parse cleanly too.
- **Per-field default unit fallback** unifies Earth's `Mean molecular weight: 28.97` (unit-less in the source) with other bodies' `g/mole` entries. Previously the same field loaded as `dimensionless` on Earth and `g/mol` elsewhere.

### Internal
- **Anti-hallucination test suite added** for `planetarypy.constants` (see Added section). 60+ new tests across four files cover the parser, loader, runtime strategy, and end-to-end archive coverage.
- **`scripts/parse_nssdc_archive.py`** gains a `SkippedReport` dataclass + optional `_report=` kwarg threaded through `_parse_pre_block`, `_parse_table`, `parse_capture`. Default behavior unchanged; the kwarg is only populated by the parser-coverage test that ensures no NSSDC content slips past the parser unnoticed.
- **`scripts/regenerate_constants.py`** rewritten to emit the three source modules (`_gm_jpl.py`, `iau2009.py`, `iau2015.py`) independently. NSSDC merging removed from the iau-module pipeline.

## [0.63.0] - 2026-05-13

### Added
- **`plp spice` CLI sub-app — five verbs covering kernel discovery + fetch.** Mirrors the existing `plp catalog` / `plp indexes` shape so SPICE work no longer requires breaking out to Python for the easy parts:
  - `plp spice missions` — Rich table of all 39 NAIF mission archives (shorthand, full mission name, date range, cumulative bundle size). Reads from `archived_kernels.datasets`.
  - `plp spice info <mission>` — date range + archive metadata + URLs (PDS readme, archive root, NAIF subsetter URL) for one mission. Misspellings get difflib suggestions ("cassiny" → "did you mean: cassini?"); tab completion on the mission shorthand.
  - `plp spice fetch <mission> --start --stop [--save-location]` — date-scoped kernel subset download. Wraps `archived_kernels.get_metakernel_and_files()`; metakernel path goes to **stdout** so shell composition Just Works (`spiceinit mkpre=$(plp spice fetch cassini --start … --stop …)`).
  - `plp spice cached [--total]` — Rich table of every kernel currently cached under `{storage_root}/spice_kernels/`, grouped by mission with file count, on-disk size, and a sample of filenames per group. New backing helper `archived_kernels.list_cached_kernels() -> dict[str, list[Path]]` (public in `__all__`).
  - `plp spice generic <name> [--force]` — fetch one generic kernel by short alias. New backing helper `generic_kernels.download_generic_kernel(name, overwrite=False) -> Path` (public). Aliases: `lsk`, `pck`, `masses`, `de430`, `mar099s`. Full path-fragments accepted for non-default kernels (e.g. `lsk/naif0011.tls`). Cached-aware: returns the local Path immediately if already on disk. Tab completion across the alias set; new `GENERIC_KERNEL_ALIASES` dict exposed alongside.

  Per the thin-wrapper rule, both new helpers live as proper Python APIs in `planetarypy.spice.*` and are tested independently of the CLI; the CLI commands are short adapters over them.

### Changed
- **`plp --help` output is now grouped into 6 panels** instead of one flat 13-verb list. Categories: *Discovery & browsing* (catalog, indexes, spice, constants), *Fetch & download* (fetch, hibrowse, hiedr, himos), *Inspect a product* (meta, example_pid), *Visualize* (ctxqv), *Science computations* (spicer), *Maintenance* (ctx-migrate). Implementation: `rich_help_panel="..."` on each `@app.command()` decorator and on each `app.add_typer(...)` registration. Panel rendering order is determined internally by typer/rich-click (not by source order or alphabetical) — the grouping itself is the readability win; tweaking the order requires a rich-click config override that isn't worth the complexity here.

### Refactor / internal
- **Body.iter_constants()** extracted from `cli.py` into `planetarypy.constants.base.Body` so the `plp constants` table-render and tab-completion paths stop duplicating the "iterate dataclass fields, filter to Constants" loop. Yields `(field_name, Constant)` for every Constant-bearing field; skips `None` fields and non-Constant values (metadata like `body_class` / `naif_id`, polynomial-coefficient tuples, scalar floats). Useful for introspection beyond the CLI; 3 unit tests in `tests/test_constants_base.py` pin the contract.
- Misc simplifications in `cli.py`: dropped over-defensive `try/except` around stable internal imports in `_complete_constants_query`; dropped `isinstance(b, Body)` filters (BodyRegistry only contains Bodies by construction); extracted `_body_name_set()` and `_suggest_and_exit()` helpers shared between completion paths and error suggestions; direct `.source` attribute access on Constants instead of `getattr(...) or ""` defensive chains. Plus a Lua-filter cleanup in `docs/_abbreviations.lua` (removed unused `suffix` capture, redundant rename, recomputed `after`, etc.).

## [0.62.0] - 2026-05-13

> Note: a 0.61.1 release was published to PyPI between 0.61.0 and 0.62.0
> with this same content. It was bumped as a patch by mistake; the new-
> CLI-subcommand addition is backwards-compatible-additive, which is
> textbook minor-version territory. 0.61.1 stays on PyPI for posterity;
> 0.62.0 is the version to install. Bytes are identical apart from the
> version string.

### Added
- **`Body.iter_constants()`** — generator yielding `(field_name,
  Constant)` for every Constant-bearing field on a body. Skips `None`
  fields and non-`Constant` values (metadata like `body_class` /
  `naif_id` / `dwarf_planet`, polynomial-coefficient tuples, scalar
  floats like `flattening`). Useful for tabular display, introspection,
  and CLI completions without callers needing to know the dataclass
  schema. Both the `plp constants` table-rendering and tab-completion
  paths now route through this method instead of duplicating the
  filter-by-isinstance loop — keeps the CLI a thin wrapper.
- **`plp constants` CLI subcommand.** Two forms:
  - `plp constants Mars` — Rich-rendered table of every scalar `Constant`
    attached to the body, with a *source* column (PCK kernel filename or
    NSSDC capture stamp per field). Filters out non-Constant metadata
    like `body_class` / `dwarf_planet` / `naif_id` so the table only
    contains actual quantities.
  - `plp constants Mars.GM` — value on **stdout**, provenance lines
    (`# source: pck00011.tpc`, `# reference: IAU 2015 — Archinal et
    al.…`) on **stderr**, so `plp constants Mars.GM | awk '{print $1}'`
    Just Works.
  Body matching is case-insensitive (`mars` == `Mars` == `MARS`). Misspelt
  bodies and unknown fields each exit non-zero with `difflib`-driven
  close-match suggestions on stderr (e.g. `'jupier'` → "did you mean:
  Jupiter, Juliet?"; `Mars.gravity` → "did you mean: surface_gravity?").
  Carries the time-travel facility through to the CLI via `--at`/`-t`:
  `plp constants Mars.pole_dec --at 2012` returns 52.886° (sourced from
  `pck00010.tpc`/IAU 2009), demonstrating the PCK-edition swap from a
  shell. Tab completion offers body names before the dot and
  Constant-bearing field names after it (gas-giant `Jupiter.surface_<TAB>`
  correctly returns nothing since those fields are unset). 13 new tests
  in `tests/test_cli_constants.py` pin the contract.

### Changed
- **Bare invocation now prints `--help` for every top-level CLI command.**
  Previously, `plp catalog` / `plp indexes` (sub-app groups) showed full
  help on bare invocation (typer's free `no_args_is_help=True` for
  groups), but the eight individual commands with required positionals
  — `fetch`, `hibrowse`, `hiedr`, `himos`, `ctxqv`, `spicer`,
  `example_pid`, `meta` — instead emitted typer's terse `Missing
  argument 'KEY'.` error (exit 2). Now every top-level command exits 0
  with the full help block when invoked without arguments. Two-positional
  commands (`fetch`, `meta`) still error on partial invocation with a
  clear `Error: missing PRODUCT_ID argument.` message and exit 2, so the
  "give me a hint, I have the first arg" flow is preserved. Pattern:
  added `ctx: typer.Context` parameter, defaulted the first positional
  to `None`, and inserted an `if x is None: typer.echo(ctx.get_help());
  raise typer.Exit()` block at the top of each function body. Cheap and
  consistent.

## [0.61.0] - 2026-05-12

### Added
- **`planetarypy.constants` — a one-stop constants subsystem.** Three layers in a single namespace:
  1. **Fundamental physics constants** re-exported from `astropy.constants`: `G`, `c`, `h`, `k_B`, `N_A`, `sigma_sb`, `m_e`, `m_p`, `M_sun`, `R_sun`, `L_sun`, `M_earth`, `R_earth`, `M_jup`, `R_jup`, `au`, `pc`, `kpc`, and ~15 more — so one import covers fundamental + per-body without managing two parallel imports. CODATA-versioned via astropy; nothing repackaged.
  2. **Per-body PCK constants** for ~145 solar-system bodies — triaxial radii, GMs, pole RA/Dec, prime meridian, rotation rate, plus polynomial coefficients — sourced from NAIF SPICE PCK kernels and versioned by IAU report edition (2009, 2015). Generated at build time from upstream kernels via `scripts/regenerate_constants.py` so the runtime never depends on spiceypy; the bare `Mars`, `Saturn`, … etc. resolve to the current IAU 2015 edition.
  3. **NSSDC fact-sheet parameters** for the Sun, eight planets, the Moon, and Pluto — bond albedo, surface pressure, scale height, satellite count, semimajor axis, sidereal period, ~25 more fields — merged transparently into each Body at build time. NSSDC = NASA's National Space Science Data Center at Goddard Space Flight Center; D. R. Williams has maintained the canonical per-body fact sheets there since 1996. Resolution rule: PCK wins for cartographic/orientation fields when both have a value; NSSDC fills in everything PCK doesn't carry. Every value returns as a `Constant` (an `astropy.units.Quantity` subclass) with `.source`, `.reference`, `.description`, and `.iau_year` metadata — `Mars.GM.source == "pck00011.tpc"`, `Mars.bond_albedo.source == "NSSDC marsfact.html updated 2025-05-19"`. Discovery helpers (`planets()`, `moons(of="Saturn")`, `asteroids()`, `mission_visited()`, `find_body("Bennu")`) for browsing the registry by class.
- **Time-travel: `Body.at_time(date)`** returns a snapshot of any body with all fields resolved as of `date`. Picks the right IAU PCK edition for the date (IAU 2009 PCK published 2010-10-21; IAU 2015 PCK published 2018-09-20) AND the right NSSDC capture (most recent revision at or before the date). `Mars.at_time('2012').pole_dec` returns 52.8865° (IAU 2009), `Mars.at_time('2024').pole_dec` returns 54.4325° (IAU 2015). A 2012 paper's calculation is reproducible without the reader knowing the submodule name `iau2009` exists. Adding a future IAU edition is a one-line append to `_PCK_EDITION_DATES`. Module-level `at_time(body, field, date)` provides the function-form alternative.
- **`planetarypy.constants.nssdc` opt-in namespace** for users who want NSSDC-only data deliberately — `nssdc.Mars.GM` returns NSSDC's own GM solution (which may differ from PCK's), not the PCK-wins-merged value. `nssdc.history(body, field)` returns the full publication history as `(date, value, capture_url)` tuples — useful for science-history studies and drift audits. `nssdc.at_time(body, field, date)` for NSSDC-only date lookup.
- **NSSDC longitudinal archive deposited at Zenodo: [10.5281/zenodo.20122987](https://doi.org/10.5281/zenodo.20122987).** 913 distinct content versions of NSSDC's 13 fact sheets (Sun, Mercury, Venus, Earth, Moon, Mars, Jupiter, Saturn, Uranus, Neptune, Pluto, asteroid summary, comet summary) captured via the Internet Archive's Wayback Machine, spanning December 1996 to May 2025. Bundle includes the canonical parsed JSON, a long-format CSV mirror, the original raw HTML corpus, CDX provenance manifest, and the stdlib-only Python scripts that re-derive everything from public sources. Each capture indexed by NSSDC's own "Last Updated" footer date (with Wayback timestamp as fallback for pre-2003 captures where the footer didn't exist yet). The parsed archive auto-downloads from Zenodo on first use when not already in the user's local cache — same lazy pattern as PDS index parquets. MIT-licensed (matches the planetarypy library license).
- **Constants tutorial:** `docs/tutorials/constants_tutorial.ipynb` walks through everyday access, provenance, discovery helpers, time-travel, paper-reproducibility, the explicit `nssdc` namespace, and visualizing per-field drift with matplotlib (Saturn's satellite count from 18 in 1996 to 274 in 2025 as the headline example).
- **Acronym tooltips in HTML docs** via a small Lua filter (`docs/_abbreviations.lua`) + matching CSS (`docs/_styles-abbr.css`). Hovering NSSDC, NAIF, PCK, SPICE, IAU, GM, JPL, etc. shows a styled tooltip with the spelled-out form. Word-boundary safe; HTML-only (skipped for PDF/LaTeX); `aria-label` (not `title`) so screen readers get the expansion without the tiny native browser tooltip overlapping the styled one.

### Fixed
- **`Constant.__repr__` now shows source provenance for NSSDC-sourced fields.** Before, the rich repr (`<Constant Mars.GM = … (IAU 2015)>`) required `iau_year` to be truthy, and NSSDC constants set `iau_year=0` since NSSDC isn't IAU-edition-versioned. NSSDC constants therefore fell back to plain Quantity repr, hiding the source. Now uses the source string as the provenance tag when no IAU year is set: `<Constant Mars.bond_albedo = 0.25 (NSSDC marsfact.html updated 2025-05-19)>`. The metadata-empty fallback to plain Quantity repr is preserved for un-annotated Constants.

## [0.60.0] - 2026-05-04

### Added
- **`plp catalog samples <key>`** — print the sample products in the catalog DB for a `mission.instrument.product_key` triple. Wraps `planetarypy.catalog.example_products()` in a Rich table with `--phase` (filter to one mission phase) and `-n / --limit` (cap rows; 0 = all). Useful for inspecting what's actually catalogued for archives without a registered fetch resolver, where these samples are the only available products.
- **`plp indexes peek <key>`** — inspect a registered PDS index's schema and a few random rows. Output is transposed (one row of the index per column of the table) so it stays readable whether the index has 4 columns (`cassini.cda.index`) or 71 (`mro.hirise.edr`). Default 3 random rows; `-n / --rows N` to vary. Motivated by discovering that `cassini.cda.index` has `FILE_SPECIFICATION_NAME` / `DATA_SET_ID` instead of the usual `PRODUCT_ID` column — peek surfaces the schema before you have to guess which column to use.

## [0.59.5] - 2026-05-04

### Fixed
- **`Index.convert_to_parquet` no longer swallows conversion errors.** Previously, any exception during parsing or parquet writing was caught, logged at ERROR level, and dropped. With loguru disabled-by-default in this library, that error never reached the user — the downstream `pd.read_parquet(self.local_parq_path)` then raised `FileNotFoundError: …/CUMINDEX.parq` from a totally unrelated code path. Two real bugs (LAMP mixed-format times in v0.59.4, cassini.cda.index label/table mismatch below) both manifested as this misleading FileNotFoundError. `convert_to_parquet` now re-raises as `RuntimeError` with the index_key in the message and the original parser exception chained as `__cause__`.
- **`IndexLabel.index_path` raises a descriptive label/table mismatch error.** When a label's `^TABLE` pointer names a file that doesn't exist on disk (under either case), the previous code logged ERROR and returned a phantom path; downstream `pd.read_csv` then raised an unhelpful *"No such file or directory: index.tab"*. Now raises a `FileNotFoundError` that names the label, the declared table filename, what files ARE present in the directory, and labels the situation as a *publishing inconsistency in the source archive* — not auto-fixable in the local cache. `cassini.cda.index` is the canonical case: SETI publishes the cumulative table as `CUMINDEX.TAB` but the included `CUMINDEX.LBL` declares `^INDEX_TABLE = "INDEX.TAB"` (the per-volume convention). Combined with the `convert_to_parquet` re-raise, users of `plp meta cassini.cda.index <pid>` now see exactly what went wrong on the first try.

## [0.59.4] - 2026-05-04

### Changed
- **`plp catalog list <mission>` now distinguishes three states per instrument** instead of collapsing two of them into a blank cell. The previous two-column view ("instrument" + "fetchable product types") couldn't tell apart (a) instruments with no PDS index registered at all from (b) instruments that *are* indexed but lack a fetch resolver (e.g. `lro.lamp` — its index has no VOLUME_ID column, and the archive splits across `LROLAM_<N>` volume directories that can't be derived from row data). New "registered indexes" column lists the index names from the static config; the fetchable cell now shows `(no fetch resolver)` when at least one index exists but no `INDEX_REGISTRY` entry maps it. The second state matters because `plp meta lro.lamp.edr <pid>` works for those instruments — only `plp fetch` doesn't — and there was no way to discover that asymmetry from the catalog browse before.

### Fixed
- **`get_index()` / `plp meta` / `plp example_pid` now work on indexes with mixed-format time columns (LAMP).** `_convert_times` previously tried up to four format-detection strategies (auto / mixed / ISO 8601 / DOY) but each had to handle the *whole column* — fine for homogeneous columns, broken for indexes that mix per-row formats. LAMP's `START_TIME` has ISO calendar (`2009-07-06T12:47:18.250`), PDS DOY (`YYYY-DDDTHH:MM:SS`), and the occasional garbage value (`'0'`) all in one column. The chain failed at the strict-ISO step, the DOY fallback raised on the first non-DOY row, and the broad `except` in `convert_to_parquet` swallowed the error — symptom was `FileNotFoundError: …/CUMINDEX.parq` from `plp example_pid lro.lamp.edr` later, with the real cause hidden. Switched to a row-wise approach: `pd.to_datetime(errors="coerce", format="mixed")` parses what pandas can (turning unparseable rows to NaT), then a row-wise `_safe_doy` fills the NaTs with the DOY converter (NaT-tolerant — garbage that neither parser handles becomes NaT instead of aborting the whole conversion). Standard parser covers >99% of indexes; DOY is the targeted fallback for what's left.

## [0.59.3] - 2026-05-03

### Fixed
- **Stale catalog DB now fails fast with an actionable message.** When `~/planetarypy_data/catalog/<db>.duckdb` was built with planetarypy ≤ 0.52 (pre-PDS-catalog rewrite), its `product_types` table lacks the `normalized_type` column added in v0.53. Queries like `example_products` / `list_products(include_phases=True)` / `search` / `ambiguous_mappings` failed opaquely with `DuckDB Binder Error: Table 'pt' does not have a column named 'normalized_type'`. `get_catalog()` now runs `PRAGMA table_info(product_types)` after opening the connection and, if `normalized_type` is missing, raises:
  > `Stale catalog DB schema (built with planetarypy ≤ 0.52). Run `plp catalog build --force` to rebuild — the source-of-truth (pdr-tests + INDEX_REGISTRY) is unchanged, so no data is lost.`

  One gate covers every catalog query (every public function routes through `get_catalog()`).

## [0.59.2] - 2026-05-03

### Fixed
- **Reverts the v0.59.1 `fetch_product` index_key fallback.** That patch made `plp fetch lro.diviner.edr1 <pid>` succeed by mapping the index_key back to its catalog triple, but it hid a real ambiguity: `edr1`, `edr2`, and `edr` all returned the same row for the same `product_id` because `_load_index_df` already concatenates the two parquets and searches across both. The product physically lives in only one parquet; silently returning a "match" for the wrong index_key was incorrect. Restored the strict catalog-product_key contract for fetch.
- **Improved error when an index_key is passed where a catalog product_key was expected.** Replaces the misleading *"variable URL paths … no PDS index is available"* message (an index *was* available, just at the catalog product_key) with a direct pointer at the right key:
  > `'lro.diviner.edr1'` is an index_key (used by `plp meta` and `plp indexes`), not a catalog product_key. For fetching use `'lro.diviner.edr'`. See `plp indexes list lro.diviner` for the full index → catalog mapping.

  Inspection commands (`plp meta`, `plp indexes`, `plp example_pid`) keep their per-parquet index_keys; fetch operates on the catalog product_key. Different scopes = different keys, by design.

## [0.59.1] - 2026-05-03

### Fixed
- **`plp fetch` now accepts both index_keys and catalog product_keys.** The round-trip `plp fetch (plp example_pid <key>) <pid>` worked for instruments where the registered index_key equals the catalog product_key (`mro.ctx.edr`, `mro.hirise.edr`, …) but broke when one catalog product type was split across multiple parquets — Diviner's catalog `edr` triple maps to indexes `edr1` + `edr2`. `plp example_pid lro.diviner.edr1` would emit a valid PID but `plp fetch lro.diviner.edr1` failed with *"This product type has variable URL paths … no PDS index is available"* because `INDEX_REGISTRY` is keyed by catalog triple, not by index_key. New `_resolve_fetch_triple()` looks the dotted key up first as a catalog triple, then (if absent) as an `index_key` on every `IndexConfig.index_key` / `extra_index_keys` field. Both `catalog.fetch_product()` and `catalog.get_product_urls()` route through it.

## [0.59.0] - 2026-05-03

### Added
- **`plp catalog` browsing subcommands** — bring the (until now build-only) `plp catalog` namespace into parity with the API by exposing the existing `planetarypy.catalog.*` browse functions as CLI verbs. Each one renders a Rich table; the catalog browse cross-references the index registry so users can see at a glance which entries are fetchable.
    - `plp catalog list [KEY]` — three-level drill-down: no arg lists all 65 missions with per-mission counts and a `✓ fetchable` flag (set when at least one product type has an `INDEX_REGISTRY` entry); `KEY=mission` lists instruments with their fetchable variants; `KEY=mission.instrument` lists product types with their `index_key` mappings.
    - `plp catalog show <KEY>` — full info for a `mission.instrument.product_key` triple, including index_key / archive / SETI volume group / completion column / prefix-strip rule when fetchable, plus the catalog DB sample-products count.
    - `plp catalog search <QUERY>` — wraps `catalog.search()`.
    - `plp catalog summary` — wraps `catalog.summary()`.
    - `plp catalog ambiguous` — wraps `catalog.ambiguous_mappings()` (a tripwire surfacing pdr-tests folder names whose `(mission, instrument)` resolution falls through to the bare-name fallback at `_mission_map.py:1083` — empty today is the *healthy* signal).
- **`plp indexes` subtree** — new top-level namespace for browsing the *operational fetch surface* (the 78 registered PDS cumulative indexes from `~/.planetarypy_index_urls.toml`), kept visually separate from the `plp catalog` inventory tree to avoid confusing "what exists in PDS" with "what we can actually fetch".
    - `plp indexes list [KEY]` — three-level drill-down matching `plp catalog list`: missions → instruments → indexes. The `mission.instrument` level surfaces which indexes are cached locally with on-disk size and which have a catalog `product_key` entry. `--tree` falls back to the legacy `print_available_indexes()` tree.
    - `plp indexes info <KEY>` — `IndexConfig` + cache status for a single registered index, including remote URL, completion column, prefix-strip rule, archive base, and reverse-lookup catalog entries that map to this index.
    - `plp indexes refresh [--config|--cache <KEY>]` — explicit force-refresh of either the upstream `planetarypy_index_urls.toml` (normally auto-refreshed once per day) or a single index's cumulative `.lbl`/`.tab` + parquet rebuild.

### Fixed
- **`plp catalog` and `plp indexes` (bare invocation) now print help instead of erroring.** Both subapps lacked `no_args_is_help=True`, so `plp catalog` and `plp indexes` exited 2 with a "Missing argument" message rather than the subcommand list. Now mirrors the parent `app`'s behavior.

## [0.58.1] - 2026-05-01

### Fixed
- **`get_example_pid()` now returns the canonical user-facing PID form** (the same shape `complete_pid` caches and `get_meta` accepts), instead of stopping at `_bare_pid` (path/extension and version-suffix only). Indexes with an `IndexConfig.pid_strip_prefix_re` — currently the two cassini.iss variants — were emitting the un-stripped form: `plp example_pid cassini.iss.index` returned `1_N1454725799` while the canonical form is `N1454725799`, so the natural round-trip `plp meta cassini.iss.index (plp example_pid cassini.iss.index)` carried a stale prefix into the meta query. CTX, HiRISE, UVIS, etc. unchanged because they have no prefix-strip rule.

### Changed
- **Follow [planetarypy_configs#1](https://github.com/planetarypy/planetarypy_configs/pull/1) — Cassini canonical-key rename.** Three sections in the upstream `planetarypy_index_urls.toml` previously violated the `mission.instrument.indexname` dotted-key shape by encoding mission phase or activity mode in the instrument slot. After upstream merge, `INDEX_REGISTRY`'s `("cassini", "iss", "edr_evj")` entry now points at `cassini.iss.cruise_index` instead of `cassini.iss_cruise.index`. UVIS and VIMS occultation variants (`uvis_occ`, `vims_occ`) had no `INDEX_REGISTRY` entries to update — they were only reachable through the static-index registry — so they get the canonical key shape (`cassini.uvis.occ_index`, `cassini.vims.occ_profile_index`, etc.) for free on the next config refresh. Local users with cached parquets at `{storage_root}/cassini/{iss_cruise,uvis_occ,vims_occ}/...` will harmlessly orphan them and re-download under the new paths on first access (~50 MB across all three).

## [0.58.0] - 2026-04-30

### Added
- **`plp meta <key> <product_id>` and `planetarypy.pds.get_meta()`** — print the metadata row for a product from any registered PDS cumulative index, rendered as a Rich two-column table. Identifier (`*_ID`, `FILE_NAME`, `PATH_NAME`) fields are surfaced first, then `*ANGLE*` fields, then everything else. Matching is tolerant of case differences and PDS path/extension/version-suffix decoration. HiRISE EDR/RDR is special-cased via a per-instrument display registry: a bare obsid yields a short per-color summary across the observation (RED / BG / IR rows for `IMAGE_LINES`, `LINE_SAMPLES`, `SCALED_PIXEL_WIDTH`); `--long` returns the RED3_1 channel's full row; a channel-suffixed PRODUCT_ID returns that exact row. Non-instrument-specific indexes go through the generic path, so any registered index works out of the box.
- **`planetarypy.pds.read_index_slice(index_key, filters=None, columns=None)`** — column-projected, predicate-pushed-down parquet read for any registered index. Use this instead of `get_index()` when you need a few columns or rows: a per-obsid HiRISE EDR lookup goes from ~3.3 s (load all 86 MB / 2.6 M rows) to ~0.03 s (28 rows via row-group skipping) — a ~100× speedup. The HiRISE meta path uses this internally; full `plp meta` calls on HiRISE EDR drop from ~4 s to ~0.2 s (CTX `plp meta` unchanged at ~0.4 s).
- **`planetarypy.pds.complete_pid(incomplete, index_key)`** — generic shell tab-completion for any registered PDS index. Backed by a sorted text cache built lazily on first use from the index's configured completion column, with `_bare_pid` normalization (strips PDS path/extension and trailing `.NNN` version suffixes) and per-index prefix stripping (see `pid_strip_prefix_re` below). Replaces the per-instrument `complete_obsid()` / `complete_ctx_pid()` functions; CTX, HiRISE EDR/RDR, UVIS, ISS, and every other registered index now tab-complete uniformly. Lookups are sub-millisecond after the one-time cache build.
- **`planetarypy.instruments.mro.hirise.browse_url(product_id, annotated=True)`** — single source of truth for HiRISE EXTRAS browse-JPEG URL construction. `get_browse()` now calls it, removing duplicated URL formatting that previously lived in the `plp hibrowse` CLI command.
- **`planetarypy.instruments.mro.hirise.create_mosaics(obsid, colors=("red",), ccds=None, ...)`** — plural orchestrator that owns the per-color loop and the "ccds applies only to RED" rule. `plp himos` collapses to flag-mapping plus a single API call.
- **`local_dir` kwarg on `planetarypy.catalog.fetch_product()`** — overrides the storage path. The `plp fetch --here` / `-H` flag now maps cleanly through the public API instead of reaching into private resolver internals.
- **`IndexConfig.completion_id_col`** — column to surface for tab completion when it differs from `product_id_col`. HiRISE EDR/RDR/DTM set it to `OBSERVATION_ID` so users complete a 15-char obsid instead of one of the 28 channel-level PIDs per observation.
- **`IndexConfig.pid_strip_prefix_re`** — regex applied after `_bare_pid` to strip a per-index leading housekeeping prefix from both stored values and user input. cassini.iss / cassini.iss_cruise set it to `r"^.{1}_"` so the stored `1_N1454725799.122` and the typed `N1454725799` both normalize to the same canonical form.
- **Per-instrument meta-display registry** at `planetarypy.pds.meta_display.get_handler(index_key)` — routes `get_meta` to a custom shaping function when an index needs more than the generic two-column dump. Currently HiRISE EDR/RDR; the registry is the extension point for future instrument-specific summaries.

### Changed
- **`get_example_pid()` skips CRU-prefixed cruise PIDs.** Cruise-phase product IDs (e.g. CTX EDR's `CRU_000001_9999_XN_99N999W`) aren't representative examples of an instrument's typical mapping output. They're now treated like the existing `UNK` placeholder skip — preferred non-CRU rows, with fallback if every row is CRU. `plp example_pid mro.ctx.edr` now returns `MOI_000009_0186_XI_18S051W` instead of `CRU_000001_9999_XN_99N999W`.
- **CLI commands moved behind public APIs.** `plp fetch`, `plp hibrowse`, and `plp himos` no longer import private resolver/instrument symbols. CLI body is purely flag-to-API mapping plus output formatting; everything `plp` can do, the API can do directly. Per-instrument completion code (`complete_obsid`, `rebuild_obsid_cache`, `_obsid_cache_path` in `hirise.py`; `complete_ctx_pid`, `rebuild_pid_cache`, `_pid_cache_path` in `ctx_edr.py`) is removed in favor of the generic registry-driven path.

### Fixed
- **Eager-tuple performance bug in the generic `get_meta` matcher.** The three matching passes (exact / case-insensitive / bare-PID) were built as a tuple literal, so all three were always evaluated even when the cheapest one would have won — the per-row Python `apply(_bare_pid)` pass burned ~4 s per column on the 2.6 M-row HiRISE EDR index for nothing. Replaced with short-circuit if/elif and a sample-based "skip the apply pass entirely if neither side is decorated" probe.
- **HiRISE RDR meta queries no longer crash.** RDR has a different schema than EDR (post-mapping with `MAP_*` and `MINIMUM/MAXIMUM_LAT/LON` instead of `IMAGE_CENTER_*`; merged-color rows with no `CCD_NAME` / `CHANNEL_NUMBER` / `SCALED_PIXEL_WIDTH`), so the EDR-shaped column projection raised `ArrowInvalid: No match for FieldRef.Name(IMAGE_CENTER_LATITUDE)`. `format_meta` now routes EDR and RDR separately: RDR matches color-suffixed PIDs (`..._RED` / `..._COLOR` / `..._IRB`) directly and falls back to the obsid's `_RED` row for bare-obsid input.

## [0.57.0] - 2026-04-27

### Added
- **`list_products(<key>, include_phases=True)` now returns a `source` column** carrying the pdr-tests definition folder (e.g. `dawn__vir` vs `dawn_certified__vir`). Some instruments have parallel archive provenances for the same logical product type — the previous DataFrame projected only `normalized_type / phase / format / product_key`, so those rows looked like exact duplicates even though they pointed at different URL paths. Calling `list_products("dawn.vir", include_phases=True)` now plainly shows e.g. `edr / dawn__vir` and `edr / dawn_certified__vir` as the two distinct sources of an `edr` row.

## [0.56.0] - 2026-04-27

### Added
- **`plp fetch --folder` / `-d`** flag prints the local folder on stdout (single line) instead of the per-file absolute paths. Composes with shell `cd`:

    ```fish
    cd (plp fetch --folder mro.ctx.edr P02_001916_2221_XI_42N027W)
    ```

  Default behavior is unchanged — without the flag, `plp fetch` still emits one absolute file path per line, so `qgis (plp fetch …)` style multi-arg command substitution keeps working.

## [0.55.0] - 2026-04-27

### Changed
- **`planetarypy.catalog.fetch_product()` now returns a `DownloadedProduct` dataclass** instead of a bare `Path`. The CLI already printed absolute file paths to stdout (so `qgis (plp fetch …)` shell substitution worked) but the API silently discarded the file list and gave callers only the directory. The new bundle plumbs both pieces through:
    - `result.product_id` — canonical PID the resolver matched (post bare-PID normalization).
    - `result.local_dir` — `Path` to the folder.
    - `result.files` — `list[Path]` of every file actually written by this call (subset when `label_only=True` or an explicit `files=` filter is passed).
    - `result.label_file` — convenience pointer to the PDS `.LBL` / `.XML` if it was among the downloaded files, else `None`.
  
  **Migration**: callers that previously did `path = fetch_product(...)` should switch to `result.local_dir`. The new dataclass is also re-exported as `from planetarypy.catalog import DownloadedProduct` for type hints / `isinstance` checks.

## [0.54.0] - 2026-04-27

### Added
- **`planetarypy.pds.get_example_pid(instr_key)`** — generic helper that returns a sample product ID for any index registered in `~/.planetarypy_index_urls.toml` (or the dynamic handler registry). Useful as a seed for `plp fetch` demos, notebook examples, smoke tests, and tab-completion fixtures — previously each instrument module had to ship its own ad-hoc example PID. Resolves the product-id column via the catalog `INDEX_REGISTRY` when available (so non-standard cases like `cassini.uvis` using `FILE_NAME` as the PID column are handled correctly), then falls back to `PRODUCT_ID` / `FILE_NAME` / `IMAGE_ID` / `OBSERVATION_ID`. Skips `"UNK"` placeholder rows (e.g. early Galileo SSI cruise frames whose `PRODUCT_ID` is literally `"UNK"`) but degrades gracefully if every row is UNK. Raises `ValueError` on unknown index keys.
- **`plp example_pid <key>`** — CLI surface for the same. Prints the PID to stdout (so it composes with `plp fetch`), exits non-zero on unknown keys, and supports tab-completion over the registered dotted index keys.

### Changed
- **PIDs are now normalized to a bare canonical form** at both ends of the round trip. `get_example_pid` and the catalog `_find_product_in_index` / `resolve_from_index` go through a shared `_bare_pid` normalizer so `plp example_pid <key>` output composes directly with `plp fetch <key> <pid>`. Two-step rule:
    1. If the value's basename ends in a known PDS file extension (`.LBL .IMG .TAB .DAT .FIT .JP2 .QUB .XML`), strip path + extension. (e.g. `cassini.uvis.index` `FILE_NAME` of `/COUVIS_0001/.../EUV1999_007_17_05.LBL` → `EUV1999_007_17_05`.)
    2. Else strip a trailing `.<digits>` version suffix only. (e.g. `cassini.iss.index` PRODUCT_ID `1_N1454725799.122`, where `.122` is the FLIGHT_SOFTWARE_VERSION_ID, → `1_N1454725799`.)
    3. Else preserve the value verbatim — keeps slashes intact when they're PID separators rather than paths (e.g. `mgs.moc.edr` `FHA/00435`, `cassini.vims.index` `1/1294638283_1`). An earlier naïve form mishandled this and would have collapsed 7677 distinct MGS MOC PIDs into a single bare form.
- `ResolvedProduct.product_id` returned from `resolve_from_index` is now also the bare form, so the per-product storage folder created by `_local_product_dir` no longer contains nested archive paths for indexes whose PID column stores a full FILE_NAME.

### Fixed
- **`src/planetarypy/__init__.py.__version__`** was stuck at `0.41.2` because no `[[tool.bumpversion.files]]` entries existed in the existing `[tool.bumpversion]` config; recent releases (0.53.5–0.53.7) bumped only `pyproject.toml`. Resynced and now wired so future `bump-my-version bump <part>` runs keep both files in sync.

## [0.53.7] - 2026-04-24

### Fixed
- **Race-safe parquet/csv cache writes.** When parallel test workers (pytest-xdist) both triggered a first-time PDS index download (e.g. `get_index("mro.hirise.rdr")` from `test_spicer.TestSolarAzimuth`), two workers would finish downloading the `.lbl` + `.tab` files and simultaneously call `df.to_parquet(path)` into the same file. The non-atomic write produced a torn parquet, with the next reader hitting `OSError: Couldn't deserialize thrift: TProtocolException: Invalid data`. Same class of race existed for the SPICE archived-kernels `datasets.csv` cache via `df.to_csv(path)`. Both are now routed through a new `planetarypy.utils.atomic_write` context manager: each writer writes to a per-PID scratch file and atomically renames into place; the first concurrent finisher wins, later finishers silently drop their copy. Verified under an 8-thread stress test.

### Added
- `planetarypy.utils.atomic_write(path)` — context manager yielding a per-PID scratch `Path`; on clean exit, atomically renames it onto `path`. Reusable for any library-level cache write that can race.

## [0.53.6] - 2026-04-24

### Fixed
- `plp ctx-migrate` now walks each `mrox_*` volume recursively, so files already nested in `<pid>/` subfolders are counted as "already in place" instead of being silently skipped. Before this fix, a post-migration re-run reported `already in place: 0`, which looked alarming even though no moves were needed. After: the summary reflects the actual number of pid-matching files found.

## [0.53.5] - 2026-04-24

### Added
- **Separate mirror/local config for CTX EDR in `~/.planetarypy_mro_ctx.toml`.** The old single `[edr]` section conflated three things (mirror layout, local layout, download URL) under one set of `with_volume` / `with_pid` toggles, so users couldn't e.g. keep the shared read-only mirror in canonical PDS layout while storing new downloads next to their calib outputs. Two new optional sub-tables decouple them:

  ```toml
  [edr]
  url = "https://pds-imaging.jpl.nasa.gov/data/mro/ctx"

  [edr.mirror]                              # read-only; may be unmounted
  path = "/Volumes/planet/Mars/CTX/pds"
  with_volume = true
  with_pid = false
  with_data_segment = false                 # PDS canonical "<vol>/data/<pid>.IMG"

  [edr.local]                               # writeable; where new downloads go
  path = ""                                 # "" → {storage_root}/mro/ctx
  with_volume = true
  with_pid = true                           # co-locate raw EDR with calib outputs
  ```

  The legacy flat shape (`local_mirror`, `local_storage`, top-level `with_volume` / `with_pid`) is still read transparently — no user TOML edit is required. Opt in by adding the sub-tables when ready.

- **`plp ctx-migrate [--dry-run]`** — one-shot utility that walks each `mrox_*` volume folder under the configured EDR local root and relocates any file named `<pid>.<ext>` (26-char CTX product_id, so `.IMG`, `.cub`, `.lev1.cub`, `.lev2.cub`, `.lev2.tif`, `.lev1.gml`, `.csm2map.tif`, etc.) to whatever layout the active config dictates. Idempotent; conflicts are skipped with a warning rather than overwriting.

### Changed
- Internal CTX path helpers refactored from `ctx_storage_folder(level, …)` + `_level_base(level)` into three small readers `_edr_mirror_folder` / `_edr_local_folder` / `_calib_folder` plus a 7-line `_apply_toggles` helper. No external API change for `EDR` / `Calib` callers.

## [0.53.4] - 2026-04-24

### Fixed
- **`url_retrieve` is now concurrency-safe.** Previously two processes (e.g. parallel pytest-xdist workers hitting `load_generic_kernels()` via `Spicer("MARS")` / `Spicer("MOON")`) could clobber each other's `.part` scratch file and race on the final `rename()`, producing `FileNotFoundError`. The scratch file now includes the writer's PID (`{name}.{pid}.part`), and when a concurrent winner has already moved the final file into place the loser silently drops its scratch copy instead of raising. This was the root cause of intermittent CI failures in `test_spicer`.

### Changed
- CI workflow `test.yaml` now prefetches the SPICE generic kernels in a single-writer step before invoking `pytest`, so parallel test workers see cached files and never trigger the download path simultaneously. Complements the `url_retrieve` fix; either alone would green CI, both together harden the library for any concurrent caller.

## [0.53.3] - 2026-04-24

### Fixed
- **`plp fetch` and `plp hibrowse` now emit only the resolved file path on stdout.** Diagnostic lines ("Resolving…", "URL:…", "Fetching…") and the "Browse:" prefix previously mixed with the final path on stdout, which made shell command substitution clumsy (e.g. `qgis (plp fetch mro.ctx.edr <pid>)` captured all of it as arguments). Diagnostics now go to stderr; only the payload path hits stdout.

## [0.53.2] - 2026-04-24

### Fixed
- **CTX storage path mismatch between `plp fetch` and `ctxqv`/`EDR`.** `plp fetch mro.ctx.edr <pid>` previously wrote to `{storage_root}/mro/ctx/edr/<pid>/` (the catalog's generic layout), while `EDR(pid).local_storage_folder` (used by `plp ctxqv` and programmatic access) wrote to `{storage_root}/mro/ctx/<volume>/…` per `~/.planetarypy_mro_ctx.toml`. Downloads from one code path were invisible to the other, causing redundant re-downloads. Both paths now resolve through the single `ctx_storage_folder(level, volume, pid)` helper and land in the same directory.

### Changed
- CTX storage layout for `plp fetch mro.ctx.edr` now follows `~/.planetarypy_mro_ctx.toml` (`[edr].local_storage`, `[edr].with_volume`, `[edr].with_pid`) instead of the generic catalog fallback. A new `_ctx_local_product_dir` resolver is registered in `planetarypy.catalog._resolver._STORAGE_RESOLVER_MODULES`.
- CTX config (`CTXCONFIG`, mirror reachability) is now read lazily on each access rather than snapshotted at import — mounting or unmounting the local mirror mid-session is reflected immediately, and `with_volume` / `with_pid` config edits take effect without re-importing.
- `EDR.with_volume`, `EDR.with_pid`, `Calib.with_volume`, `Calib.with_pid` are now `@property` reads of `CTXCONFIG` (previously snapshotted in `__init__`).

### Removed
- Shadow duplicate `EDR` class in `planetarypy.instruments.mro.ctx.ctx_calib` (leftover from the original module split). `EDR` is now defined once in `ctx_edr.py` and imported from there.
- Module-level globals `STORAGE_ROOT`, `EDR_LOCAL_STORAGE`, `EDR_LOCAL_MIRROR`, `MIRROR_READABLE`, `MIRROR_WRITEABLE` in `ctx_edr.py` and their lowercase counterparts in `ctx_calib.py`. Replaced by lazy accessors (`_storage_root`, `_edr_local_mirror`, `_mirror_readable`, `_level_base`).

### Migration notes
- If you previously ran `plp fetch mro.ctx.edr <pid>` and have existing downloads under `{storage_root}/mro/ctx/edr/<pid>/`, they will **not** be picked up by the new layout. Either move them to `{storage_root}/mro/ctx/<volume>/[<pid>/]` (per your `[edr].with_volume` / `[edr].with_pid` toggles), delete them, or re-run `plp fetch` to download into the new location.

## [0.52.2] - 2026-04-13

### Fixed
- Only show satellite ephemeris download message when actually downloading (not when loading from cache)

## [0.52.1] - 2026-04-13

### Fixed
- Spicer now works for outer solar system bodies (Jupiter, Saturn, Neptune, Pluto systems) — satellite ephemeris SPKs are downloaded on demand when needed
- Graceful fallback in CLI when SPICE ephemeris data is missing (shows what it can instead of crashing)
- Suppress pvl PendingDeprecationWarning (pvl#109)

## [0.52.0] - 2026-04-13

> *Dedicated to the memory of Candice J. Hansen — scientist, mentor, and friend. This release was built in a single long push fueled by the urgency that reminds us our time to contribute is finite.*

### Added
- **Spicer class** (`planetarypy.spice.spicer`): surface illumination calculator for any solar system body
  - `Spicer("MARS").illumination(lon, lat, time)` — solar incidence, flux, L_s, local time
  - `slope` and `aspect` parameters for tilted surface flux (south-facing slopes etc.)
  - `solar_azimuth_at(lon, lat, time)` — SPICE-computed solar azimuth, validated to <1° against HiRISE index
  - `.Ls` property for current solar longitude
  - `sun_direction_at()` for azimuth calculation via Point class
  - `illumination_at(point)` integration with `planetarypy.geo.Point`
  - `supported_bodies()` — discover all bodies available from loaded kernels (79 from generic PCK)
  - `units=True` toggle for astropy Quantity output
  - Rotation via scipy (matching SPICE right-hand convention)
- **CLI**: `plp spicer Mars` — current L_s, subsolar point, solar constant; add `--lon --lat` for surface illumination
- Spicer tutorial with diurnal flux curve, slope/aspect comparison, multi-body demo, and HiRISE index validation

## [0.51.0] - 2026-04-13

### Added
- **Geospatial module** (`planetarypy.geo`): GDAL-free coordinate transforms built on rasterio + pyproj
  - `pixel_to_xy`, `xy_to_pixel`, `pixel_to_lonlat`, `lonlat_to_pixel`, `xy_to_lonlat`, `lonlat_to_xy`
  - `is_within(source, lon, lat)`: check if coordinates fall within an image
  - `image_azimuth`: clockwise from north (standard planetary science)
  - `image_azimuth_cw_from_right`: clockwise from 3 o'clock (HiRISE convention)
  - `pixel_resolution`: pixel size from affine transform
  - Works with IAU 2015 planetary CRS codes (Mars, Moon, any solar system body)
- **Point class** (`planetarypy.geo.Point`): CRS-aware geographic point
  - Create from lon/lat, pixel coordinates, or projected coordinates
  - `.to_xy()`, `.to_pixel()`, `.is_within()`, `.azimuth_to()`, `.to_shapely()`
  - Auto-resolves pixel↔lonlat when source DataArray is provided
- **Plotting module** (`planetarypy.plotting`): visualization helpers
  - `imshow_gray`: grayscale image display with percentile stretch
  - `percentile_stretch`: reusable stretch calculation
  - `add_sun_indicator`: sun direction overlay on any axes
  - `imshow_with_sun`: combined image display + sun indicator
- **HiRISE instrument module** (`planetarypy.instruments.mro.hirise`)
  - `get_browse(pid, annotated=True)`: download browse JPEG (annotated or clean)
  - `get_metadata(pid)`: look up index metadata
  - `sun_azimuth_from_top(pid)`: convert HiRISE CW-from-right to CW-from-top
- **CLI**: `plp hibrowse --annotated/--clean` option for browse variant
- **Geospatial tutorial**: pixel↔lonlat transforms, IAU CRS codes, Point class, sun indicator verification with real HiRISE data (ESP_013807_2035)
- `rasterio`, `pyproj`, `rioxarray` added to core dependencies

### Changed
- `plp hibrowse` and `plp hifetch` now use `planetarypy.instruments.mro.hirise` module instead of inline CLI helpers
- `plp ctxqv` uses `planetarypy.plotting.imshow_gray` instead of duplicating stretch logic

## [0.50.1] - 2026-04-13

### Fixed
- Move duckdb from optional `[catalog]` extra to core dependency (was breaking installs)
- Pandas 2.x compatibility: datetime64 resolution (ns→us) and string dtype (object→StringDtype) in test assertions
- Mock catalog DB in tests that hit `get_catalog()` (fixes CI without a built catalog)
- Stale docs: rewrite pds_index_config explanation, update landing page, fix tutorial tier numbering

### Added
- Logo in docs navbar and browser favicon

## [0.50.0] - 2026-04-13

> **Breaking change:** This is a ground-up rewrite with a new API.
> The previous version (0.32.x) based on nbplanetary remains at
> [github.com/michaelaye/nbplanetary](https://github.com/michaelaye/nbplanetary).

### Added
- **PDS Catalog module** (`planetarypy.catalog`): comprehensive index of ~2000 product types across 200+ instruments from the entire PDS archive, built from MillionConcepts pdr-tests repository into a local DuckDB database
  - `build_catalog()` to clone pdr-tests and populate the database
  - Query API: `list_missions()`, `list_instruments()`, `list_products()`, `example_products()`, `search()`, `summary()`
  - Dotted key access: `list_products("mro.hirise")`, `example_products("cassini.iss.edr_sat")`
  - 150+ manual mission/instrument mappings for pdr-tests folder names
  - AST-based parser for selection_rules.py (no code execution)
  - Multi-instrument folder splitting: folders like `mro` correctly split into `ctx`, `hirise`, `marci`, `mcs` instruments based on product key prefixes; infix matching for Rosetta-style `EDR_instrument` keys
  - Product key normalization: decompose keys like `edr_sat`, `sat_rdr_asc` into 3 dimensions — `normalized_type` (data type), `phase` (target body/mission phase), `format` (ascii/binary/coordinate system). Recognized phases include planets (saturn, jupiter, neptune, uranus, earth, pluto), minor bodies (ceres, vesta, gaspra, ida, arrokoth, phobos, halley), and mission phases (cruise, launch, kem_cruise)
  - `Mission` and `Instrument` objects with human-readable full names for all 65 missions and ~180 instruments (e.g. `Mission("mro").full_name` → "Mars Reconnaissance Orbiter", `mro["ctx"]` → "Context Camera")
  - Rosetta Lander (Philae) as separate mission entry with 8 properly split instruments
  - Voyager POS ephemeris override: standalone body keys normalized to `"ephemeris"` for pre-SPICE position data
  - Generic instrument groupings (spectrometers, particles, plasma, probe, dust, lander) decomposed into real instrument names across Galileo, NEAR, Rosetta, Voyager, Dawn, Deep Impact
  - `_misc` instruments hidden from `list_instruments()` by default; accessible via `include_misc=True` or `Mission.misc`
  - URL rewrite for broken USGS Imaging Node URLs (60 of 69 rewritten to SETI Rings and JPL Planetary Data mirrors)
  - Product download API: `fetch_product("mission.instrument.type", product_id)` downloads files and returns local path, `get_product_url()` returns remote URL, `list_product_files()` returns file-to-URL mapping
  - Index-backed resolution (Tier 2): 58 product types across 29 instruments on 15 missions — arbitrary product IDs resolved via PDS cumulative indexes for CTX, HiRISE, Cassini ISS, Galileo SSI, LROC, Diviner, CRISM, LOLA, Cassini UVIS/VIMS/CIRS, Voyager 1&2 ISS, Juno JunoCam, New Horizons LORRI, MER Pancam, MGS MOC, Viking VIS, MESSENGER MDIS, Cassini RSS, Phoenix MECA instruments (WCL/AFM/TECP/ELEC), and MSL (APXS, ChemCam, CheMin, SAM)
  - Pattern-based URL resolution (Tier 3): for product types with fixed `url_stem`, resolve arbitrary product IDs without needing a PDS index
  - Per-archive URL construction: `IndexConfig` supports `path_name_col` (for FILE_NAME + PATH_NAME split indexes), `lowercase_paths`/`lowercase_files` flags, `volume_id_col=""` to skip volume in URL, and `seti_volume_group="auto"` to derive SETI volume groups dynamically
  - Verified HTTP 200 for all 50 testable registry entries (7 indexes not yet downloaded, 1 removed)
  - Explanation doc: `docs/explanation/product_url_resolution.qmd` with full direct data access status table
  - Tutorial notebook in `docs/tutorials/pds_catalog_tutorial.ipynb`
- **Unified CLI** (`plp`): single entry point built on typer
  - `plp fetch mro.ctx.edr PRODUCT_ID` — download any product by dotted key
  - `plp fetch --here` — download into current directory
  - `plp hibrowse PSP_003092_0985_RED` — fetch HiRISE browse JPEG from EXTRAS, opens in Preview on macOS
  - `plp hifetch PSP_003092_0985_RED` — fetch full HiRISE data product
  - `plp ctxqv J05_046771_1950` — CTX quickview with strided memmap
  - `plp catalog build` — build/rebuild the catalog database
- **CTX quickview** (`EDR.quickview`, `Calib.quickview`): memory-mapped strided reads for fast previews
- Dynamic URL handlers for LRO LAMP EDR and RDR indexes (volume-based URLs at JPL)
- Backup URL fallback for CTX index (pdsimage2.wr.usgs.gov)
- Quarto documentation with Diátaxis framework structure
- New how-to guides: CTX calibration, CTX EDR, ISIS autoseed
- Comprehensive API reference documentation
- `slow` pytest marker for test filtering
- SPICE datasets daily caching system
- Case-insensitive mission name resolution for SPICE

### Changed
- Renamed `list_product_types()` → `list_products()` in catalog API
- Restructured catalog internals: `_download.py` → `_resolver.py`, `_index_bridge.py` → `_index_resolver.py`, `_url_patterns.py` + `_url_examiner.py` → `_pattern_resolver.py`
- Replaced click with typer for CLI; single `plp` entry point replaces `plp_update_indexes`, `plp_build_catalog`, `ctxqv`
- Migrated documentation from Sphinx to Quarto
- Split ctx.py into separate EDR and calibration modules
- Renamed HISTORY.md to CHANGELOG.md
- Adopted Keep a Changelog format

### Removed
- Dead code: `exceptions.py` (unused), `pds/cli.py` (broken imports), `scripts/` package, old Click-based CLIs

### Fixed
- Index bridge URL construction: correct case handling per archive server (lowercase paths for JPL/WUSTL, preserve case for SETI Rings/HiRISE), proper volume_id/path_name column support, and use actual filenames from index instead of guessing extensions
- Ambiguous product resolution: raises `MultipleProductsError` when an ID matches multiple products (e.g. HiRISE observation matching both RED and COLOR)
- Canonical PID casing: local storage paths use the index's canonical case, not the user's input
- Removed LRO LAMP from index registry (index lacks volume mapping needed for URL construction)
- Cloudflare 403 errors via User-Agent header in remote timestamp checks
- Repeated Index instantiations in dynamic handler
- Test failures from outdated DataFrame schema

## [0.41.2] - 2025-01-26

### Added
- pooch dependency for data downloads

## [0.41.1] - 2025-01-26

### Fixed
- Repeated Index instantiations in dynamic index handler

## [0.1.0] - 2020-10-20

### Added
- First release on PyPI
- PDS index management system
- SPICE kernel utilities
- Basic configuration management

---

## Historical Versions (Previous Project Incarnations)

> **Note:** The following versions are from predecessor projects that evolved into
> the current planetarypy. They are included for historical reference and are not
> installable from this repository.

### nbplanetary era (2021-2025)

Using nbdev (notebook-driven development) for a complete rewrite.

- **v0.27.0** (2023-06) - BepiColombo SPICE kernels, PDW2023 tutorials
- **v0.26.0** (2023-03) - CRISM and LROC index handlers
- **v0.25.0** (2023-02) - Spicer class, ISIS integration via kalasiris
- **v0.21.x** (2022-02) - Full instrument modules: CTX, HiRISE, UVIS, CISS, Diviner

### Original planetarypy era (2015-2020)

Private collection of planetary science tools, inspired by astropy's organization.

- **v0.9.0** (2020-07) - North azimuth and sun angle calculations
- **v0.8.0** (2020-07) - GeoTools module from pymars (coordinate transforms)
- **v0.7.0** (2020-06) - url_retrieve with progress bar and timeout
- **v0.6.0** (2020-05) - Powerful Index class, config system
- **v0.5.0** (2019-07) - Renamed to planetarypy (from planetpy)
- **v0.4.0** (2019-03) - PDS index download CLI, Cassini ISS indices
- **v0.1** (2015-04) - NASA factsheet planetary constants parser
