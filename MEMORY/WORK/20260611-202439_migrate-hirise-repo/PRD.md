---
task: Migrate HiRISE-specific code to new planetarypy-hirise repo
slug: 20260611-202439_migrate-hirise-repo
effort: advanced
phase: observe
progress: 0/27
mode: interactive
started: 2026-06-11T20:24:39Z
updated: 2026-06-11T20:24:39Z
---

## Context

The user wants to extract all HiRISE-specific code out of the core `planetarypy`
package into a new standalone `planetarypy-hirise` repo/package. This is the first
concrete consumer of the **instrument-extension seams** introduced in v0.74.0, which
already provide the hooks an external instrument package needs:

- `planetarypy.catalog.register_storage_resolver(key, fn)` — storage/URL resolver hook
- `planetarypy.catalog._index_resolver` public hook (`_index_resolver.py:608`) — add product types
- `[project.entry-points."planetarypy.cli_plugins"]` (cli.py:3310) — CLI verb plugins
- `pds/meta_display.py:32` — already carries a "Transitional fallback (removed once
  HiRISE self-registers...)" comment, i.e. the migration was anticipated.

### Current HiRISE surface in core (what must move or de-entangle)

- `src/planetarypy/instruments/mro/hirise.py` (1192 lines) — the module itself:
  `get_browse`, `get_metadata`, `sun_azimuth_from_top`, `browse_url`, `download_edr`,
  `edr_products`, `format_meta`, `_hirise_local_product_dir`.
- `cli.py` HiRISE verbs: `hibrowse`, `hiedr`, `himos`, `hifetch` (+ obsid tab-completion).
- `catalog/_index_resolver.py` — `IndexConfig` entries for `mro.hirise.{edr,rdr,dtm}`.
- `catalog/_resolver.py:185` — `DEFAULT` registration of `mro.hirise` → hirise module.
- `pds/meta_display.py:32-35` — transitional fallback to `hirise.format_meta`.
- `tests/test_hirise.py` (276 lines) + scattered hirise references in other tests.
- `catalog/_mission_map.py`, `_objects.py` — hirise name/instrument mappings (likely STAY
  in core: catalog is a global registry, not instrument-specific behavior).

### Generic vs HiRISE-specific (stays in core)

The generic "short product ID" / prefix-expansion mechanism (`pds.resolve_pids`,
`get_index(prefix=True)`) is explicitly instrument-free and STAYS. The catalog name
registry (mission_map, objects) is a global discovery surface and likely STAYS. Only
*behavior* (download, browse, metadata shaping, CLI verbs, azimuth conversion) moves.

### Gating check (user said "if no burning open action item")

Reviewed: ROADMAP has priorities but nothing burning; working tree clean of pending code
(only untracked CLAUDE.html + an explanation .qmd); no urgent TODO/FIXME in src; the
conda-forge PR is awaiting external reviewer (not actionable here). → No burning item.
Proceeding with the migration.

### Open decisions (must resolve before EXECUTE — surfaced to user)

1. **Packaging model**: PEP 420 namespace package (`planetarypy.instruments.hirise`,
   pip name `planetarypy-hirise`) vs. flat top-level package (`planetarypy_hirise`).
2. **Scope this session**: scaffold the new repo + move code now, or plan-only this turn.
3. **Docs**: new repo gets its own Quarto site (user: always load Quarto skill for docs).
4. **Core cleanup timing**: remove transitional fallbacks from core in the same cycle,
   or leave them until planetarypy-hirise is published (avoids breaking `pip install
   planetarypy` users mid-migration).

## Criteria

### A. New repo scaffold
- [ ] ISC-1: New repo `planetarypy-hirise` created with src/ layout
- [ ] ISC-2: pyproject.toml declares dependency on planetarypy core
- [ ] ISC-3: pyproject declares `planetarypy.cli_plugins` entry point for HiRISE verbs
- [ ] ISC-4: Package version + changelog + license + README present
- [ ] ISC-5: Hatchling build config mirrors core conventions

### B. Code moved
- [ ] ISC-6: hirise.py module relocated into new package
- [ ] ISC-7: `get_browse`/`browse_url` import-clean in new location
- [ ] ISC-8: `download_edr`/`edr_products` import-clean in new location
- [ ] ISC-9: `get_metadata`/`format_meta` import-clean in new location
- [ ] ISC-10: `sun_azimuth_from_top` import-clean in new location
- [ ] ISC-11: `_hirise_local_product_dir` resolver relocated

### C. Self-registration via seams
- [ ] ISC-12: Package registers storage resolver for mro.hirise on import
- [ ] ISC-13: Package registers index-resolver configs for edr/rdr/dtm
- [ ] ISC-14: Package registers meta_display formatter via the seam (no core fallback)
- [ ] ISC-15: CLI verbs hibrowse/hiedr/himos/hifetch load via entry point

### D. CLI verbs moved
- [ ] ISC-16: hibrowse verb relocated to plugin module
- [ ] ISC-17: hiedr verb relocated to plugin module
- [ ] ISC-18: himos verb relocated to plugin module
- [ ] ISC-19: hifetch verb relocated to plugin module
- [ ] ISC-20: obsid tab-completion relocated with the verbs

### E. Core de-entangled
- [ ] ISC-21: core hirise.py removed (or reduced to nothing importable)
- [ ] ISC-22: core CLI no longer defines HiRISE verbs directly
- [ ] ISC-23: pds/meta_display transitional fallback removed
- [ ] ISC-24: catalog _resolver DEFAULT hirise registration removed
- [ ] ISC-A1: `pip install planetarypy` (no hirise) still imports + CLI runs clean

### F. Tests + verification
- [ ] ISC-25: test_hirise.py moved to new repo, passes there
- [ ] ISC-26: core test suite green after hirise removal
- [ ] ISC-27: end-to-end `plp hibrowse <pid>` works with both packages installed

## Decisions

(pending user input on the 4 open decisions above)

## Verification

(pending)
