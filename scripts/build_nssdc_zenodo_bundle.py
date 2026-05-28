"""Assemble the NSSDC archive bundle ready for Zenodo upload.

Creates ``build/nssdc_archive_v<N>/`` containing every artifact a researcher
or downstream tool needs to (a) consume the parsed data directly,
(b) re-derive it from the raw HTML, or (c) cite the dataset:

    nssdc_archive_v1/         (FLAT — Zenodo's web UI doesn't support folders)
    ├── README.md                       provenance, schema, citation
    ├── parsed_archive.json.gz          canonical structured data — what
    │                                   planetarypy.constants.nssdc loads
    ├── parsed_archive.csv.gz           long-format CSV mirror, for R / Excel
    ├── raw_html.tar.gz                 913 original Wayback HTML captures,
    │                                   gzipped per-file then tarred
    ├── manifest.json                   CDX provenance: timestamps, digests,
    │                                   Wayback URLs per capture
    └── nssdc_scripts.tar.gz            fetch + parse + regenerate Python
                                        scripts, stdlib-only

Maintainer-only build-time script. Output is uploaded to Zenodo by hand;
the resulting DOI is then pinned in
``src/planetarypy/constants/nssdc/_loader.py``.

Usage::

    python scripts/build_nssdc_zenodo_bundle.py
"""

from __future__ import annotations

import csv
import gzip
import json
import shutil
import sys
import tarfile
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
NSSDC_DIR = REPO_ROOT / "src/planetarypy/constants/nssdc"
RAW_DIR = NSSDC_DIR / "_raw"
PARSED_PATH = NSSDC_DIR / "parsed_archive.json.gz"
MANIFEST_PATH = RAW_DIR / "manifest.json"
SCRIPTS_DIR = REPO_ROOT / "scripts"

OUT_ROOT = REPO_ROOT / "build"

SCRIPT_NAMES = (
    "fetch_nssdc_archive.py",
    "parse_nssdc_archive.py",
    "regenerate_constants.py",
)

# Zenodo deposit version (semver applied to the dataset itself; bump per
# upload — MAJOR=breaking schema, MINOR=additive optional keys, PATCH=
# data-quality fixes only). Distinct from the JSON schema version
# embedded in parsed_archive.json.gz, which tracks only breaking shape
# changes.
DEPOSIT_VERSION = "1.1.0"

# Zenodo concept DOI — always redirects to the latest version of the
# deposit. Cited in the README for downstream papers. Distinct from the
# per-version record ID pinned in
# src/planetarypy/constants/nssdc/_loader.py (ZENODO_RECORD_ID), which
# is what the runtime loader actually downloads from and uses as the
# cache invalidation key.
CONCEPT_DOI = "10.5281/zenodo.20122986"


def _archive_schema_version() -> str:
    """Read the JSON schema version from the parsed archive's own metadata."""
    with gzip.open(PARSED_PATH, "rt", encoding="utf-8") as f:
        return json.load(f)["version"]


def _build_csv(parsed_path: Path, csv_gz_path: Path) -> int:
    """Emit a long-format CSV (one row per body/capture/field). Returns row count."""
    with gzip.open(parsed_path, "rt", encoding="utf-8") as f:
        archive = json.load(f)

    cols = (
        "body", "wayback_timestamp", "page_date", "wayback_url",
        "field", "value", "raw_value", "unit",
    )
    n_rows = 0
    with gzip.open(csv_gz_path, "wt", encoding="utf-8", newline="") as gz:
        writer = csv.writer(gz)
        writer.writerow(cols)
        for body, sheet in sorted(archive["fact_sheet"].items()):
            for cap in sheet["captures"]:
                base = (
                    body, cap["wayback_timestamp"],
                    cap.get("page_date") or "",
                    cap["wayback_url"],
                )
                for field, data in sorted(cap["fields"].items()):
                    writer.writerow(base + (
                        field,
                        "" if data.get("value") is None else data["value"],
                        data.get("raw") or "",
                        data.get("unit") or "",
                    ))
                    n_rows += 1
    return n_rows


def _build_raw_tarball(raw_dir: Path, out_path: Path) -> int:
    """Tar+gz the per-body raw HTML directories. Returns file count."""
    n_files = 0
    with tarfile.open(out_path, "w:gz", compresslevel=6) as tar:
        # Add manifest.json at the top of the tar
        tar.add(MANIFEST_PATH, arcname="manifest.json")
        # Then each body directory
        for body_dir in sorted(p for p in raw_dir.iterdir() if p.is_dir()):
            for html_file in sorted(body_dir.glob("*.html.gz")):
                tar.add(
                    html_file,
                    arcname=f"{body_dir.name}/{html_file.name}",
                )
                n_files += 1
    return n_files


def _write_readme(bundle_dir: Path, deposit_version: str, schema_version: str,
                  concept_doi: str, archive_meta: dict,
                  n_csv_rows: int, n_raw_files: int) -> None:
    """Generate a citation-friendly README inside the bundle.

    ``deposit_version`` is the Zenodo deposit revision (semver applied to
    the dataset); ``schema_version`` is the JSON shape version inside
    parsed_archive.json.gz and only bumps on breaking shape changes;
    ``concept_doi`` is the Zenodo concept DOI (always-latest redirect),
    used for the README's citation block.
    """
    body_summary = "\n".join(
        f"- **{body}**: {len(sheet['captures'])} captures"
        for body, sheet in sorted(archive_meta["fact_sheet"].items())
    )
    today = datetime.now(timezone.utc).date().isoformat()
    readme = f"""# NASA NSSDC Planetary Fact Sheet Archive (v{deposit_version})

A curated longitudinal archive of NASA's National Space Science Data
Center (NSSDC) per-body planetary fact sheets at
<https://nssdc.gsfc.nasa.gov/planetary/factsheet/>, captured via the
Internet Archive's Wayback Machine and parsed into a structured
time-indexed dataset.

## Background

The NSSDC at NASA's Goddard Space Flight Center maintains the canonical
per-body planetary fact sheets (D. R. Williams, since 1996) — the
"textbook parameters" reference for solar-system bodies:
albedo, surface pressure, scale height, satellite count, orbital
elements, etc. The pages have been periodically revised since 1996, but
no longitudinal dataset of value drift over time existed publicly. This
archive fills that gap.

## Bundle contents

- `parsed_archive.json.gz` — canonical structured data
  (39 fields × ~700 numeric captures × 11 bodies, plus the asteroid
  and comet summary sheets).
- `parsed_archive.csv.gz` — long-format CSV mirror of the same data
  ({n_csv_rows:,} rows), one row per `(body, capture, field)` tuple.
- `raw_html.tar.gz` — {n_raw_files} original Wayback HTML captures,
  gzipped per-file then tarred. Each filename encodes its Wayback
  capture timestamp: `<body>fact_<YYYYMMDDhhmmss>.html.gz`.
- `manifest.json` — Wayback CDX provenance: per-capture timestamp,
  content digest, and `web.archive.org/web/<TS>id_/...` URL so the
  exact same capture can be re-fetched.
- `nssdc_scripts.tar.gz` — three Python scripts (stdlib-only, no
  external deps for fetch + parse) that re-derive everything from
  scratch:
  - `fetch_nssdc_archive.py` (Wayback CDX walk + per-capture fetch)
  - `parse_nssdc_archive.py` (HTML → structured JSON)
  - `regenerate_constants.py` (NSSDC + NAIF SPICE PCK merge — used by
    the planetarypy package for code generation; not strictly part of
    this dataset but included for reproducibility).

## Per-body coverage

{body_summary}

## Schema (parsed_archive.json.gz)

JSON schema version: `{schema_version}` (bumps only on breaking shape
changes — independent of the deposit version above).

```json
{{
  "version": "{schema_version}",
  "generated_at": "ISO timestamp",
  "fact_sheet": {{
    "<body>": {{
      "captures": [
        {{
          "wayback_timestamp": "YYYYMMDDhhmmss",
          "page_date": "YYYY-MM-DD" | null,
          "wayback_url": "https://web.archive.org/web/<TS>id_/...",
          "fields": {{
            "<attribute_name>": {{
              "value": <float | null>,
              "raw": "<original token from page>",
              "unit": "<unit string from page header> | null"
            }},
            ...
          }}
        }},
        ...
      ]
    }},
    ...
  }}
}}
```

Captures are sorted oldest-first by `page_date` if present, falling back
to `wayback_timestamp`. The `page_date` is NSSDC's own "Last Updated"
footer stamp; the Wayback timestamp is when the Internet Archive
crawled the page. They usually agree to within a day for post-2003
captures.

## Citation

If you use this archive in published work, please cite both:

1. The original NSSDC fact sheets:
   Williams, D.R., NASA NSSDC Planetary Fact Sheets,
   <https://nssdc.gsfc.nasa.gov/planetary/factsheet/>

2. This longitudinal archive:
   Aye, K.-M. (planetarypy contributors). NSSDC Planetary Fact Sheet
   Time Series (1996–2025): A Longitudinal Archive of Solar-System
   Reference Parameters, v{deposit_version}. Zenodo, {today}.
   DOI (concept — always resolves to the latest version):
   [{concept_doi}](https://doi.org/{concept_doi}).

3. The Internet Archive Wayback Machine, which made every capture in
   this archive accessible:
   <https://web.archive.org/>

## Reproducibility

To re-derive the entire archive from scratch:

```bash
python fetch_nssdc_archive.py            # ~1.5h fetch from Wayback
python parse_nssdc_archive.py            # parse HTML → JSON
```

The `parse_nssdc_archive.py` script is deterministic: re-running it on
the same `_raw/` cache produces a bit-identical `parsed_archive.json.gz`
modulo the `generated_at` timestamp.

## License

This deposit is released under the
[MIT License](https://opensource.org/licenses/MIT). You may use, copy,
modify, and redistribute the material for any purpose, including
commercially, provided you retain the copyright notice and attribution
(see the *Citation* section above). MIT matches the license of the
companion [planetarypy](https://github.com/michaelaye/planetarypy)
library so the parsed archive can ship inside the package without
license-compatibility friction.

The license applies to original portions of this deposit (parsing
scripts, README, schema, and curation). The underlying NSSDC Planetary
Fact Sheet *values* are themselves a US Government work in the public
domain (17 USC §105) and not subject to copyright; the MIT terms cannot
extend rights to material already in the public domain. The HTML
captures are reproduced from the Internet Archive Wayback Machine and
are made available under the Internet Archive's terms of service.

## Provenance

Generated {today} from planetarypy
<https://github.com/michaelaye/planetarypy>.
"""
    (bundle_dir / "README.md").write_text(readme)


def build_bundle() -> Path:
    if not PARSED_PATH.is_file():
        raise FileNotFoundError(
            f"Missing parsed archive: {PARSED_PATH}\n"
            f"Run scripts/parse_nssdc_archive.py first."
        )
    if not MANIFEST_PATH.is_file():
        raise FileNotFoundError(
            f"Missing raw manifest: {MANIFEST_PATH}\n"
            f"Run scripts/fetch_nssdc_archive.py first."
        )

    schema_version = _archive_schema_version()
    bundle_name = f"nssdc_archive_v{DEPOSIT_VERSION}"
    bundle_dir = OUT_ROOT / bundle_name
    if bundle_dir.exists():
        shutil.rmtree(bundle_dir)
    bundle_dir.mkdir(parents=True)

    # 1. Canonical parsed archive (just copy)
    shutil.copy2(PARSED_PATH, bundle_dir / "parsed_archive.json.gz")

    # 2. Long-format CSV mirror
    csv_path = bundle_dir / "parsed_archive.csv.gz"
    n_csv_rows = _build_csv(PARSED_PATH, csv_path)

    # 3. Raw HTML tarball
    tar_path = bundle_dir / "raw_html.tar.gz"
    n_raw_files = _build_raw_tarball(RAW_DIR, tar_path)

    # 4. CDX manifest (top-level for direct inspection without untarring)
    shutil.copy2(MANIFEST_PATH, bundle_dir / "manifest.json")

    # 5. Scripts bundled into a single tarball (Zenodo's GUI is flat, no
    # subfolders — wrapping them keeps the three .py files as one upload).
    scripts_tar = bundle_dir / "nssdc_scripts.tar.gz"
    with tarfile.open(scripts_tar, "w:gz", compresslevel=6) as tar:
        for script_name in SCRIPT_NAMES:
            src = SCRIPTS_DIR / script_name
            if not src.is_file():
                print(f"  warning: missing {src}", file=sys.stderr)
                continue
            tar.add(src, arcname=script_name)

    # 6. README with citation + provenance
    with gzip.open(PARSED_PATH, "rt", encoding="utf-8") as f:
        archive_meta = json.load(f)
    _write_readme(
        bundle_dir, DEPOSIT_VERSION, schema_version, CONCEPT_DOI,
        archive_meta, n_csv_rows, n_raw_files,
    )

    return bundle_dir


def _print_summary(bundle_dir: Path) -> None:
    print(f"\nBundle ready: {bundle_dir.relative_to(REPO_ROOT)}")
    total = 0
    for path in sorted(bundle_dir.rglob("*")):
        if path.is_file():
            size = path.stat().st_size
            total += size
            rel = path.relative_to(bundle_dir)
            print(f"  {size:>12,} B   {rel}")
    print(f"  {'-' * 12}")
    print(f"  {total:>12,} B   total ({total / 1024 / 1024:.2f} MB)")
    print(f"\nUpload to Zenodo:  https://zenodo.org/uploads/new")


def main() -> None:
    bundle_dir = build_bundle()
    _print_summary(bundle_dir)


if __name__ == "__main__":
    main()
