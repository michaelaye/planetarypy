"""Regenerate `docs/factsheets.qmd` from the parsed NSSDC archive.

Reads `parsed_archive.json.gz` (the same artifact that
`planetarypy.constants` loads at runtime) and writes a Quarto page with
one tab per body showing the *latest* archived NSSDC capture.

Run from the repo root:

    python docs/_generate_factsheets.py

Override the archive path with `--archive /path/to/parsed_archive.json.gz`.
The default looks up the cached file used by `planetarypy.constants`.
"""

from __future__ import annotations

import argparse
import gzip
import json
import re
from pathlib import Path

DEFAULT_ARCHIVE = (
    Path(__file__).resolve().parent.parent
    / "src" / "planetarypy" / "constants" / "nssdc" / "parsed_archive.json.gz"
)
OUT = Path(__file__).resolve().parent / "factsheets.qmd"

BODIES: list[tuple[str, str]] = [
    ("sun",     "Sun"),
    ("mercury", "Mercury"),
    ("venus",   "Venus"),
    ("earth",   "Earth"),
    ("mars",    "Mars"),
    ("jupiter", "Jupiter"),
    ("saturn",  "Saturn"),
    ("uranus",  "Uranus"),
    ("neptune", "Neptune"),
]

SUP = {"0":"⁰","1":"¹","2":"²","3":"³","4":"⁴","5":"⁵","6":"⁶","7":"⁷","8":"⁸","9":"⁹","-":"⁻","+":"⁺"}

# Known parser misfires where the "unit" column captured non-unit text
BOGUS_UNITS = {"flattening", "atmospheric", "+/-", "+/- 0.01", ""}


def fmt_field_name(name: str) -> str:
    return name.replace("_", " ").capitalize()


def fmt_value(v):
    if v is None:
        return ""
    if isinstance(v, float):
        if v == int(v) and abs(v) < 1e15:
            return f"{int(v):,}"
        return f"{v:g}"
    return str(v)


def fmt_unit(u: str) -> str:
    if not u or u.strip().lower() in BOGUS_UNITS:
        return ""
    def repl(m):
        return "".join(SUP[ch] for ch in m.group(1))
    return re.sub(r"\^(-?\d+)", repl, u)


def latest_capture(caps):
    def keyf(c):
        return (c.get("page_date") or "") + "_" + (c.get("wayback_timestamp") or "")
    return sorted(caps, key=keyf)[-1]


HEADER = '''---
title: "Planetary Fact Sheets"
subtitle: "Latest archived NSSDC values for the Sun and the planets"
date: today
toc: true
toc-depth: 2
---

::: {.panel-tabset}
'''

FOOTER = '''## About this page

::: {.callout-note}
NASA's live [NSSDC planetary fact sheets](https://nssdc.gsfc.nasa.gov/planetary/factsheet/)
have been intermittently offline for months. The tables above are the
**most recent archived revisions** of each fact sheet, parsed from the
longitudinal archive that ships with `planetarypy.constants` and
deposited at Zenodo
([10.5281/zenodo.20122986](https://doi.org/10.5281/zenodo.20122986)).
Each tab shows the latest capture's date and links to the underlying
Wayback Machine snapshot so the original page is one click away.
:::

The values above are auto-extracted from `parsed_archive.json.gz`
--- the same dataset that `planetarypy.constants` loads at runtime.
The archive currently contains 913 distinct content versions of 13
NSSDC fact sheets, spanning December 1996 to May 2025. This page is a
static snapshot of the most recent capture per body; it does not
auto-refresh and will be regenerated whenever NSSDC publishes new
revisions (or comes back online and we re-scrape).

The Moon, Pluto, and the asteroid / comet summary sheets are also in
the archive but not shown here, since this page focuses on the Sun and
the eight planets. Access them through the
[`planetarypy.constants`](reference/constants.qmd) API the same way.

## How to consume these in code

Every value shown above is also available as an
[`astropy.units.Quantity`](https://docs.astropy.org/en/stable/units/)
through `planetarypy.constants`:

``` python
from planetarypy.constants import Mars
Mars.bond_albedo          # 0.25
Mars.bond_albedo.unit     # dimensionless
Mars.bond_albedo.source   # the originating NSSDC capture
```

For the full revision trail of any field --- not just the latest value
shown here --- use the explicit `nssdc` namespace:

``` python
from planetarypy.constants import nssdc
nssdc.history("saturn", "number_of_satellites")
```

For the dataset itself, methodology, and citation BibTeX, see the
[Zenodo deposit](https://doi.org/10.5281/zenodo.20122986).

## Source & copyright

- **Source data:** NASA / Goddard Space Flight Center / National Space
  Science Data Center (NSSDC), Planetary Fact Sheets at
  <https://nssdc.gsfc.nasa.gov/planetary/factsheet/>, long-time curator
  Dr. David R. Williams. As works of the U.S. federal government these
  data are in the public domain in the United States
  ([17 U.S.C. § 105](https://www.copyright.gov/title17/92chap1.html#105));
  please credit **NASA / NSSDC** when you reuse them.
- **Archive parsing & redistribution:** Aye, K. M. (2025).
  *A longitudinal archive of the NSSDC planetary fact sheets
  (1996--2025)* \\[Data set\\]. Zenodo. <https://doi.org/10.5281/zenodo.20122986>
  (MIT licensed).
- **Disclaimer:** `planetarypy` is a community project. It is not
  affiliated with, endorsed by, or sponsored by NASA, GSFC, or NSSDC.
'''


def build(archive_path: Path) -> str:
    data = json.loads(gzip.open(archive_path).read())
    fs = data["fact_sheet"]
    out = [HEADER]
    for slug, name in BODIES:
        cap = latest_capture(fs[slug]["captures"])
        page_date = cap.get("page_date") or "(undated)"
        wb = cap.get("wayback_url") or ""
        fields = cap.get("fields") or {}
        out.append(f"## {name}\n")
        out.append(
            f"*Latest archived revision: **{page_date}*** · "
            f"[original Wayback snapshot ↗]({wb})\n"
        )
        if not fields:
            out.append("_No parsed fields available for this capture._\n")
            continue
        out.append("| Field | Value | Unit |")
        out.append("|-------|------:|:-----|")
        for fname, fval in fields.items():
            v = fval.get("value")
            unc = fval.get("uncertainty")
            rng = fval.get("range") or {}
            raw = fval.get("raw") or ""
            unit = fmt_unit(fval.get("unit") or "")
            # Range entries omit ``value`` in the archive — show bounds
            # as ``X–Y`` so the user sees both that the quantity varies
            # and what the bracket is.
            if rng.get("min") is not None and rng.get("max") is not None:
                display = f"{fmt_value(rng['min'])}–{fmt_value(rng['max'])}"
            elif unc is not None and v is not None:
                display = f"{fmt_value(v)} ± {fmt_value(unc)}"
            else:
                display = fmt_value(v) if v not in (None, "") else raw
            display = str(display).replace("|", "\\|")
            unit_d = unit.replace("|", "\\|")
            out.append(f"| {fmt_field_name(fname)} | {display} | {unit_d} |")
        out.append("")
    out.append(":::\n")
    out.append(FOOTER)
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--archive", type=Path, default=DEFAULT_ARCHIVE,
                    help=f"path to parsed_archive.json.gz (default: {DEFAULT_ARCHIVE})")
    ap.add_argument("--out", type=Path, default=OUT,
                    help=f"output qmd path (default: {OUT})")
    args = ap.parse_args()
    if not args.archive.exists():
        raise SystemExit(f"archive not found: {args.archive}")
    args.out.write_text(build(args.archive), encoding="utf-8")
    print(f"wrote {args.out} ({args.out.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
