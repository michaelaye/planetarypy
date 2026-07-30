"""Probe every archive host planetarypy resolves indexes from.

Reachability only: one HEAD per host, no downloads. That keeps every host in
scope — including the ones ``tests/test_pds_smoke.py`` omits because their
smallest index is 100 MB+ — and runs in about a second, so it can be scheduled
far more often than the download canary.

The host list is derived from the live index-URL config rather than hardcoded,
so a newly registered node is probed without touching this file.

Writes, into ``--outdir``:
  <slug>.json   shields.io endpoint badge, one per host
  summary.json  machine-readable results, consumed by the issue automation
  STATUS.md     human-readable table

Always exits 0 — a down host is data to publish, not a reason to fail the run.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests

TIMEOUT = 25

# Friendlier badge labels than the bare hostname; anything unlisted falls back
# to the host's second-level name.
LABELS = {
    "pds-geosciences.wustl.edu": "PDS Geosciences (WUSTL)",
    "pds-rings.seti.org": "PDS Rings (SETI)",
    "planetarydata.jpl.nasa.gov": "PDS Imaging (JPL)",
    "hirise-pds.lpl.arizona.edu": "HiRISE (LPL)",
    "pds.lroc.asu.edu": "LROC (ASU)",
    "sbnarchive.psi.edu": "SBN (PSI)",
}


def index_urls() -> dict[str, str]:
    """Every registered index URL, keyed by its dotted index key."""
    from planetarypy.pds.static_index import ConfigHandler

    doc = ConfigHandler().to_dict()

    def walk(node, path=()):
        if hasattr(node, "items"):
            for key, value in node.items():
                yield from walk(value, path + (key,))
        elif isinstance(node, str) and node.startswith("http"):
            yield ".".join(path), node

    return dict(walk(doc))


def probe(item: tuple[str, str]) -> dict:
    """HEAD one index URL.

    Every registered URL is probed, not one per host: a host can serve most of
    its archive fine while a single object is unreadable — the WUSTL msl.sam.l0
    401 of 2026-07-30 sat behind a directory that returned 200.
    """
    key, url = item
    host = urlparse(url).netloc
    try:
        response = requests.head(url, timeout=TIMEOUT, allow_redirects=True)
        # Some archive servers reject HEAD outright; retry as a streamed GET and
        # drop the connection rather than mark a healthy host down.
        if response.status_code in (403, 405, 501):
            response = requests.get(
                url, timeout=TIMEOUT, allow_redirects=True, stream=True
            )
            status = response.status_code
            response.close()
        else:
            status = response.status_code
        return {
            "host": host,
            "index_key": key,
            "url": url,
            "status": str(status),
            "up": status < 400,
        }
    except Exception as exc:
        return {
            "host": host,
            "index_key": key,
            "url": url,
            "status": type(exc).__name__,
            "up": False,
        }


def slug(host: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", host.lower()).strip("-")


def label(host: str) -> str:
    return LABELS.get(host, host.split(".")[-3] if host.count(".") > 2 else host)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--outdir", type=Path, default=Path("status"))
    args = parser.parse_args()
    args.outdir.mkdir(parents=True, exist_ok=True)

    urls = index_urls()
    # Modest concurrency: this fans out across only ~6 archive servers, and a
    # monitor has no business hammering them.
    with cf.ThreadPoolExecutor(max_workers=8) as pool:
        results = sorted(pool.map(probe, urls.items()), key=lambda r: r["index_key"])

    by_host: dict[str, list[dict]] = defaultdict(list)
    for result in results:
        by_host[result["host"]].append(result)

    checked = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    hosts = []
    for host in sorted(by_host):
        entries = by_host[host]
        bad = [e for e in entries if not e["up"]]
        hosts.append(
            {"host": host, "total": len(entries), "failing": len(bad), "up": not bad}
        )
        (args.outdir / f"{slug(host)}.json").write_text(
            json.dumps(
                {
                    "schemaVersion": 1,
                    "label": label(host),
                    "message": "up" if not bad else f"{len(bad)}/{len(entries)} failing",
                    "color": "brightgreen" if not bad else "red",
                }
            )
            + "\n"
        )

    (args.outdir / "summary.json").write_text(
        json.dumps({"checked": checked, "hosts": hosts, "results": results}, indent=2)
        + "\n"
    )

    failing = [r for r in results if not r["up"]]
    lines = [
        "# PDS archive reachability",
        "",
        f"Last checked: {checked}",
        "",
        "| host | indexes | status |",
        "| --- | --- | --- |",
    ]
    for entry in hosts:
        state = "up" if entry["up"] else f"**{entry['failing']} failing**"
        lines.append(f"| {label(entry['host'])} | {entry['total']} | {state} |")
    if failing:
        lines += ["", "## Failing indexes", "", "| index key | status | url |",
                  "| --- | --- | --- |"]
        for result in failing:
            lines.append(
                f"| `{result['index_key']}` | **{result['status']}** "
                f"| {result['url']} |"
            )
    lines += [
        "",
        f"{len(results) - len(failing)}/{len(results)} index URLs reachable across "
        f"{len(hosts)} hosts.",
        "",
    ]
    (args.outdir / "STATUS.md").write_text("\n".join(lines))

    for result in failing:
        print(f"DOWN {result['status']:<18} {result['index_key']}")
    print(f"{len(results) - len(failing)}/{len(results)} index URLs reachable")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
