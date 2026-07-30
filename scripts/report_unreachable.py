"""Open, update and close GitHub issues from a reachability summary.

One issue per affected host, so an outage is durable and assignable — something
to point a node operator at — rather than a red run you only see if you look.

Self-closing is the point: without it a monitor that files issues becomes noise
nobody reads. A host that recovers gets its issue closed with how long it was
down, so an open ``upstream-outage`` issue always means "still broken now".

Reads the ``summary.json`` written by ``check_node_reachability.py``. Requires
``gh`` on PATH and authenticated (``GH_TOKEN`` on a runner). ``--dry-run``
prints what it would do and touches nothing.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import date
from pathlib import Path

LABEL = "upstream-outage"
TITLE = "Archive unreachable: {host}"
MARKER = "<!-- reachability-bot -->"


def gh(*args: str, check: bool = True) -> str:
    result = subprocess.run(
        ["gh", *args], capture_output=True, text=True, check=False
    )
    if check and result.returncode != 0:
        raise RuntimeError(f"gh {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def open_issues() -> dict[str, dict]:
    raw = gh("issue", "list", "--label", LABEL, "--state", "open",
             "--json", "number,title,body,createdAt", "--limit", "100")
    issues = json.loads(raw) if raw else []
    return {i["title"]: i for i in issues}


def body_for(host: str, failing: list[dict], checked: str) -> str:
    rows = "\n".join(
        f"| `{f['index_key']}` | **{f['status']}** | {f['url']} |" for f in failing
    )
    return (
        f"{MARKER}\n"
        f"`{host}` is serving errors for {len(failing)} registered index "
        f"URL(s) as of {checked}.\n\n"
        "| index key | status | url |\n| --- | --- | --- |\n"
        f"{rows}\n\n"
        "A **404** usually means the URL moved and our registration in "
        "`planetarypy_index_urls.toml` is stale — ours to fix. A **401/403** "
        "on a public archive is the node's to fix.\n\n"
        "Opened automatically by the `Archive reachability` workflow; it will "
        "close this issue when the host recovers."
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("summary", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    data = json.loads(args.summary.read_text())
    checked = data["checked"]
    failing_by_host: dict[str, list[dict]] = {}
    for result in data["results"]:
        if not result["up"]:
            failing_by_host.setdefault(result["host"], []).append(result)

    if args.dry_run:
        existing: dict[str, dict] = {}
    else:
        gh("label", "create", LABEL, "--color", "B60205",
           "--description", "An upstream archive is unreachable", check=False)
        existing = open_issues()

    for host, failing in sorted(failing_by_host.items()):
        title = TITLE.format(host=host)
        if title in existing:
            print(f"already open: {title}")
            continue
        print(f"OPEN  {title}  ({len(failing)} url(s))")
        if not args.dry_run:
            gh("issue", "create", "--title", title, "--label", LABEL,
               "--body", body_for(host, failing, checked))

    for title, issue in sorted(existing.items()):
        host = title.removeprefix("Archive unreachable: ")
        if host in failing_by_host:
            continue
        since = issue["createdAt"][:10]
        days = (date.today() - date.fromisoformat(since)).days
        print(f"CLOSE {title}  (down {days}d)")
        if not args.dry_run:
            gh("issue", "close", str(issue["number"]), "--comment",
               f"{MARKER}\n`{host}` is reachable again as of {checked} "
               f"(first reported {since}, {days} day(s)).")

    if not failing_by_host:
        print("all hosts reachable")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
