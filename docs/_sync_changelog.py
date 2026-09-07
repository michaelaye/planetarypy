"""Publish the root CHANGELOG.md as a docs page, single-sourced.

Run by Quarto as a `pre-render` script, so the page is regenerated on every
build and can never drift from the file the release process actually edits —
the failure mode of hand-maintaining a second copy.

Two transformations earn their keep:

- Keep a Changelog headings (``## [0.83.0] - 2026-08-13``) slug to nothing,
  so Quarto falls back to positional anchors (``#section``, ``#section-1``, …)
  that renumber on every release and rot any link ever shared. Explicit
  ``{#v0-83-0}`` anchors are stable forever.
- The announcement bar needs the current version, which lives in the changelog
  and nowhere Quarto can read. Writing it to a metadata file keeps it correct
  without a committed file that churns on every release.

Both outputs are gitignored.

The writes are conditional on the content actually differing. `quarto preview`
watches its inputs, so a script that rewrites a file on every render makes the
preview re-render that page and navigate the browser to it — repeatedly, which
leaves you unable to look at any other page.
"""
from __future__ import annotations

import pathlib
import re

DOCS = pathlib.Path(__file__).resolve().parent
CHANGELOG = DOCS.parent / "CHANGELOG.md"
PAGE = DOCS / "changelog.md"
ANNOUNCEMENT = DOCS / "_announcement.yml"

FRONT_MATTER = """---
title: "Release Notes"
subtitle: "Every released version of planetarypy, newest first"
toc: true
toc-depth: 2
---

"""

# "## [0.83.0] - 2026-08-13" and "## [Unreleased]"
VERSION_HEADING = re.compile(r"^## \[([^\]]+)\](.*)$", re.MULTILINE)


def anchor_for(version: str) -> str:
    """`0.83.0` -> `v0-83-0`; `Unreleased` -> `unreleased`."""
    slug = version.strip().lower().replace(".", "-")
    return slug if not slug[0].isdigit() else f"v{slug}"


def latest_version(text: str) -> str | None:
    for version, _ in VERSION_HEADING.findall(text):
        if version[0].isdigit():
            return version
    return None


def write_if_changed(path: pathlib.Path, content: str) -> None:
    """Leave the file's mtime alone when nothing changed — see the module docstring."""
    if path.exists() and path.read_text(encoding="utf-8") == content:
        return
    path.write_text(content, encoding="utf-8")


def main() -> None:
    text = CHANGELOG.read_text(encoding="utf-8")

    # Drop the file's own H1 — the front matter supplies the page title.
    body = re.sub(r"\A# .*?\n", "", text, count=1)

    body = VERSION_HEADING.sub(
        lambda m: f"## [{m.group(1)}]{m.group(2)} {{#{anchor_for(m.group(1))}}}",
        body,
    )

    write_if_changed(PAGE, FRONT_MATTER + body.lstrip("\n"))

    version = latest_version(text)
    if version:
        write_if_changed(
            ANNOUNCEMENT,
            "website:\n"
            "  announcement:\n"
            f'    content: "**planetarypy {version}** is out — '
            f'[see what changed](/changelog.md#{anchor_for(version)})"\n'
            "    type: primary\n"
            "    position: below-navbar\n"
            "    dismissable: true\n"
            "    icon: rocket-takeoff\n",
        )


if __name__ == "__main__":
    main()
