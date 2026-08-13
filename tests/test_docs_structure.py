"""Every narrative docs page must be reachable from the site navigation.

The sidebar in ``docs/_quarto.yml`` lists pages explicitly, which keeps the
reading order deliberate — tutorials build on each other, so alphabetical
globbing would be worse. The cost is that a new page is only one forgotten
line away from being invisible: five finished pages were reachable by URL
only before this test existed.
"""
from __future__ import annotations

import pathlib

import pytest

DOCS = pathlib.Path(__file__).resolve().parents[1] / "docs"
QUARTO_YML = DOCS / "_quarto.yml"
NARRATIVE_DIRS = ("tutorials", "howto", "explanation")
PAGE_SUFFIXES = (".qmd", ".ipynb", ".md")

pytestmark = pytest.mark.skipif(
    not QUARTO_YML.exists(), reason="docs/ not present in this checkout"
)


def _pages() -> list[pathlib.Path]:
    """Narrative pages, excluding build output and notebook checkpoints."""
    found = []
    for name in NARRATIVE_DIRS:
        for path in sorted((DOCS / name).rglob("*")):
            if path.suffix not in PAGE_SUFFIXES:
                continue
            if any(part in {"_build", "_freeze", ".ipynb_checkpoints"}
                   for part in path.parts):
                continue
            if path.name.endswith(".out.ipynb"):
                continue
            if _is_draft(path):
                continue
            found.append(path)
    return found


def _is_draft(path: pathlib.Path) -> bool:
    """Quarto's own opt-out — a `draft: true` page is meant to be unlisted."""
    if path.suffix == ".ipynb":
        return False
    head = path.read_text(errors="replace").split("---", 2)
    return len(head) > 2 and "draft: true" in head[1]


@pytest.mark.parametrize("page", _pages(), ids=lambda p: f"{p.parent.name}/{p.name}")
def test_page_is_in_the_navigation(page):
    """A page nobody links to is a page nobody reads."""
    rel = f"{page.parent.name}/{page.name}"
    assert rel in QUARTO_YML.read_text(), (
        f"{rel} exists but is not referenced in docs/_quarto.yml — "
        "add it to the sidebar contents, or delete the page."
    )


def test_navigation_has_no_dangling_entries():
    """The reverse: a sidebar entry whose file was renamed or removed."""
    missing = []
    for line in QUARTO_YML.read_text().splitlines():
        entry = line.strip().removeprefix("- ")
        if not entry.startswith(NARRATIVE_DIRS) or not entry.endswith(PAGE_SUFFIXES):
            continue
        if not (DOCS / entry).exists():
            missing.append(entry)
    assert not missing, f"docs/_quarto.yml points at files that do not exist: {missing}"
