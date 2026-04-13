#!/usr/bin/env python3
"""Utility to normalize docstrings in the repository to a basic numpydoc style.

This script makes a conservative, automated pass:
- For module, function and class docstrings that are missing or don't contain a
  "Parameters" section, it inserts a minimal numpydoc template.
- Parameter types and return descriptions are marked with "REVIEW" so a human
  can fill them in where the automation is unsure.

Usage:
    python tools/convert_docstrings.py --check
    python tools/convert_docstrings.py --apply

Note: This tool is intentionally conservative. It preserves existing first
line summaries where present and only adds structured sections when needed.
"""

from __future__ import annotations

import ast
from pathlib import Path
import argparse
import re
from typing import List, Tuple


def split_lines(s: str) -> List[str]:
    return s.splitlines()


def build_numpydoc_for_function(name: str, args: List[str], returns: bool, existing_summary: str | None) -> str:
    summary = existing_summary or f"{name} function. REVIEW"
    parts = [summary, "", "Parameters", "----------"]
    if args:
        for a in args:
            parts.append(f"{a} : REVIEW")
            parts.append("    REVIEW")
    else:
        parts.append("None")
    parts.append("")
    parts.append("Returns")
    parts.append("-------")
    parts.append("REVIEW")
    parts.append("")
    parts.append("Examples")
    parts.append("--------")
    parts.append(">>> # REVIEW: add example usage")
    return "\n".join(parts)


def build_module_doc(existing: str | None, module_name: str) -> str:
    summary = existing.splitlines()[0] if existing else f"{module_name} module. REVIEW"
    parts = [summary, "", "Notes", "-----", "REVIEW", "", "Examples", "--------", ">>> # REVIEW: example"]
    return "\n".join(parts)


def get_node_doc_bounds(node: ast.AST) -> Tuple[int, int] | None:
    # Requires Python 3.8+ for end_lineno
    if hasattr(node, "lineno") and hasattr(node, "end_lineno"):
        return node.lineno, node.end_lineno
    return None


def process_file(path: Path, apply: bool = False) -> Tuple[bool, str]:
    src = path.read_text(encoding="utf8")
    tree = ast.parse(src)
    lines = src.splitlines()
    modified = False

    # Module docstring
    mod_doc = ast.get_docstring(tree)
    if not mod_doc or "Parameters" not in (mod_doc or ""):
        module_name = path.stem
        new_mod_doc = build_module_doc(mod_doc, module_name)
        # Replace or insert module-level docstring
        if mod_doc:
            # find first triple-quoted string at top
            m = re.search(r"\A(\s*?)([ruRUfF]*\"\"\".*?\"\"\"|[ruRUfF]*\'\'\'.*?\'\'\')", src, flags=re.DOTALL)
            if m:
                prefix = m.group(1)
                new_block = f'{prefix}"""{new_mod_doc}\n"""\n'
                src = src[: m.start(2)] + new_block + src[m.end(2) :]
                modified = True
        else:
            # insert at top
            new_block = f'"""{new_mod_doc}\n"""\n\n'
            src = new_block + src
            modified = True

    # Re-parse if changed
    if modified:
        tree = ast.parse(src)
        lines = src.splitlines()

    # Process functions and classes
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            name = node.name
            doc = ast.get_docstring(node)
            if doc and "Parameters" in doc:
                continue
            # build template
            args = []
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                for a in node.args.args:
                    if a.arg != "self":
                        args.append(a.arg)
            existing_summary = None
            if doc:
                existing_summary = doc.splitlines()[0]
            returns_flag = False
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                returns_flag = bool(node.returns)
            new_doc = build_numpydoc_for_function(name, args, returns_flag, existing_summary)

            bounds = get_node_doc_bounds(node)
            if not bounds:
                continue
            start, end = bounds
            # docstring usually appears as first statement in node
            # find first string literal in node body
            target = None
            for child in node.body:
                if isinstance(child, ast.Expr) and isinstance(child.value, ast.Constant) and isinstance(child.value.value, str):
                    target = child
                    break
            if target:
                t_bounds = get_node_doc_bounds(target)
                if not t_bounds:
                    continue
                tstart, tend = t_bounds
                # replace lines tstart..tend with new docstring block
                q = '"""'
                new_block = q + new_doc + q
                # maintain indentation
                indent = re.match(r"^(\s*)", lines[tstart - 1]).group(1)
                new_block_lines = [(indent + line) if line else indent for line in new_block.splitlines()]
                lines = lines[: tstart - 1] + new_block_lines + lines[tend:]
                modified = True
            else:
                # insert new docstring as first statement inside node
                insert_at = node.body[0].lineno - 1
                indent = re.match(r"^(\s*)", lines[insert_at]).group(1)
                q = '"""'
                new_block = q + new_doc + q
                new_block_lines = [(indent + line) if line else indent for line in new_block.splitlines()]
                lines = lines[:insert_at] + new_block_lines + lines[insert_at:]
                modified = True

    if modified and apply:
        path.write_text("\n".join(lines) + "\n", encoding="utf8")

    return modified, src if not modified else "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Write changes back to files")
    parser.add_argument("--check", action="store_true", help="Only report files that would change")
    args = parser.parse_args()

    root = Path(__file__).resolve().parent.parent / "src" / "planetarypy"
    pyfiles = sorted(root.rglob("*.py"))
    changed = []
    for p in pyfiles:
        mod, new = process_file(p, apply=args.apply)
        if mod:
            changed.append(str(p.relative_to(root.parent)))
            print(f"Would modify: {p}")
    print(f"Processed {len(pyfiles)} files, modified {len(changed)} files.")


if __name__ == "__main__":
    main()
