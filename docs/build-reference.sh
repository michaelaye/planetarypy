#!/usr/bin/env bash
# Build the quartodoc API reference.
#
# instruments/ and instruments/mro/ are PEP 420 namespace packages (no __init__.py,
# kept that way for the planetarypy-hirise distribution split — see the namespace
# guard in CLAUDE.md). griffe (quartodoc's static analyser) does not walk into a
# namespace subpackage from its parent, which fails the whole API-reference build
# ("Cannot find an object named: instruments").
#
# Work around it by creating THROWAWAY __init__.py files for this build only and
# deleting them immediately (trap on EXIT, so they go even if the build fails). They
# are never committed and never shipped (also .gitignore'd), so the distribution
# layout and the hirise split are unaffected — this is a docs-generation shim only.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"          # docs/
root="$(cd "$here/.." && pwd)"                 # repo root
inits=("$root/src/planetarypy/instruments/__init__.py"
       "$root/src/planetarypy/instruments/mro/__init__.py")

cleanup() { rm -f "${inits[@]}"; }
trap cleanup EXIT

touch "${inits[@]}"
cd "$here"
python -m quartodoc build
