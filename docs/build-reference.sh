#!/usr/bin/env bash
# Build the quartodoc API reference.
#
# instruments/ is a PEP 420 namespace package (no __init__.py, kept that way so the
# planetarypy-hirise and planetarypy-ctx distributions can contribute under it —
# see the namespace guard in CLAUDE.md). griffe (quartodoc's static analyser) does
# not walk into a namespace subpackage from its parent, which fails the whole
# API-reference build ("Cannot find an object named: instruments").
#
# Work around it by creating a THROWAWAY __init__.py for this build only and
# deleting it immediately (trap on EXIT, so it goes even if the build fails). It
# is never committed and never shipped (also .gitignore'd), so the distribution
# layout and the plugin split are unaffected — this is a docs-generation shim only.
#
# instruments/mro/ used to be shimmed here too. As of 0.82.0 core has no mro/
# directory at all — it belongs to the plugin distributions — so touching it would
# fail under `set -e`, which is exactly how this script broke on the 0.82.0 push.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"          # docs/
root="$(cd "$here/.." && pwd)"                 # repo root
inits=("$root/src/planetarypy/instruments/__init__.py")

cleanup() { rm -f "${inits[@]}"; }
trap cleanup EXIT

touch "${inits[@]}"
cd "$here"
python -m quartodoc build
