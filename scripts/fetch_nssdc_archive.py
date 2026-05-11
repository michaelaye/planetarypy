"""One-time Wayback pulldown of NSSDC planetary fact sheets.

NASA NSSDC's per-body fact sheets at
``https://nssdc.gsfc.nasa.gov/planetary/factsheet/<body>fact.html`` are
behind a maintenance redirect on the origin, but the Wayback Machine has
~6,000 distinct content versions cumulative across 13 fact sheets from
1996-12 to 2025-03. This script:

1. Queries the Wayback CDX API for distinct content versions of each
   fact sheet (deduped via ``collapse=digest``).
2. Fetches each unique snapshot via the ``id_`` modifier URL pattern
   (returns the raw original bytes, no Wayback toolbar injection).
3. Caches each result to disk under
   ``src/planetarypy/constants/nssdc/_raw/<body>/<body>fact_<TIMESTAMP>.html.gz``.
4. Maintains a JSON manifest mapping (body, timestamp) -> file path.

Idempotent: re-runs skip already-fetched files.

This is a maintainer-only build-time script. The output is bundled and
uploaded to Zenodo by ``scripts/upload_nssdc_zenodo.py``; users don't
run this. Polite to Wayback: ~1 req/sec, retries on 5xx.

Usage::

    # All bodies (full pull, ~6000 captures, hours):
    python scripts/fetch_nssdc_archive.py

    # One body at a time:
    python scripts/fetch_nssdc_archive.py mars

    # Multiple bodies:
    python scripts/fetch_nssdc_archive.py mars earth jupiter
"""

from __future__ import annotations

import gzip
import json
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = REPO_ROOT / "src/planetarypy/constants/nssdc/_raw"
MANIFEST_PATH = RAW_DIR / "manifest.json"

# All known NSSDC fact-sheet bodies. Verified accessible via Wayback CDX
# (galileanfact, ioniafact, phobosfact, deimosfact don't exist as separate
# URLs — moon data lives inside each parent planet's fact sheet).
BODIES = [
    "sun", "mercury", "venus", "earth", "moon",
    "mars", "jupiter", "saturn", "uranus", "neptune", "pluto",
    "asteroid", "comet",
]

USER_AGENT = "planetarypy-nssdc-archive/1.0 (https://github.com/planetarypy/planetarypy)"
REQUEST_DELAY_SEC = 1.0      # polite to Wayback
RETRY_BACKOFF_SEC = [5, 15, 45]
CDX_TIMEOUT = 60
FETCH_TIMEOUT = 60


@dataclass(frozen=True)
class CaptureEntry:
    body: str
    timestamp: str   # YYYYMMDDhhmmss (Wayback's format)
    digest: str
    original_url: str

    @property
    def wayback_url(self) -> str:
        # The ``id_`` suffix returns the raw original capture without
        # Wayback's HTML toolbar injection.
        return (
            f"https://web.archive.org/web/{self.timestamp}id_/"
            f"{self.original_url}"
        )

    @property
    def filename(self) -> str:
        return f"{self.body}fact_{self.timestamp}.html.gz"


def _http_get(url: str, timeout: int = FETCH_TIMEOUT) -> bytes:
    """Polite HTTP GET with retries on 5xx / connection errors."""
    last_err: Exception | None = None
    for attempt, backoff in enumerate([0] + RETRY_BACKOFF_SEC):
        if backoff:
            print(f"  retrying after {backoff}s (attempt {attempt}) ...",
                  file=sys.stderr)
            time.sleep(backoff)
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except (urllib.error.HTTPError, urllib.error.URLError,
                TimeoutError, ConnectionError) as e:
            last_err = e
            # Don't retry 4xx (404 etc.)
            if isinstance(e, urllib.error.HTTPError) and 400 <= e.code < 500:
                raise
    assert last_err is not None
    raise last_err


def fetch_cdx_list(body: str) -> list[CaptureEntry]:
    """Pull the CDX listing of distinct content digests for one fact sheet.

    Uses ``collapse=digest`` so consecutive identical-content captures are
    deduped. Returns one entry per unique content digest.
    """
    url = f"https://nssdc.gsfc.nasa.gov/planetary/factsheet/{body}fact.html"
    cdx_url = (
        "https://web.archive.org/cdx/search/cdx"
        f"?url={url}"
        "&fl=timestamp,digest"
        "&collapse=digest"
        "&limit=5000"
        "&filter=statuscode:200"
    )
    print(f"  CDX query: {cdx_url}", file=sys.stderr)
    raw = _http_get(cdx_url, timeout=CDX_TIMEOUT)
    entries: list[CaptureEntry] = []
    for line in raw.decode("utf-8", errors="replace").splitlines():
        parts = line.split()
        if len(parts) != 2:
            continue
        timestamp, digest = parts
        entries.append(CaptureEntry(body=body, timestamp=timestamp,
                                    digest=digest, original_url=url))
    return entries


def fetch_capture(entry: CaptureEntry, dest_dir: Path) -> Path:
    """Fetch one capture and store it gzipped. Returns the destination path."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    out_path = dest_dir / entry.filename
    if out_path.exists():
        return out_path     # idempotent
    raw_html = _http_get(entry.wayback_url)
    with gzip.open(out_path, "wb") as f:
        f.write(raw_html)
    return out_path


def load_manifest() -> dict:
    if not MANIFEST_PATH.is_file():
        return {"bodies": {}}
    return json.loads(MANIFEST_PATH.read_text())


def save_manifest(manifest: dict) -> None:
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, sort_keys=True))


def fetch_body(body: str, manifest: dict, *, limit: int | None = None) -> int:
    """Fetch all distinct captures for one body. Returns count of new fetches."""
    print(f"\n== {body}fact.html ==", file=sys.stderr)
    body_dir = RAW_DIR / body
    body_dir.mkdir(parents=True, exist_ok=True)

    captures = fetch_cdx_list(body)
    if limit is not None:
        captures = captures[:limit]
    print(f"  {len(captures)} distinct content versions on Wayback",
          file=sys.stderr)

    body_manifest = manifest["bodies"].setdefault(body, {})
    new_count = 0
    for i, entry in enumerate(captures, 1):
        if entry.timestamp in body_manifest:
            continue       # already have it
        try:
            out_path = fetch_capture(entry, body_dir)
            body_manifest[entry.timestamp] = {
                "digest": entry.digest,
                "wayback_url": entry.wayback_url,
                "path": str(out_path.relative_to(REPO_ROOT)),
            }
            new_count += 1
            if i % 20 == 0 or i == len(captures):
                print(f"  [{i}/{len(captures)}] fetched +{new_count} new",
                      file=sys.stderr)
            time.sleep(REQUEST_DELAY_SEC)
        except urllib.error.HTTPError as e:
            print(f"  ! {entry.timestamp}: HTTP {e.code}", file=sys.stderr)
            # Don't include in manifest; will retry on next run
            continue
        except Exception as e:
            print(f"  ! {entry.timestamp}: {type(e).__name__}: {e}",
                  file=sys.stderr)
            continue

    # Periodic flush so a crash mid-body doesn't lose all progress.
    save_manifest(manifest)
    print(f"  done: +{new_count} new (total cached: {len(body_manifest)})",
          file=sys.stderr)
    return new_count


def main() -> None:
    args = sys.argv[1:]
    # Special flag for proof-of-concept: limit per body, e.g. "--limit=5 mars"
    limit: int | None = None
    bodies: list[str] = []
    for a in args:
        if a.startswith("--limit="):
            limit = int(a.split("=", 1)[1])
        else:
            bodies.append(a)
    if not bodies:
        bodies = BODIES

    manifest = load_manifest()
    total_new = 0
    for body in bodies:
        if body not in BODIES:
            print(f"Skipping unknown body: {body!r}", file=sys.stderr)
            continue
        total_new += fetch_body(body, manifest, limit=limit)
    save_manifest(manifest)
    print(f"\nDone. {total_new} new captures total.", file=sys.stderr)


if __name__ == "__main__":
    main()
