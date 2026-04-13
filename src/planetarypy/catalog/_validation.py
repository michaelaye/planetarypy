"""URL validation for catalog products.

Checks whether product URLs are reachable via HTTP HEAD requests
and updates the catalog database with the results.
"""

import datetime as dt
from pathlib import Path

import requests
from loguru import logger

from planetarypy.utils import have_internet


def validate_urls(
    storage_root: Path,
    sample_size: int = 3,
    timeout: int = 15,
) -> dict[str, int]:
    """Validate product URLs in the catalog via HEAD requests.

    Samples up to `sample_size` unchecked products per product type
    and updates their url_status in the database.

    Parameters
    ----------
    storage_root : Path
        The planetarypy storage root directory
    sample_size : int
        Max number of products to check per product type
    timeout : int
        HTTP request timeout in seconds

    Returns
    -------
    dict[str, int]
        Counts of results by status ('ok', 'not_found', 'redirect', 'error')
    """
    if not have_internet():
        logger.warning("No internet connection. Skipping URL validation.")
        return {}

    from planetarypy.catalog._schema import get_connection

    con = get_connection(storage_root)
    counts: dict[str, int] = {"ok": 0, "not_found": 0, "redirect": 0, "error": 0}

    # Get unchecked products, sampled per product type
    rows = con.execute("""
        SELECT folder_name, product_key, url_stem, label_file, rowid
        FROM products
        WHERE url_status = 'unchecked' AND url_stem IS NOT NULL AND url_stem != ''
    """).fetchall()

    if not rows:
        logger.info("No unchecked URLs to validate.")
        con.close()
        return counts

    # Group by (folder_name, product_key) and sample
    from collections import defaultdict
    grouped: dict[tuple, list] = defaultdict(list)
    for row in rows:
        key = (row[0], row[1])
        if len(grouped[key]) < sample_size:
            grouped[key].append(row)

    total = sum(len(v) for v in grouped.values())
    logger.info(f"Validating {total} URLs across {len(grouped)} product types...")

    session = requests.Session()
    session.headers.update({"User-Agent": "planetarypy-catalog/1.0"})

    checked = 0
    for (folder, pkey), samples in grouped.items():
        for row in samples:
            url_stem = row[2]
            label_file = row[3]
            rowid = row[4]

            # Construct a test URL
            if label_file and url_stem:
                test_url = f"{url_stem}/{label_file}" if not url_stem.endswith("/") else f"{url_stem}{label_file}"
            else:
                test_url = url_stem

            status = _check_url(session, test_url, timeout)
            now = dt.datetime.now(dt.timezone.utc)
            counts[status] += 1
            checked += 1

            con.execute(
                """UPDATE products SET url_status = ?, url_checked_at = ?
                   WHERE rowid = ?""",
                [status, now, rowid],
            )

            if checked % 50 == 0:
                logger.info(f"  Checked {checked}/{total} URLs...")

    con.close()
    logger.info(
        f"URL validation complete: {counts['ok']} ok, {counts['not_found']} not_found, "
        f"{counts['redirect']} redirect, {counts['error']} error"
    )
    return counts


def _check_url(session: requests.Session, url: str, timeout: int) -> str:
    """Check a single URL via HEAD request.

    Returns 'ok', 'not_found', 'redirect', or 'error'.
    """
    try:
        resp = session.head(url, timeout=timeout, allow_redirects=False)
        if resp.status_code < 300:
            return "ok"
        if 300 <= resp.status_code < 400:
            # Follow redirect to check final destination
            try:
                resp2 = session.head(url, timeout=timeout, allow_redirects=True)
                if resp2.status_code < 400:
                    return "redirect"
                return "not_found"
            except requests.RequestException:
                return "redirect"
        if resp.status_code == 404:
            return "not_found"
        return "error"
    except requests.Timeout:
        return "error"
    except requests.RequestException:
        return "error"
