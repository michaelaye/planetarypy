"""PlanetaryPy unified CLI.

Usage:
    plp fetch mro.ctx.edr P02_001916_2221_XI_42N027W
    plp hifetch PSP_003092_0985 --browse
    plp ctxqv J05_046771_1950
    plp catalog build
"""

from pathlib import Path

import click
import typer


def _apply_warning_filters() -> None:
    """Apply DeprecationWarning suppression per the user's config.

    Default ``filter_deprecation_warnings = true`` silences
    DeprecationWarning during CLI execution so end users aren't shown
    upstream noise (e.g. Typer's ``shell_complete=``-is-deprecated
    notice, which is a known signal we accept — see
    :func:`_shell_complete_index_key`).

    Devs working on planetarypy can opt in to seeing the warnings as
    reminders by setting ``filter_deprecation_warnings = false`` in
    ``~/.planetarypy_config.toml``. Standard Python ``-W`` flags and
    ``PYTHONWARNINGS`` env var still work — they're not overridden,
    they just stack with ours.

    Called at module top so the filter is installed before Typer
    compiles commands (which is when the wrapped warnings actually fire).
    """
    try:
        from planetarypy.config import config
        val = config["filter_deprecation_warnings"]
    except Exception:
        val = ""
    should_filter = True if val in ("", None) else bool(val)
    if should_filter:
        import warnings
        warnings.filterwarnings("ignore", category=DeprecationWarning)


_apply_warning_filters()


def _resolved_max_table_rows(override: int | None) -> int:
    """Resolve the row-display threshold for table-vs-CSV auto-switching.

    Order:
      1. Explicit CLI flag value (``override``) wins when not ``None``.
      2. Config value ``max_table_rows`` (~/.planetarypy_config.toml).
      3. Hardcoded fallback of ``4`` if the config is unreadable or the
         key is absent (older installs without the backfill yet).
    """
    if override is not None:
        return override
    try:
        from planetarypy.config import config
        val = config["max_table_rows"]
    except Exception:
        val = ""
    if isinstance(val, int) and val > 0:
        return val
    return 3


app = typer.Typer(
    name="plp",
    help="PlanetaryPy — Python tools for planetary science data access.",
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
)

# Help-panel labels used by every command's `rich_help_panel` arg so
# the `plp --help` output groups verbs by intent. typer/rich-click
# decides the panel rendering order internally (not by source order or
# alphabetical) — the grouping itself is what matters; the order isn't
# worth fighting.
_PANEL_DISCOVERY  = "Discovery & browsing"
_PANEL_INSPECT    = "Inspect a product"
_PANEL_FETCH      = "Fetch & download"
_PANEL_VISUALIZE  = "Visualize"
_PANEL_SCIENCE    = "Science computations"
_PANEL_MAINTAIN   = "Maintenance"


# ── fetch ────────────────────────────────────────────────────────────


def _complete_product_id(ctx: click.Context, args: list[str], incomplete: str) -> list[str]:
    """Context-aware tab completion for product IDs based on the key argument.

    Routes the dotted ``key`` argument to :func:`planetarypy.pds.complete_pid`,
    which handles every registered PDS index uniformly (per-index column
    selection, prefix stripping, and bare-PID normalization come from
    :class:`planetarypy.catalog._index_resolver.IndexConfig`). Both forms are
    accepted: a direct index key (``cassini.iss.index``, used by ``plp meta``)
    or a catalog product key (``cassini.iss.edr_sat``, used by ``plp fetch``).
    """
    key = ctx.params.get("key", "")
    if not key:
        return []
    try:
        from planetarypy.pds import complete_pid
        from planetarypy.pds.utils import _all_dotted_index_keys

        if key in _all_dotted_index_keys():
            return complete_pid(incomplete, key)

        from planetarypy.catalog._index_resolver import INDEX_REGISTRY
        parts = tuple(key.split("."))
        if len(parts) == 3 and parts in INDEX_REGISTRY:
            return complete_pid(incomplete, INDEX_REGISTRY[parts].index_key)
    except Exception:
        pass
    return []


@app.command(rich_help_panel=_PANEL_FETCH)
def fetch(
    ctx: typer.Context,
    key: str = typer.Argument(None, help="Dotted product key, e.g. mro.ctx.edr"),
    product_ids: list[str] = typer.Argument(
        None,
        help="One or more product identifiers. Optional when --pids-from is given.",
        autocompletion=_complete_product_id,
    ),
    pids_from: Path = typer.Option(
        None, "--pids-from",
        help="Read PIDs from PATH. Plain text → one PID per line "
             "(blanks + '#'-comments ignored); .csv → parse and "
             "auto-detect the PID column via the index registry, or "
             "specify --pid-key. Use '-' for stdin (treated as plain "
             "text). Mutually exclusive with positional PIDs.",
    ),
    pid_key: str = typer.Option(
        None, "--pid-key",
        help="When --pids-from is a CSV, name the column to read PIDs "
             "from. Overrides the auto-detection that uses --key.",
    ),
    pid_suffix: str = typer.Option(
        None, "--pid-suffix",
        help="Append this string to every PID read from --pids-from. "
             "E.g. --pid-suffix _RED turns each HiRISE obsid into the "
             "corresponding RED product. Ignored when PIDs are supplied "
             "as positional arguments.",
    ),
    prefix: bool = typer.Option(
        False, "--prefix",
        help="Treat each PID that has no exact PRODUCT_ID match but is a "
             "leading prefix of real ones as a request for ALL matching "
             "products (e.g. a HiRISE obsid → every CCD product). Reads "
             "the index to resolve; requires KEY to be a registered PDS "
             "index. Off by default to avoid surprise bulk downloads.",
    ),
    workers: int = typer.Option(
        4, "--workers", "-w",
        help="Parallel download threads in batch mode (default 4).",
    ),
    report: str = typer.Option(
        "errors-only", "--report",
        help="Batch outcome report mode: errors-only (default) | full | jsonl | csv. "
             "Single-PID calls ignore this flag.",
    ),
    force: bool = typer.Option(False, "--force", "-f", help="Re-download even if cached"),
    label_only: bool = typer.Option(False, "--label-only", "-l", help="Download only the label file"),
    here: bool = typer.Option(False, "--here", "-H", help="Download into current directory instead of planetarypy storage"),
    folder: bool = typer.Option(False, "--folder", "-d", help="Print the local folder instead of file paths (composes with `cd`; single-PID only)"),
):
    """Download one or more PDS products by ID.

    Examples:
        plp fetch mro.ctx.edr P02_001916_2221_XI_42N027W            # single
        plp fetch mro.ctx.edr P02_001916_2221_XI_42N027W P03_...    # batch (variadic)
        plp fetch mro.ctx.edr --pids-from my_targets.txt            # batch from file
        plp fetch mro.ctx.edr --pids-from -                          # batch from stdin
        plp fetch mro.hirise.edr ESP_075205_0930 --prefix   # obsid → all CCDs
        cd (plp fetch --folder mro.ctx.edr P02_001916_2221_XI_42N027W)
    """
    if key is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()

    from pathlib import Path

    pids = list(product_ids) if product_ids else []

    if pids_from is not None and pids:
        typer.echo(
            "Error: --pids-from is mutually exclusive with positional PIDs.",
            err=True,
        )
        raise typer.Exit(2)

    if pids_from is not None:
        from planetarypy.pds import read_pids_file
        try:
            pids = read_pids_file(
                pids_from, index_key=key, pid_key=pid_key,
                suffix=pid_suffix,
            )
        except FileNotFoundError as e:
            typer.echo(f"Error: {e}", err=True)
            raise typer.Exit(2)
        except (KeyError, ValueError) as e:
            typer.echo(f"Error: {e}", err=True)
            raise typer.Exit(2)
        if not pids:
            typer.echo(
                f"Error: no PIDs found in {pids_from} (file is empty, "
                "only comments, or the selected column has no values).",
                err=True,
            )
            raise typer.Exit(2)

    if not pids:
        typer.echo("Error: provide at least one PID (positional or --pids-from).", err=True)
        raise typer.Exit(2)

    if prefix:
        from planetarypy.pds import get_index, resolve_pids
        from planetarypy.pds.utils import _all_dotted_index_keys
        if key not in _all_dotted_index_keys():
            typer.echo(
                f"Error: --prefix requires {key!r} to be a registered PDS "
                "index, but it is not. Pass full product IDs instead.",
                err=True,
            )
            raise typer.Exit(2)
        df = get_index(key, allow_refresh=False)
        mapping = resolve_pids(key, pids, df, prefix=True)
        seen: set[str] = set()
        expanded = [
            full for p in pids for full in mapping.get(p, [])
            if not (full in seen or seen.add(full))
        ]
        missed = [p for p in pids if not mapping.get(p)]
        if missed:
            typer.echo(
                f"Warning: {len(missed)} input PID(s) matched nothing "
                f"(exact or prefix): {missed}", err=True,
            )
        if not expanded:
            typer.echo("Error: --prefix resolved no products.", err=True)
            raise typer.Exit(2)
        typer.echo(
            f"--prefix expanded {len(pids)} input(s) → {len(expanded)} "
            "product(s).", err=True,
        )
        pids = expanded

    # Single-PID path preserves the existing output contract: URL to stderr,
    # files (or folder) to stdout, so shell composition stays unchanged.
    if len(pids) == 1:
        from planetarypy.catalog import fetch_product, get_product_urls
        product_id = pids[0]
        typer.echo(f"Resolving {key} / {product_id}...", err=True)
        try:
            for url in get_product_urls(key, product_id).values():
                typer.echo(f"URL: {url}", err=True)
            downloaded = fetch_product(
                key, product_id,
                local_dir=Path.cwd() if here else None,
                label_only=label_only, force=force,
            )
            if folder:
                typer.echo(downloaded.local_dir)
            else:
                for f in downloaded.files:
                    typer.echo(f)
        except Exception as e:
            typer.echo(f"Error: {e}", err=True)
            raise typer.Exit(1)
        return

    # Batch path.
    if folder:
        typer.echo(
            "Error: --folder is only valid with a single PID (use --report "
            "for multi-PID output formats).", err=True,
        )
        raise typer.Exit(2)

    from planetarypy.catalog import OfflineError, fetch_products
    typer.echo(f"Fetching {len(pids)} products from {key} with {workers} workers...", err=True)
    try:
        results = fetch_products(
            key, pids,
            workers=workers,
            label_only=label_only, force=force,
            local_dir=Path.cwd() if here else None,
        )
    except OfflineError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)

    exit_code = _emit_batch_report(results, report, label="Fetch")
    if exit_code != 0:
        raise typer.Exit(exit_code)


def _truncate_pid(pid: str, max_len: int = 60) -> str:
    """Middle-ellipsis truncation for PIDs that would otherwise wrap the
    report. Defaults to 60 so typical PIDs (25-30 chars) are untouched
    but pathological cases (whole CSV rows masquerading as PIDs) are
    tamed before they hit the terminal."""
    if len(pid) <= max_len:
        return pid
    keep = max_len - 1  # one slot for the ellipsis
    head = keep // 2
    tail = keep - head
    return f"{pid[:head]}…{pid[-tail:]}"


def _format_fail_block(pid: str, exc: BaseException) -> str:
    """Render a multi-line FAIL entry: truncated PID on its own line,
    error message indented and wrapped on the lines below."""
    import shutil
    import textwrap

    width = max(40, min(shutil.get_terminal_size((80, 24)).columns, 100))
    body = f"{type(exc).__name__}: {exc}"
    wrapped = textwrap.fill(
        body, width=width,
        initial_indent="      └ ",
        subsequent_indent="        ",
    )
    return f"FAIL  {_truncate_pid(pid)}\n{wrapped}"


def _emit_batch_report(results, mode: str, *, label: str = "Batch") -> int:
    """Print per-PID outcomes from a BatchFetchResult list, return exit code.

    Modes
    -----
    errors-only (default)
        One multi-line FAIL block per failed PID to stderr, blank-line
        separated; trailing summary to stderr. Stdout stays empty so the
        command composes cleanly when all PIDs succeed and the user just
        wants the side-effect (downloads on disk).
    full
        OK lines for successes (one each, stdout), FAIL blocks for
        failures (multi-line, stdout); trailing summary to stderr.
    jsonl
        One JSON object per PID to stdout (machine-readable).
    csv
        Header + one row per PID to stdout.

    Returns
    -------
    int
        ``0`` if all PIDs succeeded, ``1`` if any failed. Always returns
        ``0`` from ``jsonl`` and ``csv`` modes since those are intended
        for downstream parsing where exit-code carries less meaning than
        the per-row ``ok`` flag.
    """
    import json
    import sys

    n_failed = sum(1 for r in results if not r.ok)
    n_ok = len(results) - n_failed

    if mode == "errors-only":
        fail_blocks = [
            _format_fail_block(r.product_id, r.exception)
            for r in results if not r.ok
        ]
        if fail_blocks:
            # Leading + trailing blank line quarantines the report from
            # whatever came before (notably tqdm's progress bar, which
            # leaves the cursor mid-line and would otherwise collide
            # with our first FAIL header).
            typer.echo("\n" + "\n\n".join(fail_blocks) + "\n", err=True)
        typer.echo(
            f"{label} summary: {n_ok}/{len(results)} OK, {n_failed} failed.",
            err=True,
        )
        return 1 if n_failed else 0

    if mode == "full":
        lines = []
        for r in results:
            if r.ok:
                lines.append(f"OK    {_truncate_pid(r.product_id)}")
            else:
                lines.append(_format_fail_block(r.product_id, r.exception))
        # Leading + trailing blank line quarantines the report from
        # tqdm's progress bar above (cursor-mid-line) and the summary
        # below. Blank line BETWEEN entries for scannability.
        typer.echo("\n" + "\n\n".join(lines) + "\n")
        typer.echo(
            f"{label} summary: {n_ok}/{len(results)} OK, {n_failed} failed.",
            err=True,
        )
        return 1 if n_failed else 0

    if mode == "jsonl":
        for r in results:
            obj = {"product_id": r.product_id, "ok": r.ok}
            if r.ok and r.downloaded is not None:
                obj["files"] = [str(f) for f in r.downloaded.files]
            elif r.exception is not None:
                obj["error"] = f"{type(r.exception).__name__}: {r.exception}"
            typer.echo(json.dumps(obj))
        return 0

    if mode == "csv":
        import csv
        writer = csv.writer(sys.stdout)
        writer.writerow(["product_id", "ok", "error", "files"])
        for r in results:
            if r.ok and r.downloaded is not None:
                writer.writerow([
                    r.product_id, "true", "",
                    ";".join(str(f) for f in r.downloaded.files),
                ])
            else:
                err = (f"{type(r.exception).__name__}: {r.exception}"
                       if r.exception else "")
                writer.writerow([r.product_id, "false", err, ""])
        return 0

    typer.echo(
        f"Error: unknown --report mode: {mode!r}. "
        "Choose one of: errors-only, full, jsonl, csv.",
        err=True,
    )
    raise typer.Exit(2)


# ── HiRISE commands ─────────────────────────────────────────────────


def _complete_hirise_obsid_rdr(incomplete: str) -> list[str]:
    """Tab-completion for HiRISE obsids with RDR products (browse, RDR fetch)."""
    from planetarypy.pds import complete_pid
    return complete_pid(incomplete, "mro.hirise.rdr")


def _complete_hirise_obsid_edr(incomplete: str) -> list[str]:
    """Tab-completion for HiRISE obsids from EDR index (all observations)."""
    from planetarypy.pds import complete_pid
    return complete_pid(incomplete, "mro.hirise.edr")


@app.command(rich_help_panel=_PANEL_FETCH)
def hibrowse(
    ctx: typer.Context,
    product_id: str = typer.Argument(
        None,
        help="HiRISE product ID, e.g. PSP_003092_0985_RED or PSP_004238_1135_RED1_1",
        autocompletion=_complete_hirise_obsid_rdr,
    ),
    annotated: bool = typer.Option(True, "--annotated/--clean", "-a/-c", help="Annotated (default) or clean browse"),
    here: bool = typer.Option(False, "--here", "-H", help="Download into current directory"),
    force: bool = typer.Option(False, "--force", "-f", help="Re-download even if cached"),
):
    """Download a HiRISE browse JPEG from EXTRAS.

    Bare observation IDs default to RDR RED browse.

    Examples:
        plp hibrowse PSP_003092_0985_RED          (annotated browse)
        plp hibrowse --clean PSP_003092_0985_RED   (clean browse)
        plp hibrowse ESP_075422_2040_COLOR
        plp hibrowse PSP_004238_1135_RED1_1       (EDR CCD)
        plp hibrowse PSP_003092_0985              (defaults to RDR RED)
    """
    if product_id is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()

    from pathlib import Path
    from planetarypy.instruments.mro.hirise import browse_url, get_browse

    # Show URL immediately so user knows we're waiting on the server
    typer.echo(f"Fetching {browse_url(product_id, annotated=annotated)}", err=True)

    try:
        dest = Path.cwd() if here else None
        outpath = get_browse(product_id, annotated=annotated, dest=dest, force=force)
        # Raw path on stdout so `qgis (plp hibrowse …)` and similar
        # shell substitutions capture just the path.
        typer.echo(outpath)
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)

    import platform
    if platform.system() == "Darwin":
        import subprocess
        subprocess.Popen(["open", str(outpath)])


@app.command(rich_help_panel=_PANEL_FETCH)
def hiedr(
    ctx: typer.Context,
    obsid: str = typer.Argument(None, help="HiRISE observation ID, e.g. PSP_003092_0985",
                                autocompletion=_complete_hirise_obsid_edr),
    red: bool = typer.Option(False, "--red", help="Download RED CCDs (RED0–RED9, 20 files)"),
    ir: bool = typer.Option(False, "--ir", help="Download IR CCDs (IR10–IR11, 4 files)"),
    bg: bool = typer.Option(False, "--bg", help="Download BG CCDs (BG12–BG13, 4 files)"),
    ccds: list[str] = typer.Option(
        None, "--ccds",
        help="Specific CCD numbers. Repeatable AND comma-separated: "
             "'--ccds 4 --ccds 5' is equivalent to '--ccds 4,5'.",
    ),
    here: bool = typer.Option(False, "--here", "-H", help="Download into current directory"),
    force: bool = typer.Option(False, "--force", "-f", help="Re-download even if cached"),
):
    """Download HiRISE EDR channel files by observation ID.

    Downloads both channels (0 and 1) for each CCD in the selected color.
    If no color flag is given, defaults to --red.

    Examples:
        plp hiedr PSP_003092_0985 --red           (all 20 RED files)
        plp hiedr PSP_003092_0985 --red --ccds 4,5 (RED4+RED5 only, 4 files)
        plp hiedr PSP_003092_0985 --ir             (IR10+IR11, 4 files)
        plp hiedr PSP_003092_0985 --bg             (BG12+BG13, 4 files)
        plp hiedr PSP_003092_0985 --here --ccds 4,5 (download to current dir)
    """
    if obsid is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()

    from pathlib import Path
    from planetarypy.instruments.mro.hirise import download_edr, edr_products

    # Default to RED if nothing specified
    if not red and not ir and not bg:
        red = True

    colors = []
    if red:
        colors.append("red")
    if ir:
        colors.append("ir")
    if bg:
        colors.append("bg")

    ccd_nums = _parse_ccds(ccds)
    saveroot = Path.cwd() if here else None

    products = edr_products(obsid, colors=colors, ccds=ccd_nums, saveroot=saveroot)
    typer.echo(f"{obsid}: {len(products)} EDR files from {products[0].url.parent}")

    try:
        download_edr(obsid, colors=colors, ccds=ccd_nums, saveroot=saveroot, overwrite=force)
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)

    typer.echo(f"Stored in: {products[0].local_path.parent}")


@app.command(rich_help_panel=_PANEL_FETCH)
def himos(
    ctx: typer.Context,
    obsid: str = typer.Argument(
        None,
        help="HiRISE observation ID, e.g. PSP_003092_0985",
        autocompletion=_complete_hirise_obsid_edr,
    ),
    red: bool = typer.Option(False, "--red", help="Process RED CCDs"),
    ir: bool = typer.Option(False, "--ir", help="Process IR CCDs"),
    bg: bool = typer.Option(False, "--bg", help="Process BG CCDs"),
    ccds: list[str] = typer.Option(
        None, "--ccds",
        help="Specific CCD numbers. Repeatable AND comma-separated: "
             "'--ccds 4 --ccds 5' is equivalent to '--ccds 4,5'.",
    ),
    mapfile: str = typer.Option(None, "--map", "-m", help="ISIS map projection file (.map)"),
    overwrite: bool = typer.Option(False, "--force", "-f", help="Reprocess even if mosaic exists"),
):
    """Create a HiRISE CCD mosaic from EDR data via ISIS.

    Full pipeline: download → hi2isis → spiceinit → hical → histitch →
    cubenorm → cam2map → equalizer → automos.

    If no color flag is given, defaults to --red.

    Examples:
        plp himos PSP_003092_0985                    (all 10 RED CCDs)
        plp himos PSP_003092_0985 --ccds 4,5         (RED4+RED5 central pair)
        plp himos PSP_003092_0985 --ir               (IR mosaic)
        plp himos PSP_003092_0985 --red --ir --bg    (all three colors)
        plp himos PSP_003092_0985 --map mymap.map    (custom projection)
    """
    if obsid is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()

    from planetarypy.instruments.mro.hirise import create_mosaics

    if not red and not ir and not bg:
        red = True

    colors = []
    if red:
        colors.append("red")
    if ir:
        colors.append("ir")
    if bg:
        colors.append("bg")

    ccd_nums = _parse_ccds(ccds)

    try:
        results = create_mosaics(
            obsid, colors=colors, ccds=ccd_nums,
            mapfile=mapfile, overwrite=overwrite,
        )
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)

    for color, path in results.items():
        typer.echo(f"{color.upper()} mosaic: {path}")


# ── CTX commands ────────────────────────────────────────────────────


def _complete_ctx_pid(incomplete: str) -> list[str]:
    """Tab-completion callback for CTX product IDs."""
    from planetarypy.pds import complete_pid
    return complete_pid(incomplete, "mro.ctx.edr")


@app.command(rich_help_panel=_PANEL_VISUALIZE)
def ctxqv(
    ctx: typer.Context,
    imgid: str = typer.Argument(
        None,
        help="CTX product ID (short or full), e.g. J05_046771_1950",
        autocompletion=_complete_ctx_pid,
    ),
    stride: int = typer.Option(10, "--stride", "-s", help="Downsample factor"),
    save: str = typer.Option(None, "--save", "-o", help="Save to PNG instead of displaying"),
    stretch: str = typer.Option("1,99", "--stretch", "-p", help="Percentile stretch as 'low,high'. Use 'none' to disable."),
    edr: bool = typer.Option(False, "--edr", help="Force raw EDR, skip calibrated files"),
    center_box: int = typer.Option(None, "--center-box", "-c", help="Show full-res center crop of N pixels (default 500 if flag used)"),
):
    """Show a downsampled quickview of a CTX image.

    Automatically uses the best available level:
    map-projected > calibrated > cube > raw EDR.

    Use --center-box to also show a full-resolution crop from the image center.
    """
    if imgid is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()

    import matplotlib.pyplot as plt
    import numpy as np

    arr = None
    level = None
    pid = imgid

    need_center = center_box is not None
    source_path = None

    if not edr:
        try:
            from planetarypy.instruments.mro.ctx.ctx_calib import Calib

            typer.echo(f"Checking for calibrated products for {imgid}...")
            calib = Calib(imgid)
            pid = calib.edr.pid
            arr, path, level = calib.quickview(stride=stride)
            if arr is not None:
                typer.echo(f"Using {level} product: {path.name}")
                source_path = path
        except Exception:
            pass

    if arr is None:
        from planetarypy.instruments.mro.ctx.ctx_edr import EDR

        typer.echo(f"Using raw EDR for {imgid} (stride={stride})...")
        edr_obj = EDR(imgid)
        pid = edr_obj.pid
        arr = edr_obj.quickview(stride=stride)
        level = "edr"
        source_path = edr_obj.path

    typer.echo(f"Image: {arr.shape[1]}x{arr.shape[0]} px [{level}]")

    from planetarypy.plotting import imshow_gray

    if need_center and source_path is not None:
        box = center_box
        # Read only the center crop at full resolution using rioxarray windowed read
        import rioxarray as rxr
        da = rxr.open_rasterio(source_path, chunks=True).isel(band=0, drop=True)
        h, w = da.shape
        cy, cx = h // 2, w // 2
        half = box // 2
        y0 = max(0, cy - half)
        y1 = min(h, cy + half)
        x0 = max(0, cx - half)
        x1 = min(w, cx + half)
        crop = da.values[y0:y1, x0:x1]

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 7),
                                        gridspec_kw={"width_ratios": [1, 1]})
        imshow_gray(arr, stretch=stretch, title=f"{pid} [{level}]", ax=ax1)

        # Draw center box indicator on the overview
        oy0, ox0 = y0 // stride, x0 // stride
        oy1, ox1 = y1 // stride, x1 // stride
        from matplotlib.patches import Rectangle
        rect = Rectangle((ox0, oy0), ox1 - ox0, oy1 - oy0,
                          linewidth=1.5, edgecolor="red", facecolor="none")
        ax1.add_patch(rect)

        imshow_gray(crop, stretch=stretch, title=f"center {box}px full-res", ax=ax2)
        fig.tight_layout()
        typer.echo(f"Center crop: {crop.shape[1]}x{crop.shape[0]} px at ({cx},{cy})")
    else:
        ax = imshow_gray(arr, stretch=stretch, title=f"{pid} [{level}]")
        fig = ax.get_figure()
        fig.tight_layout()

    if save:
        fig.savefig(save, dpi=150, bbox_inches="tight")
        typer.echo(f"Saved to {save}")
    else:
        plt.show()


# ── catalog ──────────────────────────────────────────────────────────

catalog_app = typer.Typer(help="PDS catalog management.", no_args_is_help=True)
app.add_typer(catalog_app, name="catalog", rich_help_panel=_PANEL_DISCOVERY)


@catalog_app.command("build")
def catalog_build(
    force: bool = typer.Option(False, "--force", help="Force rebuild from scratch"),
    validate_urls: bool = typer.Option(False, "--validate-urls", help="Run URL validation after build"),
):
    """Build the PDS catalog database from pdr-tests definitions."""
    from planetarypy.catalog import build_catalog

    typer.echo("Building PDS catalog...")
    stats = build_catalog(force=force)
    typer.echo(
        f"Done: {stats.get('instruments', '?')} instruments, "
        f"{stats.get('product_types', '?')} product types, "
        f"{stats.get('products', '?')} products"
    )
    if stats.get("ambiguous"):
        typer.echo(f"\nAmbiguous mappings ({len(stats['ambiguous'])}): {', '.join(stats['ambiguous'])}")

    if validate_urls:
        from planetarypy.catalog._validation import validate_urls as do_validate
        from planetarypy.config import config

        typer.echo("\nValidating URLs...")
        counts = do_validate(config.storage_root)
        typer.echo(f"URL validation: {counts}")


# ── catalog: browsing helpers + cross-reference to INDEX_REGISTRY ───


def _complete_index_key(incomplete: str) -> list[str]:
    """Segment-aware matcher for registered dotted PDS index keys.

    Prefix match on the whole key wins (so ``mro.<TAB>`` keeps working
    the obvious way). When that produces nothing, falls back to a
    case-insensitive *segment* prefix match — typing ``ctx<TAB>`` or
    ``CTX<TAB>`` then yields ``mro.ctx.edr`` / ``mro.ctx.calib`` because
    the middle dot-segment starts with the incomplete text. Either
    mission, instrument, or indexname alone is enough to drive the
    completion without having to remember the full path.

    This is the testable logic. Wire it into a typer.Argument via
    :func:`_shell_complete_index_key` (the Click-compatible adapter).
    """
    try:
        from planetarypy.pds.utils import _all_dotted_index_keys
        keys = _all_dotted_index_keys()
        matched = [k for k in keys if k.startswith(incomplete)]
        if not matched and incomplete:
            inc = incomplete.lower()
            matched = [
                k for k in keys
                if any(seg.lower().startswith(inc) for seg in k.split("."))
            ]
        return matched
    except Exception:
        return []


def _shell_complete_index_key(ctx, param, incomplete: str) -> list[str]:
    """Click ``shell_complete=`` adapter for :func:`_complete_index_key`.

    Wired via Click's native ``shell_complete=`` kwarg rather than
    Typer's ``autocompletion=`` because Typer's wrapper filters
    candidates via ``value.startswith(incomplete)`` (see
    ``typer/core.py``'s ``_typer_param_setup_autocompletion_compat``),
    which silently drops every segment-prefix candidate — the whole
    point of this completer. Click's ``shell_complete=`` bypasses that
    filter so ``ctx<TAB>`` can actually return ``mro.ctx.edr``.

    Typer emits a ``DeprecationWarning`` about ``shell_complete=`` going
    away in a future version. If/when that lands, the fix is either
    (a) revert to prefix-only completion under Typer's ``autocompletion=``,
    or (b) file an upstream issue asking for a filter opt-out flag.
    """
    return _complete_index_key(incomplete)


def _fetchable_sets():
    """Return three sets covering which catalog entries are fetchable.

    A catalog (mission, instrument, product_key) triple is "fetchable"
    when ``INDEX_REGISTRY`` has an entry for it — meaning ``plp fetch``
    can resolve a product through the registered cumulative index.
    """
    from planetarypy.catalog._index_resolver import INDEX_REGISTRY
    triples = set(INDEX_REGISTRY.keys())
    return {
        "products": triples,
        "instruments": {(m, i) for (m, i, _) in triples},
        "missions": {m for (m, _, _) in triples},
    }


def _catalog_show_mission(mission: str, fetchable):
    from rich.console import Console
    from rich.table import Table

    from planetarypy.catalog import list_instruments
    from planetarypy.catalog._index_resolver import INDEX_REGISTRY
    from planetarypy.pds.utils import _all_dotted_index_keys

    instruments = list_instruments(mission)
    if not instruments:
        typer.echo(f"Unknown mission: {mission!r}", err=True)
        raise typer.Exit(1)

    # Pre-compute the registered indexes per instrument (from the static
    # index URL config), so we can distinguish three states for each
    # instrument: no index at all / indexed-but-no-fetch-resolver / fetchable.
    indexes_by_inst: dict[str, list[str]] = {}
    for k in _all_dotted_index_keys():
        m, i, idx = k.split(".", 2)
        if m == mission:
            indexes_by_inst.setdefault(i, []).append(idx)

    table = Table(
        title=f"{mission} instruments",
        title_style="bold",
        header_style="bold magenta",
        pad_edge=False,
    )
    table.add_column("instrument", style="cyan", no_wrap=True)
    table.add_column("registered indexes", overflow="fold")
    table.add_column("fetchable product types", overflow="fold")

    for inst in instruments:
        idx_names = sorted(indexes_by_inst.get(inst, []))
        fetch_keys = sorted(
            f"{p} → {cfg.index_key}"
            for (m, i, p), cfg in INDEX_REGISTRY.items()
            if (m, i) == (mission, inst)
        )
        if fetch_keys:
            fetch_cell = "\n".join(fetch_keys)
        elif idx_names:
            fetch_cell = "(no fetch resolver)"
        else:
            fetch_cell = ""
        table.add_row(inst, ", ".join(idx_names), fetch_cell)
    Console().print(table)


def _catalog_show_instrument(mission: str, instrument: str, fetchable):
    from rich.console import Console
    from rich.table import Table

    from planetarypy.catalog import list_products
    from planetarypy.catalog._index_resolver import INDEX_REGISTRY

    products = list_products(f"{mission}.{instrument}")
    if not products:
        typer.echo(f"Unknown mission.instrument: {mission!r}.{instrument!r}", err=True)
        raise typer.Exit(1)

    # `list_products` returns normalized_type names; the registry is keyed by
    # raw product_key. Show both perspectives.
    table = Table(
        title=f"{mission}.{instrument} product types",
        title_style="bold",
        header_style="bold magenta",
        pad_edge=False,
    )
    table.add_column("normalized type", style="cyan")
    table.add_column("registered fetchable variants", overflow="fold")

    by_type: dict[str, list[str]] = {}
    for (m, i, p), cfg in INDEX_REGISTRY.items():
        if (m, i) != (mission, instrument):
            continue
        # Best-effort: use the first prefix segment of product_key as the type.
        norm = p.split("_", 1)[0].lower()
        by_type.setdefault(norm, []).append(f"{p} → {cfg.index_key}")

    for ptype in products:
        variants = by_type.get(ptype, [])
        table.add_row(ptype, "\n".join(variants))
    # Surface any registry entries whose product_key didn't map to a normalized type.
    for ptype, variants in by_type.items():
        if ptype not in products:
            table.add_row(ptype + " (extra)", "\n".join(variants))
    Console().print(table)


@catalog_app.command("list")
def catalog_list(
    key: str = typer.Argument(
        None,
        help="Optional dotted key: 'mission' or 'mission.instrument'. "
             "Omit to list missions.",
    ),
):
    """Browse the pdr-tests catalog inventory.

    Examples:
        plp catalog list                       # all missions
        plp catalog list cassini               # cassini instruments
        plp catalog list cassini.iss           # cassini.iss product types
    """
    from rich.console import Console
    from rich.table import Table

    fetchable = _fetchable_sets()

    if key is None:
        from planetarypy.catalog import list_missions, summary

        summary_df = summary().set_index("mission")
        missions = list_missions()
        table = Table(
            title=f"PDS catalog — {len(missions)} missions",
            title_style="bold",
            header_style="bold magenta",
            pad_edge=False,
        )
        table.add_column("mission", style="cyan", no_wrap=True)
        table.add_column("instruments", justify="right")
        table.add_column("product types", justify="right")
        table.add_column("products", justify="right")
        table.add_column("fetchable", justify="center")
        for m in missions:
            row = summary_df.loc[m] if m in summary_df.index else None
            instr_n = int(row["instruments"]) if row is not None else 0
            ptype_n = int(row["product_types"]) if row is not None else 0
            prod_n = int(row["products"]) if row is not None else 0
            mark = "✓" if m in fetchable["missions"] else ""
            table.add_row(m, str(instr_n), str(ptype_n), str(prod_n), mark)
        Console().print(table)
        return

    parts = key.split(".")
    if len(parts) == 1:
        _catalog_show_mission(parts[0], fetchable)
    elif len(parts) == 2:
        _catalog_show_instrument(parts[0], parts[1], fetchable)
    else:
        typer.echo(
            f"Too many dotted parts in {key!r}. Use 'plp catalog show <key>' "
            "for product-type detail.",
            err=True,
        )
        raise typer.Exit(1)


@catalog_app.command("show")
def catalog_show(
    key: str = typer.Argument(
        ...,
        help="Dotted key 'mission.instrument.product_key' for full detail.",
    ),
):
    """Show full catalog + INDEX_REGISTRY info for a single product type."""
    from rich.console import Console
    from rich.table import Table

    from planetarypy.catalog._index_resolver import INDEX_REGISTRY

    parts = key.split(".")
    if len(parts) != 3:
        typer.echo(
            f"Expected 'mission.instrument.product_key', got {key!r}.",
            err=True,
        )
        raise typer.Exit(1)
    mission, instrument, product_key = parts

    cfg = INDEX_REGISTRY.get((mission, instrument, product_key))

    table = Table(
        title=f"{key}",
        title_style="bold",
        header_style="bold magenta",
        show_header=False,
        pad_edge=False,
    )
    table.add_column("field", style="cyan", no_wrap=True)
    table.add_column("value", overflow="fold")
    table.add_row("mission", mission)
    table.add_row("instrument", instrument)
    table.add_row("product_key", product_key)

    if cfg is None:
        table.add_row("fetchable", "no — no INDEX_REGISTRY entry")
    else:
        table.add_row("fetchable", "yes")
        table.add_row("  index_key", cfg.index_key)
        if cfg.archive_url:
            table.add_row("  archive_url", cfg.archive_url)
        if cfg.seti_volume_group:
            table.add_row("  seti_volume_group", cfg.seti_volume_group)
        if cfg.extra_index_keys:
            table.add_row("  extra_index_keys", ", ".join(cfg.extra_index_keys))
        table.add_row("  product_id_col", cfg.product_id_col)
        if cfg.completion_id_col:
            table.add_row("  completion_id_col", cfg.completion_id_col)
        if cfg.pid_strip_prefix_re:
            table.add_row("  pid_strip_prefix_re", cfg.pid_strip_prefix_re)

    # Sample-products count from catalog DB (best-effort; may not be built yet)
    try:
        from planetarypy.catalog import example_products
        df = example_products(mission, instrument, product_key)
        table.add_row("catalog sample products", str(len(df)))
    except Exception:
        pass

    Console().print(table)


@catalog_app.command("search")
def catalog_search(
    query: str = typer.Argument(..., help="Substring to search across catalog fields."),
):
    """Search the catalog (mission/instrument/product_key/product_id)."""
    from rich.console import Console
    from rich.table import Table

    from planetarypy.catalog import search

    df = search(query)
    if df.empty:
        typer.echo(f"No catalog entries match {query!r}.")
        return

    table = Table(
        title=f"Catalog search: {query!r}  ({len(df)} hits)",
        title_style="bold",
        header_style="bold magenta",
        pad_edge=False,
    )
    for col in df.columns:
        table.add_column(str(col), overflow="fold")
    for _, row in df.iterrows():
        table.add_row(*[str(v) if v is not None else "" for v in row])
    Console().print(table)


@catalog_app.command("samples")
def catalog_samples(
    key: str = typer.Argument(
        ...,
        help="Dotted catalog key 'mission.instrument.product_key'.",
    ),
    phase: str = typer.Option(
        None, "--phase",
        help="Filter to one mission phase (e.g. 'saturn', 'cruise').",
    ),
    limit: int = typer.Option(
        20, "--limit", "-n",
        help="Cap rows printed (use 0 for all).",
    ),
):
    """Print the sample products in the catalog DB for a product type.

    Wraps ``planetarypy.catalog.example_products()``. Useful to inspect
    what's actually catalogued for a given product type, especially for
    archives without a registered fetch resolver where these samples
    are the only available products.
    """
    from rich.console import Console
    from rich.table import Table

    from planetarypy.catalog import example_products

    parts = key.split(".")
    if len(parts) != 3:
        typer.echo(
            f"Expected 'mission.instrument.product_key', got {key!r}.",
            err=True,
        )
        raise typer.Exit(1)

    df = example_products(key, phase=phase)
    if df.empty:
        typer.echo(f"No catalog samples for {key!r}"
                   + (f" (phase={phase!r})" if phase else "") + ".")
        return

    shown = df if limit == 0 else df.head(limit)
    title = f"Catalog samples: {key}"
    if phase:
        title += f"  phase={phase!r}"
    title += f"  ({len(shown)}/{len(df)} rows)"

    table = Table(title=title, title_style="bold",
                  header_style="bold magenta", pad_edge=False)
    for col in shown.columns:
        table.add_column(str(col), overflow="fold")
    for _, row in shown.iterrows():
        table.add_row(*[str(v) if v is not None else "" for v in row])
    Console().print(table)


@catalog_app.command("summary")
def catalog_summary():
    """Per-mission counts of instruments, product types, and products."""
    from rich.console import Console
    from rich.table import Table

    from planetarypy.catalog import summary

    df = summary()
    table = Table(title="Catalog summary", title_style="bold",
                  header_style="bold magenta", pad_edge=False)
    for col in df.columns:
        table.add_column(str(col),
                         justify="right" if col != "mission" else "left",
                         style="cyan" if col == "mission" else None,
                         no_wrap=(col == "mission"))
    for _, row in df.iterrows():
        table.add_row(*[str(v) for v in row])
    Console().print(table)


@catalog_app.command("ambiguous")
def catalog_ambiguous():
    """List instruments whose mission/instrument mapping is ambiguous."""
    from rich.console import Console
    from rich.table import Table

    from planetarypy.catalog import ambiguous_mappings

    df = ambiguous_mappings()
    if df.empty:
        typer.echo("No ambiguous mappings.")
        return
    table = Table(title=f"Ambiguous mappings ({len(df)})",
                  title_style="bold", header_style="bold magenta", pad_edge=False)
    for col in df.columns:
        table.add_column(str(col), overflow="fold")
    for _, row in df.iterrows():
        table.add_row(*[str(v) for v in row])
    Console().print(table)


# ── indexes: registered PDS index browse ─────────────────────────────

indexes_app = typer.Typer(
    help="Browse and manage registered PDS indexes.", no_args_is_help=True,
)
app.add_typer(indexes_app, name="indexes", rich_help_panel=_PANEL_DISCOVERY)


@indexes_app.command("list")
def indexes_list(
    key: str = typer.Argument(
        None,
        help="Optional dotted key: 'mission' or 'mission.instrument'. "
             "Omit to list missions.",
    ),
    tree: bool = typer.Option(
        False, "--tree",
        help="Render as a tree (mirrors planetarypy.pds.print_available_indexes).",
    ),
):
    """Browse the registered PDS indexes (the operational fetch surface).

    Examples:
        plp indexes list                     # all missions, summary table
        plp indexes list cassini             # cassini instruments
        plp indexes list cassini.iss         # cassini.iss indexes (with cache status)
        plp indexes list --tree              # full tree (legacy print_available_indexes)
    """
    from rich.console import Console
    from rich.table import Table

    from planetarypy.pds import print_available_indexes
    from planetarypy.pds.utils import _all_dotted_index_keys

    parts = key.split(".") if key else []
    if len(parts) > 2:
        typer.echo(
            f"Too many dotted parts in {key!r}. Use 'plp indexes info <key>' "
            "for single-index detail.",
            err=True,
        )
        raise typer.Exit(1)

    if tree:
        print_available_indexes(
            filter_mission=parts[0] if parts else None,
            filter_instrument=parts[1] if len(parts) == 2 else None,
        )
        return

    all_keys = _all_dotted_index_keys()

    if not parts:
        # Mission summary
        from collections import defaultdict
        by_mission: dict[str, dict[str, list[str]]] = defaultdict(
            lambda: defaultdict(list)
        )
        for k in all_keys:
            m, i, idx = k.split(".", 2)
            by_mission[m][i].append(idx)

        table = Table(
            title=f"Registered PDS indexes — {len(by_mission)} missions, "
                  f"{len(all_keys)} indexes",
            title_style="bold",
            header_style="bold magenta",
            pad_edge=False,
        )
        table.add_column("mission", style="cyan", no_wrap=True)
        table.add_column("instruments", justify="right")
        table.add_column("indexes", justify="right")
        table.add_column("breakdown", overflow="fold")
        for m in sorted(by_mission):
            instruments = by_mission[m]
            breakdown = ", ".join(
                f"{i} ({len(instruments[i])})" for i in sorted(instruments)
            )
            n_indexes = sum(len(v) for v in instruments.values())
            table.add_row(m, str(len(instruments)), str(n_indexes), breakdown)
        Console().print(table)
        return

    if len(parts) == 1:
        mission = parts[0]
        from collections import defaultdict
        by_instrument: dict[str, list[str]] = defaultdict(list)
        for k in all_keys:
            m, i, idx = k.split(".", 2)
            if m == mission:
                by_instrument[i].append(idx)
        if not by_instrument:
            typer.echo(f"No registered indexes for mission {mission!r}.", err=True)
            raise typer.Exit(1)

        table = Table(
            title=f"{mission} — registered indexes",
            title_style="bold",
            header_style="bold magenta",
            pad_edge=False,
        )
        table.add_column("instrument", style="cyan", no_wrap=True)
        table.add_column("indexes", justify="right")
        table.add_column("names", overflow="fold")
        for i in sorted(by_instrument):
            names = sorted(by_instrument[i])
            table.add_row(i, str(len(names)), ", ".join(names))
        Console().print(table)
        return

    # mission.instrument: per-index table with cache + catalog x-ref
    from planetarypy.catalog._index_resolver import INDEX_REGISTRY
    from planetarypy.pds import Index

    mission, instrument = parts
    matching = sorted(
        k for k in all_keys
        if k.startswith(f"{mission}.{instrument}.")
    )
    if not matching:
        typer.echo(
            f"No registered indexes for {mission!r}.{instrument!r}.", err=True
        )
        raise typer.Exit(1)

    catalog_xref: dict[str, list[str]] = {}
    for (m, i, p), cfg in INDEX_REGISTRY.items():
        catalog_xref.setdefault(cfg.index_key, []).append(f"{m}.{i}.{p}")

    table = Table(
        title=f"{mission}.{instrument} — registered indexes",
        title_style="bold",
        header_style="bold magenta",
        pad_edge=False,
    )
    table.add_column("index_key", style="cyan", no_wrap=True)
    table.add_column("cached", justify="center")
    table.add_column("size", justify="right")
    table.add_column("catalog entry", overflow="fold")
    for k in matching:
        try:
            idx = Index(k)
            parq = idx.local_parq_path
            if parq.is_file():
                cached = "✓"
                size = f"{parq.stat().st_size / 1e6:.1f} MB"
            else:
                cached = ""
                size = ""
        except Exception:
            cached = "?"
            size = ""
        xref = ", ".join(catalog_xref.get(k, []))
        table.add_row(k, cached, size, xref)
    Console().print(table)


@indexes_app.command("peek")
def indexes_peek(
    ctx: typer.Context,
    key: str = typer.Argument(
        None, help="Dotted index key, e.g. cassini.cda.index",
        shell_complete=_shell_complete_index_key,
    ),
    rows: int = typer.Option(
        3, "--rows", "-n",
        help="Number of random rows to show (default 3).",
    ),
    columns: list[str] = typer.Option(
        None, "--columns", "-c",
        help="Column projection. Repeatable AND comma-separated: "
             "'-c PRODUCT_ID -c IMAGE_TIME' is equivalent to "
             "'--columns PRODUCT_ID,IMAGE_TIME'. Default keeps every column.",
    ),
):
    """Inspect a registered PDS index: shape, column names, a few random rows.

    Useful when an index has a non-standard schema (e.g. cassini.cda.index
    has FILE_SPECIFICATION_NAME / DATA_SET_ID instead of the usual
    PRODUCT_ID column) and you want to see what's actually there before
    deciding which column to use as the product identifier.

    Output is transposed (one row of the index per column of the table)
    so it stays readable whether the index has 4 columns or 71.
    """
    if key is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()

    from planetarypy.pds import get_index
    from planetarypy.pds.utils import _all_dotted_index_keys

    if key not in _all_dotted_index_keys():
        typer.echo(f"Unknown index key: {key!r}.", err=True)
        raise typer.Exit(1)

    full = get_index(key, allow_refresh=False)
    try:
        df = get_index(key, allow_refresh=False,
                       columns=_parse_columns(columns))
    except KeyError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(2)
    n = max(0, min(rows, len(df)))
    sample = df.sample(n=n) if n > 0 else df.head(0)
    _render_index_rows(key, sample, total_rows=len(full),
                       total_cols=len(df.columns),
                       label=f"showing {n} random")


def pd_isna(v):
    """Lazy-import-friendly NaN check for index-display commands."""
    import pandas as pd
    try:
        return bool(pd.isna(v))
    except (TypeError, ValueError):
        return False


# Common time-column names PDS missions use for chronological ordering.
# Tried in order — first match wins. Per-mission overrides could land
# here as a registry if a real-world index needs one we don't catch.
_INDEX_TIME_COLUMNS: tuple[str, ...] = (
    "START_TIME",
    "OBSERVATION_TIME",
    "IMAGE_TIME",
    "TIME",
)


def _find_time_column(df) -> str | None:
    """Return the first ``_INDEX_TIME_COLUMNS`` member present in ``df``,
    or ``None`` if none is."""
    for col in _INDEX_TIME_COLUMNS:
        if col in df.columns:
            return col
    return None


def _render_index_rows(key: str, subset, *, total_rows: int,
                       total_cols: int, label: str) -> None:
    """Render a transposed Rich table of an index-row subset.

    Shared between ``plp indexes peek`` and ``plp indexes last`` so both
    commands present rows the same way (field-name column on the left,
    one row of the index per output column on the right).
    """
    from rich.console import Console
    from rich.table import Table

    n = len(subset)
    table = Table(
        title=f"{key} — {total_rows:,} rows × {total_cols} cols  ({label})",
        title_style="bold",
        header_style="bold magenta",
        pad_edge=False,
    )
    table.add_column("field", style="cyan", no_wrap=True)
    for i in range(n):
        table.add_column(f"row {i + 1}", overflow="fold")
    for col in subset.columns:
        values = [
            "" if pd_isna(subset[col].iloc[i]) else str(subset[col].iloc[i])
            for i in range(n)
        ]
        table.add_row(str(col), *values)
    Console().print(table)


@indexes_app.command("last")
def indexes_last(
    ctx: typer.Context,
    key: str = typer.Argument(
        None, help="Dotted index key, e.g. mro.ctx.edr",
        shell_complete=_shell_complete_index_key,
    ),
    rows: int = typer.Option(
        3, "--rows", "-n",
        help="Number of trailing rows to show (default 3).",
    ),
    sort: bool = typer.Option(
        False, "--sort", "-s",
        help="Sort by a time column before taking the last rows. "
             "Auto-detects START_TIME / OBSERVATION_TIME / IMAGE_TIME / TIME.",
    ),
    columns: list[str] = typer.Option(
        None, "--columns", "-c",
        help="Column projection. Repeatable AND comma-separated: "
             "'-c PRODUCT_ID -c IMAGE_TIME' is equivalent to "
             "'--columns PRODUCT_ID,IMAGE_TIME'. Applied AFTER --sort so "
             "the time column can drive sorting even if you project it away.",
    ),
):
    """Show the last entries of a registered PDS index, transposed.

    Default order is file order (most PDS indexes are appended
    chronologically so the last row IS the newest). Pass ``--sort`` to
    actually sort by a time column when one is present — useful when
    the index file isn't chronologically ordered.

    The output format matches ``plp indexes peek`` so columns line up
    the same way; only the row selection differs.
    """
    if key is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()

    from planetarypy.pds import get_index
    from planetarypy.pds.utils import _all_dotted_index_keys

    if key not in _all_dotted_index_keys():
        typer.echo(f"Unknown index key: {key!r}.", err=True)
        raise typer.Exit(1)

    df = get_index(key, allow_refresh=False)
    total_rows = len(df)
    n = max(0, min(rows, total_rows))

    label_parts = [f"last {n}"]
    if sort:
        time_col = _find_time_column(df)
        if time_col is None:
            typer.echo(
                f"--sort: no time column found in {key!r}. "
                f"Tried: {', '.join(_INDEX_TIME_COLUMNS)}. "
                "Falling back to file order.",
                err=True,
            )
        else:
            df = df.sort_values(time_col)
            label_parts.append(f"by {time_col}")

    subset = df.tail(n) if n > 0 else df.head(0)

    # Column projection applied AFTER sort so --sort can still use a
    # time column that the user is projecting away from the display.
    col_spec = _parse_columns(columns)
    if col_spec is not None:
        missing = [c for c in col_spec if c not in subset.columns]
        if missing:
            typer.echo(
                f"Error: unknown column(s) {missing!r} in {key!r}. "
                f"Available: {list(subset.columns)!r}",
                err=True,
            )
            raise typer.Exit(2)
        subset = subset[col_spec]

    _render_index_rows(key, subset, total_rows=total_rows,
                       total_cols=len(subset.columns),
                       label=", ".join(label_parts))


def _render_value_counts(key: str, column: str, series, *, total: int,
                         top: int, dropna: bool) -> None:
    """Print a plain aligned value-frequency table for one column.

    Three columns: value, count (comma-grouped), percent-of-total.
    ``top <= 0`` shows every distinct value.
    """
    vc = series.value_counts(dropna=dropna)
    n_distinct = series.nunique(dropna=True)
    shown = vc if top <= 0 else vc.head(top)

    typer.echo(f"{key}.{column} — {total:,} rows, {n_distinct:,} distinct values")
    if top > 0 and len(vc) > top:
        typer.echo(f"(showing top {top})")
    typer.echo("")

    if len(shown) == 0:
        typer.echo("(no values)")
        return

    labels = ["NaN" if pd_isna(k) else str(k) for k in shown.index]
    counts = [int(v) for v in shown.values]
    wlabel = max(len(s) for s in labels)
    wcount = max(len(f"{c:,}") for c in counts)
    for label, c in zip(labels, counts):
        pct = 100.0 * c / total if total else 0.0
        typer.echo(f"{label:<{wlabel}}  {c:>{wcount},}  {pct:5.1f}%")


@indexes_app.command("counts")
def indexes_counts(
    ctx: typer.Context,
    key: str = typer.Argument(
        None, help="Dotted index key, e.g. mro.ctx.edr",
        shell_complete=_shell_complete_index_key,
    ),
    column: str = typer.Argument(
        None, help="Column to tabulate. For several columns use --columns.",
    ),
    columns: list[str] = typer.Option(
        None, "--columns", "-c",
        help="Tabulate several columns (one block each). Repeatable AND "
             "comma-separated: '-c TARGET_NAME -c MISSION_PHASE_NAME' is "
             "equivalent to '--columns TARGET_NAME,MISSION_PHASE_NAME'.",
    ),
    top: int = typer.Option(
        10, "--top", "-t",
        help="Show the N most frequent values (default 10). Use 0 for all.",
    ),
    dropna: bool = typer.Option(
        False, "--dropna",
        help="Exclude missing (NaN) values from the counts.",
    ),
):
    """Tabulate value frequencies for one or more index columns.

    A quick ``pandas.value_counts`` view: how many rows carry each distinct
    value of a column, with percent-of-total. Handy for categorical columns
    (TARGET_NAME, MISSION_PHASE_NAME, INSTRUMENT_MODE_ID) where you want the
    lay of the land before filtering.

    Examples:
        plp indexes counts mro.ctx.edr TARGET_NAME
        plp indexes counts mro.ctx.edr MISSION_PHASE_NAME --top 0
        plp indexes counts mro.ctx.edr -c TARGET_NAME,MISSION_PHASE_NAME
    """
    if key is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()

    from planetarypy.pds import get_index
    from planetarypy.pds.utils import _all_dotted_index_keys

    if key not in _all_dotted_index_keys():
        typer.echo(f"Unknown index key: {key!r}.", err=True)
        raise typer.Exit(1)

    col_list: list[str] = []
    if column:
        col_list.append(column)
    parsed = _parse_columns(columns)
    if parsed:
        col_list.extend(parsed)
    seen: set[str] = set()
    col_list = [c for c in col_list if not (c in seen or seen.add(c))]
    if not col_list:
        typer.echo(
            "Specify a column (positional) or --columns COL[,COL...].",
            err=True,
        )
        raise typer.Exit(2)

    df = get_index(key, allow_refresh=False)
    missing = [c for c in col_list if c not in df.columns]
    if missing:
        typer.echo(
            f"Error: unknown column(s) {missing!r} in {key!r}. "
            f"Available: {list(df.columns)!r}",
            err=True,
        )
        raise typer.Exit(2)

    total = len(df)
    for i, col in enumerate(col_list):
        if i:
            typer.echo("")
        _render_value_counts(key, col, df[col], total=total, top=top,
                             dropna=dropna)


@indexes_app.command("select")
def indexes_select(
    ctx: typer.Context,
    key: str = typer.Argument(
        None, help="Dotted index key, e.g. mro.ctx.edr",
        shell_complete=_shell_complete_index_key,
    ),
    product_ids: list[str] = typer.Argument(
        None,
        help="One or more PIDs to filter to. Optional when --pids-from is given.",
    ),
    pids_from: Path = typer.Option(
        None, "--pids-from",
        help="Read PIDs from PATH. Plain text → one PID per line "
             "(blanks + '#'-comments ignored); .csv → parse and "
             "auto-detect the PID column via the index registry, or "
             "specify --pid-key. Use '-' for stdin (treated as plain "
             "text). Mutually exclusive with positional PIDs.",
    ),
    pid_key: str = typer.Option(
        None, "--pid-key",
        help="When --pids-from is a CSV, name the column to read PIDs "
             "from. Overrides the auto-detection that uses KEY.",
    ),
    pid_suffix: str = typer.Option(
        None, "--pid-suffix",
        help="Append this string to every PID read from --pids-from. "
             "Useful for files that carry observation-level identifiers "
             "when the index actually keys on a more specific product "
             "(e.g. HiRISE obsid + '_RED').",
    ),
    fmt: str = typer.Option(
        "auto", "--format",
        help="Output format: auto (default) | table | csv | jsonl. "
             "auto = transposed Rich table when matched rows < "
             "--max-table-rows, otherwise CSV.",
    ),
    max_table_rows: int = typer.Option(
        None, "--max-table-rows",
        help="Max filtered rows that still get the transposed table; "
             "above this count --format=auto switches to CSV. Defaults "
             "to the `max_table_rows` config key (3 if unset).",
    ),
    report: str = typer.Option(
        "errors-only", "--report",
        help="Missing-PID report mode: errors-only (default summary on "
             "stderr) | full (list every missing PID on stderr).",
    ),
    columns: list[str] = typer.Option(
        None, "--columns", "-c",
        help="Column projection. Repeatable AND comma-separated: "
             "'-c PRODUCT_ID -c IMAGE_TIME' is equivalent to "
             "'--columns PRODUCT_ID,IMAGE_TIME'. Applied to every "
             "output format (table / csv / jsonl).",
    ),
):
    """Filter a registered PDS index to specific PIDs and render the rows.

    Built for the "I have a small list of PIDs, show me those rows" case
    — the natural companion to ``plp fetch ... --pids-from``. Output goes
    to stdout (so it pipes cleanly); the resolution report (prefix
    expansions and missing PIDs) always goes to stderr regardless of
    ``--format``.

    A PID that matches no full PRODUCT_ID exactly but is a leading prefix
    of real ones expands automatically to all matching products — so a
    HiRISE obsid handed to the per-CCD EDR index returns every CCD
    product. An exact match is never expanded.

    Examples:
        plp indexes select mro.ctx.edr P02_001916_2221_XI_42N027W
        plp indexes select mro.ctx.edr P_A P_B P_C
        plp indexes select mro.hirise.edr ESP_075205_0930   # obsid → all CCD products
        plp indexes select mro.ctx.edr --pids-from my_targets.txt
        plp indexes select mro.ctx.edr --pids-from - --format jsonl < pids.txt
    """
    if key is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()

    from planetarypy.pds import get_index, pid_column, resolve_pids
    from planetarypy.pds.utils import _all_dotted_index_keys

    if key not in _all_dotted_index_keys():
        typer.echo(f"Unknown index key: {key!r}.", err=True)
        raise typer.Exit(1)

    pids = list(product_ids) if product_ids else []

    if pids_from is not None and pids:
        typer.echo(
            "Error: --pids-from is mutually exclusive with positional PIDs.",
            err=True,
        )
        raise typer.Exit(2)

    if pids_from is not None:
        from planetarypy.pds import read_pids_file
        try:
            pids = read_pids_file(
                pids_from, index_key=key, pid_key=pid_key,
                suffix=pid_suffix,
            )
        except FileNotFoundError as e:
            typer.echo(f"Error: {e}", err=True)
            raise typer.Exit(2)
        except (KeyError, ValueError) as e:
            typer.echo(f"Error: {e}", err=True)
            raise typer.Exit(2)
        if not pids:
            typer.echo(
                f"Error: no PIDs found in {pids_from} (file is empty, "
                "only comments, or the selected column has no values).",
                err=True,
            )
            raise typer.Exit(2)

    if not pids:
        typer.echo(
            "Error: provide at least one PID (positional or --pids-from).",
            err=True,
        )
        raise typer.Exit(2)

    full_df = get_index(key, allow_refresh=False)
    pcol = pid_column(key, full_df)
    # Resolve each requested PID against the index: an exact PRODUCT_ID
    # wins; otherwise a leading-prefix PID (e.g. a HiRISE obsid) expands
    # to every product it prefixes. resolve_pids is the generic mechanism.
    mapping = resolve_pids(key, pids, full_df, prefix=True)
    wanted = {full for ids in mapping.values() for full in ids}
    filtered = full_df[full_df[pcol].astype(str).isin(wanted)]
    expanded = {p: ids for p, ids in mapping.items() if ids and ids != [p]}
    missing = [p for p, ids in mapping.items() if not ids]

    # Column projection: validate against the unprojected `filtered`
    # (which still has every column) and apply to the display copy.
    col_spec = _parse_columns(columns)
    if col_spec is not None:
        bad = [c for c in col_spec if c not in filtered.columns]
        if bad:
            typer.echo(
                f"Error: unknown column(s) {bad!r} in {key!r}. "
                f"Available: {list(filtered.columns)!r}",
                err=True,
            )
            raise typer.Exit(2)
        filtered = filtered[col_spec]

    # Pick effective format.
    if fmt == "auto":
        threshold = _resolved_max_table_rows(max_table_rows)
        effective = "table" if len(filtered) <= threshold else "csv"
    elif fmt in ("table", "csv", "jsonl"):
        effective = fmt
    else:
        typer.echo(
            f"Error: unknown --format: {fmt!r}. "
            "Choose one of: auto, table, csv, jsonl.",
            err=True,
        )
        raise typer.Exit(2)

    # Skip emitting on an empty result: a 0-row transposed table would
    # dump the schema (all field names, no values) and read like a bug.
    # The stderr summary below explains the empty result instead.
    if len(filtered):
        if effective == "table":
            _render_index_rows(
                key, filtered,
                total_rows=len(full_df), total_cols=len(full_df.columns),
                label=f"{len(filtered)} of {len(pids)} requested",
            )
        elif effective == "csv":
            import csv
            import sys
            writer = csv.writer(sys.stdout)
            writer.writerow(list(filtered.columns))
            for _, row in filtered.iterrows():
                writer.writerow([
                    "" if pd_isna(v) else str(v) for v in row.tolist()
                ])
        elif effective == "jsonl":
            import json
            for _, row in filtered.iterrows():
                obj = {
                    col: (None if pd_isna(row[col]) else _jsonable(row[col]))
                    for col in filtered.columns
                }
                typer.echo(json.dumps(obj))

    # Resolution report (always on stderr; format-independent). Prefix
    # expansions are noted first so the user sees a short ID fanned out.
    for p, ids in expanded.items():
        typer.echo(f"  {p!r} → {len(ids)} products by prefix", err=True)

    # Missing-PID report.
    if report == "errors-only":
        if missing:
            typer.echo(
                f"select summary: {len(filtered)} rows / "
                f"{len(pids)} requested, {len(missing)} not found.",
                err=True,
            )
        else:
            typer.echo(
                f"select summary: {len(filtered)} rows / "
                f"{len(pids)} requested, all found.",
                err=True,
            )
    elif report == "full":
        for pid in missing:
            typer.echo(f"MISS {pid}", err=True)
        typer.echo(
            f"select summary: {len(filtered)} rows / "
            f"{len(pids)} requested, {len(missing)} not found.",
            err=True,
        )
    else:
        typer.echo(
            f"Error: unknown --report mode: {report!r}. "
            "Choose one of: errors-only, full.",
            err=True,
        )
        raise typer.Exit(2)

    if missing:
        raise typer.Exit(1)


def _parse_ccds(specs) -> list[int] | None:
    """Flatten a ``--ccds`` spec into a list of CCD numbers (ints).

    Same dual-idiom shape as :func:`_parse_columns` — accepts repeated
    flags, comma-separated values inside one flag, and mixed forms:

    - ``--ccds 4 --ccds 5`` → ``[4, 5]``
    - ``--ccds "4,5"`` → ``[4, 5]``
    - ``--ccds 4 --ccds "5,6"`` → ``[4, 5, 6]``

    Returns ``None`` when no spec is given. Raises ``typer.BadParameter``
    on a value that isn't a positive int — CCD numbers are always
    small integers (HiRISE: 0..13).
    """
    if specs is None:
        return None
    if isinstance(specs, str):
        specs = [specs]
    nums: list[int] = []
    for spec in specs:
        if not spec:
            continue
        for n in spec.split(","):
            n = n.strip()
            if not n:
                continue
            try:
                nums.append(int(n))
            except ValueError:
                raise typer.BadParameter(
                    f"--ccds expects integers; got {n!r} in {spec!r}."
                )
    return nums or None


def _parse_columns(specs) -> list[str] | None:
    """Flatten a ``--columns`` spec into a list of column names.

    Accepts both idioms — and any mix:

    - **Repeated flag** (Typer ``multiple=True``):
      ``-c PRODUCT_ID -c START_TIME`` → ``["PRODUCT_ID", "START_TIME"]``
    - **Comma-separated inside one flag**:
      ``--columns "PRODUCT_ID,START_TIME"`` → ``["PRODUCT_ID", "START_TIME"]``
    - **Mixed**:
      ``-c PRODUCT_ID -c "START,STOP"`` → ``["PRODUCT_ID", "START", "STOP"]``

    Returns ``None`` when no spec was given (so callers default to
    "keep all columns"). Whitespace around each name is stripped, so
    ``-c "PRODUCT_ID , START_TIME"`` works.
    """
    if specs is None:
        return None
    # Coerce a single string to a one-element list for uniform handling
    # (back-compat for callers that still pass a string directly).
    if isinstance(specs, str):
        specs = [specs]
    cols: list[str] = []
    for spec in specs:
        if not spec:
            continue
        for c in spec.split(","):
            c = c.strip()
            if c:
                cols.append(c)
    return cols or None


def _jsonable(value):
    """Best-effort coercion to a JSON-serializable scalar."""
    import datetime as _dt
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (_dt.date, _dt.datetime)):
        return value.isoformat()
    return str(value)


def _format_when(d) -> str:
    """Render a datetime with a short relative-time suffix.

    Examples:
        ``2026-05-29 10:16:23  (4d ago)``
        ``(never)``
    """
    if d is None:
        return "(never)"
    import datetime as _dt
    now = _dt.datetime.now()
    # Align tz-awareness if one side has it and the other doesn't.
    if d.tzinfo is not None and now.tzinfo is None:
        now = now.replace(tzinfo=d.tzinfo)
    elif d.tzinfo is None and now.tzinfo is not None:
        d = d.replace(tzinfo=now.tzinfo)
    secs = (now - d).total_seconds()
    if secs < 0:
        rel = "in the future"
    elif secs < 60:
        rel = "just now"
    elif secs < 3600:
        rel = f"{int(secs // 60)}m ago"
    elif secs < 86400:
        rel = f"{int(secs // 3600)}h ago"
    else:
        rel = f"{int(secs // 86400)}d ago"
    return f"{d.strftime('%Y-%m-%d %H:%M:%S')}  ({rel})"


@indexes_app.command("info")
def indexes_info(
    ctx: typer.Context,
    key: str = typer.Argument(None, help="Dotted index key, e.g. cassini.iss.index",
                              shell_complete=_shell_complete_index_key),
):
    """Show config + cache status for a registered PDS index."""
    if key is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()

    from rich.console import Console
    from rich.table import Table

    from planetarypy.pds import Index
    from planetarypy.pds.utils import (
        _all_dotted_index_keys,
        _completion_id_col_for,
        _index_config_for,
    )

    if key not in _all_dotted_index_keys():
        typer.echo(f"Unknown index key: {key!r}.", err=True)
        raise typer.Exit(1)

    idx = Index(key)
    cfg = _index_config_for(key)

    table = Table(title=key, title_style="bold", show_header=False, pad_edge=False)
    table.add_column("field", style="cyan", no_wrap=True)
    table.add_column("value", overflow="fold")

    table.add_row("index_key", key)
    try:
        table.add_row("remote URL", str(idx.url) if idx.url else "(unresolved)")
    except Exception as e:
        table.add_row("remote URL", f"(error: {e})")
    table.add_row("remote type", idx.remote_type)

    parq = idx.local_parq_path
    if parq.is_file():
        size_mb = parq.stat().st_size / 1e6
        table.add_row("local cached", f"yes — {parq}  ({size_mb:.1f} MB)")
    else:
        table.add_row("local cached", "no")

    # Freshness state from the access log (when did we last download the
    # parquet, when did we last check upstream, and is a newer one waiting).
    try:
        log = idx.remote.log
        table.add_row("last updated", _format_when(log.last_update))
        table.add_row("last checked", _format_when(log.last_check))
        try:
            ua = idx.update_available
        except Exception as e:
            ua_text = f"(check failed: {e})"
        else:
            ua_text = "yes — run `plp indexes refresh --cache KEY`" if ua else "no"
        table.add_row("update available?", ua_text)
    except Exception as e:
        table.add_row("last updated/checked", f"(unavailable: {e})")

    table.add_row(
        "completion column", _completion_id_col_for(key)
    )
    if cfg is not None:
        table.add_row("PID column", cfg.product_id_col)
        if cfg.pid_strip_prefix_re:
            table.add_row("pid_strip_prefix_re", cfg.pid_strip_prefix_re)
        if cfg.archive_url:
            table.add_row("archive_url", cfg.archive_url)
        if cfg.seti_volume_group:
            table.add_row("seti_volume_group", cfg.seti_volume_group)

    # Catalog cross-reference
    catalog_keys = []
    from planetarypy.catalog._index_resolver import INDEX_REGISTRY
    for (m, i, p), c in INDEX_REGISTRY.items():
        if c.index_key == key or key in c.extra_index_keys:
            catalog_keys.append(f"{m}.{i}.{p}")
    if catalog_keys:
        table.add_row("catalog entries", "\n".join(sorted(catalog_keys)))

    Console().print(table)


@indexes_app.command("refresh")
def indexes_refresh(
    ctx: typer.Context,
    config: bool = typer.Option(
        False, "--config",
        help="Force-refresh upstream URL config (planetarypy_index_urls.toml).",
    ),
    cache: str = typer.Option(None, "--cache",
                              help="Re-download a specific index's cumulative parquet.",
                              shell_complete=_shell_complete_index_key),
):
    """Refresh upstream index config or re-download a single index."""
    if not config and not cache:
        # No action requested → show help instead of a curt error so the
        # user can read the available switches in place.
        typer.echo(ctx.get_help())
        raise typer.Exit()

    if config:
        from planetarypy.pds.static_index import ConfigHandler
        h = ConfigHandler(force_update=True)
        typer.echo(f"Refreshed upstream config → {h.path}")

    if cache:
        from planetarypy.pds import Index
        idx = Index(cache, force_config_update=False)
        typer.echo(f"Re-downloading {cache} ...", err=True)
        idx.download(force=True)
        typer.echo(f"Cached at: {idx.local_parq_path}")


# ── CTX housekeeping ────────────────────────────────────────────────


@app.command("ctx-migrate", rich_help_panel=_PANEL_MAINTAIN)
def ctx_migrate(
    dry_run: bool = typer.Option(
        False, "--dry-run", "-n",
        help="Show what would be moved without touching anything",
    ),
):
    """Move existing CTX files on disk to match current [edr.local] layout.

    Scans each ``mrox_*`` volume folder under the configured EDR local
    root and relocates any file named ``<pid>.<ext>`` (26-char CTX
    product_id) to the location that the active config dictates.
    Idempotent: a second run does nothing.

    Typical use: after switching ``[edr.local].with_pid`` between
    ``false`` and ``true``, run this once to bring existing downloads
    (and any co-located derived files) into the new layout.
    """
    from planetarypy.instruments.mro.ctx.ctx_edr import _edr_local_folder

    root = _edr_local_folder()
    if not root.is_dir():
        typer.echo(f"No CTX storage at {root}", err=True)
        raise typer.Exit(1)

    moved = 0
    skipped = 0
    conflicts = 0
    for vol_dir in sorted(root.glob("mrox_*")):
        if not vol_dir.is_dir():
            continue
        # Walk recursively so files already nested in <pid>/ subfolders
        # get tallied as "already in place", not silently skipped.
        for f in vol_dir.rglob("*"):
            if not f.is_file():
                continue
            head, sep, _ = f.name.partition(".")
            if not sep or len(head) != 26:
                continue
            pid = head
            target = _edr_local_folder(volume=vol_dir.name, pid=pid) / f.name
            if f.resolve() == target.resolve():
                skipped += 1
                continue
            if target.exists():
                typer.echo(
                    f"[conflict] {f} → {target} (target already exists, skipping)",
                    err=True,
                )
                conflicts += 1
                continue
            if dry_run:
                typer.echo(f"[dry-run] {f} → {target}")
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                f.rename(target)
                typer.echo(f"{f} → {target}")
            moved += 1
    verb = "would move" if dry_run else "moved"
    typer.echo(
        f"{verb}: {moved}; already in place: {skipped}; conflicts: {conflicts}",
        err=True,
    )


# ── spicer ───────────────────────────────────────────────────────────


@app.command(rich_help_panel=_PANEL_SCIENCE)
def spicer(
    ctx: typer.Context,
    body: str = typer.Argument(None, help="NAIF body name, e.g. Mars, Moon, Enceladus"),
    time: str = typer.Option(None, "--time", "-t", help="UTC time (default: now)"),
    lon: float = typer.Option(None, "--lon", help="Longitude [deg] for surface illumination"),
    lat: float = typer.Option(None, "--lat", help="Latitude [deg] for surface illumination"),
):
    """Show current SPICE data for a solar system body.

    Without --lon/--lat, shows global properties (L_s, subsolar point,
    solar constant). With coordinates, adds surface illumination.

    Examples:
        plp spicer Mars
        plp spicer Moon --time 2024-06-15T12:00:00
        plp spicer Mars --lon 137.4 --lat -4.6
    """
    if body is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()

    from planetarypy.spice.spicer import Spicer

    try:
        s = Spicer(body)
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)

    typer.echo(f"\n  {s.body}")
    typer.echo(f"  {'=' * len(s.body)}")
    typer.echo(f"  Radii:          {s.radii.a:.1f} x {s.radii.b:.1f} x {s.radii.c:.1f} km")
    typer.echo(f"  Reference frame: {s.ref_frame}")

    try:
        ls = s.solar_longitude(time)
        typer.echo(f"\n  Solar longitude (L_s): {ls:.1f}")
    except Exception:
        typer.echo("\n  Solar longitude:       (needs ephemeris kernels)")

    try:
        ss_lon, ss_lat = s.subsolar_point(time)
        typer.echo(f"  Sub-solar point:       lon={ss_lon:.2f}, lat={ss_lat:.2f}")
    except Exception:
        typer.echo("  Sub-solar point:       (needs ephemeris kernels)")

    try:
        sc = s.solar_constant(time)
        typer.echo(f"  Solar constant:        {sc:.0f} W/m²")
    except Exception:
        typer.echo("  Solar constant:        (needs ephemeris kernels)")

    if lon is not None and lat is not None:
        try:
            illum = s.illumination(lon=lon, lat=lat, time=time)
            typer.echo(f"\n  Surface at ({lon}, {lat}):")
            typer.echo(f"    Solar incidence:   {illum.solar_incidence:.1f}")
            typer.echo(f"    Solar flux:        {illum.solar_flux:.0f} W/m²")
            typer.echo(f"    Local solar time:  {illum.local_solar_time}")

            az = s.solar_azimuth_at(lon, lat, time)
            typer.echo(f"    Solar azimuth:     {az:.1f} (CW from N)")
        except Exception as e:
            typer.echo("\n  Surface illumination: (needs ephemeris kernels)")
            typer.echo(f"    {e}")

    typer.echo()


# TODO: plp geo sub-app over planetarypy.geo is the next planned CLI
#       surface (Discovery panel). Verbs: crs / bodies / project /
#       unproject / pixel2lonlat / lonlat2pixel. Per the thin-wrapper
#       rule, audit planetarypy.geo for Python-API gaps first — a
#       `bodies()` or `crs_for(body)` lookup likely needs adding.


# ── spice: kernel discovery + fetch ──────────────────────────────────

spice_app = typer.Typer(
    help="SPICE kernel discovery and date-scoped fetching across NAIF "
         "mission archives.",
    no_args_is_help=True,
)
app.add_typer(spice_app, name="spice", rich_help_panel=_PANEL_DISCOVERY)


def _complete_spice_mission(incomplete: str) -> list[str]:
    """Tab completion for the NAIF mission shorthand argument."""
    from planetarypy.spice.archived_kernels import datasets
    return sorted(m for m in datasets.index
                  if m.lower().startswith(incomplete.lower()))


@spice_app.command("missions")
def spice_missions():
    """List every mission archive NAIF publishes (~39 entries).

    Each row shows the shorthand to use as the mission argument to
    `plp spice info` / `plp spice fetch`, the full mission name, the
    date range covered by the archive, and the cumulative kernel-bundle
    size on the NAIF subsetter.

    \b
    Examples:
        plp spice missions
    """
    from rich.console import Console
    from rich.table import Table

    from planetarypy.spice.archived_kernels import datasets

    table = Table(
        title=f"NAIF mission kernel archives ({len(datasets)})",
        title_style="bold",
        header_style="bold magenta",
        pad_edge=False,
    )
    table.add_column("shorthand", style="cyan", no_wrap=True)
    table.add_column("mission")
    table.add_column("start", no_wrap=True)
    table.add_column("stop", no_wrap=True)
    table.add_column("size (GB)", justify="right", style="dim")

    for shorthand, row in datasets.iterrows():
        table.add_row(
            shorthand,
            str(row["Mission Name"]),
            str(row["Start Time"]),
            str(row["Stop Time"]),
            f"{row['Data Size (GB)']:g}",
        )
    Console().print(table)


@spice_app.command("info")
def spice_info(
    ctx: typer.Context,
    mission: str = typer.Argument(
        None,
        help="NAIF mission shorthand, e.g. cassini, mro, dawn.",
        autocompletion=_complete_spice_mission,
    ),
):
    """Show date range + archive metadata for one mission.

    Use this before `plp spice fetch` to discover the date window the
    archive covers, the cumulative bundle size, and the upstream
    archive/readme URLs.

    \b
    Examples:
        plp spice info cassini
        plp spice info mro
    """
    if mission is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()

    from planetarypy.spice.archived_kernels import datasets

    if mission not in datasets.index:
        _suggest_and_exit(
            mission, sorted(datasets.index), "Unknown mission:",
        )

    row = datasets.loc[mission]

    from rich.console import Console
    from rich.table import Table

    table = Table(
        title=f"{row['Mission Name']} ({mission})",
        title_style="bold",
        header_style="bold magenta",
        show_header=False,
        pad_edge=False,
    )
    table.add_column("field", style="cyan", no_wrap=True)
    table.add_column("value", overflow="fold")
    for label, key in (
        ("date range", None),         # synthesized
        ("data size", "Data Size (GB)"),
        ("PDS version", "PDS3 or PDS4"),
        ("archive readme", "Archive Readme"),
        ("archive link", "Archive Link"),
        ("subsetter URL", "Subset Link"),
    ):
        if key is None:
            value = f"{row['Start Time']}  →  {row['Stop Time']}"
        else:
            value = str(row[key])
            if label == "data size":
                value = f"{row[key]:g} GB"
            elif label == "PDS version":
                value = f"PDS{row[key]}"
        table.add_row(label, value)
    Console().print(table)


@spice_app.command("fetch")
def spice_fetch(
    ctx: typer.Context,
    mission: str = typer.Argument(
        None,
        help="NAIF mission shorthand, e.g. cassini, mro, dawn.",
        autocompletion=_complete_spice_mission,
    ),
    start: str = typer.Option(
        None, "--start", "-s",
        help="UTC start date (inclusive), e.g. 2006-06-01",
    ),
    stop: str = typer.Option(
        None, "--stop", "-e",
        help="UTC stop date (inclusive), e.g. 2006-06-30",
    ),
    save_location: str = typer.Option(
        None, "--save-location", "-d",
        help="Directory for downloaded kernels + metakernel (default: "
             "planetarypy storage_root)",
    ),
):
    """Download a date-scoped subset of a mission's SPICE kernels.

    Thin wrapper around `planetarypy.spice.archived_kernels.
    get_metakernel_and_files()`. Generates a metakernel referencing the
    subsetted kernels; prints the metakernel path to stdout so shell
    composition Just Works:

    \b
        ISIS_PRE='spiceinit from=input.cub mkpre=$(plp spice fetch \\
            cassini --start 2006-06-01 --stop 2006-06-30)'

    \b
    Examples:
        plp spice fetch cassini --start 2006-06-01 --stop 2006-06-30
        plp spice fetch mro -s 2014-01-01 -e 2014-01-31
    """
    if mission is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()
    if start is None or stop is None:
        typer.echo("Error: --start and --stop are both required.", err=True)
        raise typer.Exit(2)

    from planetarypy.spice.archived_kernels import datasets, get_metakernel_and_files

    if mission not in datasets.index:
        _suggest_and_exit(
            mission, sorted(datasets.index), "Unknown mission:",
        )

    try:
        metakernel = get_metakernel_and_files(
            mission, start, stop, save_location=save_location,
        )
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)

    # stdout: the metakernel path (pipe-safe for shell composition).
    typer.echo(metakernel)


@spice_app.command("cached")
def spice_cached(
    bytes_total: bool = typer.Option(
        False, "--total", "-T",
        help="Also print cumulative on-disk size at the bottom.",
    ),
):
    """Show every SPICE kernel currently cached under storage_root.

    Walks ``{storage_root}/spice_kernels/`` and groups by top-level
    directory: ``generic`` (LSK/PCK/DE-series/satellites) and per-mission
    subsets (cassini, mro, …). Useful before kicking off a `plp spice
    fetch` to check whether kernels are already on disk.

    \b
    Examples:
        plp spice cached
        plp spice cached --total
    """
    from rich.console import Console
    from rich.table import Table

    from planetarypy.spice.archived_kernels import list_cached_kernels
    from planetarypy.spice.config import KERNEL_STORAGE

    cached = list_cached_kernels()
    if not cached:
        typer.echo(
            f"No SPICE kernels cached under {KERNEL_STORAGE}.", err=True,
        )
        typer.echo(
            "Use `plp spice fetch` or `plp spice generic <type>` to "
            "populate it.", err=True,
        )
        raise typer.Exit()

    table = Table(
        title=f"Cached SPICE kernels under {KERNEL_STORAGE}",
        title_style="bold",
        header_style="bold magenta",
        pad_edge=False,
    )
    table.add_column("group", style="cyan", no_wrap=True)
    table.add_column("files", justify="right")
    table.add_column("size", justify="right", style="dim")
    table.add_column("sample", overflow="fold")

    grand_total = 0
    for group, files in cached.items():
        size = sum(p.stat().st_size for p in files)
        grand_total += size
        sample = ", ".join(p.name for p in files[:3])
        if len(files) > 3:
            sample += f", … (+{len(files) - 3} more)"
        table.add_row(group, str(len(files)),
                      _humanize_bytes(size), sample)
    Console().print(table)
    if bytes_total:
        typer.echo(f"\ntotal: {_humanize_bytes(grand_total)}", err=True)


def _humanize_bytes(n: int) -> str:
    """Convert a raw byte count to a human-readable string."""
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024


def _complete_generic_alias(incomplete: str) -> list[str]:
    """Tab completion for the generic-kernel alias argument."""
    from planetarypy.spice.generic_kernels import GENERIC_KERNEL_ALIASES
    return sorted(a for a in GENERIC_KERNEL_ALIASES
                  if a.startswith(incomplete.lower()))


@spice_app.command("generic")
def spice_generic(
    ctx: typer.Context,
    name: str = typer.Argument(
        None,
        help="Short alias (lsk/pck/masses/de432s/de430/mar099s) or a full "
             "path-fragment relative to NAIF's generic_kernels/ URL.",
        autocompletion=_complete_generic_alias,
    ),
    force: bool = typer.Option(
        False, "--force", "-f",
        help="Re-download even if the local cache already has the file.",
    ),
):
    """Fetch one generic SPICE kernel by short alias.

    Generic kernels are the ones needed for *any* SPICE work — leapseconds,
    planetary constants, DE-series planetary ephemerides — and they get
    cached once under ``{storage_root}/spice_kernels/generic/``.

    The aliases the command knows about:

    \b
        lsk      naif0012.tls          (leapseconds)
        pck      pck00010.tpc          (planetary constants)
        masses   de-403-masses.tpc     (DE-403 body masses)
        de432s   de432s.bsp            (JPL DE432s ephemeris, default ~10 MB, 1950-2050)
        de430    de430.bsp             (JPL DE430 ephemeris, full range ~120 MB)
        mar099s  mar099s.bsp           (Mars satellite ephemeris)

    Pass a full path-fragment for unlisted kernels (e.g.
    ``lsk/naif0011.tls`` for an older LSK).

    \b
    Examples:
        plp spice generic lsk
        plp spice generic de430
        plp spice generic lsk/naif0011.tls    # full path for non-default
    """
    if name is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()

    from planetarypy.spice.generic_kernels import (
        GENERIC_KERNEL_ALIASES, download_generic_kernel,
    )

    try:
        path = download_generic_kernel(name, overwrite=force)
    except ValueError:
        _suggest_and_exit(
            name, sorted(GENERIC_KERNEL_ALIASES),
            "Unknown generic-kernel alias:",
        )
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)
    # stdout: the local path (pipe-safe for shell composition).
    typer.echo(path)


# ── example_pid ──────────────────────────────────────────────────────


@app.command("example_pid", rich_help_panel=_PANEL_INSPECT)
def example_pid(
    ctx: typer.Context,
    key: str = typer.Argument(
        None,
        help="Dotted index key, e.g. mro.ctx.edr",
        shell_complete=_shell_complete_index_key,
    ),
):
    """Print an example product ID from a registered PDS index.

    Useful as a seed for `plp fetch`, demo notebooks, and smoke tests.

    Examples:
        plp example_pid mro.ctx.edr
        plp example_pid cassini.iss.index
    """
    if key is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()

    from planetarypy.pds import get_example_pid

    try:
        pid = get_example_pid(key)
    except ValueError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)

    typer.echo(pid)


# ── meta ─────────────────────────────────────────────────────────────


@app.command("meta", rich_help_panel=_PANEL_INSPECT)
def meta(
    ctx: typer.Context,
    key: str = typer.Argument(
        None,
        help="Dotted index key, e.g. mro.ctx.edr",
        shell_complete=_shell_complete_index_key,
    ),
    product_id: str = typer.Argument(
        None,
        help="Product identifier",
        autocompletion=_complete_product_id,
    ),
    long: bool = typer.Option(
        False, "--long", "-l",
        help="Request the long form when the index has a custom display "
             "(no effect on indexes that always return the full row).",
    ),
):
    """Print the metadata row for a product from its PDS cumulative index.

    Thin wrapper around ``planetarypy.pds.get_meta(key, product_id, long=...)``.
    Matches the product ID against the configured (or conventional) PID
    column, tolerant of case and PDS path/extension/version-suffix
    decoration. Indexes may register custom display logic (short summaries,
    multi-row aggregation, …) which the ``--long`` flag toggles into the
    full-row form.

    Examples:
        plp meta mro.ctx.edr P02_001916_2221_XI_42N027W
        plp meta cassini.iss.index 1_N1454725799
    """
    if key is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()
    if product_id is None:
        typer.echo("Error: missing PRODUCT_ID argument.", err=True)
        raise typer.Exit(2)

    from rich.console import Console
    from rich.table import Table

    from planetarypy.pds import get_meta

    try:
        row = get_meta(key, product_id, long=long)
    except ValueError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)

    title_pid = (
        row.get("PRODUCT_ID")
        or row.get("OBSERVATION_ID")
        or row.get("FILE_NAME")
        or product_id
    )
    table = Table(
        title=f"{key} — {str(title_pid).strip()}",
        title_style="bold",
        header_style="bold magenta",
        show_lines=False,
        pad_edge=False,
    )
    table.add_column("Field", style="cyan", no_wrap=True)
    table.add_column("Value", overflow="fold")
    for col, val in row.items():
        table.add_row(str(col), "" if val is None else str(val))

    Console().print(table)


# ── constants ────────────────────────────────────────────────────────


def _body_name_set() -> list[str]:
    """All known body names in Title case, sorted. Used for completion
    and for difflib suggestions on misspelt bodies."""
    from planetarypy.constants import bodies
    return sorted({b.name.title() for b in bodies.values()})


def _complete_constants_query(
    ctx: click.Context, args: list[str], incomplete: str
) -> list[str]:
    """Tab completion for the constants subcommand.

    Matches both ``Body`` (return body names) and ``Body.field`` (after a
    dot, return that body's Constant-bearing attributes).
    """
    from planetarypy.constants import bodies

    if "." not in incomplete:
        prefix = incomplete.lower()
        return [n for n in _body_name_set() if n.lower().startswith(prefix)]

    body_part, _, field_part = incomplete.partition(".")
    body = bodies.find(body_part)
    if body is None:
        return []
    return sorted(
        f"{body_part}.{name}"
        for name, _ in body.iter_constants()
        if name.startswith(field_part)
    )


def _suggest_and_exit(target: str, choices: list[str], label: str) -> None:
    """Emit ``"<label> 'target'  did you mean: ..."`` on stderr and exit 1.

    Shared between the body-not-found and field-not-found paths of
    `plp constants` so the suggestion UX stays consistent.
    """
    import difflib

    suggestions = difflib.get_close_matches(target, choices, n=3)
    msg = f"{label} {target!r}"
    if suggestions:
        msg += f"  did you mean: {', '.join(suggestions)}?"
    typer.echo(msg, err=True)
    raise typer.Exit(1)


@app.command("constants", rich_help_panel=_PANEL_DISCOVERY)
def constants_cmd(
    ctx: typer.Context,
    query: str = typer.Argument(
        None,
        help="Body name (case-insensitive) for the full quantity table, "
             "or 'Body.field' for one value. "
             "Examples: 'Mars', 'mars.GM', 'jupiter.bond_albedo'.",
        autocompletion=_complete_constants_query,
    ),
    at: str = typer.Option(
        None, "--at", "-t",
        help="Time-travel: show the value as of this date (YYYY, YYYY-MM, "
             "or YYYY-MM-DD). Walks PCK editions + NSSDC capture history.",
    ),
):
    """Print per-body planetary constants from PCK + NSSDC sources.

    Two forms:

    \b
        plp constants Mars         # Rich table of every known constant
        plp constants Mars.GM      # Just the value (stdout) + source (stderr)

    For the single-field form, the quantity is printed to stdout and the
    source/reference goes to stderr, so output stays pipe-safe::

    \b
        plp constants Mars.GM | awk '{print $1}'

    Body name matching is case-insensitive (``mars`` == ``Mars`` ==
    ``MARS``). Unknown bodies print the closest matches as a suggestion.
    """
    import sys
    from planetarypy.constants import bodies

    # Bare invocation: typer would normally error on the missing
    # positional. Print help instead — friendlier discovery surface.
    if query is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()

    body_part, dot, field_part = query.partition(".")
    body = bodies.find(body_part)
    if body is None:
        _suggest_and_exit(body_part.title(), _body_name_set(),
                          "Unknown body:")

    if at is not None:
        body = body.at_time(at)

    # ── Field form: stdout=value, stderr=source ────────────────────────
    if dot:
        if not hasattr(body, field_part):
            _suggest_and_exit(
                field_part,
                sorted(n for n, _ in body.iter_constants()),
                f"{body.name.title()} has no field",
            )
        value = getattr(body, field_part)
        if value is None:
            typer.echo(
                f"{body.name.title()}.{field_part} is unset (None)",
                err=True,
            )
            raise typer.Exit(1)
        # stdout: bare value (pipe-safe). stderr: provenance.
        typer.echo(str(value))
        # `source` / `reference` may be empty strings (e.g. on
        # non-Constant fields like Mars.naif_id); only emit when set.
        for label, attr in (("source", "source"), ("reference", "reference")):
            if val := getattr(value, attr, ""):
                print(f"# {label}: {val}", file=sys.stderr)
        return

    # ── Body form: Rich table of every scalar Constant ─────────────────
    from rich.console import Console
    from rich.table import Table

    title = f"{body.name.title()} (NAIF {body.naif_id}, {body.body_class})"
    if at is not None:
        title += f"  — as of {at}"
    table = Table(
        title=title,
        title_style="bold",
        header_style="bold magenta",
        show_lines=False,
        pad_edge=False,
    )
    table.add_column("field", style="cyan", no_wrap=True)
    table.add_column("value", overflow="fold")
    table.add_column("source", overflow="fold", style="dim")

    for field_name, val in body.iter_constants():
        table.add_row(field_name, str(val), val.source)

    Console().print(table)


def main():
    app()
