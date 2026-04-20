"""PlanetaryPy unified CLI.

Usage:
    plp fetch mro.ctx.edr P02_001916_2221_XI_42N027W
    plp hifetch PSP_003092_0985 --browse
    plp ctxqv J05_046771_1950
    plp catalog build
"""

import typer

app = typer.Typer(
    name="plp",
    help="PlanetaryPy — Python tools for planetary science data access.",
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
)


# ── fetch ────────────────────────────────────────────────────────────


@app.command()
def fetch(
    key: str = typer.Argument(help="Dotted product key, e.g. mro.ctx.edr"),
    product_id: str = typer.Argument(help="Product identifier"),
    force: bool = typer.Option(False, "--force", "-f", help="Re-download even if cached"),
    label_only: bool = typer.Option(False, "--label-only", "-l", help="Download only the label file"),
    here: bool = typer.Option(False, "--here", "-H", help="Download into current directory instead of planetarypy storage"),
):
    """Download a PDS product by ID.

    Examples:
        plp fetch mro.ctx.edr P02_001916_2221_XI_42N027W
        plp fetch --here mro.ctx.edr P02_001916_2221_XI_42N027W
    """
    from pathlib import Path

    from planetarypy.catalog._resolver import (
        resolve_product,
        download_product,
        _local_product_dir,
    )

    mission, instrument, product_key = key.split(".")

    typer.echo(f"Resolving {key} / {product_id}...")
    try:
        resolved = resolve_product(mission, instrument, product_key, product_id)
        for f in resolved.files:
            typer.echo(f"URL: {resolved.url_stem}/{f}")
        if here:
            local_dir = Path.cwd()
        else:
            local_dir = _local_product_dir(
                mission, instrument, product_key, resolved.product_id,
            )
        download_product(resolved, local_dir, label_only=label_only, force=force)
        for f in resolved.files:
            typer.echo(local_dir / f)
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


# ── HiRISE commands ─────────────────────────────────────────────────


def _complete_hirise_obsid_rdr(incomplete: str) -> list[str]:
    """Tab-completion for HiRISE obsids with RDR products (browse, RDR fetch)."""
    from planetarypy.instruments.mro.hirise import complete_obsid
    return complete_obsid(incomplete, index="rdr")


def _complete_hirise_obsid_edr(incomplete: str) -> list[str]:
    """Tab-completion for HiRISE obsids from EDR index (all observations)."""
    from planetarypy.instruments.mro.hirise import complete_obsid
    return complete_obsid(incomplete, index="edr")


@app.command()
def hibrowse(
    product_id: str = typer.Argument(
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
    from pathlib import Path
    from planetarypy.instruments.mro.hirise import get_browse, _parse_pid, _orbit_range, HIRISE_BASE

    # Show URL immediately so user knows we're waiting on the server
    pid, parts, data_level = _parse_pid(product_id)
    obs_id = f"{parts[0]}_{parts[1]}_{parts[2]}"
    orbit = int(parts[1])
    suffix = "abrowse.jpg" if annotated else "browse.jpg"
    typer.echo(f"Fetching {HIRISE_BASE}/EXTRAS/{data_level}/{parts[0]}/{_orbit_range(orbit)}/{obs_id}/{pid}.{suffix}")

    try:
        dest = Path.cwd() if here else None
        outpath = get_browse(product_id, annotated=annotated, dest=dest, force=force)
        typer.echo(f"Browse: {outpath}")
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)

    import platform
    if platform.system() == "Darwin":
        import subprocess
        subprocess.Popen(["open", str(outpath)])


@app.command()
def hiedr(
    obsid: str = typer.Argument(help="HiRISE observation ID, e.g. PSP_003092_0985",
                                autocompletion=_complete_hirise_obsid_edr),
    red: bool = typer.Option(False, "--red", help="Download RED CCDs (RED0–RED9, 20 files)"),
    ir: bool = typer.Option(False, "--ir", help="Download IR CCDs (IR10–IR11, 4 files)"),
    bg: bool = typer.Option(False, "--bg", help="Download BG CCDs (BG12–BG13, 4 files)"),
    ccds: str = typer.Option(None, "--ccds", help="Specific CCD numbers, e.g. '4,5' for RED4+RED5 only"),
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

    ccd_nums = [int(n) for n in ccds.split(",")] if ccds else None
    saveroot = Path.cwd() if here else None

    products = edr_products(obsid, colors=colors, ccds=ccd_nums, saveroot=saveroot)
    typer.echo(f"{obsid}: {len(products)} EDR files from {products[0].url.parent}")

    try:
        download_edr(obsid, colors=colors, ccds=ccd_nums, saveroot=saveroot, overwrite=force)
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)

    typer.echo(f"Stored in: {products[0].local_path.parent}")


@app.command()
def hifetch(
    product_id: str = typer.Argument(
        help="HiRISE product ID, e.g. PSP_003092_0985_RED or PSP_003092_0985_RED4_0",
        autocompletion=_complete_hirise_obsid_rdr,
    ),
    here: bool = typer.Option(False, "--here", "-H", help="Download into current directory"),
    force: bool = typer.Option(False, "--force", "-f", help="Re-download even if cached"),
):
    """Download a full HiRISE data product (EDR .IMG or RDR .JP2).

    Examples:
        plp hifetch PSP_003092_0985_RED           (RDR JP2)
        plp hifetch PSP_003092_0985_COLOR         (RDR JP2)
        plp hifetch PSP_003092_0985_RED4_0        (EDR IMG)
    """
    from pathlib import Path
    from planetarypy.instruments.mro.hirise import _parse_pid

    try:
        pid, parts, data_level = _parse_pid(product_id)
    except ValueError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1)

    product_key = data_level.lower()

    from planetarypy.catalog._resolver import (
        resolve_product, download_product, _local_product_dir,
    )

    typer.echo(f"Resolving mro.hirise.{product_key} / {pid}...")
    try:
        resolved = resolve_product("mro", "hirise", product_key, pid)
        for f in resolved.files:
            typer.echo(f"URL: {resolved.url_stem}/{f}")
        if here:
            local_dir = Path.cwd()
        else:
            local_dir = _local_product_dir(
                "mro", "hirise", product_key, resolved.product_id,
            )
        download_product(resolved, local_dir, force=force)
        for f in resolved.files:
            typer.echo(local_dir / f)
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@app.command()
def himos(
    obsid: str = typer.Argument(
        help="HiRISE observation ID, e.g. PSP_003092_0985",
        autocompletion=_complete_hirise_obsid_edr,
    ),
    red: bool = typer.Option(False, "--red", help="Process RED CCDs"),
    ir: bool = typer.Option(False, "--ir", help="Process IR CCDs"),
    bg: bool = typer.Option(False, "--bg", help="Process BG CCDs"),
    ccds: str = typer.Option(None, "--ccds", help="Specific CCD numbers, e.g. '4,5'"),
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
    from planetarypy.instruments.mro.hirise import create_mosaic

    if not red and not ir and not bg:
        red = True

    colors = []
    if red:
        colors.append("red")
    if ir:
        colors.append("ir")
    if bg:
        colors.append("bg")

    ccd_nums = [int(n) for n in ccds.split(",")] if ccds else None

    for color in colors:
        typer.echo(f"\n{'='*60}")
        typer.echo(f"Processing {obsid} — {color.upper()} mosaic")
        typer.echo(f"{'='*60}")
        try:
            path = create_mosaic(
                obsid,
                color=color,
                ccds=ccd_nums if color == "red" else None,
                mapfile=mapfile,
                overwrite=overwrite,
            )
            typer.echo(f"Mosaic: {path}")
        except Exception as e:
            typer.echo(f"Error processing {color.upper()}: {e}", err=True)
            raise typer.Exit(1)


# ── ctxqv ────────────────────────────────────────────────────────────


@app.command()
def ctxqv(
    imgid: str = typer.Argument(help="CTX product ID (short or full), e.g. J05_046771_1950"),
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
    import matplotlib.pyplot as plt
    import numpy as np

    arr = None
    full_arr = None
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

catalog_app = typer.Typer(help="PDS catalog management.")
app.add_typer(catalog_app, name="catalog")


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



# ── spicer ───────────────────────────────────────────────────────────


@app.command()
def spicer(
    body: str = typer.Argument(help="NAIF body name, e.g. Mars, Moon, Enceladus"),
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


def main():
    app()
