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
        if here:
            local_dir = Path.cwd()
        else:
            local_dir = _local_product_dir(
                mission, instrument, product_key, resolved.product_id,
            )
        download_product(resolved, local_dir, label_only=label_only, force=force)
        typer.echo(f"Downloaded to: {local_dir}")
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


# ── hifetch ──────────────────────────────────────────────────────────


@app.command()
def hibrowse(
    product_id: str = typer.Argument(
        help="HiRISE product ID, e.g. PSP_003092_0985_RED or PSP_004238_1135_RED1_1"
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
    from planetarypy.instruments.mro.hirise import get_browse

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
def hifetch(
    product_id: str = typer.Argument(
        help="HiRISE product ID, e.g. PSP_003092_0985_RED or PSP_003092_0985_RED4_0"
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
    if len(parts) == 4 and product_key == "rdr" and parts[3] not in ("RED", "COLOR"):
        typer.echo(
            "Bare observation ID — specify a suffix:\n"
            f"  plp hifetch {pid}_RED     (RDR)\n"
            f"  plp hifetch {pid}_COLOR   (RDR)\n"
            f"  plp hifetch {pid}_RED4_0  (EDR CCD)",
            err=True,
        )
        raise typer.Exit(1)
    else:
        product_key = "edr"

    from planetarypy.catalog._resolver import (
        resolve_product, download_product, _local_product_dir,
    )

    typer.echo(f"Resolving mro.hirise.{product_key} / {pid}...")
    try:
        resolved = resolve_product("mro", "hirise", product_key, pid)
        if here:
            local_dir = Path.cwd()
        else:
            local_dir = _local_product_dir(
                "mro", "hirise", product_key, resolved.product_id,
            )
        download_product(resolved, local_dir, force=force)
        typer.echo(f"Downloaded to: {local_dir}")
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


# ── ctxqv ────────────────────────────────────────────────────────────


@app.command()
def ctxqv(
    imgid: str = typer.Argument(help="CTX product ID (short or full), e.g. J05_046771_1950"),
    stride: int = typer.Option(10, "--stride", "-s", help="Downsample factor"),
    save: str = typer.Option(None, "--save", "-o", help="Save to PNG instead of displaying"),
    stretch: str = typer.Option("1,99", "--stretch", "-p", help="Percentile stretch as 'low,high'. Use 'none' to disable."),
    edr: bool = typer.Option(False, "--edr", help="Force raw EDR, skip calibrated files"),
):
    """Show a downsampled quickview of a CTX image.

    Automatically uses the best available level:
    map-projected > calibrated > cube > raw EDR.
    """
    import matplotlib.pyplot as plt

    arr = None
    level = None
    pid = imgid

    if not edr:
        try:
            from planetarypy.instruments.mro.ctx.ctx_calib import Calib

            typer.echo(f"Checking for calibrated products for {imgid}...")
            calib = Calib(imgid)
            pid = calib.edr.pid
            arr, path, level = calib.quickview(stride=stride)
            if arr is not None:
                typer.echo(f"Using {level} product: {path.name}")
        except Exception:
            pass

    if arr is None:
        from planetarypy.instruments.mro.ctx.ctx_edr import EDR

        typer.echo(f"Using raw EDR for {imgid} (stride={stride})...")
        edr_obj = EDR(imgid)
        pid = edr_obj.pid
        arr = edr_obj.quickview(stride=stride)
        level = "edr"

    typer.echo(f"Image: {arr.shape[1]}x{arr.shape[0]} px [{level}]")

    from planetarypy.plotting import imshow_gray

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
