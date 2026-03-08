"""CLI entry points for the catalog module."""

import click


@click.command()
@click.option("--force", is_flag=True, help="Force rebuild from scratch")
@click.option("--validate-urls", is_flag=True, help="Run URL validation after build")
def plp_build_catalog(force, validate_urls):
    """Build the PDS catalog database from pdr-tests definitions."""
    from planetarypy.catalog import build_catalog

    click.echo("Building PDS catalog...")
    stats = build_catalog(force=force)
    click.echo(
        f"Done: {stats.get('instruments', '?')} instruments, "
        f"{stats.get('product_types', '?')} product types, "
        f"{stats.get('products', '?')} products"
    )

    if stats.get("ambiguous"):
        click.echo(f"\nAmbiguous mappings ({len(stats['ambiguous'])}): {', '.join(stats['ambiguous'])}")

    if validate_urls:
        from planetarypy.catalog._validation import validate_urls as do_validate
        from planetarypy.config import config

        click.echo("\nValidating URLs...")
        counts = do_validate(config.storage_root)
        click.echo(f"URL validation: {counts}")
