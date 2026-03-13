"""Command-line interface for PlanetaryPy PDS index management.

Notes
-----
REVIEW

Examples
--------
>>> # REVIEW: example
"""


import click
from loguru import logger

from .index_config import discover_dynamic_urls, load_config, update_config_from_remote


@click.command()
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output showing detailed progress.",
)
@click.option(
    "--config-only",
    is_flag=True,
    help="Only update the configuration URLs, skip dynamic URL discovery.",
)
@click.option(
    "--dynamic-only",
    is_flag=True,
    help="Only update dynamic URLs, skip configuration URL check.",
)
def plp_update_indexes(verbose, config_only, dynamic_only):
    """Force update check for PDS index configuration and dynamic URL discovery.
    
    Parameters
    ----------
    verbose : REVIEW
        REVIEW
    config_only : REVIEW
        REVIEW
    dynamic_only : REVIEW
        REVIEW
    
    Returns
    -------
    REVIEW
    
    Examples
    --------
    >>> # REVIEW: add example usage"""
    if verbose:
        logger.info("Starting PlanetaryPy index update check...")

    if config_only and dynamic_only:
        click.echo(
            "Error: Cannot specify both --config-only and --dynamic-only", err=True
        )
        raise click.Abort()

    try:
        updates_performed = []

        if not dynamic_only:
            # Force config update check
            if verbose:
                click.echo("Checking configuration URLs for updates...")

            # Force the config update by calling the function directly
            update_config_from_remote()
            updates_performed.append("configuration URLs")

        if not config_only:
            # Force dynamic URL discovery
            if verbose:
                click.echo("Checking dynamic URLs for updates...")

            # Reload config from disk to ensure we have the latest version
            load_config(auto_create=False)

            # Force dynamic URL discovery
            discovery_results = discover_dynamic_urls()
            updates_performed.append("dynamic URLs")

            if verbose:
                # Show discovery results
                for key, result in discovery_results.items():
                    if "error" in result:
                        click.echo(f"  ❌ {key}: {result['error']}", err=True)
                    elif result.get("is_update"):
                        click.echo(f"  ✅ {key}: Updated to {result['url']}")
                    else:
                        click.echo(f"  ℹ️  {key}: No change - {result['url']}")

        # Summary message
        if updates_performed:
            update_list = " and ".join(updates_performed)
            click.echo(f"✅ Successfully checked {update_list} for updates.")
        else:
            click.echo("ℹ️  No updates were performed.")

    except Exception as e:
        if verbose:
            logger.exception("Error during index update check")
        click.echo(f"❌ Error updating indexes: {e}", err=True)
        raise click.Abort()


if __name__ == "__main__":
    plp_update_indexes()
