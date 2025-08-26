"""Configuration handlers for PDS index URLs and access timestamps."""

import datetime  # Use absolute import
import json
import os
from pathlib import Path

import pooch
import tomlkit
from loguru import logger  # Use loguru for logging
from yarl import URL

from ..utils import compare_remote_content
from .index_logging import access_log

fname = "planetarypy_index_urls.toml"
base_url = URL(
    "https://raw.githubusercontent.com/planetarypy/planetarypy_configs/refs/heads/main/"
)
index_urls_url = base_url / fname
hash = "d9998ba2da3fa22ae37343433a1d54790086c059b8af7960b1e548bf2072e37b"

# Module-level state
_config_path = None
_config_doc = None


def calculate_hours_since_timestamp(timestamp: datetime.datetime) -> float:
    """Calculate hours elapsed since a given timestamp.

    Args:
        timestamp: The timestamp to compare against current time

    Returns:
        Hours elapsed as a float
    """
    now = datetime.datetime.now()

    # Ensure both timestamps have timezone info for accurate comparison
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=datetime.timezone.utc)

    time_diff = now - timestamp
    return time_diff.total_seconds() / 3600


def is_older_than_hours(timestamp: datetime.datetime, hours: float) -> bool:
    """Check if a timestamp is older than the specified number of hours.

    Args:
        timestamp: The timestamp to check
        hours: Number of hours threshold

    Returns:
        True if timestamp is older than the specified hours
    """
    return calculate_hours_since_timestamp(timestamp) > hours


def get_config_path(custom_path: str = None) -> Path:
    """Get the path to the configuration file.

    Args:
        custom_path: Optional custom path to config file

    Returns:
        Path to the configuration file
    """
    if custom_path:
        return Path(custom_path)

    return Path(os.getenv("PLANETARYPY_INDEX_URLS", Path.home() / f".{fname}"))


def create_default_config(config_path: Path):
    """Create a default configuration file by downloading from the remote URL."""
    try:
        # Use pooch.retrieve to download the configuration with hash verification
        logger.info(f"Downloading index URLs config from {index_urls_url}")

        # Download and verify the file using pooch.retrieve
        downloaded_file = pooch.retrieve(
            url=str(index_urls_url),
            known_hash=hash,
            path=pooch.os_cache("planetarypy"),
            fname=fname,
        )

        # Read the downloaded content
        with open(downloaded_file, "r") as f:
            remote_content = f.read()

    except Exception as e:
        logger.error(
            f"Could not download config from {index_urls_url}: {e}. "
            f"Please check your internet connection and try again."
        )
        raise RuntimeError(f"Failed to download configuration: {e}") from e

    try:
        # Parse the downloaded TOML to validate it
        config_doc = tomlkit.loads(remote_content)

        # Write the pristine config to the local file
        config_path.write_text(remote_content)
        logger.info(
            f"Created default index URLs config at {config_path} from remote source"
        )

        # Log the download event to the access log
        access_log.set_timestamp("config_download", datetime.datetime.now())

    except tomlkit.exceptions.TOMLKitError as e:
        logger.error(
            f"Downloaded config from {index_urls_url} contains invalid TOML: {e}. "
            f"This indicates a problem with the remote configuration file."
        )
        raise RuntimeError(f"Failed to parse downloaded configuration: {e}") from e
    # update dynamic URLs
    discover_dynamic_urls()


def load_config(config_path: str = None, auto_create: bool = True) -> dict:
    """Load configuration from file.

    Args:
        config_path: Optional custom path to config file
        auto_create: Whether to auto-create config if it doesn't exist

    Returns:
        Dictionary containing the configuration
    """
    global _config_path, _config_doc

    _config_path = get_config_path(config_path)

    # Create default config if it doesn't exist
    if not _config_path.exists():
        if not auto_create:
            raise FileNotFoundError(f"Config file not found: {_config_path}")
        logger.info("Config file doesn't exist, creating default configuration")
        create_default_config(_config_path)
    # Load config
    logger.info(f"Reading config from: {_config_path}")
    content = _config_path.read_text()
    _config_doc = tomlkit.loads(content)
    return _config_doc


def save_config():
    """Save configuration to file."""
    global _config_path, _config_doc

    if _config_path is None or _config_doc is None:
        raise RuntimeError("No config loaded. Call load_config() first.")

    content = tomlkit.dumps(_config_doc)
    _config_path.write_text(content)


def get_url(key: str) -> str:
    """Get URL by nested key.

    Args:
        key: A dotted key format, e.g., 'cassini.iss.ring_summary'
            (without 'missions.' prefix)

    Returns:
        The URL string or empty string if not found
    """
    global _config_doc

    if _config_doc is None:
        load_config()

    if not key.startswith("missions"):
        key = "missions." + key

    parts = key.split(".")
    current = _config_doc

    try:
        # Navigate through the tables to the instrument level
        for i in range(len(parts) - 1):
            current = current[parts[i]]

        # Get the URL from the last component as a direct value
        index_name = parts[-1]
        if index_name in current:
            return current[index_name]
        return ""
    except (KeyError, TypeError) as e:
        logger.error(f"get_url({key}) had exceptions:{e}")
        return ""


def set_url(key: str, url: str):
    """Set URL for a specific index.

    Args:
        key: A dotted key format, e.g., 'cassini.iss.ring_summary'
            (without 'missions.' prefix)
        url: The URL to set
    """
    global _config_doc

    if _config_doc is None:
        load_config()

    if not key.startswith("missions"):
        key = "missions." + key

    parts = key.split(".")
    current = _config_doc

    # Ensure mission table exists
    mission_key = parts[1]
    if mission_key not in current["missions"]:
        # Add a section comment and the mission table
        if len(current["missions"]) > 0:
            # Add a newline before new missions if not the first
            current["missions"].add(tomlkit.nl())

        current["missions"].add(tomlkit.comment(f"{mission_key.capitalize()} Mission"))
        current["missions"].add(tomlkit.nl())
        current["missions"][mission_key] = tomlkit.table()

    # Ensure instrument table exists
    if len(parts) > 2:
        instrument_key = parts[2]
        if instrument_key not in current["missions"][mission_key]:
            # Add a section comment and the instrument table
            current["missions"][mission_key].add(
                tomlkit.comment(f"{instrument_key.upper()} Instrument")
            )
            current["missions"][mission_key].add(tomlkit.nl())
            current["missions"][mission_key][instrument_key] = tomlkit.table()

        # Set the URL value directly as the value of the index key
        if len(parts) > 3:
            index_key = parts[3]
            current = current["missions"][mission_key][instrument_key]
            current[index_key] = url

    save_config()


def check_and_update_config():
    """Check if the config is older than one day and update if needed."""
    try:
        # Check the last download/update timestamp from the access log
        last_download = access_log.get_timestamp("config_download")
        last_update = access_log.get_timestamp("config_update")
        last_check = access_log.get_timestamp("config_check")

        # Find the most recent timestamp
        timestamps = [
            t for t in [last_download, last_update, last_check] if t is not None
        ]

        if not timestamps:
            logger.info("No update timestamp found in access log, checking for updates")
            update_config_from_remote()
            check_and_update_dynamic_urls()
            return

        # Get the most recent timestamp
        last_activity = max(timestamps)

        # Check if it's been more than one day (24 hours)
        if is_older_than_hours(last_activity, 24):
            logger.info("Last config check older than one day, checking for updates")
            update_config_from_remote()
        else:
            logger.debug(f"Config is up to date (last activity: {last_activity})")

        # Always check dynamic URLs for staleness (separate from config updates)
        check_and_update_dynamic_urls()

    except Exception as e:
        logger.warning(f"Error checking config age: {e}")
        # If we can't check the access log, update anyway
        update_config_from_remote()
        check_and_update_dynamic_urls()


def update_config_from_remote():
    """Update the config by downloading from the remote URL."""
    global _config_path, _config_doc

    if _config_path is None:
        load_config()

    try:
        logger.info(f"Checking for config updates from {index_urls_url}")

        # Read current config content
        current_content = _config_path.read_text()

        # Compare with remote content
        comparison = compare_remote_content(index_urls_url, current_content, timeout=30)

        if comparison["error"]:
            logger.warning(
                f"Could not update config from remote: {comparison['error']}"
            )
            return

        if comparison["has_updates"]:
            logger.info("Remote config has updates, downloading new version")

            # Write the pristine remote config (no preservation of dynamic URLs)
            _config_path.write_text(comparison["remote_content"])
            logger.info("Config updated successfully from remote")

            # After updating from remote, reload the config and discover dynamic URLs
            load_config(auto_create=False)
            check_and_update_dynamic_urls()

            # Log the update event to the access log
            access_log.set_timestamp("config_update", datetime.datetime.now())
        else:
            logger.info("Remote config is identical to local config")

            # Log that we checked for updates
            access_log.set_timestamp("config_check", datetime.datetime.now())

    except tomlkit.exceptions.TOMLKitError as e:
        logger.warning(f"Could not parse TOML content: {e}")


def check_and_update_dynamic_urls():
    """Check and update dynamic URLs that are older than 24 hours."""
    # Dictionary of dynamic indices that should be checked
    dynamic_indices = ["mro.ctx.edr", "lro.lroc.edr"]

    for key in dynamic_indices:
        try:
            # Get the last discovery timestamp for this dynamic URL
            last_timestamp = access_log.get_timestamp(key)

            if last_timestamp is None:
                logger.info(f"No timestamp found for {key}, discovering URL")
                discover_single_dynamic_url(key)
                continue

            # Check if it's been more than 24 hours
            if is_older_than_hours(last_timestamp, 24):
                logger.info(
                    f"Dynamic URL for {key} was last checked over 24 h ago, checking for updates"
                )
                discover_single_dynamic_url(key)
            else:
                logger.debug(
                    f"Dynamic URL for {key} is up to date (last checked: {last_timestamp})"
                )

        except Exception as e:
            logger.warning(f"Error checking dynamic URL age for {key}: {e}")


def discover_single_dynamic_url(key: str, return_info: bool = False):
    """Discover and update a single dynamic URL.

    Args:
        key: The dotted key format for the dynamic index (e.g., 'mro.ctx.edr')
        return_info: If True, return dictionary with discovery information

    Returns:
        dict or None: Discovery information if return_info=True, otherwise None
    """
    try:
        # Import here to avoid circular imports
        from .ctx_index import CTXIndex
        from .lroc_index import LROCIndex

        # Map of dynamic index keys to their classes
        index_classes = {"mro.ctx.edr": CTXIndex, "lro.lroc.edr": LROCIndex}

        if key not in index_classes:
            logger.warning(f"Unknown dynamic index key: {key}")
            return None if return_info else None

        # Create instance and get latest URL
        index_class = index_classes[key]
        index_instance = index_class()
        latest_url = index_instance.latest_index_label_url

        if latest_url:
            # Check if this URL is different from what we already have
            current_url = get_url(key)
            is_update = current_url != str(latest_url)

            if is_update:
                logger.info(f"Update found: New {key} index URL discovered!")
                logger.info(f"  Previous: {current_url}")
                logger.info(f"  New: {latest_url}")
            else:
                logger.debug(f"No change for {key}: URL is still {latest_url}")

            # Store the URL in the config (even if unchanged to update timestamp)
            set_url(key, str(latest_url))

            # Log the discovery in the access log
            access_log.log_url_discovery(
                key,
                str(latest_url),
                is_update=is_update,
                previous_url=current_url if is_update else None,
            )

            # Return info if requested
            if return_info:
                return {
                    "url": str(latest_url),
                    "is_update": is_update,
                    "previous_url": current_url if is_update else None,
                }
        else:
            logger.warning(f"Could not determine latest URL for {key}")
            if return_info:
                return {"error": f"Could not determine latest URL for {key}"}

    except Exception as e:
        logger.warning(f"Failed to discover latest URL for {key}: {e}")
        if return_info:
            return {"error": str(e)}

    return None


def discover_dynamic_urls():
    """Discover and store the latest dynamic URLs for indices that are determined at runtime.

    This method fetches the latest URLs for indices like CTX and LROC that are determined
    dynamically, and stores them in the configuration file for reference.

    Returns:
        dict: A dictionary of discovered URLs with information about updates
    """
    # Dictionary of dynamic indices that should be discovered
    dynamic_indices = ["mro.ctx.edr", "lro.lroc.edr"]
    updates_found = {}

    for key in dynamic_indices:
        try:
            result = discover_single_dynamic_url(key, return_info=True)
            if result:
                updates_found[key] = result

                # Print user-friendly notification if it's a new URL
                if result.get("is_update"):
                    print(f"Update available: New {key} index found!")
                    print(f"  Previous: {result.get('previous_url')}")
                    print(f"  New: {result.get('url')}")

        except Exception as e:
            logger.warning(f"Failed to discover latest URL for {key}: {e}")
            updates_found[key] = {"error": str(e)}

    return updates_found


def get_config_as_json() -> str:
    """Get the configuration as a JSON string for debugging."""
    global _config_doc

    if _config_doc is None:
        load_config()

    return json.dumps(_config_doc, indent=2)
