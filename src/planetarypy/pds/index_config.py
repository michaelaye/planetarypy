"""Configuration handlers for PDS index URLs and access timestamps."""

import datetime  # Use absolute import
import json
import os
from pathlib import Path

import requests
import tomlkit
from loguru import logger  # Use loguru for logging

from ..utils import compare_remote_content
from .index_logging import access_log

index_urls_url = "https://raw.githubusercontent.com/planetarypy/planetarypy_configs/refs/heads/main/planetarypy_index_urls.toml"


def _calculate_hours_since_timestamp(timestamp: datetime.datetime) -> float:
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


def _is_older_than_hours(timestamp: datetime.datetime, hours: float) -> bool:
    """Check if a timestamp is older than the specified number of hours.

    Args:
        timestamp: The timestamp to check
        hours: Number of hours threshold

    Returns:
        True if timestamp is older than the specified hours
    """
    return _calculate_hours_since_timestamp(timestamp) > hours


class IndexURLsConfig:
    """Manage PDS index URLs configuration.

    This class handles the storage and retrieval of URLs for PDS indices,
    structured by mission and instrument.
    """

    fname = "planetarypy_index_urls.toml"
    path = Path(os.getenv("PLANETARYPY_INDEX_URLS", Path.home() / f".{fname}"))

    def __init__(self, config_path: str = None):
        """Initialize with optional custom config path."""
        if config_path is not None:
            self.path = Path(config_path)

        # Create default config if it doesn't exist
        is_new = not self.path.exists()
        if is_new:
            self._create_default_config()
            # If this is a new configuration, discover latest dynamic URLs
            self.discover_dynamic_urls()
        else:
            # Check if existing config needs updating
            self._check_and_update_config()

        # Read the config after all potential modifications
        self._read_config()

    def _create_default_config(self):
        """Create a default configuration file by downloading from the remote URL."""
        try:
            # Download the configuration from the remote URL
            logger.info(f"Downloading index URLs config from {index_urls_url}")
            response = requests.get(index_urls_url, timeout=30)
            response.raise_for_status()

            # Parse the downloaded TOML to validate it
            config_doc = tomlkit.loads(response.text)

            # Write the pristine config to the local file
            self.path.write_text(tomlkit.dumps(config_doc))
            logger.info(
                f"Created default index URLs config at {self.path} from remote source"
            )

            # Log the download event to the access log
            access_log.set_timestamp("config_download", datetime.datetime.now())

        except (
            requests.RequestException,
            requests.Timeout,
            tomlkit.exceptions.TOMLKitError,
        ) as e:
            logger.error(
                f"Could not download config from {index_urls_url}: {e}. "
                f"Please check your internet connection and try again."
            )
            raise RuntimeError(f"Failed to create default configuration: {e}") from e

    def _check_and_update_config(self):
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
                logger.info(
                    "No update timestamp found in access log, checking for updates"
                )
                self._update_config_from_remote()
                self._check_and_update_dynamic_urls()
                return

            # Get the most recent timestamp
            last_activity = max(timestamps)

            # Check if it's been more than one day (24 hours)
            if _is_older_than_hours(last_activity, 24):
                logger.info(
                    "Last config check older than one day, checking for updates"
                )
                self._update_config_from_remote()
            else:
                logger.debug(f"Config is up to date (last activity: {last_activity})")

            # Always check dynamic URLs for staleness (separate from config updates)
            self._check_and_update_dynamic_urls()

        except Exception as e:
            logger.warning(f"Error checking config age: {e}")
            # If we can't check the access log, update anyway
            self._update_config_from_remote()
            self._check_and_update_dynamic_urls()

    def _update_config_from_remote(self):
        """Update the config by downloading from the remote URL."""
        try:
            logger.info(f"Checking for config updates from {index_urls_url}")

            # Read current config content
            current_content = self.path.read_text()

            # Compare with remote content
            comparison = compare_remote_content(
                index_urls_url, current_content, timeout=30
            )

            if comparison["error"]:
                logger.warning(
                    f"Could not update config from remote: {comparison['error']}"
                )
                return

            if comparison["has_updates"]:
                logger.info("Remote config has updates, downloading new version")

                # Write the pristine remote config
                self.path.write_text(comparison["remote_content"])
                logger.info("Config updated successfully")

                # Log the update event to the access log
                access_log.set_timestamp("config_update", datetime.datetime.now())
            else:
                logger.info("Remote config is identical to local config")

                # Log that we checked for updates
                access_log.set_timestamp("config_check", datetime.datetime.now())

        except tomlkit.exceptions.TOMLKitError as e:
            logger.warning(f"Could not parse TOML content: {e}")

    def _check_and_update_dynamic_urls(self):
        """Check and update dynamic URLs that are older than 24 hours."""
        # Dictionary of dynamic indices that should be checked
        dynamic_indices = ["mro.ctx.edr", "lro.lroc.edr"]

        for key in dynamic_indices:
            try:
                # Get the last discovery timestamp for this dynamic URL
                last_timestamp = access_log.get_timestamp(key)

                if last_timestamp is None:
                    logger.info(f"No timestamp found for {key}, discovering URL")
                    self._discover_single_dynamic_url(key)
                    continue

                # Check if it's been more than 24 hours
                if _is_older_than_hours(last_timestamp, 24):
                    logger.info(
                        f"Dynamic URL for {key} was last checked over 24 h ago, checking for updates"
                    )
                    self._discover_single_dynamic_url(key)
                else:
                    logger.debug(
                        f"Dynamic URL for {key} is up to date (last checked: {last_timestamp})"
                    )

            except Exception as e:
                logger.warning(f"Error checking dynamic URL age for {key}: {e}")

    def _discover_single_dynamic_url(self, key: str, return_info: bool = False):
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
                current_url = self.get_url(key)
                is_update = current_url != str(latest_url)

                if is_update:
                    logger.info(f"Update found: New {key} index URL discovered!")
                    logger.info(f"  Previous: {current_url}")
                    logger.info(f"  New: {latest_url}")
                else:
                    logger.debug(f"No change for {key}: URL is still {latest_url}")

                # Store the URL in the config (even if unchanged to update timestamp)
                self.set_url(key, str(latest_url))

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

    def _read_config(self):
        """Read the configuration file."""
        self.tomldoc = tomlkit.loads(self.path.read_text())
        if "missions" not in self.tomldoc:
            self.tomldoc["missions"] = tomlkit.table()
            self.save()

    def save(self):
        """Write the configuration to file."""
        self.path.write_text(tomlkit.dumps(self.tomldoc))

    def get_url(self, key: str) -> str:
        """Get URL by nested key.

        Args:
            key: A dotted key format, e.g., 'cassini.iss.ring_summary'
                 (without 'missions.' prefix)

        Returns:
            The URL string or empty string if not found
        """
        if not key.startswith("missions"):
            key = "missions." + key

        parts = key.split(".")
        current = self.tomldoc

        try:
            # Navigate through the tables to the instrument level
            for i in range(len(parts) - 1):
                current = current[parts[i]]

            # Get the URL from the last component as a direct value
            index_name = parts[-1]
            if index_name in current:
                return current[index_name]
            return ""
        except (KeyError, TypeError):
            return ""

    def set_url(self, key: str, url: str):
        """Set URL for a specific index.

        Args:
            key: A dotted key format, e.g., 'cassini.iss.ring_summary'
                 (without 'missions.' prefix)
            url: The URL to set
        """
        if not key.startswith("missions"):
            key = "missions." + key

        parts = key.split(".")
        current = self.tomldoc

        # Ensure mission table exists
        mission_key = parts[1]
        if mission_key not in current["missions"]:
            # Add a section comment and the mission table
            if len(current["missions"]) > 0:
                # Add a newline before new missions if not the first
                current["missions"].add(tomlkit.nl())

            current["missions"].add(
                tomlkit.comment(f"{mission_key.capitalize()} Mission")
            )
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

        self.save()

    def discover_dynamic_urls(self):
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
                result = self._discover_single_dynamic_url(key, return_info=True)
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

    def __repr__(self):
        return json.dumps(self.tomldoc, indent=2)


# Create singleton instances
urls_config = IndexURLsConfig()
