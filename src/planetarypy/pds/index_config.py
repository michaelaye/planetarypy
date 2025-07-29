"""Configuration handlers for PDS index URLs and access timestamps."""

import datetime  # Use absolute import
import json
import os
import shutil
from importlib.resources import files
from pathlib import Path
from typing import Optional, Union

import requests
import tomlkit
from dateutil import parser
from dateutil.parser import ParserError
from loguru import logger  # Use loguru for logging

from ..utils import compare_remote_content

index_urls_url = "https://raw.githubusercontent.com/planetarypy/planetarypy_configs/refs/heads/main/planetarypy_index_urls.toml"

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
        else:
            # Check if existing config needs updating
            self._check_and_update_config()

        self._read_config()

        # If this is a new configuration, discover latest dynamic URLs
        if is_new:
            self.discover_dynamic_urls()

    def _create_default_config(self):
        """Create a default configuration file by downloading from the remote URL."""
        try:
            # Try to download the configuration from the remote URL
            logger.info(f"Downloading index URLs config from {index_urls_url}")
            response = requests.get(index_urls_url, timeout=30)
            response.raise_for_status()

            # Parse the downloaded TOML to validate it
            config_doc = tomlkit.loads(response.text)

            # Add metadata about when this was downloaded
            if "metadata" not in config_doc:
                config_doc["metadata"] = tomlkit.table()
            config_doc["metadata"]["last_updated"] = datetime.datetime.now().isoformat()
            config_doc["metadata"]["source_url"] = index_urls_url

            # Write the config with metadata to the local file
            self.path.write_text(tomlkit.dumps(config_doc))
            logger.info(
                f"Created default index URLs config at {self.path} from remote source"
            )

        except (
            requests.RequestException,
            requests.Timeout,
            tomlkit.exceptions.TOMLKitError,
        ) as e:
            # If download fails, try to copy the template file from the package's data directory
            logger.warning(
                f"Could not download config from {index_urls_url}: {e}. Trying local template."
            )
            try:
                template_path = files("planetarypy.data").joinpath(self.fname)
                shutil.copy(template_path, self.path)

                # Add metadata to the copied template
                try:
                    template_doc = tomlkit.loads(self.path.read_text())
                    if "metadata" not in template_doc:
                        template_doc["metadata"] = tomlkit.table()
                    template_doc["metadata"]["last_updated"] = (
                        datetime.datetime.now().isoformat()
                    )
                    template_doc["metadata"]["source"] = "local_template"
                    self.path.write_text(tomlkit.dumps(template_doc))
                except Exception:
                    pass  # If we can't add metadata to template, that's okay

                logger.info(
                    f"Created default index URLs config at {self.path} from local template"
                )
            except (FileNotFoundError, ImportError, shutil.Error) as template_error:
                # If the template file doesn't exist or can't be copied, create a basic structure
                logger.warning(
                    f"Could not copy template file: {template_error}. Creating basic structure."
                )
                doc = tomlkit.document()

                # Add a comment at the top
                doc.add(tomlkit.comment("PlanetaryPy Index URLs Configuration"))
                doc.add(tomlkit.nl())
                doc.add(
                    tomlkit.comment(
                        "This file contains URLs for PDS indices, organized by mission and instrument"
                    )
                )
                doc.add(tomlkit.nl())
                doc.add(tomlkit.nl())

                # Add metadata
                metadata_table = tomlkit.table()
                metadata_table["last_updated"] = datetime.datetime.now().isoformat()
                metadata_table["source"] = "basic_structure"
                doc["metadata"] = metadata_table
                doc.add(tomlkit.nl())

                # Add missions table
                missions_table = tomlkit.table()
                doc["missions"] = missions_table

                # Write the document
                self.path.write_text(tomlkit.dumps(doc))
                logger.info(f"Created basic index URLs config at {self.path}")

    def _check_and_update_config(self):
        """Check if the config is older than one day and update if needed."""
        try:
            # Read the current config to check metadata
            current_doc = tomlkit.loads(self.path.read_text())

            # Check if metadata exists and has last_updated timestamp
            if (
                "metadata" not in current_doc
                or "last_updated" not in current_doc["metadata"]
            ):
                logger.info("No update timestamp found in config, checking for updates")
                self._update_config_from_remote()
                return

            # Parse the last updated timestamp
            try:
                last_updated = parser.parse(current_doc["metadata"]["last_updated"])
                now = datetime.datetime.now()

                # If timezone-naive, assume UTC
                if last_updated.tzinfo is None:
                    last_updated = last_updated.replace(tzinfo=datetime.timezone.utc)
                if now.tzinfo is None:
                    now = now.replace(tzinfo=datetime.timezone.utc)

                # Check if it's been more than one day
                time_diff = now - last_updated
                if time_diff.total_seconds() > 24 * 60 * 60:  # 24 hours in seconds
                    logger.info(
                        f"Config is {time_diff.days} days old, checking for updates"
                    )
                    self._update_config_from_remote()
                else:
                    logger.debug(f"Config is up to date (last updated: {last_updated})")

            except (ParserError, ValueError) as e:
                logger.warning(
                    f"Could not parse last_updated timestamp: {e}, checking for updates"
                )
                self._update_config_from_remote()

        except Exception as e:
            logger.warning(f"Error checking config age: {e}")

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

                # Parse the remote config and add metadata
                remote_doc = tomlkit.loads(comparison["remote_content"])

                # Add metadata about when this was updated
                if "metadata" not in remote_doc:
                    remote_doc["metadata"] = tomlkit.table()
                remote_doc["metadata"]["last_updated"] = (
                    datetime.datetime.now().isoformat()
                )
                remote_doc["metadata"]["source_url"] = index_urls_url

                # Write the updated config
                self.path.write_text(tomlkit.dumps(remote_doc))
                logger.info("Config updated successfully")
            else:
                logger.info("Remote config is identical to local config")

                # Update just the timestamp to avoid checking again for another day
                current_doc = tomlkit.loads(current_content)
                if "metadata" not in current_doc:
                    current_doc["metadata"] = tomlkit.table()
                current_doc["metadata"]["last_updated"] = (
                    datetime.datetime.now().isoformat()
                )
                self.path.write_text(tomlkit.dumps(current_doc))

        except tomlkit.exceptions.TOMLKitError as e:
            logger.warning(f"Could not parse TOML content: {e}")

    def _read_config(self):
        """Read the configuration file."""
        self.tomldoc = tomlkit.loads(self.path.read_text())
        if "missions" not in self.tomldoc:
            self.tomldoc["missions"] = tomlkit.table()
            self.save()
        if "metadata" not in self.tomldoc:
            self.tomldoc["metadata"] = tomlkit.table()
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
        updates_found = {}
        try:
            # Import here to avoid circular imports
            from .ctx_index import CTXIndex
            from .lroc_index import LROCIndex

            # Dictionary of dynamic index classes
            dynamic_indices = {"mro.ctx.edr": CTXIndex, "lro.lroc.edr": LROCIndex}

            # For each dynamic index, get and store its latest URL
            for key, index_class in dynamic_indices.items():
                try:
                    # Create instance and get latest URL
                    index_instance = index_class()
                    latest_url = index_instance.latest_index_label_url

                    if latest_url:
                        # Check if this URL is different from what we already have
                        current_url = self.get_url(key)
                        is_update = current_url != str(latest_url)

                        # Store information about this update
                        updates_found[key] = {
                            "url": str(latest_url),
                            "is_update": is_update,
                            "previous_url": current_url if is_update else None,
                        }

                        # Print user-friendly notification if it's a new URL
                        if is_update:
                            print(f"Update available: New {key} index found!")
                            print(f"  Previous: {current_url}")
                            print(f"  New: {latest_url}")

                        # Store the URL in the config
                        self.set_url(key, str(latest_url))

                        # Also store metadata about when this URL was discovered
                        comment = f"Latest URL discovered on {datetime.datetime.now().strftime('%Y-%m-%d')}"
                        parts = key.split(".")
                        if len(parts) >= 3:
                            mission, instrument, index = parts
                            current = self.tomldoc["missions"][mission][instrument]

                            # Try to add comment before the index entry
                            if hasattr(current, "add") and callable(current.add):
                                # Check if we need to add a comment
                                try:
                                    if not str(current).strip().endswith(comment):
                                        current.add(tomlkit.nl())
                                        current.add(tomlkit.comment(comment))
                                        self.save()
                                except Exception as e:
                                    logger.debug(f"Couldn't add comment for {key}: {e}")

                        logger.info(f"Discovered latest URL for {key}: {latest_url}")
                except Exception as e:
                    logger.warning(f"Failed to discover latest URL for {key}: {e}")
                    updates_found[key] = {"error": str(e)}

        except Exception as e:
            logger.warning(f"Error discovering dynamic URLs: {e}")

        return updates_found

    def __repr__(self):
        return json.dumps(self.tomldoc, indent=2)


class IndexAccessLog:
    """Manage access timestamps for PDS indices.

    This class handles storing and retrieving timestamps of when PDS indices
    were last accessed or updated.
    """

    fname = "planetarypy_index.log"
    path = Path(os.getenv("PLANETARYPY_INDEX_LOG", Path.home() / f".{fname}"))

    def __init__(self, log_path: str = None):
        """Initialize with optional custom log path."""
        if log_path is not None:
            self.path = Path(log_path)

        # Create empty log file if it doesn't exist
        if not self.path.exists():
            self._create_empty_log()

        self._read_log()

    def _create_empty_log(self):
        """Create an empty log file."""
        self.path.write_text(json.dumps({}))

    def _read_log(self):
        """Read the log file into memory."""
        try:
            self.log_data = json.loads(self.path.read_text())
        except json.JSONDecodeError:
            logger.warning(f"Could not parse log file {self.path}. Creating a new one.")
            self.log_data = {}
            self.save()

    def save(self):
        """Write log data to file."""
        self.path.write_text(json.dumps(self.log_data, indent=2))

    def get_timestamp(self, key: str) -> Optional[datetime.datetime]:
        """Get timestamp for a specific index.

        Args:
            key: A dotted key format, e.g., 'cassini.iss.ring_summary'

        Returns:
            Datetime object or None if no timestamp exists
        """
        if key in self.log_data:
            try:
                return parser.parse(self.log_data[key])
            except ParserError:
                return None
        return None

    def set_timestamp(self, key: str, timestamp: Union[datetime.datetime, str]):
        """Set timestamp for a specific index.

        Args:
            key: A dotted key format, e.g., 'cassini.iss.ring_summary'
            timestamp: Datetime object or ISO format string
        """
        if isinstance(timestamp, datetime.datetime):
            timestamp_str = timestamp.isoformat()
        else:
            timestamp_str = timestamp

        self.log_data[key] = timestamp_str
        self.save()

    def __repr__(self):
        return json.dumps(self.log_data, indent=2)


# Create singleton instances
urls_config = IndexURLsConfig()
access_log = IndexAccessLog()
