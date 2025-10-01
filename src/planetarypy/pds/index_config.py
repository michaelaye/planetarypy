import datetime
import os
from pathlib import Path

import pooch
import tomlkit
from loguru import logger
from yarl import URL

from ..utils import compare_remote_content
from .index_logging import access_log


# --- Legacy API module-level functions ---
def get_url(key):
    return IndexConfigManager().get_url(key)


def set_url(key, url):
    return IndexConfigManager().set_url(key, url)


def load_config(config_path=None, auto_create=True):
    return IndexConfigManager(config_path, auto_create).config_doc


def save_config():
    # This is a placeholder; actual save logic may need to be stateful
    pass


"""Configuration handlers for PDS index URLs and access timestamps."""


fname = "planetarypy_index_urls.toml"
base_url = URL(
    "https://raw.githubusercontent.com/planetarypy/planetarypy_configs/refs/heads/main/"
)
index_urls_url = base_url / fname
hash = None  # Set to None to always get latest version


class IndexConfigManager:
    def __init__(self, config_path: str = None, auto_create: bool = True):
        self.config_path = self.get_config_path(config_path)
        self.config_doc = None
        self.load_config(auto_create=auto_create)

    def get_config_path(self, custom_path: str = None) -> Path:
        if custom_path:
            return Path(custom_path)
        return Path(os.getenv("PLANETARYPY_INDEX_URLS", Path.home() / f".{fname}"))

    def create_default_config(self):
        try:
            logger.info(f"Downloading index URLs config from {index_urls_url}")
            downloaded_file = pooch.retrieve(
                url=str(index_urls_url),
                known_hash=hash,
                path=pooch.os_cache("planetarypy"),
                fname=fname,
            )
            with open(downloaded_file, "r") as f:
                remote_content = f.read()
        except Exception as e:
            logger.error(
                f"Could not download config from {index_urls_url}: {e}. "
                f"Please check your internet connection and try again."
            )
            raise RuntimeError(f"Failed to download configuration: {e}") from e
        try:
            _ = tomlkit.loads(remote_content)
            self.config_path.write_text(remote_content)
            logger.info(
                f"Created default index URLs config at {self.config_path} from remote source"
            )
            access_log.set_timestamp("config_download", datetime.datetime.now())
        except tomlkit.exceptions.TOMLKitError as e:
            logger.error(
                f"Downloaded config from {index_urls_url} contains invalid TOML: {e}. "
                f"This indicates a problem with the remote configuration file."
            )
            raise RuntimeError(f"Failed to parse downloaded configuration: {e}") from e

    def load_config(self, auto_create: bool = True) -> dict:
        if not self.config_path.exists():
            if not auto_create:
                raise FileNotFoundError(f"Config file not found: {self.config_path}")
            logger.info("Config file doesn't exist, creating default configuration")
            self.create_default_config()
        logger.info(f"Reading config from: {self.config_path}")
        content = self.config_path.read_text()
        self.config_doc = tomlkit.loads(content)
        return self.config_doc

    def save_config(self):
        if self.config_path is None or self.config_doc is None:
            raise RuntimeError("No config loaded. Call load_config() first.")
        content = tomlkit.dumps(self.config_doc)
        self.config_path.write_text(content)

    def get_url(self, key: str) -> str:
        if self.config_doc is None:
            self.load_config()
        try:
            # First try to get as a flat key (legacy support)
            if key in self.config_doc:
                return self.config_doc.get(key, "")

            # If not found as flat key, try nested structure
            # Split key like "cassini.iss.index" into parts
            parts = key.split(".")
            if len(parts) >= 3:
                mission, instrument, index_name = parts[0], parts[1], parts[2]
                section_key = f"{mission}.{instrument}"
                if section_key in self.config_doc and isinstance(
                    self.config_doc[section_key], dict
                ):
                    return self.config_doc[section_key].get(index_name, "")

            return ""
        except (KeyError, TypeError) as e:
            logger.error(f"get_url({key}) had exceptions:{e}")
            return ""

    def set_url(self, key: str, url: str):
        if self.config_doc is None:
            self.load_config()

        # Handle nested structure for 3-part keys
        parts = key.split(".")
        if len(parts) >= 3:
            mission, instrument, index_name = parts[0], parts[1], parts[2]
            section_key = f"{mission}.{instrument}"

            # Ensure the section exists
            if section_key not in self.config_doc:
                self.config_doc[section_key] = {}
            elif not isinstance(self.config_doc[section_key], dict):
                self.config_doc[section_key] = {}

            self.config_doc[section_key][index_name] = url
        else:
            # Fall back to flat key for backwards compatibility
            self.config_doc[key] = url

        self.save_config()

    def check_and_update_config(self):
        try:
            last_download = access_log.get_timestamp("config_download")
            last_update = access_log.get_timestamp("config_update")
            last_check = access_log.get_timestamp("config_check")
            timestamps = [
                t for t in [last_download, last_update, last_check] if t is not None
            ]
            if not timestamps:
                logger.info(
                    "No update timestamp found in access log, checking for updates"
                )
                self.update_config_from_remote()
                self.check_and_update_dynamic_urls()
                return
            last_activity = max(timestamps)
            from ..utils import is_older_than_hours

            if is_older_than_hours(last_activity, 24):
                logger.info(
                    "Last config check older than one day, checking for updates"
                )
                self.update_config_from_remote()
            else:
                logger.debug(f"Config is up to date (last activity: {last_activity})")
            self.check_and_update_dynamic_urls()
        except Exception as e:
            logger.warning(f"Error checking config age: {e}")
            self.update_config_from_remote()
            self.check_and_update_dynamic_urls()

    def update_config_from_remote(self):
        if self.config_path is None:
            self.load_config()
        try:
            logger.info(f"Checking for config updates from {index_urls_url}")
            current_content = self.config_path.read_text()
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
                self.config_path.write_text(comparison["remote_content"])
                logger.info("Config updated successfully from remote")
                self.load_config(auto_create=False)
                self.check_and_update_dynamic_urls()
                access_log.set_timestamp("config_update", datetime.datetime.now())
            else:
                logger.info("Remote config is identical to local config")
                access_log.set_timestamp("config_check", datetime.datetime.now())
        except tomlkit.exceptions.TOMLKitError as e:
            logger.warning(f"Could not parse TOML content: {e}")


# --- Module-level dynamic URL functions ---
def check_and_update_dynamic_urls():
    dynamic_indices = ["mro.ctx.edr", "lro.lroc.edr"]
    manager = IndexConfigManager()
    for key in dynamic_indices:
        try:
            last_timestamp = access_log.get_timestamp(key)
            if last_timestamp is None:
                logger.info(f"No timestamp found for {key}, discovering URL")
                discover_single_dynamic_url(key)
                continue
            if manager.is_older_than_hours(last_timestamp, 24):
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
    try:
        from .ctx_index import CTXIndex
        from .lroc_index import LROCIndex

        manager = IndexConfigManager()
        index_classes = {"mro.ctx.edr": CTXIndex, "lro.lroc.edr": LROCIndex}
        if key not in index_classes:
            logger.warning(f"Unknown dynamic index key: {key}")
            return None if return_info else None
        index_class = index_classes[key]
        index_instance = index_class()
        latest_url = index_instance.latest_index_label_url
        if latest_url:
            current_url = manager.get_url(key)
            is_update = current_url != str(latest_url)
            if is_update:
                logger.info(f"Update found: New {key} index URL discovered!")
                logger.info(f"  Previous: {current_url}")
                logger.info(f"  New: {latest_url}")
            else:
                logger.debug(f"No change for {key}: URL is still {latest_url}")
            manager.set_url(key, str(latest_url))
            access_log.log_url_discovery(
                key,
                str(latest_url),
                is_update=is_update,
                previous_url=current_url if is_update else None,
            )
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
    dynamic_indices = ["mro.ctx.edr", "lro.lroc.edr"]
    updates_found = {}
    for key in dynamic_indices:
        try:
            result = discover_single_dynamic_url(key, return_info=True)
            if result:
                updates_found[key] = result
                if result.get("is_update"):
                    print(f"Update available: New {key} index found!")
                    print(f"  Previous: {result.get('previous_url')}")
                    print(f"  New: {result.get('url')}")
        except Exception as e:
            logger.warning(f"Failed to discover latest URL for {key}: {e}")
            updates_found[key] = {"error": str(e)}
    return updates_found
