"""General configuration for the planetarypy package.

This module handles general configuration settings such as storage locations.
PDS index URLs and timestamps are now managed separately in pds/index_config.py.
"""

__all__ = ["config", "Config"]

import json
import os
from functools import reduce
from pathlib import Path

import tomlkit


class Config:
    """Manage general configuration settings.

    This class handles general configuration settings for the planetarypy package.
    At minimum, there should be the `storage_root` attribute for storing data.

    Note: PDS index URLs and timestamps are now managed separately in pds/index_config.py.
    """

    # This part enables a config path location override using env PLANETARYPY_CONFIG
    fname = "planetarypy_config.toml"
    # separating fname from fpath so that resource_path below is correct.
    path = Path(os.getenv("PLANETARYPY_CONFIG", Path.home() / f".{fname}"))

    def __init__(self, config_path: str = None):  # str or pathlib.Path
        """Switch to other config file location with `config_path`."""
        if config_path is not None:
            self.path = Path(config_path)
        if not self.path.exists():
            self._create_default_config()
        self._read_config()

    def _create_default_config(self):
        """Create a minimal default config file with just storage_root."""
        doc = tomlkit.document()
        doc.add(tomlkit.comment("PlanetaryPy Configuration"))
        doc.add(tomlkit.nl())
        doc.add(tomlkit.comment("Root directory for storing all planetarypy data"))
        doc.add(tomlkit.nl())
        doc["storage_root"] = ""  # Empty string will be updated during _read_config
        self.path.write_text(tomlkit.dumps(doc))

    def _read_config(self):
        """Read the configfile and store config dict.

        `storage_root` will be stored as attribute.
        """
        self.tomldoc = tomlkit.loads(self.path.read_text())
        if not self.tomldoc.get("storage_root"):
            path = Path.home() / "planetarypy_data"
            path.mkdir(exist_ok=True)
            self.tomldoc["storage_root"] = str(path)
            self.storage_root = path
            self.save()
        else:
            self.storage_root = Path(self.tomldoc["storage_root"])

    @property
    def d(self):
        """Get the Python dictionary from the TOML document."""
        return self.tomldoc

    def __getitem__(self, key: str):
        """Get sub-dictionary by nested key."""
        try:
            return reduce(lambda c, k: c[k], key.split("."), self.d)
        except KeyError:
            return ""

    def get_value(
        self,
        key: str,  # A nested key in dotted format
    ) -> str:  # Returning empty string if not existing, because Path('') is False which is handy
        """Get sub-dictionary by nested key."""
        try:
            return reduce(lambda c, k: c[k], key.split("."), self.d)
        except KeyError:
            return ""

    def set_value(
        self,
        nested_key: str,  # A nested key in dotted format
        value: float | str,  # Value for the given key to be stored
        save: bool = True,  # Switch to control writing out to disk
    ):
        """Set value in sub-dic using dotted key."""
        dic = self.tomldoc
        keys = nested_key.split(".")
        for key in keys[:-1]:
            # Create the parent dictionaries if they don't exist
            if key not in dic:
                dic[key] = {}
            dic = dic[key]
        dic[keys[-1]] = value
        if save:
            self.save()

    def __setitem__(self, nested_key: str, value: float | str):
        """Set value in sub-dic using dotted key."""
        self.set_value(nested_key, value)

    def save(self):
        """Write the TOML doc to file."""
        self.path.write_text(tomlkit.dumps(self.tomldoc))

    def __repr__(self):
        return json.dumps(self.d, indent=2)


# Create a singleton instance
config = Config()
