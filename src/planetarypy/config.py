"""General configuration for the planetarypy package.

This module handles general configuration settings such as storage locations.
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
        """Create a minimal default config file with documented defaults."""
        doc = tomlkit.document()
        doc.add(tomlkit.comment("PlanetaryPy Configuration"))
        doc.add(tomlkit.nl())
        doc.add(tomlkit.comment("Root directory for storing all planetarypy data"))
        doc.add(tomlkit.nl())
        doc["storage_root"] = ""  # Empty string will be updated during _read_config
        doc.add(tomlkit.nl())
        doc.add(tomlkit.comment(
            "Suppress upstream DeprecationWarning during CLI execution"
        ))
        doc.add(tomlkit.comment(
            "(e.g. Typer's shell_complete= deprecation notice)."
        ))
        doc.add(tomlkit.comment(
            "Devs developing planetarypy can set this to false to see"
        ))
        doc.add(tomlkit.comment(
            "deprecation notices as reminders to track upstream."
        ))
        doc["filter_deprecation_warnings"] = True
        doc.add(tomlkit.nl())
        doc.add(tomlkit.comment(
            "Row-count threshold above which row-display commands"
        ))
        doc.add(tomlkit.comment(
            "(e.g. `plp indexes select`) switch from the transposed"
        ))
        doc.add(tomlkit.comment(
            "Rich table to CSV. Lower it if your terminal is narrow."
        ))
        doc["max_table_rows"] = 4
        self.path.write_text(tomlkit.dumps(doc))

    def _read_config(self):
        """Read the configfile and store config dict.

        `storage_root` will be stored as attribute. Backfills new
        default-bearing keys that aren't present in older config files
        so users see the available knobs next time they open the file.
        """
        self.tomldoc = tomlkit.loads(self.path.read_text())
        dirty = False
        if not self.tomldoc.get("storage_root"):
            path = Path.home() / "planetarypy_data"
            path.mkdir(exist_ok=True)
            self.tomldoc["storage_root"] = str(path)
            self.storage_root = path
            dirty = True
        else:
            self.storage_root = Path(self.tomldoc["storage_root"])
        # Backfill for older configs created before the key existed.
        # Use ``in`` rather than ``get()`` so an explicit ``false`` is
        # preserved — falsy-but-present is a user choice we honor.
        if "filter_deprecation_warnings" not in self.tomldoc:
            self.tomldoc.add(tomlkit.nl())
            self.tomldoc.add(tomlkit.comment(
                "Suppress upstream DeprecationWarning during CLI execution"
            ))
            self.tomldoc.add(tomlkit.comment(
                "(e.g. Typer's shell_complete= deprecation notice)."
            ))
            self.tomldoc.add(tomlkit.comment(
                "Devs developing planetarypy can set this to false to see"
            ))
            self.tomldoc.add(tomlkit.comment(
                "deprecation notices as reminders to track upstream."
            ))
            self.tomldoc["filter_deprecation_warnings"] = True
            dirty = True
        if "max_table_rows" not in self.tomldoc:
            self.tomldoc.add(tomlkit.nl())
            self.tomldoc.add(tomlkit.comment(
                "Row-count threshold above which row-display commands"
            ))
            self.tomldoc.add(tomlkit.comment(
                "(e.g. `plp indexes select`) switch from the transposed"
            ))
            self.tomldoc.add(tomlkit.comment(
                "Rich table to CSV. Lower it if your terminal is narrow."
            ))
            self.tomldoc["max_table_rows"] = 4
            dirty = True
        if dirty:
            self.save()

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
