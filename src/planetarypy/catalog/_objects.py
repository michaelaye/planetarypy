"""Mission and Instrument objects for navigating the PDS catalog.

These provide an object-oriented interface to the catalog, with
human-readable full names for missions and instruments.

    >>> from planetarypy.catalog import Mission
    >>> mro = Mission("mro")
    >>> mro.full_name
    'Mars Reconnaissance Orbiter'
    >>> mro.instruments
    ['crism', 'ctx', 'hirise', 'marci', 'mcs', 'rss', 'sharad']
    >>> ctx = mro["ctx"]
    >>> ctx.full_name
    'Context Camera'
    >>> ctx.product_types
    ['edr', 'rdr', ...]
"""

from __future__ import annotations

from planetarypy.catalog._mission_map import (
    MISSION_FULL_NAMES,
    INSTRUMENT_FULL_NAMES,
)


class Instrument:
    """A single instrument on a planetary science mission.

    Parameters
    ----------
    mission : str
        Mission shorthand (e.g. 'mro')
    name : str
        Instrument shorthand (e.g. 'ctx')
    """

    def __init__(self, mission: str, name: str):
        self.mission = mission
        self.name = name

    @property
    def full_name(self) -> str | None:
        """Human-readable instrument name, or None if not in lookup."""
        return INSTRUMENT_FULL_NAMES.get((self.mission, self.name))

    @property
    def product_types(self) -> list[str]:
        """Normalized product types from the catalog DB."""
        from planetarypy.catalog import list_products
        return list_products(self.mission, self.name)

    def product_type_details(self):
        """Product types with phase/format breakdown as a DataFrame."""
        from planetarypy.catalog import list_products
        return list_products(self.mission, self.name, include_phases=True)

    def __repr__(self) -> str:
        full = self.full_name
        if full:
            return f"Instrument('{self.mission}.{self.name}', '{full}')"
        return f"Instrument('{self.mission}.{self.name}')"


class Mission:
    """A planetary science mission in the PDS catalog.

    Works in two modes:
    - Static: full_name is always available from the lookup table
    - DB-backed: instruments, product_types require a built catalog

    Parameters
    ----------
    name : str
        Mission shorthand (e.g. 'mro', 'cassini', 'hst')
    """

    def __init__(self, name: str):
        self.name = name

    @property
    def full_name(self) -> str | None:
        """Human-readable mission name, or None if not in lookup."""
        return MISSION_FULL_NAMES.get(self.name)

    @property
    def instruments(self) -> list[str]:
        """List instrument shorthands from the catalog DB (excludes _misc)."""
        from planetarypy.catalog import list_instruments
        return list_instruments(self.name)

    @property
    def misc(self) -> Instrument | None:
        """Access the _misc catch-all instrument, or None if it doesn't exist."""
        from planetarypy.catalog import list_instruments
        all_instr = list_instruments(self.name, include_misc=True)
        if "_misc" in all_instr:
            return Instrument(self.name, "_misc")
        return None

    def __getitem__(self, instrument: str) -> Instrument:
        """Get an Instrument object by shorthand name."""
        return Instrument(self.name, instrument)

    def __repr__(self) -> str:
        full = self.full_name
        if full:
            return f"Mission('{self.name}', '{full}')"
        return f"Mission('{self.name}')"
