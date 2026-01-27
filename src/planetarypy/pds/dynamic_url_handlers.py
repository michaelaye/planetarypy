"""dynamic_url_handlers module.
"""

__all__ = ["CTXIndex", "LROCIndex"]

import pandas as pd
from loguru import logger
from yarl import URL


class CTXIndex:
    url = "https://planetarydata.jpl.nasa.gov/img/data/mro/ctx/"
    backup_url = "https://pdsimage2.wr.usgs.gov/Mars_Reconnaissance_Orbiter/CTX"

    def __init__(self):
        self._volumes_table = None
        self._successful_url = None

    @property
    def volumes_table(self):
        if self._volumes_table is None:
            # Try primary URL first
            try:
                self._volumes_table = (
                    pd.read_html(self.url)[0]
                    .dropna(how="all", axis=1)
                    .dropna(how="all", axis=0)
                    .iloc[1:, :-1]
                )
                self._successful_url = self.url
            except Exception as e:
                # If primary URL fails, try backup URL
                logger.warning(
                    f"Failed to fetch CTX volumes table from primary URL {self.url}: {e}. "
                    f"Trying backup URL {self.backup_url}."
                )
                try:
                    self._volumes_table = (
                        pd.read_html(self.backup_url)[0]
                        .dropna(how="all", axis=1)
                        .dropna(how="all", axis=0)
                        .iloc[1:, :-1]
                    )
                    self._successful_url = self.backup_url
                    logger.info(
                        "Successfully fetched CTX volumes table from backup URL"
                    )
                except Exception as backup_error:
                    logger.error(
                        f"Failed to fetch CTX volumes table from backup URL {self.backup_url}: {backup_error}"
                    )
                    raise backup_error
        return self._volumes_table

    @property
    def latest_release_folder(self):
        return self.volumes_table.iloc[-2, 0]

    @property
    def latest_release_number(self):
        return self.latest_release_folder.rstrip("/").split("_")[1]

    @property
    def latest_index_label_url(self):
        # Use the successful URL (primary or backup) for constructing the label URL
        base_url = self._successful_url if self._successful_url else self.url
        return URL(base_url) / f"{self.latest_release_folder}index/cumindex.lbl"


class LROCIndex:
    edr_url = "https://pds.lroc.asu.edu/data/LRO-L-LROC-2-EDR-V1.0/"

    def __init__(self):
        self._volumes_table = None

    @property
    def volumes_table(self):
        if self._volumes_table is None:
            self._volumes_table = (
                pd.read_html(self.edr_url)[0]
                .dropna(how="all", axis=1)
                .dropna(how="all", axis=0)
                .iloc[1:-1, :-1]
            )
        return self._volumes_table

    @property
    def latest_release_folder(self):
        return self.volumes_table.iloc[-1, 0]

    @property
    def latest_release_number(self):
        return self.latest_release_folder.rstrip("/").split("_")[1]

    @property
    def latest_index_label_url(self):
        return URL(self.edr_url) / f"{self.latest_release_folder}INDEX/CUMINDEX.LBL"
