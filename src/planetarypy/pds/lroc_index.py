"""LROC Index is not at a fixed URL, so need to determine dynamically."""

__all__ = ["LROCIndex"]

import pandas as pd
from yarl import URL


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
        return URL(self.edr_url) / f"{self.latest_release_folder}/INDEX/CUMINDEX.LBL"
