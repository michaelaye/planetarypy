"""Scraping the latest CTX index file from the latest added volume."""

__all__ = ["CTXIndex"]

import pandas as pd
from yarl import URL


class CTXIndex:
    url = "https://planetarydata.jpl.nasa.gov/img/data/mro/ctx/"

    def __init__(self):
        self._volumes_table = None

    @property
    def volumes_table(self):
        if self._volumes_table is None:
            self._volumes_table = (
                pd.read_html(self.url)[0]
                .dropna(how="all", axis=1)
                .dropna(how="all", axis=0)
                .iloc[1:, :-1]
            )
        return self._volumes_table

    @property
    def latest_release_folder(self):
        return self.volumes_table.iloc[-2, 0]

    @property
    def latest_release_number(self):
        return self.latest_release_folder.rstrip("/").split("_")[1]

    @property
    def latest_index_label_url(self):
        return URL(self.url) / f"{self.latest_release_folder}index/cumindex.lbl"
