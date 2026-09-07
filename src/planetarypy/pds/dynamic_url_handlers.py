"""dynamic_url_handlers module.
"""

__all__ = ["CTXIndex", "LROCIndex", "LAMPEDRIndex", "LAMPRDRIndex"]

import re

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
                        "Failed to fetch CTX volumes table from backup URL "
                        f"{self.backup_url}: {backup_error}"
                    )
                    raise backup_error
        return self._volumes_table

    @property
    def latest_release_folder(self):
        """Newest ``mrox_NNNN/`` volume directory in the scraped listing.

        Selected by matching the volume-directory pattern, not by position. The
        listing interleaves a checksum file after every volume and ends with an
        unrelated entry:

            mrox_5584/
            mrox_5584_md5.txt
            nssdca/

        so the previous ``iloc[-2, 0]`` returned ``mrox_5584_md5.txt``. Combined
        with the f-string join below — which assumes a trailing slash — that built
        ``.../mrox_5584_md5.txtindex/cumindex.lbl`` and 404'd.
        """
        vols = [
            v for v in self.volumes_table.iloc[:, 0].astype(str)
            if re.fullmatch(r"mrox_\d+/?", v.strip())
        ]
        if not vols:
            msg = "no mrox_* volume directories found in the CTX volumes listing"
            raise RuntimeError(msg)
        latest = max(vols, key=lambda s: int(re.search(r"\d+", s).group()))
        # normalise the trailing slash so the URL join below cannot concatenate
        return latest.strip().rstrip("/") + "/"

    @property
    def latest_release_number(self):
        return self.latest_release_folder.rstrip("/").split("_")[1]

    @property
    def latest_index_label_url(self):
        # Use the successful URL (primary or backup) for constructing the label URL
        base_url = self._successful_url if self._successful_url else self.url
        return URL(base_url) / f"{self.latest_release_folder}index/cumindex.lbl"


class _LAMPIndexBase:
    """Base class for LRO LAMP dynamic index discovery.

    LAMP volumes are delivered to planetarydata.jpl.nasa.gov with incrementing
    volume numbers (LROLAM_0062 for EDR, LROLAM_1062 for RDR). The cumulative
    index lives in the latest volume's INDEX/ directory.
    """

    url: str  # set by subclass

    def __init__(self):
        self._volumes_table = None

    @property
    def volumes_table(self):
        if self._volumes_table is None:
            df = pd.read_html(self.url)[1]  # table 1 is the file listing
            # Filter to volume directories only (LROLAM_NNNN/)
            mask = df["Name"].str.match(r"LROLAM_\d{4}/", na=False)
            self._volumes_table = df[mask].reset_index(drop=True)
        return self._volumes_table

    @property
    def latest_release_folder(self):
        return self.volumes_table.iloc[-1]["Name"]

    @property
    def latest_release_number(self):
        return self.latest_release_folder.rstrip("/").split("_")[1]

    @property
    def latest_index_label_url(self):
        return URL(self.url) / f"{self.latest_release_folder}INDEX/CUMINDEX.LBL"


class LAMPEDRIndex(_LAMPIndexBase):
    url = "https://planetarydata.jpl.nasa.gov/img/data/lro/lamp/edr/"


class LAMPRDRIndex(_LAMPIndexBase):
    url = "https://planetarydata.jpl.nasa.gov/img/data/lro/lamp/rdr/"


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
