from functools import cached_property
from pathlib import Path

from yarl import URL

from planetarypy.config import config
from planetarypy.pds.utils import get_index
from planetarypy.utils import url_retrieve
from loguru import logger

BROWSE_URLBASE = URL("https://pds-rings.seti.org/viewmaster/volumes/GO_0xxx/")
DOWNLOAD_URLBASE = URL("https://pds-rings.seti.org/holdings/volumes/GO_0xxx/")
PREVIEWS_URLBASE = URL("https://pds-rings.seti.org/holdings/previews/GO_0xxx/")

STORAGE_ROOT = Path(config["storage_root"]) / "GO" / "SSI"
STORAGE_ROOT.mkdir(parents=True, exist_ok=True)

cache = dict()


def get_edr_index(refresh=False):
    "add some useful extra columns to the index."
    if "edrindex" in cache and not refresh:
        return cache["edrindex"]
    else:
        edrindex = get_index("go.ssi.edr", refresh=refresh)
        cache["edrindex"] = edrindex
        return edrindex


class EDR:
    """Support class to work with GO SSI EDR data.

    Note that some papers like McEwen(1998) use PICNO as image_id, but the PDS index calls it IMAGE_ID.
    """

    def __init__(self, image_id, save_folder=None):
        self.image_id = image_id
        self.save_folder = Path(save_folder) if save_folder else STORAGE_ROOT

    @cached_property
    def meta(self):
        return get_edr_index().query(f"IMAGE_ID=='{self.image_id}'").squeeze()

    @property
    def picno(self):
        "alias as some papers use picno"
        return self.image_id

    @property
    def volume(self):
        return self.meta["VOLUME_ID"]
    @property
    def urlpath(self):
        return f"{self.volume}/{self.meta["FILE_SPECIFICATION_NAME"]}"

    @property
    def browse_url(self):
        return BROWSE_URLBASE / self.urlpath

    @property
    def download_url(self):
        return DOWNLOAD_URLBASE / self.urlpath

    @property
    def label_url(self):
        return self.download_url.with_suffix(".LBL")

    @property
    def local_folder(self):
        return self.save_folder / f"EDR/{self.volume}"

    @property
    def image_path(self):
        return self.local_folder / self.download_url.name

    @property
    def label_path(self):
        return self.local_folder / self.label_url.name

    def download_image(self, refresh=False):
        d = self.local_folder
        d.mkdir(parents=True, exist_ok=True)
        if all([self.image_path.exists(), self.label_path.exists()]) and not refresh:
            logger.info(f"Local files exist. Force download with 'refresh=True'.")
            return self.local_folder
            return
        url_retrieve(self.download_url, self.image_path)
        url_retrieve(self.label_url, self.local_folder / self.label_url.name)
        logger.info(f"Downloaded image and label to {self.local_folder}")
        return self.local_folder

    def _gen_preview_url(self, size):
        """size one of ["full", "med", "small", "thumb"]."""
        p = Path(self.urlpath)
        return PREVIEWS_URLBASE / f"{p.parent}/{p.stem}_{size}.jpg"

    @property
    def preview_full_url(self):
        return self._gen_preview_url("full")

    @property
    def preview_med_url(self):
        return self._gen_preview_url("med")

    @property
    def preview_small_url(self):
        return self._gen_preview_url("small")

    @property
    def preview_thumb_url(self):
        return self._gen_preview_url("thumb")

    def download_all(self):
        self.download_image()
        url_retrieve(self.preview_full_url, self.local_folder / self.preview_full_url.name)
