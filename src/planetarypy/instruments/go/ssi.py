from pathlib import Path

from loguru import logger
from yarl import URL

from planetarypy.config import config
from planetarypy.pds.utils import get_index
from planetarypy.utils import url_retrieve

BROWSE_URLBASE = URL("https://pds-rings.seti.org/viewmaster/volumes/GO_0xxx/")
DOWNLOAD_URLBASE = URL("https://pds-rings.seti.org/holdings/volumes/GO_0xxx/")
PREVIEWS_URLBASE = URL("https://pds-rings.seti.org/holdings/previews/GO_0xxx/")

STORAGE_ROOT = Path(config["storage_root"]) / "GO" / "SSI"
STORAGE_ROOT.mkdir(parents=True, exist_ok=True)

cache = dict()


def get_general_index(refresh=False):
    "add some useful extra columns to the index."
    if "edrindex" in cache and not refresh:
        return cache["edrindex"]
    else:
        edrindex = get_index("go.ssi.index", refresh=refresh)
        edrindex["subpath"] = (
            edrindex.VOLUME_ID + "/" + edrindex.FILE_SPECIFICATION_NAME
        )
        cache["edrindex"] = edrindex
        return edrindex


class EDR:
    """Support class to work with GO SSI EDR data.

    Using frameno a.k.a. frame number as the only unique identifier for GO SSI images.
    Index parameters like `IMAGE_ID` or OBSERVATION_ID are not unique and therefore can
    return sets of images. Use `EDRSet` for those.
    """

    def __init__(self, frameno, save_dir=None):
        self.frameno = frameno
        self.save_dir = Path(save_dir) if save_dir else STORAGE_ROOT
        ind = get_general_index()
        df = ind[
            ind["FILE_SPECIFICATION_NAME"].str.contains(f"{self.frameno}", na=False)
        ].T
        if df.empty:
            logger.error(f"No image found for frameno {self.frameno}")
        elif len(df.columns) > 1:
            logger.warning(
                f"Multiple images found for frameno {self.frameno}. Using the first one."
                "Use `EDRSet.from_frameno()` to get all matches and download or process all of them."
            )
        self._meta = df.iloc[:, 0]

    @property
    def meta(self):
        return self._meta

    @property
    def volume(self):
        return self.meta.loc["VOLUME_ID"]

    @property
    def target(self):
        return self.meta.loc["TARGET_NAME"]

    @property
    def subpath(self):
        return self.meta.loc["subpath"]

    @property
    def browse_url(self):
        return BROWSE_URLBASE / self.subpath

    @property
    def download_url(self):
        return DOWNLOAD_URLBASE / self.subpath

    @property
    def label_url(self):
        return self.download_url.with_suffix(".LBL")

    @property
    def image_path(self):
        return self.save_dir / Path("EDR") / self.subpath

    @property
    def label_path(self):
        return self.image_path.with_suffix(".LBL")

    @property
    def exists(self):
        return self.image_path.exists() and self.label_path.exists()

    @property
    def local_dir(self):
        return self.image_path.parent

    def _gen_preview_url(self, size):
        """size one of ["full", "med", "small", "thumb"]."""
        p = Path(self.subpath)
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

    def download(self, refresh=False, leave_tqdm=True):
        self.local_dir.mkdir(parents=True, exist_ok=True)
        if self.exists and not refresh:
            logger.info("Local files exist. Force download with 'refresh=True'.")
            return self.local_dir
        url_retrieve(self.download_url, self.image_path, leave_tqdm=leave_tqdm)
        url_retrieve(self.label_url, self.label_path, leave_tqdm=leave_tqdm)
        url_retrieve(
            self.preview_full_url,
            self.local_dir / self.preview_full_url.name,
            leave_tqdm=leave_tqdm,
        )

        logger.info(f"Downloaded image, label, and preview to {self.local_dir}")


class EDRSet:
    """Support class to work with GO SSI EDR data.

    The only unique identifier for GO SSI images seems to be what McEwen
    Searching through the FILE_SPECIFICATION_NAME column of the GO.SSI.INDEX for a
    frame number seems really to be the only realiable and sane way to identify a
    specific image.
    McEwen calls these numbers in the filename "frame number", maybe they are defined as
    such in the SIS? (TBchecked), so I will use that term here.

    E.g.: edr = EDR("349542200")
    """

    @classmethod
    def from_frameno(cls, frameno, save_dir=None):
        """Alternative constructor to create an EDRSet from a frameno."""
        ind = get_general_index()
        df = ind[ind["FILE_SPECIFICATION_NAME"].str.contains(f"{frameno}", na=False)]
        if df.empty:
            logger.error(f"No image(s) found for frameno {frameno}")
            return
        elif len(df) == 1:
            logger.warning(
                f"Only one image found for frameno {frameno}. Using EDR instead of EDRSet."
            )
            return EDR(frameno, save_dir=save_dir)
        else:
            instance = cls(df, save_dir=save_dir)
            instance._meta = df.set_index("FILE_SPECIFICATION_NAME")
            return instance

    @classmethod
    def from_image_id(cls, image_id, save_dir=None):
        """Alternative constructor to create an EDRSet from a frameno."""
        ind = get_general_index()
        df = ind.query("IMAGE_ID == @image_id")
        if df.empty:
            logger.error(f"No image(s) found for image_id {image_id}")
            return
        elif len(df) == 1:
            logger.warning(
                f"Only one image found for image_id {image_id}. Using EDR instead of EDRSet."
            )
            return EDR(df.FILENAME_SPECIFICATION_NAME, save_dir=save_dir)
        else:
            instance = cls(df, save_dir=save_dir)
            instance._meta = df
            return instance

    def __init__(self, meta, save_dir=None):
        self._meta = meta
        self.save_dir = Path(save_dir) if save_dir else STORAGE_ROOT

    @property
    def meta(self):
        return self._meta

    @property
    def volume(self):
        return self.meta["VOLUME_ID"]

    @property
    def target(self):
        return self.meta["TARGET_NAME"]

    @property
    def subpath(self):
        return self.meta.T.loc["subpath"]

    @property
    def browse_url(self):
        return BROWSE_URLBASE / self.subpath

    @property
    def download_url(self):
        return DOWNLOAD_URLBASE / self.subpath

    @property
    def label_url(self):
        return [i.with_suffix(".LBL") for i in self.download_url]

    @property
    def image_path(self):
        return self.save_dir / Path("EDR") / self.subpath

    @property
    def local_dir(self):
        return [i.parent for i in self.image_path]

    @property
    def images_exists(self):
        return all([i.exists() for i in self.image_path])

    @property
    def labels_exists(self):
        return all([i.exists() for i in self.label_path])

    @property
    def exists_with_names(self):
        return [f"{i}: {i.exists()}" for i in self.image_path]

    @property
    def label_path(self):
        return [i.with_suffix(".LBL") for i in self.image_path]

    def _gen_preview_url(self, size):
        """size one of ["full", "med", "small", "thumb"]."""
        paths = [Path(p) for p in self.subpath]
        return [PREVIEWS_URLBASE / f"{p.parent}/{p.stem}_{size}.jpg" for p in paths]

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

    def download(self, refresh=False):
        if self.images_exists and self.labels_exists and not refresh:
            logger.info("Local files exist. Force download with 'refresh=True'.")
            return self.local_dir
            return
        failed_urls = []
        for url, path in zip(self.download_url, self.image_path):
            path.parent.mkdir(parents=True, exist_ok=True)
            try:
                url_retrieve(url, path)
            except Exception:
                failed_urls.append(str(url))
        for url, path, preview in zip(
            self.label_url, self.label_path, self.preview_full_url
        ):
            path.parent.mkdir(parents=True, exist_ok=True)
            try:
                url_retrieve(url, path)
                url_retrieve(preview, path.parent / preview.name)
            except Exception:
                failed_urls.append(str(url))
        for url, path in self.preview_full_url:
            path = self.local_dir / url.name
            path.parent.mkdir(parents=True, exist_ok=True)
            try:
                url_retrieve(url, path)
            except Exception:
                failed_urls.append(str(url))

        logger.info(f"Downloaded image, label, and preview to {self.local_dir}")
        if failed_urls:
            logger.warning(
                f"Failed to download {len(failed_urls)} files. Stored as .failed_urls."
            )
            self.failed_urls = failed_urls
        return self.local_dir
