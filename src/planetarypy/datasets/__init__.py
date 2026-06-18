"""Body-namespaced access to remote (non-PDS) reference rasters.

First slice of the design in ``Plans/datasets_subpackage_design.qmd``: a
baked-in registry of remote data products, namespaced **by body**, with windowed
``/vsicurl/`` reads of cloud-optimised GeoTIFFs by lon/lat box — no full
download. Raster + vsicurl only for now; the remote-refreshed registry, download
mode, and ``plp datasets`` CLI from the design doc are deferred.

    from planetarypy import datasets

    datasets.bodies()                       # ['mars']
    datasets.mars.hrsc_level3               # a RemoteRaster
    da = datasets.mars.hrsc_level3.read_window(lon=0, lat=0, size=1.0)   # DataArray
    datasets.mars.hrsc_level3.read_window(0, 0, 1.0, out="patch.tif")    # + GeoTIFF
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

__all__ = [
    "RemoteRaster", "StacCollection", "StacItem",
    "list_datasets", "bodies", "read_window", "read_bbox", "stac_search",
]

# GDAL options that make /vsicurl/ COG reads fast (no directory listing, only
# fetch the byte ranges the window needs).
_GDAL_ENV = {
    "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
    "CPL_VSIL_CURL_ALLOWED_EXTENSIONS": ".tif",
    "GDAL_HTTP_MAX_RETRY": "3",
    "GDAL_HTTP_RETRY_DELAY": "1",
}


def _set_gdal_env() -> None:
    import os

    for k, v in _GDAL_ENV.items():
        os.environ.setdefault(k, v)


@dataclass(frozen=True)
class RemoteRaster:
    """A remote reference raster, accessed by streaming (``/vsicurl/``).

    ``crs`` is informational metadata (the read uses the file's own CRS); the
    other fields document provenance. See the design doc for the fuller
    ``DatasetConfig`` this will grow into.
    """

    key: str               # dotted: "dlr.mars.hrsc.level3_eqc"
    short: str             # body-namespace attribute, e.g. "hrsc_level3"
    name: str
    body: str              # lower-case body name, e.g. "mars"
    provider: str
    url: str
    crs: str               # e.g. "IAU_2015:49910"
    kind: str = "vsicurl"
    nodata: "Optional[float]" = None
    pixel_scale_m: "Optional[float]" = None
    version: "Optional[str]" = None
    homepage: "Optional[str]" = None
    citation: "Optional[str]" = None

    @property
    def vsicurl(self) -> str:
        """The GDAL ``/vsicurl/`` URL for streaming reads."""
        return self.url if self.url.startswith("/vsicurl/") else f"/vsicurl/{self.url}"

    def open(self):
        """Open the remote COG as a ``rasterio`` dataset (streamed, not downloaded)."""
        import rasterio

        _set_gdal_env()
        return rasterio.open(self.vsicurl)

    def read_window(self, lon: float, lat: float, size: float = 1.0, *,
                    anchor: str = "center",
                    out: "Optional[Union[str, Path]]" = None):
        """Read a ``size``-degree lon/lat box positioned at ``(lon, lat)``.

        ``anchor`` says where ``(lon, lat)`` sits on the box:
        ``"center"`` (default), or a corner — ``"lower-left"``/``"sw"``,
        ``"upper-left"``/``"nw"``, ``"lower-right"``/``"se"``,
        ``"upper-right"``/``"ne"`` (degree-square tiling conventions use the SW
        corner; raster/array origin is the NW corner).

        Returns a georeferenced :class:`xarray.DataArray` (rioxarray). If ``out``
        is given, the window is also written to that GeoTIFF. Reads only the byte
        ranges the window needs over HTTP — no full download.
        """
        return read_window(self, lon, lat, size, anchor=anchor, out=out)

    def read_bbox(self, west: float, south: float, east: float, north: float, *,
                  out: "Optional[Union[str, Path]]" = None):
        """Read an explicit lon/lat box (degrees). See :func:`read_bbox`."""
        return read_bbox(self, west, south, east, north, out=out)


@dataclass(frozen=True)
class StacItem:
    """One STAC item resolved to its COG asset — reads like a RemoteRaster."""

    id: str
    cog_url: str
    collection: str
    bbox: tuple = ()                 # raw STAC bbox (USGS reports it in projected m)
    datetime: "Optional[str]" = None
    nodata: "Optional[float]" = None
    short: str = "stac_item"

    @property
    def vsicurl(self) -> str:
        u = self.cog_url
        return u if u.startswith("/vsicurl/") else f"/vsicurl/{u}"

    def read_window(self, lon, lat, size=1.0, *, anchor="center", out=None):
        return read_window(self, lon, lat, size, anchor=anchor, out=out)

    def read_bbox(self, west, south, east, north, *, out=None):
        return read_bbox(self, west, south, east, north, out=out)


@dataclass(frozen=True)
class StacCollection:
    """A STAC-backed dataset: query a collection by location → COG items.

    Unlike :class:`RemoteRaster` (one fixed COG), a collection holds many COGs
    (per quad / per observation); :meth:`search` / :meth:`at` resolve a location
    to the matching :class:`StacItem` (s), which then read like any COG.
    """

    key: str
    short: str
    name: str
    body: str
    provider: str
    stac_url: str                    # STAC API root, e.g. ".../api"
    collection: str                  # STAC collection id
    kind: str = "stac"
    asset_key: "Optional[str]" = None  # which asset to read; None = first COG asset
    homepage: "Optional[str]" = None

    def search(self, *, bbox=None, lon=None, lat=None, limit: int = 20):
        """Items overlapping a bbox ``(west, south, east, north)`` deg, or a point."""
        return stac_search(self, bbox=bbox, lon=lon, lat=lat, limit=limit)

    def at(self, lon: float, lat: float, *, limit: int = 20):
        """Items covering a lon/lat point."""
        return stac_search(self, lon=lon, lat=lat, limit=limit)

    def read_window(self, lon, lat, size=1.0, *, anchor="center", out=None):
        """Convenience: read from the first item covering ``(lon, lat)``."""
        items = self.at(lon, lat, limit=1)
        if not items:
            raise ValueError(
                f"no {self.collection!r} item covers lon={lon}, lat={lat}"
            )
        return items[0].read_window(lon, lat, size, anchor=anchor, out=out)


def _pick_cog_asset(assets: dict, asset_key: "Optional[str]") -> "Optional[str]":
    """Href of the COG asset: the named one, else the first GeoTIFF/COG asset."""
    if asset_key and asset_key in assets:
        return assets[asset_key].get("href")
    for a in assets.values():
        typ = str(a.get("type", "")).lower()
        href = a.get("href", "")
        if "cloud-optimized" in typ or "geotiff" in typ or href.lower().endswith(".tif"):
            return href
    return None


def stac_search(coll: "StacCollection", *, bbox=None, lon=None, lat=None,
                limit: int = 20) -> "list[StacItem]":
    """Query a STAC collection; return its COG-bearing items as StacItems.

    ``bbox`` is ``(west, south, east, north)`` in degrees; or pass ``lon``/``lat``
    for a point (expanded to a tiny box, since STAC rejects a zero-area bbox).
    """
    import requests

    if bbox is None:
        if lon is None or lat is None:
            raise ValueError("provide bbox=(west, south, east, north) or lon= and lat=")
        eps = 1e-4
        bbox = (lon - eps, lat - eps, lon + eps, lat + eps)
    params = {
        "collections": coll.collection,
        "bbox": ",".join(str(v) for v in bbox),
        "limit": int(limit),
    }
    resp = requests.get(f"{coll.stac_url}/search", params=params, timeout=60)
    resp.raise_for_status()
    items: "list[StacItem]" = []
    for f in resp.json().get("features", []):
        href = _pick_cog_asset(f.get("assets", {}), coll.asset_key)
        if not href:
            continue
        items.append(StacItem(
            id=f.get("id"),
            cog_url=href,
            collection=coll.collection,
            bbox=tuple(f.get("bbox", ()) or ()),
            datetime=(f.get("properties", {}) or {}).get("datetime"),
            short=coll.short,
        ))
    return items


# ── baked-in registry (the remote-refreshed TOML from the design doc is deferred) ──
_USGS_STAC = "https://stac.astrogeology.usgs.gov/api"

_REGISTRY: dict = {
    r.key: r
    for r in [
        # ── single global COG (FU Berlin / DLR) ──
        RemoteRaster(
            key="dlr.mars.hrsc.level3_eqc",
            short="hrsc_level3",
            name="HRSC level-3 global mosaic (equirectangular, IAU 49910)",
            body="mars",
            provider="FU Berlin / DLR (HRSC)",
            url="https://maps.planet.fu-berlin.de/level3/level3-iau-eqc.tif",
            crs="IAU_2015:49910",
            kind="vsicurl",
            nodata=-32768.0,
            pixel_scale_m=50.0,
            homepage="https://maps.planet.fu-berlin.de/",
        ),
        # ── USGS Astrogeology STAC collections (astrogeo-ard COGs) ──
        StacCollection(
            key="usgs.mars.themis.controlled_mosaics",
            short="themis_mosaics",
            name="THEMIS controlled IR mosaics (per MC quad)",
            body="mars",
            provider="USGS Astrogeology",
            stac_url=_USGS_STAC,
            collection="mo_themis_controlled_mosaics",
            homepage="https://stac.astrogeology.usgs.gov/",
        ),
        StacCollection(
            key="usgs.mars.ctx.controlled_dtms",
            short="ctx_dtms",
            name="CTX controlled USGS DTMs",
            body="mars",
            provider="USGS Astrogeology",
            stac_url=_USGS_STAC,
            collection="mro_ctx_controlled_usgs_dtms",
            asset_key="geoid_adjusted_dem",
            homepage="https://stac.astrogeology.usgs.gov/",
        ),
        StacCollection(
            key="usgs.moon.lola.dtms",
            short="lola_dtms",
            name="Lunar Orbiter Laser Altimeter DTMs",
            body="moon",
            provider="USGS Astrogeology",
            stac_url=_USGS_STAC,
            collection="lunar_orbiter_laser_altimeter",
            homepage="https://stac.astrogeology.usgs.gov/",
        ),
    ]
}


def list_datasets(body: "Optional[str]" = None) -> "list[RemoteRaster]":
    """Registered rasters, optionally filtered to one body (case-insensitive)."""
    out = list(_REGISTRY.values())
    if body is not None:
        out = [r for r in out if r.body == body.lower()]
    return out


def bodies() -> "list[str]":
    """Sorted list of bodies that have at least one registered raster."""
    return sorted({r.body for r in _REGISTRY.values()})


# anchor name (and aliases) -> internal code: where (lon, lat) sits on the box.
_ANCHOR_ALIASES = {
    "center": "center", "centre": "center", "c": "center",
    "lower-left": "ll", "lower left": "ll", "ll": "ll", "sw": "ll", "southwest": "ll",
    "upper-left": "ul", "upper left": "ul", "ul": "ul", "nw": "ul", "northwest": "ul",
    "lower-right": "lr", "lower right": "lr", "lr": "lr", "se": "lr", "southeast": "lr",
    "upper-right": "ur", "upper right": "ur", "ur": "ur", "ne": "ur", "northeast": "ur",
}


def _box_lonlat(lon: float, lat: float, size: float, anchor: str):
    """``(lon, lat, size, anchor)`` → geographic ``(west, south, east, north)``."""
    a = _ANCHOR_ALIASES.get(str(anchor).strip().lower())
    if a is None:
        raise ValueError(
            "anchor must be center / lower-left / upper-left / lower-right / "
            f"upper-right (or sw/nw/se/ne); got {anchor!r}"
        )
    if a == "center":
        h = size / 2.0
        return (lon - h, lat - h, lon + h, lat + h)
    if a == "ll":   # (lon, lat) is the SW corner
        return (lon, lat, lon + size, lat + size)
    if a == "ul":   # NW corner
        return (lon, lat - size, lon + size, lat)
    if a == "lr":   # SE corner
        return (lon - size, lat, lon, lat + size)
    return (lon - size, lat - size, lon, lat)  # "ur": NE corner


@dataclass(frozen=True)
class _UrlSource:
    """Minimal readable source for a bare COG URL (no registry entry)."""

    url: str
    nodata: "Optional[float]" = None
    short: str = "window"

    @property
    def vsicurl(self) -> str:
        return self.url if self.url.startswith("/vsicurl/") else f"/vsicurl/{self.url}"


def _as_source(source):
    """Resolve a RemoteRaster / StacItem / registry key / URL to a readable source."""
    if hasattr(source, "vsicurl"):
        return source
    if isinstance(source, str):
        if source in _REGISTRY and hasattr(_REGISTRY[source], "vsicurl"):
            return _REGISTRY[source]
        if source.startswith(("http://", "https://", "/vsicurl/")):
            return _UrlSource(source)
        raise KeyError(f"{source!r} is not a readable registry key or URL")
    raise TypeError(f"cannot read from {type(source).__name__}")


def read_bbox(source, west: float, south: float, east: float, north: float, *,
              out: "Optional[Union[str, Path]]" = None):
    """Read an explicit lon/lat box (degrees) from a raster source.

    ``source`` is a :class:`RemoteRaster`, a :class:`StacItem`, a registry key,
    or a bare COG URL. The box is transformed into the file's own CRS via pyproj
    (so this works for any body / projection, not just equirectangular), then
    read with a single rasterio window. Returns a georeferenced
    :class:`xarray.DataArray`; writes a GeoTIFF too when ``out`` is given. Reads
    only the byte ranges the window needs.
    """
    import rasterio
    import xarray as xr
    from pyproj import CRS, Transformer
    from rasterio.windows import from_bounds

    import rioxarray  # noqa: F401  (registers the .rio accessor)

    src = _as_source(source)
    _set_gdal_env()
    with rasterio.open(src.vsicurl) as ds:
        geodetic = CRS.from_user_input(ds.crs).geodetic_crs
        tf = Transformer.from_crs(geodetic, ds.crs, always_xy=True)
        xs, ys = tf.transform([west, east, west, east], [south, south, north, north])
        win = from_bounds(
            min(xs), min(ys), max(xs), max(ys), ds.transform
        ).round_offsets().round_lengths()
        data = ds.read(window=win)
        transform = ds.window_transform(win)
        crs = ds.crs
        nodata = src.nodata if src.nodata is not None else ds.nodata

    da = xr.DataArray(
        data, dims=("band", "y", "x"),
        coords={"band": list(range(1, data.shape[0] + 1))},
        name=getattr(src, "short", "window"),
    )
    da.rio.write_crs(crs, inplace=True)
    da.rio.write_transform(transform, inplace=True)
    if nodata is not None:
        da.rio.write_nodata(nodata, inplace=True)
    if out is not None:
        da.rio.to_raster(str(out))
    return da


def read_window(source, lon: float, lat: float, size: float = 1.0, *,
                anchor: str = "center", out: "Optional[Union[str, Path]]" = None):
    """Read a ``size``-degree lon/lat box positioned at ``(lon, lat)``.

    ``source`` is anything :func:`read_bbox` accepts (RemoteRaster, StacItem,
    registry key, or COG URL). ``anchor`` places ``(lon, lat)`` on the box:
    ``"center"`` (default) or a corner — ``"lower-left"``/``"sw"`` (degree-square
    tiling convention), ``"upper-left"``/``"nw"`` (raster origin),
    ``"lower-right"``/``"se"``, ``"upper-right"``/``"ne"``. Thin wrapper over
    :func:`read_bbox`.
    """
    west, south, east, north = _box_lonlat(lon, lat, size, anchor)
    return read_bbox(source, west, south, east, north, out=out)


class _BodyNamespace:
    """``datasets.mars`` — exposes that body's rasters as attributes."""

    def __init__(self, body: str, rasters: "list[RemoteRaster]"):
        self._body = body
        self._by_short = {r.short: r for r in rasters}

    def __getattr__(self, name: str) -> RemoteRaster:
        try:
            return self._by_short[name]
        except KeyError:
            raise AttributeError(
                f"{self._body!r} has no dataset {name!r}; "
                f"available: {sorted(self._by_short)}"
            ) from None

    def __dir__(self):
        return list(super().__dir__()) + list(self._by_short)

    def __repr__(self):
        return f"<datasets.{self._body}: {sorted(self._by_short)}>"


def __getattr__(name: str):
    """``datasets.<body>`` → a body namespace (e.g. ``datasets.mars``)."""
    rasters = [r for r in _REGISTRY.values() if r.body == name.lower()]
    if rasters:
        return _BodyNamespace(name.lower(), rasters)
    raise AttributeError(f"module 'planetarypy.datasets' has no attribute {name!r}")


def __dir__():
    return sorted(set(list(globals()) + bodies()))
