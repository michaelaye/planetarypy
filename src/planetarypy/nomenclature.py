"""IAU-approved surface feature names, as a plottable layer.

The matplotlib basemap/cartopy habit of dropping `coastlines()` onto a plot to
make it legible has no planetary equivalent — there are no coastlines on Mars.
What there is: the USGS Gazetteer of Planetary Nomenclature, the authority for
IAU-adopted feature names on 47 bodies. This module fetches it, caches it, and
puts it on your axes.

    >>> from planetarypy import nomenclature
    >>> nomenclature.features("mars", type="Crater", min_diameter=100)
    >>> nomenclature.add_features(ax, "mars")           # onto axes you already have

Data comes from the Gazetteer's per-body GIS downloads. There is no JSON API —
the search pages are HTML only — so the shapefile zips are the machine-readable
path. Everything shipped is ``approval == "Adopted by IAU"``.

A note on coordinate systems, because it matters here: the gazetteer ships ESRI
authority codes (Mars is ``ESRI:104905``, an ellipsoid), while planetarypy
standardises on ``IAU_2015``. Results are therefore reprojected by default, and
:func:`features` says so through :class:`~planetarypy.crs.CRSConversionWarning`
unless you named a CRS yourself. Set :func:`~planetarypy.crs.set_target_crs` to
put nomenclature, footprints and rasters in one frame for a whole session.

Requires geopandas: ``pip install "planetarypy[geo]"``.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

__all__ = ["features", "add_features", "download", "bodies", "BODIES"]

BASE = "https://asc-planetarynames-data.s3.us-west-2.amazonaws.com"

#: Bodies the gazetteer publishes GIS downloads for (verified 2026-08-01).
BODIES = (
    "amalthea", "ariel", "bennu", "callisto", "ceres", "charon", "dactyl",
    "deimos", "didymos", "dimorphos", "dinkinesh", "dione", "donaldjohanson",
    "enceladus", "epimetheus", "eros", "europa", "ganymede", "gaspra",
    "hyperion", "iapetus", "ida", "io", "itokawa", "janus", "lutetia", "mars",
    "mathilde", "mercury", "mimas", "miranda", "moon", "nix", "oberon",
    "phobos", "phoebe", "pluto", "proteus", "puck", "rhea", "ryugu", "steins",
    "tethys", "thebe", "titan", "titania", "triton", "umbriel", "venus", "vesta",
)

#: Where planetarypy's body spelling differs from the gazetteer's file naming.
_BODY_ALIASES = {"luna": "MOON"}


def bodies() -> tuple[str, ...]:
    """Bodies with published nomenclature, lowercase."""
    return BODIES


def _gazetteer_name(body) -> str:
    """The gazetteer's file-name token for ``body``.

    File names are uppercase — ``MARS`` returns 200, ``Mars`` returns 404 — so
    the casing is not cosmetic.
    """
    key = str(body).strip().lower()
    return _BODY_ALIASES.get(key, key.upper())


def _cache_dir(body) -> Path:
    from planetarypy.config import config

    return Path(config.storage_root) / "nomenclature" / _gazetteer_name(body)


def download(body, *, refresh: bool = False) -> Path:
    """Fetch and unpack one body's nomenclature shapefile; return the ``.shp``.

    Cached under ``storage_root/nomenclature/<BODY>/``. The gazetteer grows as
    the IAU adopts names, so ``refresh=True`` re-fetches.
    """
    import urllib.error
    import urllib.request

    name = _gazetteer_name(body)
    dest = _cache_dir(body)
    shp = dest / f"{name}_nomenclature_center_pts.shp"
    if shp.exists() and not refresh:
        return shp

    url = f"{BASE}/{name}_nomenclature_center_pts.zip"
    dest.mkdir(parents=True, exist_ok=True)
    zip_path = dest / "nomenclature.zip"
    try:
        urllib.request.urlretrieve(url, zip_path)
    except urllib.error.HTTPError as exc:
        raise ValueError(
            f"No published nomenclature for {body!r} ({url} -> HTTP {exc.code}). "
            f"Known bodies: {', '.join(BODIES)}"
        ) from exc
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(dest)
    zip_path.unlink(missing_ok=True)
    if not shp.exists():
        raise FileNotFoundError(f"{url} unpacked without the expected {shp.name}")
    return shp


def features(
    body,
    *,
    type: str | None = None,
    min_diameter: float | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    to_crs=None,
    refresh: bool = False,
):
    """IAU-adopted named features for ``body``, as a GeoDataFrame.

    Parameters
    ----------
    body : str
        Body name, e.g. ``"mars"``. See :func:`bodies`.
    type : str, optional
        Feature class, matched against the leading singular of the gazetteer's
        ``type`` column — pass ``"Crater"``, not ``"Crater, craters"``.
    min_diameter : float, optional
        Keep features at least this many km across. The gazetteer will happily
        return 1237 Mars craters; diameter is the usual way to mean "the ones
        worth naming on this plot".
    bbox : tuple, optional
        ``(west, south, east, north)`` in degrees east / planetocentric north,
        the same convention as :func:`planetarypy.search.bbox_from_point`.
    to_crs : optional
        Reproject to this CRS. Overrides the session target CRS.
    refresh : bool
        Re-download rather than using the cache.

    Returns
    -------
    geopandas.GeoDataFrame
        Columns include ``name``, ``type``, ``diameter`` (km),
        ``center_lon``/``center_lat`` and the feature's bounding box as
        ``min_lon``/``max_lon``/``min_lat``/``max_lat``.

    Notes
    -----
    CRS precedence is ``to_crs`` > session target > the body's own IAU CRS.
    Reprojection you did not explicitly request warns via
    :class:`~planetarypy.crs.CRSConversionWarning` — the gazetteer's ESRI codes
    and planetarypy's IAU codes differ in more than their label, so the
    conversion is worth knowing about.
    """
    import geopandas as gpd

    from planetarypy import crs as pcrs

    gdf = gpd.read_file(download(body, refresh=refresh))
    shipped = gdf.crs

    if type is not None:
        leading = gdf["type"].str.split(",").str[0].str.casefold()
        gdf = gdf[leading == type.casefold()]
    if min_diameter is not None:
        gdf = gdf[gdf["diameter"] >= min_diameter]
    if bbox is not None:
        west, south, east, north = bbox
        gdf = gdf[
            gdf["center_lon"].between(west, east)
            & gdf["center_lat"].between(south, north)
        ]

    target = pcrs.resolve_crs(to_crs, fallback=pcrs.body_crs(body))
    if target is not None and shipped is not None and shipped != target:
        if to_crs is None:  # not asked for -> say so
            pcrs.announce_conversion(shipped, target, what=f"{body} nomenclature")
        gdf = gdf.to_crs(target)
    return gdf.reset_index(drop=True)


def _place_labels(ax, rows, text_kw, max_labels):
    """Draw labels largest-first, skipping any that would collide.

    Overlapping text is the difference between a legible map and a smear. Greedy
    rather than optimal: place the biggest feature's label, then keep a later one
    only if its box misses everything already placed.

    Falls back to placing every label if the renderer is unavailable (some
    backends before the first draw), because a crowded plot beats a crash.
    """
    placed = []
    try:
        renderer = ax.figure.canvas.get_renderer()
    except Exception:  # pragma: no cover - backend dependent
        renderer = None

    drawn = 0
    for _, row in rows.iterrows():
        if drawn >= max_labels:
            break
        annotation = ax.annotate(
            row["name"],
            (row["center_lon"], row["center_lat"]),
            xytext=(4, 4),
            textcoords="offset points",
            **text_kw,
        )
        if renderer is None:
            drawn += 1
            continue
        box = annotation.get_window_extent(renderer=renderer)
        if any(box.overlaps(other) for other in placed):
            annotation.remove()
            continue
        placed.append(box)
        drawn += 1
    return drawn


def add_features(
    ax,
    body,
    *,
    type: str | None = None,
    min_diameter: float | None = None,
    bbox="auto",
    extent: bool = True,
    label: bool = True,
    max_labels: int = 25,
    declutter: bool = True,
    to_crs=None,
    marker_kw: dict | None = None,
    extent_kw: dict | None = None,
    text_kw: dict | None = None,
):
    """Overlay named features onto existing axes — the ``coastlines()`` move.

    Parameters
    ----------
    ax : matplotlib.axes.Axes
        Axes to draw on. Whatever is already plotted stays.
    bbox : "auto" or tuple
        ``"auto"`` (default) reads the axes' current limits, so this composes
        with a plot you already made without restating the area.
    extent : bool
        Draw each feature's lat/lon box from the gazetteer's ``min/max``
        columns. On by default: a centre point says a name is *near* here, the
        box says whether your footprint actually overlaps the feature. Set
        ``False`` for centre points only.
    label : bool
        Draw feature names.
    max_labels : int
        Cap on labels drawn, largest features first.
    declutter : bool
        Skip labels that would overlap one already placed.
    to_crs : optional
        Passed to :func:`features`; overrides the session target CRS.

    Returns
    -------
    geopandas.GeoDataFrame
        The features drawn, so you can inspect or annotate further.
    """
    if isinstance(bbox, str):
        if bbox != "auto":
            raise ValueError(f"bbox must be 'auto' or a 4-tuple, got {bbox!r}")
        x0, x1 = ax.get_xlim()
        y0, y1 = ax.get_ylim()
        bbox = (min(x0, x1), min(y0, y1), max(x0, x1), max(y0, y1))

    gdf = features(
        body, type=type, min_diameter=min_diameter, bbox=bbox, to_crs=to_crs
    )
    if gdf.empty:
        return gdf

    if extent:
        from matplotlib.patches import Rectangle

        style = {
            "fill": False, "ec": "#d95f02", "lw": 0.9,
            "ls": "--", "alpha": 0.55, "zorder": 4,
        }
        style.update(extent_kw or {})
        for _, row in gdf.iterrows():
            width = row["max_lon"] - row["min_lon"]
            height = row["max_lat"] - row["min_lat"]
            if width <= 0 or height <= 0:  # point-like feature, no real extent
                continue
            ax.add_patch(Rectangle((row["min_lon"], row["min_lat"]),
                                   width, height, **style))

    marker = {"s": 18, "c": "#d95f02", "marker": "o", "zorder": 5}
    marker.update(marker_kw or {})
    ax.scatter(gdf["center_lon"], gdf["center_lat"], **marker)

    if label:
        text = {"fontsize": 8, "color": "#d95f02", "zorder": 6,
                "ha": "left", "va": "bottom"}
        text.update(text_kw or {})
        ranked = gdf.nlargest(len(gdf), "diameter")
        if declutter:
            _place_labels(ax, ranked, text, max_labels)
        else:
            for _, row in ranked.head(max_labels).iterrows():
                ax.annotate(row["name"], (row["center_lon"], row["center_lat"]),
                            xytext=(4, 4), textcoords="offset points", **text)
    return gdf
