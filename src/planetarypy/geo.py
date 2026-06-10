"""Geospatial utilities for planetary image data.

Coordinate transforms between pixel, map-projected, and geographic
coordinate systems, built on rasterio and pyproj. No GDAL required.

Works with rioxarray DataArrays (the standard way to open planetary
images in planetarypy) or standalone with affine transforms and CRS.
Supports IAU planetary CRS codes for Mars, Moon, and all solar system
bodies.

Functions for direct use:
    pixel_to_xy, xy_to_pixel, pixel_to_lonlat, lonlat_to_pixel,
    xy_to_lonlat, lonlat_to_xy, is_within, image_azimuth,
    image_azimuth_cw_from_right, pixel_resolution

Projected-raster geometry (GDAL-native, ISIS-free, format-agnostic):
    is_projected, raster_footprint, footprints_to_gdf, overlaps

Point class:
    Combines lon/lat, pixel, and projected coordinates in one object
    with CRS awareness. Handles transforms, bounds checking, azimuths,
    and Shapely interop — filling a gap that Shapely (no CRS/pixels),
    pyproj (no point object), and rasterio (no lon/lat) each leave open.

Examples
--------
>>> from planetarypy.geo import Point, pixel_to_lonlat
>>> p = Point(lon=137.85, lat=-5.08, crs="IAU_2015:49900")
>>> p.is_within(da)
True
>>> p.to_shapely()
<POINT (137.85 -5.08)>
"""

from contextlib import contextmanager

import numpy as np
from rasterio.transform import AffineTransformer


def pixel_to_xy(transform, sample, line):
    """Convert pixel coordinates to map-projected (x, y) coordinates.

    Parameters
    ----------
    transform : affine.Affine or rasterio.transform.Affine
        The affine geotransform (from rasterio dataset or rioxarray).
    sample : float or array-like
        Pixel column(s) (0-based).
    line : float or array-like
        Pixel row(s) (0-based).

    Returns
    -------
    x, y : float or np.ndarray
        Map-projected coordinates.
    """
    transformer = AffineTransformer(transform)
    return transformer.xy(line, sample)


def xy_to_pixel(transform, x, y):
    """Convert map-projected (x, y) coordinates to pixel coordinates.

    Parameters
    ----------
    transform : affine.Affine
        The affine geotransform.
    x, y : float or array-like
        Map-projected coordinates.

    Returns
    -------
    sample, line : float or np.ndarray
        Pixel column(s) and row(s) (0-based, fractional).
    """
    transformer = AffineTransformer(transform)
    line, sample = transformer.rowcol(x, y)
    return sample, line


def pixel_to_lonlat(source, sample, line):
    """Convert pixel coordinates to geographic (lon, lat).

    Parameters
    ----------
    source : xarray.DataArray or rasterio.DatasetReader
        Must have a CRS and transform (e.g. opened via rioxarray).
    sample : float or array-like
        Pixel column(s).
    line : float or array-like
        Pixel row(s).

    Returns
    -------
    lon, lat : float or np.ndarray
        Geographic coordinates in degrees.
    """
    transform, crs = _get_transform_and_crs(source)
    x, y = pixel_to_xy(transform, sample, line)
    return xy_to_lonlat(crs, x, y)


def lonlat_to_pixel(source, lon, lat):
    """Convert geographic (lon, lat) to pixel coordinates.

    Parameters
    ----------
    source : xarray.DataArray or rasterio.DatasetReader
        Must have a CRS and transform.
    lon, lat : float or array-like
        Geographic coordinates in degrees.

    Returns
    -------
    sample, line : float or np.ndarray
        Pixel column(s) and row(s).
    """
    transform, crs = _get_transform_and_crs(source)
    x, y = lonlat_to_xy(crs, lon, lat)
    return xy_to_pixel(transform, x, y)


def xy_to_lonlat(crs, x, y):
    """Convert map-projected coordinates to geographic (lon, lat).

    Works for both Earth (EPSG) and planetary CRS (IAU codes, proj4 strings).

    Parameters
    ----------
    crs : rasterio.crs.CRS or pyproj.CRS
        The coordinate reference system of the projected data.
    x, y : float or array-like
        Projected coordinates.

    Returns
    -------
    lon, lat : float or np.ndarray
        Geographic coordinates in degrees.
    """
    from pyproj import CRS as ProjCRS, Transformer

    proj_crs = ProjCRS(crs)
    geo_crs = proj_crs.geodetic_crs
    proj = Transformer.from_crs(proj_crs, geo_crs, always_xy=True)
    lon, lat = proj.transform(x, y)
    return lon, lat


def lonlat_to_xy(crs, lon, lat):
    """Convert geographic (lon, lat) to map-projected coordinates.

    Works for both Earth (EPSG) and planetary CRS (IAU codes, proj4 strings).

    Parameters
    ----------
    crs : rasterio.crs.CRS or pyproj.CRS
        The target coordinate reference system.
    lon, lat : float or array-like
        Geographic coordinates in degrees.

    Returns
    -------
    x, y : float or np.ndarray
        Projected coordinates.
    """
    from pyproj import CRS as ProjCRS, Transformer

    proj_crs = ProjCRS(crs)
    geo_crs = proj_crs.geodetic_crs
    proj = Transformer.from_crs(geo_crs, proj_crs, always_xy=True)
    x, y = proj.transform(lon, lat)
    return x, y


def is_within(source, lon, lat):
    """Check if geographic coordinates fall within an image.

    Parameters
    ----------
    source : xarray.DataArray or rasterio.DatasetReader
        Must have a CRS and transform.
    lon, lat : float or array-like
        Geographic coordinates in degrees.

    Returns
    -------
    bool or np.ndarray of bool
        True if the point(s) are within the image bounds.
    """
    s, l = lonlat_to_pixel(source, lon, lat)
    # Ensure plain numeric types (not xarray-wrapped)
    s = np.asarray(s)
    l = np.asarray(l)

    if hasattr(source, "rio"):
        ny, nx = source.rio.height, source.rio.width
    else:
        ny, nx = source.height, source.width

    return (0 <= s) & (s < nx) & (0 <= l) & (l < ny)


def image_azimuth(sample1, line1, sample2, line2):
    """Calculate azimuth angle between two image points.

    Returns the clockwise angle from image-north (up) in degrees.
    This matches the convention for images with origin at upper-left
    (standard for planetary data).

    Parameters
    ----------
    sample1, line1 : float
        Origin point (column, row).
    sample2, line2 : float
        Target point (column, row).

    Returns
    -------
    azimuth : float
        Angle in degrees [0, 360), clockwise from up.
    """
    ds = sample2 - sample1
    dl = line2 - line1
    # In image coords, line increases downward. For clockwise-from-north
    # (up), we negate dl so that "up" (decreasing line) maps to 0 degrees.
    az = np.degrees(np.arctan2(ds, -dl))
    return az % 360.0


def image_azimuth_cw_from_right(sample1, line1, sample2, line2):
    """Calculate azimuth angle clockwise from right (3 o'clock).

    This is the convention used by HiRISE, where angles are measured
    clockwise from the +sample direction (right/east in image space).
    The y-axis is flipped (line increases downward), so clockwise in
    image space matches the trigonometric direction.

    Parameters
    ----------
    sample1, line1 : float
        Origin point (column, row).
    sample2, line2 : float
        Target point (column, row).

    Returns
    -------
    azimuth : float
        Angle in degrees [0, 360), clockwise from right.
    """
    ds = sample2 - sample1
    dl = line2 - line1
    az = np.degrees(np.arctan2(dl, ds))
    return az % 360.0


def pixel_resolution(transform):
    """Return the pixel resolution in map units.

    Parameters
    ----------
    transform : affine.Affine
        The affine geotransform.

    Returns
    -------
    res_x, res_y : float
        Pixel size in x and y (map units, typically meters).
        res_y is returned as a positive value.
    """
    return abs(transform.a), abs(transform.e)


# ── Point class ──────────────────────────────────────────────────────


class Point:
    """A geographic point on a planetary body.

    Can be created from lon/lat, projected (x, y), or pixel coordinates.
    Converts lazily between coordinate systems as needed.

    Parameters
    ----------
    lon, lat : float, optional
        Geographic coordinates in degrees.
    x, y : float, optional
        Map-projected coordinates.
    sample, line : float, optional
        Pixel coordinates.
    crs : str or pyproj.CRS, optional
        CRS for projected/geographic coordinates (e.g. "IAU_2015:49900").
    source : xarray.DataArray or rasterio.DatasetReader, optional
        Image source — provides CRS and transform for pixel conversions.

    Examples
    --------
    >>> p = Point(lon=137.85, lat=-5.08, crs="IAU_2015:49900")
    >>> p.to_xy("IAU_2015:49910")
    (8167439.5, -301670.8)

    >>> p = Point(sample=250, line=250, source=da)
    >>> p.lon, p.lat
    (137.4, -4.6)
    """

    def __init__(self, *, lon=None, lat=None, x=None, y=None,
                 sample=None, line=None, crs=None, source=None):
        self._lon = lon
        self._lat = lat
        self._x = x
        self._y = y
        self._sample = sample
        self._line = line
        self._source = source

        if crs is not None:
            from pyproj import CRS as ProjCRS
            self._crs = ProjCRS(crs)
        elif source is not None:
            _, src_crs = _get_transform_and_crs(source)
            from pyproj import CRS as ProjCRS
            self._crs = ProjCRS(src_crs)
        else:
            self._crs = None

        # Auto-resolve from pixel if source is given
        if source is not None and sample is not None and line is not None:
            self._lon, self._lat = pixel_to_lonlat(source, sample, line)

        # Auto-resolve from lon/lat to pixel if source is given
        if source is not None and lon is not None and lat is not None and sample is None:
            self._sample, self._line = lonlat_to_pixel(source, lon, lat)

    @property
    def lon(self):
        return self._lon

    @property
    def lat(self):
        return self._lat

    @property
    def sample(self):
        return self._sample

    @property
    def line(self):
        return self._line

    @property
    def crs(self):
        return self._crs

    def to_xy(self, target_crs=None):
        """Convert to map-projected coordinates.

        Parameters
        ----------
        target_crs : str or CRS, optional
            Target projected CRS. If None, uses the point's own CRS.

        Returns
        -------
        x, y : float
        """
        if self._lon is None:
            raise ValueError("Point has no geographic coordinates")
        crs = target_crs if target_crs is not None else self._crs
        if crs is None:
            raise ValueError("No CRS specified")
        return lonlat_to_xy(crs, self._lon, self._lat)

    def to_pixel(self, source=None):
        """Convert to pixel coordinates in an image.

        Parameters
        ----------
        source : xarray.DataArray or rasterio.DatasetReader, optional
            Image to project into. Uses the source from __init__ if not given.

        Returns
        -------
        sample, line : float
        """
        src = source if source is not None else self._source
        if src is None:
            raise ValueError("No image source specified")
        if self._lon is None:
            raise ValueError("Point has no geographic coordinates")
        return lonlat_to_pixel(src, self._lon, self._lat)

    def is_within(self, source=None):
        """Check if this point falls within an image.

        Parameters
        ----------
        source : xarray.DataArray or rasterio.DatasetReader, optional
            Image to check against. Uses the source from __init__ if not given.

        Returns
        -------
        bool
        """
        src = source if source is not None else self._source
        if src is None:
            raise ValueError("No image source specified")
        if self._lon is None:
            raise ValueError("Point has no geographic coordinates")
        return bool(is_within(src, self._lon, self._lat))

    def azimuth_to(self, other):
        """Azimuth angle from this point to another, clockwise from north.

        Both points must have pixel coordinates (via source).

        Parameters
        ----------
        other : Point

        Returns
        -------
        float
            Degrees, clockwise from north.
        """
        if self._sample is None or other._sample is None:
            raise ValueError("Both points need pixel coordinates (provide a source)")
        return image_azimuth(self._sample, self._line,
                             other._sample, other._line)

    def to_shapely(self):
        """Convert to a Shapely Point (lon, lat).

        Returns
        -------
        shapely.geometry.Point
        """
        if self._lon is None:
            raise ValueError("Point has no geographic coordinates")
        from shapely.geometry import Point as ShapelyPoint
        return ShapelyPoint(self._lon, self._lat)

    def __repr__(self):
        parts = []
        if self._lon is not None:
            parts.append(f"lon={self._lon:.4f}, lat={self._lat:.4f}")
        if self._sample is not None:
            parts.append(f"sample={self._sample:.0f}, line={self._line:.0f}")
        if self._crs is not None:
            parts.append(f"crs='{self._crs.name}'")
        return f"Point({', '.join(parts)})"


# ── Internal helpers ─────────────────────────────────────────────────


def _get_transform_and_crs(source):
    """Extract affine transform and CRS from a rioxarray DataArray or rasterio dataset."""
    # rioxarray DataArray
    if hasattr(source, "rio"):
        transform = source.rio.transform()
        crs = source.rio.crs
        if crs is None:
            raise ValueError("DataArray has no CRS. Open with rioxarray or set it via .rio.write_crs()")
        return transform, crs

    # rasterio DatasetReader
    if hasattr(source, "transform") and hasattr(source, "crs"):
        return source.transform, source.crs

    raise TypeError(
        f"Expected a rioxarray DataArray or rasterio DatasetReader, got {type(source)}"
    )


# ── Projected-raster geometry (GDAL-native, format-agnostic) ────────────
#
# These read CRS/footprint/overlaps straight from the raster via rasterio,
# so they work on any GDAL-readable file — ISIS .cub today, GeoTIFF after
# ISIS v10's move to native GDAL formats — with no ISIS dependency.


@contextmanager
def _as_rasterio_dataset(source):
    """Yield a rasterio dataset from a file path or an already-open dataset.

    Closes the dataset on exit only when this function opened it.
    """
    import os

    import rasterio

    if hasattr(source, "read") and hasattr(source, "dataset_mask"):
        yield source  # already an open rasterio DatasetReader
    elif hasattr(source, "rio"):
        raise TypeError(
            "Pass a file path or rasterio dataset, not a rioxarray DataArray "
            "(footprint extraction reads the data mask from the file)."
        )
    else:
        with rasterio.open(os.fspath(source)) as ds:
            yield ds


def is_projected(source) -> bool:
    """Return True if the raster's CRS is projected (not geographic).

    Accepts a file path, an open rasterio dataset, or a rioxarray DataArray.
    Format-agnostic (ISIS ``.cub``, GeoTIFF, …).
    """
    if hasattr(source, "rio") or (
        hasattr(source, "transform") and hasattr(source, "crs")
    ):
        _, crs = _get_transform_and_crs(source)
    else:
        with _as_rasterio_dataset(source) as ds:
            crs = ds.crs
    if crs is None:
        raise ValueError("Raster has no CRS; cannot determine projection.")
    return bool(crs.is_projected)


def raster_footprint(source, *, simplify=None):
    """Valid-data footprint of a raster as a shapely (Multi)Polygon.

    Built from the dataset mask, so nodata borders — common on
    map-projected products — are excluded: the result is the actual data
    outline, not the bounding box. The polygon is in the raster's own CRS.
    Returns ``None`` for an all-nodata raster.

    Parameters
    ----------
    source : path | rasterio dataset
    simplify : float, optional
        Douglas-Peucker tolerance (CRS units) to thin the outline.
    """
    from rasterio.features import shapes
    from shapely.geometry import shape
    from shapely.ops import unary_union

    with _as_rasterio_dataset(source) as ds:
        mask = ds.dataset_mask()
        geoms = [
            shape(geom)
            for geom, val in shapes(mask, mask=mask > 0, transform=ds.transform)
            if val > 0
        ]
    if not geoms:
        return None
    footprint = unary_union(geoms)
    if simplify:
        footprint = footprint.simplify(simplify)
    return footprint


def footprints_to_gdf(sources, *, id_fn=None, simplify=None):
    """Assemble valid-data footprints of many rasters into a GeoDataFrame.

    One row per source with an ``id`` and a ``geometry`` column. All sources
    must share a single CRS (raises otherwise); the GeoDataFrame carries it.

    ``id_fn`` maps a source to its identifier. The default uses the file
    name *with extension* (e.g. ``ESP_075205_0930_RED5_0.cub``) — generic,
    instrument-free, and lossless: keeping the extension distinguishes the
    same product in different formats (e.g. a ``.cub`` and its ``.tif``
    conversion, whose footprints may differ). Note that neither the name nor
    its stem is the PDS *product id* (filenames carry processing suffixes);
    pass your own ``id_fn`` for a domain key, or ``id_fn=str`` for full paths.

    Raises if the resulting ids aren't unique — duplicate ids would silently
    corrupt downstream id-keyed operations like :func:`overlaps`.

    Requires geopandas (the ``[isis]`` extra).
    """
    import os
    from collections import Counter

    import geopandas as gpd
    import rasterio

    if id_fn is None:
        def id_fn(s):
            from pathlib import Path
            return Path(os.fspath(s)).name

    ids, geoms, crs = [], [], None
    for src in sources:
        with rasterio.open(os.fspath(src)) as ds:
            if crs is None:
                crs = ds.crs
            elif ds.crs != crs:
                raise ValueError(
                    "footprints_to_gdf requires all rasters in one CRS; got "
                    f"{ds.crs} vs {crs}. Reproject before assembling."
                )
            geoms.append(raster_footprint(ds, simplify=simplify))
        ids.append(id_fn(src))

    dupes = sorted(i for i, n in Counter(ids).items() if n > 1)
    if dupes:
        raise ValueError(
            f"footprints_to_gdf produced duplicate ids {dupes}. Pass id_fn "
            "to disambiguate (e.g. id_fn=str for full paths)."
        )
    return gpd.GeoDataFrame({"id": ids, "geometry": geoms}, crs=crs)


def overlaps(gdf):
    """Pairwise overlaps (positive area) between footprints in ``gdf``.

    Takes a GeoDataFrame with ``id`` + ``geometry`` (as built by
    :func:`footprints_to_gdf`, whose ``id`` values must be unique) and
    returns one with ``id_1``, ``id_2``,
    ``geometry`` (the intersection) and ``area`` (CRS units) per unordered
    intersecting pair. Edge-only touches (zero area) are dropped.

    Requires geopandas (the ``[isis]`` extra).
    """
    import geopandas as gpd

    joined = gpd.sjoin(gdf, gdf, predicate="intersects")
    pairs = joined[joined["id_left"] < joined["id_right"]]
    by_id = dict(zip(gdf["id"], gdf["geometry"]))
    rows = []
    for left, right in zip(pairs["id_left"], pairs["id_right"]):
        inter = by_id[left].intersection(by_id[right])
        if inter.area > 0:
            rows.append(
                {"id_1": left, "id_2": right,
                 "geometry": inter, "area": inter.area}
            )
    return gpd.GeoDataFrame(rows, columns=["id_1", "id_2", "geometry", "area"],
                            crs=gdf.crs)
