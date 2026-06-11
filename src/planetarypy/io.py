"""Open planetary data products with one call.

:func:`open` inspects a file and returns the most useful in-memory object:

- **Projected rasters** (GeoTIFF, ISIS ``.cub``) come back as a
  georeferenced :class:`xarray.DataArray` (via :func:`rioxarray.open_rasterio`).
- **Everything else** (PDS3 ``.IMG``/``.LBL``, PDS4, FITS, …) comes back as a
  dict-like data handle whose objects you reach by key::

      >>> import planetarypy
      >>> d = planetarypy.open("P02_001916_2221_XI_42N027W.LBL")
      >>> d.keys()
      ['LABEL', 'IMAGE']
      >>> arr = d["IMAGE"]          # numpy array
      >>> d.metaget("LINES")        # label metadata

The routing is automatic but overridable with the ``projected`` keyword.
"""

from pathlib import Path

__all__ = ["open", "read", "read_image"]

# Suffixes routed to the projected-raster reader (rioxarray) rather than the
# generic reader. These are already-projected products where a CRS-aware
# xarray DataArray is the better return type. Override per-call with
# ``projected=True``/``False``.
_PROJECTED_SUFFIXES = {".tif", ".tiff", ".cub"}

# Fires only on a broken install (a core dependency was force-removed). The
# reader engine ships as a core dependency, so a normal install never sees
# this — hence the message points at reinstalling planetarypy, not at the
# engine package the user is not expected to know about.
_READER_MISSING_MSG = (
    "planetarypy.open() could not import its reader engine, which means a "
    "core dependency is missing from your environment. Reinstall planetarypy "
    'to restore it: `pip install -U planetarypy` (or `conda install -c '
    "conda-forge planetarypy`)."
)


def open(path, *, projected: bool | None = None, **kwargs):
    """Open a planetary data product and return it in memory.

    Parameters
    ----------
    path : str | os.PathLike
        Path to a local data product. For detached-label PDS3 products,
        pass the label (``.LBL``) — it is the correct entry point.
    projected : bool | None
        Routing control. ``None`` (default) auto-detects from the suffix:
        GeoTIFF / ``.cub`` → a georeferenced :class:`xarray.DataArray`;
        everything else → the generic multi-object reader. ``True`` forces
        the projected-raster path, ``False`` forces the generic reader.
    **kwargs
        Forwarded to the generic reader (ignored on the projected path).

    Returns
    -------
    xarray.DataArray
        For projected rasters.
    data handle
        For everything else: a dict-like object where ``handle.keys()``
        lists the contained objects, ``handle["IMAGE"]`` returns a numpy
        array, ``handle["TABLE"]`` a :class:`pandas.DataFrame`, and
        ``handle.metaget(key)`` reads label metadata.
    """
    path = Path(path)
    use_projected = (
        projected
        if projected is not None
        else path.suffix.lower() in _PROJECTED_SUFFIXES
    )
    if use_projected:
        return read_image(path)
    try:
        import pdr
    except ImportError as exc:  # pragma: no cover - only on a broken install
        raise ImportError(_READER_MISSING_MSG) from exc
    return pdr.read(str(path), **kwargs)


def read(path, *, projected: bool | None = None, **kwargs):
    """Alias for :func:`open` (``pandas.read_*`` muscle memory)."""
    return open(path, projected=projected, **kwargs)


def read_image(p):
    """Read a projected raster (GeoTIFF / ISIS cube) as an ``xarray.DataArray``.

    Returns a masked, dask-backed, georeferenced array with ``x``/``y`` axis
    attributes set (so ``.hvplot`` does the right thing automatically).
    """
    import rioxarray as rxr

    da = rxr.open_rasterio(p, mask_and_scale=True, chunks=True).isel(
        band=0, drop=True
    )
    da.x.attrs["axis"] = "X"
    da.y.attrs["axis"] = "Y"
    return da
