import rioxarray as rxr

def read_image(p):
    """Read all cubes or geotiffs in a standardized way."""
    da = rxr.open_rasterio(p, mask_and_scale=True, chunks=True).isel(band=0, drop=True)
    # the following is required for hvplot automagically doing the right thing
    da.x.attrs["axis"] = "X"
    da.y.attrs["axis"] = "Y"
    return da
