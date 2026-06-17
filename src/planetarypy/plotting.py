"""Plotting utilities for planetary image data.

Provides common visualization helpers for planetary science:
percentile stretching, grayscale image display, sun direction indicators.
"""

import warnings

import numpy as np

# One-shot guard so a multi-panel figure doesn't emit the experimental
# warning once per subplot.
_SUN_INDICATOR_WARNED = False


def percentile_stretch(image, lo=1, hi=99):
    """Compute display limits from percentiles, ignoring zeros and NaN.

    Parameters
    ----------
    image : array-like
        Image data (2D or nD).
    lo, hi : float
        Lower and upper percentile (default 1, 99).

    Returns
    -------
    vmin, vmax : float
        Display limits suitable for imshow(vmin=, vmax=).
        Returns (None, None) if no valid pixels.
    """
    arr = np.asarray(image, dtype=float)
    valid = arr[np.isfinite(arr) & (arr != 0)]
    if valid.size == 0:
        return None, None
    return np.percentile(valid, [lo, hi])


def imshow_gray(image, stretch="1,99", title=None, ax=None, **imshow_kwargs):
    """Display a grayscale image with optional percentile stretch.

    Parameters
    ----------
    image : 2D array
        Image data.
    stretch : str or None
        Percentile stretch as "lo,hi" (e.g. "1,99"). Use None or
        "none" to disable.
    title : str, optional
        Plot title.
    ax : matplotlib.axes.Axes, optional
        Axes to plot on. Created if not provided.
    **imshow_kwargs
        Passed to ax.imshow().

    Returns
    -------
    ax : matplotlib.axes.Axes
    """
    import matplotlib.pyplot as plt

    if ax is None:
        aspect = image.shape[0] / image.shape[1]
        fig, ax = plt.subplots(figsize=(8, 8 * aspect))

    kwargs = dict(cmap="gray", origin="upper")
    kwargs.update(imshow_kwargs)

    if stretch and str(stretch).lower() != "none":
        lo, hi = (float(x) for x in str(stretch).split(","))
        vmin, vmax = percentile_stretch(image, lo, hi)
        if vmin is not None:
            kwargs.setdefault("vmin", vmin)
            kwargs.setdefault("vmax", vmax)

    ax.imshow(image, **kwargs)
    if title:
        ax.set_title(title, fontsize=12)
    ax.set_axis_off()
    return ax


def add_sun_indicator(ax, sun_azimuth_deg, position="upper right",
                      length=0.12, color="yellow", inner_color="orange"):
    """Add a sun direction indicator to an image plot. **Experimental.**

    .. warning::

       Experimental. The azimuth-convention handoff is not fully validated:
       this function expects **clockwise-from-image-top** (the PDS
       ``SUB_SOLAR_AZIMUTH`` convention for *unprojected* images), but
       :meth:`planetarypy.spice.spicer.Spicer.solar_azimuth_at` returns
       **clockwise-from-north** (geographic). They agree only when image-north
       points up; otherwise you must rotate by the image's north azimuth before
       passing the value here. Placement and appearance may change.

    The indicator is a small compass glyph (sun ball + arrow) drawn in a corner,
    pointing toward the sun. It is rendered in **axes-fraction coordinates**, so
    it neither rescales the image nor depends on the image's ``origin``.

    Parameters
    ----------
    ax : matplotlib.axes.Axes
        The axes containing the image.
    sun_azimuth_deg : float
        Solar azimuth in degrees, clockwise from image top (see warning).
    position : str
        "upper right", "upper left", "lower right", "lower left".
    length : float
        Arrow length as a fraction of the axes (default 0.12).
    color : str
        Outer circle and arrow color.
    inner_color : str
        Inner circle color (smaller, overlaid on outer).

    Returns
    -------
    ax : matplotlib.axes.Axes
    """
    global _SUN_INDICATOR_WARNED
    if not _SUN_INDICATOR_WARNED:
        warnings.warn(
            "add_sun_indicator is experimental: the azimuth convention "
            "(clockwise-from-top vs Spicer's clockwise-from-north) is not fully "
            "validated, and placement/appearance may change.",
            UserWarning,
            stacklevel=2,
        )
        _SUN_INDICATOR_WARNED = True

    L = float(length)
    # Inset the anchor by at least the arrow length so the glyph stays inside the
    # axes for *any* azimuth. Axes-fraction coords (y up, independent of image
    # origin); drawing here never touches the data limits, so the image is not
    # rescaled — the bug the old data-coordinate corner placement had.
    inset = L + 0.06
    corners = {
        "upper right": (1 - inset, 1 - inset),
        "upper left":  (inset,     1 - inset),
        "lower right": (1 - inset, inset),
        "lower left":  (inset,     inset),
    }
    cx, cy = corners.get(position, corners["upper right"])

    az = np.radians(sun_azimuth_deg)
    # Clockwise from top in axes-fraction space (y increases upward):
    # 0deg -> up (+y), 90deg -> right (+x).
    dx = L * np.sin(az)
    dy = L * np.cos(az)
    sx, sy = cx + dx, cy + dy

    # Stop the arrow just short of the sun ball.
    shrink = 0.12 * L
    norm = np.hypot(dx, dy) or 1.0
    tipx, tipy = sx - dx / norm * shrink, sy - dy / norm * shrink

    ax.annotate("",
                xy=(tipx, tipy), xytext=(cx, cy),
                xycoords="axes fraction", textcoords="axes fraction",
                arrowprops=dict(arrowstyle="-|>", color=color, lw=2.5,
                                mutation_scale=20),
                zorder=5, annotation_clip=False)
    ax.plot(sx, sy, "o", color=color, markersize=14, zorder=8,
            transform=ax.transAxes, clip_on=False)
    ax.plot(sx, sy, "o", color=inner_color, markersize=9, zorder=9,
            transform=ax.transAxes, clip_on=False)
    return ax


def imshow_with_sun(image, sun_azimuth_deg, title=None, ax=None,
                    stretch="1,99", sun_position="upper right", **imshow_kwargs):
    """Display a grayscale image with a sun direction indicator. **Experimental.**

    Combines `imshow_gray` with `add_sun_indicator` (see that function's
    experimental warning about the azimuth convention).

    Parameters
    ----------
    image : 2D array
        Image data.
    sun_azimuth_deg : float
        Solar azimuth in degrees, clockwise from image top.
    title : str, optional
        Plot title.
    ax : matplotlib.axes.Axes, optional
        Axes to plot on. Created if not provided.
    stretch : str or None
        Percentile stretch (default "1,99"). None to disable.
    sun_position : str
        Indicator position: "upper right", "upper left", etc.
    **imshow_kwargs
        Passed to ax.imshow().

    Returns
    -------
    ax : matplotlib.axes.Axes
    """
    ax = imshow_gray(image, stretch=stretch, title=title, ax=ax, **imshow_kwargs)
    add_sun_indicator(ax, sun_azimuth_deg, position=sun_position)
    return ax
