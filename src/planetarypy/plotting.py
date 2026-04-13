"""Plotting utilities for planetary image data.

Provides common visualization helpers for planetary science:
percentile stretching, grayscale image display, sun direction indicators.
"""

import numpy as np


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
    """Add a sun direction indicator to an image plot.

    The indicator shows a circle (sun) with a line pointing in the
    direction of illumination. The azimuth is measured clockwise from
    the top of the image.

    Parameters
    ----------
    ax : matplotlib.axes.Axes
        The axes containing the image.
    sun_azimuth_deg : float
        Solar azimuth in degrees, clockwise from image top.
        This matches the PDS `SUB_SOLAR_AZIMUTH` convention for
        unprojected images.
    position : str
        Where to place the indicator: "upper right", "upper left",
        "lower right", "lower left".
    length : float
        Line length as fraction of image size (default 0.12).
    color : str
        Outer circle and line color.
    inner_color : str
        Inner circle color (smaller, overlaid on outer).

    Returns
    -------
    ax : matplotlib.axes.Axes
    """
    xlim = ax.get_xlim()
    ylim = ax.get_ylim()
    # Image extent (works for both origin='upper' and data coords)
    width = abs(xlim[1] - xlim[0])
    height = abs(ylim[1] - ylim[0])

    # Position the indicator
    margin = 0.08
    positions = {
        "upper right": (xlim[0] + width * (1 - margin), min(ylim) + height * margin),
        "upper left":  (xlim[0] + width * margin, min(ylim) + height * margin),
        "lower right": (xlim[0] + width * (1 - margin), min(ylim) + height * (1 - margin)),
        "lower left":  (xlim[0] + width * margin, min(ylim) + height * (1 - margin)),
    }
    cx, cy = positions.get(position, positions["upper right"])

    line_len = length * max(width, height)
    az_rad = np.radians(sun_azimuth_deg)
    # CW from top: dx = sin(az), dy = -cos(az) for origin='upper'
    dx = line_len * np.sin(az_rad)
    dy = -line_len * np.cos(az_rad)

    # Sun circle in the sun direction, arrow points outward toward sun
    sx, sy = cx + dx, cy + dy

    # Shorten arrow slightly so the tip doesn't overlap the sun ball
    shrink = 0.08 * line_len
    norm = np.hypot(dx, dy)
    ax_dx = dx / norm * shrink
    ax_dy = dy / norm * shrink

    ax.annotate("",
                xy=(sx - ax_dx, sy - ax_dy),  # arrow tip (gap before sun ball)
                xytext=(cx, cy),               # arrow tail (interior)
                arrowprops=dict(arrowstyle="-|>", color=color, lw=2.5,
                                mutation_scale=20),
                zorder=5)

    # Sun symbol just past the arrow tip
    ax.plot(sx, sy, "o", color=color, markersize=14, zorder=8)
    ax.plot(sx, sy, "o", color=inner_color, markersize=9, zorder=9)

    return ax


def imshow_with_sun(image, sun_azimuth_deg, title=None, ax=None,
                    stretch="1,99", sun_position="upper right", **imshow_kwargs):
    """Display a grayscale image with a sun direction indicator.

    Combines `imshow_gray` with `add_sun_indicator`.

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
