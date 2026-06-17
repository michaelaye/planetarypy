"""Tests for planetarypy.plotting.

The sun-indicator is experimental, but its placement has one hard contract worth
guarding: drawing it must NOT rescale the image (the bug fixed in the
instrument-extraction era — a data-coordinate corner glyph triggered matplotlib
autoscale and pushed the sun ball outside the frame).
"""

import warnings

import numpy as np
import pytest

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from planetarypy.plotting import add_sun_indicator, imshow_gray  # noqa: E402


@pytest.fixture
def gray_ax():
    img = np.random.default_rng(0).random((120, 200))
    ax = imshow_gray(img, stretch=None)
    yield ax
    plt.close(ax.figure)


@pytest.mark.parametrize("az", [0, 45, 90, 135, 180, 225, 270, 315])
@pytest.mark.parametrize(
    "position", ["upper right", "upper left", "lower right", "lower left"]
)
def test_sun_indicator_does_not_rescale_image(gray_ax, az, position):
    xlim0, ylim0 = gray_ax.get_xlim(), gray_ax.get_ylim()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")  # experimental UserWarning
        add_sun_indicator(gray_ax, az, position=position)
    assert gray_ax.get_xlim() == xlim0
    assert gray_ax.get_ylim() == ylim0


def test_sun_indicator_warns_experimental(gray_ax):
    import planetarypy.plotting as plotting

    plotting._SUN_INDICATOR_WARNED = False  # reset the one-shot guard
    with pytest.warns(UserWarning, match="experimental"):
        add_sun_indicator(gray_ax, 90)
