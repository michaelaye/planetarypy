import datetime as dt

import pytest

from planetarypy.spice.spicer import IllumAngles, MarsSpicer, Spicer, SurfaceCoords


def test_spicer_initialization():
    """Test basic Spicer initialization."""
    spicer = Spicer("MARS", time="2020-01-01T00:00:00")
    assert spicer.body == "MARS"
    assert isinstance(spicer.time, dt.datetime)


def test_illum_angles():
    """Test IllumAngles class."""
    angles = IllumAngles(phase=30, solar=45, emission=60)
    assert angles.dphase.value == pytest.approx(30)
    assert angles.dsolar.value == pytest.approx(45)
    assert angles.demission.value == pytest.approx(60)


def test_surface_coords():
    """Test SurfaceCoords class."""
    coords = SurfaceCoords(lon=180, lat=45, radius=3396.2)
    assert coords.dlon.value == pytest.approx(180)
    assert coords.dlat.value == pytest.approx(45)
    assert coords.radius.value == pytest.approx(3396.2)


def test_mars_spicer():
    """Test MarsSpicer specific functionality."""
    mars = MarsSpicer(time="2020-01-01T00:00:00")
    assert mars.target == "MARS"
    assert mars.ref_frame == "IAU_MARS"


@pytest.mark.parametrize(
    "time_str",
    [
        "2020-01-01T00:00:00",
        "2020-06-15T12:30:45",
    ],
)
def test_spicer_time_handling(time_str):
    """Test time handling in Spicer."""
    spicer = Spicer("MARS", time=time_str)
    assert isinstance(spicer.time, dt.datetime)
    assert spicer.utc == spicer.time.isoformat()
