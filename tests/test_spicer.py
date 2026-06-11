"""Tests for the Spicer illumination calculator."""

import numpy as np
import pytest

spiceypy = pytest.importorskip("spiceypy")

from planetarypy.spice.spicer import Spicer, _rotate_vector  # noqa: E402


class TestSpicerBasics:
    def test_create(self):
        s = Spicer("MARS")
        assert s.body == "MARS"
        assert s.ref_frame == "IAU_MARS"

    def test_radii(self):
        s = Spicer("MARS")
        assert s.radii.a == pytest.approx(3396.19, abs=1)
        assert s.radii.c == pytest.approx(3376.2, abs=1)

    def test_case_insensitive(self):
        s = Spicer("mars")
        assert s.body == "MARS"

    def test_moon(self):
        s = Spicer("MOON")
        assert s.radii.a == pytest.approx(1737.4, abs=1)


class TestSolarLongitude:
    def test_returns_float(self):
        s = Spicer("MARS")
        ls = s.solar_longitude("2024-01-01")
        assert isinstance(ls, float)
        assert 0 <= ls < 360

    def test_units(self):
        s = Spicer("MARS", units=True)
        ls = s.solar_longitude("2024-01-01")
        assert hasattr(ls, "unit")


class TestIllumination:
    def test_basic(self):
        s = Spicer("MARS")
        illum = s.illumination(lon=0, lat=0, time="2024-06-15T12:00:00")
        assert 0 <= illum.solar_incidence <= 180
        assert illum.local_solar_time is not None

    def test_nighttime_zero_flux(self):
        s = Spicer("MARS")
        # Try multiple longitudes — at least one should be in darkness
        for lon in range(0, 360, 90):
            illum = s.illumination(lon=lon, lat=0, time="2024-06-15T12:00:00")
            if illum.solar_incidence > 90:
                assert illum.solar_flux == 0.0
                return
        pytest.skip("No nighttime point found at test time")

    def test_no_observer_nan(self):
        s = Spicer("MARS")
        illum = s.illumination(lon=0, lat=0, time="2024-06-15T12:00:00")
        assert np.isnan(illum.emission)
        assert np.isnan(illum.phase)

    def test_slope_south_facing_more_flux(self):
        """During southern summer, south-facing slopes at Gale should get more flux."""
        s = Spicer("MARS")
        time = "2024-06-15T00:00:00"  # L_s ≈ 275 (southern summer)
        flat = s.illumination(lon=137.4, lat=-4.6, time=time)
        south = s.illumination(lon=137.4, lat=-4.6, time=time, slope=30, aspect=180)
        north = s.illumination(lon=137.4, lat=-4.6, time=time, slope=30, aspect=0)
        assert south.solar_flux > flat.solar_flux
        assert north.solar_flux < flat.solar_flux

    def test_units_mode(self):
        s = Spicer("MARS", units=True)
        illum = s.illumination(lon=0, lat=0, time="2024-06-15T12:00:00")
        assert hasattr(illum.solar_incidence, "unit")
        assert hasattr(illum.l_s, "unit")


class TestSolarAzimuth:
    """Validate SPICE solar azimuth against recorded HiRISE RDR values.

    The reference rows are immutable historical values captured once from the
    HiRISE RDR cumulative index — image-center lon/lat (mean of the four
    corners), START_TIME, and SUB_SOLAR_AZIMUTH — embedded here so the test
    needs no ~200 MB index download. SUB_SOLAR_AZIMUTH is HiRISE
    CW-from-3-o'clock; ``+ 90`` converts to CW-from-north (geographic).
    """

    # pid, center_lon, center_lat, start_time, sub_solar_azimuth
    REFS = [
        ("ESP_013807_2035_RED", 41.0046, 23.383150, "2009-07-07 08:39:01", 129.324),
        ("PSP_001414_2165_RED", 351.1485, 36.104525, "2006-11-14 15:51:49", 171.569),
        ("PSP_003630_1715_RED", 345.837, -8.607100, "2007-05-06 07:12:58", 161.684),
    ]

    @pytest.fixture()
    def mars(self):
        return Spicer("MARS")

    @pytest.mark.parametrize("pid,lon,lat,start_time,sub_solar_az", REFS)
    def test_matches_rdr_index(self, mars, pid, lon, lat, start_time, sub_solar_az):
        spice_az = mars.solar_azimuth_at(lon, lat, time=start_time)
        hirise_geographic = (sub_solar_az + 90) % 360
        diff = abs(spice_az - hirise_geographic)
        if diff > 180:
            diff = 360 - diff
        assert diff < 5.0, (
            f"{pid}: SPICE={spice_az:.1f}° vs index={hirise_geographic:.1f}°, "
            f"Δ={diff:.1f}°"
        )


class TestRotateVector:
    def test_90_deg_around_z(self):
        result = _rotate_vector([1, 0, 0], [0, 0, 1], np.pi / 2)
        assert result == pytest.approx([0, 1, 0], abs=1e-10)

    def test_identity(self):
        result = _rotate_vector([1, 2, 3], [0, 0, 1], 0)
        assert result == pytest.approx([1, 2, 3], abs=1e-10)


class TestPointIntegration:
    def test_illumination_at(self):
        from planetarypy.geo import Point
        s = Spicer("MARS")
        p = Point(lon=0, lat=0, crs="IAU_2015:49900")
        illum = s.illumination_at(p, time="2024-06-15T12:00:00")
        assert 0 <= illum.solar_incidence <= 180
