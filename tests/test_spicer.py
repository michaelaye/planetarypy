"""Tests for the Spicer illumination calculator."""

import numpy as np
import pytest

from planetarypy.spice.spicer import Spicer, _rotate_vector


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
    """Validate SPICE solar azimuth against HiRISE RDR index values."""

    @pytest.fixture()
    def mars(self):
        return Spicer("MARS")

    @pytest.fixture()
    def rdr_index(self):
        from planetarypy.pds import get_index
        return get_index("mro.hirise.rdr", allow_refresh=False)

    def _compare(self, mars, rdr_index, pid, max_diff=5.0):
        obs = rdr_index[rdr_index["PRODUCT_ID"] == pid].squeeze()
        center_lat = np.mean([obs[f"CORNER{i}_LATITUDE"] for i in range(1, 5)])
        center_lon = np.mean([obs[f"CORNER{i}_LONGITUDE"] for i in range(1, 5)])
        obs_time = str(obs["START_TIME"])

        spice_az = mars.solar_azimuth_at(center_lon, center_lat, time=obs_time)
        hirise_geographic = (obs["SUB_SOLAR_AZIMUTH"] + 90) % 360
        diff = abs(spice_az - hirise_geographic)
        if diff > 180:
            diff = 360 - diff
        assert diff < max_diff, (
            f"{pid}: SPICE={spice_az:.1f}° vs index={hirise_geographic:.1f}°, Δ={diff:.1f}°"
        )

    def test_northern_hemisphere(self, mars, rdr_index):
        self._compare(mars, rdr_index, "ESP_013807_2035_RED")

    def test_northern_different_lon(self, mars, rdr_index):
        self._compare(mars, rdr_index, "PSP_001414_2165_RED")

    def test_southern_hemisphere(self, mars, rdr_index):
        self._compare(mars, rdr_index, "PSP_003630_1715_RED")


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
