"""Tests for the catalog URL rewrite module."""

import pytest

from planetarypy.catalog._url_rewrite import rewrite_usgs_url


USGS = "http://pdsimage2.wr.usgs.gov/Missions/"


class TestSETIRewrites:
    """URLs rewritten to pds-rings.seti.org."""

    def test_cassini_iss(self):
        url = f"{USGS}Cassini/ISS/coiss_2022/data/1523783972_1523789335"
        result = rewrite_usgs_url(url)
        assert result == (
            "https://pds-rings.seti.org/viewmaster/volumes/"
            "COISS_2xxx/COISS_2022/data/1523783972_1523789335"
        )

    def test_cassini_iss_calib(self):
        url = f"{USGS}Cassini/ISS/coiss_0005/data/wacfm/focus/1272"
        result = rewrite_usgs_url(url)
        assert result.startswith("https://pds-rings.seti.org/viewmaster/volumes/COISS_0xxx/")

    def test_galileo_ssi(self):
        url = f"{USGS}Galileo/SSI/go_0001/blemish"
        result = rewrite_usgs_url(url)
        assert result == (
            "https://pds-rings.seti.org/viewmaster/volumes/"
            "GO_0xxx/GO_0001/blemish"
        )

    def test_juno_junocam(self):
        url = f"{USGS}Juno/JunoCam/JNOJNC_0002/DATA/EDR/JUPITER/ORBIT_00"
        result = rewrite_usgs_url(url)
        assert result == (
            "https://pds-rings.seti.org/viewmaster/volumes/"
            "JNOJNC_0xxx/JNOJNC_0002/DATA/EDR/JUPITER/ORBIT_00"
        )


class TestJPLRewrites:
    """URLs rewritten to planetarydata.jpl.nasa.gov."""

    def test_cassini_radar(self):
        url = f"{USGS}Cassini/RADAR/CORADR_0261/DATA/BIDR"
        result = rewrite_usgs_url(url)
        assert result == (
            "https://planetarydata.jpl.nasa.gov/img/data/"
            "cassini/cassini_orbiter/CORADR_0261/DATA/BIDR"
        )

    def test_phoenix(self):
        url = f"{USGS}Phoenix/phxssi_0xxx/data/sol126"
        result = rewrite_usgs_url(url)
        assert result == (
            "https://planetarydata.jpl.nasa.gov/img/data/"
            "phoenix/phxssi_0xxx/data/sol126"
        )

    def test_mars_pathfinder_rover(self):
        url = f"{USGS}Mars_Pathfinder/mprv_0001/apxs_ddr"
        result = rewrite_usgs_url(url)
        assert result == (
            "https://planetarydata.jpl.nasa.gov/img/data/"
            "mpf/rover/mprv_0001/apxs_ddr"
        )

    def test_mars_pathfinder_imp(self):
        url = f"{USGS}Mars_Pathfinder/mpim_0003/mars/seq0297/c1252xxx"
        result = rewrite_usgs_url(url)
        assert result == (
            "https://planetarydata.jpl.nasa.gov/img/data/"
            "mpf/imp/mpim_0003/mars/seq0297/c1252xxx"
        )

    def test_mro_ctx(self):
        url = f"{USGS}Mars_Reconnaissance_Orbiter/CTX/mrox_1232/data"
        result = rewrite_usgs_url(url)
        assert "mro/ctx/mrox_1232/data" in result

    def test_mgs_moc(self):
        url = f"{USGS}Mars_Global_Surveyor/MOC/mgsc_0004/sp1240"
        result = rewrite_usgs_url(url)
        assert "mgs/moc/mgsc_0004/sp1240" in result

    def test_magellan(self):
        url = f"{USGS}Magellan/mg_1232/fl42s042"
        result = rewrite_usgs_url(url)
        assert "magellan/mg_1232/fl42s042" in result

    def test_lunar_orbiter(self):
        url = f"{USGS}Lunar_Orbiter/LO_1001/DATA/LO3/FRAME_3027"
        result = rewrite_usgs_url(url)
        assert "lo/LO_1001/DATA/LO3/FRAME_3027" in result


class TestNoRule:
    """URLs without rewrite rules return None."""

    def test_galileo_nims(self):
        url = f"{USGS}Galileo/NIMS/go_1104/sl9"
        assert rewrite_usgs_url(url) is None

    def test_chandrayaan(self):
        url = f"{USGS}Chandrayaan_1/M3/CH1M3_0002/DATA/20090415_20090816/200904/L0"
        assert rewrite_usgs_url(url) is None

    def test_apollo(self):
        url = f"{USGS}Apollo/Metric_Camera/A15MC_0001/DATA/REVOLUTION_71"
        assert rewrite_usgs_url(url) is None

    def test_non_usgs_url(self):
        url = "https://pds-geosciences.wustl.edu/data/some/path"
        assert rewrite_usgs_url(url) is None
