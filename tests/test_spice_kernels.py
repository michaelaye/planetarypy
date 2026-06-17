import re

import pandas as pd
import pytest
from astropy.time import Time

pytest.importorskip("spiceypy")

from planetarypy.spice import archived_kernels as kernels


def test_receive_datasets_dataframe():
    assert isinstance(kernels.datasets, pd.DataFrame)


def test_cassini_valid_times():
    assert kernels._is_start_valid("cassini", Time("1998-01-01")) is True
    assert kernels._is_start_valid("cassini", Time("1997-01-01")) is False
    assert kernels._is_stop_valid("cassini", "2017-01-01") is True
    assert kernels._is_stop_valid("cassini", "2018-01-01") is False


def test_Subsetter_kernel_names():
    subset = kernels.Subsetter("cassini", "2014-270")
    assert len(subset.kernel_names) == 31


def test_Subsetter_filenames():
    subset = kernels.Subsetter("cassini", "2011-02-13", "2011-02-14")
    # Version segments (cosp_NNNN, v18/v19/…) drift as NAIF re-issues the
    # archive; match the structure + date range, not the exact version.
    assert re.fullmatch(r"urls_cosp_\d+_110213_110214\.txt", subset.urls_file)
    assert re.fullmatch(r"cas_2011_v\d+_110213_110214\.tm", subset.metakernel_file)


class TestRepointPathValues:
    """Regression for the PATH_VALUES rewrite (bug: only '/./data' handled)."""

    ABS = "/data/spice_kernels/hayabusa2"

    def test_hayabusa2_dotdot_convention(self):
        # Hayabusa2's PDS4 metakernel uses PATH_VALUES = ('..') — the case that
        # used to slip through unrewritten and break furnsh.
        mk = (
            "   \\begindata\n"
            "      PATH_VALUES     = (\n"
            "                         '..'\n"
            "                        )\n"
            "      PATH_SYMBOLS    = ( 'K' )\n"
            "      KERNELS_TO_LOAD = ( '$K/lsk/naif0012.tls',\n"
            "                          '$K/spk/hyb2.bsp' )\n"
        )
        out = kernels._repoint_path_values(mk, self.ABS)
        assert f"'{self.ABS}'" in out
        assert "'..'" not in out
        # KERNELS_TO_LOAD symbol references must be left intact.
        assert "'$K/lsk/naif0012.tls'" in out
        assert "'$K/spk/hyb2.bsp'" in out

    def test_dotdata_convention_single_line(self):
        mk = "   PATH_VALUES = ( './data' )\n   KERNELS_TO_LOAD = ( '$K/lsk/x.tls' )\n"
        out = kernels._repoint_path_values(mk, self.ABS)
        assert f"PATH_VALUES = ( '{self.ABS}' )" in out
        assert "'./data'" not in out
        assert "'$K/lsk/x.tls'" in out  # unchanged
