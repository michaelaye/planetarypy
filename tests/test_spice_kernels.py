import pandas as pd
from astropy.time import Time

from planetarypy.spice import archived_kernels as kernels


def test_receive_datasets_dataframe():
    assert isinstance(kernels.datasets, pd.DataFrame)


def test_cassini_valid_times():
    assert kernels.is_start_valid("cassini", Time("1998-01-01")) is True
    assert kernels.is_start_valid("cassini", Time("1997-01-01")) is False
    assert kernels.is_stop_valid("cassini", "2017-01-01") is True
    assert kernels.is_stop_valid("cassini", "2018-01-01") is False


def test_Subsetter_kernel_names():
    subset = kernels.Subsetter("cassini", "2014-270")
    assert len(subset.kernel_names) == 31


def test_Subsetter_filenames():
    subset = kernels.Subsetter("cassini", "2011-02-13", "2011-02-14")
    assert subset.urls_file == "urls_cosp_1000_110213_110214.txt"
    assert subset.metakernel_file == "cas_2011_v18_110213_110214.tm"
