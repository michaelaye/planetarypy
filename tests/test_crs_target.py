"""Session-wide target CRS: setter, context manager, precedence, announcement."""

import warnings

import pytest
from pyproj import CRS

from planetarypy import crs as pcrs

MARS_OCENTRIC = "IAU_2015:49900"
MARS_OGRAPHIC = "IAU_2015:49901"
GAZETTEER = "ESRI:104905"  # what USGS nomenclature actually ships


@pytest.fixture(autouse=True)
def _clean_session():
    """No test may leak session state into the next one."""
    pcrs.clear_target_crs()
    yield
    pcrs.clear_target_crs()


# --- setter / getter --------------------------------------------------------


def test_unset_by_default():
    assert pcrs.get_target_crs() is None


def test_set_and_get_roundtrip():
    pcrs.set_target_crs(MARS_OCENTRIC)
    assert pcrs.get_target_crs().to_authority() == ("IAU_2015", "49900")


def test_set_accepts_a_crs_object():
    pcrs.set_target_crs(CRS.from_user_input(MARS_OCENTRIC))
    assert pcrs.get_target_crs().to_authority() == ("IAU_2015", "49900")


def test_set_returns_the_previous_value():
    pcrs.set_target_crs(MARS_OCENTRIC)
    previous = pcrs.set_target_crs(MARS_OGRAPHIC)
    assert previous.to_authority() == ("IAU_2015", "49900")
    assert pcrs.get_target_crs().to_authority() == ("IAU_2015", "49901")


def test_set_none_clears():
    pcrs.set_target_crs(MARS_OCENTRIC)
    pcrs.set_target_crs(None)
    assert pcrs.get_target_crs() is None


def test_clear_target_crs():
    pcrs.set_target_crs(MARS_OCENTRIC)
    pcrs.clear_target_crs()
    assert pcrs.get_target_crs() is None


# --- context manager --------------------------------------------------------


def test_context_manager_sets_inside():
    with pcrs.target_crs(MARS_OCENTRIC) as active:
        assert active.to_authority() == ("IAU_2015", "49900")
        assert pcrs.get_target_crs().to_authority() == ("IAU_2015", "49900")


def test_context_manager_restores_unset_state():
    with pcrs.target_crs(MARS_OCENTRIC):
        pass
    assert pcrs.get_target_crs() is None


def test_context_manager_restores_previous_value():
    pcrs.set_target_crs(MARS_OGRAPHIC)
    with pcrs.target_crs(MARS_OCENTRIC):
        assert pcrs.get_target_crs().to_authority() == ("IAU_2015", "49900")
    assert pcrs.get_target_crs().to_authority() == ("IAU_2015", "49901")


def test_context_manager_nests():
    with pcrs.target_crs(MARS_OCENTRIC):
        with pcrs.target_crs(MARS_OGRAPHIC):
            assert pcrs.get_target_crs().to_authority() == ("IAU_2015", "49901")
        assert pcrs.get_target_crs().to_authority() == ("IAU_2015", "49900")


def test_context_manager_restores_on_exception():
    pcrs.set_target_crs(MARS_OGRAPHIC)
    with pytest.raises(RuntimeError):
        with pcrs.target_crs(MARS_OCENTRIC):
            raise RuntimeError("boom")
    assert pcrs.get_target_crs().to_authority() == ("IAU_2015", "49901")


def test_context_manager_can_suspend_the_session_setting():
    pcrs.set_target_crs(MARS_OCENTRIC)
    with pcrs.target_crs(None):
        assert pcrs.get_target_crs() is None
    assert pcrs.get_target_crs().to_authority() == ("IAU_2015", "49900")


# --- precedence -------------------------------------------------------------


def test_explicit_beats_session():
    pcrs.set_target_crs(MARS_OCENTRIC)
    assert pcrs.resolve_crs(MARS_OGRAPHIC).to_authority() == ("IAU_2015", "49901")


def test_session_beats_fallback():
    pcrs.set_target_crs(MARS_OCENTRIC)
    got = pcrs.resolve_crs(None, fallback=MARS_OGRAPHIC)
    assert got.to_authority() == ("IAU_2015", "49900")


def test_fallback_used_when_nothing_else_set():
    got = pcrs.resolve_crs(None, fallback=MARS_OGRAPHIC)
    assert got.to_authority() == ("IAU_2015", "49901")


def test_all_none_resolves_to_none():
    assert pcrs.resolve_crs(None) is None


# --- announcement -----------------------------------------------------------


def test_announce_warns_on_a_real_conversion():
    with pytest.warns(pcrs.CRSConversionWarning, match="reprojected"):
        pcrs.announce_conversion(GAZETTEER, MARS_OCENTRIC)


def test_announcement_names_both_authorities():
    with pytest.warns(pcrs.CRSConversionWarning) as rec:
        pcrs.announce_conversion(GAZETTEER, MARS_OCENTRIC, what="nomenclature")
    message = str(rec[0].message)
    assert "ESRI:104905" in message
    assert "IAU_2015:49900" in message
    assert "nomenclature" in message


def test_announce_is_silent_when_crs_match():
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        pcrs.announce_conversion(MARS_OCENTRIC, MARS_OCENTRIC)


def test_announce_is_silent_when_either_side_is_none():
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        pcrs.announce_conversion(None, MARS_OCENTRIC)
        pcrs.announce_conversion(MARS_OCENTRIC, None)


def test_conversion_warning_is_silenceable_on_its_own():
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        warnings.filterwarnings("ignore", category=pcrs.CRSConversionWarning)
        pcrs.announce_conversion(GAZETTEER, MARS_OCENTRIC)


# --- isolation --------------------------------------------------------------


def test_session_state_does_not_leak_across_threads():
    """ContextVar, not a module global — a thread must not see the main setting."""
    import threading

    pcrs.set_target_crs(MARS_OCENTRIC)
    seen = []

    def worker():
        seen.append(pcrs.get_target_crs())

    t = threading.Thread(target=worker)
    t.start()
    t.join()
    assert seen == [None]
