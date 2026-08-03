"""Project-wide astropy-units switch: setter, context manager, isolation."""

import pytest
from astropy import units as u

from planetarypy import units


@pytest.fixture(autouse=True)
def _default_state():
    """Units are on by default; no test may leak its setting."""
    units.set_units(True)
    yield
    units.set_units(True)


def test_units_are_on_by_default():
    assert units.get_units() is True


def test_set_units_returns_previous():
    previous = units.set_units(False)
    assert previous is True
    assert units.get_units() is False


def test_maybe_quantity_attaches_when_enabled():
    got = units.maybe_quantity(47.5, "km")
    assert isinstance(got, u.Quantity)
    assert got.unit == u.km
    assert got.value == pytest.approx(47.5)


def test_maybe_quantity_passes_through_when_disabled():
    units.set_units(False)
    assert units.maybe_quantity(47.5, "km") == 47.5
    assert not isinstance(units.maybe_quantity(47.5, "km"), u.Quantity)


def test_maybe_quantity_with_no_unit_is_a_passthrough():
    assert units.maybe_quantity(47.5, None) == 47.5


def test_context_manager_disables_within_block():
    with units.use_units(False):
        assert units.get_units() is False
        assert not isinstance(units.maybe_quantity(1.0, "km"), u.Quantity)
    assert units.get_units() is True


def test_context_manager_restores_previous_not_just_default():
    units.set_units(False)
    with units.use_units(True):
        assert units.get_units() is True
    assert units.get_units() is False


def test_context_manager_nests():
    with units.use_units(False):
        with units.use_units(True):
            assert units.get_units() is True
        assert units.get_units() is False


def test_context_manager_restores_on_exception():
    with pytest.raises(RuntimeError):
        with units.use_units(False):
            raise RuntimeError("boom")
    assert units.get_units() is True


def test_attach_and_read_units_on_a_frame():
    pd = pytest.importorskip("pandas")
    frame = pd.DataFrame({"diameter": [1.0, 2.0]})
    units.attach_units(frame, {"diameter": "km"})
    assert units.units_of(frame) == {"diameter": "km"}


def test_units_of_is_empty_for_a_bare_object():
    assert units.units_of(object()) == {}


def test_unit_map_survives_regardless_of_the_toggle():
    """The map is metadata; the toggle only governs value wrapping."""
    pd = pytest.importorskip("pandas")
    frame = pd.DataFrame({"diameter": [1.0]})
    units.attach_units(frame, {"diameter": "km"})
    units.set_units(False)
    assert units.units_of(frame) == {"diameter": "km"}


def test_setting_does_not_leak_across_threads():
    """ContextVar, not a module global."""
    import threading

    units.set_units(False)
    seen = []

    def worker():
        seen.append(units.get_units())

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join()
    assert seen == [True]   # the worker sees the default, not our False
