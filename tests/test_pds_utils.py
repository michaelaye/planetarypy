import pytest

# Target under test
from planetarypy.pds import utils as pds_utils


class _DummyHandler:
    pass


@pytest.fixture(autouse=True)
def patch_config_and_dynamic(monkeypatch):
    """Patch static and dynamic sources used by utils to avoid network and use a small fixture.

    We replace planetarypy.pds.static_index.ConfigHandler with a dummy class whose
    to_dict() returns our test config. We also patch
    planetarypy.pds.dynamic_index.DYNAMIC_URL_HANDLERS with a small mapping.
    """
    # Fake static nested config (leaf values don't matter; only keys do)
    static_cfg = {
        "cassini": {
            "iss": {
                "index": "X",
                "moon_summary": "X",
            },
            "uvis": {
                "index": "X",
            },
        },
        "mro": {
            "ctx": {
                "edr": "X",
            }
        },
    }

    class DummyConfigHandler:
        def __init__(self, *args, **kwargs):
            pass

        def to_dict(self):
            return static_cfg

    monkeypatch.setattr(
        "planetarypy.pds.static_index.ConfigHandler", DummyConfigHandler, raising=True
    )

    # Fake dynamic registry with full dotted keys
    dynamic = {
        "mro.ctx.edr": _DummyHandler,
        "lro.lroc.edr": _DummyHandler,
    }
    monkeypatch.setattr(
        "planetarypy.pds.dynamic_index.DYNAMIC_URL_HANDLERS", dynamic, raising=True
    )

    yield


def test_get_mission_names():
    missions = pds_utils.get_mission_names()
    assert set(["cassini", "lro", "mro"]).issubset(missions)


def test_get_instrument_names():
    # cassini from static only
    instruments = pds_utils.get_instrument_names("cassini")
    assert set(["iss", "uvis"]).issubset(instruments)
    # lro has no static, but dynamic has lroc
    instruments = pds_utils.get_instrument_names("lro")
    assert set(["lroc"]).issubset(instruments)


def test_get_index_names():
    # mro.ctx has static edr and dynamic rdr
    idx = pds_utils.get_index_names("mro.hirise")
    assert set(["edr", "rdr"]).issubset(idx)

    # cassini.iss only static
    idx2 = pds_utils.get_index_names("cassini.iss")
    assert set(["index", "moon_summary"]).issubset(idx2)


def test_print_available_indexes_keys_only_and_filters():
    keys = pds_utils.print_available_indexes(keys_only=True)
    # All dotted keys from static + dynamic
    expected_keys = [
        "cassini.iss.index",
        "cassini.iss.moon_summary",
        "cassini.uvis.index",
        "lro.lroc.edr",
        "mro.ctx.edr",
        "mro.hirise.rdr",
    ]
    assert set(expected_keys).issubset(keys)

    # Filter by mission
    mro_keys = pds_utils.print_available_indexes("mro", keys_only=True)
    assert set(["mro.ctx.edr", "mro.hirise.rdr"]).issubset(mro_keys)

    # Filter by mission and instrument
    mro_ctx_keys = pds_utils.print_available_indexes("mro", "ctx", keys_only=True)
    assert set(["mro.ctx.edr"]).issubset(mro_ctx_keys)
