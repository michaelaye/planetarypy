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
        "mro.ctx.rdr": _DummyHandler,
        "lro.lroc.edr": _DummyHandler,
    }
    monkeypatch.setattr(
        "planetarypy.pds.dynamic_index.DYNAMIC_URL_HANDLERS", dynamic, raising=True
    )

    yield


def test_list_missions_includes_static_and_dynamic():
    missions = pds_utils.list_missions()
    assert missions == ["cassini", "lro", "mro"]


def test_list_instruments_for_mission_static_only():
    # cassini from static only
    instruments = pds_utils.list_instruments("cassini")
    assert instruments == ["iss", "uvis"]


def test_list_instruments_for_mission_with_dynamic():
    # lro has no static, but dynamic has lroc
    instruments = pds_utils.list_instruments("lro")
    assert instruments == ["lroc"]


def test_list_indexes_for_combined_sources():
    # mro.ctx has static edr and dynamic rdr
    idx = pds_utils.list_indexes("mro.ctx")
    assert idx == ["edr", "rdr"]

    # cassini.iss only static
    idx2 = pds_utils.list_indexes("cassini.iss")
    assert idx2 == ["index", "moon_summary"]


def test_list_available_indexes_keys_only_and_filters():
    keys = pds_utils.list_available_indexes(keys_only=True)
    # All dotted keys from static + dynamic
    assert keys == [
        "cassini.iss.index",
        "cassini.iss.moon_summary",
        "cassini.uvis.index",
        "lro.lroc.edr",
        "mro.ctx.edr",
        "mro.ctx.rdr",
    ]

    # Filter by mission
    mro_keys = pds_utils.list_available_indexes("mro", keys_only=True)
    assert mro_keys == ["mro.ctx.edr", "mro.ctx.rdr"]

    # Filter by mission and instrument
    mro_ctx_keys = pds_utils.list_available_indexes("mro", "ctx", keys_only=True)
    assert mro_ctx_keys == ["mro.ctx.edr", "mro.ctx.rdr"]
