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


class TestValidateIndexKey:
    """One home for a check that had been copy-pasted seven times."""

    def test_accepts_a_registered_key(self):
        from planetarypy.pds.utils import validate_index_key

        assert validate_index_key("mro.ctx.edr") == "mro.ctx.edr"

    @pytest.mark.parametrize("key", ["foo", "foo.bar", "a.b.c.d"])
    def test_malformed_key_names_the_shape_problem(self, key):
        from planetarypy.pds.utils import MalformedIndexKeyError, validate_index_key

        # Previously these reached Index.__init__'s bare tuple unpack and came
        # back as "not enough values to unpack (expected 3, got 2)".
        with pytest.raises(MalformedIndexKeyError, match="Malformed index key"):
            validate_index_key(key)

    def test_malformed_key_is_never_looked_up(self, monkeypatch):
        """A wrong-shaped key has no registry to be in, so don't consult it."""
        from planetarypy.pds import utils as u

        called = []
        monkeypatch.setattr(u, "_all_dotted_index_keys",
                            lambda: called.append(1) or [])
        with pytest.raises(u.MalformedIndexKeyError):
            u.validate_index_key("a.b.c.d")
        assert called == []

    def test_the_two_rejections_have_distinct_types(self):
        from planetarypy.pds.utils import (
            IndexKeyError, MalformedIndexKeyError, UnknownIndexKeyError,
        )

        # Both are IndexKeyError, and both remain ValueError so existing
        # `except ValueError` handlers keep working.
        for cls in (MalformedIndexKeyError, UnknownIndexKeyError):
            assert issubclass(cls, IndexKeyError)
            assert issubclass(cls, ValueError)
        assert not issubclass(MalformedIndexKeyError, UnknownIndexKeyError)
        assert not issubclass(UnknownIndexKeyError, MalformedIndexKeyError)

    def test_unknown_key_error_carries_the_key_for_handlers(self):
        from planetarypy.pds.utils import UnknownIndexKeyError, validate_index_key

        with pytest.raises(UnknownIndexKeyError) as excinfo:
            validate_index_key("definitely.not.key")
        assert excinfo.value.index_key == "definitely.not.key"

    def test_well_formed_but_unregistered_key_is_unknown_not_malformed(self):
        from planetarypy.pds.utils import validate_index_key

        with pytest.raises(ValueError, match="Unknown index key"):
            validate_index_key("foo.bar.baz")

    def test_index_construction_checks_shape(self):
        from planetarypy.pds import Index

        with pytest.raises(ValueError, match="Malformed index key"):
            Index("foo.bar")

    def test_index_stays_constructible_with_an_unregistered_key(self, monkeypatch):
        """Index is low-level: tests and not-yet-registered indexes need it.

        Membership is checked at the user-facing boundary, not here — enforcing
        it in __init__ broke four existing tests that build an Index around a
        synthetic key with a monkeypatched remote.
        """
        from planetarypy.pds.utils import check_index_key_shape

        assert check_index_key_shape("go.ssi.raw") == "go.ssi.raw"
