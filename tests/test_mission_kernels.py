"""Tests for finding spacecraft metakernels through spice-kernel-db.

spice-kernel-db, its database and the mission kernels are all mocked: the
selection and messaging logic is what is under test, not a local kernel store.
"""

import sys
from types import SimpleNamespace

import pytest

pytest.importorskip("spiceypy")

from planetarypy.spice import mission_kernels  # noqa: E402


@pytest.fixture(scope="module", autouse=True)
def _require_generic_kernels(is_transient_network_error):
    """Time conversion needs the LSK; skip only on a transient NAIF outage."""
    from planetarypy.spice.generic_kernels import load_generic_kernels

    try:
        load_generic_kernels()
    except Exception as exc:
        if is_transient_network_error(exc):
            pytest.skip(f"SPICE generic kernels unavailable — NAIF unreachable: {exc!r}")
        raise


def _fake_store(monkeypatch, coverage):
    """A read-only skd database whose metakernels cover MPO over ``coverage[filename]``."""

    class FakeDB:
        def __init__(self, read_only):
            assert read_only

        def metakernels_covering(self, body_id, et=None, mission=None):
            return [
                {"filename": name, "mission": "BEPICOLOMBO", "mk_path": f"/mk/{name}",
                 "intervals": spans}
                for name, spans in coverage.items()
                if spans and (et is None or any(s <= et <= e for s, e in spans))
            ]

        def close(self):
            pass

    monkeypatch.setitem(sys.modules, "spice_kernel_db", SimpleNamespace(KernelDB=FakeDB))


def _et(utc):
    return mission_kernels.spice.utc2et(utc)


class TestFindMetakernel:
    def test_ops_wins_over_plan_and_scenarios(self, monkeypatch):
        span = [(_et("2020-01-01"), _et("2030-01-01"))]
        _fake_store(monkeypatch, {"bc_crema.tm": span, "bc_plan.tm": span, "bc_ops.tm": span})
        assert mission_kernels.find_metakernel("MPO", "2026-09-14").name == "bc_ops.tm"

    def test_falls_through_to_the_one_that_covers_the_time(self, monkeypatch):
        _fake_store(monkeypatch, {
            "bc_ops.tm": [(_et("2020-01-01"), _et("2027-04-11"))],
            "bc_plan.tm": [(_et("2020-01-01"), _et("2031-05-16"))],
        })
        assert mission_kernels.find_metakernel("MPO", "2027-06-01").name == "bc_plan.tm"

    def test_uncovered_time_points_at_browse_for_that_mission(self, monkeypatch):
        _fake_store(monkeypatch, {"bc_plan.tm": [(_et("2020-01-01"), _et("2031-05-16"))]})
        with pytest.raises(LookupError, match="spice-kernel-db browse BEPICOLOMBO"):
            mission_kernels.find_metakernel("MPO", "2032-01-01")

    def test_unknown_spacecraft_points_at_mission_add(self, monkeypatch):
        _fake_store(monkeypatch, {"bc_plan.tm": []})
        with pytest.raises(LookupError, match="spice-kernel-db mission add"):
            mission_kernels.find_metakernel("MPO", "2027-06-01")


class TestDatabaseAccess:
    def test_missing_package_gives_install_hint(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "spice_kernel_db", None)
        with pytest.raises(ImportError, match=r"planetarypy\[skd\]"):
            mission_kernels.tracked_metakernels()

    def test_locked_database_is_explained(self, monkeypatch):
        duckdb = pytest.importorskip("duckdb")

        def locked(read_only):
            raise duckdb.IOException('IO Error: Could not set lock on file "kernels.duckdb"')

        monkeypatch.setitem(sys.modules, "spice_kernel_db", SimpleNamespace(KernelDB=locked))
        with pytest.raises(LookupError, match="in use by another process"):
            mission_kernels.tracked_metakernels()

    def test_identical_aliases_are_dropped(self, monkeypatch):
        class FakeDB:
            def __init__(self, read_only):
                assert read_only

            def list_metakernels(self, show=True):
                assert show is False
                return [
                    {"filename": "bc_plan.tm", "identical_to": None},
                    {"filename": "bc_plan_v2.tm", "identical_to": "bc_plan.tm"},
                ]

            def close(self):
                pass

        monkeypatch.setitem(sys.modules, "spice_kernel_db", SimpleNamespace(KernelDB=FakeDB))
        assert [r["filename"] for r in mission_kernels.tracked_metakernels()] == ["bc_plan.tm"]


class TestResolveMetakernel:
    def test_existing_path_is_returned_as_is(self, tmp_path):
        mk = tmp_path / "local.tm"
        mk.touch()
        assert mission_kernels.resolve_metakernel(str(mk)) == mk

    def test_tracked_filename_resolves_to_its_path(self, monkeypatch):
        rows = [{"filename": "bc_ops.tm", "mk_path": "/kernels/BEPICOLOMBO/mk/bc_ops.tm"}]
        monkeypatch.setattr(mission_kernels, "tracked_metakernels", lambda: rows)
        assert str(mission_kernels.resolve_metakernel("bc_ops.tm")).endswith("mk/bc_ops.tm")

    def test_unknown_name_says_how_to_list(self, monkeypatch):
        monkeypatch.setattr(mission_kernels, "tracked_metakernels", lambda: [])
        with pytest.raises(LookupError, match="spice-kernel-db metakernels"):
            mission_kernels.resolve_metakernel("nope.tm")


class TestSpacecraftForMission:
    @pytest.fixture(autouse=True)
    def _archive(self, monkeypatch):
        import pandas as pd

        from planetarypy.spice import archived_kernels

        table = pd.DataFrame(
            {"Mission Name": ["Psyche", "Lucy", "Hayabusa2", "BepiColombo"]},
            index=["psyche", "lucy", "hayabusa2", "bc"],
        )
        monkeypatch.setattr(archived_kernels, "datasets", table)

    def test_collision_resolves_by_prefix(self):
        assert mission_kernels.spacecraft_for_mission("Psyche") == "PSYC"

    def test_exact_name_wins_over_a_shorter_prefix(self):
        assert mission_kernels.spacecraft_for_mission("hayabusa2") == "HAYABUSA2"

    def test_matching_name_is_returned_as_is(self):
        assert mission_kernels.spacecraft_for_mission("LUCY") == "LUCY"

    def test_not_an_archived_mission(self):
        assert mission_kernels.spacecraft_for_mission("Mars") is None

    def test_archived_mission_without_a_matching_spacecraft_name(self):
        assert mission_kernels.spacecraft_for_mission("bc") is None


class TestFindSpacecraftKernel:
    def test_tracked_metakernel_comes_first(self, monkeypatch):
        from planetarypy.spice import operational_kernels

        monkeypatch.setattr(mission_kernels, "find_metakernel", lambda sc, t: "bc_plan.tm")
        monkeypatch.setattr(operational_kernels, "find_local_spk", pytest.fail)
        assert mission_kernels.find_spacecraft_kernel("MPO", "2027-06-01") == "bc_plan.tm"

    def test_falls_back_to_a_fetched_operational_spk(self, monkeypatch, tmp_path):
        from planetarypy.spice import operational_kernels

        def no_skd(sc, t):
            raise ImportError("no spice-kernel-db")

        spk = tmp_path / "psyche_sc-eph.bsp"
        monkeypatch.setattr(mission_kernels, "find_metakernel", no_skd)
        monkeypatch.setattr(operational_kernels, "find_local_spk", lambda sc, t: spk)
        assert mission_kernels.find_spacecraft_kernel("PSYC", "2026-09-14") == spk

    def test_neither_explains_both_routes(self, monkeypatch):
        from planetarypy.spice import operational_kernels

        def nothing(sc, t):
            raise LookupError("No metakernel tracked by spice-kernel-db covers PSYC.")

        monkeypatch.setattr(mission_kernels, "find_metakernel", nothing)
        monkeypatch.setattr(operational_kernels, "find_local_spk", lambda sc, t: None)
        with pytest.raises(LookupError) as e:
            mission_kernels.find_spacecraft_kernel("PSYC", "2026-09-14")
        assert "No metakernel tracked" in str(e.value)
        assert "plp spice spk <MISSION>" in str(e.value)
