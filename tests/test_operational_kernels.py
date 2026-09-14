"""Tests for fetching spacecraft SPKs from NAIF's operational server.

The server listing, the downloads and the SPK coverage checks are mocked, so
these run offline; the selection rules are what is under test.
"""

import pytest

pytest.importorskip("spiceypy")

from planetarypy.spice import operational_kernels as ok  # noqa: E402

LISTING = """
<pre><img src="/icons/back.gif"> <a href="/pub/naif/PSYCHE/kernels/">Parent Directory</a>
<a href="de440.bsp">de440.bsp</a>              2022-06-09 13:04  114M
<a href="de440.bsp.lbl">de440.bsp.lbl</a>      2022-06-09 13:04   59K
<a href="psyche_sc-eph_a.bsp">psyche_sc-eph_a.bsp</a>  2026-08-11 15:55  4.6M
<a href="psyche_ref_b.bsp">psyche_ref_b.bsp</a>    2026-09-10 09:58  7.2M
<a href="huge.bsp">huge.bsp</a>                  2026-09-11 09:00  2.1G
</pre>
"""


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


@pytest.fixture()
def server(monkeypatch, tmp_path):
    """A fake NAIF listing, downloads that write empty files, and a coverage oracle."""
    import requests

    class Response:
        status_code = 200
        text = LISTING

        def raise_for_status(self):
            pass

    monkeypatch.setattr(requests, "get", lambda url, timeout: Response())
    monkeypatch.setattr(ok, "OPERATIONAL_STORAGE", tmp_path)
    downloads = []

    def retrieve(url, outfile):
        downloads.append(url.rsplit("/", 1)[1])
        open(outfile, "w").close()

    monkeypatch.setattr(ok, "url_retrieve", retrieve)
    covering = {"psyche_sc-eph_a.bsp"}
    monkeypatch.setattr(ok, "_covers", lambda path, body_id, et: path.name in covering)
    return downloads, covering, tmp_path


class TestListSpks:
    def test_parses_bsp_rows_newest_first_and_skips_labels(self, server):
        names = [r["name"] for r in ok.list_spks("psyche")]
        assert names == ["huge.bsp", "psyche_ref_b.bsp", "psyche_sc-eph_a.bsp", "de440.bsp"]
        assert ok.list_spks("psyche")[2]["url"].endswith("/PSYCHE/kernels/spk/psyche_sc-eph_a.bsp")

    def test_missing_mission_folder(self, monkeypatch):
        import requests

        class NotFound:
            status_code = 404

        monkeypatch.setattr(requests, "get", lambda url, timeout: NotFound())
        with pytest.raises(LookupError, match="no operational SPK folder"):
            ok.list_spks("nosuchmission")

    @pytest.mark.parametrize("size, expected", [("928K", 928 * 1024), ("4.6M", 4.6 * 1024**2),
                                                ("2.1G", 2.1 * 1024**3), ("6123", 6123)])
    def test_size_column(self, size, expected):
        assert ok._bytes(size) == pytest.approx(expected)


class TestFetchSpk:
    def test_newest_covering_file_is_kept_and_misses_are_deleted(self, server):
        downloads, _, storage = server
        path = ok.fetch_spk("psyche", "2026-09-14")
        assert path.name == "psyche_sc-eph_a.bsp"
        assert downloads == ["psyche_ref_b.bsp", "psyche_sc-eph_a.bsp"]  # huge.bsp skipped
        assert sorted(p.name for p in (storage / "PSYCHE" / "spk").iterdir()) == [
            "psyche_sc-eph_a.bsp"
        ]

    def test_covering_file_on_disk_means_no_download(self, server):
        downloads, _, _ = server
        ok.fetch_spk("psyche", "2026-09-14")
        downloads.clear()
        assert ok.fetch_spk("psyche", "2026-09-14").name == "psyche_sc-eph_a.bsp"
        assert downloads == []

    def test_nothing_covers_names_what_was_tried_and_skipped(self, server):
        _, covering, _ = server
        covering.clear()
        with pytest.raises(LookupError) as e:
            ok.fetch_spk("psyche", "2030-01-01")
        assert "psyche_ref_b.bsp" in str(e.value)
        assert "huge.bsp (2.1G)" in str(e.value)

    def test_underivable_spacecraft_asks_for_it(self, server, monkeypatch):
        from planetarypy.spice import mission_kernels

        monkeypatch.setattr(mission_kernels, "spacecraft_for_mission", lambda m: None)
        with pytest.raises(ValueError, match="spacecraft="):
            ok.fetch_spk("psyche", "2026-09-14")


class TestFindLocalSpk:
    def test_finds_a_fetched_file_for_the_spacecraft(self, server):
        ok.fetch_spk("psyche", "2026-09-14")
        assert ok.find_local_spk("PSYC", "2026-09-14").name == "psyche_sc-eph_a.bsp"

    def test_nothing_fetched_yet(self, server):
        assert ok.find_local_spk("PSYC", "2026-09-14") is None
