"""Tests for orphaned index-cache detection and removal."""

import pytest
from typer.testing import CliRunner

from planetarypy.cli import app
from planetarypy.pds import index_prune
from planetarypy.pds.index_prune import (
    find_orphan_index_caches,
    remove_orphan_index_caches,
)

runner = CliRunner()

REGISTERED = ["mro.ctx.edr", "cassini.iss.index"]


@pytest.fixture
def registry(monkeypatch):
    monkeypatch.setattr(index_prune, "_all_dotted_index_keys", lambda: REGISTERED)


def _write(path, size=16):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    return path


class TestFindOrphanIndexCaches:
    def test_current_layout_is_live(self, tmp_path, registry):
        _write(tmp_path / "mro/ctx/indexes/edr/cumindex.parq")
        assert find_orphan_index_caches(tmp_path) == []

    def test_legacy_top_level_indexes_tree_is_orphaned(self, tmp_path, registry):
        _write(tmp_path / "indexes/mro/ctx/edr/cumindex.parq")
        (orphan,) = find_orphan_index_caches(tmp_path)
        assert orphan.directory == tmp_path / "indexes/mro/ctx/edr"

    def test_loose_files_beside_current_layout_are_orphaned(self, tmp_path, registry):
        _write(tmp_path / "mro/ctx/indexes/cumindex.parq")
        (orphan,) = find_orphan_index_caches(tmp_path)
        assert orphan.directory == tmp_path / "mro/ctx/indexes"

    def test_unregistered_key_in_current_shape_is_orphaned(self, tmp_path, registry):
        """A renamed mission or index leaves a correctly-shaped stale cache."""
        _write(tmp_path / "mer/opportunity/indexes/pancam_rdr/index.parq")
        (orphan,) = find_orphan_index_caches(tmp_path)
        assert orphan.directory == tmp_path / "mer/opportunity/indexes/pancam_rdr"

    def test_empty_directories_are_ignored(self, tmp_path, registry):
        (tmp_path / "go/ssi/indexes/edr").mkdir(parents=True)
        assert find_orphan_index_caches(tmp_path) == []

    def test_missing_storage_root(self, tmp_path):
        assert find_orphan_index_caches(tmp_path / "nope") == []

    def test_reports_file_count_and_size(self, tmp_path, registry):
        _write(tmp_path / "go/ssi/indexes/a.parq", size=10)
        _write(tmp_path / "go/ssi/indexes/b.parq", size=20)
        (orphan,) = find_orphan_index_caches(tmp_path)
        assert len(orphan.files) == 2
        assert orphan.n_bytes == 30


class TestRemoveOrphanIndexCaches:
    def test_live_cache_nested_in_orphaned_parent_survives(self, tmp_path, registry):
        live = _write(tmp_path / "mro/ctx/indexes/edr/cumindex.parq")
        stale = _write(tmp_path / "mro/ctx/indexes/cumindex.parq")

        caches = find_orphan_index_caches(tmp_path)
        remove_orphan_index_caches(caches, tmp_path)

        assert not stale.exists()
        assert live.exists()

    def test_removes_files_and_empty_directories(self, tmp_path, registry):
        _write(tmp_path / "indexes/mro/ctx/edr/cumindex.parq")

        caches = find_orphan_index_caches(tmp_path)
        n_files, n_bytes = remove_orphan_index_caches(caches, tmp_path)

        assert (n_files, n_bytes) == (1, 16)
        assert not (tmp_path / "indexes").exists()
        assert tmp_path.is_dir()

    def test_leaves_nothing_for_a_second_run(self, tmp_path, registry):
        _write(tmp_path / "go/ssi/indexes/a.parq")
        remove_orphan_index_caches(find_orphan_index_caches(tmp_path), tmp_path)
        assert find_orphan_index_caches(tmp_path) == []


class TestPruneCLI:
    @pytest.fixture
    def storage(self, tmp_path, monkeypatch):
        monkeypatch.setattr(index_prune, "_all_dotted_index_keys", lambda: REGISTERED)
        from planetarypy.config import config

        monkeypatch.setattr(config, "storage_root", tmp_path)
        return tmp_path

    def test_dry_run_deletes_nothing(self, storage):
        stale = _write(storage / "indexes/mro/ctx/edr/cumindex.parq")
        result = runner.invoke(app, ["indexes", "prune"])
        assert result.exit_code == 0
        assert stale.exists()
        assert "Nothing deleted" in result.output

    def test_yes_deletes(self, storage, monkeypatch):
        monkeypatch.setattr("planetarypy.utils.have_internet", lambda *a, **k: False)
        stale = _write(storage / "indexes/mro/ctx/edr/cumindex.parq")
        result = runner.invoke(app, ["indexes", "prune", "--yes"])
        assert result.exit_code == 0
        assert not stale.exists()
        assert "Deleted 1 files" in result.output

    def test_clean_storage_root(self, storage):
        _write(storage / "mro/ctx/indexes/edr/cumindex.parq")
        result = runner.invoke(app, ["indexes", "prune"])
        assert result.exit_code == 0
        assert "No orphaned index caches" in result.output
