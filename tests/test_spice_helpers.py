"""Unit tests for the SPICE helper functions added for the
``plp spice cached`` / ``plp spice generic`` CLI verbs:

  - :func:`planetarypy.spice.archived_kernels.list_cached_kernels`
  - :func:`planetarypy.spice.generic_kernels.download_generic_kernel`

Tests use ``tmp_path`` + ``monkeypatch`` to stub out ``KERNEL_STORAGE``
(so the real user cache is untouched) and ``url_retrieve`` (so no
network is hit).
"""

from pathlib import Path

import pytest


class TestListCachedKernels:
    """``list_cached_kernels()`` walks {KERNEL_STORAGE}/* and groups
    files by top-level subdirectory."""

    def test_empty_storage_returns_empty_dict(self, tmp_path, monkeypatch):
        empty = tmp_path / "spice_kernels"
        empty.mkdir()
        monkeypatch.setattr(
            "planetarypy.spice.archived_kernels.KERNEL_STORAGE", empty,
        )
        from planetarypy.spice.archived_kernels import list_cached_kernels

        assert list_cached_kernels() == {}

    def test_nonexistent_storage_returns_empty_dict(self, tmp_path, monkeypatch):
        missing = tmp_path / "never_created"
        monkeypatch.setattr(
            "planetarypy.spice.archived_kernels.KERNEL_STORAGE", missing,
        )
        from planetarypy.spice.archived_kernels import list_cached_kernels

        assert list_cached_kernels() == {}

    def test_groups_by_top_level_directory(self, tmp_path, monkeypatch):
        storage = tmp_path / "spice_kernels"
        # generic kernels under nested subdirs
        (storage / "generic" / "lsk").mkdir(parents=True)
        (storage / "generic" / "lsk" / "naif0012.tls").touch()
        (storage / "generic" / "pck").mkdir()
        (storage / "generic" / "pck" / "pck00010.tpc").touch()
        # mission kernels flat
        (storage / "cassini").mkdir()
        (storage / "cassini" / "metakernel.tm").touch()
        (storage / "cassini" / "ck1.bc").touch()

        monkeypatch.setattr(
            "planetarypy.spice.archived_kernels.KERNEL_STORAGE", storage,
        )
        from planetarypy.spice.archived_kernels import list_cached_kernels

        result = list_cached_kernels()
        assert set(result) == {"generic", "cassini"}
        assert len(result["generic"]) == 2
        assert len(result["cassini"]) == 2

    def test_skips_loose_files_at_top_level(self, tmp_path, monkeypatch):
        """The grouping is by *directory*; loose files at the top level
        don't belong to any group and should be ignored."""
        storage = tmp_path / "spice_kernels"
        storage.mkdir()
        (storage / "loose.tls").touch()      # not under a dir → ignored
        (storage / "cassini").mkdir()
        (storage / "cassini" / "ck1.bc").touch()

        monkeypatch.setattr(
            "planetarypy.spice.archived_kernels.KERNEL_STORAGE", storage,
        )
        from planetarypy.spice.archived_kernels import list_cached_kernels

        result = list_cached_kernels()
        assert "cassini" in result
        # loose.tls didn't make it into any group
        all_files = [p.name for paths in result.values() for p in paths]
        assert "loose.tls" not in all_files

    def test_skips_empty_subdirs(self, tmp_path, monkeypatch):
        storage = tmp_path / "spice_kernels"
        (storage / "empty_group").mkdir(parents=True)
        (storage / "populated").mkdir()
        (storage / "populated" / "k1.bc").touch()

        monkeypatch.setattr(
            "planetarypy.spice.archived_kernels.KERNEL_STORAGE", storage,
        )
        from planetarypy.spice.archived_kernels import list_cached_kernels

        result = list_cached_kernels()
        assert "populated" in result
        assert "empty_group" not in result


class TestDownloadGenericKernel:
    """``download_generic_kernel(alias)`` fetches one generic kernel
    by short alias or by full path-fragment relative to GENERIC_URL."""

    def test_unknown_alias_raises_valueerror(self):
        from planetarypy.spice.generic_kernels import download_generic_kernel

        with pytest.raises(ValueError) as exc:
            download_generic_kernel("xyz")
        msg = str(exc.value)
        assert "xyz" in msg
        # Error message lists known aliases as a hint
        assert "lsk" in msg

    def test_known_alias_returns_cached_path_without_redownload(
        self, tmp_path, monkeypatch,
    ):
        """If the file already exists at the expected cache location,
        no network call is made — function returns the path directly."""
        storage = tmp_path / "generic"
        (storage / "lsk").mkdir(parents=True)
        cached = storage / "lsk" / "naif0012.tls"
        cached.write_text("fake LSK")

        monkeypatch.setattr(
            "planetarypy.spice.generic_kernels.GENERIC_STORAGE", storage,
        )

        def fail_if_called(*a, **kw):
            raise AssertionError(
                f"url_retrieve should not be called (cached). "
                f"args={a}, kwargs={kw}",
            )
        monkeypatch.setattr(
            "planetarypy.spice.generic_kernels.url_retrieve", fail_if_called,
        )
        from planetarypy.spice.generic_kernels import download_generic_kernel

        path = download_generic_kernel("lsk")
        assert path == cached
        assert path.read_text() == "fake LSK"

    def test_missing_file_triggers_download(self, tmp_path, monkeypatch):
        storage = tmp_path / "generic"
        storage.mkdir()
        monkeypatch.setattr(
            "planetarypy.spice.generic_kernels.GENERIC_STORAGE", storage,
        )

        calls = []

        def fake_url_retrieve(url, dest):
            calls.append((str(url), str(dest)))
            Path(dest).parent.mkdir(parents=True, exist_ok=True)
            Path(dest).write_text("downloaded")
        monkeypatch.setattr(
            "planetarypy.spice.generic_kernels.url_retrieve", fake_url_retrieve,
        )
        from planetarypy.spice.generic_kernels import download_generic_kernel

        path = download_generic_kernel("pck")
        assert len(calls) == 1
        url, dest = calls[0]
        assert url.endswith("pck/pck00010.tpc")
        assert str(path).endswith("pck/pck00010.tpc")
        assert path.read_text() == "downloaded"

    def test_overwrite_redownloads_even_if_cached(self, tmp_path, monkeypatch):
        storage = tmp_path / "generic"
        (storage / "lsk").mkdir(parents=True)
        cached = storage / "lsk" / "naif0012.tls"
        cached.write_text("old version")

        monkeypatch.setattr(
            "planetarypy.spice.generic_kernels.GENERIC_STORAGE", storage,
        )

        def fake_url_retrieve(url, dest):
            Path(dest).write_text("new version")
        monkeypatch.setattr(
            "planetarypy.spice.generic_kernels.url_retrieve", fake_url_retrieve,
        )
        from planetarypy.spice.generic_kernels import download_generic_kernel

        path = download_generic_kernel("lsk", overwrite=True)
        assert path.read_text() == "new version"

    def test_full_path_fragment_accepted_for_unlisted_kernels(
        self, tmp_path, monkeypatch,
    ):
        """Full path-fragments (containing '/') bypass the alias lookup
        — lets users grab non-default kernels like older LSK editions."""
        storage = tmp_path / "generic"
        storage.mkdir()
        monkeypatch.setattr(
            "planetarypy.spice.generic_kernels.GENERIC_STORAGE", storage,
        )

        calls = []

        def fake_url_retrieve(url, dest):
            calls.append(str(url))
            Path(dest).parent.mkdir(parents=True, exist_ok=True)
            Path(dest).touch()
        monkeypatch.setattr(
            "planetarypy.spice.generic_kernels.url_retrieve", fake_url_retrieve,
        )
        from planetarypy.spice.generic_kernels import download_generic_kernel

        path = download_generic_kernel("lsk/naif0011.tls")
        assert "lsk/naif0011.tls" in calls[0]
        assert str(path).endswith("lsk/naif0011.tls")
