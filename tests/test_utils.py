"""Tests for utils module."""

from pathlib import Path
import pytest

from planetarypy import utils


# File helpers
def test_file_variations():
    fname = "abc.txt"
    extensions = [".cub", ".cal.cub", ".map.cal.cub"]
    variations = utils.file_variations(fname, extensions)
    assert len(variations) == len(extensions)
    assert set(variations) == {
        Path("abc.cub"),
        Path("abc.cal.cub"),
        Path("abc.map.cal.cub"),
    }

def test_file_variations_with_path():
    fname = Path("/path/to/abc.txt")
    extensions = [".cub", ".cal.cub"]
    variations = utils.file_variations(fname, extensions)
    assert len(variations) == len(extensions)
    assert set(variations) == {
        Path("/path/to/abc.cub"),
        Path("/path/to/abc.cal.cub"),
    }

def test_file_variations_empty_extensions():
    fname = "abc.txt"
    extensions = []
    variations = utils.file_variations(fname, extensions)
    assert len(variations) == 0
    assert variations == []

def test_file_variations_invalid_extension():
    fname = "abc.txt"
    extensions = ["cub"]  # Missing dot prefix
    with pytest.raises(ValueError):
        utils.file_variations(fname, extensions)

def test_file_variations_non_list():
    fname = "abc.txt"
    invalid_inputs = [
        None,
        42,
        {"ext": ".cub"},
        ".cub",  # Single string instead of list
    ]
    for invalid_input in invalid_inputs:
        with pytest.raises((TypeError, AttributeError)):
            utils.file_variations(fname, invalid_input)


class _FakeResponse:
    status_code = 200
    headers = {"content-length": "11"}

    def __init__(self, payload):
        self._payload = payload

    def iter_content(self, chunk_size=4096):
        for i in range(0, len(self._payload), chunk_size):
            yield self._payload[i : i + chunk_size]


def test_url_retrieve_writes_file_and_cleans_part(tmp_path, monkeypatch):
    payload = b"hello world"
    monkeypatch.setattr(
        utils.requests, "get", lambda *a, **k: _FakeResponse(payload)
    )
    outfile = tmp_path / "data.bin"
    utils.url_retrieve(
        "http://example.invalid/data.bin", str(outfile), disable_tqdm=True
    )

    assert outfile.read_bytes() == payload
    # No leftover scratch file, and the part-file handle is released — on
    # Windows a leaked handle would have blocked the final rename
    # (PermissionError WinError 32).
    assert list(tmp_path.glob("*.part")) == []
    # Reopen/replace must succeed (would raise on Windows if a handle leaked).
    outfile.replace(tmp_path / "data2.bin")
