"""Tests for planetarypy.pds.index_logging.AccessLog."""

from datetime import datetime, timedelta

import pytest
import tomlkit

from planetarypy.pds.index_logging import AccessLog


KEY = "mro.ctx.edr"


@pytest.fixture()
def access_log(tmp_path, monkeypatch):
    """Return an AccessLog whose FILE_PATH points to a temp file."""
    tmp_file = tmp_path / "test_index_log.toml"
    monkeypatch.setattr(AccessLog, "FILE_PATH", tmp_file)
    return AccessLog(KEY)


# --- 1. Creation with a fresh temp file ---


def test_create_new_access_log(access_log):
    """A fresh AccessLog should have an empty doc and no last_check."""
    assert access_log.last_check is None
    assert access_log.last_update is None


# --- 2. log_check_time ---


def test_log_check_time(access_log):
    before = datetime.now().replace(microsecond=0)
    access_log.log_check_time()
    after = datetime.now().replace(microsecond=0)

    last = access_log.last_check
    assert last is not None
    assert before <= last <= after


def test_log_check_time_persists_to_disk(access_log):
    access_log.log_check_time()
    # Re-read from disk
    reloaded = AccessLog(KEY)
    assert reloaded.last_check is not None


# --- 3. log_update_time ---


def test_log_update_time(access_log):
    before = datetime.now().replace(microsecond=0)
    access_log.log_update_time()
    after = datetime.now().replace(microsecond=0)

    last = access_log.last_update
    assert last is not None
    assert before <= last <= after


# --- 4. log_current_url ---


def test_log_current_url(access_log):
    url = "https://example.com/data/index.tab"
    access_log.log_current_url(url)
    access_log.save()
    assert access_log.current_url == url

    # Verify persistence
    reloaded = AccessLog(KEY)
    assert reloaded.current_url == url


# --- 5. log_available_url ---


def test_log_available_url(access_log):
    url = "https://example.com/data/new_index.tab"
    access_log.log_available_url(url)

    assert access_log.available_url == url
    assert access_log.update_available is True
    # log_available_url also calls log_check_time
    assert access_log.last_check is not None


# --- 6. log_update_available ---


def test_log_update_available_true(access_log):
    access_log.log_update_available(True)
    assert access_log.update_available is True


def test_log_update_available_false(access_log):
    access_log.log_update_available(True)
    access_log.log_update_available(False)
    assert access_log.update_available is False


def test_log_update_available_persists(access_log):
    access_log.log_update_available(True)
    reloaded = AccessLog(KEY)
    assert reloaded.update_available is True


# --- 7. should_check ---


def test_should_check_true_when_never_checked(access_log):
    assert access_log.should_check is True


def test_should_check_false_when_just_checked(access_log):
    access_log.log_check_time()
    assert access_log.should_check is False


def test_should_check_true_when_stale(access_log):
    """Manually write a last_checked timestamp >1 day ago."""
    old_time = datetime.now() - timedelta(days=2)
    access_log.set(KEY, "last_checked", old_time.replace(microsecond=0))
    access_log.save()
    assert access_log.should_check is True


# --- 8. time_since_last_check ---


def test_time_since_last_check_none_when_never_checked(access_log):
    assert access_log.time_since_last_check is None


def test_time_since_last_check_returns_timedelta(access_log):
    access_log.log_check_time()
    delta = access_log.time_since_last_check
    assert isinstance(delta, timedelta)
    # Should be very small (< 2 seconds)
    assert delta.total_seconds() < 2


def test_time_since_last_check_old_timestamp(access_log):
    old_time = datetime.now() - timedelta(hours=36)
    access_log.set(KEY, "last_checked", old_time.replace(microsecond=0))
    access_log.save()

    delta = access_log.time_since_last_check
    assert delta.total_seconds() > 35 * 3600


# --- 9. current_url, available_url, update_available getters ---


def test_getters_return_none_initially(access_log):
    assert access_log.current_url is None
    assert access_log.available_url is None
    assert access_log.update_available is None


# --- 10. _delete ---


def test_delete_removes_file(access_log):
    # Write something so the file exists on disk
    access_log.log_check_time()
    assert access_log.FILE_PATH.is_file()

    access_log._delete()
    assert not access_log.FILE_PATH.is_file()


def test_delete_nonexistent_file_does_not_raise(access_log):
    """Calling _delete when file doesn't exist should not raise."""
    # Ensure file does not exist
    if access_log.FILE_PATH.is_file():
        access_log.FILE_PATH.unlink()
    # Should just log a warning, not raise
    access_log._delete()


# --- Extra: _log_yesterday_check helper ---


def test_log_yesterday_check_forces_should_check(access_log):
    access_log.log_check_time()
    assert access_log.should_check is False

    access_log._log_yesterday_check()
    assert access_log.should_check is True


# --- Extra: log_remote_timestamp ---


def test_log_remote_timestamp(access_log):
    ts = datetime(2025, 6, 15, 12, 30, 0)
    access_log.log_remote_timestamp(ts)
    access_log.save()

    stored = access_log.get(KEY, "remote_timestamp")
    assert stored == ts
    # Also logs check time
    assert access_log.last_check is not None


# --- Extra: __str__ / __repr__ ---


def test_str_with_key(access_log):
    access_log.log_check_time()
    s = str(access_log)
    # Should be a TOML dump, containing the key structure
    assert isinstance(s, str)


def test_str_without_key(access_log):
    """When key is None, __str__ reads the file directly."""
    access_log.log_check_time()
    access_log.key = None
    s = str(access_log)
    assert isinstance(s, str)


# --- Extra: multiple keys don't interfere ---


def test_multiple_keys_independent(tmp_path, monkeypatch):
    """Two keys written sequentially persist independently."""
    tmp_file = tmp_path / "multi_key_log.toml"
    monkeypatch.setattr(AccessLog, "FILE_PATH", tmp_file)

    # Write first key and save to disk
    log_a = AccessLog("mro.ctx.edr")
    log_a.log_current_url("https://a.example.com")
    log_a.save()

    # Create second instance *after* first has saved, so it loads existing data
    log_b = AccessLog("cassini.iss.raw")
    log_b.log_current_url("https://b.example.com")
    log_b.save()

    reloaded_a = AccessLog("mro.ctx.edr")
    reloaded_b = AccessLog("cassini.iss.raw")
    assert reloaded_a.current_url == "https://a.example.com"
    assert reloaded_b.current_url == "https://b.example.com"
