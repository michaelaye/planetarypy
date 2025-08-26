#!/usr/bin/env python3
"""Test the clean logging approach for URL discovery.

This test suite validates the IndexAccessLog functionality for tracking
URL discoveries in a clean, separate log file rather than modifying
the URL configuration file itself.

Run with: python test_clean_logging.py
Or as part of the test suite: pytest test_clean_logging.py
"""

import os
import sys
import tempfile
from pathlib import Path

# Add the src directory to the path to import the module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

try:
    import tomlkit

    from planetarypy.pds.index_logging import IndexAccessLog

    DEPENDENCIES_AVAILABLE = True
except ImportError as e:
    print(f"Skipping test due to missing dependencies: {e}")
    DEPENDENCIES_AVAILABLE = False


def test_index_access_log_discovery():
    """Test the IndexAccessLog discovery functionality."""

    if not DEPENDENCIES_AVAILABLE:
        print("Dependencies not available, skipping test")
        return

    # Create a temporary log file for testing
    with tempfile.TemporaryDirectory() as temp_dir:
        log_path = Path(temp_dir) / "test_discovery.log"

        # Create IndexAccessLog instance with test path
        access_log = IndexAccessLog(log_path=str(log_path))

        # Test logging a new URL discovery
        key = "mro.ctx.edr"
        url = "https://example.com/new_index.lbl"
        previous_url = "https://example.com/old_index.lbl"

        # Log the discovery
        access_log.log_url_discovery(
            key, url, is_update=True, previous_url=previous_url
        )

        # Verify the log was created and contains expected data
        assert log_path.exists(), "Log file should be created"

        # Read the log data as TOML
        log_toml = tomlkit.loads(log_path.read_text())

        # Check that discovery data exists in dynamic_urls section
        key_parts = key.split(".")
        assert len(key_parts) >= 3, "Key should have mission.instrument.index format"
        assert key in log_toml["dynamic_urls"], (
            f"Key {key} should exist in dynamic_urls"
        )

        discovery_info = log_toml["dynamic_urls"][key]
        assert discovery_info["url"] == url, "URL should match"
        assert discovery_info["is_update"], "Should be marked as update"
        assert discovery_info["previous_url"] == previous_url, (
            "Previous URL should match"
        )
        assert "discovered_at" in discovery_info, "Should have discovery timestamp"

        print("✓ test_index_access_log_discovery passed")


def test_get_discovery_info():
    """Test retrieving discovery information."""

    if not DEPENDENCIES_AVAILABLE:
        print("Dependencies not available, skipping test")
        return

    with tempfile.TemporaryDirectory() as temp_dir:
        log_path = Path(temp_dir) / "test_get_discovery.log"

        # Create IndexAccessLog instance
        access_log = IndexAccessLog(log_path=str(log_path))

        # Log multiple discoveries
        keys_and_urls = [
            ("mro.ctx.edr", "https://example.com/ctx_index.lbl"),
            ("lro.lroc.edr", "https://example.com/lroc_index.lbl"),
        ]

        for key, url in keys_and_urls:
            access_log.log_url_discovery(key, url, is_update=False)

        # Test retrieving specific discovery info
        ctx_info = access_log.get_discovery_info("mro.ctx.edr")
        assert ctx_info is not None, "Should find CTX discovery info"
        assert ctx_info["url"] == "https://example.com/ctx_index.lbl", (
            "URL should match"
        )
        assert not ctx_info["is_update"], "Should not be marked as update"

        # Test retrieving non-existent discovery info
        missing_info = access_log.get_discovery_info("nonexistent.key")
        assert missing_info is None, "Should return None for non-existent key"

        print("✓ test_get_discovery_info passed")


def test_get_all_discoveries():
    """Test retrieving all discovery information."""

    if not DEPENDENCIES_AVAILABLE:
        print("Dependencies not available, skipping test")
        return

    with tempfile.TemporaryDirectory() as temp_dir:
        log_path = Path(temp_dir) / "test_all_discoveries.log"

        # Create IndexAccessLog instance
        access_log = IndexAccessLog(log_path=str(log_path))

        # Log multiple discoveries
        discoveries = [
            ("mro.ctx.edr", "https://example.com/ctx_index.lbl", False),
            ("lro.lroc.edr", "https://example.com/lroc_index.lbl", True),
            ("cassini.iss.index", "https://example.com/iss_index.lbl", False),
        ]

        for key, url, is_update in discoveries:
            previous = "https://example.com/old.lbl" if is_update else None
            access_log.log_url_discovery(
                key, url, is_update=is_update, previous_url=previous
            )

        # Test retrieving all discoveries
        all_discoveries = access_log.get_all_discoveries()

        assert len(all_discoveries) == 3, "Should have 3 discoveries"
        assert "mro.ctx.edr" in all_discoveries, "Should contain CTX discovery"
        assert "lro.lroc.edr" in all_discoveries, "Should contain LROC discovery"
        assert "cassini.iss.index" in all_discoveries, "Should contain ISS discovery"

        # Check that LROC discovery has update info
        lroc_info = all_discoveries["lro.lroc.edr"]
        assert lroc_info["is_update"], "LROC should be marked as update"
        assert lroc_info["previous_url"] == "https://example.com/old.lbl", (
            "Should have previous URL"
        )

        print("✓ test_get_all_discoveries passed")


def demonstrate_clean_logging():
    """Demonstrate how URL discovery is now logged cleanly in TOML format."""

    # Create temporary files for testing
    with tempfile.TemporaryDirectory() as temp_dir:
        log_path = Path(temp_dir) / "test.toml"

        # Simulate what the IndexAccessLog would do in TOML format
        toml_content = """# PlanetaryPy Index Access Log

# This file tracks access timestamps and URL discoveries

[timestamps]
"mro.ctx.edr" = "2025-07-29T19:45:00"

[discoveries]

[discoveries."mro.ctx.edr"]
url = "https://example.com/new_index.lbl"
discovered_at = "2025-07-29T19:45:00"
is_update = true
previous_url = "https://example.com/old_index.lbl"
"""

        # Write to file
        log_path.write_text(toml_content)

        print("=== Clean TOML Logging Approach Demo ===")
        print(f"Log file created at: {log_path}")
        print("\nLog contents:")
        print(log_path.read_text())

        print("\n=== Key Benefits ===")
        print("✓ URL config file stays pristine (no comments added)")
        print("✓ All discovery tracking is in the dedicated TOML log file")
        print("✓ Human-readable TOML format")
        print("✓ Easy to query discovery history programmatically")
        print("✓ Clean separation of concerns")

        # Demonstrate querying (if tomlkit is available)
        try:
            import tomlkit

            print("\n=== Querying Discovery Info (TOML) ===")
            log_data = tomlkit.loads(toml_content)
            key = "mro.ctx.edr"

            if key in log_data["discoveries"]:
                discovery_info = log_data["discoveries"][key]
                print(f"URL for {key}: {discovery_info['url']}")
                print(f"Discovered at: {discovery_info['discovered_at']}")
                print(f"Was update: {discovery_info['is_update']}")
                if discovery_info.get("previous_url"):
                    print(f"Previous URL: {discovery_info['previous_url']}")
        except ImportError:
            print("\n=== TOML parsing not available in current environment ===")
            print("In production, discovery info would be easily accessible")


def run_all_tests():
    """Run all tests."""
    print("Running clean logging tests...\n")

    test_index_access_log_discovery()
    test_get_discovery_info()
    test_get_all_discoveries()

    print("\n" + "=" * 50)
    print("All tests completed!")
    print("=" * 50 + "\n")

    # Run the demonstration
    demonstrate_clean_logging()


if __name__ == "__main__":
    run_all_tests()
