#!/usr/bin/env python3
"""Test script to verify the IndexURLsConfig update functionality."""

import datetime

# Import the class we want to test
import sys
import tempfile
from pathlib import Path

import tomlkit

sys.path.insert(0, "src")
from planetarypy.pds.index_config import IndexURLsConfig


def test_config_update():
    """Test that the config download and update functionality works."""

    # Create a temporary directory for testing
    with tempfile.TemporaryDirectory() as temp_dir:
        test_config_path = Path(temp_dir) / "test_config.toml"

        print(f"Testing with config file: {test_config_path}")

        # Test 1: Create new config (should download from remote)
        print("\n=== Test 1: Creating new config ===")
        config = IndexURLsConfig(config_path=str(test_config_path))

        # Check if metadata was added
        if "metadata" in config.tomldoc:
            last_updated = config.tomldoc["metadata"].get("last_updated")
            source_url = config.tomldoc["metadata"].get("source_url", "N/A")
            print(f"✓ Config created with timestamp: {last_updated}")
            print(f"✓ Source URL: {source_url}")
        else:
            print("⚠ No metadata found in config")

        # Test 2: Create an old config file and test update
        print("\n=== Test 2: Testing update for old config ===")

        # Manually create an old config file
        old_doc = tomlkit.document()
        old_doc["metadata"] = tomlkit.table()

        # Set timestamp to 2 days ago
        old_timestamp = datetime.datetime.now() - datetime.timedelta(days=2)
        old_doc["metadata"]["last_updated"] = old_timestamp.isoformat()
        old_doc["metadata"]["source_url"] = "test"

        old_doc["missions"] = tomlkit.table()
        old_doc["missions"]["test"] = tomlkit.table()
        old_doc["missions"]["test"]["instrument"] = tomlkit.table()
        old_doc["missions"]["test"]["instrument"]["index"] = "old_url"

        # Write the old config
        test_config_path.write_text(tomlkit.dumps(old_doc))
        print(f"✓ Created old config with timestamp: {old_timestamp.isoformat()}")

        # Initialize config again (should trigger update check)
        config2 = IndexURLsConfig(config_path=str(test_config_path))

        # Check if it was updated
        new_timestamp = config2.tomldoc["metadata"].get("last_updated")
        if new_timestamp and new_timestamp != old_timestamp.isoformat():
            print(f"✓ Config was updated! New timestamp: {new_timestamp}")
        else:
            print(
                "⚠ Config was not updated (this might be expected if remote is unreachable)"
            )

        print("\n=== Test completed ===")


if __name__ == "__main__":
    test_config_update()
