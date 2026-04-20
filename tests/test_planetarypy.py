#!/usr/bin/env python

"""Tests for `planetarypy` package."""

import pytest


def test_package_imports():
    """Test that main package components are importable."""

def test_version():
    """Test that version string exists."""
    from planetarypy import __version__
    assert isinstance(__version__, str)
    assert len(__version__) > 0

def test_package_metadata():
    """Test package metadata."""
    from planetarypy import __author__, __email__
    assert isinstance(__author__, str)
    assert isinstance(__email__, str)
    assert '@' in __email__

@pytest.mark.skip(reason="Template test - implement actual functionality")
def test_content(response):
    """Sample pytest test function with the pytest fixture as an argument."""
    pass
