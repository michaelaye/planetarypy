import pytest
from planetarypy.exceptions import (
    Error, SomethingNotSetError, ProjectionNotSetError,
    GeoTransformNotSetError, SpicerError, SPointNotSetError,
    ObserverNotSetError, SpiceError, MissingParameterError
)

def test_base_error():
    """Test base Error class."""
    with pytest.raises(Error):
        raise Error()

def test_something_not_set_error():
    """Test SomethingNotSetError formatting."""
    error = SomethingNotSetError("location", "parameter")
    assert str(error) == "parameter not set in location"

def test_projection_not_set_error():
    """Test ProjectionNotSetError."""
    error = ProjectionNotSetError("location")
    assert "Projection" in str(error)

def test_spoint_not_set_error():
    """Test SPointNotSetError."""
    error = SPointNotSetError("test message")
    assert "spoint" in str(error).lower()

def test_missing_parameter_error():
    """Test MissingParameterError."""
    error = MissingParameterError("required_param")
    assert "required_param" in str(error)

def test_geotransform_not_set_error():
    """Test GeoTransformNotSetError."""
    error = GeoTransformNotSetError("location")
    assert "GeoTransform" in str(error) 