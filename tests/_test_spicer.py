import datetime as dt

import numpy as np
import pytest

from planetarypy.spice.spicer import IllumAngles, MarsSpicer, Spicer, SurfaceCoords


def test_spicer_initialization():
    """Test basic Spicer initialization."""
    spicer = Spicer("MARS", time="2020-01-01T00:00:00")
    assert spicer.body == "MARS"
    assert isinstance(spicer.time, dt.datetime)


def test_illum_angles():
    """Test IllumAngles class."""
    angles = IllumAngles(phase=30, solar=45, emission=60)
    assert angles.dphase.value == pytest.approx(30)
    assert angles.dsolar.value == pytest.approx(45)
    assert angles.demission.value == pytest.approx(60)


def test_surface_coords():
    """Test SurfaceCoords class."""
    coords = SurfaceCoords(lon=180, lat=45, radius=3396.2)
    assert coords.dlon.value == pytest.approx(180)
    assert coords.dlat.value == pytest.approx(45)
    assert coords.radius.value == pytest.approx(3396.2)


def test_mars_spicer():
    """Test MarsSpicer specific functionality."""
    mars = MarsSpicer(time="2020-01-01T00:00:00")
    assert mars.target == "MARS"
    assert mars.ref_frame == "IAU_MARS"


@pytest.mark.parametrize(
    "time_str",
    [
        "2020-01-01T00:00:00",
        "2020-06-15T12:30:45",
    ],
)
def test_spicer_time_handling(time_str):
    """Test time handling in Spicer."""
    spicer = Spicer("MARS", time=time_str)
    assert isinstance(spicer.time, dt.datetime)
    assert spicer.utc == spicer.time.isoformat()


def test_make_axis_rotation_matrix():
    """Test the make_axis_rotation_matrix function with numeric checks."""
    from planetarypy.spice.spicer import make_axis_rotation_matrix

    # Test 1: 90-degree rotation around z-axis
    z_axis = np.array([0, 0, 1])
    rot_z = make_axis_rotation_matrix(z_axis, np.pi / 2)
    expected_z = np.array([[0, -1, 0], [1, 0, 0], [0, 0, 1]])
    np.testing.assert_allclose(rot_z, expected_z, rtol=1e-10)

    # Test 2: 180-degree rotation around x-axis
    x_axis = np.array([1, 0, 0])
    rot_x = make_axis_rotation_matrix(x_axis, np.pi)
    expected_x = np.array([[1, 0, 0], [0, -1, 0], [0, 0, -1]])
    np.testing.assert_allclose(rot_x, expected_x, rtol=1e-10)

    # Test 3: Verify rotation matrix properties
    # 3.1: Orthogonality (R * R^T = I)
    arbitrary_axis = np.array([1, 1, 1])
    arbitrary_axis /= np.linalg.norm(arbitrary_axis)
    arbitrary_angle = np.pi / 4
    R = make_axis_rotation_matrix(arbitrary_axis, arbitrary_angle)
    np.testing.assert_allclose(R @ R.T, np.eye(3), rtol=1e-10)

    # 3.2: Determinant should be 1
    assert np.abs(np.linalg.det(R) - 1.0) < 1e-10

    # 3.3: Eigenvalues should be 1, e^(iθ), e^(-iθ)
    eigenvalues = np.linalg.eigvals(R)
    # Sort eigenvalues by magnitude to handle numerical precision
    eigenvalues = sorted(eigenvalues, key=lambda x: abs(x))
    np.testing.assert_allclose(eigenvalues[0], 1.0, rtol=1e-10)
    np.testing.assert_allclose(abs(eigenvalues[1]), 1.0, rtol=1e-10)
    np.testing.assert_allclose(abs(eigenvalues[2]), 1.0, rtol=1e-10)

    # Test 4: Verify rotation preserves vector length
    test_vector = np.array([1.0, 2.0, 3.0])
    rotated_vector = R @ test_vector
    np.testing.assert_allclose(
        np.linalg.norm(test_vector), np.linalg.norm(rotated_vector), rtol=1e-10
    )

    # Test 5: Verify rotation around arbitrary axis preserves the axis
    arbitrary_axis = np.array([1.0, 2.0, 3.0])
    arbitrary_axis /= np.linalg.norm(arbitrary_axis)
    arbitrary_angle = np.pi / 3
    R = make_axis_rotation_matrix(arbitrary_axis, arbitrary_angle)
    # The axis should be an eigenvector with eigenvalue 1
    np.testing.assert_allclose(R @ arbitrary_axis, arbitrary_axis, rtol=1e-10)
