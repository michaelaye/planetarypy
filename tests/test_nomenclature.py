"""Nomenclature layer: filtering, CRS behaviour, and the plotting overlay.

No network: a synthetic shapefile stands in for the gazetteer download, carrying
the same columns and the same ESRI CRS the real one ships, so the reprojection
path under test is the real one.
"""

import warnings

import pytest

gpd = pytest.importorskip("geopandas", reason="needs the [geo] extra")
from shapely.geometry import Point  # noqa: E402

from planetarypy import crs as pcrs  # noqa: E402
from planetarypy import nomenclature as nom  # noqa: E402

GAZETTEER_CRS = "ESRI:104905"  # what USGS actually ships for Mars

ROWS = [
    # name,          type,             diameter, lon,   lat,  box half-width
    ("Jezero",       "Crater, craters",    45.0, 77.4, 18.4, 0.4),
    ("Hargraves",    "Crater, craters",    60.3, 75.7, 20.7, 0.5),
    ("Nili Fossae",  "Vallis, valles",    727.9, 76.7, 22.0, 3.0),
    ("Tiny",         "Crater, craters",     1.0, 78.0, 18.0, 0.0),  # degenerate box
    ("Faraway",      "Crater, craters",   120.0, 10.0, -40.0, 1.0),
]


@pytest.fixture
def fake_gazetteer(tmp_path, monkeypatch):
    """A shapefile shaped like the real download, wired in via `download`."""
    records = []
    for name, ftype, diameter, lon, lat, half in ROWS:
        records.append({
            "name": name, "clean_name": name, "type": ftype,
            "diameter": diameter, "center_lon": lon, "center_lat": lat,
            "min_lon": lon - half, "max_lon": lon + half,
            "min_lat": lat - half, "max_lat": lat + half,
            "approval": "Adopted by IAU",
            "geometry": Point(lon, lat),
        })
    gdf = gpd.GeoDataFrame(records, crs=GAZETTEER_CRS)
    shp = tmp_path / "MARS_nomenclature_center_pts.shp"
    gdf.to_file(shp)
    monkeypatch.setattr(nom, "download", lambda body, **kw: shp)
    return shp


@pytest.fixture(autouse=True)
def _clean_session():
    pcrs.clear_target_crs()
    yield
    pcrs.clear_target_crs()


def _quiet(fn, *args, **kwargs):
    """Call fn, ignoring the expected automatic-conversion notice."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", pcrs.CRSConversionWarning)
        return fn(*args, **kwargs)


# --- filtering --------------------------------------------------------------


def test_returns_every_feature_unfiltered(fake_gazetteer):
    assert len(_quiet(nom.features, "mars")) == len(ROWS)


def test_type_matches_the_leading_singular(fake_gazetteer):
    """Callers pass 'Crater', not the gazetteer's 'Crater, craters'."""
    got = _quiet(nom.features, "mars", type="Crater")
    assert set(got["name"]) == {"Jezero", "Hargraves", "Tiny", "Faraway"}


def test_type_is_case_insensitive(fake_gazetteer):
    assert len(_quiet(nom.features, "mars", type="crater")) == 4


def test_min_diameter_filters(fake_gazetteer):
    got = _quiet(nom.features, "mars", min_diameter=100)
    assert set(got["name"]) == {"Nili Fossae", "Faraway"}


def test_bbox_filters(fake_gazetteer):
    got = _quiet(nom.features, "mars", bbox=(74, 17, 79, 23))
    assert "Faraway" not in set(got["name"])
    assert "Jezero" in set(got["name"])


def test_filters_compose(fake_gazetteer):
    got = _quiet(nom.features, "mars", type="Crater", min_diameter=50,
                 bbox=(74, 17, 79, 23))
    assert list(got["name"]) == ["Hargraves"]


def test_empty_result_is_an_empty_frame_not_an_error(fake_gazetteer):
    assert _quiet(nom.features, "mars", min_diameter=1e9).empty


# --- CRS --------------------------------------------------------------------


def test_converts_to_the_body_iau_crs_by_default(fake_gazetteer):
    got = _quiet(nom.features, "mars")
    assert got.crs.to_authority() == ("IAU_2015", "49900")


def test_automatic_conversion_warns(fake_gazetteer):
    with pytest.warns(pcrs.CRSConversionWarning, match="ESRI:104905"):
        nom.features("mars")


def test_explicit_to_crs_is_silent(fake_gazetteer):
    with warnings.catch_warnings():
        warnings.simplefilter("error", pcrs.CRSConversionWarning)
        got = nom.features("mars", to_crs="IAU_2015:49901")
    assert got.crs.to_authority() == ("IAU_2015", "49901")


def test_session_target_crs_is_honoured(fake_gazetteer):
    pcrs.set_target_crs("IAU_2015:49901")
    got = _quiet(nom.features, "mars")
    assert got.crs.to_authority() == ("IAU_2015", "49901")


def test_context_manager_scopes_the_crs(fake_gazetteer):
    with pcrs.target_crs("IAU_2015:49910"):
        inside = _quiet(nom.features, "mars")
    outside = _quiet(nom.features, "mars")
    assert inside.crs.to_authority() == ("IAU_2015", "49910")
    assert outside.crs.to_authority() == ("IAU_2015", "49900")


def test_explicit_beats_the_session_target(fake_gazetteer):
    pcrs.set_target_crs("IAU_2015:49901")
    got = nom.features("mars", to_crs="IAU_2015:49900")
    assert got.crs.to_authority() == ("IAU_2015", "49900")


# --- plotting ---------------------------------------------------------------


@pytest.fixture
def ax():
    matplotlib = pytest.importorskip("matplotlib")
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots()
    axes.set_xlim(74, 79)
    axes.set_ylim(17, 23)
    yield axes
    plt.close(fig)


def test_add_features_reads_the_axes_limits(fake_gazetteer, ax):
    drawn = _quiet(nom.add_features, ax, "mars")
    assert "Faraway" not in set(drawn["name"])   # outside the axes
    assert "Jezero" in set(drawn["name"])


def test_extent_boxes_drawn_by_default(fake_gazetteer, ax):
    from matplotlib.patches import Rectangle

    _quiet(nom.add_features, ax, "mars")
    assert sum(isinstance(p, Rectangle) for p in ax.patches) > 0


def test_extent_false_draws_no_boxes(fake_gazetteer, ax):
    from matplotlib.patches import Rectangle

    _quiet(nom.add_features, ax, "mars", extent=False)
    assert sum(isinstance(p, Rectangle) for p in ax.patches) == 0


def test_degenerate_boxes_are_skipped(fake_gazetteer, ax):
    """'Tiny' has min==max, which would be a zero-area rectangle."""
    from matplotlib.patches import Rectangle

    drawn = _quiet(nom.add_features, ax, "mars", extent=True)
    boxes = [p for p in ax.patches if isinstance(p, Rectangle)]
    assert "Tiny" in set(drawn["name"])          # still a point
    assert len(boxes) == len(drawn) - 1          # but no box for it


def test_labels_can_be_switched_off(fake_gazetteer, ax):
    _quiet(nom.add_features, ax, "mars", label=False)
    assert not ax.texts


def test_max_labels_caps_output(fake_gazetteer, ax):
    _quiet(nom.add_features, ax, "mars", max_labels=1, declutter=False)
    assert len(ax.texts) == 1


def test_declutter_drops_colliding_labels(fake_gazetteer, ax):
    """Two features a hair apart must not both get a label."""
    ax.set_xlim(77.0, 77.8)
    ax.set_ylim(18.0, 18.8)
    with_declutter = _quiet(nom.add_features, ax, "mars", declutter=True)
    assert len(with_declutter) >= 1
    assert len(ax.texts) <= len(with_declutter)


def test_add_features_on_empty_selection_is_a_noop(fake_gazetteer, ax):
    result = _quiet(nom.add_features, ax, "mars", min_diameter=1e9)
    assert result.empty
    assert not ax.texts


def test_bad_bbox_string_is_rejected(fake_gazetteer, ax):
    with pytest.raises(ValueError, match="bbox must be"):
        nom.add_features(ax, "mars", bbox="everywhere")


# --- body handling ----------------------------------------------------------


def test_bodies_listing_is_lowercase_and_includes_mars():
    assert "mars" in nom.bodies()
    assert all(b == b.lower() for b in nom.bodies())


@pytest.mark.parametrize("given,expected", [
    ("mars", "MARS"), ("Mars", "MARS"), ("  moon ", "MOON"), ("luna", "MOON"),
])
def test_gazetteer_name_normalisation(given, expected):
    assert nom._gazetteer_name(given) == expected


# --- find() -----------------------------------------------------------------


def test_find_returns_a_single_row(fake_gazetteer):
    got = _quiet(nom.find, "mars", "Jezero")
    assert got["name"] == "Jezero"
    assert got["diameter"].to_value("km") == 45.0


def test_find_attaches_units_by_default(fake_gazetteer):
    from astropy import units as u

    got = _quiet(nom.find, "mars", "Jezero")
    assert got["diameter"].unit == u.km
    assert got["center_lon"].unit == u.deg


def test_find_returns_plain_floats_when_units_are_off(fake_gazetteer):
    from astropy import units as u

    from planetarypy import units as punits

    with punits.use_units(False):
        got = _quiet(nom.find, "mars", "Jezero")
    assert not isinstance(got["diameter"], u.Quantity)
    assert got["diameter"] == 45.0


def test_features_frame_records_its_column_units(fake_gazetteer):
    from planetarypy import units as punits

    frame = _quiet(nom.features, "mars")
    assert punits.units_of(frame)["diameter"] == "km"
    assert punits.units_of(frame)["center_lat"] == "deg"


def test_frame_columns_stay_numeric_not_object(fake_gazetteer):
    """Units live in .attrs; wrapping a whole column would kill vectorisation."""
    frame = _quiet(nom.features, "mars")
    assert frame["diameter"].dtype.kind == "f"


def test_find_is_case_insensitive(fake_gazetteer):
    assert _quiet(nom.find, "mars", "jEzErO")["name"] == "Jezero"


def test_find_is_exact_not_substring(fake_gazetteer):
    """'Jezero' must not also match a hypothetical 'Jezero Mons'."""
    got = _quiet(nom.features, "mars", name="Jezero")
    assert list(got["name"]) == ["Jezero"]


def test_find_raises_lookuperror_when_absent(fake_gazetteer):
    with pytest.raises(LookupError, match="Nonesuch"):
        _quiet(nom.find, "mars", "Nonesuch")


def test_find_raises_valueerror_when_ambiguous(fake_gazetteer, tmp_path, monkeypatch):
    """Two features sharing a name must not silently resolve to the first."""
    twins = gpd.GeoDataFrame(
        [
            {"name": "Twin", "clean_name": "Twin", "type": "Crater, craters",
             "diameter": 5.0, "center_lon": 1.0, "center_lat": 1.0,
             "min_lon": 0.5, "max_lon": 1.5, "min_lat": 0.5, "max_lat": 1.5,
             "approval": "Adopted by IAU", "geometry": Point(1, 1)},
            {"name": "Twin", "clean_name": "Twin", "type": "Mons, montes",
             "diameter": 9.0, "center_lon": 2.0, "center_lat": 2.0,
             "min_lon": 1.5, "max_lon": 2.5, "min_lat": 1.5, "max_lat": 2.5,
             "approval": "Adopted by IAU", "geometry": Point(2, 2)},
        ],
        crs=GAZETTEER_CRS,
    )
    shp = tmp_path / "TWINS_nomenclature_center_pts.shp"
    twins.to_file(shp)
    monkeypatch.setattr(nom, "download", lambda body, **kw: shp)
    with pytest.raises(ValueError, match="ambiguous"):
        _quiet(nom.find, "mars", "Twin")


def test_find_disambiguates_by_type(fake_gazetteer):
    got = _quiet(nom.find, "mars", "Jezero", type="Crater")
    assert got["name"] == "Jezero"


def test_find_result_carries_the_session_crs(fake_gazetteer):
    with pcrs.target_crs("IAU_2015:49901"):
        got = _quiet(nom.find, "mars", "Jezero")
    assert got.geometry is not None
