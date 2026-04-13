from pathlib import Path


class PolygonSeederAlgorithm:
    def __init__(self):
        self.Name = None  # Must be defined in subclass
        self.MinimumThickness = 0.0
        self.MinimumArea = 1000
        self.XSpacing = 20000
        self.YSpacing = 10000
        self.PixelsFromEdge = 20.0
        self.MinEmission = 20.0
        self.MaxEmission = 75.0
        self.MinIncidence = 25.0
        self.MaxIncidecne = 80.0  # Note: intentional typo preserved
        self.MinResolution = 255.0
        self.MaxResolution = 259.0
        self.MinDN = 0.145
        self.MaxDN = 0.175

    def to_template_string(self):
        lines = [
            "Object = AutoSeed",
            "   Group = PolygonSeederAlgorithm",
            f"     Name = {self.Name}",
            f"     MinimumThickness = {self.MinimumThickness}",
            f"     MinimumArea = {self.MinimumArea} <meters>",
            f"     XSpacing = {self.XSpacing} <meters>",
            f"     YSpacing = {self.YSpacing} <meters>",
            f"     PixelsFromEdge = {self.PixelsFromEdge}",
            f"     MinEmission = {self.MinEmission}",
            f"     MaxEmission = {self.MaxEmission}",
            f"     MinIncidence = {self.MinIncidence}",
            f"     MaxIncidecne = {self.MaxIncidecne}",
            f"     MinResolution = {self.MinResolution}",
            f"     MaxResolution = {self.MaxResolution}",
            "     # MinDN and MaxDN are costly checks",
            f"     MinDN = {self.MinDN}",
            f"     MaxDN = {self.MaxDN}",
        ]

        # Add extra attributes if they exist
        if hasattr(self, "MajorAxisPoints"):
            lines.append(f"     MajorAxisPoints = {self.MajorAxisPoints}")
        if hasattr(self, "MinorAxisPoints"):
            lines.append(f"     MinorAxisPoints = {self.MinorAxisPoints}")

        lines.extend(["   EndGroup", "EndObject"])
        return "\n".join(lines)

    def write(self, filename=None):
        if filename is None:
            filename = f"{self.__class__.__name__.lower()}.def"
        Path(filename).write_text(self.to_template_string())
        print("created", filename)


class Strip(PolygonSeederAlgorithm):
    """Seeds the polygon with Control Points by creating a grid, the centermost
    grid is placed in the center of the polygon. Then, 2 points within each
    grid is checked for overlap. One of these points lies 1/6th of the grid
    dimension up and left from the grid's center point, while the other checked
    point lies 1/6th of the grid dimension down and right from the grid's center
    point. If these new points contain overlap, then they are seeded with a
    Control Point.The following XSpacing/YSpacing parameters denote the dimensions
    of the grid.
    Strip is best used on polygons whos overlaps results in strips of data,
    such as polygons from line scan cameras.

    The minimum thickness required to seed the polygon with Control Points. A
    thickness of 1.0 means that the polygon is a square. A thickness of 0.5
    means that the polygon is a 2:1 rectangle.  A thickness of 0.25 means that
    the polygon is a 4:1 rectangle, and so forth.
    Any polygon with a thickness less than MinimumThickness will not be seeded.

    The minimum area (in square meters) required to seed the polygon with
    Control Points. Any polygon with an area less than MinimumArea will not be
    seeded.

    The spacing in meters between Control Points in the X direction. One
    Control Point will be placed every XSpacing meters in the X direction.
    This combined with the YSpacing, will create a grid of Control Points
    across the polygon.

    The spacing in meters between Control Points in the Y direction. One
    Control Point will be placed every YSpacing meters in the Y direction.
    This combined with the XSpacing, will create a grid of Control Points
    across the polygon.
    """

    Name = "Strip"


class Grid(PolygonSeederAlgorithm):
    """Seeds the polygon with Control Points by creating a grid, the centermost grid
    is placed in the center of the polygon. Then, one Control Point is placed in the
    center of each grid where there is overlap at that center of that grid square.
    The following XSpacing/YSpacing parameters denote the dimensions of the grid.

    The minimum thickness required to seed the polygon with Control Points. A
    thickness of 1.0 means that the polygon is a square. A thickness of 0.5
    means that the polygon is a 2:1 rectangle.  A thickness of 0.25 means that
    the polygon is a 4:1 rectangle, and so forth.
    Any polygon with a thickness less than MinimumThickness will not be seeded.

    The minimum area (in square meters) required to seed the polygon with
    Control Points. Any polygon with an area less than MinimumArea will not be
    seeded.

    The spacing in meters between Control Points in the X direction. One
    Control Point will be placed every XSpacing meters in the X direction.
    This combined with the YSpacing, will create a grid of Control Points
    across the polygon.

    The spacing in meters between Control Points in the Y direction. One
    Control Point will be placed every YSpacing meters in the Y direction.
    This combined with the XSpacing, will create a grid of Control Points
    across the polygon.
    """

    Name = "Grid"


class Limit(PolygonSeederAlgorithm):
    """Seeds the polygons with Control Points by creating a grid, the centermost
    grid is placed in the center of the polygon. Each grid square is then
    checked. If the grid square contains any overlap, a box is then created
    around the overlap contained within the grid square. The center point of
    that outlined region is then checked for overlap. If overlap exists on the
    center point of the outlined box, a Control Point is created on that point.
    Else no control point is created for that grid selection.
    Grid square dimensions are created by MajorAxisPoints/MinorAxisPoints as
    described below.

    The minimum thickness required to seed the polygon with Control Points. A
    thickness of 1.0 means that the polygon is a square. A thickness of 0.5
    means that the polygon is a 2:1 rectangle.  A thickness of 0.25 means that
    the polygon is a 4:1 rectangle, and so forth.
    Any polygon with a thickness less than MinimumThickness will not be seeded.

    The minimum area (in square meters) required to seed the polygon with
    Control Points. Any polygon with an area less than MinimumArea will not be
    seeded.

    The number of points to place on the major axis, where the major axis is
    the x or y axis that is the greatest in length.
    A MajorAxisPoints of 2 combined with a MinorAxisPoints of 2 means that
    every polygon which passes the MinimumThickness and MinimumArea checks,
    will be seeded by 4 points (an evenly distributed 2x2 matrix).

    The number of points to place on the minor axis, where the minor axis is
    the x or y axis that is the least in length.
    A MinorAxisPoints of 2 combined with a MajorAxisPoints of 2 means that
    every polygon which passes the MinimumThickness and MinimumArea checks,
    will be seeded by 4 points (an evenly distributed 2x2 matrix).
    """

    Name = "Limit"

    def __init__(self):
        super().__init__()
        self.MajorAxisPoints = 2
        self.MinorAxisPoints = 2
