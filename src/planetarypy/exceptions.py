"""PlanetarPy exceptions."""

__all__ = [
    "Error",
    "SomethingNotSetError",
    "ProjectionNotSetError",
    "GeoTransformNotSetError",
    "SpicerError",
    "SPointNotSetError",
    "ObserverNotSetError",
    "SpiceError",
    "MissingParameterError",
]


class Error(Exception):
    """Base class for exceptions in this module."""

    pass


class SomethingNotSetError(Error):
    """Exception raised for errors in the input of transformations.

    Attributes:
        where -- where is something missing
        what     -- what is missing
    """

    def __init__(self, where, what):
        self.where = where
        self.what = what

    def __str__(self):
        return "{0} not set in {1}".format(self.what, self.where)


class ProjectionNotSetError(SomethingNotSetError):
    """Exception raised when a projection is not set."""

    def __init__(self, where):
        super().__init__(where, "Projection")


class GeoTransformNotSetError(SomethingNotSetError):
    """Exception raised when a GeoTransform is not set."""

    def __init__(self, where):
        super().__init__(where, "GeoTransform")


class SpicerError(Exception):
    """Base class for exceptions in this module."""

    pass


class SPointNotSetError(SpicerError):
    def __init__(self, txt):
        self.txt = txt

    def __str__(self):
        return """You are trying to use a method that requires that the surface
point is defined. The class member is <spoint>. It can be set using the method
'set_spoint_by'. This operation had no effect."""


class ObserverNotSetError(SpicerError):
    def __str__(self):
        return """The method you called requires an observer to be set.
                  This operation had no effect."""


class SpiceError(SpicerError):
    def __init__(self, function):
        self.function = function

    def __str__(self):
        return "SPICE: Calulating {} failed.".format(self.function)


class MissingParameterError(SpicerError):
    def __init__(self, txt):
        self.txt = txt

    def __str__(self):
        return "Parameter missing: {}".format(self.txt)
