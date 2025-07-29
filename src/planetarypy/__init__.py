"""Top-level package for planetarypy."""

__author__ = """PlanetaryPy Developers"""
__email__ = "kmichael.aye@gmail.com"
__version__ = "0.2.1"


# as advised by logger docs, for library use via `import planetarypy` etc.:
# https://loguru.readthedocs.io/en/stable/resources/recipes.html#configuring-loguru-to-be-used-by-a-library-or-an-application
# the user shall be responsible to enable logging using logger.enable("planetarypy")
from loguru import logger
logger.disable("planetarypy")