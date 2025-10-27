"""Top-level package for planetarypy."""

__author__ = """PlanetaryPy Developers"""
__email__ = "kmichael.aye@gmail.com"
__version__ = "0.2.1"

__all__ = ["enable_logging"]


# as advised by logger docs, for library use via `import planetarypy` etc.:
# https://loguru.readthedocs.io/en/stable/resources/recipes.html#configuring-loguru-to-be-used-by-a-library-or-an-application
# the user shall be responsible to enable logging using logger.enable("planetarypy")
from loguru import logger
import sys

# Pre-configure the handler with INFO level and nice formatting
# This will be used when the user calls logger.enable("planetarypy")
logger.remove()  # Remove default handler
logger.add(
    sys.stderr,
    level="INFO",  # Default to INFO level
    filter=lambda record: record["name"].startswith("planetarypy"),
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
)

# Disable planetarypy logging by default (user must explicitly enable)
logger.disable("planetarypy")


def enable_logging(level: str = "INFO") -> None:
    """Enable planetarypy logging with specified level.
    
    This is a convenience function that both enables the planetarypy namespace
    and optionally reconfigures the handler to a different level.
    
    For most users, simply calling logger.enable("planetarypy") after importing
    will enable INFO-level logging. Use this function if you want a different level.
    
    Args:
        level: Minimum log level to display. Options: "DEBUG", "INFO", "WARNING", "ERROR".
               Default is "INFO" which shows informational messages without verbose debug output.
    
    Examples:
        >>> import planetarypy
        >>> from loguru import logger
        >>> logger.enable("planetarypy")  # Uses pre-configured INFO level
        
        >>> # Or use the convenience function to change level:
        >>> planetarypy.enable_logging("DEBUG")  # Shows all messages including DEBUG
        >>> planetarypy.enable_logging("WARNING")  # Shows only WARNING and ERROR
    """
    logger.enable("planetarypy")
    # Reconfigure handler if a different level is requested
    if level != "INFO":
        logger.remove()
        logger.add(
            sys.stderr,
            level=level,
            filter=lambda record: record["name"].startswith("planetarypy"),
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
        )