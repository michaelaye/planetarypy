"""Dynamic index management

These are the codes for web-scraped PDS archive pages to discover the most recent 
volume delivery with a new index file and its URL.
"""
__all__ = ["DYNAMIC_URL_HANDLERS", "DynamicRemoteHandler"]

from loguru import logger

from .dynamic_url_handlers import CTXIndex, LROCIndex
from .index_logging import AccessLog

# Dynamic URL handlers registry
DYNAMIC_URL_HANDLERS = {
    "mro.ctx.edr": CTXIndex,
    "lro.lroc.edr": LROCIndex,
}


class DynamicRemoteHandler:
    """Manages dynamic index URLs and their discovery/caching.
    
    It uses the AccessLog to track TWO URLs:
    1. current_url: The URL of the locally cached index
    2. available_url: The URL of a newly discovered update (if any)
    
    This separation ensures we always know which URL corresponds to our cached data.

    Parameters
    ----------
    index_key : str
        The dotted index key (e.g. "mro.ctx.edr").     
    """
 
    def __init__(self, index_key : str):
        self.key = index_key
        self.log = AccessLog(key=index_key)
        if self.should_check:
            self._check_for_updates()
    
    @property
    def handler_class(self) -> type | None:
        """Get handler class."""
        return DYNAMIC_URL_HANDLERS.get(self.key)


    @property
    def should_check(self) -> bool:
        """Determine if we should check for updates based on last check time."""
        return self.log.should_check

    def discover_latest_url(self) -> str | None:
        """Discover the latest index URL from the remote source.
                
        This method only discovers and returns the URL - it does NOT update any logs.
        Logging is handled by the caller based on what they want to do with the URL.
        """
        key = self.key
        handler_class = self.handler_class
        if not handler_class:
            raise ValueError(f"No dynamic handler available for {key}")

        try:
            logger.debug(
                f"Discovering latest URL for {key} using {handler_class.__name__}"
            )
            handler = handler_class()
            latest_url = str(handler.latest_index_label_url)
            return latest_url if latest_url else None

        except Exception as e:
            logger.error(f"Error discovering URL for {key}: {e}")
            return None
    
    def _check_for_updates(self) -> None:
        """Check for new URLs and log if an update is available."""
        latest_url = self.discover_latest_url()
        
        if not latest_url:
            logger.warning(f"No URL discovered for {self.key}")
            return
        
        current = self.log.current_url
        
        if not current:
            # First time - no local cache yet
            # Store the URL as available_url so it can be used for download
            logger.info(f"First discovery for {self.key}: {latest_url}")
            self.log.log_available_url(latest_url)
        elif latest_url != current:
            # New URL found - this is an update!
            logger.warning(f"New index available for {self.key}: {latest_url}")
            self.log.log_available_url(latest_url)
        else:
            # Same URL - no update
            logger.debug(f"No update for {self.key}, still at {current}")
            self.log.log_check_time()

    @property
    def update_available(self) -> bool:
        """Check if an update is available.
        
        Returns True if we've logged an available_url that differs from current_url.
        """
        # Check the logged flag first (fast path for second calls)
        if self.log.update_available:
            return True
        
        # If we shouldn't check remote, return False
        if not self.should_check:
            logger.debug(f"Skipping update check for {self.key}, checked recently.")
            return False
        
        # Check for updates (this will log if found)
        self._check_for_updates()
        
        # Return the logged status
        return self.log.update_available or False

    @property
    def url(self) -> str | None:
        """Get the URL to use for downloading.
        
        Returns available_url if an update is available, otherwise current_url.
        If neither exists, discovers the latest URL.
        """
        # If we have an available update, return that URL
        available = self.log.available_url
        if available:
            return available
        
        # Otherwise return the current URL
        current = self.log.current_url
        if current:
            return current
        
        # If we have no URLs at all, discover the latest
        # (this handles first-time usage)
        if self.should_check:
            return self.discover_latest_url()
        
        return None
