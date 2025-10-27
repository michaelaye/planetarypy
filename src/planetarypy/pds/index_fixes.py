"""Index-specific data formatting fixes.

This module contains workarounds for known formatting issues in PDS index files.
Each function applies fixes for a specific index before parsing.
"""

from pathlib import Path
from loguru import logger
from planetarypy.pds import utils


def fix_go_ssi_index(table_path: Path) -> None:
    """Fix known formatting issue in Galileo SSI index.
    
    The GO SSI index has a malformed value with a quote instead of a comma.
    
    Args:
        table_path: Path to the table file to fix
    """
    logger.debug("Fixing known formatting issue in GO SSI index.")
    utils.simple_replace_in_file(table_path, '-23.629"', "-23.629,")


# Add more fix functions here as needed:
# def fix_another_index(table_path: Path) -> None:
#     """Fix known formatting issue in Another Index.
#     
#     Description of the issue and fix.
#     
#     Args:
#         table_path: Path to the table file to fix
#     """
#     logger.debug("Fixing known formatting issue in Another Index.")
#     # Apply fixes here
#     pass


# Mapping of index keys to their fix functions
INDEX_FIXES = {
    "go.ssi.index": fix_go_ssi_index,
    # Add more mappings here:
    # "another.index.key": fix_another_index,
}
