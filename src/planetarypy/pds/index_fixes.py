"""Index-specific data formatting fixes.

This module contains workarounds for known formatting issues in PDS index files.
Each function applies fixes for a specific index before parsing.
"""

from pathlib import Path
from loguru import logger

def replace_in_dataframe(df, old_text, new_text, columns=None, regex=False, inplace=False):
    """Replace text in a pandas DataFrame string columns.

    Args:
        df: pandas DataFrame to operate on
        old_text: text or pattern to replace
        new_text: replacement text
        columns: optional list of column names to restrict replacements; when None, all object/string columns are used
        regex: whether to treat old_text as a regex pattern
        inplace: if True, modify df in place and return df

    Returns:
        DataFrame with replacements applied (same object if inplace=True)
    """
    # Avoid importing pandas at module import time; operate generically on DF-like
    target_df = df if inplace else df.copy()
    if columns is None:
        # Use columns with string/object dtype or that support .str accessor
        try:
            cols = [c for c in target_df.columns if hasattr(target_df[c], 'str')]
        except Exception:
            cols = list(target_df.columns)
    else:
        cols = list(columns)

    for col in cols:
        try:
            target_df[col] = target_df[col].astype("string").str.replace(old_text, new_text, regex=regex)
        except Exception as e:
            logger.debug(f"replace_in_dataframe: skipped column {col}: {e}")
            continue
    return target_df

def replace_in_file(filename: str | Path, old_text: str, new_text: str) -> None:
    """Simple in-place text replacement in a file."""
    filename = str(filename)
    with open(filename, "r") as file:
        content = file.read()
    new_content = content.replace(old_text, new_text)
    if new_content != content:
        with open(filename, "w") as file:
            file.write(new_content)
        logger.debug(f"Replaced '{old_text}' with '{new_text}' in {filename}")
    else:
        logger.debug(f"No occurrences of '{old_text}' found in {filename}")


def fix_mer_rdr_df(df):
    """DataFrame-level fix for MER Pancam RDR index.
    
    The MER Pancam RDR index has:
    1. Missing 'Z' at the end of time strings
    2. "TBD" string values in the RELEASE_ID integer column
    
    Args:
        df: pandas DataFrame with parsed index data
        
    Returns:
        DataFrame with fixed time columns and RELEASE_ID
    """
    logger.debug("Applying DataFrame-level fix for MER Pancam RDR index.")
    df = df.copy()
    
    # Fix time columns - add missing 'Z' timezone marker
    tcols = [col for col in df.columns if "TIME" in col]
    for col in tcols:
        df[col] = df[col].apply(lambda x: x + "Z" if isinstance(x, str) and not x.endswith("Z") else x)
    
    # Fix RELEASE_ID column - replace "TBD" with NaN and convert to numeric
    if "RELEASE_ID" in df.columns:
        import pandas as pd
        df["RELEASE_ID"] = pd.to_numeric(df["RELEASE_ID"], errors="coerce")
    
    return df


def fix_go_ssi_file(table_path: str | Path) -> None:
    """File-level fix for Galileo SSI index before parsing.

    The GO SSI index has a malformed value with a quote instead of a comma.
    This must be fixed in the raw table file prior to parsing.
    """
    logger.debug("Applying file-level fix for GO SSI index.")
    replace_in_file(table_path, '-23.629"', '-23.629,')


# Registries
# File-level fixes to apply BEFORE parsing
FILE_FIXES = {
    "go.ssi.index": fix_go_ssi_file,
}

# DataFrame-level fixes to apply AFTER parsing but BEFORE time conversion
DF_PRETIME_FIXES = {
    "mer.opportunity.pancam_rdr": fix_mer_rdr_df,
}


def apply_file_fixer(index_key: str, table_path: str | Path) -> None:
    """Apply a file-level fixer for the given index key if registered."""
    fixer = FILE_FIXES.get(index_key)
    if fixer:
        try:
            fixer(table_path)
        except Exception as e:
            logger.warning(f"apply_file_fixer failed for {index_key} on {table_path}: {e}")


def apply_pre_time_df_fixer(index_key: str, df):
    """Apply a DataFrame-level fixer prior to time conversion if registered.

    Returns the DataFrame (potentially modified).
    """
    fixer = DF_PRETIME_FIXES.get(index_key)
    if fixer:
        try:
            return fixer(df)
        except Exception as e:
            logger.warning(f"apply_pre_time_df_fixer failed for {index_key}: {e}")
            return df
    return df