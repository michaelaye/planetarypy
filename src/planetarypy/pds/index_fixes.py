"""Index-specific data formatting fixes.

This module contains workarounds for known formatting issues in PDS index
files. Each fixer is either a file-level fixer (applied to the raw table file
before parsing) or a DataFrame-level fixer (applied after parsing but before
time conversions).
"""

from pathlib import Path
from loguru import logger
import pandas as pd
import numpy as np


def replace_in_dataframe(df, old_text, new_text, columns=None, regex=False, inplace=False):
    """Replace text in a pandas DataFrame string columns.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame to operate on.
    old_text : str
        Text or pattern to replace.
    new_text : str
        Replacement text.
    columns : list[str] or None, optional
        Columns to restrict replacements to. If None, all string-like columns
        are used.
    regex : bool, optional
        Whether to interpret ``old_text`` as a regular expression.
    inplace : bool, optional
        If True, modify the original DataFrame and return it. Otherwise a copy
        is returned.

    Returns
    -------
    pandas.DataFrame
        DataFrame with replacements applied.
    """
    target_df = df if inplace else df.copy()
    if columns is None:
        # Use columns with string/object dtype or that support .str accessor
        try:
            cols = [c for c in target_df.columns if hasattr(target_df[c], "str")]
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
    """Simple in-place text replacement in a file.

    Parameters
    ----------
    filename : str or Path
        Path to the file to modify.
    old_text : str
        Text to replace.
    new_text : str
        Replacement text.
    """
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

    The MER Pancam RDR index historically has missing timezone markers ("Z") at
    the end of time strings and non-numeric values like "TBD" in the
    ``RELEASE_ID`` column.

    Parameters
    ----------
    df : pandas.DataFrame
        Parsed index DataFrame.

    Returns
    -------
    pandas.DataFrame
        Fixed DataFrame.
    """
    logger.debug("Applying DataFrame-level fix for MER Pancam RDR index.")
    df = df.copy()

    tcols = [col for col in df.columns if "TIME" in col]
    for col in tcols:
        df[col] = df[col].apply(lambda x: x + "Z" if isinstance(x, str) and not x.endswith("Z") else x)

    if "RELEASE_ID" in df.columns:
        df["RELEASE_ID"] = pd.to_numeric(df["RELEASE_ID"], errors="coerce")

    return df


def fix_go_ssi_file(table_path: str | Path) -> None:
    """File-level fix for Galileo SSI index before parsing.

    The GO SSI index has a malformed value with a quote instead of a comma that
    must be fixed in the raw table file prior to parsing.
    """
    logger.debug("Applying file-level fix for GO SSI index.")
    replace_in_file(table_path, '-23.629"', '-23.629,')


def fix_lro_lola_rdr_df(df):
    """Fix PRODUCT_CREATION_TIME column in lro.lola.rdr index.

    If the value contains only a date (YYYY-MM-DD) the missing time portion is
    filled with ``T00:00:00`` so that the column can be parsed uniformly as a
    full datetime.

    Parameters
    ----------
    df : pandas.DataFrame
        Parsed index DataFrame.

    Returns
    -------
    pandas.DataFrame
        Modified DataFrame with ``PRODUCT_CREATION_TIME`` parsed to datetimes.
    """
    logger.debug("Applying DataFrame-level fix for lro.lola.rdr index PRODUCT_CREATION_TIME column.")
    df = df.copy()
    col = "PRODUCT_CREATION_TIME"
    if col in df.columns:
        def fix_time(val):
            if pd.isna(val):
                return np.nan
            val = str(val).strip()
            if not val:
                return np.nan
            if len(val) == 10 and val.count("-") == 2:
                val = val + "T00:00:00"
            return val
        df[col] = df[col].apply(fix_time)
        df[col] = pd.to_datetime(df[col], format="%Y-%m-%dT%H:%M:%S", errors="coerce")
    return df


def apply_file_fixer(index_key: str, table_path: str | Path) -> None:
    """Apply a file-level fixer for the given index key if registered.

    Parameters
    ----------
    index_key : str
        The dotted index key (e.g. "go.ssi.index").
    table_path : str or Path
        Path to the table file to fix.
    """
    fixer = FILE_FIXES.get(index_key)
    if fixer:
        try:
            fixer(table_path)
        except Exception as e:
            logger.warning(f"apply_file_fixer failed for {index_key} on {table_path}: {e}")


def apply_pre_time_df_fixer(index_key: str, df):
    """Apply a DataFrame-level fixer prior to time conversion if registered.

    Parameters
    ----------
    index_key : str
        The dotted index key.
    df : pandas.DataFrame
        The parsed DataFrame.

    Returns
    -------
    pandas.DataFrame
        Potentially modified DataFrame.
    """
    fixer = DF_PRETIME_FIXES.get(index_key)
    if fixer:
        try:
            return fixer(df)
        except Exception as e:
            logger.warning(f"apply_pre_time_df_fixer failed for {index_key}: {e}")
            return df
    return df


# Registries
# File-level fixes to apply BEFORE parsing
FILE_FIXES = {
    "go.ssi.index": fix_go_ssi_file,
}
# DataFrame-level fixes to apply BEFORE time conversion
DF_PRETIME_FIXES = {
    "mer.opportunity.pancam_rdr": fix_mer_rdr_df,
    "mer.spirit.pancam_rdr": fix_mer_rdr_df,
    "lro.lola.rdr": fix_lro_lola_rdr_df,
}

