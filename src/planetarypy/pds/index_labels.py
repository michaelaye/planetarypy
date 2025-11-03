"""PDS index label handling and processing.

This module provides classes and functions for working with PDS index labels,
parsing index files, and converting them to convenient data structures.
"""

__all__ = [
    "PVLColumn",
    "IndexLabel",
    "index_to_df",
    "decode_line",
    "find_mixed_type_cols",
]
# Temporarily suppress PendingDeprecationWarning from pvl.collections.Units
# This can be removed once pvl version > 1.3.2 is used
import warnings

import numpy as np
import pandas as pd
import pvl
from fastcore.utils import Path
from loguru import logger
from tqdm.auto import tqdm

from .. import datetime_format_converters as tformats

warnings.filterwarnings("ignore", category=PendingDeprecationWarning, module="pvl")


class PVLColumn:
    """Manages just one of the columns in a table that is described via PVL.

    Parameters
    ----------
    
    pvlobj :
    """

    def __init__(self, pvlobj):
        self.pvlobj = pvlobj

    @property
    def name(self):
        return self.pvlobj["NAME"]

    @property
    def name_as_list(self):
        "needs to return a list for consistency for cases when it's an array."
        if self.items is None:
            return [self.name]
        else:
            return [self.name + "_" + str(i + 1) for i in range(self.items)]

    @property
    def start(self):
        "Decrease by one as Python is 0-indexed."
        return self.pvlobj["START_BYTE"] - 1

    @property
    def stop(self):
        return self.start + self.pvlobj["BYTES"]

    @property
    def items(self):
        return self.pvlobj.get("ITEMS")

    @property
    def item_bytes(self):
        return self.pvlobj.get("ITEM_BYTES")

    @property
    def item_offset(self):
        return self.pvlobj.get("ITEM_OFFSET")

    @property
    def colspecs(self):
        if self.items is None:
            return (self.start, self.stop)
        else:
            i = 0
            bucket = []
            for _ in range(self.items):
                off = self.start + self.item_offset * i
                bucket.append((off, off + self.item_bytes))
                i += 1
            return bucket

    def decode(self, linedata):
        if self.items is None:
            start, stop = self.colspecs
            return linedata[start:stop]
        else:
            bucket = []
            for start, stop in self.colspecs:
                bucket.append(linedata[start:stop])
            return bucket

    def __repr__(self):
        return self.pvlobj.__repr__()


class IndexLabel:
    "Support working with label files of PDS Index tables."

    def __init__(
        self,
        # Path to the labelfile for a PDS Indexfile.
        # The actual table should reside in the same folder to be automatically parsed
        # when calling the `read_index_data` method.
        labelpath: str | Path,
        index_key: str | None = None,
    ):
        self.path = Path(labelpath)
        self.index_key = index_key
        "search for table name pointer and store key and fpath."
        tuple = [i for i in self.pvl_lbl if i[0].startswith("^")][0]
        self.tablename = tuple[0][1:]
        self.index_name = tuple[1]

    @property
    def index_path(self):
        p = self.path.parent / self.index_name
        logger.info(f"Looking for index table file at {p}")
        if not p.exists():
            # Fudging path name to lower case, opposing label value. (PDS data inconsistency)"
            p = self.path.parent / self.index_name.lower()
            logger.warning(f"Index table file not found, now trying {p}")
        if not p.exists():
            logger.error("Index table file not found.")
        return p

    @property
    def pvl_lbl(self):
        return pvl.load(str(self.path))

    @property
    def table(self):
        return self.pvl_lbl[self.tablename]

    @property
    def pvl_columns(self):
        return self.table.getlist("COLUMN")

    @property
    def columns_dic(self):
        return {col["NAME"]: col for col in self.pvl_columns}

    @property
    def colnames(self):
        """Read the columns in an PDS index label file.

        The label file for the PDS indices describes the content
        of the index files.
        """
        colnames = []
        for col in self.pvl_columns:
            colnames.extend(PVLColumn(col).name_as_list)
        return colnames

    @property
    def colspecs(self):
        colspecs = []
        columns = self.table.getlist("COLUMN")
        for column in columns:
            pvlcol = PVLColumn(column)
            if pvlcol.items is None:
                colspecs.append(pvlcol.colspecs)
            else:
                colspecs.extend(pvlcol.colspecs)
        return colspecs

    def read_index_data(self, convert_times=True):
        return index_to_df(self.index_path, self, convert_times=convert_times)


def _convert_times(df):
    missing_strings = [r"^UNK\s*$", r"^NULL\s*$", r"^N/A\s*$", r"^NA\s*$", r"^NONE\s*$"]
    for column in [col for col in df.columns if "TIME" in col]:
        if column in ["LOCAL_TIME", "DWELL_TIME"] or column.startswith("NTV"):
            continue
        logger.debug(f"Trying to convert {column} column to datetime type.")
        # Replace all known missing value strings with np.nan
        col_data = df[column]
        for miss in missing_strings:
            col_data = col_data.replace(miss, np.nan, regex=True)
        try:
            df[column] = pd.to_datetime(col_data)
        except ValueError:
            logger.warning(
                f"Could not convert {column} with one format, trying multiple formats. (slower)"
            )
            try:
                df[column] = pd.to_datetime(col_data, format="mixed")
            except ValueError:
                logger.warning(
                    f"Mixed-format conversion failed for {column}, trying DOY format converter."
                )
                df[column] = df[column].apply(tformats.fromdoyformat)
    logger.info("Converted time strings to datetime objects.")
    return df


def index_to_df(
    # Path to the index TAB file
    indexpath: str | Path,
    # Label object that has both the column names and the columns widths as attributes
    # 'colnames' and 'colspecs'
    label: IndexLabel,
    # Switch to control if to convert columns with "TIME" in name (unless COUNT is as well in name) to datetime
    convert_times: bool = True,
):
    """The main reader function for PDS Index files.

    In conjunction with an IndexLabel object that figures out the column widths,
    this reader should work for all PDS TAB files.
    """
    from .index_fixes import apply_file_fixer, apply_pre_time_df_fixer

    indexpath = Path(indexpath)
    # Apply any file-level fixers before parsing (if index_key known)
    if getattr(label, "index_key", None):
        apply_file_fixer(label.index_key, indexpath)
    # get n_lines fast for progress bar
    with open(indexpath, "rb") as f:  # courtesy of https://stackoverflow.com/a/1019572
        num_lines = sum(1 for _ in f)
    chunksize = 5000
    df = pd.concat(
        [
            chunk
            for chunk in tqdm(
                pd.read_csv(
                    indexpath,
                    header=None,
                    names=label.colnames,
                    chunksize=chunksize,
                    quotechar='"',
                    skipinitialspace=True,
                ),
                total=int(num_lines / chunksize),
                desc="Loading index in chunks",
            )
        ]
    )
    logger.info(f"Collected {len(df)} rows from {indexpath}")
    df = df.convert_dtypes()
    for col in df.select_dtypes(include=["string"]).columns:
        logger.debug(f"Stripping whitespace from string column {col}")
        df[col] = df[col].str.strip()
    # Apply any DataFrame-level pre-time fixers before converting times (if index_key known)
    if getattr(label, "index_key", None):
        df = apply_pre_time_df_fixer(label.index_key, df)
    if convert_times:
        df = _convert_times(df)
    return df


def decode_line(
    linedata: str,  # One line of a .tab data file
    labelpath: str | Path,  # Path to the appropriate label that describes the data.
):
    "Decode one line of tabbed data with the appropriate label file."
    label = IndexLabel(labelpath)
    for column in label.pvl_columns:
        pvlcol = PVLColumn(column)
        print(pvlcol.name, pvlcol.decode(linedata))


def find_mixed_type_cols(
    # Dataframe to be searched for mixed data-types
    df: pd.DataFrame,
    # Switch to control if NaN values in these problem columns should be replaced by the string 'UNKNOWN'
    fix: bool = True,
) -> list:  # List of column names that have data type changes within themselves.
    """For a given dataframe, find the columns that are of mixed type.

    Tool to help with the performance warning when trying to save a pandas DataFrame as a HDF.
    When a column changes datatype somewhere, pickling occurs, slowing down the reading process of the HDF file.
    """
    result = []
    for col in df.columns:
        weird = (df[[col]].map(type) != df[[col]].iloc[0].apply(type)).any(axis=1)
        if len(df[weird]) > 0:
            result.append(col)
            print(col)
            for i, t in df[weird][col].items():
                print(i, type(t))
    if fix:
        for col in result:
            df[col] = df[col].astype(str)
            # df[col] = df[col].fillna("UNKNOWN")
    return result
