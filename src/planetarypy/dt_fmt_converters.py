"""This module contains datetime functions with extended
ISO 8601 handling to deal with day-of-year formats and trailing "Z"
characters instead of +00:00 to indicate UTC.
"""

# Copyright 2022, United States Government as represented by the
#     Administrator of the National Aeronautics and Space Administration.
#     All rights reserved.
# Copyright 2024, PlanetaryPy developers
#
# Reuse is permitted under the terms of the license.
# The AUTHORS file and the LICENSE file are at the
# top level of this library.
#
# The fromisozformat() and isozformat() functions were taken from vipersci 0.8.0,
# which is Copyright by the U.S. Government under an Apache 2 license.

import datetime
import re


def doyformat(date_time: datetime.datetime, sep="T", timespec="auto") -> str:
    """
    Return a string representing the date and time in ISO 8601 format with
    the date portion in ISO 8601 Ordinal date format, also known as
    day-of-year format:

    * YYYY-DDDTHH:MM:SS.ffffff, if microsecond is not 0
    * YYYY-DDDTHH:MM:SS, if microsecond is 0

    If utcoffset() does not return None, a string is appended, giving the UTC offset:

    * YYYY-DDDTHH:MM:SS.ffffff+HH:MM[:SS[.ffffff]], if microsecond is not 0
    * YYYY-DDDTHH:MM:SS+HH:MM[:SS[.ffffff]], if microsecond is 0

    This function is similar to the standard library's
    datetime.datetime.isoformat() function (see that documentation
    for details on *sep* and *timespec*).
    """
    # These functions do not have the term "ordinal" in them because the
    # Python Standard Library datetime module already has functions with
    # that name which refer to the "proleptic Gregorian ordinal" and not
    # ISO 8601 ordinal date format.

    d_str = date_time.strftime("%Y-%j")
    t_str = date_time.timetz().isoformat(timespec)
    return d_str + sep + t_str


def fromdoyformat(date_string) -> datetime.datetime:
    """
    Return a datetime corresponding to *date_string* in one of the formats
    emitted by doyformat().

    Specifically, this function supports strings in the format:

    * YYYY-DDD[*HH[:MM[:SS[.fff[fff]]]][+HH:MM[:SS[.ffffff]]]]

    where * can match any single character.

    This function is similar to the  standard library's
    datetime.datetime.fromisoformat() function.
    """
    match = re.match(r"(?P<year>\d{4})-?(?P<doy>\d{3})(.(?P<rest>.*))*", date_string)
    if match:
        parsed = match.groupdict()
    else:
        raise ValueError(f"{date_string} did not begin with YYYY-DDD or YYYYDDD.")

    date_obj = datetime.datetime.strptime(
        parsed["year"] + "-" + parsed["doy"], "%Y-%j"
    ).date()

    if parsed["rest"] is None:
        time_obj = datetime.time()
    else:
        time_obj = datetime.time.fromisoformat(parsed["rest"])

    return datetime.datetime.combine(date_obj, time_obj)


def fromisozformat(date_string):
    """
    Return a datetime corresponding to *date_string* in one of the formats emitted by
    isozformat().  This datetime will be a timezone-aware datetime.
    """
    try:
        dt = datetime.datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        dt = datetime.datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%fZ")

    return dt.replace(tzinfo=datetime.timezone.utc)


def isozformat(date_time, sep="T", timespec="auto"):
    """
    Return a string representing the UTC date and time in ISO 8601 format with
    the trailing letter 'Z' representing the UTC offset for the provided
    UTC *date_time* object.

    This function is similar to the standard library's
    datetime.datetime.isoformat() function (see that documentation
    for details on *sep* and *timespec*).

    The difference is that the provided *date_time* must be timezone
    aware and it must be in UTC (its utcoffset() must not be None and
    must equal zero). Otherwise, it will raise a ValueError.

    The most important difference is that rather than returning a string
    representation that ends in "+00:00" for a UTC datetime object, it
    will return a representation that ends in "Z".  Both formats are
    valid ISO 8601 date formats, but the standard library picks one
    string representation, and this function provides the other, which
    is the required format for PDS4 labels.
    """
    if date_time.utcoffset() == datetime.timedelta():
        return date_time.replace(tzinfo=None).isoformat(sep, timespec) + "Z"
    else:
        raise ValueError(
            "The datetime object is either naive (not timezone aware), "
            "or has a non-zero offset from UTC.  Maybe you just want "
            "the datetime object's isoformat() function?"
        )
