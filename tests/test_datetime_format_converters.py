#!/usr/bin/env python
"""This module has tests for the datetime functions."""

# Copyright 2022, United States Government as represented by the
#     Administrator of the National Aeronautics and Space Administration.
#     All rights reserved.
# Copyright 2024, PlanetaryPy developers
#
# Reuse is permitted under the terms of the license.
# The AUTHORS file and the LICENSE file are at the
# top level of this library.
#
# The tests for fromisozformat() and isozformat() functions were taken from
# vipersci 0.8.0, which is Copyright by the U.S. Government under an
# Apache 2 license.

import datetime
import unittest

from planetarypy import datetime_format_converters as ppydt


class TestDOY(unittest.TestCase):
    def test_fromdoyformat(self):
        dt = datetime.datetime(2024, 5, 6, 11, 15, 0, tzinfo=datetime.timezone.utc)
        self.assertEqual(dt, ppydt.fromdoyformat("2024-127T11:15:00+00:00"))

        self.assertRaises(ValueError, ppydt.fromdoyformat, "2022-10-01T13:20:00")

    def test_doyformat(self):
        dt = datetime.datetime(2024, 5, 6, 14, 48, 0)
        self.assertEqual(ppydt.doyformat(dt), "2024-127T14:48:00")

    def test_variations(self):
        ordinal_date = "2010-110"
        ordinal_datetime = "2010-110T10:12:14"
        ordinal_datetime_with_ms = "2010-110T10:12:14.123000"

        calendar_date = "2010-04-20"
        calendar_datetime = "2010-04-20T10:12:14"
        calendar_datetime_with_ms = "2010-04-20T10:12:14.123000"

        # test_ordinal_time_to_datetime(ordinal_datetimes):
        self.assertEqual(
            datetime.datetime(2010, 4, 20, 0, 0),
            ppydt.fromdoyformat(ordinal_date),
        )
        self.assertEqual(
            datetime.datetime(2010, 4, 20, 10, 12, 14),
            ppydt.fromdoyformat(ordinal_datetime),
        )
        self.assertEqual(
            datetime.datetime(2010, 4, 20, 10, 12, 14, 123000),
            ppydt.fromdoyformat(ordinal_datetime_with_ms),
        )

        # test_ordinal_time_to_calendar(ordinal_datetimes):
        orddt = ppydt.fromdoyformat(ordinal_date)
        self.assertEqual(datetime.datetime.isoformat(orddt), "2010-04-20T00:00:00")
        self.assertEqual(orddt.date().isoformat(), "2010-04-20")
        self.assertEqual(orddt.isoformat(), "2010-04-20T00:00:00")

        # test_calendar_to_ordinal_time(calendar_datetimes, ordinal_datetimes):
        self.assertEqual(
            datetime.date.fromisoformat(calendar_date),
            ppydt.fromdoyformat(ordinal_date).date(),
        )
        self.assertEqual(
            datetime.datetime.fromisoformat(calendar_datetime),
            ppydt.fromdoyformat(ordinal_datetime),
        )
        self.assertEqual(
            datetime.datetime.fromisoformat(calendar_datetime_with_ms),
            ppydt.fromdoyformat(ordinal_datetime_with_ms),
        )

    def test_fromdoyformat_invalid_format(self):
        invalid_formats = [
            "2024/127T11:15:00",  # Wrong separator
            "2024-367T11:15:00",  # Invalid day of year
            "2024-000T11:15:00",  # Day of year can't be 0
            "abcd-123T11:15:00",  # Invalid year
        ]
        for invalid_format in invalid_formats:
            with self.assertRaises(ValueError):
                ppydt.fromdoyformat(invalid_format)

    def test_doyformat_edge_cases(self):
        # Test first day of year
        dt = datetime.datetime(2024, 1, 1, 0, 0, 0)
        self.assertEqual(ppydt.doyformat(dt), "2024-001T00:00:00")
        
        # Test last day of year
        dt = datetime.datetime(2024, 12, 31, 23, 59, 59)
        self.assertEqual(ppydt.doyformat(dt), "2024-366T23:59:59")  # 2024 is leap year


class TestIsoZ(unittest.TestCase):
    def test_fromisozformat(self):
        dt = datetime.datetime(2022, 10, 1, 13, 20, 0, tzinfo=datetime.timezone.utc)
        self.assertEqual(dt, ppydt.fromisozformat("2022-10-01T13:20:00Z"))

        self.assertRaises(ValueError, ppydt.fromisozformat, "2022-10-01T13:20:00")

    def test_isozformat(self):
        dt = datetime.datetime(2022, 10, 1, 13, 20, 0, tzinfo=datetime.timezone.utc)
        self.assertEqual(ppydt.isozformat(dt), "2022-10-01T13:20:00Z")

        no_tz = dt.replace(tzinfo=None)

        self.assertRaises(ValueError, ppydt.isozformat, no_tz)

    def test_fromisozformat_with_microseconds(self):
        dt = datetime.datetime(2022, 10, 1, 13, 20, 0, 123456, 
                              tzinfo=datetime.timezone.utc)
        self.assertEqual(dt, 
                        ppydt.fromisozformat("2022-10-01T13:20:00.123456Z"))

    def test_fromisozformat_invalid_formats(self):
        invalid_formats = [
            "2022-10-01T13:20:00+00:00",  # Wrong timezone format
            "2022-10-01 13:20:00Z",  # Missing T
            "2022-13-01T13:20:00Z",  # Invalid month
            "2022-10-32T13:20:00Z",  # Invalid day
        ]
        for invalid_format in invalid_formats:
            with self.assertRaises(ValueError):
                ppydt.fromisozformat(invalid_format)
