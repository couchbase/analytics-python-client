#  Copyright 2016-2025. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.


from __future__ import annotations

import pytest

from couchbase_analytics.common._core.duration_str_utils import parse_duration_str


class DurationParsingTestSuite:
    TEST_MANIFEST = [
        'test_invalid_durations',
        'test_valid_durations',
    ]

    @pytest.mark.parametrize(
        'duration',
        [
            '',
            '10',
            '10Gs',
            'abc',
            '-',
            '+',
            '1h-',
            '1h 30m',
            '1h_30m',
            'h1',
            '-.5s',
            '1.2.3s',
        ],
    )
    def test_invalid_durations(self, duration: str) -> None:
        with pytest.raises(ValueError):
            parse_duration_str(duration)

    @pytest.mark.parametrize(
        'duration, expected_millis',
        [
            ('0', 0),
            ('0s', 0),
            ('1h', 3.6e6),
            ('+1h', 3.6e6),
            ('1h10m', 4.2e6),
            ('1.h10m', 4.2e6),
            ('1.234h', 1.234 * 3.6e6),
            ('1h30m0s', 5.4e6),
            ('0.1h10m', 9.6e5),
            # NOTE: apparently this is invalid in Go, but was okay w/ C++ implementation
            ('.1h10m', 9.6e5),
            ('0001h00010m', 4.2e6),
            ('100ns', 1e-4),
            ('100us', 0.1),
            ('100μs', 0.1),
            ('100µs', 0.1),
            ('1000000ns', 1),
            ('1000us', 1),
            ('1000μs', 1),
            ('1000µs', 1),
            ('3h15m10s500ms', 11710.5 * 1e3),
            ('1h1m1s1ms1us1ns', 3.6e6 + 60e3 + 1e3 + 1 + 0.001 + 0.000001),
            ('2m3s4ms', 123004),
            ('4ms3s2m', 123004),
            ('4ms3s2m5s', 128004),
            ('2m3.125s', 123125),
        ],
    )
    def test_valid_durations(self, duration: str, expected_millis: float) -> None:
        actual = parse_duration_str(duration, in_millis=True)
        # if we don't allow for a tolerance, we will have issues with float precision
        # examples:
        #   100us yields 0.09999999999999999 != 0.1
        #   4ms3s2m5s yields 128004.00000000001 != 128004
        assert abs(actual - expected_millis) < 1e-9


class DurationParsingTests(DurationParsingTestSuite):
    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self) -> None:
        def valid_test_method(meth: str) -> bool:
            attr = getattr(DurationParsingTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')

        method_list = [meth for meth in dir(DurationParsingTests) if valid_test_method(meth)]
        test_list = set(DurationParsingTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra tests: {test_list}.')
