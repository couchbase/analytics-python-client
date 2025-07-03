#  Copyright 2016-2024. Couchbase, Inc.
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

from couchbase_analytics.common.backoff_calculator import DefaultBackoffCalculator

MIN = 100
MAX = 60 * 1000
EXPONENT_BASE = 2

class BackoffCalcTestSuite:
    TEST_MANIFEST = [
        'test_backoff_calcs',
    ]

    @pytest.mark.parametrize('retry_count, max_expected',
                             [(1, MIN * EXPONENT_BASE ** 0),
                              (2, MIN * EXPONENT_BASE ** 1),
                              (3, MIN * EXPONENT_BASE ** 2),
                              (4, MIN * EXPONENT_BASE ** 3),
                              (5, MIN * EXPONENT_BASE ** 4),
                              (6, MIN * EXPONENT_BASE ** 5),
                              (7, MIN * EXPONENT_BASE ** 6),
                              (8, MIN * EXPONENT_BASE ** 7),
                              (9, MIN * EXPONENT_BASE ** 8),
                              (10, MIN * EXPONENT_BASE ** 9),
                              (1000, MAX),
                              ])
    def test_backoff_calcs(self, retry_count: int, max_expected: float) -> None:
        calc = DefaultBackoffCalculator()
        for _ in range(10):
            delay = calc.calculate_backoff(retry_count)
            assert delay <= max_expected


class BackoffCalcTests(BackoffCalcTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self) -> None:
        def valid_test_method(meth: str) -> bool:
            attr = getattr(BackoffCalcTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(BackoffCalcTests) if valid_test_method(meth)]
        test_list = set(BackoffCalcTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra tests: {test_list}.')
