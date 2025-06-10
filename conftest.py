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

from typing import List

import pytest

pytest_plugins = [
    'tests.analytics_config',
    'tests.environments.base_environment',
    'tests.environments.simple_environment'
]

_UNIT_TESTS = [
    'couchbase_analytics/tests/json_parsing_t.py::JsonParsingTests',
]

_INTEGRATRION_TESTS = [
]

@pytest.fixture(scope='class')
def anyio_backend():
    return 'asyncio'

# https://docs.pytest.org/en/stable/reference/reference.html#std-hook-pytest_collection_modifyitems
def pytest_collection_modifyitems(session: pytest.Session,
                                  config: pytest.Config,
                                  items: List[pytest.Item]) -> None:  # noqa: C901
    for item in items:
        item_details = item.nodeid.split('::')

        item_api = item_details[0].split('/')
        if item_api[0] == 'couchbase_analytics':
            item.add_marker(pytest.mark.pycbac_couchbase)
        elif item_api[0] == 'acouchbase_analytics':
            item.add_marker(pytest.mark.pycbac_acouchbase)

        test_class_path = '::'.join(item_details[:-1])
        if test_class_path in _UNIT_TESTS:
            item.add_marker(pytest.mark.pycbac_unit)
        elif test_class_path in _INTEGRATRION_TESTS:
            item.add_marker(pytest.mark.pycbac_integration)