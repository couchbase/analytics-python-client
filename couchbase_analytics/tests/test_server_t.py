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

import json
from concurrent.futures import Future
from typing import TYPE_CHECKING

import pytest

from tests import YieldFixture

if TYPE_CHECKING:
    from tests.environments.base_environment import BlockingTestEnvironment


class TestServerTestSuite:

    TEST_MANIFEST = [
        'test_simple',
    ]

    def test_simple(self, test_env: BlockingTestEnvironment) -> None:
        test_env.set_url_path('/test_post')
        test_env.update_request_json({'test_timeout': 10})
        test_env.update_request_extensions({'timeout': {'pool': 5,
                                                        'test_pool_timeout': 5,
                                                        'test_connect_timeout': 5}})
        statement = 'SELECT "Hello, data!" AS greeting'
        res = test_env.cluster.execute_query(statement)
        print(f'Have result: {res=}')
        if isinstance(res, Future):
            print('Result is a Future')
            res = res.result()

class TestServerTests(TestServerTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self) -> None:
        def valid_test_method(meth: str) -> bool:
            attr = getattr(TestServerTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(TestServerTests) if valid_test_method(meth)]
        test_list = set(TestServerTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra tests: {test_list}.')

    @pytest.fixture(scope='class', name='test_env')
    def couchbase_test_environment(self, sync_test_env_with_server: BlockingTestEnvironment) -> YieldFixture[BlockingTestEnvironment]:
        test_env = sync_test_env_with_server.enable_test_server()
        yield test_env
        test_env.disable_test_server()