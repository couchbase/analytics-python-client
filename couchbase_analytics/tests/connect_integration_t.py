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

from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from couchbase_analytics.cluster import Cluster
from couchbase_analytics.credential import Credential
from couchbase_analytics.errors import AnalyticsError, TimeoutError
from couchbase_analytics.options import QueryOptions
from tests import YieldFixture

if TYPE_CHECKING:
    from tests.environments.base_environment import BlockingTestEnvironment


class ConnectTestSuite:
    TEST_MANIFEST = [
        'test_connect_timeout_max_retry_limit',
        'test_connect_timeout_query_timeout',
    ]

    def test_connect_timeout_max_retry_limit(self, test_env: BlockingTestEnvironment) -> None:
        statement = 'SELECT sleep("some value", 10000) AS some_field;'

        username, pw = test_env.config.get_username_and_pw()
        cred = Credential.from_username_and_password(username, pw)
        # ignoring the port enables the failure
        connstr = test_env.config.get_connection_string(ignore_port=True)
        cluster = Cluster.create_instance(connstr, cred)

        allowed_retries = 5
        q_opts = QueryOptions(max_retries=allowed_retries, timeout=timedelta(seconds=10))
        with pytest.raises(AnalyticsError) as ex:
            cluster.execute_query(statement, q_opts)

        assert ex.value._message is not None
        assert 'Retry limit exceeded' in ex.value._message
        test_env.assert_error_context_num_attempts(allowed_retries + 1, ex.value._context)

    def test_connect_timeout_query_timeout(self, test_env: BlockingTestEnvironment) -> None:
        statement = 'SELECT sleep("some value", 10000) AS some_field;'

        username, pw = test_env.config.get_username_and_pw()
        cred = Credential.from_username_and_password(username, pw)
        # ignoring the port enables the failure
        connstr = test_env.config.get_connection_string(ignore_port=True)
        cluster = Cluster.create_instance(connstr, cred)

        # increase the max retries to ensure that the timeout is hit
        q_opts = QueryOptions(max_retries=20, timeout=timedelta(seconds=3))
        with pytest.raises(TimeoutError) as ex:
            cluster.execute_query(statement, q_opts)

        assert ex.value._message is not None
        assert 'Request timed out during retry delay' in ex.value._message
        test_env.assert_error_context_num_attempts(2, ex.value._context, exact=False)


class ConnectTests(ConnectTestSuite):
    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self) -> None:
        def valid_test_method(meth: str) -> bool:
            attr = getattr(ConnectTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')

        method_list = [meth for meth in dir(ConnectTests) if valid_test_method(meth)]
        test_list = set(ConnectTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra tests: {test_list}.')

    @pytest.fixture(scope='class', name='test_env')
    def couchbase_test_environment(
        self, sync_test_env: BlockingTestEnvironment
    ) -> YieldFixture[BlockingTestEnvironment]:
        sync_test_env.setup()
        yield sync_test_env
        sync_test_env.teardown()
