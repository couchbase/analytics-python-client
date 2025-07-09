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
from asyncio import CancelledError, Task
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from acouchbase_analytics.deserializer import PassthroughDeserializer
from acouchbase_analytics.errors import QueryError, TimeoutError
from acouchbase_analytics.options import QueryOptions
from acouchbase_analytics.result import AsyncQueryResult
from couchbase_analytics.common.streaming import StreamingState
from tests import AsyncYieldFixture

if TYPE_CHECKING:
    from tests.environments.base_environment import AsyncTestEnvironment


class QueryTestSuite:
    TEST_MANIFEST = [
        'test_query_cancel_prior_iterating',
        'test_query_cancel_async_while_iterating',
        'test_query_cancel_while_iterating',
        'test_query_metadata',
        'test_query_metadata_not_available',
        'test_query_named_parameters',
        'test_query_named_parameters_no_options',
        'test_query_named_parameters_override',
        'test_query_passthrough_deserializer',
        'test_query_positional_params',
        'test_query_positional_params_no_option',
        'test_query_positional_params_override',
        'test_query_raises_exception_prior_to_iterating',
        'test_query_raw_options',
        'test_query_timeout',
        'test_query_timeout_while_streaming',
        'test_simple_query',
    ]

    @pytest.fixture(scope='class')
    def query_statement_limit2(self, test_env: AsyncTestEnvironment) -> str:
        if test_env.use_scope:
            return f'SELECT * FROM {test_env.collection_name} LIMIT 2;'
        else:
            return f'SELECT * FROM {test_env.fqdn} LIMIT 2;'

    @pytest.fixture(scope='class')
    def query_statement_pos_params_limit2(self, test_env: AsyncTestEnvironment) -> str:
        if test_env.use_scope:
            return f'SELECT * FROM {test_env.collection_name} WHERE country = $1 LIMIT 2;'
        else:
            return f'SELECT * FROM {test_env.fqdn} WHERE country = $1 LIMIT 2;'

    @pytest.fixture(scope='class')
    def query_statement_named_params_limit2(self, test_env: AsyncTestEnvironment) -> str:
        if test_env.use_scope:
            return f'SELECT * FROM {test_env.collection_name} WHERE country = $country LIMIT 2;'
        else:
            return f'SELECT * FROM {test_env.fqdn} WHERE country = $country LIMIT 2;'

    @pytest.fixture(scope='class')
    def query_statement_limit5(self, test_env: AsyncTestEnvironment) -> str:
        if test_env.use_scope:
            return f'SELECT * FROM {test_env.collection_name} LIMIT 5;'
        else:
            return f'SELECT * FROM {test_env.fqdn} LIMIT 5;'

    async def test_query_cancel_prior_iterating(self, test_env: AsyncTestEnvironment) -> None:
        # simulate query that takes time to return
        statement = 'SELECT sleep("some value", 10000) AS some_field;'
        qtask = test_env.cluster_or_scope.execute_query(statement)
        assert isinstance(qtask, Task)
        await test_env.sleep(1)
        qtask.cancel()
        with pytest.raises(CancelledError):
            await qtask

    async def test_query_cancel_async_while_iterating(
        self, test_env: AsyncTestEnvironment, query_statement_limit5: str
    ) -> None:
        qtask = test_env.cluster_or_scope.execute_query(query_statement_limit5)
        assert isinstance(qtask, Task)
        res = await qtask
        assert isinstance(res, AsyncQueryResult)
        expected_state = StreamingState.StreamingResults
        assert res._http_response._request_context.request_state == expected_state
        rows = []
        count = 0
        async for row in res.rows():
            if count == 2:
                await res.cancel_async()
            assert row is not None
            rows.append(row)
            count += 1

        assert len(rows) == count
        expected_state = StreamingState.Cancelled
        assert res._http_response._request_context.request_state == expected_state
        with pytest.raises(RuntimeError):
            res.metadata()
        test_env.assert_streaming_response_state(res)

    async def test_query_cancel_while_iterating(
        self, test_env: AsyncTestEnvironment, query_statement_limit5: str
    ) -> None:
        qtask = test_env.cluster_or_scope.execute_query(query_statement_limit5)
        assert isinstance(qtask, Task)
        res = await qtask
        assert isinstance(res, AsyncQueryResult)
        expected_state = StreamingState.StreamingResults
        assert res._http_response._request_context.request_state == expected_state
        rows = []
        count = 0
        async for row in res.rows():
            if count == 2:
                res.cancel()
            assert row is not None
            rows.append(row)
            count += 1

        assert len(rows) == count
        expected_state = StreamingState.Cancelled
        assert res._http_response._request_context.request_state == expected_state
        with pytest.raises(RuntimeError):
            res.metadata()
        # if we don't cancel via the async path, we want to ensure the stream/response is shutdown appropriately
        await res.shutdown()
        test_env.assert_streaming_response_state(res)

    async def test_query_metadata(self, test_env: AsyncTestEnvironment, query_statement_limit5: str) -> None:
        result = await test_env.cluster_or_scope.execute_query(query_statement_limit5)
        expected_count = 5
        await test_env.assert_rows(result, expected_count)

        metadata = result.metadata()

        assert len(metadata.warnings()) == 0
        assert len(metadata.request_id()) > 0

        metrics = metadata.metrics()

        assert metrics.result_size() > 0
        assert metrics.result_count() == expected_count
        assert metrics.processed_objects() > 0
        assert metrics.elapsed_time() > timedelta(0)
        assert metrics.execution_time() > timedelta(0)
        test_env.assert_streaming_response_state(result)

    async def test_query_metadata_not_available(
        self, test_env: AsyncTestEnvironment, query_statement_limit5: str
    ) -> None:
        result = await test_env.cluster_or_scope.execute_query(query_statement_limit5)

        with pytest.raises(RuntimeError):
            result.metadata()

        # Read one row -- NOTE: anext()/aiter() add in Python 3.10
        aiter = result.rows()
        row = await aiter.__anext__()
        assert row is not None
        assert isinstance(row, dict)

        with pytest.raises(RuntimeError):
            result.metadata()

        # Iterate the rest of the rows
        rows = [r async for r in result.rows()]
        assert len(rows) == 4

        metadata = result.metadata()
        assert len(metadata.warnings()) == 0
        assert len(metadata.request_id()) > 0
        test_env.assert_streaming_response_state(result)

    async def test_query_named_parameters(
        self,
        test_env: AsyncTestEnvironment,
        query_statement_named_params_limit2: str,
    ) -> None:
        q_opts = QueryOptions(named_parameters={'country': 'United States'})
        result = await test_env.cluster_or_scope.execute_query(query_statement_named_params_limit2, q_opts)
        await test_env.assert_rows(result, 2)
        test_env.assert_streaming_response_state(result)

    async def test_query_named_parameters_no_options(
        self, test_env: AsyncTestEnvironment, query_statement_named_params_limit2: str
    ) -> None:
        result = await test_env.cluster_or_scope.execute_query(
            query_statement_named_params_limit2, country='United States'
        )
        await test_env.assert_rows(result, 2)
        test_env.assert_streaming_response_state(result)

    async def test_query_named_parameters_override(
        self, test_env: AsyncTestEnvironment, query_statement_named_params_limit2: str
    ) -> None:
        q_opts = QueryOptions(named_parameters={'country': 'abcdefg'})
        result = await test_env.cluster_or_scope.execute_query(
            query_statement_named_params_limit2, q_opts, country='United States'
        )
        await test_env.assert_rows(result, 2)
        test_env.assert_streaming_response_state(result)

    async def test_query_passthrough_deserializer(self, test_env: AsyncTestEnvironment) -> None:
        statement = 'FROM range(0, 10) AS num SELECT *'
        result = await test_env.cluster_or_scope.execute_query(
            statement, QueryOptions(deserializer=PassthroughDeserializer())
        )
        idx = 0
        async for row in result.rows():
            assert isinstance(row, bytes)
            assert json.loads(row) == {'num': idx}
            idx += 1
        test_env.assert_streaming_response_state(result)

    async def test_query_positional_params(
        self, test_env: AsyncTestEnvironment, query_statement_pos_params_limit2: str
    ) -> None:
        q_opts = QueryOptions(positional_parameters=['United States'])
        result = await test_env.cluster_or_scope.execute_query(query_statement_pos_params_limit2, q_opts)
        await test_env.assert_rows(result, 2)
        test_env.assert_streaming_response_state(result)

    async def test_query_positional_params_no_option(
        self, test_env: AsyncTestEnvironment, query_statement_pos_params_limit2: str
    ) -> None:
        result = await test_env.cluster_or_scope.execute_query(query_statement_pos_params_limit2, 'United States')
        await test_env.assert_rows(result, 2)
        test_env.assert_streaming_response_state(result)

    async def test_query_positional_params_override(
        self, test_env: AsyncTestEnvironment, query_statement_pos_params_limit2: str
    ) -> None:
        q_opts = QueryOptions(positional_parameters=['abcdefg'])
        result = await test_env.cluster_or_scope.execute_query(
            query_statement_pos_params_limit2, q_opts, 'United States'
        )
        await test_env.assert_rows(result, 2)
        test_env.assert_streaming_response_state(result)

    async def test_query_raises_exception_prior_to_iterating(self, test_env: AsyncTestEnvironment) -> None:
        statement = "I'm not N1QL!"
        with pytest.raises(QueryError):
            await test_env.cluster_or_scope.execute_query(statement)

    async def test_query_raw_options(
        self, test_env: AsyncTestEnvironment, query_statement_pos_params_limit2: str
    ) -> None:
        # via raw, we should be able to pass any option
        # if using named params, need to match full name param in query
        # which is different for when we pass in name_parameters via their specific
        # query option (i.e. include the $ when using raw)
        if test_env.use_scope:
            statement = f'SELECT * FROM {test_env.collection_name} WHERE country = $country LIMIT $1;'
        else:
            statement = f'SELECT * FROM {test_env.fqdn} WHERE country = $country LIMIT $1;'

        q_opts = QueryOptions(raw={'$country': 'United States', 'args': [2]})
        result = await test_env.cluster_or_scope.execute_query(statement, q_opts)
        await test_env.assert_rows(result, 2)

        result = await test_env.cluster_or_scope.execute_query(
            query_statement_pos_params_limit2, QueryOptions(raw={'args': ['United States']})
        )
        await test_env.assert_rows(result, 2)
        test_env.assert_streaming_response_state(result)

    async def test_query_timeout(self, test_env: AsyncTestEnvironment) -> None:
        statement = 'SELECT sleep("some value", 10000) AS some_field;'

        with pytest.raises(TimeoutError):
            await test_env.cluster_or_scope.execute_query(statement, QueryOptions(timeout=timedelta(seconds=2)))

    async def test_query_timeout_while_streaming(self, test_env: AsyncTestEnvironment) -> None:
        statement = 'SELECT {"x1": 1, "x2": 2, "x3": 3} FROM range(1, 100000) r;'
        res = test_env.cluster_or_scope.execute_query(statement, QueryOptions(timeout=timedelta(seconds=2)))
        assert isinstance(res, Task)
        result = await res

        with pytest.raises(TimeoutError):
            async for _ in result.rows():
                pass
        test_env.assert_streaming_response_state(result)

    async def test_simple_query(self, test_env: AsyncTestEnvironment, query_statement_limit2: str) -> None:
        result = await test_env.cluster_or_scope.execute_query(query_statement_limit2)
        await test_env.assert_rows(result, 2)
        test_env.assert_streaming_response_state(result)


class ClusterQueryTests(QueryTestSuite):
    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self) -> None:
        def valid_test_method(meth: str) -> bool:
            attr = getattr(ClusterQueryTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')

        method_list = [meth for meth in dir(ClusterQueryTests) if valid_test_method(meth)]
        test_list = set(QueryTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra tests: {test_list}.')

    @pytest.fixture(scope='class', name='test_env')
    async def couchbase_test_environment(
        self, async_test_env: AsyncTestEnvironment
    ) -> AsyncYieldFixture[AsyncTestEnvironment]:
        await async_test_env.setup()
        yield async_test_env
        await async_test_env.teardown()


class ScopeQueryTests(QueryTestSuite):
    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self) -> None:
        def valid_test_method(meth: str) -> bool:
            attr = getattr(ScopeQueryTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')

        method_list = [meth for meth in dir(ScopeQueryTests) if valid_test_method(meth)]
        test_list = set(QueryTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra tests: {test_list}.')

    @pytest.fixture(scope='class', name='test_env')
    async def couchbase_test_environment(
        self, async_test_env: AsyncTestEnvironment
    ) -> AsyncYieldFixture[AsyncTestEnvironment]:
        await async_test_env.setup()
        test_env = async_test_env.enable_scope()
        yield test_env
        test_env.disable_scope()
        await test_env.teardown()
