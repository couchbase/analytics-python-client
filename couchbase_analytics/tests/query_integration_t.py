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
from concurrent.futures import CancelledError, Future
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Dict, Optional

import pytest

from couchbase_analytics.common.request import RequestState
from couchbase_analytics.deserializer import PassthroughDeserializer
from couchbase_analytics.errors import QueryError, TimeoutError
from couchbase_analytics.options import QueryOptions
from couchbase_analytics.query import QueryScanConsistency
from couchbase_analytics.result import BlockingQueryResult
from tests import SyncQueryType, YieldFixture

if TYPE_CHECKING:
    from tests.environments.base_environment import BlockingTestEnvironment


class QueryTestSuite:
    TEST_MANIFEST = [
        'test_cancel_prior_iterating',
        'test_cancel_prior_iterating_positional_params',
        'test_cancel_prior_iterating_with_kwargs',
        'test_cancel_prior_iterating_with_options',
        'test_cancel_prior_iterating_with_opts_and_kwargs',
        'test_cancel_while_iterating',
        'test_query_cannot_set_both_cancel_and_lazy_execution',
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
        'test_query_with_lazy_execution',
        'test_query_with_lazy_execution_raises_exception',
    ]

    @pytest.fixture(scope='class')
    def query_statement_limit2(self, test_env: BlockingTestEnvironment) -> str:
        if test_env.use_scope:
            return f'SELECT * FROM {test_env.collection_name} LIMIT 2;'
        else:
            return f'SELECT * FROM {test_env.fqdn} LIMIT 2;'

    @pytest.fixture(scope='class')
    def query_statement_pos_params_limit2(self, test_env: BlockingTestEnvironment) -> str:
        if test_env.use_scope:
            return f'SELECT * FROM {test_env.collection_name} WHERE country = $1 LIMIT 2;'
        else:
            return f'SELECT * FROM {test_env.fqdn} WHERE country = $1 LIMIT 2;'

    @pytest.fixture(scope='class')
    def query_statement_named_params_limit2(self, test_env: BlockingTestEnvironment) -> str:
        if test_env.use_scope:
            return f'SELECT * FROM {test_env.collection_name} WHERE country = $country LIMIT 2;'
        else:
            return f'SELECT * FROM {test_env.fqdn} WHERE country = $country LIMIT 2;'

    @pytest.fixture(scope='class')
    def query_statement_limit5(self, test_env: BlockingTestEnvironment) -> str:
        if test_env.use_scope:
            return f'SELECT * FROM {test_env.collection_name} LIMIT 5;'
        else:
            return f'SELECT * FROM {test_env.fqdn} LIMIT 5;'

    @pytest.mark.parametrize('cancel_via_future', [False, True])
    def test_cancel_prior_iterating(self, test_env: BlockingTestEnvironment, cancel_via_future: bool) -> None:
        statement = 'FROM range(0, 100000) AS r SELECT *'
        ft = test_env.cluster_or_scope.execute_query(statement, enable_cancel=True)
        assert isinstance(ft, Future)
        res: Optional[BlockingQueryResult] = None
        rows = []
        if cancel_via_future:
            ft.cancel()
            with pytest.raises(CancelledError):
                res = ft.result()
                for row in res.rows():
                    rows.append(row)

            assert res is None
            assert len(rows) == 0
        else:
            res = ft.result()
            res.cancel()

            assert isinstance(res, BlockingQueryResult)
            assert res._http_response._request_context.request_state == RequestState.Cancelled

            for row in res.rows():
                rows.append(row)

            with pytest.raises(RuntimeError):
                res.metadata()

            test_env.assert_streaming_response_state(res)

    @pytest.mark.parametrize('cancel_via_future', [False, True])
    def test_cancel_prior_iterating_positional_params(
        self, test_env: BlockingTestEnvironment, query_statement_pos_params_limit2: str, cancel_via_future: bool
    ) -> None:
        ft = test_env.cluster_or_scope.execute_query(
            query_statement_pos_params_limit2, 'United States', enable_cancel=True
        )
        assert isinstance(ft, Future)
        res: Optional[BlockingQueryResult] = None
        rows = []
        if cancel_via_future:
            ft.cancel()
            with pytest.raises(CancelledError):
                res = ft.result()
                for row in res.rows():
                    rows.append(row)

            assert res is None
            assert len(rows) == 0
        else:
            res = ft.result()
            res.cancel()

            assert isinstance(res, BlockingQueryResult)
            assert res._http_response._request_context.request_state == RequestState.Cancelled

            for row in res.rows():
                rows.append(row)

            with pytest.raises(RuntimeError):
                res.metadata()

            test_env.assert_streaming_response_state(res)

    @pytest.mark.parametrize('cancel_via_future', [False, True])
    def test_cancel_prior_iterating_with_kwargs(
        self, test_env: BlockingTestEnvironment, cancel_via_future: bool
    ) -> None:
        statement = 'FROM range(0, 100000) AS r SELECT *'
        ft = test_env.cluster_or_scope.execute_query(statement, timeout=timedelta(seconds=4), enable_cancel=True)
        assert isinstance(ft, Future)
        res: Optional[BlockingQueryResult] = None
        rows = []
        if cancel_via_future:
            ft.cancel()
            with pytest.raises(CancelledError):
                res = ft.result()
                for row in res.rows():
                    rows.append(row)

            assert res is None
            assert len(rows) == 0
        else:
            res = ft.result()
            res.cancel()

            assert isinstance(res, BlockingQueryResult)
            assert res._http_response._request_context.request_state == RequestState.Cancelled

            for row in res.rows():
                rows.append(row)

            with pytest.raises(RuntimeError):
                res.metadata()

            test_env.assert_streaming_response_state(res)

    @pytest.mark.parametrize('cancel_via_future', [False, True])
    def test_cancel_prior_iterating_with_options(
        self, test_env: BlockingTestEnvironment, cancel_via_future: bool
    ) -> None:
        statement = 'FROM range(0, 100000) AS r SELECT *'
        ft = test_env.cluster_or_scope.execute_query(
            statement, QueryOptions(timeout=timedelta(seconds=4)), enable_cancel=True
        )
        assert isinstance(ft, Future)
        res: Optional[BlockingQueryResult] = None
        rows = []
        if cancel_via_future:
            ft.cancel()
            with pytest.raises(CancelledError):
                res = ft.result()
                for row in res.rows():
                    rows.append(row)

            assert res is None
            assert len(rows) == 0
        else:
            res = ft.result()
            res.cancel()

            assert isinstance(res, BlockingQueryResult)
            assert res._http_response._request_context.request_state == RequestState.Cancelled

            for row in res.rows():
                rows.append(row)

            with pytest.raises(RuntimeError):
                res.metadata()
            test_env.assert_streaming_response_state(res)

    @pytest.mark.parametrize('cancel_via_future', [False, True])
    def test_cancel_prior_iterating_with_opts_and_kwargs(
        self, test_env: BlockingTestEnvironment, cancel_via_future: bool
    ) -> None:
        statement = 'FROM range(0, 100000) AS r SELECT *'
        ft = test_env.cluster_or_scope.execute_query(
            statement,
            QueryOptions(scan_consistency=QueryScanConsistency.NOT_BOUNDED),
            timeout=timedelta(seconds=4),
            enable_cancel=True,
        )
        assert isinstance(ft, Future)
        res: Optional[BlockingQueryResult] = None
        rows = []
        if cancel_via_future:
            ft.cancel()
            with pytest.raises(CancelledError):
                res = ft.result()
                for row in res.rows():
                    rows.append(row)

            assert res is None
            assert len(rows) == 0
        else:
            res = ft.result()
            res.cancel()

            assert isinstance(res, BlockingQueryResult)
            assert res._http_response._request_context.request_state == RequestState.Cancelled

            for row in res.rows():
                rows.append(row)

            with pytest.raises(RuntimeError):
                res.metadata()
            test_env.assert_streaming_response_state(res)

    @pytest.mark.parametrize('query_type', [SyncQueryType.NORMAL, SyncQueryType.LAZY, SyncQueryType.CANCELLABLE])
    def test_cancel_while_iterating(
        self, test_env: BlockingTestEnvironment, query_statement_limit5: str, query_type: SyncQueryType
    ) -> None:
        if query_type == SyncQueryType.NORMAL:
            result = test_env.cluster_or_scope.execute_query(query_statement_limit5)
        elif query_type == SyncQueryType.LAZY:
            result = test_env.cluster_or_scope.execute_query(query_statement_limit5, QueryOptions(lazy_execute=True))
        else:
            res = test_env.cluster_or_scope.execute_query(query_statement_limit5, enable_cancel=True)
            assert isinstance(res, Future)
            result = res.result()

        assert isinstance(result, BlockingQueryResult)
        if query_type != SyncQueryType.LAZY:
            expected_state = RequestState.StreamingResults
        else:
            expected_state = RequestState.NotStarted
        assert result._http_response._request_context.request_state == expected_state
        rows = []
        count = 0
        for row in result.rows():
            if count == 2:
                result.cancel()
            assert row is not None
            rows.append(row)
            count += 1

        assert len(rows) == count
        expected_state = RequestState.Cancelled
        assert result._http_response._request_context.request_state == expected_state
        with pytest.raises(RuntimeError):
            result.metadata()
        test_env.assert_streaming_response_state(result)

    def test_query_cannot_set_both_cancel_and_lazy_execution(self, test_env: BlockingTestEnvironment) -> None:
        statement = 'SELECT 1=1'
        with pytest.raises(RuntimeError):
            test_env.cluster_or_scope.execute_query(statement, QueryOptions(lazy_execute=True), enable_cancel=True)

    @pytest.mark.parametrize('query_type', [SyncQueryType.NORMAL, SyncQueryType.LAZY, SyncQueryType.CANCELLABLE])
    def test_query_metadata(
        self, test_env: BlockingTestEnvironment, query_statement_limit5: str, query_type: SyncQueryType
    ) -> None:
        if query_type == SyncQueryType.NORMAL:
            result = test_env.cluster_or_scope.execute_query(query_statement_limit5)
        elif query_type == SyncQueryType.LAZY:
            result = test_env.cluster_or_scope.execute_query(query_statement_limit5, QueryOptions(lazy_execute=True))
        else:
            res = test_env.cluster_or_scope.execute_query(query_statement_limit5, enable_cancel=True)
            assert isinstance(res, Future)
            result = res.result()

        expected_count = 5
        test_env.assert_rows(result, expected_count)

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

    @pytest.mark.parametrize('query_type', [SyncQueryType.NORMAL, SyncQueryType.LAZY, SyncQueryType.CANCELLABLE])
    def test_query_metadata_not_available(
        self, test_env: BlockingTestEnvironment, query_statement_limit5: str, query_type: SyncQueryType
    ) -> None:
        if query_type == SyncQueryType.NORMAL:
            result = test_env.cluster_or_scope.execute_query(query_statement_limit5)
        elif query_type == SyncQueryType.LAZY:
            result = test_env.cluster_or_scope.execute_query(query_statement_limit5, QueryOptions(lazy_execute=True))
        else:
            res = test_env.cluster_or_scope.execute_query(query_statement_limit5, enable_cancel=True)
            assert isinstance(res, Future)
            result = res.result()

        with pytest.raises(RuntimeError):
            result.metadata()

        # Read one row
        next(iter(result.rows()))

        with pytest.raises(RuntimeError):
            result.metadata()

        # This would attempt to send the request when using lazy execution
        if query_type == SyncQueryType.LAZY:
            with pytest.raises(RuntimeError):
                list(result.rows())
            return
        # Iterate the rest of the rows
        rows = list(result.rows())
        assert len(rows) == 4

        metadata = result.metadata()
        assert len(metadata.warnings()) == 0
        assert len(metadata.request_id()) > 0
        test_env.assert_streaming_response_state(result)

    @pytest.mark.parametrize('query_type', [SyncQueryType.NORMAL, SyncQueryType.LAZY, SyncQueryType.CANCELLABLE])
    def test_query_named_parameters(
        self, test_env: BlockingTestEnvironment, query_statement_named_params_limit2: str, query_type: SyncQueryType
    ) -> None:
        named_parameters: Dict[str, Any] = {'country': 'United States'}
        if query_type == SyncQueryType.NORMAL:
            result = test_env.cluster_or_scope.execute_query(
                query_statement_named_params_limit2, QueryOptions(named_parameters=named_parameters)
            )
        elif query_type == SyncQueryType.LAZY:
            result = test_env.cluster_or_scope.execute_query(
                query_statement_named_params_limit2, QueryOptions(named_parameters=named_parameters, lazy_execute=True)
            )
        else:
            res = test_env.cluster_or_scope.execute_query(
                query_statement_named_params_limit2, QueryOptions(named_parameters=named_parameters), enable_cancel=True
            )
            assert isinstance(res, Future)
            result = res.result()
        test_env.assert_rows(result, 2)
        test_env.assert_streaming_response_state(result)

    @pytest.mark.parametrize('query_type', [SyncQueryType.NORMAL, SyncQueryType.LAZY, SyncQueryType.CANCELLABLE])
    def test_query_named_parameters_no_options(
        self, test_env: BlockingTestEnvironment, query_statement_named_params_limit2: str, query_type: SyncQueryType
    ) -> None:
        if query_type == SyncQueryType.NORMAL:
            result = test_env.cluster_or_scope.execute_query(
                query_statement_named_params_limit2, country='United States'
            )
        elif query_type == SyncQueryType.LAZY:
            # this format does not really make sense, if users are using static type checking it will prevent them
            # but, technically viable so we test it
            result = test_env.cluster_or_scope.execute_query(
                query_statement_named_params_limit2,  # type: ignore[call-overload]
                lazy_execute=True,
                country='United States',
            )
        else:
            res = test_env.cluster_or_scope.execute_query(
                query_statement_named_params_limit2, country='United States', enable_cancel=True
            )
            assert isinstance(res, Future)
            result = res.result()
        test_env.assert_rows(result, 2)
        test_env.assert_streaming_response_state(result)

    @pytest.mark.parametrize('query_type', [SyncQueryType.NORMAL, SyncQueryType.LAZY, SyncQueryType.CANCELLABLE])
    def test_query_named_parameters_override(
        self, test_env: BlockingTestEnvironment, query_statement_named_params_limit2: str, query_type: SyncQueryType
    ) -> None:
        if query_type == SyncQueryType.NORMAL:
            result = test_env.cluster_or_scope.execute_query(
                query_statement_named_params_limit2,
                QueryOptions(named_parameters={'country': 'abcdefg'}),
                country='United States',
            )
        elif query_type == SyncQueryType.LAZY:
            result = test_env.cluster_or_scope.execute_query(
                query_statement_named_params_limit2,
                QueryOptions(named_parameters={'country': 'abcdefg'}, lazy_execute=True),
                country='United States',
            )
        else:
            res = test_env.cluster_or_scope.execute_query(
                query_statement_named_params_limit2,
                QueryOptions(named_parameters={'country': 'abcdefg'}),
                country='United States',
                enable_cancel=True,
            )
            assert isinstance(res, Future)
            result = res.result()
        test_env.assert_rows(result, 2)
        test_env.assert_streaming_response_state(result)

    @pytest.mark.parametrize('query_type', [SyncQueryType.NORMAL, SyncQueryType.LAZY, SyncQueryType.CANCELLABLE])
    def test_query_passthrough_deserializer(self, test_env: BlockingTestEnvironment, query_type: SyncQueryType) -> None:
        statement = 'FROM range(0, 10) AS num SELECT *'

        if query_type == SyncQueryType.NORMAL:
            result = test_env.cluster_or_scope.execute_query(
                statement, QueryOptions(deserializer=PassthroughDeserializer())
            )
        elif query_type == SyncQueryType.LAZY:
            result = test_env.cluster_or_scope.execute_query(
                statement, QueryOptions(deserializer=PassthroughDeserializer(), lazy_execute=True)
            )
        else:
            res = test_env.cluster_or_scope.execute_query(
                statement, QueryOptions(deserializer=PassthroughDeserializer()), enable_cancel=True
            )
            assert isinstance(res, Future)
            result = res.result()

        for idx, row in enumerate(result.rows()):
            assert isinstance(row, bytes)
            assert json.loads(row) == {'num': idx}
        test_env.assert_streaming_response_state(result)

    @pytest.mark.parametrize('query_type', [SyncQueryType.NORMAL, SyncQueryType.LAZY, SyncQueryType.CANCELLABLE])
    def test_query_positional_params(
        self, test_env: BlockingTestEnvironment, query_statement_pos_params_limit2: str, query_type: SyncQueryType
    ) -> None:
        if query_type == SyncQueryType.NORMAL:
            result = test_env.cluster_or_scope.execute_query(
                query_statement_pos_params_limit2, QueryOptions(positional_parameters=['United States'])
            )
        elif query_type == SyncQueryType.LAZY:
            result = test_env.cluster_or_scope.execute_query(
                query_statement_pos_params_limit2,
                QueryOptions(positional_parameters=['United States'], lazy_execute=True),
            )
        else:
            res = test_env.cluster_or_scope.execute_query(
                query_statement_pos_params_limit2,
                QueryOptions(positional_parameters=['United States']),
                enable_cancel=True,
            )
            assert isinstance(res, Future)
            result = res.result()
        test_env.assert_rows(result, 2)
        test_env.assert_streaming_response_state(result)

    @pytest.mark.parametrize('query_type', [SyncQueryType.NORMAL, SyncQueryType.LAZY, SyncQueryType.CANCELLABLE])
    def test_query_positional_params_no_option(
        self, test_env: BlockingTestEnvironment, query_statement_pos_params_limit2: str, query_type: SyncQueryType
    ) -> None:
        if query_type == SyncQueryType.NORMAL:
            result = test_env.cluster_or_scope.execute_query(query_statement_pos_params_limit2, 'United States')
        elif query_type == SyncQueryType.LAZY:
            # this format does not really make sense, if users are using static type checking it will prevent them
            # but, technically viable so we test it
            result = test_env.cluster_or_scope.execute_query(
                query_statement_pos_params_limit2,  # type: ignore[call-overload]
                'United States',
                lazy_execute=True,
            )
        else:
            res = test_env.cluster_or_scope.execute_query(
                query_statement_pos_params_limit2, 'United States', enable_cancel=True
            )
            assert isinstance(res, Future)
            result = res.result()
        test_env.assert_rows(result, 2)
        test_env.assert_streaming_response_state(result)

    @pytest.mark.parametrize('query_type', [SyncQueryType.NORMAL, SyncQueryType.LAZY, SyncQueryType.CANCELLABLE])
    def test_query_positional_params_override(
        self, test_env: BlockingTestEnvironment, query_statement_pos_params_limit2: str, query_type: SyncQueryType
    ) -> None:
        if query_type == SyncQueryType.NORMAL:
            result = test_env.cluster_or_scope.execute_query(
                query_statement_pos_params_limit2, QueryOptions(positional_parameters=['abcdefg']), 'United States'
            )
        elif query_type == SyncQueryType.LAZY:
            result = test_env.cluster_or_scope.execute_query(
                query_statement_pos_params_limit2,
                QueryOptions(positional_parameters=['abcdefg'], lazy_execute=True),
                'United States',
            )
        else:
            res = test_env.cluster_or_scope.execute_query(
                query_statement_pos_params_limit2,
                QueryOptions(positional_parameters=['abcdefg']),
                'United States',
                enable_cancel=True,
            )
            assert isinstance(res, Future)
            result = res.result()
        test_env.assert_rows(result, 2)
        test_env.assert_streaming_response_state(result)

    # We test lazy execution in a separate test
    @pytest.mark.parametrize('query_type', [SyncQueryType.NORMAL, SyncQueryType.CANCELLABLE])
    def test_query_raises_exception_prior_to_iterating(
        self, test_env: BlockingTestEnvironment, query_type: SyncQueryType
    ) -> None:
        statement = "I'm not N1QL!"
        if query_type == SyncQueryType.NORMAL:
            with pytest.raises(QueryError):
                test_env.cluster_or_scope.execute_query(statement)
        else:
            res = test_env.cluster_or_scope.execute_query(statement, enable_cancel=True)
            assert isinstance(res, Future)
            with pytest.raises(QueryError):
                res.result()

    @pytest.mark.parametrize('query_type', [SyncQueryType.NORMAL, SyncQueryType.LAZY, SyncQueryType.CANCELLABLE])
    def test_query_raw_options(
        self, test_env: BlockingTestEnvironment, query_statement_pos_params_limit2: str, query_type: SyncQueryType
    ) -> None:
        # via raw, we should be able to pass any option
        # if using named params, need to match full name param in query
        # which is different for when we pass in name_parameters via their specific
        # query option (i.e. include the $ when using raw)
        if test_env.use_scope:
            statement = f'SELECT * FROM {test_env.collection_name} WHERE country = $country LIMIT $1;'
        else:
            statement = f'SELECT * FROM {test_env.fqdn} WHERE country = $country LIMIT $1;'

        if query_type == SyncQueryType.NORMAL:
            result = test_env.cluster_or_scope.execute_query(
                statement, QueryOptions(raw={'$country': 'United States', 'args': [2]})
            )
        elif query_type == SyncQueryType.LAZY:
            result = test_env.cluster_or_scope.execute_query(
                statement, QueryOptions(raw={'$country': 'United States', 'args': [2]}, lazy_execute=True)
            )
        else:
            res = test_env.cluster_or_scope.execute_query(
                statement, QueryOptions(raw={'$country': 'United States', 'args': [2]}), enable_cancel=True
            )
            assert isinstance(res, Future)
            result = res.result()

        test_env.assert_rows(result, 2)

        if query_type == SyncQueryType.NORMAL:
            result = test_env.cluster_or_scope.execute_query(
                query_statement_pos_params_limit2, QueryOptions(raw={'args': ['United States']})
            )
        elif query_type == SyncQueryType.LAZY:
            result = test_env.cluster_or_scope.execute_query(
                query_statement_pos_params_limit2, QueryOptions(raw={'args': ['United States']}, lazy_execute=True)
            )
        else:
            res = test_env.cluster_or_scope.execute_query(
                query_statement_pos_params_limit2, QueryOptions(raw={'args': ['United States']}), enable_cancel=True
            )
            assert isinstance(res, Future)
            result = res.result()
        test_env.assert_rows(result, 2)
        test_env.assert_streaming_response_state(result)

    @pytest.mark.parametrize('query_type', [SyncQueryType.NORMAL, SyncQueryType.LAZY, SyncQueryType.CANCELLABLE])
    def test_query_timeout(self, test_env: BlockingTestEnvironment, query_type: SyncQueryType) -> None:
        statement = 'SELECT sleep("some value", 10000) AS some_field;'

        if query_type == SyncQueryType.NORMAL:
            with pytest.raises(TimeoutError):
                result = test_env.cluster_or_scope.execute_query(statement, QueryOptions(timeout=timedelta(seconds=2)))
        elif query_type == SyncQueryType.LAZY:
            result = test_env.cluster_or_scope.execute_query(
                statement, QueryOptions(timeout=timedelta(seconds=2), lazy_execute=True)
            )
            with pytest.raises(TimeoutError):
                for _ in result.rows():
                    pass
        else:
            res = test_env.cluster_or_scope.execute_query(
                statement, QueryOptions(timeout=timedelta(seconds=2)), enable_cancel=True
            )
            assert isinstance(res, Future)
            with pytest.raises(TimeoutError):
                result = res.result()

    @pytest.mark.parametrize('query_type', [SyncQueryType.NORMAL, SyncQueryType.LAZY, SyncQueryType.CANCELLABLE])
    def test_query_timeout_while_streaming(self, test_env: BlockingTestEnvironment, query_type: SyncQueryType) -> None:
        statement = 'SELECT {"x1": 1, "x2": 2, "x3": 3} FROM range(1, 100000) r;'
        if query_type == SyncQueryType.NORMAL:
            result = test_env.cluster_or_scope.execute_query(statement, QueryOptions(timeout=timedelta(seconds=2)))
        elif query_type == SyncQueryType.LAZY:
            result = test_env.cluster_or_scope.execute_query(
                statement, QueryOptions(timeout=timedelta(seconds=2), lazy_execute=True)
            )
        else:
            res = test_env.cluster_or_scope.execute_query(
                statement, QueryOptions(timeout=timedelta(seconds=2)), enable_cancel=True
            )
            assert isinstance(res, Future)
            result = res.result()

        with pytest.raises(TimeoutError):
            for _ in result.rows():
                pass
        test_env.assert_streaming_response_state(result)

    @pytest.mark.parametrize('query_type', [SyncQueryType.NORMAL, SyncQueryType.LAZY, SyncQueryType.CANCELLABLE])
    def test_simple_query(
        self, test_env: BlockingTestEnvironment, query_statement_limit2: str, query_type: SyncQueryType
    ) -> None:
        if query_type == SyncQueryType.NORMAL:
            result = test_env.cluster_or_scope.execute_query(query_statement_limit2)
        elif query_type == SyncQueryType.LAZY:
            result = test_env.cluster_or_scope.execute_query(query_statement_limit2, lazy_execute=True)
        else:
            res = test_env.cluster_or_scope.execute_query(query_statement_limit2, enable_cancel=True)
            assert isinstance(res, Future)
            result = res.result()
        test_env.assert_rows(result, 2)
        test_env.assert_streaming_response_state(result)

    def test_query_with_lazy_execution(self, test_env: BlockingTestEnvironment, query_statement_limit2: str) -> None:
        result = test_env.cluster_or_scope.execute_query(query_statement_limit2, QueryOptions(lazy_execute=True))
        expected_state = RequestState.NotStarted
        assert result._http_response._request_context.request_state == expected_state
        expected_state = RequestState.StreamingResults
        count = 0
        for row in result.rows():
            assert result._http_response._request_context.request_state == expected_state
            assert row is not None
            count += 1
        assert count == 2
        test_env.assert_streaming_response_state(result)

    def test_query_with_lazy_execution_raises_exception(self, test_env: BlockingTestEnvironment) -> None:
        statement = "I'm not N1QL!"
        result = test_env.cluster_or_scope.execute_query(statement, QueryOptions(lazy_execute=True))
        expected_state = RequestState.NotStarted
        assert result._http_response._request_context.request_state == expected_state
        with pytest.raises(QueryError):
            list(result.rows())
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
    def couchbase_test_environment(
        self, sync_test_env: BlockingTestEnvironment
    ) -> YieldFixture[BlockingTestEnvironment]:
        sync_test_env.setup()
        yield sync_test_env
        sync_test_env.teardown()


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
    def couchbase_test_environment(
        self, sync_test_env: BlockingTestEnvironment
    ) -> YieldFixture[BlockingTestEnvironment]:
        sync_test_env.setup()
        test_env = sync_test_env.enable_scope()
        yield test_env
        test_env.disable_scope()
        test_env.teardown()
