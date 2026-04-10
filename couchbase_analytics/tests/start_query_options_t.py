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

from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict, List, Optional, Union

import pytest

from couchbase_analytics import JSONType
from couchbase_analytics.credential import Credential
from couchbase_analytics.options import StartQueryOptions, StartQueryOptionsKwargs
from couchbase_analytics.protocol._core.client_adapter import _ClientAdapter
from couchbase_analytics.protocol._core.request import _RequestBuilder
from couchbase_analytics.protocol.options import StartQueryOptionsTransformedKwargs


@dataclass
class QueryContext:
    database_name: Optional[str] = None
    scope_name: Optional[str] = None

    def validate_query_context(self, body: Dict[str, Union[str, object]]) -> None:
        if self.database_name is None or self.scope_name is None:
            with pytest.raises(KeyError):
                body['query_context']
        else:
            assert body['query_context'] == f'default:`{self.database_name}`.`{self.scope_name}`'


class StartQueryOptionsTestSuite:
    TEST_MANIFEST = [
        'test_options_max_retries',
        'test_options_max_retries_kwargs',
        'test_options_named_parameters',
        'test_options_named_parameters_kwargs',
        'test_options_positional_parameters',
        'test_options_positional_parameters_kwargs',
        'test_options_raw',
        'test_options_raw_kwargs',
        'test_options_readonly',
        'test_options_readonly_kwargs',
        'test_options_scan_consistency',
        'test_options_scan_consistency_kwargs',
        'test_options_timeout',
        'test_options_timeout_kwargs',
        'test_options_timeout_must_be_positive',
        'test_options_timeout_must_be_positive_kwargs',
    ]

    @pytest.fixture(scope='class')
    def query_statment(self) -> str:
        return 'SELECT * FROM default'

    @pytest.mark.parametrize('max_retries', [5, 10, 0, None])
    def test_options_max_retries(
        self, query_statment: str, request_builder: _RequestBuilder, query_ctx: QueryContext, max_retries: Optional[int]
    ) -> None:
        if max_retries is not None:
            q_opts = StartQueryOptions(max_retries=max_retries)
            req = request_builder.build_start_query_request(query_statment, q_opts)
        else:
            req = request_builder.build_start_query_request(query_statment)
        exp_opts: StartQueryOptionsTransformedKwargs = {}
        assert req.options == exp_opts
        assert req.max_retries == (max_retries if max_retries is not None else 7)
        query_ctx.validate_query_context(req.body)

    @pytest.mark.parametrize('max_retries', [5, 10, 0, None])
    def test_options_max_retries_kwargs(
        self, query_statment: str, request_builder: _RequestBuilder, query_ctx: QueryContext, max_retries: Optional[int]
    ) -> None:
        if max_retries is not None:
            kwargs: StartQueryOptionsKwargs = {'max_retries': max_retries}
            req = request_builder.build_start_query_request(query_statment, **kwargs)
        else:
            req = request_builder.build_start_query_request(query_statment)
        exp_opts: StartQueryOptionsTransformedKwargs = {}
        assert req.options == exp_opts
        assert req.max_retries == (max_retries if max_retries is not None else 7)
        query_ctx.validate_query_context(req.body)

    def test_options_named_parameters(
        self, query_statment: str, request_builder: _RequestBuilder, query_ctx: QueryContext
    ) -> None:
        params: Dict[str, JSONType] = {'foo': 'bar', 'baz': 1, 'quz': False}
        q_opts = StartQueryOptions(named_parameters=params)
        req = request_builder.build_start_query_request(query_statment, q_opts)
        exp_opts: StartQueryOptionsTransformedKwargs = {'named_parameters': params}
        assert req.options == exp_opts
        query_ctx.validate_query_context(req.body)

    def test_options_named_parameters_kwargs(
        self, query_statment: str, request_builder: _RequestBuilder, query_ctx: QueryContext
    ) -> None:
        params: Dict[str, JSONType] = {'foo': 'bar', 'baz': 1, 'quz': False}
        kwargs: StartQueryOptionsKwargs = {'named_parameters': params}
        req = request_builder.build_start_query_request(query_statment, **kwargs)
        exp_opts: StartQueryOptionsTransformedKwargs = {'named_parameters': params}
        assert req.options == exp_opts
        query_ctx.validate_query_context(req.body)

    def test_options_positional_parameters(
        self, query_statment: str, request_builder: _RequestBuilder, query_ctx: QueryContext
    ) -> None:
        params: List[JSONType] = ['foo', 'bar', 1, False]
        q_opts = StartQueryOptions(positional_parameters=params)
        req = request_builder.build_start_query_request(query_statment, q_opts)
        exp_opts: StartQueryOptionsTransformedKwargs = {'positional_parameters': params}
        assert req.options == exp_opts
        query_ctx.validate_query_context(req.body)

    def test_options_positional_parameters_kwargs(
        self, query_statment: str, request_builder: _RequestBuilder, query_ctx: QueryContext
    ) -> None:
        params: List[JSONType] = ['foo', 'bar', 1, False]
        kwargs: StartQueryOptionsKwargs = {'positional_parameters': params}
        req = request_builder.build_start_query_request(query_statment, **kwargs)
        exp_opts: StartQueryOptionsTransformedKwargs = {'positional_parameters': params}
        assert req.options == exp_opts
        query_ctx.validate_query_context(req.body)

    def test_options_raw(self, query_statment: str, request_builder: _RequestBuilder, query_ctx: QueryContext) -> None:
        pos_params: List[JSONType] = ['foo', 'bar', 1, False]
        params: Dict[str, Any] = {'readonly': True, 'positional_params': pos_params}
        q_opts = StartQueryOptions(raw=params)
        req = request_builder.build_start_query_request(query_statment, q_opts)
        exp_opts: StartQueryOptionsTransformedKwargs = {'raw': params}
        assert req.options == exp_opts
        query_ctx.validate_query_context(req.body)

    def test_options_raw_kwargs(
        self, query_statment: str, request_builder: _RequestBuilder, query_ctx: QueryContext
    ) -> None:
        pos_params: List[JSONType] = ['foo', 'bar', 1, False]
        params: Dict[str, Any] = {'readonly': True, 'positional_params': pos_params}
        kwargs: StartQueryOptionsKwargs = {'raw': params}
        req = request_builder.build_start_query_request(query_statment, **kwargs)
        exp_opts: StartQueryOptionsTransformedKwargs = {'raw': params}
        assert req.options == exp_opts
        query_ctx.validate_query_context(req.body)

    def test_options_readonly(
        self, query_statment: str, request_builder: _RequestBuilder, query_ctx: QueryContext
    ) -> None:
        q_opts = StartQueryOptions(readonly=True)
        req = request_builder.build_start_query_request(query_statment, q_opts)
        exp_opts: StartQueryOptionsTransformedKwargs = {'readonly': True}
        assert req.options == exp_opts
        query_ctx.validate_query_context(req.body)

    def test_options_readonly_kwargs(
        self, query_statment: str, request_builder: _RequestBuilder, query_ctx: QueryContext
    ) -> None:
        kwargs: StartQueryOptionsKwargs = {'readonly': True}
        req = request_builder.build_start_query_request(query_statment, **kwargs)
        exp_opts: StartQueryOptionsTransformedKwargs = {'readonly': True}
        assert req.options == exp_opts
        query_ctx.validate_query_context(req.body)

    def test_options_scan_consistency(
        self, query_statment: str, request_builder: _RequestBuilder, query_ctx: QueryContext
    ) -> None:
        from couchbase_analytics.query import QueryScanConsistency

        q_opts = StartQueryOptions(scan_consistency=QueryScanConsistency.REQUEST_PLUS)
        req = request_builder.build_start_query_request(query_statment, q_opts)
        exp_opts: StartQueryOptionsTransformedKwargs = {'scan_consistency': QueryScanConsistency.REQUEST_PLUS.value}
        assert req.options == exp_opts
        query_ctx.validate_query_context(req.body)

    def test_options_scan_consistency_kwargs(
        self, query_statment: str, request_builder: _RequestBuilder, query_ctx: QueryContext
    ) -> None:
        from couchbase_analytics.query import QueryScanConsistency

        kwargs: StartQueryOptionsKwargs = {'scan_consistency': QueryScanConsistency.REQUEST_PLUS}
        req = request_builder.build_start_query_request(query_statment, **kwargs)
        exp_opts: StartQueryOptionsTransformedKwargs = {'scan_consistency': QueryScanConsistency.REQUEST_PLUS.value}
        assert req.options == exp_opts
        query_ctx.validate_query_context(req.body)

    def test_options_timeout(
        self, query_statment: str, request_builder: _RequestBuilder, query_ctx: QueryContext
    ) -> None:
        q_opts = StartQueryOptions(timeout=timedelta(seconds=20))
        req = request_builder.build_start_query_request(query_statment, q_opts)
        exp_opts: StartQueryOptionsTransformedKwargs = {'timeout': 20.0}
        assert req.options == exp_opts
        # NOTE: we add time to the server timeout to ensure a client side timeout
        assert req.body['timeout'] == '25000.0ms'
        query_ctx.validate_query_context(req.body)

    def test_options_timeout_kwargs(
        self, query_statment: str, request_builder: _RequestBuilder, query_ctx: QueryContext
    ) -> None:
        kwargs: StartQueryOptionsKwargs = {'timeout': timedelta(seconds=20)}
        req = request_builder.build_start_query_request(query_statment, **kwargs)
        exp_opts: StartQueryOptionsTransformedKwargs = {'timeout': 20.0}
        assert req.options == exp_opts
        # NOTE: we add time to the server timeout to ensure a client side timeout
        assert req.body['timeout'] == '25000.0ms'
        query_ctx.validate_query_context(req.body)

    def test_options_timeout_must_be_positive(self, query_statment: str, request_builder: _RequestBuilder) -> None:
        q_opts = StartQueryOptions(timeout=timedelta(seconds=-1))
        with pytest.raises(ValueError):
            request_builder.build_start_query_request(query_statment, q_opts)

    def test_options_timeout_must_be_positive_kwargs(
        self, query_statment: str, request_builder: _RequestBuilder
    ) -> None:
        kwargs: StartQueryOptionsKwargs = {'timeout': timedelta(seconds=-1)}
        with pytest.raises(ValueError):
            request_builder.build_start_query_request(query_statment, **kwargs)


class ClusterStartQueryOptionsTests(StartQueryOptionsTestSuite):
    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self) -> None:
        def valid_test_method(meth: str) -> bool:
            attr = getattr(ClusterStartQueryOptionsTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')

        method_list = [meth for meth in dir(ClusterStartQueryOptionsTests) if valid_test_method(meth)]
        test_list = set(StartQueryOptionsTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra tests: {test_list}.')

    @pytest.fixture(scope='class', name='query_ctx')
    def query_context(self) -> QueryContext:
        return QueryContext()

    @pytest.fixture(scope='class')
    def request_builder(self) -> _RequestBuilder:
        cred = Credential.from_username_and_password('Administrator', 'password')
        return _RequestBuilder(_ClientAdapter('https://localhost', cred))


class ScopeStartQueryOptionsTests(StartQueryOptionsTestSuite):
    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self) -> None:
        def valid_test_method(meth: str) -> bool:
            attr = getattr(ScopeStartQueryOptionsTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')

        method_list = [meth for meth in dir(ScopeStartQueryOptionsTests) if valid_test_method(meth)]
        test_list = set(StartQueryOptionsTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra tests: {test_list}.')

    @pytest.fixture(scope='class', name='query_ctx')
    def query_context(self) -> QueryContext:
        return QueryContext('test-database', 'test-scope')

    @pytest.fixture(scope='class')
    def request_builder(self) -> _RequestBuilder:
        cred = Credential.from_username_and_password('Administrator', 'password')
        return _RequestBuilder(_ClientAdapter('https://localhost', cred), 'test-database', 'test-scope')
