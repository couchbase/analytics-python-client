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
import pathlib
import sys

from os import path
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    List,
                    Optional,
                    TypedDict,
                    Union)

if sys.version_info < (3, 11):
    from typing_extensions import Unpack
else:
    from typing import  Unpack

import pytest

from acouchbase_analytics.cluster import AsyncCluster
from acouchbase_analytics.result import AsyncQueryResult
from acouchbase_analytics.scope import AsyncScope
from couchbase_analytics.cluster import Cluster
from couchbase_analytics.credential import Credential
from couchbase_analytics.options import ClusterOptions, SecurityOptions
from couchbase_analytics.result import BlockingQueryResult
from couchbase_analytics.scope import Scope

from tests import AnalyticsTestEnvironmentError
from tests.utils._run_web_server import WebServerHandler


if TYPE_CHECKING:
    from tests.analytics_config import AnalyticsConfig


TEST_AIRLINE_DATA_PATH = path.join(pathlib.Path(__file__).parent.parent,
                                   'test_data',
                                   'airline.json')

class TestEnvironmentOptionsKwargs(TypedDict, total=False):
    async_cluster: Optional[AsyncCluster]
    cluster: Optional[Cluster]
    database_name: Optional[str]
    scope_name: Optional[str]
    collection_name: Optional[str]
    server_handler: Optional[WebServerHandler]
    backend: Optional[str]

class TestEnvironment:

    def __init__(self, config: AnalyticsConfig, **kwargs: Unpack[TestEnvironmentOptionsKwargs]) -> None:
        self._config = config
        self._async_cluster = kwargs.pop('async_cluster', None)
        self._cluster = kwargs.pop('cluster', None)
        self._database_name = kwargs.pop('database_name', None)
        self._scope_name = kwargs.pop('scope_name', None)
        self._collection_name = kwargs.pop('collection_name', None)
        self._async_scope: Optional[AsyncScope] = None
        self._scope: Optional[Scope] = None
        self._use_scope = False
        self._server_handler = kwargs.pop('server_handler', None)

    @property
    def config(self) -> AnalyticsConfig:
        return self._config
    
    @property
    def fqdn(self) -> str:
        return self.config.fqdn

    @property
    def collection_name(self) -> Optional[str]:
        return self._collection_name

    @property
    def use_scope(self) -> bool:
        return self._use_scope

    def load_collection_data_from_file(self, file_path: str, limit: Optional[int] = 100) -> List[Dict[str, Any]]:
        with open(file_path, mode='+r') as json_file:
            json_data: List[Dict[str, Any]] = json.load(json_file)

        if limit is not None and len(json_data) > limit:
            return json_data[:limit]
        return json_data
    
class BlockingTestEnvironment(TestEnvironment):
    def __init__(self, config: AnalyticsConfig, **kwargs: Unpack[TestEnvironmentOptionsKwargs]) -> None:
        super().__init__(config, **kwargs)

    @property
    def cluster(self) -> Cluster:
        if self._cluster is None:
            raise AnalyticsTestEnvironmentError('No cluster available.')
        return self._cluster
    
    @property
    def scope(self) -> Scope:
        if self._scope is None:
            raise AnalyticsTestEnvironmentError('No scope available.')
        return self._scope

    @property
    def cluster_or_scope(self) -> Union[Cluster, Scope]:
        if self._scope is not None:
            return self.scope
        return self.cluster

    def assert_rows(self, result: BlockingQueryResult, expected_count: int) -> None:
        count = 0
        assert isinstance(result, (BlockingQueryResult,))
        for row in result.rows():
            assert row is not None
            count += 1
        assert count >= expected_count

    def disable_scope(self) -> BlockingTestEnvironment:
        self._scope = None
        self._use_scope = False
        return self
    
    def disable_test_server(self) -> BlockingTestEnvironment:
        if self._server_handler is not None:
            self._server_handler.stop_server()
            self._server_handler = None
        return self

    def enable_scope(self,
                     database_name: Optional[str] = None,
                     scope_name: Optional[str] = None) -> BlockingTestEnvironment:

        if self._cluster is None:
            raise AnalyticsTestEnvironmentError('No cluster available.')
        db_name = database_name if database_name is not None else self._database_name
        if db_name is None:
            raise AnalyticsTestEnvironmentError('Cannot create scope without a database name.')
        scope_name = scope_name if scope_name is not None else self._scope_name
        if scope_name is None:
            raise AnalyticsTestEnvironmentError('Cannot create scope without a scope name.')
        self._scope = self._cluster.database(db_name).scope(scope_name)
        self._use_scope = True
        return self

    def enable_test_server(self) -> BlockingTestEnvironment:
        if self._server_handler is None:
            raise AnalyticsTestEnvironmentError('No server handler provided, cannot enable test server.')
        if self._cluster is None or not hasattr(self._cluster, '_impl'):
            raise AnalyticsTestEnvironmentError('No cluster available, cannot enable test server.')
        from tests.utils._client_adapter import _TestClientAdapter
        from tests.utils._test_httpx import TestHTTPTransport
        new_adapter = _TestClientAdapter(adapter=self._cluster._impl._client_adapter,  # type: ignore[call-arg]
                                         http_transport_cls=TestHTTPTransport)
        new_adapter.create_client()
        self._cluster._impl._client_adapter = new_adapter
        scheme, host, port = self._cluster._impl.client_adapter.connection_details.get_scheme_host_and_port()
        print(f"Connecting to test server at {scheme}://{host}:{port}")
        self._server_handler.start_server()
        return self
    
    def setup(self) -> None:
        if self.config.create_keyspace is False:
            return

        setup_statements = [
            f'CREATE DATABASE `{self.config.database_name}` IF NOT EXISTS;',
            f'CREATE SCOPE `{self.config.database_name}`.`{self.config.scope_name}` IF NOT EXISTS;',
            ('CREATE COLLECTION '
             f'`{self.config.database_name}`.`{self.config.scope_name}`.`{self.config.collection_name}`'
             ' IF NOT EXISTS PRIMARY KEY (pk: UUID) AUTOGENERATED;')
        ]

        for statement in setup_statements:
            try:
                self.cluster.execute_query(statement)
            except Exception as ex:
                raise AnalyticsTestEnvironmentError(f'Unable to execute statement={statement}. Error: {ex}')

        json_data = self.load_collection_data_from_file(TEST_AIRLINE_DATA_PATH)
        docs = []
        for d in json_data:
            if 'collection' in d:
                d['collection'] = self.config.collection_name
            if 'scope' in d:
                d['scope'] = self.config.scope_name
            docs.append(json.dumps(d))
        statement = (f'USE `{self.config.database_name}`.`{self.config.scope_name}`; '
                     f'UPSERT INTO `{self.config.collection_name}` ({",".join(docs)})')

        try:
            self.cluster.execute_query(statement)
        except Exception as ex:
            raise AnalyticsTestEnvironmentError(f'Unable to load collection data. Error: {ex}')

    def set_url_path(self, url_path: str) -> None:
        if self._server_handler is None:
            raise AnalyticsTestEnvironmentError('No server handler provided, cannot set URL path.')
        if self._cluster is None or not hasattr(self._cluster, '_impl'):
            raise AnalyticsTestEnvironmentError('No cluster available, cannot enable test server.')
        self._cluster._impl._client_adapter.set_request_path(url_path)

    def teardown(self) -> None:
        if self.config.create_keyspace is False:
            return

        teardown_statements = [
            f'DROP DATABASE `{self.config.database_name}` IF EXISTS;',
            f'DROP SCOPE `{self.config.database_name}`.`{self.config.scope_name}` IF EXISTS;',
            ('DROP COLLECTION '
             f'`{self.config.database_name}`.`{self.config.scope_name}`.`{self.config.collection_name}`'
             ' IF EXISTS;')
        ]

        for statement in teardown_statements:
            try:
                self.cluster.execute_query(statement)
            except Exception as ex:
                raise AnalyticsTestEnvironmentError(f'Unable to execute statement={statement}. Error: {ex}')

    def update_request_extensions(self, extensions: Dict[str, object]) -> None:
        if self._server_handler is None:
            raise AnalyticsTestEnvironmentError('No server handler provided, cannot update request extensions.')
        if self._cluster is None or not hasattr(self._cluster, '_impl'):
            raise AnalyticsTestEnvironmentError('No cluster available, cannot enable test server.')
        self._cluster._impl._client_adapter.update_request_extensions(extensions)

    def update_request_json(self, json: Dict[str, object]) -> None:
        if self._server_handler is None:
            raise AnalyticsTestEnvironmentError('No server handler provided, cannot update request JSON.')
        if self._cluster is None or not hasattr(self._cluster, '_impl'):
            raise AnalyticsTestEnvironmentError('No cluster available, cannot enable test server.')
        self._cluster._impl._client_adapter.update_request_json(json)

    @classmethod
    def get_environment(cls,
                        config: AnalyticsConfig,
                        server_handler: Optional[WebServerHandler]=None) -> BlockingTestEnvironment:
        if config is None:
            raise AnalyticsTestEnvironmentError('No test config provided.')

        env_opts: TestEnvironmentOptionsKwargs = {}
        if server_handler is not None:
            connstr = server_handler.connstr
            env_opts['server_handler'] = server_handler
        else:
            connstr = config.get_connection_string()
        username, pw = config.get_username_and_pw()
        cred = Credential.from_username_and_password(username, pw)
        sec_opts: Optional[SecurityOptions] = None
        if config.nonprod is True:
            from couchbase_analytics.common.core._certificates import _Certificates
            sec_opts = SecurityOptions.trust_only_certificates(_Certificates.get_nonprod_certificates())

        if config.disable_server_certificate_verification is True:
            if sec_opts is not None:
                sec_opts['disable_server_certificate_verification'] = True
            else:
                sec_opts = SecurityOptions(disable_server_certificate_verification=True)

        if sec_opts is not None:
            opts = ClusterOptions(security_options=sec_opts)
            env_opts['cluster'] = Cluster.create_instance(connstr, cred, opts)
        else:
            env_opts['cluster'] = Cluster.create_instance(connstr, cred)

        env_opts['database_name'] = config.database_name
        env_opts['scope_name'] = config.scope_name
        env_opts['collection_name'] = config.collection_name
        return cls(config, **env_opts)
    

    
class AsyncTestEnvironment(TestEnvironment):
    def __init__(self, config: AnalyticsConfig, **kwargs: Unpack[TestEnvironmentOptionsKwargs]) -> None:
        self._backend = kwargs.pop('backend', None)
        super().__init__(config, **kwargs)

    @property
    def cluster(self) -> AsyncCluster:
        if self._async_cluster is None:
            raise AnalyticsTestEnvironmentError('No async cluster available.')
        return self._async_cluster

    @property
    def cluster_or_scope(self) -> Union[AsyncCluster, AsyncScope]:
        if self._async_scope is not None:
            return self.scope
        return self.cluster
    
    @property
    def scope(self) -> AsyncScope:
        if self._async_scope is None:
            raise AnalyticsTestEnvironmentError('No scope available.')
        return self._async_scope

    async def assert_rows(self, result: AsyncQueryResult, expected_count: int) -> None:
        count = 0
        assert isinstance(result, (AsyncQueryResult,))
        async for row in result.rows():
            assert row is not None
            count += 1
        assert count >= expected_count

    def disable_scope(self) -> AsyncTestEnvironment:
        self._async_scope = None
        self._use_scope = False
        return self
    
    def disable_test_server(self) -> AsyncTestEnvironment:
        if self._server_handler is not None:
            self._server_handler.stop_server()
            self._server_handler = None
        return self

    def enable_scope(self,
                     database_name: Optional[str] = None,
                     scope_name: Optional[str] = None) -> AsyncTestEnvironment:

        if self._async_cluster is None:
            raise AnalyticsTestEnvironmentError('No cluster available.')
        db_name = database_name if database_name is not None else self._database_name
        if db_name is None:
            raise AnalyticsTestEnvironmentError('Cannot create scope without a database name.')
        scope_name = scope_name if scope_name is not None else self._scope_name
        if scope_name is None:
            raise AnalyticsTestEnvironmentError('Cannot create scope without a scope name.')
        self._async_scope = self._async_cluster.database(db_name).scope(scope_name)
        self._use_scope = True
        return self

    async def enable_test_server(self) -> AsyncTestEnvironment:
        if self._server_handler is None:
            raise AnalyticsTestEnvironmentError('No server handler provided, cannot enable test server.')
        if self._async_cluster is None or not hasattr(self._async_cluster, '_impl'):
            raise AnalyticsTestEnvironmentError('No cluster available, cannot enable test server.')
        from tests.utils._async_client_adapter import _TestAsyncClientAdapter
        from tests.utils._test_async_httpx import TestAsyncHTTPTransport
        # close the adapter here b/c we need to await
        await self._async_cluster._impl._client_adapter.close_client()
        new_adapter = _TestAsyncClientAdapter(adapter=self._async_cluster._impl._client_adapter,  # type: ignore[call-arg]
                                              http_transport_cls=TestAsyncHTTPTransport)
        await new_adapter.create_client()
        self._async_cluster._impl._client_adapter = new_adapter
        scheme, host, port = self._async_cluster._impl.client_adapter.connection_details.get_scheme_host_and_port()
        print(f"Connecting to test server at {scheme}://{host}:{port}")
        self._server_handler.start_server()
        return self
    
    async def setup(self) -> None:
        if self.config.create_keyspace is False:
            return

        setup_statements = [
            f'CREATE DATABASE `{self.config.database_name}` IF NOT EXISTS;',
            f'CREATE SCOPE `{self.config.database_name}`.`{self.config.scope_name}` IF NOT EXISTS;',
            ('CREATE COLLECTION '
             f'`{self.config.database_name}`.`{self.config.scope_name}`.`{self.config.collection_name}`'
             ' IF NOT EXISTS PRIMARY KEY (pk: UUID) AUTOGENERATED;')
        ]

        for statement in setup_statements:
            try:
                await self.cluster.execute_query(statement)
            except Exception as ex:
                raise AnalyticsTestEnvironmentError(f'Unable to execute statement={statement}. Error: {ex}')

        json_data = self.load_collection_data_from_file(TEST_AIRLINE_DATA_PATH)
        docs = []
        for d in json_data:
            if 'collection' in d:
                d['collection'] = self.config.collection_name
            if 'scope' in d:
                d['scope'] = self.config.scope_name
            docs.append(json.dumps(d))
        statement = (f'USE `{self.config.database_name}`.`{self.config.scope_name}`; '
                     f'UPSERT INTO `{self.config.collection_name}` ({",".join(docs)})')

        try:
            await self.cluster.execute_query(statement)
        except Exception as ex:
            raise AnalyticsTestEnvironmentError(f'Unable to load collection data. Error: {ex}')
    
    def set_url_path(self, url_path: str) -> None:
        if self._server_handler is None:
            raise AnalyticsTestEnvironmentError('No server handler provided, cannot set URL path.')
        if self._async_cluster is None or not hasattr(self._async_cluster, '_impl'):
            raise AnalyticsTestEnvironmentError('No cluster available, cannot enable test server.')
        self._async_cluster._impl._client_adapter.set_request_path(url_path)

    async def teardown(self) -> None:
        if self.config.create_keyspace is False:
            return

        teardown_statements = [
            f'DROP DATABASE `{self.config.database_name}` IF EXISTS;',
            f'DROP SCOPE `{self.config.database_name}`.`{self.config.scope_name}` IF EXISTS;',
            ('DROP COLLECTION '
             f'`{self.config.database_name}`.`{self.config.scope_name}`.`{self.config.collection_name}`'
             ' IF EXISTS;')
        ]

        for statement in teardown_statements:
            try:
                await self.cluster.execute_query(statement)
            except Exception as ex:
                raise AnalyticsTestEnvironmentError(f'Unable to execute statement={statement}. Error: {ex}')

    def update_request_extensions(self, extensions: Dict[str, object]) -> None:
        if self._server_handler is None:
            raise AnalyticsTestEnvironmentError('No server handler provided, cannot update request extensions.')
        if self._async_cluster is None or not hasattr(self._async_cluster, '_impl'):
            raise AnalyticsTestEnvironmentError('No cluster available, cannot enable test server.')
        self._async_cluster._impl._client_adapter.update_request_extensions(extensions)

    def update_request_json(self, json: Dict[str, object]) -> None:
        if self._server_handler is None:
            raise AnalyticsTestEnvironmentError('No server handler provided, cannot update request JSON.')
        if self._async_cluster is None or not hasattr(self._async_cluster, '_impl'):
            raise AnalyticsTestEnvironmentError('No cluster available, cannot enable test server.')
        self._async_cluster._impl._client_adapter.update_request_json(json)

    @classmethod
    def get_environment(cls,
                        config: AnalyticsConfig,
                        server_handler: Optional[WebServerHandler]=None,
                        backend: Optional[str]=None) -> AsyncTestEnvironment:
        if config is None:
            raise AnalyticsTestEnvironmentError('No test config provided.')

        env_opts: TestEnvironmentOptionsKwargs = {}
        if server_handler is not None:
            connstr = server_handler.connstr
            env_opts['server_handler'] = server_handler
        else:
            connstr = config.get_connection_string()
        if backend is not None:
            env_opts['backend'] = backend
        username, pw = config.get_username_and_pw()
        cred = Credential.from_username_and_password(username, pw)
        sec_opts: Optional[SecurityOptions] = None
        if config.nonprod is True:
            from couchbase_analytics.common.core._certificates import _Certificates
            sec_opts = SecurityOptions.trust_only_certificates(_Certificates.get_nonprod_certificates())

        if config.disable_server_certificate_verification is True:
            if sec_opts is not None:
                sec_opts['disable_server_certificate_verification'] = True
            else:
                sec_opts = SecurityOptions(disable_server_certificate_verification=True)

        print(f'{env_opts=}')
        if sec_opts is not None:
            opts = ClusterOptions(security_options=sec_opts)
            env_opts['async_cluster'] = AsyncCluster.create_instance(connstr, cred, opts)
        else:
            env_opts['async_cluster'] = AsyncCluster.create_instance(connstr, cred)

        env_opts['database_name'] = config.database_name
        env_opts['scope_name'] = config.scope_name
        env_opts['collection_name'] = config.collection_name
        return cls(config, **env_opts)
    
@pytest.fixture(scope='class', name='sync_test_env')
def base_test_environment(analytics_config: AnalyticsConfig) -> BlockingTestEnvironment:
    print("Creating sync test environment")
    return BlockingTestEnvironment.get_environment(analytics_config)

@pytest.fixture(scope='class', name='sync_test_env_with_server')
def base_test_environment_with_server(analytics_config: AnalyticsConfig) -> BlockingTestEnvironment:
    print("Creating sync test environment w/ test server")
    server_handler = WebServerHandler()
    return BlockingTestEnvironment.get_environment(analytics_config, server_handler=server_handler)

@pytest.fixture(scope='class', name='async_test_env')
def base_async_test_environment(analytics_config: AnalyticsConfig, anyio_backend: str) -> AsyncTestEnvironment:
    print("Creating async test environment")
    return AsyncTestEnvironment.get_environment(analytics_config, backend=anyio_backend)

@pytest.fixture(scope='class', name='async_test_env_with_server')
def base_async_test_environment_with_server(analytics_config: AnalyticsConfig, anyio_backend:str) -> AsyncTestEnvironment:
    print("Creating async test environment w/ test server")
    server_handler = WebServerHandler()
    return AsyncTestEnvironment.get_environment(analytics_config,
                                                server_handler=server_handler,
                                                backend=anyio_backend)