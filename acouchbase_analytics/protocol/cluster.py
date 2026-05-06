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

import sys
from typing import TYPE_CHECKING, Awaitable, Optional
from uuid import uuid4

if sys.version_info < (3, 10):
    from typing_extensions import TypeAlias
else:
    from typing import TypeAlias

from acouchbase_analytics.protocol._core.anyio_utils import current_async_library
from acouchbase_analytics.protocol._core.client_adapter import _AsyncClientAdapter
from acouchbase_analytics.protocol._core.request_context import AsyncRequestContext, AsyncStreamingRequestContext
from acouchbase_analytics.protocol._core.response import AsyncHttpResponse
from acouchbase_analytics.protocol.query_handle import AsyncQueryHandle
from acouchbase_analytics.protocol.streaming import AsyncHttpStreamingResponse
from couchbase_analytics.common.logging import LogLevel
from couchbase_analytics.common.result import AsyncQueryResult
from couchbase_analytics.protocol._core.request import _RequestBuilder

if TYPE_CHECKING:
    from acouchbase_analytics.options import ClusterOptions
    from couchbase_analytics.common._core import JsonStreamConfig
    from couchbase_analytics.common.credential import Credential


class AsyncCluster:
    def __init__(
        self, endpoint: str, credential: Credential, options: Optional[ClusterOptions] = None, **kwargs: object
    ) -> None:
        self._cluster_id = str(uuid4())
        kwargs['cluster_id'] = self._cluster_id
        self._client_adapter = _AsyncClientAdapter(endpoint, credential, options, **kwargs)
        self._request_builder = _RequestBuilder(self._client_adapter)
        self._backend = current_async_library()

    @property
    def client_adapter(self) -> _AsyncClientAdapter:
        """
        **INTERNAL**
        """
        return self._client_adapter

    @property
    def cluster_id(self) -> str:
        """
        **INTERNAL**
        """
        return self._cluster_id

    @property
    def has_client(self) -> bool:
        """
        bool: Indicator on if the cluster HTTP client has been created or not.
        """
        return self._client_adapter.has_client

    async def _shutdown(self) -> None:
        """
        **INTERNAL**
        """
        await self._client_adapter.close_client()
        self._client_adapter.reset_client()

    async def _create_client(self) -> None:
        """
        **INTERNAL**
        """
        await self._client_adapter.create_client()

    async def shutdown(self) -> None:
        """Shuts down this cluster instance. Cleaning up all resources associated with it.

        .. warning::
            Use of this method is almost *always* unnecessary.  Cluster resources should be cleaned
            up once the cluster instance falls out of scope.  However, in some applications tuning resources
            is necessary and in those types of applications, this method might be beneficial.

        """
        if self.has_client:
            await self._shutdown()
        else:
            self.client_adapter.log_message('Cluster does not have a connection.  Ignoring shutdown.', LogLevel.WARNING)

    async def set_credential(self, credential: Credential) -> None:
        await self._client_adapter.update_credential(credential)

    async def _execute_query(self, http_resp: AsyncHttpStreamingResponse) -> AsyncQueryResult:
        if not self.has_client:
            self.client_adapter.log_message(
                'Cluster does not have a connection.  Creating the client.', LogLevel.WARNING
            )
            await self._create_client()
        await http_resp.send_request()
        return AsyncQueryResult(http_resp)

    def execute_query(self, statement: str, *args: object, **kwargs: object) -> Awaitable[AsyncQueryResult]:
        req = self._request_builder.build_query_request(statement, *args, **kwargs)
        stream_config = req.options.pop('stream_config', None)
        request_context = AsyncStreamingRequestContext(
            self.client_adapter, req, stream_config=stream_config, backend=self._backend
        )
        resp = AsyncHttpStreamingResponse(request_context)
        if self._backend.backend_lib == 'asyncio':
            return request_context.create_response_task(self._execute_query, resp)
        return self._execute_query(resp)

    async def _start_query(
        self, http_resp: AsyncHttpResponse, stream_config: Optional[JsonStreamConfig]
    ) -> AsyncQueryHandle:
        if not self.has_client:
            self.client_adapter.log_message(
                'Cluster does not have a connection.  Creating the client.', LogLevel.WARNING
            )
            await self._create_client()
        await http_resp.send_request()
        return AsyncQueryHandle(self._client_adapter, self._request_builder, http_resp, stream_config=stream_config)

    def start_query(self, statement: str, *args: object, **kwargs: object) -> Awaitable[AsyncQueryHandle]:
        req = self._request_builder.build_start_query_request(statement, *args, **kwargs)
        stream_config = req.options.pop('stream_config', None)
        request_context = AsyncRequestContext(self.client_adapter, req, backend=self._backend)
        resp = AsyncHttpResponse(request_context)
        if self._backend.backend_lib == 'asyncio':
            return request_context.create_response_task(self._start_query, resp, stream_config)
        return self._start_query(resp, stream_config)

    @classmethod
    def create_instance(
        cls, endpoint: str, credential: Credential, options: Optional[ClusterOptions] = None, **kwargs: object
    ) -> AsyncCluster:
        return cls(endpoint, credential, options, **kwargs)


Cluster: TypeAlias = AsyncCluster
