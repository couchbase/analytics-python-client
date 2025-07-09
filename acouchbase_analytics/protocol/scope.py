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

import sys
from typing import TYPE_CHECKING, Awaitable

if sys.version_info < (3, 10):
    from typing_extensions import TypeAlias
else:
    from typing import TypeAlias

from acouchbase_analytics.protocol._core.anyio_utils import current_async_library
from acouchbase_analytics.protocol._core.client_adapter import _AsyncClientAdapter
from acouchbase_analytics.protocol._core.request_context import AsyncRequestContext
from acouchbase_analytics.protocol.streaming import AsyncHttpStreamingResponse
from couchbase_analytics.common.result import AsyncQueryResult
from couchbase_analytics.protocol._core.request import _RequestBuilder

if TYPE_CHECKING:
    from acouchbase_analytics.protocol.database import AsyncDatabase


class AsyncScope:
    def __init__(self, database: AsyncDatabase, scope_name: str) -> None:
        self._database = database
        self._scope_name = scope_name
        self._request_builder = _RequestBuilder(self.client_adapter, self._database.name, self.name)
        self._backend = current_async_library()

    @property
    def client_adapter(self) -> _AsyncClientAdapter:
        """
        **INTERNAL**
        """
        return self._database.client_adapter

    @property
    def name(self) -> str:
        """
        str: The name of this :class:`~acouchbase_analytics.protocol.scope.Scope` instance.
        """
        return self._scope_name

    async def _create_client(self) -> None:
        """
        **INTERNAL**
        """
        await self.client_adapter.create_client()

    async def _execute_query(self, http_resp: AsyncHttpStreamingResponse) -> AsyncQueryResult:
        if not self.client_adapter.has_client:
            # TODO: add log message??
            await self._create_client()
        await http_resp.send_request()
        return AsyncQueryResult(http_resp)

    def execute_query(self, statement: str, *args: object, **kwargs: object) -> Awaitable[AsyncQueryResult]:
        base_req = self._request_builder.build_base_query_request(statement, *args, is_async=True, **kwargs)
        stream_config = base_req.options.pop('stream_config', None)
        request_context = AsyncRequestContext(
            client_adapter=self.client_adapter, request=base_req, stream_config=stream_config, backend=self._backend
        )
        resp = AsyncHttpStreamingResponse(request_context)
        if self._backend.backend_lib == 'asyncio':
            return request_context.create_response_task(self._execute_query, resp)
        return self._execute_query(resp)


Scope: TypeAlias = AsyncScope
