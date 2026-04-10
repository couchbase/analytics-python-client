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

from typing import TYPE_CHECKING, Any, Optional

from acouchbase_analytics.protocol._core.client_adapter import _AsyncClientAdapter
from acouchbase_analytics.protocol._core.request_context import AsyncRequestContext, AsyncStreamingRequestContext
from acouchbase_analytics.protocol._core.response import AsyncHttpResponse
from acouchbase_analytics.protocol.streaming import AsyncHttpStreamingResponse
from couchbase_analytics.common._core.query_handle import QueryHandleStatusResponse
from couchbase_analytics.common.errors import AnalyticsError, QueryNotFoundError
from couchbase_analytics.common.query_handle import AsyncQueryHandle as _CoreAsyncQueryHandle
from couchbase_analytics.common.query_handle import AsyncQueryResultHandle as _CoreAsyncQueryResultHandle
from couchbase_analytics.common.query_handle import AsyncQueryStatus as _CoreAsyncQueryStatus
from couchbase_analytics.common.result import AsyncQueryResult
from couchbase_analytics.protocol._core.request import _RequestBuilder

if TYPE_CHECKING:
    from acouchbase_analytics.options import FetchResultsOptions
    from couchbase_analytics.common._core import JsonStreamConfig


class AsyncQueryHandle(_CoreAsyncQueryHandle):
    def __init__(
        self,
        client_adapter: _AsyncClientAdapter,
        request_builder: _RequestBuilder,
        http_response: AsyncHttpResponse,
        stream_config: Optional[JsonStreamConfig] = None,
    ) -> None:
        super().__init__()
        self._client_adapter = client_adapter
        self._request_builder = request_builder
        self._http_response = http_response
        self._stream_config = stream_config
        self._request_id: str = ''
        self._handle: str = ''
        self._get_status_handle()

    async def fetch_status(self, options: Optional[Any] = None, **kwargs: Any) -> AsyncQueryStatus:
        server_req = self._request_builder.build_request_from_handle(self._handle)
        request_context = AsyncRequestContext(self._client_adapter, server_req)
        resp = AsyncHttpResponse(request_context)
        await resp.send_request()
        if resp.json_response is None:
            raise AnalyticsError(message='HTTP response does not contain JSON data.')

        status_response = self._get_handle_status_response(resp)
        return AsyncQueryStatus(
            self._client_adapter,
            self._request_builder,
            status_response,
            stream_config=self._stream_config,
        )

    async def cancel(self, options: Optional[Any] = None, **kwargs: Any) -> None:
        cancel_req = self._request_builder.build_cancel_request(self._request_id)
        request_context = AsyncRequestContext(self._client_adapter, cancel_req)
        resp = AsyncHttpResponse(request_context, has_no_body_response=True, request_id=self._request_id)
        await resp.send_request()

    def _get_status_handle(self) -> None:
        if self._http_response.json_response is None:
            raise AnalyticsError(message='HTTP response does not contain JSON data.')

        request_id = self._http_response.json_response.get('requestID', None)
        if request_id is None:
            raise QueryNotFoundError(message='Server response is missing "requestID" field.')
        handle = self._http_response.json_response.get('handle', None)
        if handle is None:
            raise QueryNotFoundError(message='Server response is missing "handle" field.')

        self._request_id = request_id
        self._handle = handle

    def _get_handle_status_response(self, resp: AsyncHttpResponse) -> QueryHandleStatusResponse:
        if resp.json_response is None:
            raise AnalyticsError(message='HTTP response does not contain JSON data.')

        return QueryHandleStatusResponse.from_server(self._request_id, resp.json_response)


class AsyncQueryResultHandle(_CoreAsyncQueryResultHandle):
    def __init__(
        self,
        client_adapter: _AsyncClientAdapter,
        request_builder: _RequestBuilder,
        request_id: str,
        handle: str,
        stream_config: Optional[JsonStreamConfig] = None,
    ) -> None:
        super().__init__()
        self._client_adapter = client_adapter
        self._request_builder = request_builder
        self._request_id = request_id
        self._handle = handle
        self._stream_config = stream_config

    async def fetch_results(self, options: Optional[FetchResultsOptions] = None, **kwargs: Any) -> AsyncQueryResult:
        server_req = self._request_builder.build_fetch_results_request(self._handle, options, **kwargs)
        request_context = AsyncStreamingRequestContext(
            self._client_adapter, server_req, stream_config=self._stream_config
        )
        resp = AsyncHttpStreamingResponse(request_context, request_id=self._request_id)
        await resp.send_request()
        return AsyncQueryResult(resp)

    async def discard_results(self, options: Optional[Any] = None, **kwargs: Any) -> None:
        req = self._request_builder.build_discard_results_request(self._handle)
        request_context = AsyncRequestContext(self._client_adapter, req)
        resp = AsyncHttpResponse(request_context, has_no_body_response=True, request_id=self._request_id)
        await resp.send_request()


class AsyncQueryStatus(_CoreAsyncQueryStatus):
    def __init__(
        self,
        client_adapter: _AsyncClientAdapter,
        request_builder: _RequestBuilder,
        status_resp: QueryHandleStatusResponse,
        stream_config: Optional[JsonStreamConfig] = None,
    ) -> None:
        super().__init__()
        self._client_adapter = client_adapter
        self._request_builder = request_builder
        self._status_resp = status_resp
        self._stream_config = stream_config

    def results_ready(self) -> bool:
        return self._status_resp.handle is not None

    def result_handle(self) -> AsyncQueryResultHandle:
        if self._status_resp.handle is None:
            raise AnalyticsError(message='Query is not ready. Handle is not available.')

        return AsyncQueryResultHandle(
            self._client_adapter,
            self._request_builder,
            self._status_resp.request_id,
            self._status_resp.handle,
            stream_config=self._stream_config,
        )

    def __repr__(self) -> str:
        return f'AsyncQueryStatus({self._status_resp.get_details()})'

    def __str__(self) -> str:
        return self.__repr__()
