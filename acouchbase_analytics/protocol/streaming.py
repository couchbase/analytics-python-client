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
import sys

from asyncio import CancelledError
from functools import wraps
from typing import (Any,
                    Callable,
                    Coroutine,
                    Optional)

if sys.version_info < (3, 10):
    from typing_extensions import TypeAlias
else:
    from typing import TypeAlias

from httpx import Response as HttpCoreResponse

# TODO: errors?
from couchbase_analytics.common.errors import (AnalyticsError,
                                               InternalSDKError,
                                               TimeoutError)
from acouchbase_analytics.protocol.core._request_context import AsyncRequestContext
from couchbase_analytics.common.core import (JsonStreamConfig,
                                             ParsedResult,
                                             ParsedResultType)
from couchbase_analytics.common.core.async_json_stream import AsyncJsonStream
from couchbase_analytics.common.core.query import build_query_metadata
from couchbase_analytics.common.query import QueryMetadata
from couchbase_analytics.common.streaming import StreamingState




class RequestWrapper:
    """
        **INTERNAL**
    """

    @classmethod
    def handle_retries(cls) -> Callable[[SendRequestFunc], WrappedSendRequestFunc]:  # noqa: C901
        """
            **INTERNAL**
        """

        def decorator(fn: SendRequestFunc) -> WrappedSendRequestFunc:  # noqa: C901
            @wraps(fn)
            async def wrapped_fn(self: AsyncHttpStreamingResponse) -> None:
                try:
                    await fn(self)
                except AnalyticsError:
                    # if an AnalyticsError is raised, we have already shut down the request context
                    raise
                except RuntimeError as ex:
                    await self._request_context.shutdown(type(ex), ex, ex.__traceback__)
                    raise ex
                except BaseException as ex:
                    await self._request_context.shutdown(type(ex), ex, ex.__traceback__)
                    if self._request_context.timed_out:
                        raise TimeoutError(cause=self._request_context.request_error,
                                           message='Request timed out.') from None
                    if self._request_context.cancelled:
                        raise CancelledError('Request was cancelled.') from None
                    if self._request_context.request_error is not None:
                        raise self._request_context.request_error from None
                    raise InternalSDKError(ex) from None
                finally:
                    if not StreamingState.is_okay(self._request_context.request_state):
                        await self.close()
                    

            return wrapped_fn
        return decorator

class AsyncHttpStreamingResponse:
    def __init__(self,
                 request_context: AsyncRequestContext,
                 stream_config: Optional[JsonStreamConfig]=None) -> None:
        self._metadata: Optional[QueryMetadata] = None
        self._core_response: HttpCoreResponse
        self._stream_config = stream_config or JsonStreamConfig()
        self._json_stream: AsyncJsonStream
        # Goal is to treat the AsyncHttpStreamingResponse as a "task group"
        self._request_context = request_context

    async def _finish_processing_stream(self) -> None:
        if not self._request_context.has_stage_completed:
            await self._request_context.wait_for_stage_to_complete()
        
        while not self._json_stream.token_stream_exhausted:
            self._request_context.start_next_stage(self._json_stream.continue_parsing, reset_previous_stage=True)
            await self._request_context.wait_for_stage_to_complete()

    async def _handle_iteration_abort(self) -> None:
        await self.close()
        if self._request_context.cancelled:
            raise StopAsyncIteration
        elif self._request_context.timed_out:
            raise TimeoutError(message='Request timeout.')
        else:
            raise StopAsyncIteration

    def _maybe_continue_to_process_stream(self) -> None:
        if not self._request_context.has_stage_completed:
            return

        if self._json_stream.token_stream_exhausted:
            return
        
        self._request_context.start_next_stage(self._json_stream.continue_parsing, reset_previous_stage=True)

    async def _process_response(self, raw_response: Optional[ParsedResult]=None) -> None:
        if raw_response is None:
            raw_response = await self._json_stream.get_result()
            if raw_response is None:
                raise AnalyticsError(message='Received unexpected empty result from JsonStream.')
                
        if raw_response.value is None:
            raise AnalyticsError(message='Received unexpected empty result from JsonStream.')

        # we have all the data, close the core response/stream
        await self.close()

        json_response = json.loads(raw_response.value)
        if 'errors' in json_response:
            await self._request_context.process_error(json_response['errors'])
        self.set_metadata(json_data=json_response)

    def _start(self) -> None:
        """
        **INTERNAL**
        """
        if hasattr(self, '_json_stream'):
            # TODO: logging; I don't think this is an error...
            return
        
        self._json_stream = AsyncJsonStream(self._core_response.aiter_bytes(), stream_config=self._stream_config)
        self._request_context.start_next_stage(self._json_stream.start_parsing)

    async def _close_in_background(self) -> None:
        await self.close()

    async def close(self) -> None:
        """
        **INTERNAL**
        """
        if hasattr(self, '_core_response'):
            await self._core_response.aclose()
            del self._core_response
    
    def cancel(self) -> None:
        """
        **INTERNAL**
        """
        self._request_context.cancel_request(self._close_in_background)

    async def cancel_async(self) -> None:
        """
        **INTERNAL**
        """
        await self.close()
        self._request_context.cancel_request()
        await self._request_context.shutdown()

    def get_metadata(self) -> QueryMetadata:
        if self._metadata is None:
            if self._request_context.cancelled:
                raise CancelledError('Request was cancelled.')
            elif self._request_context.timed_out:
                raise TimeoutError(message='Request timeout.')
            raise RuntimeError('Query metadata is only available after all rows have been iterated.')
        return self._metadata

    def set_metadata(self,
                     json_data: Optional[Any]=None,
                     raw_metadata: Optional[bytes]=None) -> None:
        try:
            self._metadata = QueryMetadata(build_query_metadata(json_data=json_data, raw_metadata=raw_metadata))
        except AnalyticsError as err:
            raise err
        except Exception as ex:
            raise InternalSDKError(str(ex))

    async def get_next_row(self) -> Any:
        """
            **INTERNAL**
        """
        if not (hasattr(self, '_core_response')
                and self._core_response is not None
                and self._request_context.okay_to_iterate):
            await self._handle_iteration_abort()
        
        self._maybe_continue_to_process_stream()
        raw_response = await self._json_stream.get_result()
        if raw_response.result_type == ParsedResultType.ROW:
            if raw_response.value is None:
                raise AnalyticsError(message='Unexpected empty row response while streaming.')
            return self._request_context.deserializer.deserialize(raw_response.value)
        elif raw_response.result_type in [ParsedResultType.ERROR, ParsedResultType.UNKNOWN]:
            await self._process_response(raw_response=raw_response)
        elif raw_response.result_type == ParsedResultType.END:
            self.set_metadata(raw_metadata=raw_response.value)
            await self.close()
            raise StopAsyncIteration
        else:
            await self._process_response(raw_response=raw_response)
                
    @RequestWrapper.handle_retries()
    async def send_request(self) -> None:
        if not self._request_context.okay_to_stream:
            raise RuntimeError('Query has been canceled or previously executed.')

        # start cancel scope
        await self._request_context.initialize()
        self._core_response = await self._request_context.send_request(enable_trace_handling=True)
        self._start()
        # block until we either know we have rows or we have an error
        await self._json_stream.has_results_or_errors.wait()
        if self._json_stream.has_results_or_errors_type == ParsedResultType.ROW:
            # we move to iterating rows
            self._request_context.set_state_to_streaming()
        else:
            await self._finish_processing_stream()
            await self._process_response()

    async def shutdown(self) -> None:
        await self.close()
        await self._request_context.shutdown()

SendRequestFunc: TypeAlias = Callable[[AsyncHttpStreamingResponse], Coroutine[Any, Any, None]]
# Although, SendRequestFunc is the same type as WrappedSendRequestFunc, keep separate for clarity and indicate
# WrappedSendRequestFunc is a decorator
WrappedSendRequestFunc: TypeAlias = Callable[[AsyncHttpStreamingResponse], Coroutine[Any, Any, None]]