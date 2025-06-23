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

from concurrent.futures import CancelledError
from functools import wraps
from typing import (Any,
                    Callable,
                    Optional)

if sys.version_info < (3, 10):
    from typing_extensions import TypeAlias
else:
    from typing import TypeAlias

from httpx import Response as HttpCoreResponse

# TODO: errors?
from couchbase_analytics.common.errors import AnalyticsError, InternalSDKError, TimeoutError
from couchbase_analytics.common.core import (JsonStreamConfig,
                                             ParsedResult,
                                             ParsedResultType)
from couchbase_analytics.common.core.json_stream import JsonStream
from couchbase_analytics.common.core.query import build_query_metadata
from couchbase_analytics.common.query import QueryMetadata
from couchbase_analytics.common.streaming import StreamingState
from couchbase_analytics.protocol.core._request_context import RequestContext, ThreadSafeBytesIterator

class RequestWrapper:
    """
        **INTERNAL**
    """

    @classmethod
    def handle_retries(cls) -> Callable[[SendRequestFunc], WrappedSendRequestFunc]:
        """
            **INTERNAL**
        """

        def decorator(fn: SendRequestFunc) -> WrappedSendRequestFunc:
            @wraps(fn)
            def wrapped_fn(self: HttpStreamingResponse) -> None:
                try:
                    fn(self)
                except AnalyticsError:
                    # if an AnalyticsError is raised, we have already shut down the request context
                    raise
                except RuntimeError as ex:
                    self._request_context.shutdown(ex)
                    raise ex
                except BaseException as ex:
                    self._request_context.shutdown(ex)
                    if self._request_context.request_error is not None:
                        raise self._request_context.request_error from None
                    if self._request_context.timed_out:
                        raise TimeoutError(message='Request timeout.') from None
                    if self._request_context.cancelled:
                        raise CancelledError('Request was cancelled.') from None
                    raise InternalSDKError(ex) from None
                finally:
                    if not StreamingState.is_okay(self._request_context.request_state):
                        self.close()

            return wrapped_fn
        return decorator

class HttpStreamingResponse:
    def __init__(self,
                 request_context: RequestContext,
                 lazy_execute: Optional[bool] = None,
                 stream_config: Optional[JsonStreamConfig]=None) -> None:
        self._request_context = request_context
        if lazy_execute is not None:
            self._lazy_execute = lazy_execute
        else:
            self._lazy_execute = False
        self._metadata: Optional[QueryMetadata] = None
        self._core_response: HttpCoreResponse
        self._stream_config = stream_config or JsonStreamConfig()
        self._json_stream: JsonStream

    @property
    def lazy_execute(self) -> bool:
        """
        **INTERNAL**
        """
        return self._lazy_execute

    def _finish_processing_stream(self) -> None:
        if not self._request_context.has_stage_completed:
            self._request_context.wait_for_stage_completed()
        
        if self._request_context.cancelled:
            return

        while not self._json_stream.token_stream_exhausted:
            self._json_stream.continue_parsing()

    def _handle_iteration_abort(self) -> None:
        self.close()
        if self._request_context.cancelled:
            print('Request was cancelled, closing stream.')
            raise StopIteration
        elif self._request_context.timed_out:
            print('Request timed out, closing stream.')
            raise TimeoutError(message='Request timeout.')
        else:
            raise StopIteration

    def _maybe_continue_to_process_stream(self) -> None:
        if not self._request_context.has_stage_completed:
            return
        
        if self._json_stream.token_stream_exhausted:
            return
        
        if self._request_context.cancelled:
            return

        # NOTE:  start_next_stage injects the request context into args
        self._request_context.start_next_stage(self._json_stream.continue_parsing, reset_previous_stage=True)

    def _process_response(self, raw_response: Optional[ParsedResult]=None) -> None:
        if raw_response is None:
            raw_response = self._json_stream.get_result(self._stream_config.queue_timeout)
            if raw_response is None:
                raise AnalyticsError(message='Received unexpected empty result from JsonStream.')
                
        if raw_response.value is None:
            raise AnalyticsError(message='Received unexpected empty result from JsonStream.')

        json_response = json.loads(raw_response.value)
        if 'errors' in json_response:
            self._request_context.process_error(json_response['errors'])
        self.set_metadata(json_data=json_response)
        # we have all the data, close the core response/stream
        self.close()

    def _start(self) -> None:
        """
        **INTERNAL**
        """
        if hasattr(self, '_json_stream'):
            # TODO: logging; I don't think this is an error...
            return
        
        # TODO: need to confirm if the httpx Response iterator is thread-safe
        self._json_stream = JsonStream(self._core_response.iter_bytes(), stream_config=self._stream_config)
        # NOTE:  start_next_stage injects the request context into args
        self._request_context.start_next_stage(self._json_stream.start_parsing, create_notification=True)
    
    def close(self) -> None:
        """
        **INTERNAL**
        """
        if hasattr(self,'_core_response'):
            self._core_response.close()
            del self._core_response
    
    def cancel(self) -> None:
        """
        **INTERNAL**
        """
        self._request_context.cancel_request()
        self.close()

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

    def get_next_row(self) -> Any:
        """
            **INTERNAL**
        """
        if not (hasattr(self, '_core_response')
                and self._core_response is not None
                and self._request_context.okay_to_iterate):
            self._handle_iteration_abort()
        
        self._maybe_continue_to_process_stream()
        check_state = False
        while True:
            if check_state and not self._request_context.okay_to_iterate:
                self._handle_iteration_abort()

            raw_response = self._json_stream.get_result(self._stream_config.queue_timeout)
            if raw_response is None:
                check_state = True
                continue
            if raw_response.result_type == ParsedResultType.ROW:
                if raw_response.value is None:
                    raise AnalyticsError(message='Unexpected empty row response while streaming.')
                return self._request_context.deserializer.deserialize(raw_response.value)
            elif raw_response.result_type in [ParsedResultType.ERROR, ParsedResultType.UNKNOWN]:
                self._process_response(raw_response=raw_response)
            elif raw_response.result_type == ParsedResultType.END:
                self.set_metadata(raw_metadata=raw_response.value)
                self.close()
                raise StopIteration
                
    @RequestWrapper.handle_retries()
    def send_request(self) -> None:
        if not self._request_context.okay_to_stream:
            raise RuntimeError('Query has been canceled or previously executed.')

        self._request_context.initialize()
        # TODO: do we need to use the tracing?
        self._core_response = self._request_context.send_request()
        if self._request_context.cancelled:
            raise CancelledError('Request was cancelled.')
        self._start()
        # block until we either know we have rows or errors
        result_type = self._request_context.wait_for_stage_notification()
        if result_type == ParsedResultType.ROW:
            # we move to iterating rows
            self._request_context.set_state_to_streaming()
        else:
            self._finish_processing_stream()
            self._process_response()

SendRequestFunc: TypeAlias = Callable[[HttpStreamingResponse], None]
# Although, SendRequestFunc is the same type as WrappedSendRequestFunc, keep separate for clarity and indicate
# WrappedSendRequestFunc is a decorator
WrappedSendRequestFunc: TypeAlias = Callable[[HttpStreamingResponse], None]