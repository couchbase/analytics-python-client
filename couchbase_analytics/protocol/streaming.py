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

from concurrent.futures import CancelledError
from typing import Any, Optional

from httpx import Response as HttpCoreResponse

from couchbase_analytics.common._core import ParsedResult, ParsedResultType
from couchbase_analytics.common._core.query import build_query_metadata
from couchbase_analytics.common.errors import (AnalyticsError,
                                               InternalSDKError,
                                               TimeoutError)
from couchbase_analytics.common.query import QueryMetadata
from couchbase_analytics.protocol._core.request_context import RequestContext
from couchbase_analytics.protocol._core.retries import RetryHandler


class HttpStreamingResponse:
    def __init__(self,
                 request_context: RequestContext,
                 lazy_execute: Optional[bool] = None) -> None:
        self._request_context = request_context
        if lazy_execute is not None:
            self._lazy_execute = lazy_execute
        else:
            self._lazy_execute = False
        self._metadata: Optional[QueryMetadata] = None
        self._core_response: HttpCoreResponse

    @property
    def lazy_execute(self) -> bool:
        """
        **INTERNAL**
        """
        return self._lazy_execute

    def _handle_iteration_abort(self) -> None:
        self.close()
        if self._request_context.cancelled:
            self._request_context.shutdown()
            raise StopIteration
        elif self._request_context.timed_out:
            err = TimeoutError(message='Unable to complete iteration. Request timed out.',
                               context=str(self._request_context.error_context))
            self._request_context.shutdown(err)
            raise err
        else:
            self._request_context.shutdown()
            raise StopIteration

    def _process_response(self,
                          raw_response: Optional[ParsedResult]=None,
                          handle_context_shutdown: Optional[bool]=False) -> None:
        json_response = self._request_context.process_response(self.close,
                                                               raw_response=raw_response,
                                                               handle_context_shutdown=handle_context_shutdown)
        self.set_metadata(json_data=json_response)

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
        self.close()
        self._request_context.cancel_request()
        self._request_context.shutdown()


    def get_metadata(self) -> QueryMetadata:
        if self._metadata is None:
            raise RuntimeError('Query metadata is only available after all rows have been iterated.')
        return self._metadata

    def set_metadata(self,
                     json_data: Optional[Any]=None,
                     raw_metadata: Optional[bytes]=None) -> None:
        try:
            self._metadata = QueryMetadata(build_query_metadata(json_data=json_data, raw_metadata=raw_metadata))
            self._request_context.shutdown()
        except (AnalyticsError, ValueError) as err:
            self._request_context.shutdown(err)
            raise err
        except Exception as ex:
            internal_err = InternalSDKError(cause=ex,
                                            message=str(ex),
                                            context=str(self._request_context.error_context))
            self._request_context.shutdown(internal_err)
        finally:
            self.close()

    def get_next_row(self) -> Any:
        """
            **INTERNAL**
        """
        if not (hasattr(self, '_core_response')
                and self._core_response is not None
                and self._request_context.okay_to_iterate):
            self._handle_iteration_abort()

        self._request_context.maybe_continue_to_process_stream()
        check_state = False
        while True:
            if check_state and not self._request_context.okay_to_iterate:
                self._handle_iteration_abort()

            raw_response = self._request_context.get_result_from_stream()
            if raw_response is None:
                check_state = True
                continue
            if raw_response.result_type == ParsedResultType.ROW:
                if raw_response.value is None:
                    err = AnalyticsError(message='Unexpected empty row response while streaming.',
                                         context=str(self._request_context.error_context))
                    self._request_context.shutdown(err)
                    self.close()
                    raise err
                return self._request_context.deserialize_result(raw_response.value)
            elif raw_response.result_type in [ParsedResultType.ERROR, ParsedResultType.UNKNOWN]:
                self._process_response(raw_response=raw_response, handle_context_shutdown=True)
            elif raw_response.result_type == ParsedResultType.END:
                self.set_metadata(raw_metadata=raw_response.value)
                raise StopIteration

    @RetryHandler.with_retries
    def send_request(self) -> None:
        if not self._request_context.okay_to_stream:
            raise RuntimeError('Query has been canceled or previously executed.')

        self._request_context.initialize()
        self._core_response = self._request_context.send_request()
        if self._request_context.cancelled:
            raise CancelledError('Request was cancelled.')
        self._request_context.start_stream(self._core_response)
        # block until we either know we have rows or errors
        self._request_context.wait_for_stage_notification()
        if not self._request_context.okay_to_iterate:
            self._request_context.finish_processing_stream()
            self._process_response()
