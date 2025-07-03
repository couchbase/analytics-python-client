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

from typing import AsyncIterator, Optional

import ijson
from anyio import EndOfStream, Event, create_memory_object_stream

from acouchbase_analytics.protocol._core.async_json_token_parser import AsyncJsonTokenParser
from couchbase_analytics.common._core.json_parsing import (
    JsonParsingError,
    JsonStreamConfig,
    ParsedResult,
    ParsedResultType,
)
from couchbase_analytics.common.errors import AnalyticsError


class AsyncJsonStream:
    def __init__(self,
                 http_stream_iter: AsyncIterator[bytes],
                 *,
                 stream_config: Optional[JsonStreamConfig]=None,
                 ) -> None:
        # HTTP stream handling
        if stream_config is None:
            stream_config = JsonStreamConfig()
        self._http_stream_iter = http_stream_iter
        self._http_stream_buffer_size = stream_config.http_stream_buffer_size
        self._http_response_buffer = bytearray()
        self._http_stream_exhausted = False

        # results handling
        self._send_stream, self._receive_stream = create_memory_object_stream[ParsedResult](max_buffer_size=stream_config.buffered_row_max) # noqa: E501
        self._json_stream_parser = None
        self._buffer_entire_result = stream_config.buffer_entire_result
        handler = None if self._buffer_entire_result is True else self._handle_json_result
        self._json_token_parser = AsyncJsonTokenParser(handler)
        self._token_stream_exhausted = False
        self._has_results_or_errors_evt = Event()
        self._results_or_errors_type = ParsedResultType.UNKNOWN

    @property
    def has_results_or_errors(self) -> Event:
        """
        **INTERNAL**
        """
        return self._has_results_or_errors_evt

    @property
    def results_or_errors_type(self) -> ParsedResultType:
        """
        **INTERNAL**
        """
        return self._results_or_errors_type

    @property
    def token_stream_exhausted(self) -> bool:
        """
        **INTERNAL**
        """
        return self._token_stream_exhausted

    def _continue_processing(self) -> bool:
        """
        **INTERNAL**
        """
        if self._token_stream_exhausted:
            return False
        if self._buffer_entire_result:
            return True

        stats = self._receive_stream.statistics()
        if stats.current_buffer_used >= stats.max_buffer_size:
            return False
        return True

    async def _send_to_stream(self, result: ParsedResult, close: Optional[bool]=False) -> None:
        """
        **INTERNAL**
        """
        await self._send_stream.send(result)
        if close is True:
            await self._send_stream.aclose()

    async def _handle_json_result(self, row: bytes) -> None:
        """
        **INTERNAL**
        """
        if not self._has_results_or_errors_evt.is_set():
            self._handle_notification(ParsedResultType.ROW)
        await self._send_to_stream(ParsedResult(row, ParsedResultType.ROW))

    def _handle_notification(self, result_type: Optional[ParsedResultType]=None) -> None:
        if self._has_results_or_errors_evt.is_set():
            return

        if result_type is None:
            self._results_or_errors_type = ParsedResultType.END
            self._has_results_or_errors_evt.set()
            return

        self._results_or_errors_type = result_type
        self._has_results_or_errors_evt.set()

    async def _process_token_stream(self) -> None:
        """
        **INTERNAL**
        """
        if self._json_stream_parser is None:
                self._json_stream_parser = ijson.parse_async(self, buf_size=self._http_stream_buffer_size)

        while self._continue_processing():
            try:
                _, event, value = await self._json_stream_parser.__anext__()  # type: ignore[attr-defined]
                # this is a hack b/c the ijson.parse_async iterator does not yield to the event loop
                # TODO:  create PYCO to either build custom JSON parsing, or dig into ijson root cause
                await self._json_token_parser.parse_token(event, value)
            except StopAsyncIteration:
                self._token_stream_exhausted = True
            except ijson.common.IncompleteJSONError as ex:
                raise JsonParsingError(cause=ex) from None


        if self._token_stream_exhausted:
            result_type = ParsedResultType.ERROR if self._json_token_parser.has_errors else ParsedResultType.END
            await self._send_to_stream(ParsedResult(self._json_token_parser.get_result(), result_type), close=True)
            self._handle_notification(result_type)

    async def read(self, size: Optional[int]=-1) -> bytes:
        """
        **INTERNAL**
        """
        if size is None or size == 0 or self._http_stream_exhausted:
            return b''

        while not self._http_stream_exhausted:
            if size >= 0 and len(self._http_response_buffer) > size:
                break
            try:
                chunk = await self._http_stream_iter.__anext__()
                self._http_response_buffer += chunk
            except StopAsyncIteration:
                self._http_stream_exhausted = True
                break

        if size == -1:
            data = bytes(self._http_response_buffer[:])
            del self._http_response_buffer[:]
        else:
            end = min(size, len(self._http_response_buffer))
            data = bytes(self._http_response_buffer[:end])
            del self._http_response_buffer[:end]
        return data

    async def get_result(self) -> ParsedResult:
        try:
            return await self._receive_stream.receive()
        except EndOfStream as ex:
            raise AnalyticsError(ex, 'AsyncJsonStream has been closed.') from None

    async def start_parsing(self) -> None:
        if self._json_stream_parser is not None:
            # TODO: logging; I don't think this is an error...
            return
        await self._process_token_stream()

    async def continue_parsing(self) -> None:
        # TODO: error is _json_stream_parser is None?
        await self._process_token_stream()
