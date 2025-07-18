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

import socket
from functools import wraps
from typing import Any, Callable, Coroutine, Optional

from couchbase_analytics.common.errors import AnalyticsError
from couchbase_analytics.common.logging import LogLevel
from couchbase_analytics.protocol.errors import WrappedError


class ErrorMapper:
    @staticmethod
    def handle_socket_error_async(
        fn: Callable[[str, int, Optional[Callable[..., None]]], Coroutine[Any, Any, str]],
    ) -> Callable[[str, int, Optional[Callable[..., None]]], Coroutine[Any, Any, str]]:
        @wraps(fn)
        async def wrapped_fn(host: str, port: int, logger_handler: Optional[Callable[..., None]] = None) -> str:
            try:
                return await fn(host, port, logger_handler)
            except socket.gaierror as ex:
                if logger_handler:
                    logger_handler(f'getaddrinfo() failed for {host}:{port} with error: {ex}', LogLevel.ERROR)
                msg = 'Connection error occurred while sending request.'
                raise WrappedError(AnalyticsError(cause=ex, message=msg), retriable=True) from None

        return wrapped_fn
