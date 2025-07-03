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
import sys
from functools import wraps
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Set, Union

if sys.version_info < (3, 10):
    from typing_extensions import TypeAlias
else:
    from typing import TypeAlias

from couchbase_analytics.common._core.error_context import ErrorContext
from couchbase_analytics.common.errors import (
    AnalyticsError,
    InternalSDKError,
    InvalidCredentialError,
    QueryError,
    TimeoutError,
)

AnalyticsClientError: TypeAlias = Union[AnalyticsError,
                                        InternalSDKError,
                                        QueryError,
                                        RuntimeError,
                                        ValueError]

class ServerQueryError(NamedTuple):
    """
    **INTERNAL**
    """
    code: int
    message: str
    retriable: bool = False

    def to_dict(self) -> Dict[str, Any]:
        output: Dict[str, Any] = {
            'code': self.code,
            'msg': self.message,
        }
        if self.retriable is not None:
            output['retriable'] = self.retriable
        return output

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ServerQueryError):
            return False
        return self.code == other.code and self.message == other.message

    def __repr__(self) -> str:
        return f'ServerQueryError(code={self.code}, message={self.message}, retriable={self.retriable})'

    @classmethod
    def from_json(cls, json_data: Dict[str, Any]) -> ServerQueryError:
        """
        **INTERNAL**
        """
        code = json_data.get('code', 0)
        message = json_data.get('msg', 'Unknown error')
        retriable = bool(json_data.get('retriable', False))
        return cls(code=code, message=message, retriable=retriable)

class WrappedError(Exception):
    def __init__(self,
                 cause: Union[BaseException, Exception],
                 retriable: bool = False) -> None:
        super().__init__()
        self._cause = cause
        self._retriable = retriable

    @property
    def retriable(self) -> bool:
        return self._retriable

    @retriable.setter
    def retriable(self, value: bool) -> None:
        self._retriable = value

    def maybe_set_cause_context(self, context: ErrorContext) -> None:
        if not isinstance(self._cause, (AnalyticsError, InvalidCredentialError, QueryError, TimeoutError)):
            return

        if hasattr(self._cause, '_context') and self._cause._context is None:
            self._cause._context = str(context)

    def unwrap(self) -> Union[BaseException, Exception]:
        """
        Unwraps the cause of the error, returning the original exception.
        """
        return self._cause

    def __repr__(self) -> str:
        return f'{type(self).__name__}(cause={self._cause!r}, retriable={self._retriable})'

    def __str__(self) -> str:
        return self.__repr__()

# https://github.com/python/cpython/blob/0f866cbfefd797b4dae25962457c5579bb90dde5/Modules/addrinfo.h#L58-L71
_NON_RETRYABLE_SOCKET_ERRORS: List[int] = [
    socket.EAI_ADDRFAMILY,
    socket.EAI_BADFLAGS,
    socket.EAI_FAIL,
    socket.EAI_FAMILY,
    socket.EAI_MEMORY,
    socket.EAI_NODATA,
    socket.EAI_NONAME,
    socket.EAI_SERVICE,
    socket.EAI_SOCKTYPE,
    socket.EAI_SYSTEM,
    socket.EAI_BADHINTS,
    socket.EAI_PROTOCOL,
    socket.EAI_MAX
]


class ErrorMapper:

    @staticmethod  # noqa: C901
    def build_error_from_json(json_data: List[Dict[str, Any]], context: ErrorContext) -> WrappedError:
        if context.status_code is None:
            return WrappedError(AnalyticsError(context=str(context), message='Unknown error occurred.'))
        if context.status_code == 401:
            return WrappedError(InvalidCredentialError(context=str(context), message='Invalid credentials provided.'))

        first_non_retriable_error: Optional[ServerQueryError] = None
        first_retriable_error: Optional[ServerQueryError] = None
        errs: List[ServerQueryError] = []
        for err_data in json_data:
            err = ServerQueryError.from_json(err_data)
            errs.append(err)
            retriable = bool(err_data.get('retriable', False)) or False
            if not retriable and first_non_retriable_error is None:
                first_non_retriable_error = err

            if retriable and first_retriable_error is None:
                first_retriable_error = err

        first_err = first_non_retriable_error or first_retriable_error
        context.set_errors([e.to_dict() for e in errs])
        if first_err is None:
            err_msg = 'Could not parse errors from server response (expected JSON array).'
            return WrappedError(AnalyticsError(context=str(context), message=err_msg))

        if first_err.code == 20000:
            return WrappedError(InvalidCredentialError(context=str(context)))
        if first_err.code == 21002:
            return WrappedError(TimeoutError(context=str(context), message='Received timeout error from server.'))

        retriable = first_non_retriable_error is None and first_retriable_error is not None
        return WrappedError(QueryError(code=first_err.code,
                                       server_message=first_err.message,
                                       context=str(context)),
                                       retriable=retriable)

    @staticmethod
    def handle_socket_error(fn: Callable[[str, int, Optional[Set[str]]], Optional[str]]
                            ) -> Callable[[str, int, Optional[Set[str]]], Optional[str]]:
        @wraps(fn)
        def wrapped_fn(host: str,
                    port: int,
                    previous_ips: Optional[Set[str]]=None) -> Optional[str]:
            try:
                return fn(host, port, previous_ips)
            except socket.gaierror as ex:
                # print(f'getaddrinfo failed for {host}:{port} with error: {ex}')
                msg='Connection error occurred while sending request.'
                raise WrappedError(AnalyticsError(cause=ex, message=msg),
                                   retriable=(ex.errno not in _NON_RETRYABLE_SOCKET_ERRORS)) from None

        return wrapped_fn
