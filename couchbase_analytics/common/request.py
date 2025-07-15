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

from dataclasses import dataclass
from enum import IntEnum
from typing import Dict, Optional


class RequestState(IntEnum):
    """
    **INTERNAL
    """

    NotStarted = 0
    ResetAndNotStarted = 1
    Started = 2
    Cancelled = 3
    Completed = 4
    StreamingResults = 5
    Error = 6
    Timeout = 7
    AsyncCancelledPriorToTimeout = 8
    SyncCancelledPriorToTimeout = 9

    @staticmethod
    def okay_to_stream(state: RequestState) -> bool:
        """
        **INTERNAL
        """
        return state in [RequestState.NotStarted, RequestState.ResetAndNotStarted]

    @staticmethod
    def okay_to_iterate(state: RequestState) -> bool:
        """
        **INTERNAL
        """
        return state == RequestState.StreamingResults

    @staticmethod
    def is_okay(state: RequestState) -> bool:
        """
        **INTERNAL
        """
        return state not in [RequestState.Cancelled, RequestState.Error, RequestState.Timeout]

    @staticmethod
    def is_timeout_or_cancelled(state: RequestState) -> bool:
        """
        **INTERNAL
        """
        return state in [
            RequestState.Cancelled,
            RequestState.Timeout,
            RequestState.AsyncCancelledPriorToTimeout,
            RequestState.SyncCancelledPriorToTimeout,
        ]


@dataclass
class RequestURL:
    scheme: str
    host: str
    port: int
    ip: Optional[str] = None
    path: Optional[str] = None

    def get_formatted_url(self) -> str:
        """Get the formatted URL for this request."""
        if self.path is None:
            host = self.ip if self.ip else self.host
            return f'{self.scheme}://{host}:{self.port}'
        return f'{self.scheme}://{host}:{self.port}{self.path}'

    def __repr__(self) -> str:
        details: Dict[str, str] = {
            'scheme': self.scheme,
            'host': self.host,
            'port': str(self.port),
            'path': self.path if self.path else '',
        }
        return f'{type(self).__name__}({details})'

    def __str__(self) -> str:
        return self.__repr__()
