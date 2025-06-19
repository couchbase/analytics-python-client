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
from typing import Optional, NamedTuple

class JsonParsingError(Exception):
    def __init__(self, cause: Optional[Exception]=None) -> None:
        super().__init__(cause)
        self._cause = cause

    @property
    def cause(self) -> Optional[Exception]:
        return self._cause

    def __repr__(self) -> str:
        return f'JsonParsingError(cause={self._cause})'
        
    def __str__(self) -> str:
        return self.__repr__()
    

# buffer size in httpcore is 2 ** 16 (65kiB) which matches the default buffer size in ijson
# passing in a chunk_size is only applying an abstraction over the httpcore stream
DEFAULT_HTTP_STREAM_BUFFER_SIZE = 2**16

@dataclass
class JsonStreamConfig:
    http_stream_buffer_size: int = DEFAULT_HTTP_STREAM_BUFFER_SIZE
    buffer_entire_result: bool = False
    buffered_row_max: int = 100
    buffered_row_threshold_percent: float = 0.75
    queue_timeout: float = 0.25


class ParsedResultType(IntEnum):
    """
    **INTERNAL**
    """
    ROW = 0
    ERROR = 1
    END = 2
    UNKNOWN = 3

class ParsedResult(NamedTuple):
    """
    **INTERNAL**
    """
    value: Optional[bytes]
    result_type: ParsedResultType