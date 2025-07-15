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

from collections.abc import AsyncIterator as PyAsyncIterator
from collections.abc import Iterator
from enum import IntEnum
from typing import TYPE_CHECKING, Any, List, NamedTuple

from couchbase_analytics.common.errors import AnalyticsError, InternalSDKError

if TYPE_CHECKING:
    from acouchbase_analytics.protocol.streaming import AsyncHttpStreamingResponse
    from couchbase_analytics.protocol.streaming import HttpStreamingResponse


class BlockingIterator(Iterator[Any]):
    """
    **INTERNAL
    """

    def __init__(self, http_response: HttpStreamingResponse) -> None:
        self._http_response = http_response

    def get_all_rows(self) -> List[Any]:
        """
        **INTERNAL
        """
        return list(self)

    def __iter__(self) -> BlockingIterator:
        """
        **INTERNAL
        """
        if self._http_response.lazy_execute is True:
            self._http_response.send_request()

        return self

    def __next__(self) -> Any:
        """
        **INTERNAL
        """
        try:
            return self._http_response.get_next_row()
        except StopIteration:
            raise
        except AnalyticsError as err:
            raise err
        except Exception as ex:
            raise InternalSDKError(cause=ex, message='Error attempting to obtain next row.') from None


class AsyncIterator(PyAsyncIterator[Any]):
    """
    **INTERNAL
    """

    def __init__(self, http_response: AsyncHttpStreamingResponse) -> None:
        self._http_response = http_response

    async def get_all_rows(self) -> List[Any]:
        """
        **INTERNAL
        """
        return [r async for r in self]

    def __aiter__(self) -> AsyncIterator:
        """
        **INTERNAL
        """
        return self

    async def __anext__(self) -> Any:
        """
        **INTERNAL
        """
        try:
            return await self._http_response.get_next_row()
        except StopAsyncIteration:
            raise
        except AnalyticsError as err:
            raise err
        except Exception as ex:
            raise InternalSDKError(cause=ex, message='Error attempting to obtain next row.') from None


class HttpResponseType(IntEnum):
    """
    **INTERNAL**
    """

    ROW = 0
    ERROR = 1
    END = 2


class ParsedResult(NamedTuple):
    """
    **INTERNAL**
    """

    result: str
    result_type: HttpResponseType
