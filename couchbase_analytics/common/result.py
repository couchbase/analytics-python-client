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

from typing import (TYPE_CHECKING,
                    Any,
                    List,
                    Optional)

from couchbase_analytics.common._core.result import QueryResult as QueryResult
from couchbase_analytics.common.query import QueryMetadata
from couchbase_analytics.common.streaming import AsyncIterator, BlockingIterator

if TYPE_CHECKING:
    from acouchbase_analytics.protocol.streaming import AsyncHttpStreamingResponse
    from couchbase_analytics.protocol.streaming import HttpStreamingResponse


class BlockingQueryResult(QueryResult):
    def __init__(self, http_response: HttpStreamingResponse, lazy_execute: Optional[bool] = None) -> None:
        self._http_response = http_response
        self._lazy_execute = lazy_execute

    def cancel(self) -> None:
        """Cancel streaming the query results.

        **VOLATILE** This API is subject to change at any time.
        """
        self._http_response.cancel()

    def get_all_rows(self) -> List[Any]:
        """Convenience method to load all query results into memory.

        Returns:
            A list of query results.

        Example:
            Read all rows from simple query::

                q_str = 'SELECT * FROM `travel-sample`.inventory WHERE country LIKE 'United%' LIMIT 2;'
                q_rows = cluster.execute_query(q_str).all_rows()

        """
        return BlockingIterator(self._http_response).get_all_rows()

    def metadata(self) -> QueryMetadata:
        """Get the query metadata.

        Returns:
            A QueryMetadata instance (if available).

        Raises:
            RuntimeError: When the metadata is not available. Metadata is only available once all rows have been iterated.
        """  # noqa: E501
        return self._http_response.get_metadata()

    def rows(self) -> BlockingIterator:
        """Retrieve the rows which have been returned by the query.

        Returns:
            A blocking iterator for iterating over query results.
        """
        return BlockingIterator(self._http_response)

    def __iter__(self) -> BlockingIterator:
        return iter(BlockingIterator(self._http_response))

    def __repr__(self) -> str:
        return "BlockingQueryResult()"


class AsyncQueryResult(QueryResult):
    def __init__(self, http_response: AsyncHttpStreamingResponse) -> None:
        self._http_response = http_response

    def cancel(self) -> None:
        """Cancel streaming the query results.

        **VOLATILE** This API is subject to change at any time.
        """
        self._http_response.cancel()

    async def cancel_async(self) -> None:
        """Cancel streaming the query results.

        **VOLATILE** This API is subject to change at any time.
        """
        await self._http_response.cancel_async()

    async def get_all_rows(self) -> List[Any]:
        """Convenience method to load all query results into memory.

        Returns:
            A list of query results.

        Example:

            Read all rows from simple query::

                q_str = 'SELECT * FROM `travel-sample`.inventory WHERE country LIKE 'United%' LIMIT 2;'
                q_rows = await cluster.execute_query(q_str).all_rows()

        """
        return await AsyncIterator(self._http_response).get_all_rows()

    def metadata(self) -> QueryMetadata:
        """The meta-data which has been returned by the query.

        Returns:
            A QueryMetadata instance (if available).

        Raises:
            RuntimeError: When the metadata is not available. Metadata is only available once all rows have been iterated.
        """  # noqa: E501
        return self._http_response.get_metadata()

    def rows(self) -> AsyncIterator:
        """Retrieve the rows which have been returned by the query.

        .. note::
            Bee sure to use ``async for`` when looping over rows.

        Returns:
            An async iterator for iterating over query results.
        """
        return AsyncIterator(self._http_response)

    async def shutdown(self) -> None:
        """Shutdown the streaming connection."""
        await self._http_response.shutdown()

    def __aiter__(self) -> AsyncIterator:
        return AsyncIterator(self._http_response).__aiter__()

    def __repr__(self) -> str:
        return "AsyncQueryResult()"
