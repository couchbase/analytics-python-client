#  Copyright 2016-2025. Couchbase, Inc.
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

from abc import abstractmethod
from typing import Any, Awaitable, Optional

from couchbase_analytics.common._core.query_handle import QueryHandle, QueryResultHandle, QueryStatus
from couchbase_analytics.common.options import FetchResultsOptions
from couchbase_analytics.common.result import AsyncQueryResult, BlockingQueryResult


class AsyncQueryHandle(QueryHandle):
    @abstractmethod
    def cancel(self, options: Optional[Any] = None, **kwargs: Any) -> Awaitable[None]:
        """
        Cancel the query associated with the QueryHandle.
        """
        raise NotImplementedError

    @abstractmethod
    def fetch_status(self, options: Optional[Any] = None, **kwargs: Any) -> Awaitable[AsyncQueryStatus]:
        raise NotImplementedError


class BlockingQueryHandle(QueryHandle):
    @abstractmethod
    def cancel(self, options: Optional[Any] = None, **kwargs: Any) -> None:
        """
        Cancel the query associated with the QueryHandle.
        """
        raise NotImplementedError

    @abstractmethod
    def fetch_status(self, options: Optional[Any] = None, **kwargs: Any) -> BlockingQueryStatus:
        raise NotImplementedError


class AsyncQueryResultHandle(QueryResultHandle):
    """Abstract base class for async query result handle."""

    @abstractmethod
    def fetch_results(
        self, options: Optional[FetchResultsOptions] = None, **kwargs: Any
    ) -> Awaitable[AsyncQueryResult]:
        """
        Get all the results.
        """
        raise NotImplementedError

    @abstractmethod
    def discard_results(self, options: Optional[Any] = None, **kwargs: Any) -> Awaitable[None]:
        """
        Discard the query results associated with the AsyncQueryResultHandle.
        """
        raise NotImplementedError


class BlockingQueryResultHandle(QueryResultHandle):
    """Abstract base class for query result handle."""

    @abstractmethod
    def fetch_results(self, options: Optional[FetchResultsOptions] = None, **kwargs: Any) -> BlockingQueryResult:
        """
        Get all the results.
        """
        raise NotImplementedError

    @abstractmethod
    def discard_results(self, options: Optional[Any] = None, **kwargs: Any) -> None:
        """
        Discard the query results associated with the BlockingQueryResultHandle.
        """
        raise NotImplementedError


class AsyncQueryStatus(QueryStatus):
    @abstractmethod
    def results_ready(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def result_handle(self) -> AsyncQueryResultHandle:
        raise NotImplementedError


class BlockingQueryStatus(QueryStatus):
    @abstractmethod
    def results_ready(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def result_handle(self) -> BlockingQueryResultHandle:
        raise NotImplementedError
