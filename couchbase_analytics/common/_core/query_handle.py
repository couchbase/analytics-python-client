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

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Awaitable, List, Mapping, Optional, TypedDict, Union

from couchbase_analytics.common._core.result import QueryResult


class QueryHandle(ABC):
    @abstractmethod
    def cancel(self) -> Union[Awaitable[None], None]:
        """
        Cancel the query associated with the QueryHandle.
        """
        raise NotImplementedError

    @abstractmethod
    def fetch_status(self) -> Union[Awaitable[QueryStatus], QueryStatus]:
        raise NotImplementedError


class QueryResultHandle(ABC):
    """Abstract base class for query result handle."""

    @abstractmethod
    def fetch_results(self) -> Union[Awaitable[QueryResult], QueryResult]:
        """
        Get all the results.
        """
        raise NotImplementedError

    @abstractmethod
    def discard_results(self) -> Union[Awaitable[None], None]:
        """
        Discard the query results associated with the QueryResultHandle.
        """
        raise NotImplementedError


class QueryStatus(ABC):
    @abstractmethod
    def results_ready(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def result_handle(self) -> QueryResultHandle:
        raise NotImplementedError


class ResultPartition(TypedDict):
    handle: str
    result_count: Optional[int]


@dataclass
class QueryHandleStatusResponse:
    """**INTERNAL**"""

    request_id: str
    status: str
    handle: Optional[str] = None
    result_count: Optional[int] = None
    partitions: Optional[List[ResultPartition]] = None
    result_set_ordered: Optional[bool] = None
    metrics: Optional[Mapping[str, Union[str, int]]] = None
    created_at: Optional[str] = None

    def get_details(self) -> Mapping[str, Any]:
        """**INTERNAL**"""
        return {
            'request_id': self.request_id,
            'status': self.status,
            'handle': self.handle,
            'metrics': self.metrics,
        }

    @classmethod
    def from_server(cls, request_id: str, raw_json: Any) -> QueryHandleStatusResponse:
        raw_partitions = raw_json.get('partitions', [])
        partitions: list[ResultPartition] = []
        for partition in raw_partitions:
            partitions.append(
                {'handle': partition.get('handle', None), 'result_count': partition.get('resultCount', None)}
            )
        return cls(
            request_id,
            raw_json.get('status', None),
            raw_json.get('handle', None),
            result_count=raw_json.get('resultCount', None),
            partitions=partitions,
            result_set_ordered=raw_json.get('resultSetOrdered', None),
            metrics=raw_json.get('metrics', None),
            created_at=raw_json.get('createdAt', None),
        )
