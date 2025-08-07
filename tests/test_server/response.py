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

import json
from dataclasses import dataclass, field
from random import choice
from typing import Any, Callable, Dict, Generator, List, Optional, Union
from uuid import uuid4

from tests.test_server import ErrorType, NonRetriableSpecificationType, ResultType, RetriableGroupType

US_CITIES = [
    'New York City',
    'Los Angeles',
    'Chicago',
    'Houston',
    'Phoenix',
    'Philadelphia',
    'San Antonio',
    'San Diego',
    'Dallas',
    'San Jose',
    'Austin',
    'Jacksonville',
    'Fort Worth',
    'Columbus',
    'Charlotte',
    'Indianapolis',
    'San Francisco',
    'Seattle',
    'Denver',
    'Washington, D.C.',
    'Boston',
    'El Paso',
    'Nashville',
    'Detroit',
    'Oklahoma City',
    'Portland',
    'Las Vegas',
    'Memphis',
    'Louisville',
    'Baltimore',
    'Milwaukee',
    'Albuquerque',
    'Tucson',
    'Fresno',
    'Sacramento',
    'Mesa',
    'Atlanta',
    'Kansas City',
    'Colorado Springs',
    'Raleigh',
    'Omaha',
    'Miami',
    'Long Beach',
    'Virginia Beach',
    'Oakland',
    'Minneapolis',
    'Tampa',
    'New Orleans',
    'Cleveland',
    'Orlando',
]

NAMES = [
    'Alice Smith',
    'Bob Johnson',
    'Catherine Davis',
    'David Miller',
    'Emily Wilson',
    'Frank Moore',
    'Grace Taylor',
    'Henry Anderson',
    'Ivy Thomas',
    'Jack Jackson',
    'Karen White',
    'Leo Harris',
    'Mia Martin',
    'Noah Garcia',
    'Olivia Rodriguez',
    'Peter Martinez',
    'Quinn Clark',
    'Rachel Lewis',
    'Sam Lee',
    'Tina Hall',
    'Uma Young',
    'Victor King',
    'Wendy Wright',
    'Xavier Lopez',
    'Yara Hill',
    'Zack Scott',
    'Amelia Green',
    'Ben Baker',
    'Chloe Adams',
    'Daniel Nelson',
    'Ella Carter',
    'Finn Mitchell',
    'Georgia Perez',
    'Harry Roberts',
    'Isla Turner',
    'James Phillips',
    'Kim Campbell',
    'Liam Parker',
    'Maya Evans',
    'Nathan Edwards',
    'Owen Collins',
    'Penelope Stewart',
    'Ryan Sanchez',
    'Sophia Morris',
    'Thomas Rogers',
    'Victoria Reed',
    'William Cook',
    'Zara Bell',
    'Ethan Murphy',
    'Lily Russell',
]


@dataclass
class ServerResponseMetrics:
    elapsed_time: float
    execution_time: float
    compile_time: float
    queue_wait_time: float
    result_count: int
    result_size: int
    processed_objects: int
    buffer_cache_hit_ratio: float
    buffer_cache_page_read_count: int
    error_count: int

    def to_json_repr(self) -> Dict[str, Union[str, int]]:
        return {
            'elapsedTime': f'{self.elapsed_time:.2f}s',
            'executionTime': f'{self.execution_time:.2f}s',
            'compileTime': f'{self.compile_time}ns',
            'queueWaitTime': f'{self.queue_wait_time}ns',
            'resultCount': self.result_count,
            'resultSize': self.result_size,
            'processedObjects': self.processed_objects,
            'bufferCacheHitRatio': f'{self.buffer_cache_hit_ratio}%',
            'bufferCachePageReadCount': self.buffer_cache_page_read_count,
            'errorCount': self.error_count,
        }

    @staticmethod
    def create() -> ServerResponseMetrics:
        return ServerResponseMetrics(0.0, 0.0, 0.0, 0.0, 0, 0, 0, 0.0, 0, 0)


@dataclass
class ServerResponseError:
    code: int
    msg: str
    retriable: Optional[bool] = None

    def to_json_repr(self) -> Dict[str, Union[str, int, bool]]:
        output: Dict[str, Union[str, int, bool]] = {'code': self.code, 'msg': self.msg}
        if self.retriable is not None:
            output['retriable'] = self.retriable
        return output

    @staticmethod
    def _build_retry_group(
        retry_specification: NonRetriableSpecificationType, err_count: int, retriable_idx: Optional[int] = -1
    ) -> List[ServerResponseError]:
        errors: List[ServerResponseError] = []
        for err_idx in range(err_count):
            if err_idx == retriable_idx:
                errors.append(ServerResponseError(24045, 'Some unknown error occurred', True))
            elif retry_specification == NonRetriableSpecificationType.AllEmpty:
                errors.append(ServerResponseError(24040, 'Some unknown error occurred'))
            elif retry_specification == NonRetriableSpecificationType.AllFalse:
                errors.append(ServerResponseError(24040, 'Some unknown error occurred', False))
            elif retry_specification == NonRetriableSpecificationType.Random:
                if choice([0, 1]):
                    errors.append(ServerResponseError(24040, 'Some unknown error occurred', False))
                else:
                    errors.append(ServerResponseError(24040, 'Some unknown error occurred'))
            else:
                raise RuntimeError('Unrecognized retry specification type.')

        return errors

    @staticmethod
    def build_retry_group(
        group_type: RetriableGroupType,
        retry_specification: Optional[NonRetriableSpecificationType] = None,
        err_count: Optional[int] = None,
    ) -> List[ServerResponseError]:
        if err_count is None:
            err_count = choice([2, 3, 4, 5])
        if group_type == RetriableGroupType.All:
            return [ServerResponseError(24045, 'Some unknown retriable error occurred', True) for _ in range(err_count)]

        if retry_specification is None:
            raise RuntimeError('No non-retriable specification type provided.')
        if group_type == RetriableGroupType.Zero:
            return ServerResponseError._build_retry_group(retry_specification, err_count)
        elif group_type == RetriableGroupType.First:
            return ServerResponseError._build_retry_group(retry_specification, err_count, retriable_idx=0)
        elif group_type == RetriableGroupType.Middle:
            return ServerResponseError._build_retry_group(
                retry_specification, err_count, retriable_idx=(err_count // 2)
            )
        elif group_type == RetriableGroupType.Last:
            return ServerResponseError._build_retry_group(retry_specification, err_count, retriable_idx=err_count - 1)
        else:
            raise RuntimeError('Unrecognized retriable group type.')

    @staticmethod
    def build_errors(
        resp: ServerResponse,
        error_type: ErrorType,
        group_type: Optional[RetriableGroupType] = None,
        retry_specification: Optional[NonRetriableSpecificationType] = None,
        err_count: Optional[int] = None,
    ) -> ServerResponse:
        if error_type == ErrorType.Timeout:
            resp.http_status = 200
            resp.status = 'timeout'
            resp.metrics.error_count = 1
            resp.errors = [ServerResponseError(21002, 'Request timed out and will be cancelled.', True)]
        elif error_type == ErrorType.Http503:
            resp.http_status = 503
            resp.status = 'fatal'
            resp.metrics.error_count = 1
            resp.errors = [ServerResponseError(23000, 'Service is currently unavailable.')]
        elif error_type == ErrorType.InsufficientPermissions:
            resp.http_status = 403
            resp.status = 'fatal'
            resp.metrics.error_count = 1
            resp.errors = [
                ServerResponseError(20001, 'Insufficient permissions or the requested object does not exist')
            ]
        elif error_type == ErrorType.Unauthorized:
            resp.http_status = 401
            resp.status = 'fatal'
            resp.metrics.error_count = 1
            resp.errors = [ServerResponseError(20000, 'Unauthorized user.')]
        elif error_type == ErrorType.Retriable:
            resp.http_status = 200
            resp.status = 'fatal'
            if group_type is None:
                raise RuntimeError('No retry group type provided.')
            if group_type != RetriableGroupType.All and retry_specification is None:
                raise RuntimeError('No non-retriable specification type provided.')
            resp.errors = ServerResponseError.build_retry_group(group_type, retry_specification, err_count=err_count)
            resp.metrics.error_count = len(resp.errors)
        else:
            raise RuntimeError('Unrecognized error type.')

        return resp


@dataclass
class ServerResponseResults:
    results: Union[List[str], List[Dict[str, Any]]]

    def to_json_repr(self) -> Union[List[str], List[Dict[str, Any]]]:
        return self.results

    @staticmethod
    def build_results(resp: ServerResponse, row_count: int, result_type: ResultType) -> None:
        if result_type == ResultType.Object:
            obj_results: List[Dict[str, Any]] = []
            for idx in range(row_count):
                name = choice(NAMES)
                city = choice(US_CITIES)
                obj_results.append({'id': idx + 1, 'name': name, 'city': city})
            resp.results = ServerResponseResults(obj_results)
            resp.metrics.result_count = row_count
            resp.metrics.result_size = row_count * 10
        elif result_type == ResultType.Raw:
            resp.results = ServerResponseResults([choice(US_CITIES) for _ in range(row_count)])
            resp.metrics.result_count = row_count
            resp.metrics.result_size = row_count * 10
        else:
            raise RuntimeError(f'Unrecognized result type. Got type: {result_type}')

    @staticmethod
    def get_result_generator(result_type: ResultType) -> Callable[[], Union[Generator[bytes, None, None]]]:
        if result_type == ResultType.Object:

            def obj_generator() -> Generator[bytes, None, None]:
                idx = 0
                while True:
                    name = choice(NAMES)
                    city = choice(US_CITIES)
                    yield json.dumps({'id': idx + 1, 'name': name, 'city': city}).encode('utf-8')
                    idx += 1

            return obj_generator
        elif result_type == ResultType.Raw:

            def raw_generator() -> Generator[bytes, None, None]:
                while True:
                    yield bytes(choice(NAMES), 'utf-8')

            return raw_generator
        else:
            raise RuntimeError(f'Unrecognized result type. Got type: {result_type}')


@dataclass
class ServerResponse:
    http_status: int
    status: str
    metrics: ServerResponseMetrics
    request_id: str = field(default_factory=lambda: str(uuid4()))
    signature: Optional[Dict[str, str]] = None
    plans: Optional[Dict[str, Any]] = None
    results: Optional[ServerResponseResults] = None
    errors: Optional[List[ServerResponseError]] = None

    def to_json_repr(self, exclude_metrics: Optional[bool] = False) -> Dict[str, Any]:
        output: Dict[str, Any] = {'requestID': self.request_id, 'status': self.status}
        if self.signature is not None:
            output['signature'] = self.signature
        if self.plans is not None:
            output['plans'] = self.plans
        if self.results is not None:
            output['results'] = self.results.to_json_repr()
        if self.errors is not None:
            output['errors'] = [e.to_json_repr() for e in self.errors]
        if exclude_metrics is False:
            output['metrics'] = self.metrics.to_json_repr()
        return output

    def update_elapsed_time(self, t: float) -> None:
        self.metrics.elapsed_time = t
        self.metrics.execution_time = t

    @staticmethod
    def create() -> ServerResponse:
        return ServerResponse(200, 'success', ServerResponseMetrics.create())
