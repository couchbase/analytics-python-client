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

from enum import Enum


class ErrorType(Enum):
    Timeout = 'timeout'
    Unauthorized = 'unauthorized'
    InsufficientPermissions = 'insufficient_permissions'
    Retriable = 'retriable'

    @staticmethod
    def from_str(error_type: str) -> ErrorType:
        match = next((t for t in ErrorType if t.value == error_type), None)
        if not match:
            raise ValueError(f'Invalid error type: {error_type}. '
                             f'Valid options are: {[e.value for e in ErrorType]}')
        return match

class ResultType(Enum):
    Object = 'object'
    Raw = 'raw'

    @staticmethod
    def from_str(result_type: str) -> ResultType:
        match = next((t for t in ResultType if t.value == result_type), None)
        if not match:
            raise ValueError(f'Invalid result type: {result_type}. '
                             f'Valid options are: {[e.value for e in ResultType]}')
        return match

class RetriableGroupType(Enum):
    All = 'all'
    Zero = 'zero'
    First = 'first'
    Middle = 'middle'
    Last = 'last'

    @staticmethod
    def from_str(rgt: str) -> RetriableGroupType:
        match = next((t for t in RetriableGroupType if t.value == rgt), None)
        if not match:
            raise ValueError(f'Invalid retriable group type: {rgt}. '
                             f'Valid options are: {[e.value for e in RetriableGroupType]}')
        return match

class NonRetriableSpecificationType(Enum):
    AllEmpty = 'all_empty'
    AllFalse = 'all_false'
    Random = 'random'

    @staticmethod
    def from_str(nrst: str) -> NonRetriableSpecificationType:
        match = next((t for t in NonRetriableSpecificationType if t.value == nrst), None)
        if not match:
            raise ValueError(f'Invalid non-retriable specification type: {nrst}. '
                             f'Valid options are: {[e.value for e in NonRetriableSpecificationType]}')
        return match
