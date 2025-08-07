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

import sys
from concurrent.futures import Future, ThreadPoolExecutor
from typing import overload

if sys.version_info < (3, 11):
    from typing_extensions import Unpack
else:
    from typing import Unpack

from couchbase_analytics import JSONType
from couchbase_analytics.common.result import BlockingQueryResult
from couchbase_analytics.options import QueryOptions, QueryOptionsKwargs
from couchbase_analytics.protocol._core.client_adapter import _ClientAdapter
from couchbase_analytics.protocol.database import Database as Database

class Scope:
    def __init__(self, database: Database, scope_name: str) -> None: ...
    @property
    def client_adapter(self) -> _ClientAdapter: ...
    @property
    def name(self) -> str: ...
    @property
    def threadpool_executor(self) -> ThreadPoolExecutor: ...
    @overload
    def execute_query(self, statement: str) -> BlockingQueryResult: ...
    @overload
    def execute_query(self, statement: str, options: QueryOptions) -> BlockingQueryResult: ...
    @overload
    def execute_query(self, statement: str, **kwargs: Unpack[QueryOptionsKwargs]) -> BlockingQueryResult: ...
    @overload
    def execute_query(
        self, statement: str, options: QueryOptions, **kwargs: Unpack[QueryOptionsKwargs]
    ) -> BlockingQueryResult: ...
    @overload
    def execute_query(
        self, statement: str, options: QueryOptions, *args: JSONType, **kwargs: Unpack[QueryOptionsKwargs]
    ) -> BlockingQueryResult: ...
    @overload
    def execute_query(
        self, statement: str, options: QueryOptions, *args: JSONType, **kwargs: str
    ) -> BlockingQueryResult: ...
    @overload
    def execute_query(self, statement: str, *args: JSONType, **kwargs: str) -> BlockingQueryResult: ...
    @overload
    def execute_query(self, statement: str, enable_cancel: bool) -> Future[BlockingQueryResult]: ...
    @overload
    def execute_query(self, statement: str, enable_cancel: bool, *args: JSONType) -> Future[BlockingQueryResult]: ...
    @overload
    def execute_query(
        self, statement: str, options: QueryOptions, enable_cancel: bool
    ) -> Future[BlockingQueryResult]: ...
    @overload
    def execute_query(
        self, statement: str, enable_cancel: bool, **kwargs: Unpack[QueryOptionsKwargs]
    ) -> Future[BlockingQueryResult]: ...
    @overload
    def execute_query(
        self, statement: str, options: QueryOptions, enable_cancel: bool, **kwargs: Unpack[QueryOptionsKwargs]
    ) -> Future[BlockingQueryResult]: ...
    @overload
    def execute_query(
        self,
        statement: str,
        options: QueryOptions,
        enable_cancel: bool,
        *args: JSONType,
        **kwargs: Unpack[QueryOptionsKwargs],
    ) -> Future[BlockingQueryResult]: ...
    @overload
    def execute_query(
        self,
        statement: str,
        options: QueryOptions,
        *args: JSONType,
        enable_cancel: bool,
        **kwargs: Unpack[QueryOptionsKwargs],
    ) -> Future[BlockingQueryResult]: ...
    @overload
    def execute_query(
        self, statement: str, options: QueryOptions, enable_cancel: bool, *args: JSONType, **kwargs: str
    ) -> Future[BlockingQueryResult]: ...
    @overload
    def execute_query(
        self, statement: str, options: QueryOptions, *args: JSONType, enable_cancel: bool, **kwargs: str
    ) -> Future[BlockingQueryResult]: ...
    @overload
    def execute_query(
        self, statement: str, enable_cancel: bool, *args: JSONType, **kwargs: str
    ) -> Future[BlockingQueryResult]: ...
    @overload
    def execute_query(
        self, statement: str, *args: JSONType, enable_cancel: bool, **kwargs: str
    ) -> Future[BlockingQueryResult]: ...
