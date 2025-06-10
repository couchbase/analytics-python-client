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

import sys
from concurrent.futures import Future, ThreadPoolExecutor
from typing import overload

if sys.version_info < (3, 11):
    from typing_extensions import Unpack
else:
    from typing import Unpack

from couchbase_analytics import JSONType
from couchbase_analytics.common.credential import Credential
from couchbase_analytics.common.result import BlockingQueryResult
from couchbase_analytics.options import (ClusterOptions,
                                         ClusterOptionsKwargs,
                                         QueryOptions,
                                         QueryOptionsKwargs)
from couchbase_analytics.protocol.core.client_adapter import _ClientAdapter

class Cluster:
    @overload
    def __init__(self, http_endpoint: str, credential: Credential) -> None: ...

    @overload
    def __init__(self,
                 http_endpoint: str,
                 credential: Credential,
                 options: ClusterOptions) -> None: ...

    @overload
    def __init__(self,
                 http_endpoint: str,
                 credential: Credential,
                 **kwargs: Unpack[ClusterOptionsKwargs]) -> None: ...

    @overload
    def __init__(self,
                 http_endpoint: str,
                 credential: Credential,
                 options: ClusterOptions,
                 **kwargs: Unpack[ClusterOptionsKwargs]) -> None: ...

    @property
    def client_adapter(self) -> _ClientAdapter: ...

    @property
    def connected(self) -> bool: ...

    @property
    def threadpool_executor(self) -> ThreadPoolExecutor: ...

    @overload
    def execute_query(self, statement: str) -> BlockingQueryResult: ...

    @overload
    def execute_query(self,
                      statement: str,
                      options: QueryOptions) -> BlockingQueryResult: ...

    @overload
    def execute_query(self,
                      statement: str,
                      **kwargs: Unpack[QueryOptionsKwargs]) -> BlockingQueryResult: ...

    @overload
    def execute_query(self,
                      statement: str,
                      options: QueryOptions,
                      **kwargs: Unpack[QueryOptionsKwargs]
                      ) -> BlockingQueryResult: ...

    @overload
    def execute_query(self,
                      statement: str,
                      options: QueryOptions,
                      *args: JSONType,
                      **kwargs: Unpack[QueryOptionsKwargs]) -> BlockingQueryResult: ...

    @overload
    def execute_query(self,
                      statement: str,
                      options: QueryOptions,
                      *args: JSONType,
                      **kwargs: str) -> BlockingQueryResult: ...

    @overload
    def execute_query(self,
                      statement: str,
                      *args: JSONType,
                      **kwargs: str) -> BlockingQueryResult: ...

    @overload
    def execute_query(self,
                      statement: str,
                      enable_cancel: bool) -> Future[BlockingQueryResult]: ...

    @overload
    def execute_query(self,
                      statement: str,
                      enable_cancel: bool,
                      *args: JSONType) -> Future[BlockingQueryResult]: ...

    @overload
    def execute_query(self,
                      statement: str,
                      options: QueryOptions,
                      enable_cancel: bool) -> Future[BlockingQueryResult]: ...

    @overload
    def execute_query(self,
                      statement: str,
                      enable_cancel: bool,
                      **kwargs: Unpack[QueryOptionsKwargs]) -> Future[BlockingQueryResult]: ...

    @overload
    def execute_query(self,
                      statement: str,
                      options: QueryOptions,
                      enable_cancel: bool,
                      **kwargs: Unpack[QueryOptionsKwargs]) -> Future[BlockingQueryResult]: ...

    @overload
    def execute_query(self,
                      statement: str,
                      options: QueryOptions,
                      enable_cancel: bool,
                      *args: JSONType,
                      **kwargs: Unpack[QueryOptionsKwargs]) -> Future[BlockingQueryResult]: ...

    @overload
    def execute_query(self,
                      statement: str,
                      options: QueryOptions,
                      *args: JSONType,
                      enable_cancel: bool,
                      **kwargs: Unpack[QueryOptionsKwargs]) -> Future[BlockingQueryResult]: ...


    @overload
    def execute_query(self,
                      statement: str,
                      options: QueryOptions,
                      enable_cancel: bool,
                      *args: JSONType,
                      **kwargs: str) -> Future[BlockingQueryResult]: ...

    @overload
    def execute_query(self,
                      statement: str,
                      options: QueryOptions,
                      *args: JSONType,
                      enable_cancel: bool,
                      **kwargs: str) -> Future[BlockingQueryResult]: ...

    @overload
    def execute_query(self,
                      statement: str,
                      enable_cancel: bool,
                      *args: JSONType,
                      **kwargs: str) -> Future[BlockingQueryResult]: ...

    @overload
    def execute_query(self,
                      statement: str,
                      *args: JSONType,
                      enable_cancel: bool,
                      **kwargs: str) -> Future[BlockingQueryResult]: ...

    def shutdown(self) -> None: ...

    @overload
    @classmethod
    def create_instance(cls, http_endpoint: str, credential: Credential) -> Cluster: ...

    @overload
    @classmethod
    def create_instance(cls,
                        http_endpoint: str,
                        credential: Credential,
                        options: ClusterOptions) -> Cluster: ...

    @overload
    @classmethod
    def create_instance(cls,
                        http_endpoint: str,
                        credential: Credential,
                        **kwargs: Unpack[ClusterOptionsKwargs]) -> Cluster: ...

    @overload
    @classmethod
    def create_instance(cls,
                        http_endpoint: str,
                        credential: Credential,
                        options: ClusterOptions,
                        **kwargs: Unpack[ClusterOptionsKwargs]) -> Cluster: ...
