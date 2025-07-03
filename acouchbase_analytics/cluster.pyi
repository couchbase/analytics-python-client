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
from typing import Awaitable, overload

if sys.version_info < (3, 11):
    from typing_extensions import Unpack
else:
    from typing import Unpack

from acouchbase_analytics.database import AsyncDatabase
from couchbase_analytics.credential import Credential
from couchbase_analytics.options import ClusterOptions, ClusterOptionsKwargs, QueryOptions, QueryOptionsKwargs
from couchbase_analytics.result import AsyncQueryResult

class AsyncCluster:
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

    def database(self, database_name: str) -> AsyncDatabase: ...

    @overload
    def execute_query(self, statement: str) -> Awaitable[AsyncQueryResult]: ...

    @overload
    def execute_query(self, statement: str, options: QueryOptions) -> Awaitable[AsyncQueryResult]: ...

    @overload
    def execute_query(self, statement: str, **kwargs: Unpack[QueryOptionsKwargs]) -> Awaitable[AsyncQueryResult]: ...

    @overload
    def execute_query(self,
                      statement: str,
                      options: QueryOptions,
                      **kwargs: Unpack[QueryOptionsKwargs]) -> Awaitable[AsyncQueryResult]: ...

    @overload
    def execute_query(self,
                      statement: str,
                      options: QueryOptions,
                      *args: str,
                      **kwargs: Unpack[QueryOptionsKwargs]) -> Awaitable[AsyncQueryResult]: ...

    @overload
    def execute_query(self,
                      statement: str,
                      options: QueryOptions,
                      *args: str,
                      **kwargs: str) -> Awaitable[AsyncQueryResult]: ...

    @overload
    def execute_query(self,
                      statement: str,
                      *args: str,
                      **kwargs: str) -> Awaitable[AsyncQueryResult]: ...

    def shutdown(self) -> None: ...

    @overload
    @classmethod
    def create_instance(cls, http_endpoint: str, credential: Credential) -> AsyncCluster: ...

    @overload
    @classmethod
    def create_instance(cls,
                        http_endpoint: str,
                        credential: Credential,
                        options: ClusterOptions) -> AsyncCluster: ...

    @overload
    @classmethod
    def create_instance(cls,
                        http_endpoint: str,
                        credential: Credential,
                        **kwargs: Unpack[ClusterOptionsKwargs]) -> AsyncCluster: ...

    @overload
    @classmethod
    def create_instance(cls,
                        http_endpoint: str,
                        credential: Credential,
                        options: ClusterOptions,
                        **kwargs: Unpack[ClusterOptionsKwargs]) -> AsyncCluster: ...
