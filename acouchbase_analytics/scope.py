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

import sys
from typing import TYPE_CHECKING, Awaitable

if sys.version_info < (3, 10):
    from typing_extensions import TypeAlias
else:
    from typing import TypeAlias

from acouchbase_analytics.query_handle import AsyncQueryHandle
from acouchbase_analytics.result import AsyncQueryResult

if TYPE_CHECKING:
    from acouchbase_analytics.protocol.database import AsyncDatabase


class AsyncScope:
    def __init__(self, database: AsyncDatabase, scope_name: str) -> None:
        from acouchbase_analytics.protocol.scope import AsyncScope as _AsyncScope

        self._impl = _AsyncScope(database, scope_name)

    @property
    def name(self) -> str:
        """
        str: The name of this :class:`~acouchbase_analytics.scope.AsyncScope` instance.
        """
        return self._impl.name

    def execute_query(self, statement: str, *args: object, **kwargs: object) -> Awaitable[AsyncQueryResult]:
        """Executes a query against an Analytics scope.

        .. note::
            A departure from the operational SDK, the query is *NOT* executed lazily.

        .. seealso::
            * :meth:`acouchbase_analytics.Cluster.execute_query`: For how to execute cluster-level queries.

        Args:
            statement (str): The N1QL statement to execute.
            options (:class:`~acouchbase_analytics.options.QueryOptions`): Optional parameters for the query operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to override provided :class:`~acouchbase_analytics.options.QueryOptions`

        Returns:
            :class:`~couchbase_analytics.result.AsyncQueryResult`: An instance of a :class:`~acouchbase_analytics.result.AsyncQueryResult`.

        Examples:
            Simple query::

                q_str = 'SELECT * FROM airline WHERE country LIKE 'United%' LIMIT 2;'
                q_res = scope.execute_query(q_str)
                async for row in q_res.rows():
                    print(f'Found row: {row}')

            Simple query with positional parameters::

                from acouchbase_analytics.options import QueryOptions

                # ... other code ...

                q_str = 'SELECT * FROM airline WHERE country LIKE $1 LIMIT $2;'
                q_res = scope.execute_query(q_str, QueryOptions(positional_parameters=['United%', 5]))
                async for row in q_res.rows():
                    print(f'Found row: {row}')

            Simple query with named parameters::

                from acouchbase_analytics.options import QueryOptions

                # ... other code ...

                q_str = 'SELECT * FROM airline WHERE country LIKE $country LIMIT $lim;'
                q_res = scope.execute_query(q_str, QueryOptions(named_parameters={'country': 'United%', 'lim':2}))
                async for row in q_res.rows():
                    print(f'Found row: {row}')

            Retrieve metadata and/or metrics from query::

                from acouchbase_analytics.options import QueryOptions

                # ... other code ...

                q_str = 'SELECT * FROM `travel-sample` WHERE country LIKE $country LIMIT $lim;'
                q_res = scope.execute_query(q_str, QueryOptions(named_parameters={'country': 'United%', 'lim':2}))
                async for row in q_res.rows():
                    print(f'Found row: {row}')

                print(f'Query metadata: {q_res.metadata()}')
                print(f'Query metrics: {q_res.metadata().metrics()}')

        """  # noqa: E501
        return self._impl.execute_query(statement, *args, **kwargs)

    def start_query(self, statement: str, *args: object, **kwargs: object) -> Awaitable[AsyncQueryHandle]:
        """Executes a query against an Analytics scope in async mode.

        .. seealso::
            :meth:`acouchbase_analytics.AsyncCluster.start_query`: For how to execute cluster-level queries.

        Args:
            statement: The SQL++ statement to execute.
            options (:class:`~acouchbase_analytics.options.StartQueryOptions`): Optional parameters for the query operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to override provided :class:`~acouchbase_analytics.options.StartQueryOptions`

        Returns:
            :class:`~acouchbase_analytics.query_handle.AsyncQueryHandle`: An instance of a :class:`~acouchbase_analytics.query_handle.AsyncQueryHandle`
        """  # noqa: E501
        return self._impl.start_query(statement, *args, **kwargs)


Scope: TypeAlias = AsyncScope
