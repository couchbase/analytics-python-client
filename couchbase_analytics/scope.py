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

from concurrent.futures import Future
from typing import TYPE_CHECKING, Union

from couchbase_analytics.result import BlockingQueryResult

if TYPE_CHECKING:
    from couchbase_analytics.protocol.database import Database


class Scope:
    """Create a Scope instance.

    The Scope instance exposes the operations which are available to be performed against an Analytics scope.

    Args:
        database (:class:`~couchbase_analytics.database.Database`): A :class:`~couchbase_analytics.database.Database` instance.
        scope_name (str): The scope name.

    """  # noqa: E501

    def __init__(self, database: Database, scope_name: str) -> None:
        from couchbase_analytics.protocol.scope import Scope as _Scope

        self._impl = _Scope(database, scope_name)

    @property
    def name(self) -> str:
        """
        str: The name of this :class:`~couchbase_analytics.scope.Scope` instance.
        """
        return self._impl.name

    def execute_query(
        self, statement: str, *args: object, **kwargs: object
    ) -> Union[Future[BlockingQueryResult], BlockingQueryResult]:
        """Executes a query against an Analytics scope.

        .. note::
            A departure from the operational SDK, the query is *NOT* executed lazily.

        .. seealso::
            * :meth:`couchbase_analytics.Cluster.execute_query`: For how to execute cluster-level queries.

        Args:
            statement (str): The N1QL statement to execute.
            options (:class:`~couchbase_analytics.options.QueryOptions`): Optional parameters for the query operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to override provided :class:`~couchbase_analytics.options.QueryOptions`

        Returns:
            :class:`~couchbase_analytics.result.BlockingQueryResult`: An instance of a :class:`~couchbase_analytics.result.BlockingQueryResult` which
            provides access to iterate over the query results and access metadata and metrics about the query.

        Examples:
            Simple query::

                q_str = 'SELECT * FROM airline WHERE country LIKE 'United%' LIMIT 2;'
                q_res = scope.execute_query(q_str)
                for row in q_res.rows():
                    print(f'Found row: {row}')

            Simple query with positional parameters::

                from couchbase_analytics.options import QueryOptions

                # ... other code ...

                q_str = 'SELECT * FROM airline WHERE country LIKE $1 LIMIT $2;'
                q_res = scope.execute_query(q_str, QueryOptions(positional_parameters=['United%', 5]))
                for row in q_res.rows():
                    print(f'Found row: {row}')

            Simple query with named parameters::

                from couchbase_analytics.options import QueryOptions

                # ... other code ...

                q_str = 'SELECT * FROM airline WHERE country LIKE $country LIMIT $lim;'
                q_res = scope.execute_query(q_str, QueryOptions(named_parameters={'country': 'United%', 'lim':2}))
                for row in q_res.rows():
                    print(f'Found row: {row}')

            Retrieve metadata and/or metrics from query::

                from couchbase_analytics.options import QueryOptions

                # ... other code ...

                q_str = 'SELECT * FROM `travel-sample` WHERE country LIKE $country LIMIT $lim;'
                q_res = scope.execute_query(q_str, QueryOptions(named_parameters={'country': 'United%', 'lim':2}))
                for row in q_res.rows():
                    print(f'Found row: {row}')

                print(f'Query metadata: {q_res.metadata()}')
                print(f'Query metrics: {q_res.metadata().metrics()}')

        """  # noqa: E501
        return self._impl.execute_query(statement, *args, **kwargs)
