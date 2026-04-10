=====================
AsyncScope Overloads
=====================

.. _async-scope-overloads-ref:

AsyncScope
==============

.. module:: acouchbase_analytics.scope
    :no-index:

.. important::
    Not all class methods are listed.  Only methods that allow overloads.

.. py:class:: AysncScope
    :no-index:

    .. py:method:: execute_query(statement: str) -> Awaitable[AsyncQueryResult]
                   execute_query(statement: str, options: QueryOptions) -> Awaitable[AsyncQueryResult]
                   execute_query(statement: str, **kwargs: QueryOptionsKwargs) -> Awaitable[AsyncQueryResult]
                   execute_query(statement: str, options: QueryOptions, **kwargs: QueryOptionsKwargs) -> Awaitable[AysncQueryResult]
                   execute_query(statement: str, options: QueryOptions, *args: JSONType, **kwargs: QueryOptionsKwargs) -> Awaitable[AsyncQueryResult]
                   execute_query(statement: str, options: QueryOptions, *args: JSONType, **kwargs: str) -> Awaitable[AsyncQueryResult]
                   execute_query(statement: str, *args: JSONType, **kwargs: str) -> Awaitable[AsyncQueryResult]
        :no-index:

        Executes a query against an analytics scope.

        .. important::
            The cancel API is **VOLATILE** and is subject to change at any time.

        :param statement: The SQL++ statement to execute.
        :type statement: str
        :param options: Options to set for the query.
        :type options: Optional[:class:`~acouchbase_analytics.options.QueryOptions`]
        :param \*args: Can be used to pass in positional query placeholders.
        :type \*args: Optional[:py:type:`~acouchbase_analytics.JSONType`]
        :param \*\*kwargs: Keyword arguments that can be used in place or to overrride provided :class:`~acouchbase_analytics.options.ClusterOptions`.
            Can also be used to pass in named query placeholders.
        :type \*\*kwargs: Optional[Union[:class:`~acouchbase_analytics.options.QueryOptionsKwargs`, str]]

        :returns: :class:`~couchbase_analytics.result.AsyncQueryResult`: An instance of a :class:`~acouchbase_analytics.result.AsyncQueryResult`.
        :rtype: Awaitable[:class:`~acouchbase_analytics.result.AsyncQueryResult`]

    .. py:method:: start_query(statement: str) -> Awaitable[AsyncQueryHandle]
                   start_query(statement: str, options: StartQueryOptions) -> Awaitable[AsyncQueryHandle]
                   start_query(statement: str, **kwargs: StartQueryOptionsKwargs) -> Awaitable[AsyncQueryHandle]
                   start_query(statement: str, options: StartQueryOptions, **kwargs: StartQueryOptionsKwargs) -> Awaitable[AsyncQueryHandle]
                   start_query(statement: str, options: StartQueryOptions, *args: JSONType, **kwargs: StartQueryOptionsKwargs) -> Awaitable[AsyncQueryHandle]
                   start_query(statement: str, options: StartQueryOptions, *args: JSONType, **kwargs: str) -> Awaitable[AsyncQueryHandle]
                   start_query(statement: str, *args: JSONType, **kwargs: str) -> Awaitable[AsyncQueryHandle]
        :no-index:

        Executes a query against an analytics scope using the asynchronous server requests API.

        :param statement: The SQL++ statement to execute.
        :type statement: str
        :param options: Options to set for the query.
        :type options: Optional[:class:`~acouchbase_analytics.options.StartQueryOptions`]
        :param \*args: Can be used to pass in positional query placeholders.
        :type \*args: Optional[:py:type:`~acouchbase_analytics.JSONType`]
        :param \*\*kwargs: Keyword arguments that can be used in place or to overrride provided :class:`~acouchbase_analytics.options.StartClusterOptions`.
            Can also be used to pass in named query placeholders.
        :type \*\*kwargs: Optional[Union[:class:`~acouchbase_analytics.options.StartQueryOptionsKwargs`, str]]

        :returns: :class:`~acouchbase_analytics.query_handle.AsyncQueryHandle`: An instance of a :class:`~acouchbase_analytics.query_handle.AsyncQueryHandle`
        :rtype: Awaitable[:class:`~acouchbase_analytics.query_handle.AsyncQueryHandle`]
