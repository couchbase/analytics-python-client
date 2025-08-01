=================
Scope Overloads
=================

.. _scope-overloads-ref:

Scope
==============

.. module:: couchbase_analytics.scope
    :no-index:

.. important::
    Not all class methods are listed.  Only methods that allow overloads.

.. py:class:: Scope
    :no-index:

    .. py:method:: execute_query(statement: str) -> BlockingQueryResult
                   execute_query(statement: str, options: QueryOptions) -> BlockingQueryResult
                   execute_query(statement: str, **kwargs: QueryOptionsKwargs) -> BlockingQueryResult
                   execute_query(statement: str, options: QueryOptions, **kwargs: QueryOptionsKwargs) -> BlockingQueryResult
                   execute_query(statement: str, options: QueryOptions, *args: JSONType, **kwargs: QueryOptionsKwargs) -> BlockingQueryResult
                   execute_query(statement: str, options: QueryOptions, *args: JSONType, **kwargs: str) -> BlockingQueryResult
                   execute_query(statement: str, *args: JSONType, **kwargs: str) -> BlockingQueryResult
                   execute_query(statement: str, enable_cancel: bool) -> Future[BlockingQueryResult]
                   execute_query(statement: str, enable_cancel: bool, *args: JSONType) -> Future[BlockingQueryResult]
                   execute_query(statement: str, options: QueryOptions, enable_cancel: bool) -> Future[BlockingQueryResult]
                   execute_query(statement: str, enable_cancel: bool, **kwargs: QueryOptionsKwargs) -> Future[BlockingQueryResult]
                   execute_query(statement: str, options: QueryOptions, enable_cancel: bool, **kwargs: QueryOptionsKwargs) -> Future[BlockingQueryResult]
                   execute_query(statement: str, options: QueryOptions, enable_cancel: bool, *args: JSONType, **kwargs: QueryOptionsKwargs) -> Future[BlockingQueryResult]
                   execute_query(statement: str, options: QueryOptions, *args: JSONType, enable_cancel: bool, **kwargs: QueryOptionsKwargs) -> Future[BlockingQueryResult]
                   execute_query(statement: str, options: QueryOptions, enable_cancel: bool, *args: JSONType, **kwargs: str) -> Future[BlockingQueryResult]
                   execute_query(statement: str, options: QueryOptions, *args: JSONType, enable_cancel: bool, **kwargs: str) -> Future[BlockingQueryResult]
                   execute_query(statement: str, enable_cancel: bool, *args: JSONType, **kwargs: str) -> Future[BlockingQueryResult]
                   execute_query(statement: str, *args: JSONType, enable_cancel: bool, **kwargs: str) -> Future[BlockingQueryResult]
        :no-index:

        Executes a query against an Analytics scope.

        .. important::
            The cancel API is **VOLATILE** and is subject to change at any time.

        :param statement: The SQL++ statement to execute.
        :type statement: str
        :param options: Options to set for the query.
        :type options: Optional[:class:`~couchbase_analytics.options.QueryOptions`]
        :param enable_cancel: Enable cancellation of the result or results stream.
        :type enable_cancel: Optional[bool]
        :param \*args: Can be used to pass in positional query placeholders.
        :type \*args: Optional[:py:type:`~couchbase_analytics.JSONType`]
        :param \*\*kwargs: Keyword arguments that can be used in place or to overrride provided :class:`~couchbase_analytics.options.ClusterOptions`.
            Can also be used to pass in named query placeholders.
        :type \*\*kwargs: Optional[Union[:class:`~couchbase_analytics.options.QueryOptionsKwargs`, str]]

        :returns: An instance of :class:`~couchbase_analytics.result.BlockingQueryResult`. When a cancel token is provided
            a :class:`~concurrent.futures.Future` is returned.  Once the :class:`~concurrent.futures.Future` completes, an instance of a :class:`~couchbase_analytics.result.BlockingQueryResult` will be available.
        :rtype: Union[Future[:class:`~couchbase_analytics.result.BlockingQueryResult`], :class:`~couchbase_analytics.result.BlockingQueryResult`]
