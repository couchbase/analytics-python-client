=================
Cluster Overloads
=================

.. _cluster-overloads-ref:

Cluster
==============

.. module:: couchbase_analytics.cluster
    :no-index:

.. important::
    Not all class methods are listed.  Only methods that allow overloads.

.. py:class:: Cluster
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

        Executes a query against an Analytics cluster.

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

    .. py:method:: create_instance(endpoint: str, credential: Credential) -> Cluster
                   create_instance(endpoint: str, credential: Credential, options: ClusterOptions) -> Cluster
                   create_instance(endpoint: str, credential: Credential, **kwargs: ClusterOptionsKwargs) -> Cluster
                   create_instance(endpoint: str, credential: Credential, options: ClusterOptions, **kwargs: ClusterOptionsKwargs) -> Cluster
        :classmethod:
        :no-index:

        Create a Cluster instance

        .. important::
            The appropriate port needs to be specified. The SDK's default ports are 80 (http) and 443 (https).
            If attempting to connect to Capella, the correct ports are most likely to be 8095 (http) and 18095 (https).

            Capella example: https://cb.2xg3vwszqgqcrsix.cloud.couchbase.com:18095


        :param endpoint: The endpoint to use for sending HTTP requests to the Analytics server.
                        The format of the endpoint string is the **scheme** (``http`` or ``https`` is *required*, use ``https`` for TLS enabled connections), followed a hostname and optional port.
        :type endpoint: str
        :param credential: The user credentials.
        :type credential: :class:`~couchbase_analytics.credential.Credential`
        :param options: Global options to set for the cluster.
                        Some operations allow the global options to be overriden by passing in options to the operation.
        :type options: Optional[:class:`~couchbase_analytics.options.ClusterOptions`]
        :param \*\*kwargs: Keyword arguments that can be used in place or to overrride provided :class:`~couchbase_analytics.options.ClusterOptions`
        :type \*\*kwargs: Optional[:class:`~couchbase_analytics.options.ClusterOptionsKwargs`]

        :returns: An Analytics Cluster instance.
        :rtype: :class:`.Cluster`

        :raises ValueError: If incorrect endpoint is provided.
        :raises ValueError: If incorrect options are provided.
