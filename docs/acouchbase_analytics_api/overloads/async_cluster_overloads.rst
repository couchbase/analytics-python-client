=======================
AsyncCluster Overloads
=======================

.. _async-cluster-overloads-ref:

AsyncCluster
==============

.. module:: acouchbase_analytics.cluster
    :no-index:

.. important::
    Not all class methods are listed.  Only methods that allow overloads.

.. py:class:: AsyncCluster
    :no-index:

    .. py:method:: execute_query(statement: str) -> Awaitable[AsyncQueryResult]
                   execute_query(statement: str, options: QueryOptions) -> Awaitable[AsyncQueryResult]
                   execute_query(statement: str, **kwargs: QueryOptionsKwargs) -> Awaitable[AsyncQueryResult]
                   execute_query(statement: str, options: QueryOptions, **kwargs: QueryOptionsKwargs) -> Awaitable[AsyncQueryResult]
                   execute_query(statement: str, options: QueryOptions, *args: JSONType, **kwargs: QueryOptionsKwargs) -> Awaitable[AsyncQueryResult]
                   execute_query(statement: str, options: QueryOptions, *args: JSONType, **kwargs: str) -> Awaitable[AsyncQueryResult]
                   execute_query(statement: str, *args: JSONType, **kwargs: str) -> Awaitable[AsyncQueryResult]
        :no-index:

        Executes a query against a Capella analytics cluster.

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

        :returns: An `Awaitable` is returned.  Once the `Awaitable` completes, an instance of a :class:`~acouchbase_analytics.result.AsyncQueryResult` will be available.
        :rtype: Awaitable[:class:`~acouchbase_analytics.result.AsyncQueryResult`]

    .. py:method:: create_instance(endpoint: str, credential: Credential) -> AsyncCluster
                   create_instance(endpoint: str, credential: Credential, options: ClusterOptions) -> AsyncCluster
                   create_instance(endpoint: str, credential: Credential, **kwargs: ClusterOptionsKwargs) -> AsyncCluster
                   create_instance(endpoint: str, credential: Credential, options: ClusterOptions, **kwargs: ClusterOptionsKwargs) -> AsyncCluster
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
        :type credential: :class:`~acouchbase_analytics.credential.Credential`
        :param options: Global options to set for the cluster.
                        Some operations allow the global options to be overriden by passing in options to the operation.
        :type options: Optional[:class:`~acouchbase_analytics.options.ClusterOptions`]
        :param \*\*kwargs: Keyword arguments that can be used in place or to overrride provided :class:`~acouchbase_analytics.options.ClusterOptions`
        :type \*\*kwargs: Optional[:class:`~acouchbase_analytics.options.ClusterOptionsKwargs`]

        :returns: An Analytics AsyncCluster instance.
        :rtype: :class:`.AsyncCluster`

        :raises ValueError: If incorrect endpoint is provided.
        :raises ValueError: If incorrect options are provided.
