=========================
Server Async Request API
=========================

.. contents::
    :local:

.. module:: couchbase_analytics.query_handle

BlockingQueryHandle
+++++++++++++++++++

.. py:class:: BlockingQueryHandle

    .. automethod:: fetch_status
    .. automethod:: cancel

BlockingQueryResultHandle
+++++++++++++++++++++++++

.. py:class:: BlockingQueryResultHandle

    .. automethod:: fetch_results
    .. automethod:: discard_results

BlockingQueryStatus
+++++++++++++++++++

.. py:class:: BlockingQueryStatus

    .. automethod:: results_ready
    .. automethod:: result_handle
