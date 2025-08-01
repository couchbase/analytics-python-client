==============
Query (SQL++)
==============

.. contents::
    :local:
    :depth: 2


Enumerations
===============

.. module:: couchbase_analytics.query
.. autoenum:: QueryScanConsistency


Options
===============

.. module:: couchbase_analytics.options
    :no-index:

.. autoclass:: QueryOptions
    :no-index:


Results
===============

BlockingQueryResult
+++++++++++++++++++
.. module:: couchbase_analytics.result
    :no-index:

.. py:class:: BlockingQueryResult
    :no-index:

    .. automethod:: cancel
        :no-index:
    .. automethod:: rows
        :no-index:
    .. automethod:: get_all_rows
        :no-index:
    .. automethod:: metadata
        :no-index:

.. module:: couchbase_analytics.query
    :no-index:

QueryMetadata
+++++++++++++++++++
.. py:class:: QueryMetadata

    .. automethod:: request_id
    .. automethod:: warnings
    .. automethod:: metrics

QueryMetrics
+++++++++++++++++++
.. py:class:: QueryMetrics

    .. automethod:: elapsed_time
    .. automethod:: execution_time
    .. automethod:: result_count
    .. automethod:: result_size
    .. automethod:: processed_objects

QueryWarning
+++++++++++++++++++
.. py:class:: QueryWarning

    .. automethod:: code
    .. automethod:: message
