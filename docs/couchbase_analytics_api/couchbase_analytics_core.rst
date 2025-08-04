===========================
Core API
===========================

.. contents::
    :local:

Cluster
==============

.. module:: couchbase_analytics.cluster
.. autoclass:: Cluster

    .. important::
        See :ref:`Cluster Overloads<cluster-overloads-ref>` for details on overloaded methods.

    .. automethod:: create_instance
    .. automethod:: database

    .. important::
        See :ref:`Cluster Overloads<cluster-overloads-ref>` for details on overloaded methods.

    .. automethod:: execute_query
    .. automethod:: shutdown


Database
==============

.. module:: couchbase_analytics.database
.. autoclass:: Database

    .. autoproperty:: name
    .. automethod:: scope

Scope
==============

.. module:: couchbase_analytics.scope
.. autoclass:: Scope

    .. autoproperty:: name

    .. important::
        See :ref:`Scope Overloads<scope-overloads-ref>` for details on overloaded methods.

    .. automethod:: execute_query
