.. _api-reference:

API Reference
=============

.. tip::
    | For a more detailed explanation or implementation of each `PGMQ function`,
    | see `PGMQ: SQL functions.md <https://github.com/tembo-io/pgmq/blob/main/docs/api/sql/functions.md>`_.


PGMQueue
--------

The ``PGMQueue`` class provides a high-level interface for managing PGMQ queues. It handles session management internally.

.. autoclass:: pgmq_sqlalchemy.PGMQueue
    :members:
    :inherited-members:
    :member-order: bysource
    :special-members: __init__


PGMQOperation (op)
------------------

The ``PGMQOperation`` class (aliased as ``op``) provides static methods for transaction-friendly operations.
All methods require a session to be passed in, giving you full control over transaction boundaries.
This is useful when you need to combine PGMQ operations with your existing business logic within the same transaction.

.. autoclass:: pgmq_sqlalchemy.operation.PGMQOperation
    :members:
    :inherited-members:
    :member-order: bysource


Schema Classes
--------------

.. autoclass:: pgmq_sqlalchemy.schema.Message
    :members:
    :undoc-members:
    :inherited-members:
    :exclude-members: __init__
    


.. autoclass:: pgmq_sqlalchemy.schema.QueueMetrics
    :members:
    :undoc-members:
    :inherited-members:
    :exclude-members: __init__
    