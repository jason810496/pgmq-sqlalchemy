.. _api-reference:

API Reference
=============

.. tip::
    | For a more detailed explanation or implementation of each `PGMQ function`,
    | see `PGMQ: SQL functions.md <https://github.com/tembo-io/pgmq/blob/main/docs/api/sql/functions.md>`_.


.. autoclass:: pgmq_sqlalchemy.PGMQueue
    :members:
    :inherited-members:
    :member-order: bysource
    :special-members: __init__


Function
--------

.. automodule:: pgmq_sqlalchemy.func
    :members:
    :member-order: bysource

Async Function
--------------

.. automodule:: pgmq_sqlalchemy.async_func
    :members:
    :member-order: bysource
    

Data Classes
------------

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
    