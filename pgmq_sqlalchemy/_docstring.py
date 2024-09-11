from typing import Any, Callable

"""
Common docstrings for
- func.py
- async_func.py
- queue.py

The above classes only need to define the usage of the function.
The before and after docstring is added by `add_common_docstring` decorator.
"""


def add_common_docstring(
    doc_string_before: str = "", doc_string_after: str = ""
) -> str:
    """
    decorator for adding common docstring for func.py, async_func.py, queue.py
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        func.__doc__ = doc_string_before + func.__doc__ + doc_string_after
        return func

    return decorator


def register_docstring(
    func: Callable[..., Any], doc_string_before: str = "", doc_string_after: str = ""
) -> Callable[..., Any]:
    func.__doc__ = doc_string_before + func.__doc__ + doc_string_after
    return func


CREATE_QUEUE_DOCSTRING_BEFORE = """
.. _unlogged_table: https://www.postgresql.org/docs/current/sql-createtable.html#SQL-CREATETABLE-UNLOGGED
.. |unlogged_table| replace:: **UNLOGGED TABLE**

**Create a new queue.**

* if ``unlogged`` is ``True``, the queue will be created as an |unlogged_table|_ .
* ``queue_name`` must be **less than 48 characters**.

"""

CREATE_QUEUE_DOCSTRING_AFTER = """
"""

CREATE_PARTITIONED_QUEUE_DOCSTRING_BEFORE = """
Create a new **partitioned** queue.

.. _pgmq_partitioned_queue: https://github.com/tembo-io/pgmq?tab=readme-ov-file#partitioned-queues
.. |pgmq_partitioned_queue| replace:: **PGMQ: Partitioned Queues**

"""

CREATE_PARTITIONED_QUEUE_DOCSTRING_AFTER = """
Args:
    queue_name (str): The name of the queue, should be less than 48 characters.
    partition_interval (int): Will create a new partition every ``partition_interval`` messages.
    retention_interval (int): The interval for retaining partitions. Any messages that have a `msg_id` less than ``max(msg_id)`` - ``retention_interval`` will be dropped.

        .. note::
            | Currently, only support for partitioning by **msg_id**.
            | Will add **time-based partitioning** in the future ``pgmq-sqlalchemy`` release.

.. important::
    | You must make sure that the ``pg_partman`` extension already **installed** in the Postgres.
    | ``pgmq-sqlalchemy`` will **auto create** the ``pg_partman`` extension if it does not exist in the Postgres.
    | For more details about ``pgmq`` with ``pg_partman``, checkout the |pgmq_partitioned_queue|_.

"""

DROP_QUEUE_DOCSTRING_BEFORE = """
Drop a queue.

.. _drop_queue_method: ref:`pgmq_sqlalchemy.PGMQueue.drop_queue`
.. |drop_queue_method| replace:: :py:meth:`~pgmq_sqlalchemy.PGMQueue.drop_queue`
"""

DROP_QUEUE_DOCSTRING_AFTER = """
.. warning::
    | All messages and queue itself will be deleted. (``pgmq.q_<queue_name>`` table)
    | **Archived tables** (``pgmq.a_<queue_name>`` table **will be dropped as well. )**
    |
    | See |archive_method|_ for more details.
"""

LIST_QUEUES_DOCSTRING_BEFORE = """List all queues."""

LIST_QUEUES_DOCSTRING_AFTER = """
"""

SEND_DOCSTRING_BEFORE = """Send a message to a queue."""

SEND_DOCSTRING_AFTER = """
"""

SEND_BATCH_DOCSTRING_BEFORE = """
Send a batch of messages to a queue.
"""

SEND_BATCH_DOCSTRING_AFTER = """
"""

READ_DOCSTRING_BEFORE = """
.. _for_update_skip_locked: https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE
.. |for_update_skip_locked| replace:: **FOR UPDATE SKIP LOCKED**

.. _read_method: ref:`pgmq_sqlalchemy.PGMQueue.read`
.. |read_method| replace:: :py:meth:`~pgmq_sqlalchemy.PGMQueue.read`

Read a message from the queue.

Returns:
    |schema_message_class|_ or ``None`` if the queue is empty.

.. note::
    | ``PGMQ`` use |for_update_skip_locked|_ lock to make sure **a message is only read by one consumer**.
    | See the `pgmq.read <https://github.com/tembo-io/pgmq/blob/main/pgmq-extension/sql/pgmq.sql?plain=1#L44-L75>`_ function for more details.
    |
    | For **consumer retries mechanism** (e.g. mark a message as failed after a certain number of retries) can be implemented by using the ``read_ct`` field in the |schema_message_class|_ object.


.. important::
    | ``vt`` is the **visibility timeout** in seconds.
    | When a message is read from the queue, it will be invisible to other consumers for the duration of the ``vt``.
"""

READ_DOCSTRING_AFTER = """
"""

READ_BATCH_DOCSTRING_BEFORE = """
| Read a batch of messages from the queue.
| Usage:

Returns:
    List of |schema_message_class|_ or ``None`` if the queue is empty.
"""

READ_BATCH_DOCSTRING_AFTER = """
"""

READ_WITH_POLL_DOCSTRING_BEFORE = """
.. _read_with_poll_method: ref:`pgmq_sqlalchemy.PGMQueue.read_with_poll`
.. |read_with_poll_method| replace:: :py:meth:`~pgmq_sqlalchemy.PGMQueue.read_with_poll`


| Read messages from a queue with long-polling.
|
| When the queue is empty, the function block at most ``max_poll_seconds`` seconds.
| During the polling, the function will check the queue every ``poll_interval_ms`` milliseconds, until the queue has ``qty`` messages.

Args:
    queue_name (str): The name of the queue.
    vt (Optional[int]): The visibility timeout in seconds.
    qty (int): The number of messages to read.
    max_poll_seconds (int): The maximum number of seconds to poll.
    poll_interval_ms (int): The interval in milliseconds to poll.

Returns:
    List of |schema_message_class|_ or ``None`` if the queue is empty.
"""

READ_WITH_POLL_DOCSTRING_AFTER = """
"""

POP_DOCSTRING_BEFORE = """
Reads a single message from a queue and deletes it upon read.
"""

POP_DOCSTRING_AFTER = """
"""

DELETE_DOCSTRING_BEFORE = """
Delete a message from the queue.

.. _delete_method: ref:`pgmq_sqlalchemy.PGMQueue.delete`
.. |delete_method| replace:: :py:meth:`~pgmq_sqlalchemy.PGMQueue.delete`

* Raises an error if the ``queue_name`` does not exist.
* Returns ``True`` if the message is deleted successfully.
* If the message does not exist, returns ``False``.
"""

DELETE_DOCSTRING_AFTER = """
"""

DELETE_BATCH_DOCSTRING_BEFORE = """
Delete a batch of messages from the queue.

.. _delete_batch_method: ref:`pgmq_sqlalchemy.PGMQueue.delete_batch`
.. |delete_batch_method| replace:: :py:meth:`~pgmq_sqlalchemy.PGMQueue.delete_batch`

.. note::
    | Instead of return `bool` like |delete_method|_,
    | |delete_batch_method|_ will return a list of ``msg_id`` that are successfully deleted.
"""

DELETE_BATCH_DOCSTRING_AFTER = """
"""

ARCHIVE_DOCSTRING_BEFORE = """
Archive a message from a queue.

.. _archive_method: ref:`pgmq_sqlalchemy.PGMQueue.archive`
.. |archive_method| replace:: :py:meth:`~pgmq_sqlalchemy.PGMQueue.archive`


* Message will be deleted from the queue and moved to the archive table.
    * Will be deleted from ``pgmq.q_<queue_name>`` and be inserted into the ``pgmq.a_<queue_name>`` table.
* raises an error if the ``queue_name`` does not exist.
* returns ``True`` if the message is archived successfully.
"""

ARCHIVE_DOCSTRING_AFTER = """
"""

ARCHIVE_BATCH_DOCSTRING_BEFORE = """
Archive multiple messages from a queue.

* Messages will be deleted from the queue and moved to the archive table.
* Returns a list of ``msg_id`` that are successfully archived.
"""

ARCHIVE_BATCH_DOCSTRING_AFTER = """
"""

PURGE_DOCSTRING_BEFORE = """
* Delete all messages from a queue, return the number of messages deleted.
* Archive tables will **not** be affected.
"""

PURGE_DOCSTRING_AFTER = """
"""

METRICS_DOCSTRING_BEFORE = """
Get metrics for a queue.

Returns:
    |schema_queue_metrics_class|_ or ``None`` if the queue does not exist.
"""

METRICS_DOCSTRING_AFTER = """
"""

METRICS_ALL_DOCSTRING_BEFORE = """
.. _read_committed_isolation_level: https://www.postgresql.org/docs/current/transaction-iso.html#XACT-READ-COMMITTED
.. |read_committed_isolation_level| replace:: **READ COMMITTED**

.. _metrics_all_method: ref:`pgmq_sqlalchemy.PGMQueue.metrics_all`
.. |metrics_all_method| replace:: :py:meth:`~pgmq_sqlalchemy.PGMQueue.metrics_all`

Get metrics for all queues.

Returns:
    List of |schema_queue_metrics_class|_ or ``None`` if there are no queues.
"""

METRICS_ALL_DOCSTRING_AFTER = """
.. warning::
    | You should use a **distributed lock** to avoid **race conditions** when calling |metrics_all_method|_ in **concurrent** |drop_queue_method|_ **scenarios**.
    |
    | Since the default PostgreSQL isolation level is |read_committed_isolation_level|_, the queue metrics to be fetched **may not exist** if there are **concurrent** |drop_queue_method|_ **operations**.
    | Check the `pgmq.metrics_all <https://github.com/tembo-io/pgmq/blob/main/pgmq-extension/sql/pgmq.sql?plain=1#L334-L346>`_ function for more details.
"""
