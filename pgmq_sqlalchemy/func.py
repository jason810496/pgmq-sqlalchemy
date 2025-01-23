"""
sync functions
"""

from typing import List, Optional

from ._types import DBAPICursor
from .statement import db_api_statement as _statement
from .schema import Message, QueueMetrics


def create_queue(
    db_cursor: DBAPICursor, queue_name: str, unlogged: bool = False
) -> None:
    """

    .. code-block:: python

        from pgmq_sqlalchemy import func as pgmq_func
        pgmq_func.create_queue(db_cursor, 'my_queue')
        # or unlogged table queue
        pgmq_func.create_queue(db_cursor, 'my_queue', unlogged=True)

    """
    sql, params = _statement.create_queue(queue_name, unlogged)
    db_cursor.execute(sql, params)


def create_partitioned_queue(
    db_cursor: DBAPICursor,
    queue_name: str,
    partition_interval: int = 10000,
    retention_interval: int = 100000,
) -> None:
    """
    Create a new **partitioned** queue.

    .. _pgmq_partitioned_queue: https://github.com/tembo-io/pgmq?tab=readme-ov-file#partitioned-queues
    .. |pgmq_partitioned_queue| replace:: **PGMQ: Partitioned Queues**

    .. code-block:: python

        from pgmq_sqlalchemy import func as pgmq_func
        pgmq_func.create_partitioned_queue(db_cursor, 'my_partitioned_queue', partition_interval=10000, retention_interval=100000)

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
    sql, params = _statement.create_partitioned_queue(
        queue_name, partition_interval, retention_interval
    )
    db_cursor.execute(sql, params)


def validate_queue_name(db_cursor: DBAPICursor, queue_name: str) -> None:
    """
    * Will raise an error if the ``queue_name`` is more than 48 characters.

    .. code-block:: python

        from pgmq_sqlalchemy import func as pgmq_func
        pgmq_func.validate_queue_name(db_cursor, 'my_queue')
    """
    sql, params = _statement.validate_queue_name(queue_name)
    db_cursor.execute(sql, params)


def drop_queue(db_cursor: DBAPICursor, queue: str, partitioned: bool = False) -> bool:
    """
    Drop a queue.

    .. code-block:: python

        from pgmq_sqlalchemy import func as pgmq_func
        pgmq_func.drop_queue(db_cursor, 'my_queue')
        # for partitioned queue
        pgmq_func.drop_queue(db_cursor, 'my_partitioned_queue', partitioned=True)

    .. warning::
        | All messages and queue itself will be deleted. (``pgmq.q_<queue_name>`` table)
        | **Archived tables** (``pgmq.a_<queue_name>`` table **will be dropped as well. )**
    """
    sql, params = _statement.drop_queue(queue, partitioned)
    return db_cursor.execute(sql, params).fetchone()[0]


def list_queues(db_cursor: DBAPICursor) -> List[str]:
    """
    List all queues.

    .. code-block:: python

        from pgmq_sqlalchemy import func as pgmq_func
        queue_list = pgmq_func.list_queues(db_cursor)
        print(queue_list)
    """
    sql, params = _statement.list_queues()
    return [row[0] for row in db_cursor.execute(sql, params).fetchall()]


def send(db_cursor: DBAPICursor, queue_name: str, message: dict, delay: int = 0) -> int:
    """
    Send a message to a queue.

    .. code-block:: python

        from pgmq_sqlalchemy import func as pgmq_func
        msg_id = pgmq_func.send(db_cursor, 'my_queue', {'key': 'value', 'key2': 'value2'})
        print(msg_id)

    Example with delay:

    .. code-block:: python

        from pgmq_sqlalchemy import func as pgmq_func
        import time

        msg_id = pgmq_func.send(db_cursor, 'my_queue', {'key': 'value', 'key2': 'value2'}, delay=10)
        msg = pgmq_func.read(db_cursor, 'my_queue')
        assert msg is None
        time.sleep(10)
        msg = pgmq_func.read(db_cursor, 'my_queue')
        assert msg is not None
    """
    sql, params = _statement.send(queue_name, message, delay)
    return db_cursor.execute(sql, params).fetchone()[0]


def send_batch(
    db_cursor: DBAPICursor, queue_name: str, messages: List[dict], delay: int = 0
) -> List[int]:
    """
    Send a batch of messages to a queue.

    .. code-block:: python

        from pgmq_sqlalchemy import func as pgmq_func
        msgs = [{'key': 'value', 'key2': 'value2'}, {'key': 'value', 'key2': 'value2'}]
        msg_ids = pgmq_func.send_batch(db_cursor, 'my_queue', msgs)
        print(msg_ids)
        # send with delay
        msg_ids = pgmq_func.send_batch(db_cursor, 'my_queue', msgs, delay=10)
    """
    sql, params = _statement.send_batch(queue_name, messages, delay)
    return [row[0] for row in db_cursor.execute(sql, params).fetchall()]


def read(
    db_cursor: DBAPICursor, queue_name: str, vt: Optional[int] = None
) -> Optional[Message]:
    """
    .. _for_update_skip_locked: https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE
    .. |for_update_skip_locked| replace:: **FOR UPDATE SKIP LOCKED**

    Read a message from the queue.

    Returns:
        Message or ``None`` if the queue is empty.

    .. note::
        | ``PGMQ`` use |for_update_skip_locked|_ lock to make sure **a message is only read by one consumer**.
        | See the `pgmq.read <https://github.com/tembo-io/pgmq/blob/main/pgmq-extension/sql/pgmq.sql?plain=1#L44-L75>`_ function for more details.
        |
        | For **consumer retries mechanism** (e.g. mark a message as failed after a certain number of retries) can be implemented by using the ``read_ct`` field in the Message object.

    .. important::
        | ``vt`` is the **visibility timeout** in seconds.
        | When a message is read from the queue, it will be invisible to other consumers for the duration of the ``vt``.

    Usage:

    .. code-block:: python

        from pgmq_sqlalchemy import func as pgmq_func
        from pgmq_sqlalchemy.schema import Message

        msg: Message = pgmq_func.read(db_cursor, 'my_queue')
        if msg:
            print(msg.msg_id)
            print(msg.message)
            print(msg.read_ct)  # read count, how many times the message has been read

    Example with ``vt``:

    .. code-block:: python

        from pgmq_sqlalchemy import func as pgmq_func
        import time

        # assert `read_vt_demo` is empty
        pgmq_func.send(db_cursor, 'read_vt_demo', {'key': 'value', 'key2': 'value2'})
        msg = pgmq_func.read(db_cursor, 'read_vt_demo', vt=10)
        assert msg is not None

        # try to read immediately
        msg = pgmq_func.read(db_cursor, 'read_vt_demo')
        assert msg is None  # will return None because the message is still invisible

        # try to read after 5 seconds
        time.sleep(5)
        msg = pgmq_func.read(db_cursor, 'read_vt_demo')
        assert msg is None  # still invisible after 5 seconds

        # try to read after 11 seconds
        time.sleep(6)
        msg = pgmq_func.read(db_cursor, 'read_vt_demo')
        assert msg is not None  # the message is visible after 10 seconds
    """
    sql, params = _statement.read(queue_name, vt)
    row = db_cursor.execute(sql, params).fetchone()
    if row is None:
        return None
    return Message(
        msg_id=row[0], read_ct=row[1], enqueued_at=row[2], vt=row[3], message=row[4]
    )


def read_batch(
    db_cursor: DBAPICursor,
    queue_name: str,
    batch_size: int = 1,
    vt: Optional[int] = None,
) -> Optional[List[Message]]:
    """
    Read a batch of messages from the queue.

    Returns:
        List of Message or ``None`` if the queue is empty.

    .. code-block:: python

        from pgmq_sqlalchemy import func as pgmq_func
        from pgmq_sqlalchemy.schema import Message

        msgs: List[Message] = pgmq_func.read_batch(db_cursor, 'my_queue', batch_size=10)
        # with vt
        msgs: List[Message] = pgmq_func.read_batch(db_cursor, 'my_queue', batch_size=10, vt=10)
    """
    sql, params = _statement.read_batch(queue_name, vt, batch_size)
    rows = db_cursor.execute(sql, params).fetchall()
    if not rows:
        return None
    return [
        Message(
            msg_id=row[0], read_ct=row[1], enqueued_at=row[2], vt=row[3], message=row[4]
        )
        for row in rows
    ]


def read_with_poll(
    db_cursor: DBAPICursor,
    queue_name: str,
    vt: Optional[int] = None,
    qty: int = 1,
    max_poll_seconds: int = 5,
    poll_interval_ms: int = 100,
) -> Optional[List[Message]]:
    """
    Read messages from a queue with long-polling.

    When the queue is empty, the function block at most ``max_poll_seconds`` seconds.
    During the polling, the function will check the queue every ``poll_interval_ms`` milliseconds, until the queue has ``qty`` messages.

    Args:
        queue_name (str): The name of the queue.
        vt (Optional[int]): The visibility timeout in seconds.
        qty (int): The number of messages to read.
        max_poll_seconds (int): The maximum number of seconds to poll.
        poll_interval_ms (int): The interval in milliseconds to poll.

    Returns:
        List of Message or ``None`` if the queue is empty.

    Usage:

    .. code-block:: python

        from pgmq_sqlalchemy import func as pgmq_func
        import time

        msg_id = pgmq_func.send(db_cursor, 'my_queue', {'key': 'value'}, delay=6)

        # the following code will block for 5 seconds
        msgs = pgmq_func.read_with_poll(db_cursor, 'my_queue', qty=1, max_poll_seconds=5, poll_interval_ms=100)
        assert msgs is None

        # try read_with_poll again
        # the following code will only block for 1 second
        msgs = pgmq_func.read_with_poll(db_cursor, 'my_queue', qty=1, max_poll_seconds=5, poll_interval_ms=100)
        assert msgs is not None

    Another example:

    .. code-block:: python

        from pgmq_sqlalchemy import func as pgmq_func

        msg = {'key': 'value'}
        msg_ids = pgmq_func.send_batch(db_cursor, 'my_queue', [msg, msg, msg, msg], delay=3)

        # the following code will block for 3 seconds
        msgs = pgmq_func.read_with_poll(db_cursor, 'my_queue', qty=3, max_poll_seconds=5, poll_interval_ms=100)
        assert len(msgs) == 3  # will read at most 3 messages (qty=3)
    """
    sql, params = _statement.read_with_poll(
        queue_name, vt, qty, max_poll_seconds, poll_interval_ms
    )
    rows = db_cursor.execute(sql, params).fetchall()
    if not rows:
        return None
    return [
        Message(
            msg_id=row[0], read_ct=row[1], enqueued_at=row[2], vt=row[3], message=row[4]
        )
        for row in rows
    ]


def pop(db_cursor: DBAPICursor, queue_name: str) -> Optional[Message]:
    """
    Reads a single message from a queue and deletes it upon read.

    .. code-block:: python

        from pgmq_sqlalchemy import func as pgmq_func
        msg = pgmq_func.pop(db_cursor, 'my_queue')
        if msg:
            print(msg.msg_id)
            print(msg.message)
    """
    sql, params = _statement.pop(queue_name)
    row = db_cursor.execute(sql, params).fetchone()
    if row is None:
        return None
    return Message(
        msg_id=row[0], read_ct=row[1], enqueued_at=row[2], vt=row[3], message=row[4]
    )


def delete(db_cursor: DBAPICursor, queue_name: str, msg_id: int) -> bool:
    """
    Delete a message from the queue.

    * Raises an error if the ``queue_name`` does not exist.
    * Returns ``True`` if the message is deleted successfully.
    * If the message does not exist, returns ``False``.

    .. code-block:: python

        from pgmq_sqlalchemy import func as pgmq_func
        msg_id = pgmq_func.send(db_cursor, 'my_queue', {'key': 'value'})
        assert pgmq_func.delete(db_cursor, 'my_queue', msg_id)
                assert pgmq_func.delete(db_cursor, 'my_queue', msg_id)
        assert not pgmq_func.delete(db_cursor, 'my_queue', msg_id)
    """
    sql, params = _statement.delete(queue_name, msg_id)
    return db_cursor.execute(sql, params).fetchone()[0]


def delete_batch(
    db_cursor: DBAPICursor, queue_name: str, msg_ids: List[int]
) -> List[int]:
    """
    Delete a batch of messages from the queue.

    .. note::
        | Instead of return `bool` like `delete`,
        | `delete_batch` will return a list of `msg_id` that are successfully deleted.

    .. code-block:: python

        from pgmq_sqlalchemy import func as pgmq_func
        msg_ids = pgmq_func.send_batch(db_cursor, 'my_queue', [{'key': 'value'}, {'key': 'value'}])
        assert pgmq_func.delete_batch(db_cursor, 'my_queue', msg_ids) == msg_ids
    """
    sql, params = _statement.delete_batch(queue_name, msg_ids)
    return [row[0] for row in db_cursor.execute(sql, params).fetchall()]


def archive(db_cursor: DBAPICursor, queue_name: str, msg_id: int) -> bool:
    """
    Archive a message from a queue.

    * Message will be deleted from the queue and moved to the archive table.
        * Will be deleted from ``pgmq.q_<queue_name>`` and be inserted into the ``pgmq.a_<queue_name>`` table.
    * raises an error if the ``queue_name`` does not exist.
    * returns ``True`` if the message is archived successfully.

    .. code-block:: python

        from pgmq_sqlalchemy import func as pgmq_func
        msg_id = pgmq_func.send(db_cursor, 'my_queue', {'key': 'value'})
        assert pgmq_func.archive(db_cursor, 'my_queue', msg_id)
        # since the message is archived, queue will be empty
        assert pgmq_func.read(db_cursor, 'my_queue') is None
    """
    sql, params = _statement.archive(queue_name, msg_id)
    return db_cursor.execute(sql, params).fetchone()[0]


def archive_batch(
    db_cursor: DBAPICursor, queue_name: str, msg_ids: List[int]
) -> List[int]:
    """
    Archive multiple messages from a queue.

    * Messages will be deleted from the queue and moved to the archive table.
    * Returns a list of ``msg_id`` that are successfully archived.

    .. code-block:: python

        from pgmq_sqlalchemy import func as pgmq_func
        msg_ids = pgmq_func.send_batch(db_cursor, 'my_queue', [{'key': 'value'}, {'key': 'value'}])
        assert pgmq_func.archive_batch(db_cursor, 'my_queue', msg_ids) == msg_ids
        assert pgmq_func.read(db_cursor, 'my_queue') is None
    """
    sql, params = _statement.archive_batch(queue_name, msg_ids)
    return [row[0] for row in db_cursor.execute(sql, params).fetchall()]


def purge(db_cursor: DBAPICursor, queue_name: str) -> int:
    """
    * Delete all messages from a queue, return the number of messages deleted.
    * Archive tables will **not** be affected.

    .. code-block:: python

        from pgmq_sqlalchemy import func as pgmq_func
        msg_ids = pgmq_func.send_batch(db_cursor, 'my_queue', [{'key': 'value'}, {'key': 'value'}])
        assert pgmq_func.purge(db_cursor, 'my_queue') == 2
        assert pgmq_func.read(db_cursor, 'my_queue') is None
    """
    sql, params = _statement.purge(queue_name)
    return db_cursor.execute(sql, params).fetchone()[0]


def metrics(db_cursor: DBAPICursor, queue_name: str) -> Optional[QueueMetrics]:
    """
    Get metrics for a queue.

    Returns:
        QueueMetrics or ``None`` if the queue does not exist.

    Usage:

    .. code-block:: python

        from pgmq_sqlalchemy import func as pgmq_func
        from pgmq_sqlalchemy.schema import QueueMetrics

        metrics: QueueMetrics = pgmq_func.metrics(db_cursor, 'my_queue')
        if metrics:
            print(metrics.queue_name)
            print(metrics.queue_length)
            print(metrics.total_messages)
    """
    sql, params = _statement.metrics(queue_name)
    row = db_cursor.execute(sql, params).fetchone()
    if row is None:
        return None
    return QueueMetrics(
        queue_name=row[0],
        queue_length=row[1],
        newest_msg_age_sec=row[2],
        oldest_msg_age_sec=row[3],
        total_messages=row[4],
    )


def metrics_all(db_cursor: DBAPICursor) -> Optional[List[QueueMetrics]]:
    """
    .. _read_committed_isolation_level: https://www.postgresql.org/docs/current/transaction-iso.html#XACT-READ-COMMITTED
    .. |read_committed_isolation_level| replace:: **READ COMMITTED**

    Get metrics for all queues.

    Returns:
        List of QueueMetrics or ``None`` if there are no queues.

    Usage:

    .. code-block:: python

        from pgmq_sqlalchemy import func as pgmq_func
        from pgmq_sqlalchemy.schema import QueueMetrics

        metrics: List[QueueMetrics] = pgmq_func.metrics_all(db_cursor)
        if metrics:
            for m in metrics:
                print(m.queue_name)
                print(m.queue_length)
                print(m.total_messages)

    .. warning::
        | You should use a **distributed lock** to avoid **race conditions** when calling `metrics_all` in **concurrent** `drop_queue` **scenarios**.
        |
        | Since the default PostgreSQL isolation level is |read_committed_isolation_level|_, the queue metrics to be fetched **may not exist** if there are **concurrent** `drop_queue` **operations**.
        | Check the `pgmq.metrics_all <https://github.com/tembo-io/pgmq/blob/main/pgmq-extension/sql/pgmq.sql?plain=1#L334-L346>`_ function for more details.
    """
    sql, params = _statement.metrics_all()
    rows = db_cursor.execute(sql, params).fetchall()
    if not rows:
        return None
    return [
        QueueMetrics(
            queue_name=row[0],
            queue_length=row[1],
            newest_msg_age_sec=row[2],
            oldest_msg_age_sec=row[3],
            total_messages=row[4],
        )
        for row in rows
    ]
