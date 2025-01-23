"""
async functions
"""

from typing import List, Optional

from ._types import AsyncDBAPICursor
from . import _statement
from .schema import Message, QueueMetrics


async def create_queue(
    db_cursor: AsyncDBAPICursor, queue_name: str, unlogged: bool = False
) -> None:
    """

    .. code-block:: python

        from pgmq_sqlalchemy import async_func as pgmq_func
        await pgmq_func.create_queue(db_cursor, 'my_queue')
        # or unlogged table queue
        await pgmq_func.create_queue(db_cursor, 'my_queue', unlogged=True)
    """
    sql, params = _statement.create_queue(queue_name, unlogged)
    await db_cursor.execute(sql, *params)


async def create_partitioned_queue(
    db_cursor: AsyncDBAPICursor,
    queue_name: str,
    partition_interval: int = 10000,
    retention_interval: int = 100000,
) -> None:
    """

    .. code-block:: python

        from pgmq_sqlalchemy import async_func as pgmq_func
        await pgmq_func.create_partitioned_queue(db_cursor, 'my_partitioned_queue', partition_interval=10000, retention_interval=100000)

    # ... (rest of the docstring remains the same) ...
    """
    sql, params = _statement.create_partitioned_queue(
        queue_name, partition_interval, retention_interval
    )
    await db_cursor.execute(sql, params)


async def validate_queue_name(db_cursor: AsyncDBAPICursor, queue_name: str) -> None:
    """
    * Will raise an error if the ``queue_name`` is more than 48 characters.

    .. code-block:: python

        from pgmq_sqlalchemy import async_func as pgmq_func
        await pgmq_func.validate_queue_name(db_cursor, 'my_queue')
    """
    sql, params = _statement.validate_queue_name(queue_name)
    await db_cursor.execute(sql, params)


async def drop_queue(
    db_cursor: AsyncDBAPICursor, queue: str, partitioned: bool = False
) -> bool:
    """

    .. code-block:: python

        from pgmq_sqlalchemy import async_func as pgmq_func
        await pgmq_func.drop_queue(db_cursor, 'my_queue')
        # for partitioned queue
        await pgmq_func.drop_queue(db_cursor, 'my_partitioned_queue', partitioned=True)

    """
    sql, params = _statement.drop_queue(queue, partitioned)
    await db_cursor.execute(sql, params)


async def list_queues(db_cursor: AsyncDBAPICursor) -> List[str]:
    """

    .. code-block:: python

        from pgmq_sqlalchemy import async_func as pgmq_func
        queue_list = await pgmq_func.list_queues(db_cursor)
        print(queue_list)

    """
    sql, params = _statement.list_queues()
    await db_cursor.execute(sql, params)


async def send(
    db_cursor: AsyncDBAPICursor, queue_name: str, message: dict, delay: int = 0
) -> int:
    """

    .. code-block:: python

        from pgmq_sqlalchemy import async_func as pgmq_func
        msg_id = await pgmq_func.send(db_cursor, 'my_queue', {'key': 'value', 'key2': 'value2'})
        print(msg_id)

    """
    sql, params = _statement.send(queue_name, message, delay)
    await db_cursor.execute(sql, params)


async def send_batch(
    db_cursor: AsyncDBAPICursor, queue_name: str, messages: List[dict], delay: int = 0
) -> List[int]:
    """

    .. code-block:: python

        from pgmq_sqlalchemy import async_func as pgmq_func
        msgs = [{'key': 'value', 'key2': 'value2'}, {'key': 'value', 'key2': 'value2'}]
        msg_ids = await pgmq_func.send_batch(db_cursor, 'my_queue', msgs)
        print(msg_ids)
        # send with delay
        msg_ids = await pgmq_func.send_batch(db_cursor, 'my_queue', msgs, delay=10)
    """
    sql, params = _statement.send_batch(queue_name, messages, delay)
    await db_cursor.execute(sql, params)


async def read(
    db_cursor: AsyncDBAPICursor, queue_name: str, vt: Optional[int] = None
) -> Optional[Message]:
    """

    .. code-block:: python

        from pgmq_sqlalchemy import async_func as pgmq_func
        msg = await pgmq_func.read(db_cursor, 'my_queue')
        if msg:
            print(msg.msg_id)
            print(msg.message)
    """
    sql, params = _statement.read(queue_name, vt)
    await db_cursor.execute(sql, params)


async def read_batch(
    db_cursor: AsyncDBAPICursor,
    queue_name: str,
    batch_size: int = 1,
    vt: Optional[int] = None,
) -> Optional[List[Message]]:
    """

    .. code-block:: python

        from pgmq_sqlalchemy import async_func as pgmq_func
        msgs = await pgmq_func.read_batch(db_cursor, 'my_queue', batch_size=10)
        if msgs:
            for msg in msgs:
                print(msg.msg_id)
                print(msg.message)

    """
    sql, params = _statement.read_batch(queue_name, vt, batch_size)
    await db_cursor.execute(sql, params)


async def read_with_poll(
    db_cursor: AsyncDBAPICursor,
    queue_name: str,
    vt: Optional[int] = None,
    qty: int = 1,
    max_poll_seconds: int = 5,
    poll_interval_ms: int = 100,
) -> Optional[List[Message]]:
    """

    Usage:

    .. code-block:: python

        msg_id = pgmq_client.send('my_queue', {'key': 'value'}, delay=6)

        # the following code will block for 5 seconds
        msgs = pgmq_client.read_with_poll('my_queue', qty=1, max_poll_seconds=5, poll_interval_ms=100)
        assert msgs is None

        # try read_with_poll again
        # the following code will only block for 1 second
        msgs = pgmq_client.read_with_poll('my_queue', qty=1, max_poll_seconds=5, poll_interval_ms=100)
        assert msgs is not None

    Another example:

    .. code-block:: python

        msg = {'key': 'value'}
        msg_ids = pgmq_client.send_batch('my_queue', [msg, msg, msg, msg], delay=3)

        # the following code will block for 3 seconds
        msgs = pgmq_client.read_with_poll('my_queue', qty=3, max_poll_seconds=5, poll_interval_ms=100)
        assert len(msgs) == 3 # will read at most 3 messages (qty=3)

    """
    sql, params = _statement.read_with_poll(
        queue_name, vt, qty, max_poll_seconds, poll_interval_ms
    )
    await db_cursor.execute(sql, params)


async def pop(db_cursor: AsyncDBAPICursor, queue_name: str) -> Optional[Message]:
    """

    .. code-block:: python

        from pgmq_sqlalchemy import async_func as pgmq_func
        msg = await pgmq_func.pop(db_cursor, 'my_queue')
        if msg:
            print(msg.msg_id)
            print(msg.message)
    """
    sql, params = _statement.pop(queue_name)
    await db_cursor.execute(sql, params)


async def delete(db_cursor: AsyncDBAPICursor, queue_name: str, msg_id: int) -> bool:
    """

    .. code-block:: python

            msg_id = pgmq_client.send('my_queue', {'key': 'value'})
            assert pgmq_client.delete('my_queue', msg_id)
            assert not pgmq_client.delete('my_queue', msg_id)

    """
    sql, params = _statement.delete(queue_name, msg_id)
    await db_cursor.execute(sql, params)


async def delete_batch(
    db_cursor: AsyncDBAPICursor, queue_name: str, msg_ids: List[int]
) -> List[int]:
    """
    .. code-block:: python

        msg_ids = pgmq_client.send_batch('my_queue', [{'key': 'value'}, {'key': 'value'}])
        assert pgmq_client.delete_batch('my_queue', msg_ids) == msg_ids
    """
    sql, params = _statement.delete_batch(queue_name, msg_ids)
    await db_cursor.execute(sql, params)


async def archive(db_cursor: AsyncDBAPICursor, queue_name: str, msg_id: int) -> bool:
    """
    Archive a message from a queue.

    Returns:
        bool: ``True`` if the message is archived successfully, ``False`` if the message does not exist.
    """
    sql, params = _statement.archive(queue_name, msg_id)
    await db_cursor.execute(sql, params)


async def archive_batch(
    db_cursor: AsyncDBAPICursor, queue_name: str, msg_ids: List[int]
) -> List[int]:
    """
    Archive a batch of messages from a queue.

    Returns:
        List of int: The list of message IDs that are successfully archived.
    """
    sql, params = _statement.archive_batch(queue_name, msg_ids)
    await db_cursor.execute(sql, params)


async def purge(db_cursor: AsyncDBAPICursor, queue_name: str) -> int:
    """
    Purge a queue.

    Returns:
        int: The number of messages purged.
    """
    sql, params = _statement.purge(queue_name)
    await db_cursor.execute(sql, params)


async def metrics(
    db_cursor: AsyncDBAPICursor, queue_name: str
) -> Optional[QueueMetrics]:
    """
    Get metrics for a queue.

    Returns:
        QueueMetrics or ``None`` if the queue does not exist.
    """
    sql, params = _statement.metrics(queue_name)
    await db_cursor.execute(sql, params)


async def metrics_all(db_cursor: AsyncDBAPICursor) -> Optional[List[QueueMetrics]]:
    """
    Get metrics for all queues.

    Returns:
        List of QueueMetrics or ``None`` if there are no queues.

    Usage:

    .. code-block:: python

        from pgmq_sqlalchemy import async_func as pgmq_func
        from pgmq_sqlalchemy.schema import QueueMetrics

        metrics: List[QueueMetrics] = await pgmq_func.metrics_all(db_cursor)
        if metrics:
            for m in metrics:
                print(m.queue_name)
                print(m.queue_length)
                print(m.total_messages)

    # ... (rest of the docstring remains the same) ...
    """
    sql, params = _statement.metrics_all()
    await db_cursor.execute(sql, params)
