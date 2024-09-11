"""
async functions
"""

from typing import List, Optional

from ._types import AsyncDBAPICursor
from . import func
from .schema import Message, QueueMetrics


async def create_queue(
    db_cursor: AsyncDBAPICursor, queue_name: str, unlogged: bool = False
) -> None:
    """
    .. _unlogged_table: https://www.postgresql.org/docs/current/sql-createtable.html#SQL-CREATETABLE-UNLOGGED
    .. |unlogged_table| replace:: **UNLOGGED TABLE**

    **Create a new queue.**

    * if ``unlogged`` is ``True``, the queue will be created as an |unlogged_table|_ .
    * ``queue_name`` must be **less than 48 characters**.

        .. code-block:: python

            from pgmq_sqlalchemy import async_func as pgmq_func
            await pgmq_func.create_queue(db_cursor, 'my_queue')
            # or unlogged table queue
            await pgmq_func.create_queue(db_cursor, 'my_queue', unlogged=True)
    """
    await func.create_queue(db_cursor, queue_name, unlogged)


async def create_partitioned_queue(
    db_cursor: AsyncDBAPICursor,
    queue_name: str,
    partition_interval: int = 10000,
    retention_interval: int = 100000,
) -> None:
    """
    Create a new **partitioned** queue.

    .. code-block:: python

        from pgmq_sqlalchemy import async_func as pgmq_func
        await pgmq_func.create_partitioned_queue(db_cursor, 'my_partitioned_queue', partition_interval=10000, retention_interval=100000)

    # ... (rest of the docstring remains the same) ...
    """
    await func.create_partitioned_queue(
        db_cursor, queue_name, partition_interval, retention_interval
    )


async def validate_queue_name(db_cursor: AsyncDBAPICursor, queue_name: str) -> None:
    """
    * Will raise an error if the ``queue_name`` is more than 48 characters.

    .. code-block:: python

        from pgmq_sqlalchemy import async_func as pgmq_func
        await pgmq_func.validate_queue_name(db_cursor, 'my_queue')
    """
    await func.validate_queue_name(db_cursor, queue_name)


async def drop_queue(
    db_cursor: AsyncDBAPICursor, queue: str, partitioned: bool = False
) -> bool:
    """
    Drop a queue.

    .. code-block:: python

        from pgmq_sqlalchemy import async_func as pgmq_func
        await pgmq_func.drop_queue(db_cursor, 'my_queue')
        # for partitioned queue
        await pgmq_func.drop_queue(db_cursor, 'my_partitioned_queue', partitioned=True)

    # ... (rest of the docstring remains the same) ...
    """
    return await func.drop_queue(db_cursor, queue, partitioned)


async def list_queues(db_cursor: AsyncDBAPICursor) -> List[str]:
    """
    List all queues.

    .. code-block:: python

        from pgmq_sqlalchemy import async_func as pgmq_func
        queue_list = await pgmq_func.list_queues(db_cursor)
        print(queue_list)
    """
    return await func.list_queues(db_cursor)


async def send(
    db_cursor: AsyncDBAPICursor, queue_name: str, message: dict, delay: int = 0
) -> int:
    """
    Send a message to a queue.

    .. code-block:: python

        from pgmq_sqlalchemy import async_func as pgmq_func
        msg_id = await pgmq_func.send(db_cursor, 'my_queue', {'key': 'value', 'key2': 'value2'})
        print(msg_id)

    # ... (rest of the docstring remains the same) ...
    """
    return await func.send(db_cursor, queue_name, message, delay)


async def send_batch(
    db_cursor: AsyncDBAPICursor, queue_name: str, messages: List[dict], delay: int = 0
) -> List[int]:
    """
    Send a batch of messages to a queue.

    .. code-block:: python

        from pgmq_sqlalchemy import async_func as pgmq_func
        msgs = [{'key': 'value', 'key2': 'value2'}, {'key': 'value', 'key2': 'value2'}]
        msg_ids = await pgmq_func.send_batch(db_cursor, 'my_queue', msgs)
        print(msg_ids)
        # send with delay
        msg_ids = await pgmq_func.send_batch(db_cursor, 'my_queue', msgs, delay=10)
    """
    return await func.send_batch(db_cursor, queue_name, messages, delay)


async def read(
    db_cursor: AsyncDBAPICursor, queue_name: str, vt: Optional[int] = None
) -> Optional[Message]:
    """
    Read a message from the queue.

    Returns:
        Message or ``None`` if the queue is empty.
    """
    return await func.read(db_cursor, queue_name, vt)


async def read_batch(
    db_cursor: AsyncDBAPICursor,
    queue_name: str,
    batch_size: int = 1,
    vt: Optional[int] = None,
) -> Optional[List[Message]]:
    """
    Read a batch of messages from the queue.

    Returns:
        List of Message or ``None`` if the queue is empty.
    """
    return await func.read_batch(db_cursor, queue_name, vt, batch_size)


async def read_with_poll(
    db_cursor: AsyncDBAPICursor,
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
    """
    return await func.read_with_poll(
        db_cursor, queue_name, vt, qty, max_poll_seconds, poll_interval_ms
    )


async def pop(db_cursor: AsyncDBAPICursor, queue_name: str) -> Optional[Message]:
    """
    Reads a single message from a queue and deletes it upon read.

    .. code-block:: python

        from pgmq_sqlalchemy import async_func as pgmq_func
        msg = await pgmq_func.pop(db_cursor, 'my_queue')
        if msg:
            print(msg.msg_id)
            print(msg.message)
    """
    return await func.pop(db_cursor, queue_name)


async def delete(db_cursor: AsyncDBAPICursor, queue_name: str, msg_id: int) -> bool:
    """
    Delete a message from the queue.

    Returns:
        bool: ``True`` if the message is deleted successfully, ``False`` if the message does not exist.
    """
    return await func.delete(db_cursor, queue_name, msg_id)


async def delete_batch(
    db_cursor: AsyncDBAPICursor, queue_name: str, msg_ids: List[int]
) -> List[int]:
    """
    Delete a batch of messages from the queue.

    Returns:
        List of int: The list of message IDs that are successfully deleted.
    """
    return await func.delete_batch(db_cursor, queue_name, msg_ids)


async def archive(db_cursor: AsyncDBAPICursor, queue_name: str, msg_id: int) -> bool:
    """
    Archive a message from a queue.

    Returns:
        bool: ``True`` if the message is archived successfully, ``False`` if the message does not exist.
    """
    return await func.archive(db_cursor, queue_name, msg_id)


async def archive_batch(
    db_cursor: AsyncDBAPICursor, queue_name: str, msg_ids: List[int]
) -> List[int]:
    """
    Archive a batch of messages from a queue.

    Returns:
        List of int: The list of message IDs that are successfully archived.
    """
    return await func.archive_batch(db_cursor, queue_name, msg_ids)


async def purge(db_cursor: AsyncDBAPICursor, queue_name: str) -> int:
    """
    Purge a queue.

    Returns:
        int: The number of messages purged.
    """
    return await func.purge(db_cursor, queue_name)


async def metrics(
    db_cursor: AsyncDBAPICursor, queue_name: str
) -> Optional[QueueMetrics]:
    """
    Get metrics for a queue.

    Returns:
        QueueMetrics or ``None`` if the queue does not exist.
    """
    return await func.metrics(db_cursor, queue_name)


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
    return await func.metrics_all(db_cursor)
