"""Tests for PGMQOperation class.

This test suite tests the PGMQOperation class methods directly,
which are transaction-friendly static methods that accept sessions.
"""

import time
import uuid

import pytest
from sqlalchemy.exc import ProgrammingError, InternalError

from pgmq_sqlalchemy.operation import PGMQOperation
from pgmq_sqlalchemy.schema import QueueMetrics
from tests._utils import check_queue_exists
from tests.constant import MSG


# Sync tests


def test_check_pgmq_ext_sync(get_session_maker):
    """Test that pgmq extension check works."""
    with get_session_maker() as session:
        # Should not raise any exception
        PGMQOperation.check_pgmq_ext(session=session, commit=True)


def test_create_queue_sync(get_session_maker, db_session):
    """Test creating a queue using PGMQOperation."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    with get_session_maker() as session:
        PGMQOperation.create_queue(
            queue_name, unlogged=False, session=session, commit=True
        )

    assert check_queue_exists(db_session, queue_name) is True

    # Clean up
    with get_session_maker() as session:
        PGMQOperation.drop_queue(
            queue_name, partitioned=False, session=session, commit=True
        )


def test_create_unlogged_queue_sync(get_session_maker, db_session):
    """Test creating an unlogged queue using PGMQOperation."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    with get_session_maker() as session:
        PGMQOperation.create_queue(
            queue_name, unlogged=True, session=session, commit=True
        )

    assert check_queue_exists(db_session, queue_name) is True

    # Clean up
    with get_session_maker() as session:
        PGMQOperation.drop_queue(
            queue_name, partitioned=False, session=session, commit=True
        )


@pytest.mark.asyncio
async def test_create_unlogged_queue_async(get_async_session_maker, db_session):
    """Test creating an unlogged queue using PGMQOperation asynchronously."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    async with get_async_session_maker() as session:
        await PGMQOperation.create_queue_async(
            queue_name, unlogged=True, session=session, commit=True
        )

    assert check_queue_exists(db_session, queue_name) is True

    # Clean up
    async with get_async_session_maker() as session:
        await PGMQOperation.drop_queue_async(
            queue_name, partitioned=False, session=session, commit=True
        )


def test_validate_queue_name_sync(get_session_maker):
    """Test queue name validation."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    with get_session_maker() as session:
        # Should not raise for valid name
        PGMQOperation.validate_queue_name(queue_name, session=session, commit=True)

        # Should raise for name that's too long (either ProgrammingError or InternalError depending on driver)
        with pytest.raises((ProgrammingError, InternalError, Exception)) as e:
            PGMQOperation.validate_queue_name("a" * 49, session=session, commit=True)
        error_msg = str(e.value.orig) if hasattr(e.value, "orig") else str(e.value)
        assert "queue name is too long" in error_msg


@pytest.mark.asyncio
async def test_validate_queue_name_async(get_async_session_maker):
    """Test queue name validation asynchronously."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    async with get_async_session_maker() as session:
        # Should not raise for valid name
        await PGMQOperation.validate_queue_name_async(
            queue_name, session=session, commit=True
        )

        # Should raise for name that's too long (either ProgrammingError or InternalError depending on driver)
        with pytest.raises((ProgrammingError, InternalError, Exception)) as e:
            await PGMQOperation.validate_queue_name_async(
                "a" * 49, session=session, commit=True
            )
        error_msg = str(e.value.orig) if hasattr(e.value, "orig") else str(e.value)
        assert "queue name is too long" in error_msg


def test_list_queues_sync(get_session_maker, db_session):
    """Test listing queues."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    # Create a queue
    with get_session_maker() as session:
        PGMQOperation.create_queue(
            queue_name, unlogged=False, session=session, commit=True
        )

    # List queues
    with get_session_maker() as session:
        queues = PGMQOperation.list_queues(session=session, commit=True)

    assert queue_name in queues

    # Clean up
    with get_session_maker() as session:
        PGMQOperation.drop_queue(
            queue_name, partitioned=False, session=session, commit=True
        )


@pytest.mark.asyncio
async def test_list_queues_async(get_async_session_maker, db_session):
    """Test listing queues asynchronously."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    # Create a queue
    async with get_async_session_maker() as session:
        await PGMQOperation.create_queue_async(
            queue_name, unlogged=False, session=session, commit=True
        )

    # List queues
    async with get_async_session_maker() as session:
        queues = await PGMQOperation.list_queues_async(session=session, commit=True)

    assert queue_name in queues

    # Clean up
    async with get_async_session_maker() as session:
        await PGMQOperation.drop_queue_async(
            queue_name, partitioned=False, session=session, commit=True
        )


def test_send_and_read_sync(get_session_maker, db_session):
    """Test sending and reading messages."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    # Create queue
    with get_session_maker() as session:
        PGMQOperation.create_queue(
            queue_name, unlogged=False, session=session, commit=True
        )

    # Send a message
    with get_session_maker() as session:
        msg_id = PGMQOperation.send(
            queue_name, MSG, delay=0, session=session, commit=True
        )

    assert msg_id > 0

    # Read the message
    with get_session_maker() as session:
        msg = PGMQOperation.read(queue_name, vt=30, session=session, commit=True)

    assert msg is not None
    assert msg.msg_id == msg_id
    assert msg.message == MSG

    # Clean up
    with get_session_maker() as session:
        PGMQOperation.drop_queue(
            queue_name, partitioned=False, session=session, commit=True
        )


def test_send_batch_sync(get_session_maker, db_session):
    """Test sending a batch of messages."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    messages = [{"key": f"value{i}"} for i in range(5)]

    # Create queue
    with get_session_maker() as session:
        PGMQOperation.create_queue(
            queue_name, unlogged=False, session=session, commit=True
        )

    # Send batch
    with get_session_maker() as session:
        msg_ids = PGMQOperation.send_batch(
            queue_name, messages, delay=0, session=session, commit=True
        )

    assert len(msg_ids) == 5

    # Read batch
    with get_session_maker() as session:
        msgs = PGMQOperation.read_batch(
            queue_name, vt=30, batch_size=5, session=session, commit=True
        )

    assert len(msgs) == 5

    # Clean up
    with get_session_maker() as session:
        PGMQOperation.drop_queue(
            queue_name, partitioned=False, session=session, commit=True
        )


@pytest.mark.asyncio
async def test_send_batch_async(get_async_session_maker, db_session):
    """Test sending a batch of messages asynchronously."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    messages = [{"key": f"value{i}"} for i in range(5)]

    # Create queue
    async with get_async_session_maker() as session:
        await PGMQOperation.create_queue_async(
            queue_name, unlogged=False, session=session, commit=True
        )

    # Send batch
    async with get_async_session_maker() as session:
        msg_ids = await PGMQOperation.send_batch_async(
            queue_name, messages, delay=0, session=session, commit=True
        )

    assert len(msg_ids) == 5

    # Read batch
    async with get_async_session_maker() as session:
        msgs = await PGMQOperation.read_batch_async(
            queue_name, vt=30, batch_size=5, session=session, commit=True
        )

    assert len(msgs) == 5

    # Clean up
    async with get_async_session_maker() as session:
        await PGMQOperation.drop_queue_async(
            queue_name, partitioned=False, session=session, commit=True
        )


def test_pop_sync(get_session_maker, db_session):
    """Test popping a message from the queue."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    # Create queue and send message
    with get_session_maker() as session:
        PGMQOperation.create_queue(
            queue_name, unlogged=False, session=session, commit=True
        )
        msg_id = PGMQOperation.send(
            queue_name, MSG, delay=0, session=session, commit=True
        )

    # Pop message
    with get_session_maker() as session:
        msg = PGMQOperation.pop(queue_name, session=session, commit=True)

    assert msg is not None
    assert msg.msg_id == msg_id

    # Verify queue is empty
    with get_session_maker() as session:
        msg2 = PGMQOperation.pop(queue_name, session=session, commit=True)

    assert msg2 is None

    # Clean up
    with get_session_maker() as session:
        PGMQOperation.drop_queue(
            queue_name, partitioned=False, session=session, commit=True
        )


@pytest.mark.asyncio
async def test_pop_async(get_async_session_maker, db_session):
    """Test popping a message from the queue asynchronously."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    # Create queue and send message
    async with get_async_session_maker() as session:
        await PGMQOperation.create_queue_async(
            queue_name, unlogged=False, session=session, commit=True
        )
        msg_id = await PGMQOperation.send_async(
            queue_name, MSG, delay=0, session=session, commit=True
        )

    # Pop message
    async with get_async_session_maker() as session:
        msg = await PGMQOperation.pop_async(queue_name, session=session, commit=True)

    assert msg is not None
    assert msg.msg_id == msg_id

    # Verify queue is empty
    async with get_async_session_maker() as session:
        msg2 = await PGMQOperation.pop_async(queue_name, session=session, commit=True)

    assert msg2 is None

    # Clean up
    async with get_async_session_maker() as session:
        await PGMQOperation.drop_queue_async(
            queue_name, partitioned=False, session=session, commit=True
        )


def test_delete_sync(get_session_maker, db_session):
    """Test deleting a message."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    # Create queue and send message
    with get_session_maker() as session:
        PGMQOperation.create_queue(
            queue_name, unlogged=False, session=session, commit=True
        )
        msg_id = PGMQOperation.send(
            queue_name, MSG, delay=0, session=session, commit=True
        )

    # Delete message
    with get_session_maker() as session:
        deleted = PGMQOperation.delete(queue_name, msg_id, session=session, commit=True)

    assert deleted is True

    # Clean up
    with get_session_maker() as session:
        PGMQOperation.drop_queue(
            queue_name, partitioned=False, session=session, commit=True
        )


@pytest.mark.asyncio
async def test_delete_async(get_async_session_maker, db_session):
    """Test deleting a message asynchronously."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    # Create queue and send message
    async with get_async_session_maker() as session:
        await PGMQOperation.create_queue_async(
            queue_name, unlogged=False, session=session, commit=True
        )
        msg_id = await PGMQOperation.send_async(
            queue_name, MSG, delay=0, session=session, commit=True
        )

    # Delete message
    async with get_async_session_maker() as session:
        deleted = await PGMQOperation.delete_async(
            queue_name, msg_id, session=session, commit=True
        )

    assert deleted is True

    # Clean up
    async with get_async_session_maker() as session:
        await PGMQOperation.drop_queue_async(
            queue_name, partitioned=False, session=session, commit=True
        )


def test_set_vt_sync(get_session_maker, db_session):
    """Test setting visibility timeout."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    # Create queue and send message
    with get_session_maker() as session:
        PGMQOperation.create_queue(
            queue_name, unlogged=False, session=session, commit=True
        )
        msg_id = PGMQOperation.send(
            queue_name, MSG, delay=0, session=session, commit=True
        )
        # Read message to set initial vt
        PGMQOperation.read(queue_name, vt=5, session=session, commit=True)

    # Set new vt
    with get_session_maker() as session:
        msg = PGMQOperation.set_vt(
            queue_name, msg_id, vt=60, session=session, commit=True
        )

    assert msg is not None

    # Clean up
    with get_session_maker() as session:
        PGMQOperation.drop_queue(
            queue_name, partitioned=False, session=session, commit=True
        )


@pytest.mark.asyncio
async def test_set_vt_async(get_async_session_maker, db_session):
    """Test setting visibility timeout asynchronously."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    # Create queue and send message
    async with get_async_session_maker() as session:
        await PGMQOperation.create_queue_async(
            queue_name, unlogged=False, session=session, commit=True
        )
        msg_id = await PGMQOperation.send_async(
            queue_name, MSG, delay=0, session=session, commit=True
        )
        # Read message to set initial vt
        await PGMQOperation.read_async(queue_name, vt=5, session=session, commit=True)

    # Set new vt
    async with get_async_session_maker() as session:
        msg = await PGMQOperation.set_vt_async(
            queue_name, msg_id, vt=60, session=session, commit=True
        )

    assert msg is not None

    # Clean up
    async with get_async_session_maker() as session:
        await PGMQOperation.drop_queue_async(
            queue_name, partitioned=False, session=session, commit=True
        )


def test_archive_sync(get_session_maker, db_session):
    """Test archiving a message."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    # Create queue and send message
    with get_session_maker() as session:
        PGMQOperation.create_queue(
            queue_name, unlogged=False, session=session, commit=True
        )
        msg_id = PGMQOperation.send(
            queue_name, MSG, delay=0, session=session, commit=True
        )

    # Archive message
    with get_session_maker() as session:
        archived = PGMQOperation.archive(
            queue_name, msg_id, session=session, commit=True
        )

    assert archived is True

    # Clean up
    with get_session_maker() as session:
        PGMQOperation.drop_queue(
            queue_name, partitioned=False, session=session, commit=True
        )


@pytest.mark.asyncio
async def test_archive_async(get_async_session_maker, db_session):
    """Test archiving a message asynchronously."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    # Create queue and send message
    async with get_async_session_maker() as session:
        await PGMQOperation.create_queue_async(
            queue_name, unlogged=False, session=session, commit=True
        )
        msg_id = await PGMQOperation.send_async(
            queue_name, MSG, delay=0, session=session, commit=True
        )

    # Archive message
    async with get_async_session_maker() as session:
        archived = await PGMQOperation.archive_async(
            queue_name, msg_id, session=session, commit=True
        )

    assert archived is True

    # Clean up
    async with get_async_session_maker() as session:
        await PGMQOperation.drop_queue_async(
            queue_name, partitioned=False, session=session, commit=True
        )


def test_metrics_sync(get_session_maker, db_session):
    """Test getting queue metrics."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    # Create queue
    with get_session_maker() as session:
        PGMQOperation.create_queue(
            queue_name, unlogged=False, session=session, commit=True
        )

    # Get metrics for empty queue
    with get_session_maker() as session:
        metrics = PGMQOperation.metrics(queue_name, session=session, commit=True)

    assert metrics is not None
    assert isinstance(metrics, QueueMetrics)
    assert metrics.queue_name == queue_name
    assert metrics.queue_length == 0
    assert metrics.total_messages == 0

    # Send some messages
    with get_session_maker() as session:
        for i in range(3):
            PGMQOperation.send(
                queue_name, {"index": i}, delay=0, session=session, commit=True
            )

    # Get metrics after adding messages
    with get_session_maker() as session:
        metrics = PGMQOperation.metrics(queue_name, session=session, commit=True)

    assert metrics.queue_length == 3
    assert metrics.total_messages == 3

    # Clean up
    with get_session_maker() as session:
        PGMQOperation.drop_queue(
            queue_name, partitioned=False, session=session, commit=True
        )


def test_metrics_all_sync(get_session_maker, db_session):
    """Test getting metrics for all queues."""
    queue_name1 = f"test_queue_{uuid.uuid4().hex}"
    queue_name2 = f"test_queue_{uuid.uuid4().hex}"

    # Create two queues
    with get_session_maker() as session:
        PGMQOperation.create_queue(
            queue_name1, unlogged=False, session=session, commit=True
        )
        PGMQOperation.create_queue(
            queue_name2, unlogged=False, session=session, commit=True
        )

    # Get metrics for all queues
    with get_session_maker() as session:
        all_metrics = PGMQOperation.metrics_all(session=session, commit=True)

    assert all_metrics is not None
    assert len(all_metrics) >= 2
    queue_names = [m.queue_name for m in all_metrics]
    assert queue_name1 in queue_names
    assert queue_name2 in queue_names

    # Clean up
    with get_session_maker() as session:
        PGMQOperation.drop_queue(
            queue_name1, partitioned=False, session=session, commit=True
        )
        PGMQOperation.drop_queue(
            queue_name2, partitioned=False, session=session, commit=True
        )


@pytest.mark.asyncio
async def test_metrics_all_async(get_async_session_maker, db_session):
    """Test getting metrics for all queues asynchronously."""
    queue_name1 = f"test_queue_{uuid.uuid4().hex}"
    queue_name2 = f"test_queue_{uuid.uuid4().hex}"

    # Create two queues
    async with get_async_session_maker() as session:
        await PGMQOperation.create_queue_async(
            queue_name1, unlogged=False, session=session, commit=True
        )
        await PGMQOperation.create_queue_async(
            queue_name2, unlogged=False, session=session, commit=True
        )

    # Get metrics for all queues
    async with get_async_session_maker() as session:
        all_metrics = await PGMQOperation.metrics_all_async(
            session=session, commit=True
        )

    assert all_metrics is not None
    assert len(all_metrics) >= 2
    queue_names = [m.queue_name for m in all_metrics]
    assert queue_name1 in queue_names
    assert queue_name2 in queue_names

    # Clean up
    async with get_async_session_maker() as session:
        await PGMQOperation.drop_queue_async(
            queue_name1, partitioned=False, session=session, commit=True
        )
        await PGMQOperation.drop_queue_async(
            queue_name2, partitioned=False, session=session, commit=True
        )


def test_transaction_rollback_sync(get_session_maker, db_session):
    """Test that operations can be rolled back when commit=False."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    # Create queue with commit=False, then rollback
    with get_session_maker() as session:
        PGMQOperation.create_queue(
            queue_name, unlogged=False, session=session, commit=False
        )
        session.rollback()

    # Queue should not exist
    assert check_queue_exists(db_session, queue_name) is False


@pytest.mark.asyncio
async def test_transaction_rollback_async(get_async_session_maker, db_session):
    """Test that operations can be rolled back when commit=False asynchronously."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    # Create queue with commit=False, then rollback
    async with get_async_session_maker() as session:
        await PGMQOperation.create_queue_async(
            queue_name, unlogged=False, session=session, commit=False
        )
        await session.rollback()

    # Queue should not exist
    assert check_queue_exists(db_session, queue_name) is False


def test_transaction_commit_sync(get_session_maker, db_session):
    """Test that operations are committed when commit=True."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    # Create queue with commit=True
    with get_session_maker() as session:
        PGMQOperation.create_queue(
            queue_name, unlogged=False, session=session, commit=True
        )

    # Queue should exist
    assert check_queue_exists(db_session, queue_name) is True

    # Clean up
    with get_session_maker() as session:
        PGMQOperation.drop_queue(
            queue_name, partitioned=False, session=session, commit=True
        )


@pytest.mark.asyncio
async def test_transaction_commit_async(get_async_session_maker, db_session):
    """Test that operations are committed when commit=True asynchronously."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    # Create queue with commit=True
    async with get_async_session_maker() as session:
        await PGMQOperation.create_queue_async(
            queue_name, unlogged=False, session=session, commit=True
        )

    # Queue should exist
    assert check_queue_exists(db_session, queue_name) is True

    # Clean up
    async with get_async_session_maker() as session:
        await PGMQOperation.drop_queue_async(
            queue_name, partitioned=False, session=session, commit=True
        )


# Async tests


@pytest.mark.asyncio
async def test_check_pgmq_ext_async(get_async_session_maker):
    """Test that pgmq extension check works asynchronously."""
    async with get_async_session_maker() as session:
        # Should not raise any exception
        await PGMQOperation.check_pgmq_ext_async(session=session, commit=True)


@pytest.mark.asyncio
async def test_create_queue_async(get_async_session_maker, db_session):
    """Test creating a queue using PGMQOperation asynchronously."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    async with get_async_session_maker() as session:
        await PGMQOperation.create_queue_async(
            queue_name, unlogged=False, session=session, commit=True
        )

    assert check_queue_exists(db_session, queue_name) is True

    # Clean up
    async with get_async_session_maker() as session:
        await PGMQOperation.drop_queue_async(
            queue_name, partitioned=False, session=session, commit=True
        )


@pytest.mark.asyncio
async def test_send_and_read_async(get_async_session_maker, db_session):
    """Test sending and reading messages asynchronously."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    # Create queue
    async with get_async_session_maker() as session:
        await PGMQOperation.create_queue_async(
            queue_name, unlogged=False, session=session, commit=True
        )

    # Send a message
    async with get_async_session_maker() as session:
        msg_id = await PGMQOperation.send_async(
            queue_name, MSG, delay=0, session=session, commit=True
        )

    assert msg_id > 0

    # Read the message
    async with get_async_session_maker() as session:
        msg = await PGMQOperation.read_async(
            queue_name, vt=30, session=session, commit=True
        )

    assert msg is not None
    assert msg.msg_id == msg_id
    assert msg.message == MSG

    # Clean up
    async with get_async_session_maker() as session:
        await PGMQOperation.drop_queue_async(
            queue_name, partitioned=False, session=session, commit=True
        )


@pytest.mark.asyncio
async def test_metrics_async(get_async_session_maker, db_session):
    """Test getting queue metrics asynchronously."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    # Create queue
    async with get_async_session_maker() as session:
        await PGMQOperation.create_queue_async(
            queue_name, unlogged=False, session=session, commit=True
        )

    # Get metrics
    async with get_async_session_maker() as session:
        metrics = await PGMQOperation.metrics_async(
            queue_name, session=session, commit=True
        )

    assert metrics is not None
    assert isinstance(metrics, QueueMetrics)
    assert metrics.queue_name == queue_name
    assert metrics.queue_length == 0

    # Clean up
    async with get_async_session_maker() as session:
        await PGMQOperation.drop_queue_async(
            queue_name, partitioned=False, session=session, commit=True
        )


def test_delete_batch_sync(get_session_maker, db_session):
    """Test deleting a batch of messages."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    # Create queue and send messages
    with get_session_maker() as session:
        PGMQOperation.create_queue(
            queue_name, unlogged=False, session=session, commit=True
        )
        msg_id1 = PGMQOperation.send(
            queue_name, MSG, delay=0, session=session, commit=True
        )
        msg_id2 = PGMQOperation.send(
            queue_name, MSG, delay=0, session=session, commit=True
        )
        msg_id3 = PGMQOperation.send(
            queue_name, MSG, delay=0, session=session, commit=True
        )
        msg_ids = [msg_id1, msg_id2, msg_id3]

    # Delete batch
    with get_session_maker() as session:
        deleted_ids = PGMQOperation.delete_batch(
            queue_name, msg_ids, session=session, commit=True
        )

    assert len(deleted_ids) == 3
    assert set(deleted_ids) == set(msg_ids)

    # Verify messages are deleted
    with get_session_maker() as session:
        msg = PGMQOperation.read(queue_name, vt=30, session=session, commit=True)

    assert msg is None

    # Clean up
    with get_session_maker() as session:
        PGMQOperation.drop_queue(
            queue_name, partitioned=False, session=session, commit=True
        )


def test_archive_batch_sync(get_session_maker, db_session):
    """Test archiving a batch of messages."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    # Create queue and send messages
    with get_session_maker() as session:
        PGMQOperation.create_queue(
            queue_name, unlogged=False, session=session, commit=True
        )
        msg_id1 = PGMQOperation.send(
            queue_name, MSG, delay=0, session=session, commit=True
        )
        msg_id2 = PGMQOperation.send(
            queue_name, MSG, delay=0, session=session, commit=True
        )
        msg_id3 = PGMQOperation.send(
            queue_name, MSG, delay=0, session=session, commit=True
        )
        msg_ids = [msg_id1, msg_id2, msg_id3]

    # Archive batch
    with get_session_maker() as session:
        archived_ids = PGMQOperation.archive_batch(
            queue_name, msg_ids, session=session, commit=True
        )

    assert len(archived_ids) == 3
    assert set(archived_ids) == set(msg_ids)

    # Verify messages are archived (queue should be empty)
    with get_session_maker() as session:
        msg = PGMQOperation.read(queue_name, vt=30, session=session, commit=True)

    assert msg is None

    # Clean up
    with get_session_maker() as session:
        PGMQOperation.drop_queue(
            queue_name, partitioned=False, session=session, commit=True
        )


def test_purge_sync(get_session_maker, db_session):
    """Test purging all messages from a queue."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    # Create queue and send messages
    with get_session_maker() as session:
        PGMQOperation.create_queue(
            queue_name, unlogged=False, session=session, commit=True
        )
        PGMQOperation.send(queue_name, MSG, delay=0, session=session, commit=True)
        PGMQOperation.send(queue_name, MSG, delay=0, session=session, commit=True)
        PGMQOperation.send(queue_name, MSG, delay=0, session=session, commit=True)
        PGMQOperation.send(queue_name, MSG, delay=0, session=session, commit=True)
        PGMQOperation.send(queue_name, MSG, delay=0, session=session, commit=True)

    # Purge queue
    with get_session_maker() as session:
        purged_count = PGMQOperation.purge(queue_name, session=session, commit=True)

    assert purged_count == 5

    # Verify queue is empty
    with get_session_maker() as session:
        msg = PGMQOperation.read(queue_name, vt=30, session=session, commit=True)

    assert msg is None

    # Clean up
    with get_session_maker() as session:
        PGMQOperation.drop_queue(
            queue_name, partitioned=False, session=session, commit=True
        )


def test_read_with_poll_sync(get_session_maker, db_session):
    """Test reading messages with polling."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    # Create queue
    with get_session_maker() as session:
        PGMQOperation.create_queue(
            queue_name, unlogged=False, session=session, commit=True
        )

    # Test with empty queue - should return None after polling
    start = time.time()
    with get_session_maker() as session:
        msgs = PGMQOperation.read_with_poll(
            queue_name,
            vt=30,
            qty=1,
            max_poll_seconds=2,
            poll_interval_ms=100,
            session=session,
            commit=True,
        )
    elapsed = time.time() - start

    assert msgs is None
    assert elapsed >= 2  # Should have polled for at least 2 seconds

    # Send a message and test immediate read
    with get_session_maker() as session:
        msg_id = PGMQOperation.send(
            queue_name, MSG, delay=0, session=session, commit=True
        )

    with get_session_maker() as session:
        msgs = PGMQOperation.read_with_poll(
            queue_name,
            vt=30,
            qty=1,
            max_poll_seconds=5,
            poll_interval_ms=100,
            session=session,
            commit=True,
        )

    assert msgs is not None
    assert len(msgs) == 1
    assert msgs[0].msg_id == msg_id

    # Clean up
    with get_session_maker() as session:
        PGMQOperation.drop_queue(
            queue_name, partitioned=False, session=session, commit=True
        )


def test_drop_queue_sync(get_session_maker, db_session):
    """Test dropping a queue."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    # Create queue
    with get_session_maker() as session:
        PGMQOperation.create_queue(
            queue_name, unlogged=False, session=session, commit=True
        )

    # Verify queue exists
    assert check_queue_exists(db_session, queue_name) is True

    # Drop queue
    with get_session_maker() as session:
        dropped = PGMQOperation.drop_queue(
            queue_name, partitioned=False, session=session, commit=True
        )

    assert dropped is True

    # Verify queue is dropped
    assert check_queue_exists(db_session, queue_name) is False


def test_check_pg_partman_ext_sync(get_session_maker):
    """Test that pg_partman extension check works."""
    with get_session_maker() as session:
        # Should not raise any exception
        # Note: This will only succeed if pg_partman is installed
        try:
            PGMQOperation.check_pg_partman_ext(session=session, commit=True)
        except Exception as e:
            # If pg_partman is not installed, we expect an error
            # This is acceptable for this test
            pytest.skip(f"pg_partman extension not available: {e}")


def test_create_partitioned_queue_sync(get_session_maker, db_session):
    """Test creating a partitioned queue."""
    queue_name = f"part_{uuid.uuid4().hex[:20]}"

    # First ensure pg_partman extension is available
    try:
        with get_session_maker() as session:
            PGMQOperation.check_pg_partman_ext(session=session, commit=True)
    except Exception as e:
        pytest.skip(f"pg_partman extension not available: {e}")

    # Create partitioned queue with numeric partitioning
    with get_session_maker() as session:
        PGMQOperation.create_partitioned_queue(
            queue_name,
            partition_interval="10000",
            retention_interval="100000",
            session=session,
            commit=True,
        )

    assert check_queue_exists(db_session, queue_name) is True

    # Test sending and reading from partitioned queue
    with get_session_maker() as session:
        msg_id = PGMQOperation.send(
            queue_name, MSG, delay=0, session=session, commit=True
        )
        msg = PGMQOperation.read(queue_name, vt=30, session=session, commit=True)

    assert msg is not None
    assert msg.msg_id == msg_id

    # Clean up
    with get_session_maker() as session:
        PGMQOperation.drop_queue(
            queue_name, partitioned=True, session=session, commit=True
        )


def test_create_time_based_partitioned_queue_sync(get_session_maker, db_session):
    """Test creating a time-based partitioned queue."""
    queue_name = f"time_{uuid.uuid4().hex[:20]}"

    # First ensure pg_partman extension is available
    try:
        with get_session_maker() as session:
            PGMQOperation.check_pg_partman_ext(session=session, commit=True)
    except Exception as e:
        pytest.skip(f"pg_partman extension not available: {e}")

    # Create partitioned queue with time-based partitioning
    with get_session_maker() as session:
        PGMQOperation.create_partitioned_queue(
            queue_name,
            partition_interval="1 day",
            retention_interval="7 days",
            session=session,
            commit=True,
        )

    assert check_queue_exists(db_session, queue_name) is True

    # Test sending and reading from time-based partitioned queue
    with get_session_maker() as session:
        msg_id = PGMQOperation.send(
            queue_name, MSG, delay=0, session=session, commit=True
        )
        msg = PGMQOperation.read(queue_name, vt=30, session=session, commit=True)

    assert msg is not None
    assert msg.msg_id == msg_id

    # Clean up
    with get_session_maker() as session:
        PGMQOperation.drop_queue(
            queue_name, partitioned=True, session=session, commit=True
        )


@pytest.mark.asyncio
async def test_create_time_based_partitioned_queue_async(
    get_async_session_maker, db_session
):
    """Test creating a time-based partitioned queue asynchronously."""
    queue_name = f"time_{uuid.uuid4().hex[:20]}"

    # First ensure pg_partman extension is available
    try:
        async with get_async_session_maker() as session:
            await PGMQOperation.check_pg_partman_ext_async(session=session, commit=True)
    except Exception as e:
        pytest.skip(f"pg_partman extension not available: {e}")

    # Create partitioned queue with time-based partitioning
    async with get_async_session_maker() as session:
        await PGMQOperation.create_partitioned_queue_async(
            queue_name,
            partition_interval="1 day",
            retention_interval="7 days",
            session=session,
            commit=True,
        )

    assert check_queue_exists(db_session, queue_name) is True

    # Test sending and reading from time-based partitioned queue
    async with get_async_session_maker() as session:
        msg_id = await PGMQOperation.send_async(
            queue_name, MSG, delay=0, session=session, commit=True
        )
        msg = await PGMQOperation.read_async(
            queue_name, vt=30, session=session, commit=True
        )

    assert msg is not None
    assert msg.msg_id == msg_id

    # Clean up
    async with get_async_session_maker() as session:
        await PGMQOperation.drop_queue_async(
            queue_name, partitioned=True, session=session, commit=True
        )


# Async tests for newly added coverage


@pytest.mark.asyncio
async def test_delete_batch_async(get_async_session_maker, db_session):
    """Test deleting a batch of messages asynchronously."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    # Create queue and send messages
    async with get_async_session_maker() as session:
        await PGMQOperation.create_queue_async(
            queue_name, unlogged=False, session=session, commit=True
        )
        msg_id1 = await PGMQOperation.send_async(
            queue_name, MSG, delay=0, session=session, commit=True
        )
        msg_id2 = await PGMQOperation.send_async(
            queue_name, MSG, delay=0, session=session, commit=True
        )
        msg_id3 = await PGMQOperation.send_async(
            queue_name, MSG, delay=0, session=session, commit=True
        )
        msg_ids = [msg_id1, msg_id2, msg_id3]

    # Delete batch
    async with get_async_session_maker() as session:
        deleted_ids = await PGMQOperation.delete_batch_async(
            queue_name, msg_ids, session=session, commit=True
        )

    assert len(deleted_ids) == 3
    assert set(deleted_ids) == set(msg_ids)

    # Clean up
    async with get_async_session_maker() as session:
        await PGMQOperation.drop_queue_async(
            queue_name, partitioned=False, session=session, commit=True
        )


@pytest.mark.asyncio
async def test_archive_batch_async(get_async_session_maker, db_session):
    """Test archiving a batch of messages asynchronously."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    # Create queue and send messages
    async with get_async_session_maker() as session:
        await PGMQOperation.create_queue_async(
            queue_name, unlogged=False, session=session, commit=True
        )
        msg_id1 = await PGMQOperation.send_async(
            queue_name, MSG, delay=0, session=session, commit=True
        )
        msg_id2 = await PGMQOperation.send_async(
            queue_name, MSG, delay=0, session=session, commit=True
        )
        msg_id3 = await PGMQOperation.send_async(
            queue_name, MSG, delay=0, session=session, commit=True
        )
        msg_ids = [msg_id1, msg_id2, msg_id3]

    # Archive batch
    async with get_async_session_maker() as session:
        archived_ids = await PGMQOperation.archive_batch_async(
            queue_name, msg_ids, session=session, commit=True
        )

    assert len(archived_ids) == 3
    assert set(archived_ids) == set(msg_ids)

    # Clean up
    async with get_async_session_maker() as session:
        await PGMQOperation.drop_queue_async(
            queue_name, partitioned=False, session=session, commit=True
        )


@pytest.mark.asyncio
async def test_purge_async(get_async_session_maker, db_session):
    """Test purging all messages from a queue asynchronously."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    # Create queue and send messages
    async with get_async_session_maker() as session:
        await PGMQOperation.create_queue_async(
            queue_name, unlogged=False, session=session, commit=True
        )
        await PGMQOperation.send_async(
            queue_name, MSG, delay=0, session=session, commit=True
        )
        await PGMQOperation.send_async(
            queue_name, MSG, delay=0, session=session, commit=True
        )
        await PGMQOperation.send_async(
            queue_name, MSG, delay=0, session=session, commit=True
        )
        await PGMQOperation.send_async(
            queue_name, MSG, delay=0, session=session, commit=True
        )
        await PGMQOperation.send_async(
            queue_name, MSG, delay=0, session=session, commit=True
        )

    # Purge queue
    async with get_async_session_maker() as session:
        purged_count = await PGMQOperation.purge_async(
            queue_name, session=session, commit=True
        )

    assert purged_count == 5

    # Clean up
    async with get_async_session_maker() as session:
        await PGMQOperation.drop_queue_async(
            queue_name, partitioned=False, session=session, commit=True
        )


@pytest.mark.asyncio
async def test_read_with_poll_async(get_async_session_maker, db_session):
    """Test reading messages with polling asynchronously."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    # Create queue
    async with get_async_session_maker() as session:
        await PGMQOperation.create_queue_async(
            queue_name, unlogged=False, session=session, commit=True
        )

    # Test with empty queue - should return None after polling
    start = time.time()
    async with get_async_session_maker() as session:
        msgs = await PGMQOperation.read_with_poll_async(
            queue_name,
            vt=30,
            qty=1,
            max_poll_seconds=2,
            poll_interval_ms=100,
            session=session,
            commit=True,
        )
    elapsed = time.time() - start

    assert msgs is None
    assert elapsed >= 2  # Should have polled for at least 2 seconds

    # Send a message and test immediate read
    async with get_async_session_maker() as session:
        msg_id = await PGMQOperation.send_async(
            queue_name, MSG, delay=0, session=session, commit=True
        )

    async with get_async_session_maker() as session:
        msgs = await PGMQOperation.read_with_poll_async(
            queue_name,
            vt=30,
            qty=1,
            max_poll_seconds=5,
            poll_interval_ms=100,
            session=session,
            commit=True,
        )

    assert msgs is not None
    assert len(msgs) == 1
    assert msgs[0].msg_id == msg_id

    # Clean up
    async with get_async_session_maker() as session:
        await PGMQOperation.drop_queue_async(
            queue_name, partitioned=False, session=session, commit=True
        )


@pytest.mark.asyncio
async def test_drop_queue_async(get_async_session_maker, db_session):
    """Test dropping a queue asynchronously."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"

    # Create queue
    async with get_async_session_maker() as session:
        await PGMQOperation.create_queue_async(
            queue_name, unlogged=False, session=session, commit=True
        )

    # Verify queue exists
    assert check_queue_exists(db_session, queue_name) is True

    # Drop queue
    async with get_async_session_maker() as session:
        dropped = await PGMQOperation.drop_queue_async(
            queue_name, partitioned=False, session=session, commit=True
        )

    assert dropped is True

    # Verify queue is dropped
    assert check_queue_exists(db_session, queue_name) is False


@pytest.mark.asyncio
async def test_check_pg_partman_ext_async(get_async_session_maker):
    """Test that pg_partman extension check works asynchronously."""
    async with get_async_session_maker() as session:
        # Should not raise any exception
        # Note: This will only succeed if pg_partman is installed
        try:
            await PGMQOperation.check_pg_partman_ext_async(session=session, commit=True)
        except Exception as e:
            # If pg_partman is not installed, we expect an error
            # This is acceptable for this test
            pytest.skip(f"pg_partman extension not available: {e}")


@pytest.mark.asyncio
async def test_create_partitioned_queue_async(get_async_session_maker, db_session):
    """Test creating a partitioned queue asynchronously."""
    queue_name = f"part_{uuid.uuid4().hex[:20]}"

    # First ensure pg_partman extension is available
    try:
        async with get_async_session_maker() as session:
            await PGMQOperation.check_pg_partman_ext_async(session=session, commit=True)
    except Exception as e:
        pytest.skip(f"pg_partman extension not available: {e}")

    # Create partitioned queue with numeric partitioning
    async with get_async_session_maker() as session:
        await PGMQOperation.create_partitioned_queue_async(
            queue_name,
            partition_interval="10000",
            retention_interval="100000",
            session=session,
            commit=True,
        )

    assert check_queue_exists(db_session, queue_name) is True

    # Test sending and reading from partitioned queue
    async with get_async_session_maker() as session:
        msg_id = await PGMQOperation.send_async(
            queue_name, MSG, delay=0, session=session, commit=True
        )
        msg = await PGMQOperation.read_async(
            queue_name, vt=30, session=session, commit=True
        )

    assert msg is not None
    assert msg.msg_id == msg_id

    # Clean up
    async with get_async_session_maker() as session:
        await PGMQOperation.drop_queue_async(
            queue_name, partitioned=True, session=session, commit=True
        )
