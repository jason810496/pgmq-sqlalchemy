"""Tests for PGMQOperation class.

This test suite tests the PGMQOperation class methods directly,
which are transaction-friendly static methods that accept sessions.
"""
import uuid
import pytest

from sqlalchemy.exc import ProgrammingError

from pgmq_sqlalchemy.operation import PGMQOperation
from pgmq_sqlalchemy.schema import Message, QueueMetrics
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
        PGMQOperation.create_queue(queue_name, unlogged=False, session=session, commit=True)
    
    assert check_queue_exists(db_session, queue_name) is True
    
    # Clean up
    with get_session_maker() as session:
        PGMQOperation.drop_queue(queue_name, partitioned=False, session=session, commit=True)


def test_create_unlogged_queue_sync(get_session_maker, db_session):
    """Test creating an unlogged queue using PGMQOperation."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    
    with get_session_maker() as session:
        PGMQOperation.create_queue(queue_name, unlogged=True, session=session, commit=True)
    
    assert check_queue_exists(db_session, queue_name) is True
    
    # Clean up
    with get_session_maker() as session:
        PGMQOperation.drop_queue(queue_name, partitioned=False, session=session, commit=True)


def test_validate_queue_name_sync(get_session_maker):
    """Test queue name validation."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    
    with get_session_maker() as session:
        # Should not raise for valid name
        PGMQOperation.validate_queue_name(queue_name, session=session, commit=True)
        
        # Should raise for name that's too long
        with pytest.raises(Exception) as e:
            PGMQOperation.validate_queue_name("a" * 49, session=session, commit=True)
        error_msg = str(e.value.orig)
        assert "queue name is too long" in error_msg


def test_list_queues_sync(get_session_maker, db_session):
    """Test listing queues."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    
    # Create a queue
    with get_session_maker() as session:
        PGMQOperation.create_queue(queue_name, unlogged=False, session=session, commit=True)
    
    # List queues
    with get_session_maker() as session:
        queues = PGMQOperation.list_queues(session=session, commit=True)
    
    assert queue_name in queues
    
    # Clean up
    with get_session_maker() as session:
        PGMQOperation.drop_queue(queue_name, partitioned=False, session=session, commit=True)


def test_send_and_read_sync(get_session_maker, db_session):
    """Test sending and reading messages."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    
    # Create queue
    with get_session_maker() as session:
        PGMQOperation.create_queue(queue_name, unlogged=False, session=session, commit=True)
    
    # Send a message
    with get_session_maker() as session:
        msg_id = PGMQOperation.send(queue_name, MSG, delay=0, session=session, commit=True)
    
    assert msg_id > 0
    
    # Read the message
    with get_session_maker() as session:
        msg = PGMQOperation.read(queue_name, vt=30, session=session, commit=True)
    
    assert msg is not None
    assert msg.msg_id == msg_id
    assert msg.message == MSG
    
    # Clean up
    with get_session_maker() as session:
        PGMQOperation.drop_queue(queue_name, partitioned=False, session=session, commit=True)


def test_send_batch_sync(get_session_maker, db_session):
    """Test sending a batch of messages."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    messages = [{"key": f"value{i}"} for i in range(5)]
    
    # Create queue
    with get_session_maker() as session:
        PGMQOperation.create_queue(queue_name, unlogged=False, session=session, commit=True)
    
    # Send batch
    with get_session_maker() as session:
        msg_ids = PGMQOperation.send_batch(queue_name, messages, delay=0, session=session, commit=True)
    
    assert len(msg_ids) == 5
    
    # Read batch
    with get_session_maker() as session:
        msgs = PGMQOperation.read_batch(queue_name, vt=30, batch_size=5, session=session, commit=True)
    
    assert len(msgs) == 5
    
    # Clean up
    with get_session_maker() as session:
        PGMQOperation.drop_queue(queue_name, partitioned=False, session=session, commit=True)


def test_pop_sync(get_session_maker, db_session):
    """Test popping a message from the queue."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    
    # Create queue and send message
    with get_session_maker() as session:
        PGMQOperation.create_queue(queue_name, unlogged=False, session=session, commit=True)
        msg_id = PGMQOperation.send(queue_name, MSG, delay=0, session=session, commit=True)
    
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
        PGMQOperation.drop_queue(queue_name, partitioned=False, session=session, commit=True)


def test_delete_sync(get_session_maker, db_session):
    """Test deleting a message."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    
    # Create queue and send message
    with get_session_maker() as session:
        PGMQOperation.create_queue(queue_name, unlogged=False, session=session, commit=True)
        msg_id = PGMQOperation.send(queue_name, MSG, delay=0, session=session, commit=True)
    
    # Delete message
    with get_session_maker() as session:
        deleted = PGMQOperation.delete(queue_name, msg_id, session=session, commit=True)
    
    assert deleted is True
    
    # Clean up
    with get_session_maker() as session:
        PGMQOperation.drop_queue(queue_name, partitioned=False, session=session, commit=True)


def test_set_vt_sync(get_session_maker, db_session):
    """Test setting visibility timeout."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    
    # Create queue and send message
    with get_session_maker() as session:
        PGMQOperation.create_queue(queue_name, unlogged=False, session=session, commit=True)
        msg_id = PGMQOperation.send(queue_name, MSG, delay=0, session=session, commit=True)
        # Read message to set initial vt
        PGMQOperation.read(queue_name, vt=5, session=session, commit=True)
    
    # Set new vt
    with get_session_maker() as session:
        msg = PGMQOperation.set_vt(queue_name, msg_id, vt=60, session=session, commit=True)
    
    assert msg is not None
    
    # Clean up
    with get_session_maker() as session:
        PGMQOperation.drop_queue(queue_name, partitioned=False, session=session, commit=True)


def test_archive_sync(get_session_maker, db_session):
    """Test archiving a message."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    
    # Create queue and send message
    with get_session_maker() as session:
        PGMQOperation.create_queue(queue_name, unlogged=False, session=session, commit=True)
        msg_id = PGMQOperation.send(queue_name, MSG, delay=0, session=session, commit=True)
    
    # Archive message
    with get_session_maker() as session:
        archived = PGMQOperation.archive(queue_name, msg_id, session=session, commit=True)
    
    assert archived is True
    
    # Clean up
    with get_session_maker() as session:
        PGMQOperation.drop_queue(queue_name, partitioned=False, session=session, commit=True)


def test_metrics_sync(get_session_maker, db_session):
    """Test getting queue metrics."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    
    # Create queue
    with get_session_maker() as session:
        PGMQOperation.create_queue(queue_name, unlogged=False, session=session, commit=True)
    
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
            PGMQOperation.send(queue_name, {"index": i}, delay=0, session=session, commit=True)
    
    # Get metrics after adding messages
    with get_session_maker() as session:
        metrics = PGMQOperation.metrics(queue_name, session=session, commit=True)
    
    assert metrics.queue_length == 3
    assert metrics.total_messages == 3
    
    # Clean up
    with get_session_maker() as session:
        PGMQOperation.drop_queue(queue_name, partitioned=False, session=session, commit=True)


def test_metrics_all_sync(get_session_maker, db_session):
    """Test getting metrics for all queues."""
    queue_name1 = f"test_queue_{uuid.uuid4().hex}"
    queue_name2 = f"test_queue_{uuid.uuid4().hex}"
    
    # Create two queues
    with get_session_maker() as session:
        PGMQOperation.create_queue(queue_name1, unlogged=False, session=session, commit=True)
        PGMQOperation.create_queue(queue_name2, unlogged=False, session=session, commit=True)
    
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
        PGMQOperation.drop_queue(queue_name1, partitioned=False, session=session, commit=True)
        PGMQOperation.drop_queue(queue_name2, partitioned=False, session=session, commit=True)


def test_transaction_rollback_sync(get_session_maker, db_session):
    """Test that operations can be rolled back when commit=False."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    
    # Create queue with commit=False, then rollback
    with get_session_maker() as session:
        PGMQOperation.create_queue(queue_name, unlogged=False, session=session, commit=False)
        session.rollback()
    
    # Queue should not exist
    assert check_queue_exists(db_session, queue_name) is False


def test_transaction_commit_sync(get_session_maker, db_session):
    """Test that operations are committed when commit=True."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    
    # Create queue with commit=True
    with get_session_maker() as session:
        PGMQOperation.create_queue(queue_name, unlogged=False, session=session, commit=True)
    
    # Queue should exist
    assert check_queue_exists(db_session, queue_name) is True
    
    # Clean up
    with get_session_maker() as session:
        PGMQOperation.drop_queue(queue_name, partitioned=False, session=session, commit=True)


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
        await PGMQOperation.create_queue_async(queue_name, unlogged=False, session=session, commit=True)
    
    assert check_queue_exists(db_session, queue_name) is True
    
    # Clean up
    async with get_async_session_maker() as session:
        await PGMQOperation.drop_queue_async(queue_name, partitioned=False, session=session, commit=True)


@pytest.mark.asyncio
async def test_send_and_read_async(get_async_session_maker, db_session):
    """Test sending and reading messages asynchronously."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    
    # Create queue
    async with get_async_session_maker() as session:
        await PGMQOperation.create_queue_async(queue_name, unlogged=False, session=session, commit=True)
    
    # Send a message
    async with get_async_session_maker() as session:
        msg_id = await PGMQOperation.send_async(queue_name, MSG, delay=0, session=session, commit=True)
    
    assert msg_id > 0
    
    # Read the message
    async with get_async_session_maker() as session:
        msg = await PGMQOperation.read_async(queue_name, vt=30, session=session, commit=True)
    
    assert msg is not None
    assert msg.msg_id == msg_id
    assert msg.message == MSG
    
    # Clean up
    async with get_async_session_maker() as session:
        await PGMQOperation.drop_queue_async(queue_name, partitioned=False, session=session, commit=True)


@pytest.mark.asyncio
async def test_metrics_async(get_async_session_maker, db_session):
    """Test getting queue metrics asynchronously."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    
    # Create queue
    async with get_async_session_maker() as session:
        await PGMQOperation.create_queue_async(queue_name, unlogged=False, session=session, commit=True)
    
    # Get metrics
    async with get_async_session_maker() as session:
        metrics = await PGMQOperation.metrics_async(queue_name, session=session, commit=True)
    
    assert metrics is not None
    assert isinstance(metrics, QueueMetrics)
    assert metrics.queue_name == queue_name
    assert metrics.queue_length == 0
    
    # Clean up
    async with get_async_session_maker() as session:
        await PGMQOperation.drop_queue_async(queue_name, partitioned=False, session=session, commit=True)
