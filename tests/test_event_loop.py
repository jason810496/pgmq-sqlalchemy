import asyncio
import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from pgmq_sqlalchemy import PGMQueue


def test_event_loop_with_provided_loop(get_async_dsn):
    """Test that PGMQueue uses the provided event loop."""
    custom_loop = asyncio.new_event_loop()
    pgmq = PGMQueue(dsn=get_async_dsn, loop=custom_loop)
    
    assert pgmq.loop is custom_loop
    assert pgmq.is_async is True
    
    # Clean up
    custom_loop.close()


def test_event_loop_creates_new_when_not_provided(get_async_dsn):
    """Test that PGMQueue creates a new event loop when none is provided and no loop is running."""
    pgmq = PGMQueue(dsn=get_async_dsn)
    
    assert pgmq.loop is not None
    assert pgmq.is_async is True
    assert isinstance(pgmq.loop, asyncio.AbstractEventLoop)


def test_event_loop_with_sync_dsn_has_no_loop(get_dsn):
    """Test that sync PGMQueue does not have an event loop."""
    pgmq = PGMQueue(dsn=get_dsn)
    
    assert pgmq.loop is None
    assert pgmq.is_async is False


def test_event_loop_with_provided_engine(get_async_engine):
    """Test that PGMQueue uses provided loop with async engine."""
    custom_loop = asyncio.new_event_loop()
    pgmq = PGMQueue(engine=get_async_engine, loop=custom_loop)
    
    assert pgmq.loop is custom_loop
    assert pgmq.is_async is True
    
    # Clean up
    custom_loop.close()


@pytest.mark.asyncio
async def test_event_loop_uses_running_loop_when_available(get_async_dsn):
    """Test that PGMQueue uses the running event loop when available."""
    # Get the currently running event loop
    running_loop = asyncio.get_running_loop()
    
    # Create PGMQueue without providing a loop
    # Since we're inside an async context, it should use the running loop
    pgmq = PGMQueue(dsn=get_async_dsn)
    
    # The PGMQueue should have captured the running loop
    assert pgmq.loop is running_loop
    assert pgmq.is_async is True
