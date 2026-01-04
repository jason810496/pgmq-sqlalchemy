"""Tests for FastAPI pub/sub example."""
import asyncio
import time
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from pgmq_sqlalchemy import PGMQueue, op


@pytest.fixture(scope="module")
def test_queue_name():
    """Return a unique queue name for testing."""
    return "test_order_queue"


@pytest.fixture(scope="module")
def setup_api_app(sync_database_url, test_queue_name):
    """Setup the FastAPI app with test configuration."""
    # Import after fixture is set up
    import sys
    import os
    
    # Add examples directory to path
    examples_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 
        "examples", 
        "fastapi_pub_sub"
    )
    sys.path.insert(0, examples_dir)
    
    # Import and configure api module
    import api
    
    # Override configuration with test values
    api.DATABASE_URL = sync_database_url
    api.QUEUE_NAME = test_queue_name
    
    # Create new engine and session maker with test config
    api.engine = create_engine(sync_database_url)
    api.SessionLocal = sessionmaker(bind=api.engine, autocommit=False, autoflush=False)
    
    # Create tables and queue
    api.Base.metadata.create_all(bind=api.engine)
    
    with api.SessionLocal() as session:
        api.op.check_pgmq_ext(session=session, commit=True)
        try:
            api.op.create_queue(test_queue_name, session=session, commit=True)
        except Exception as e:
            # Queue already exists from a previous test run
            import logging
            logging.warning(f"Could not create queue (may already exist): {e}")
    
    yield api
    
    # Cleanup
    with api.SessionLocal() as session:
        # Drop the test queue
        try:
            api.op.drop_queue(test_queue_name, session=session, commit=True)
        except Exception as e:
            import logging
            logging.warning(f"Could not drop queue: {e}")
        
        # Drop tables
        session.execute(text("DROP TABLE IF EXISTS orders CASCADE"))
        session.commit()
    
    # Remove from path
    sys.path.remove(examples_dir)


@pytest.fixture(scope="module")
def client(setup_api_app):
    """Create a test client for the FastAPI app."""
    return TestClient(setup_api_app.app)


def test_health_check(client):
    """Test the health check endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_create_order(client, sync_database_url, test_queue_name):
    """Test creating an order via the API."""
    order_data = {
        "customer_name": "John Doe",
        "product_name": "Widget",
        "quantity": 5,
        "price": 29.99
    }
    
    response = client.post("/orders", json=order_data)
    assert response.status_code == 201
    
    data = response.json()
    assert data["customer_name"] == order_data["customer_name"]
    assert data["product_name"] == order_data["product_name"]
    assert data["quantity"] == order_data["quantity"]
    assert data["price"] == order_data["price"]
    assert "id" in data
    assert "message_id" in data
    assert "created_at" in data
    
    # Verify message was published to queue
    engine = create_engine(sync_database_url)
    SessionLocal = sessionmaker(bind=engine)
    
    with SessionLocal() as session:
        msg = op.read(test_queue_name, vt=30, session=session, commit=True)
        
        assert msg is not None
        assert msg.message["order_id"] == data["id"]
        assert msg.message["customer_name"] == order_data["customer_name"]
        
        # Clean up message
        op.delete(test_queue_name, msg.msg_id, session=session, commit=True)


def test_get_order(client):
    """Test retrieving an order by ID."""
    # First create an order
    order_data = {
        "customer_name": "Jane Smith",
        "product_name": "Gadget",
        "quantity": 3,
        "price": 49.99
    }
    
    create_response = client.post("/orders", json=order_data)
    assert create_response.status_code == 201
    order_id = create_response.json()["id"]
    
    # Then retrieve it
    get_response = client.get(f"/orders/{order_id}")
    assert get_response.status_code == 200
    
    data = get_response.json()
    assert data["customer_name"] == order_data["customer_name"]
    assert data["product_name"] == order_data["product_name"]
    assert data["quantity"] == order_data["quantity"]
    assert data["price"] == order_data["price"]


def test_get_nonexistent_order(client):
    """Test retrieving a non-existent order."""
    response = client.get("/orders/999999")
    assert response.status_code == 404
    assert response.json()["detail"] == "Order not found"


@pytest.mark.asyncio
async def test_consumer_processing(async_database_url, sync_database_url, test_queue_name):
    """Test the async consumer processing messages."""
    # Create a test order message directly in the queue
    from sqlalchemy import create_engine as sync_create_engine
    from sqlalchemy.orm import sessionmaker as sync_sessionmaker
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker as async_sessionmaker
    from pgmq_sqlalchemy import op
    
    engine = sync_create_engine(sync_database_url)
    SessionLocal = sync_sessionmaker(bind=engine)
    
    # Create the queue first
    with SessionLocal() as session:
        op.check_pgmq_ext(session=session, commit=True)
        try:
            op.create_queue(test_queue_name, session=session, commit=True)
        except Exception as e:
            # Queue already exists from a previous test run
            import logging
            logging.warning(f"Could not create queue (may already exist): {e}")
    
    test_message = {
        "order_id": 12345,
        "customer_name": "Test Customer",
        "product_name": "Test Product",
        "quantity": 10,
        "price": 99.99,
        "created_at": "2024-01-01T00:00:00"
    }
    
    msg_id = None
    with SessionLocal() as session:
        msg_id = op.send(test_queue_name, test_message, session=session, commit=True)
    
    assert msg_id is not None
    
    # Now test consumer logic by reading and processing with async operations
    async_engine = create_async_engine(async_database_url)
    async_session_maker = async_sessionmaker(bind=async_engine, class_=AsyncSession)
    
    # Read the message using async operations directly
    async with async_session_maker() as session:
        messages = await op.read_batch_async(test_queue_name, vt=30, batch_size=10, session=session, commit=True)
    
    assert len(messages) >= 1
    
    # Find our test message
    test_msg = None
    for msg in messages:
        if msg.message.get("order_id") == 12345:
            test_msg = msg
            break
    
    assert test_msg is not None
    assert test_msg.message["customer_name"] == "Test Customer"
    assert test_msg.message["product_name"] == "Test Product"
    
    # Simulate processing and deletion
    async with async_session_maker() as session:
        deleted = await op.delete_async(test_queue_name, test_msg.msg_id, session=session, commit=True)
        assert deleted is True
    
    # Verify message was deleted
    await asyncio.sleep(1)  # Wait a bit for deletion
    async with async_session_maker() as session:
        remaining_messages = await op.read_batch_async(test_queue_name, vt=30, batch_size=100, session=session, commit=True)
    
    # Our message should not be in the remaining messages (if any)
    if remaining_messages:
        for msg in remaining_messages:
            assert msg.msg_id != test_msg.msg_id
    
    # Cleanup
    await async_engine.dispose()
