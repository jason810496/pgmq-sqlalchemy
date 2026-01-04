"""Integration tests for FastAPI pub/sub example with subprocess."""
import asyncio
import os
import subprocess
import sys
import time
import signal
import pytest
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


@pytest.fixture(scope="module")
def examples_dir():
    """Return the path to the examples directory."""
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "examples",
        "fastapi_pub_sub"
    )


@pytest.fixture(scope="module")
def test_queue_name():
    """Return a unique queue name for testing."""
    return "test_integration_order_queue"


@pytest.fixture(scope="module")
def database_url(request):
    """Get database URL from environment or CLI."""
    db_name = request.config.getoption("--db-name")
    if not db_name:
        db_name = os.getenv("SQLALCHEMY_DB", "postgres")
    
    host = os.getenv("SQLALCHEMY_HOST", "localhost")
    port = os.getenv("SQLALCHEMY_PORT", "5432")
    user = os.getenv("SQLALCHEMY_USER", "postgres")
    password = os.getenv("SQLALCHEMY_PASSWORD", "postgres")
    
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"


@pytest.fixture(scope="module", autouse=True)
def api_instance(examples_dir, database_url, test_queue_name):
    """Fixture to spin up the API server as a subprocess."""
    # Update the API to use test queue
    api_py = os.path.join(examples_dir, "api.py")
    
    # Set environment variables for the subprocess
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    env["QUEUE_NAME"] = test_queue_name
    
    # Start the API server
    process = subprocess.Popen(
        [sys.executable, api_py],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        preexec_fn=os.setsid if hasattr(os, 'setsid') else None
    )
    
    # Wait for the server to start
    max_attempts = 30
    for i in range(max_attempts):
        try:
            response = requests.get("http://localhost:8000/health", timeout=1)
            if response.status_code == 200:
                break
        except requests.exceptions.RequestException:
            time.sleep(1)
    else:
        # Kill the process if it didn't start
        if hasattr(os, 'killpg'):
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
        else:
            process.terminate()
        pytest.fail("API server failed to start")
    
    yield process
    
    # Teardown: kill the API server
    if hasattr(os, 'killpg'):
        os.killpg(os.getpgid(process.pid), signal.SIGTERM)
    else:
        process.terminate()
    process.wait(timeout=10)


@pytest.fixture(scope="module", autouse=True)
def consumer_instance(examples_dir, database_url, test_queue_name, api_instance):
    """Fixture to spin up the consumer as a subprocess."""
    # Update the consumer to use test queue
    consumer_py = os.path.join(examples_dir, "consumer.py")
    
    # Set environment variables for the subprocess
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    env["QUEUE_NAME"] = test_queue_name
    
    # Start the consumer
    process = subprocess.Popen(
        [sys.executable, consumer_py],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        preexec_fn=os.setsid if hasattr(os, 'setsid') else None
    )
    
    # Give the consumer some time to start
    time.sleep(3)
    
    yield process
    
    # Teardown: kill the consumer
    if hasattr(os, 'killpg'):
        os.killpg(os.getpgid(process.pid), signal.SIGTERM)
    else:
        process.terminate()
    process.wait(timeout=10)


def test_api_consumer_integration(api_instance, consumer_instance, database_url):
    """Test creating 100 orders parallelly and waiting for consumer to process them all."""
    import concurrent.futures
    
    # Create 100 orders in parallel
    num_orders = 100
    
    def create_order(order_num):
        """Helper function to create a single order."""
        order_data = {
            "customer_name": f"Customer {order_num}",
            "product_name": f"Product {order_num}",
            "quantity": order_num % 10 + 1,
            "price": 10.0 + (order_num % 50)
        }
        response = requests.post("http://localhost:8000/orders", json=order_data, timeout=5)
        return response.status_code == 201, response.json() if response.status_code == 201 else None
    
    # Create orders in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(create_order, i) for i in range(num_orders)]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]
    
    # Check that all orders were created successfully
    successful_orders = sum(1 for success, _ in results if success)
    assert successful_orders == num_orders, f"Only {successful_orders}/{num_orders} orders were created"
    
    # Wait for the consumer to process all messages
    # Check the queue periodically until it's empty
    engine = create_engine(database_url)
    SessionLocal = sessionmaker(bind=engine)
    
    max_wait = 120  # Wait up to 2 minutes
    start_time = time.time()
    
    while time.time() - start_time < max_wait:
        # Check queue metrics to see if there are any messages left
        with SessionLocal() as session:
            from pgmq_sqlalchemy import op
            # Get the test queue name from environment or use default
            test_queue = os.getenv("QUEUE_NAME", "test_integration_order_queue")
            
            try:
                metrics = op.metrics(test_queue, session=session, commit=True)
                if metrics.queue_length == 0:
                    # All messages have been processed
                    break
            except Exception as e:
                # Queue might not exist yet or other error
                print(f"Error checking metrics: {e}")
        
        time.sleep(2)
    else:
        pytest.fail(f"Consumer did not process all messages within {max_wait} seconds")
    
    # Verify that all messages were processed
    with SessionLocal() as session:
        test_queue = os.getenv("QUEUE_NAME", "test_integration_order_queue")
        metrics = op.metrics(test_queue, session=session, commit=True)
        assert metrics.queue_length == 0, f"Queue still has {metrics.queue_length} messages"
        # The total_messages should be at least num_orders (could be more if retries happened)
        assert metrics.total_messages >= num_orders, f"Expected at least {num_orders} total messages, got {metrics.total_messages}"
