"""Integration tests for FastAPI pub/sub example with subprocess."""

import os
import sys
import logging
import time

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from pgmq_sqlalchemy import op

from examples_tests.utils.console import MultiSubprocessesRenderer, CmdArg

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def test_queue_name():
    """Return a unique queue name for testing."""
    return os.getenv("QUEUE_NAME", "order_queue")


def test_api_consumer_integration(
    sync_database_url: str, test_queue_name: str, request: pytest.FixtureRequest
):
    """Test creating 100 orders parallelly and waiting for consumer to process them all."""

    # Check if pytest was run with -v flag
    verbose_mode = request.config.getoption("verbose") > 0

    # Wait for the consumer to process all messages
    # Check the queue periodically until it's empty
    engine = create_engine(sync_database_url)
    SessionLocal = sessionmaker(bind=engine)

    max_wait = 120  # Wait up to 2 minutes
    num_orders = 100

    def stop_condition() -> bool:
        # Check queue metrics to see if there are any messages left
        with SessionLocal() as session:
            # Get the test queue name from environment or use default
            try:
                metrics = op.metrics(test_queue_name, session=session, commit=True)
                if metrics:
                    logger.info("%s queue metrics: %s", test_queue_name, str(metrics))
                    if metrics.queue_length == 0:
                        # All messages have been processed
                        return True
            except Exception as e:
                # Queue might not exist yet or other error
                logger.error(f"Error checking metrics: {e}")
        return False

    logger.info("Wait for Consumer to process all the orders")
    start_time = None

    # Build consumer command with optional verbose flag
    consumer_cmd = [sys.executable, "-u", "examples/fastapi_pub_sub/consumer.py"]
    if verbose_mode:
        consumer_cmd.append("-v")

    with MultiSubprocessesRenderer(
        cmds=[
            CmdArg(
                [sys.executable, "-u", "examples/fastapi_pub_sub/api.py"],
                panel_title="API process",
            ),
            CmdArg(
                consumer_cmd,
                panel_title="Consumer process",
            ),
            CmdArg(
                [
                    sys.executable,
                    "-u",
                    "examples/fastapi_pub_sub/create_orders_coordinator.py",
                ],
                panel_title="Create Orders process",
            ),
        ],
        timeout=max_wait,
        wait_process_init_time=3,
        stop_condition_callable=stop_condition,
    ) as renderer:
        start_time = time.time()
        renderer.start_render()

    with SessionLocal() as session:
        metrics = op.metrics(test_queue_name, session=session, commit=True)
        if metrics:
            logger.info("%s queue metrics: %s", test_queue_name, str(metrics))

    if (time.time() - start_time) > max_wait:
        pytest.fail(f"Consumer did not process all messages within {max_wait} seconds")

    # Verify that all messages were processed
    with SessionLocal() as session:
        metrics = op.metrics(test_queue_name, session=session, commit=True)
        assert (
            metrics.queue_length == 0
        ), f"Queue still has {metrics.queue_length} messages"
        # The total_messages should be at least num_orders (could be more if retries happened)
        assert (
            metrics.total_messages >= num_orders
        ), f"Expected at least {num_orders} total messages, got {metrics.total_messages}"
