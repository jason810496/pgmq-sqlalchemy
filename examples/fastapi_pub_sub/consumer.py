"""Async consumer for processing orders from PGMQ.

This example demonstrates:
- Using asyncio for asynchronous message processing
- Using asyncpg driver with PGMQueue
- Reading and processing messages from PGMQ
- Deleting messages after successful processing
"""
import argparse
import asyncio
import logging
import os

from pgmq_sqlalchemy import PGMQueue
from pgmq_sqlalchemy.schema import Message

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s][%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Database configuration - can be overridden by environment variables
DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/postgres"
)
QUEUE_NAME = os.getenv("QUEUE_NAME", "order_queue")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "30"))
VT = int(os.getenv("VT", "10"))


async def process_order(message: Message, verbose: bool = False) -> bool:
    """Process an order message.

    Args:
        message: Message from PGMQ containing order data
        verbose: If True, log detailed order information

    Returns:
        True if processing was successful, False otherwise
    """
    try:
        order_data = message.message
        if verbose:
            logger.info(f"Processing order {order_data.get('order_id')}")
            logger.info(f"  Customer: {order_data.get('customer_name')}")
            logger.info(f"  Product: {order_data.get('product_name')}")
            logger.info(f"  Quantity: {order_data.get('quantity')}")
            logger.info(f"  Price: ${order_data.get('price')}")

        # Simulate order processing (e.g., inventory check, payment processing, etc.)
        await asyncio.sleep(1)

        # Simulate msg_id%6 will fail twice, msg_id%2 will fail once
        if message.msg_id % 2 == 0 and message.read_ct == 1:
            logger.info(
                f"Order {order_data.get('order_id')} processed fail at first try"
            )
            return False
        elif message.msg_id % 3 == 0 and message.read_ct == 2:
            logger.info(
                f"Order {order_data.get('order_id')} processed fail at second try"
            )
            return False

        if verbose:
            logger.info(f"Order {order_data.get('order_id')} processed successfully")
        return True
    except Exception as e:
        logger.error(f"Error processing order: {e}")
        return False


async def consume_messages(
    pgmq: PGMQueue, batch_size: int, vt: int, verbose: bool = False
):
    """Continuously consume and process messages from the queue.

    Args:
        pgmq: PGMQueue instance
        batch_size: Number of messages to read in each batch
        vt: Visibility timeout in seconds
        verbose: If True, log detailed order information
    """
    logger.info(f"Starting consumer for queue: {QUEUE_NAME}")
    logger.info(f"Batch size: {batch_size}, Visibility timeout: {vt}s")
    if verbose:
        logger.info("Verbose mode enabled")

    while True:
        try:
            # Read a batch of messages using pgmq instance method
            messages = await pgmq.read_batch_async(
                QUEUE_NAME, vt=vt, batch_size=batch_size
            )

            if not messages:
                logger.debug("No messages available, waiting...")
                await asyncio.sleep(1)
                continue

            logger.info(f"Received {len(messages)} messages")

            # Process messages concurrently
            tasks = []
            for message in messages:
                task = process_order(message, verbose=verbose)
                tasks.append((message.msg_id, task))

            # Wait for all processing to complete
            results = await asyncio.gather(
                *[t[1] for t in tasks], return_exceptions=True
            )

            # Delete successfully processed messages using pgmq instance method
            deleted_cnt = 0
            for (msg_id, _), result in zip(tasks, results):
                if isinstance(result, bool) and result:
                    deleted = await pgmq.delete_async(QUEUE_NAME, msg_id)
                    if deleted:
                        deleted_cnt += 1
                        if verbose:
                            logger.info("Deleted message %d", msg_id)
                elif isinstance(result, Exception):
                    logger.error(f"Exception processing message {msg_id}: {result}")
                else:
                    logger.warning(
                        f"Message {msg_id} processing failed, will retry later"
                    )
            logger.info("%d messages processed successfully", deleted_cnt)

        except KeyboardInterrupt:
            logger.info("Received shutdown signal, stopping consumer...")
            break
        except Exception as e:
            logger.error(f"Error in consumer loop: {e}")
            await asyncio.sleep(5)


async def main(verbose: bool = False):
    """Main entry point for the consumer.

    Args:
        verbose: If True, log detailed order information
    """
    # Initialize PGMQueue with async session maker and event loop
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker

    async_engine = create_async_engine(DATABASE_URL)
    async_session_maker = sessionmaker(bind=async_engine, class_=AsyncSession)

    pgmq = PGMQueue(session_maker=async_session_maker)

    try:
        # Start consuming messages
        await consume_messages(pgmq, batch_size=BATCH_SIZE, vt=VT, verbose=verbose)
    finally:
        logger.info("Consumer stopped")
        await async_engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="PGMQ async consumer for processing orders"
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose logging with order details",
    )
    args = parser.parse_args()

    asyncio.run(main(verbose=args.verbose))
