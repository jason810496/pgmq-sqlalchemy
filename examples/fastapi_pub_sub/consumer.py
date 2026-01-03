"""Async consumer for processing orders from PGMQ.

This example demonstrates:
- Using asyncio for asynchronous message processing
- Using asyncpg driver with PGMQueue
- Reading and processing messages from PGMQ
- Deleting messages after successful processing
"""
import asyncio
import logging
from typing import Optional

from pgmq_sqlalchemy import PGMQueue
from pgmq_sqlalchemy.schema import Message

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database configuration
DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/postgres"
QUEUE_NAME = "order_queue"


async def process_order(message: Message) -> bool:
    """Process an order message.
    
    Args:
        message: Message from PGMQ containing order data
        
    Returns:
        True if processing was successful, False otherwise
    """
    try:
        order_data = message.message
        logger.info(f"Processing order {order_data.get('order_id')}")
        logger.info(f"  Customer: {order_data.get('customer_name')}")
        logger.info(f"  Product: {order_data.get('product_name')}")
        logger.info(f"  Quantity: {order_data.get('quantity')}")
        logger.info(f"  Price: ${order_data.get('price')}")
        
        # Simulate order processing (e.g., inventory check, payment processing, etc.)
        await asyncio.sleep(1)
        
        logger.info(f"Order {order_data.get('order_id')} processed successfully")
        return True
    except Exception as e:
        logger.error(f"Error processing order: {e}")
        return False


async def consume_messages(pgmq: PGMQueue, batch_size: int = 10, vt: int = 30):
    """Continuously consume and process messages from the queue.
    
    Args:
        pgmq: PGMQueue instance
        batch_size: Number of messages to read in each batch
        vt: Visibility timeout in seconds
    """
    from pgmq_sqlalchemy import op
    
    logger.info(f"Starting consumer for queue: {QUEUE_NAME}")
    logger.info(f"Batch size: {batch_size}, Visibility timeout: {vt}s")
    
    while True:
        try:
            # Read a batch of messages
            async with pgmq.session_maker() as session:
                messages = await op.read_batch_async(QUEUE_NAME, vt=vt, batch_size=batch_size, session=session, commit=True)
            
            if not messages:
                logger.debug("No messages available, waiting...")
                await asyncio.sleep(1)
                continue
            
            logger.info(f"Received {len(messages)} messages")
            
            # Process messages concurrently
            tasks = []
            for message in messages:
                task = process_order(message)
                tasks.append((message.msg_id, task))
            
            # Wait for all processing to complete
            results = await asyncio.gather(*[t[1] for t in tasks], return_exceptions=True)
            
            # Delete successfully processed messages
            for (msg_id, _), result in zip(tasks, results):
                if isinstance(result, bool) and result:
                    async with pgmq.session_maker() as session:
                        await op.delete_async(QUEUE_NAME, msg_id, session=session, commit=True)
                    logger.info(f"Deleted message {msg_id}")
                elif isinstance(result, Exception):
                    logger.error(f"Exception processing message {msg_id}: {result}")
                else:
                    logger.warning(f"Message {msg_id} processing failed, will retry later")
                    
        except KeyboardInterrupt:
            logger.info("Received shutdown signal, stopping consumer...")
            break
        except Exception as e:
            logger.error(f"Error in consumer loop: {e}")
            await asyncio.sleep(5)


async def main():
    """Main entry point for the consumer."""
    # Initialize PGMQueue with async session maker
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    
    async_engine = create_async_engine(DATABASE_URL)
    async_session_maker = sessionmaker(bind=async_engine, class_=AsyncSession)
    
    # Note: Manual PGMQueue setup to avoid event loop conflicts
    # PGMQueue.__init__ tries to run a nested event loop which conflicts
    # with asyncio.run(). This is a known limitation when using PGMQueue
    # in an async context manager like asyncio.run().
    # For proper usage, consider using PGMQOperation methods directly with sessions.
    pgmq = PGMQueue.__new__(PGMQueue)
    pgmq.engine = async_engine
    pgmq.session_maker = async_session_maker
    pgmq.is_async = True
    pgmq.delay = 0
    pgmq.vt = 30
    pgmq.loop = None
    pgmq.is_pg_partman_ext_checked = True
    
    # Check PGMQ extension manually
    async with async_session_maker() as session:
        from pgmq_sqlalchemy import op
        await op.check_pgmq_ext_async(session=session, commit=True)
    
    try:
        # Start consuming messages
        await consume_messages(pgmq, batch_size=10, vt=30)
    finally:
        logger.info("Consumer stopped")
        await async_engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
