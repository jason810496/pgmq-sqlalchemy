.. _getting-started:

Getting Started
===============

.. Note::
    Make sure you have the following installed:
        * `Docker <https://docs.docker.com/engine/install/>`_ 
        * `Docker Compose <https://docs.docker.com/compose/install/>`_

Postgres Setup
--------------

For quick setup:
    .. code-block:: bash

        docker run -d --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 quay.io/tembo/pg16-pgmq:latest


Or using **Docker Compose** to start Postgres with ``PGMQ`` extension:
    ``docker-compose.yml``:
        .. code-block:: yaml

            services:
            pgmq_postgres:
                container_name: pgmq_postgres
                image: quay.io/tembo/pg16-pgmq:latest
                environment:
                - POSTGRES_PASSWORD=postgres
                ports:
                - "5432:5432"
                volumes:
                - ./pgmq_postgres_volume:/var/lib/postgresql

    Then run the following command:

    .. code-block:: bash

        docker-compose up pgmq_postgres -d 


For more information, see `PGMQ GitHub <https://github.com/tembo-io/pgmq>`_.

pgmq-sqlalchemy Setup
---------------------

.. tip::

    See `API Reference <api-reference>`_ for **more examples and detailed usage**.

For ``dispatcher.py``:

    .. code-block:: python

        from typing import List
        from pgmq_sqlalchemy import PGMQueue

        postgres_dsn = 'postgresql://postgres:postgres@localhost:5432/postgres'

        pgmq = PGMQueue(dsn=postgres_dsn)
        pgmq.create_queue('my_queue')

        msg = {'key': 'value', 'key2': 'value2'}
        msg_id:int = pgmq.send('my_queue', msg)

        # could also send a list of messages
        msg_ids:List[int] = pgmq.send_batch('my_queue', [msg, msg])

    .. seealso::

        .. _init_method: ref:`pgmq_sqlalchemy.PGMQueue.__init__`
        .. |init_method| replace:: :py:meth:`~pgmq_sqlalchemy.PGMQueue.__init__`

        .. _send_method: ref:`pgmq_sqlalchemy.PGMQueue.send`
        .. |send_method| replace:: :py:meth:`~pgmq_sqlalchemy.PGMQueue.send`

        See |init_method|_ for more options on how to initialize the ``PGMQueue`` object, and advance usage with |send_method|_ on `API Reference <api-reference>`_.


For ``consumer.py``:

    .. code-block:: python

        from pgmq_sqlalchemy import PGMQueue
        from pgmq_sqlalchemy.schema import Message

        postgres_dsn = 'postgresql://postgres:postgres@localhost:5432/postgres'

        pgmq = PGMQueue(dsn=postgres_dsn)

        # read a single message
        msg:Message = pgmq.read('my_queue')

        # read a batch of messages
        msgs:List[Message] = pgmq.read_batch('my_queue', 10)

    .. seealso::

        .. _read_with_poll_method: ref:`pgmq_sqlalchemy.PGMQueue.read_with_poll`
        .. |read_with_poll_method| replace:: :py:meth:`~pgmq_sqlalchemy.PGMQueue.read_with_poll`

        .. _read_method: ref:`pgmq_sqlalchemy.PGMQueue.read`
        .. |read_method| replace:: :py:meth:`~pgmq_sqlalchemy.PGMQueue.read`

        See |read_with_poll_method|_ for reading messages with long-polling, and advance usage with |read_method|_ for **consumer retries mechanism** and more control over message consumption on `API Reference <api-reference>`_.

For ``monitor.py``:

    .. code-block:: python

        from pgmq_sqlalchemy import PGMQueue
        from pgmq_sqlalchemy.schema import QueueMetrics

        postgres_dsn = 'postgresql://postgres:postgres@localhost:5432/postgres'

        pgmq = PGMQueue(dsn=postgres_dsn)

        # get queue metrics
        metrics:QueueMetrics = pgmq.metrics('my_queue')
        print(metrics.queue_length)
        print(metrics.total_messages)


Using Transaction-Friendly Operations
--------------------------------------

.. tip::

    The ``op`` module provides static methods that accept sessions, allowing you to control transactions manually.
    This is useful when you need to combine PGMQ operations with your existing business logic within the same transaction.

The ``op`` module (``PGMQOperation``) provides transaction-friendly operations that give you full control over transaction boundaries.
This is particularly useful when you need to combine PGMQ operations with your existing business logic within the same transaction.

**Synchronous Transaction Example**

Combining business logic with PGMQ operations in a single transaction:

    .. code-block:: python

        from sqlalchemy import create_engine, text
        from sqlalchemy.orm import sessionmaker
        from pgmq_sqlalchemy import op

        # Setup
        engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres')
        SessionLocal = sessionmaker(bind=engine)

        # Perform multiple operations in a single transaction
        with SessionLocal() as session:
            try:
                # Check/create PGMQ extension
                op.check_pgmq_ext(session=session, commit=False)
                
                # Create a queue
                op.create_queue('orders_queue', session=session, commit=False)
                
                # Execute business logic (e.g., insert order into database)
                session.execute(
                    text("INSERT INTO orders (user_id, total) VALUES (:user_id, :total)"),
                    {"user_id": 123, "total": 99.99}
                )
                
                # Send message to queue about the new order
                msg_id = op.send(
                    'orders_queue',
                    {'user_id': 123, 'order_total': 99.99, 'action': 'process_order'},
                    session=session,
                    commit=False
                )
                
                # Commit all operations together
                session.commit()
                print(f"Order created and message {msg_id} sent successfully")
                
            except Exception as e:
                # Rollback everything if any operation fails
                session.rollback()
                print(f"Transaction failed: {e}")

**Asynchronous Transaction Example**

    .. code-block:: python

        from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
        from sqlalchemy.orm import sessionmaker
        from sqlalchemy import text
        from pgmq_sqlalchemy import op

        # Setup
        engine = create_async_engine('postgresql+asyncpg://postgres:postgres@localhost:5432/postgres')
        AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        async def process_order_with_queue():
            async with AsyncSessionLocal() as session:
                try:
                    # Check/create PGMQ extension
                    await op.check_pgmq_ext_async(session=session, commit=False)
                    
                    # Create a queue
                    await op.create_queue_async('orders_queue', session=session, commit=False)
                    
                    # Execute business logic
                    await session.execute(
                        text("INSERT INTO orders (user_id, total) VALUES (:user_id, :total)"),
                        {"user_id": 456, "total": 149.99}
                    )
                    
                    # Send message to queue
                    msg_id = await op.send_async(
                        'orders_queue',
                        {'user_id': 456, 'order_total': 149.99, 'action': 'process_order'},
                        session=session,
                        commit=False
                    )
                    
                    # Commit all operations together
                    await session.commit()
                    print(f"Order created and message {msg_id} sent successfully")
                    
                except Exception as e:
                    # Rollback everything if any operation fails
                    await session.rollback()
                    print(f"Transaction failed: {e}")

**Reading Messages with Transaction Control**

    .. code-block:: python

        from sqlalchemy.orm import sessionmaker
        from sqlalchemy import create_engine, text
        from pgmq_sqlalchemy import op

        engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres')
        SessionLocal = sessionmaker(bind=engine)

        with SessionLocal() as session:
            # Read message
            msg = op.read('orders_queue', vt=30, session=session, commit=False)
            
            if msg:
                try:
                    # Process the message and update database
                    session.execute(
                        text("UPDATE orders SET status = :status WHERE user_id = :user_id"),
                        {"status": "processing", "user_id": msg.message['user_id']}
                    )
                    
                    # Delete message from queue after successful processing
                    op.delete('orders_queue', msg.msg_id, session=session, commit=False)
                    
                    # Commit both the database update and message deletion
                    session.commit()
                    print(f"Message {msg.msg_id} processed successfully")
                    
                except Exception as e:
                    # Rollback if processing fails (message will become visible again)
                    session.rollback()
                    print(f"Processing failed: {e}")

    .. seealso::

        See `API Reference <api-reference>`_ for the complete list of available operations in the ``op`` module.


FastAPI Pub/Sub Example
-----------------------

For a complete, real-world example that combines ``PGMQOperation`` (``op``)
with FastAPI and ``PGMQueue`` for asynchronous consumption, see
`FastAPI Pub/Sub Example with PGMQ <example-with-fastapi-pub-sub>`_.


        