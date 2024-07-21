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

            version: '3.8'
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


        