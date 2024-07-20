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

PGMQ SQLAlchemy
---------------

