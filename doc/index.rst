.. _index:


.. image:: https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json
    :target: https://github.com/astral-sh/uv
    :alt: uv
.. image:: https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json
    :target: https://github.com/astral-sh/ruff
    :alt: Ruff
.. image:: https://img.shields.io/pypi/v/pgmq-sqlalchemy
    :target: https://pypi.org/project/pgmq-sqlalchemy/
    :alt: PyPI - Version
.. image:: https://img.shields.io/pypi/l/pgmq-sqlalchemy.svg
    :target: https://pypi.org/project/pgmq-sqlalchemy/
    :alt: PyPI - License
.. image:: https://codecov.io/gh/jason810496/pgmq-sqlalchemy/graph/badge.svg?token=C5ZVZCW7TE
    :target: https://codecov.io/gh/jason810496/pgmq-sqlalchemy
    :alt: Codecov

pgmq-sqlalchemy
===============

`PGMQ Postgres extension <https://github.com/tembo-io/pgmq>`_ Python client supporting **SQLAlchemy ORM** .

Features
--------

* Supports **async** and **sync** ``engines``, ``sessionmakers``, or directly constructed from ``dsn``.
* Supports all Postgres DBAPIs supported by ``SQLAlchemy``.
    * Examples: ``psycopg``, ``psycopg2``, ``asyncpg``
    * See `SQLAlchemy Postgresql Dialects <https://docs.sqlalchemy.org/en/20/dialects/postgresql.html>`_
* **Transaction-friendly operations** via the `op` module for combining PGMQ with your business logic in the same transaction.
* `Fully tested across all supported DBAPIs in both async and sync modes <https://github.com/jason810496/pgmq-sqlalchemy/actions/workflows/codecov.yml>`_.
* Battle-tested with `real-world FastAPI Pub/Sub examples <https://github.com/jason810496/pgmq-sqlalchemy/tree/main/examples/fastapi_pub_sub/README.md>`_ and `corresponding tests <https://github.com/jason810496/pgmq-sqlalchemy/actions/workflows/examples.yml>`_.

Table of Contents
-----------------

.. toctree::
    :maxdepth: 2

    self
    installation
    getting-started
    example-with-fastapi-pub-sub
    api-reference
    development
    release