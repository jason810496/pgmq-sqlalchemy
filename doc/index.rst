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
* **Automatically** creates ``pgmq`` extension on the database if not exists.
* Supports all Postgres DBAPIs supported by ``SQLAlchemy``.
    * Examples: ``psycopg``, ``psycopg2``, ``asyncpg``
    * See `SQLAlchemy Postgresql Dialects <https://docs.sqlalchemy.org/en/20/dialects/postgresql.html>`_


Table of Contents
-----------------

.. toctree::
    :maxdepth: 2

    self
    installation
    getting-started
    api-reference
    development
    release