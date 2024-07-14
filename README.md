[![Poetry](https://img.shields.io/endpoint?url=https://python-poetry.org/badge/v0.json)](https://python-poetry.org/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
![PyPI - Version](https://img.shields.io/pypi/v/pgmq-sqlalchemy)
[![PyPI - License](https://img.shields.io/pypi/l/pgmq-sqlalchemy.svg)](https://github.com/pgmq-sqlalchemy/pgmq-sqlalchemy-python/blob/main/LICENSE)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pgmq-sqlalchemy.svg)](https://pypi.python.org/pypi/pgmq-sqlalchemy)
[![codecov](https://codecov.io/gh/jason810496/pgmq-sqlalchemy/graph/badge.svg?token=C5ZVZCW7TE)](https://codecov.io/gh/jason810496/pgmq-sqlalchemy)
[![Docs](https://readthedocs.org/projects/pgmq-sqlalchemy-python/badge/?version=latest)](http://pgmq-sqlalchemy-python.readthedocs.io/en/latest/?badge=latest)

# pgmq-sqlalchemy

Python client using **sqlalchemy ORM** for the PGMQ Postgres extension.

支援 **SQLAlchemy ORM** 的 Python 客戶端 <br>
用於 [PGMQ Postgres 插件](https://github.com/tembo-io/pgmq) 。

## Features

- 支援 **async** 和 **sync** `engines`、`sessionmakers`，或由 `dsn` 構建。
- 支援所有 sqlalchemy 支持的 postgres DBAPIs。
    > 例如：`psycopg`, `psycopg2`, `asyncpg`
    > 可見 [SQLAlchemy Postgresql Dialects](https://docs.sqlalhttps://docs.sqlalchemy.org/en/20/dialects/postgresql.html)


