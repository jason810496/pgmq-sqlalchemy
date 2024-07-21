[![Poetry](https://img.shields.io/endpoint?url=https://python-poetry.org/badge/v0.json)](https://python-poetry.org/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
![PyPI - Version](https://img.shields.io/pypi/v/pgmq-sqlalchemy)
[![PyPI - License](https://img.shields.io/pypi/l/pgmq-sqlalchemy.svg)](https://github.com/pgmq-sqlalchemy/pgmq-sqlalchemy-python/blob/main/LICENSE)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pgmq-sqlalchemy.svg)](https://pypi.python.org/pypi/pgmq-sqlalchemy)
[![codecov](https://codecov.io/gh/jason810496/pgmq-sqlalchemy/graph/badge.svg?token=C5ZVZCW7TE)](https://codecov.io/gh/jason810496/pgmq-sqlalchemy)
[![Docs](https://readthedocs.org/projects/pgmq-sqlalchemy-python/badge/?version=latest)](http://pgmq-sqlalchemy-python.readthedocs.io/en/latest/?badge=latest)

# pgmq-sqlalchemy

More flexible [PGMQ Postgres extension](https://github.com/tembo-io/pgmq) Python client that using **sqlalchemy ORM**, supporting both **async** and **sync** `engines`, `sessionmakers` or built from `dsn`.

## Table of Contents

* [pgmq-sqlalchemy](#pgmq-sqlalchemy)
   * [Features](#features)
   * [Installation](#installation)
   * [Getting Started](#getting-started)
      * [Postgres Setup](#postgres-setup)
      * [Usage](#usage)
   * [Issue/ Contributing / Development](#issue-contributing--development)
   * [TODO](#todo)


## Features

- Supports **async** and **sync** `engines` and `sessionmakers`, or built from `dsn`.
- Supports **all postgres DBAPIs supported by sqlalchemy**.
    > e.g. `psycopg`, `psycopg2`, `asyncpg` .. <br>
    > See [SQLAlchemy Postgresql Dialects](https://docs.sqlalhttps://docs.sqlalchemy.org/en/20/dialects/postgresql.html)

## Installation

Install with pip:

```bash
pip install pgmq-sqlalchemy
```

Install with additional DBAPIs packages:

```bash
pip install pgmq-sqlalchemy[psycopg2]
pip install pgmq-sqlalchemy[asyncpg]
```

## Getting Started

### Postgres Setup

Prerequisites: **Postgres** with **PGMQ** extension installed. <br>
For quick setup: 
```bash
docker run -d --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 quay.io/tembo/pg16-pgmq:latest
```
> For more information, see [PGMQ](https://github.com/tembo-io/pgmq)

### Usage

> [!NOTE]  
> Check [pgmq-sqlalchemy Document](https://pgmq-sqlalchemy-python.readthedocs.io/en/latest/) for more examples and detailed usage.


`dispatcher.py`:
```python
from pgmq_sqlalchemy import PGMQueue

pgmq = PGMQueue(dsn='postgresql+psycopg://postgres:postgres@localhost:5432/postgres')
pgmq.create_queue('my_queue')

pgmq.send('my_queue', {'key': 'value'})
```

`consumer.py`:
```python
from pgmq_sqlalchemy import PGMQueue
from pgmq_sqlalchemy.schema import Message

pgmq = PGMQueue(dsn='postgresql+psycopg://postgres:postgres@localhost:5432/postgres')
msg:Message = pgmq.read('my_queue')

if msg:
    print(msg.msg_id)
    print(msg.message)
```

## Issue/ Contributing / Development 

Welcome to open an issue or pull request ! <br>
See [`Development` on Online Document](https://pgmq-sqlalchemy-python.readthedocs.io/en/latest/)or [CONTRIBUTING.md](.github/CONTRIBUTING.md) for more information.

## TODO 

- [ ] Add **time-based** partition option and validation to `create_partitioned_queue` method.
- [ ] Read(single/batch) Archive Table ( `read_archive` method )
- [ ] Detach Archive Table ( `detach_archive` method )
- [ ] Add `set_vt` utils method.