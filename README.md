[![Poetry](https://img.shields.io/endpoint?url=https://python-poetry.org/badge/v0.json)](https://python-poetry.org/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
![PyPI - Version](https://img.shields.io/pypi/v/pgmq-sqlalchemy)
[![PyPI - License](https://img.shields.io/pypi/l/pgmq-sqlalchemy.svg)](https://github.com/jason810496/pgmq-sqlalchemy/blob/main/LICENSE)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pgmq-sqlalchemy.svg)](https://pypi.python.org/pypi/pgmq-sqlalchemy)
[![codecov](https://codecov.io/gh/jason810496/pgmq-sqlalchemy/graph/badge.svg?token=C5ZVZCW7TE)](https://codecov.io/gh/jason810496/pgmq-sqlalchemy)
[![Documentation Status](https://readthedocs.org/projects/pgmq-sqlalchemy/badge/?version=latest)](https://pgmq-sqlalchemy.readthedocs.io/en/latest/?badge=latest)


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
- **Automatically** creates `pgmq` (or `pg_partman`) extension on the database if not exists.
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
pip install "pgmq-sqlalchemy[asyncpg]"
pip install "pgmq-sqlalchemy[psycopg2-binary]"
# pip install "pgmq-sqlalchemy[postgres-python-driver]"
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
> Check [pgmq-sqlalchemy Document](https://pgmq-sqlalchemy.readthedocs.io/en/latest/) for more examples and detailed usage.


For `dispatcher.py`:
```python
from typing import List
from pgmq_sqlalchemy import PGMQueue

postgres_dsn = 'postgresql://postgres:postgres@localhost:5432/postgres'

pgmq = PGMQueue(dsn=postgres_dsn)
pgmq.create_queue('my_queue')

msg = {'key': 'value', 'key2': 'value2'}
msg_id:int = pgmq.send('my_queue', msg)

# could also send a list of messages
msg_ids:List[int] = pgmq.send_batch('my_queue', [msg, msg])
```

For `consumer.py`:
```python
from pgmq_sqlalchemy import PGMQueue
from pgmq_sqlalchemy.schema import Message

postgres_dsn = 'postgresql://postgres:postgres@localhost:5432/postgres'

pgmq = PGMQueue(dsn=postgres_dsn)

# read a single message
msg:Message = pgmq.read('my_queue')

# read a batch of messages
msgs:List[Message] = pgmq.read_batch('my_queue', 10)
```

For `monitor.py`:
```python
from pgmq_sqlalchemy import PGMQueue
from pgmq_sqlalchemy.schema import QueueMetrics

postgres_dsn = 'postgresql://postgres:postgres@localhost:5432/postgres'

pgmq = PGMQueue(dsn=postgres_dsn)

# get queue metrics
metrics:QueueMetrics = pgmq.metrics('my_queue')
print(metrics.queue_length)
print(metrics.total_messages)
```

## Issue/ Contributing / Development 

Welcome to open an issue or pull request ! <br>
See [`Development` on Online Document](https://pgmq-sqlalchemy.readthedocs.io/en/latest/) or [CONTRIBUTING.md](.github/CONTRIBUTING.md) for more information.

## TODO 

- [ ] Add **time-based** partition option and validation to `create_partitioned_queue` method.
- [ ] Read(single/batch) Archive Table ( `read_archive` method )
- [ ] Detach Archive Table ( `detach_archive` method )
- [ ] Add `set_vt` utils method.