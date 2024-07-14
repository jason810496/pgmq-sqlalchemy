# pgmq-sqlalchemy

Python client using **sqlalchemy ORM** for the PGMQ Postgres extension.

支援 **SQLAlchemy ORM** 的 Python 客戶端 <br>
用於 [PGMQ Postgres 插件](https://github.com/tembo-io/pgmq) 。

## Features

- 支援 **async** 和 **sync** `engines`、`sessionmakers`，或由 `dsn` 構建。
- 支援所有 sqlalchemy 支持的 postgres DBAPIs。
    > 例如：`psycopg`, `psycopg2`, `asyncpg`
    > 可見 [SQLAlchemy Postgresql Dialects](https://docs.sqlalhttps://docs.sqlalchemy.org/en/20/dialects/postgresql.html)