import pytest
import uuid
from psycopg2 import pool
import asyncpg
import psycopg_pool
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession
import pgmq_sqlalchemy

queue_name = "test_queue"


def get_queue_name():
    return f"test_queue_{uuid.uuid4().hex}"


def test_psycopg2_create_queue(
    get_sa_host, get_sa_port, get_sa_user, get_sa_password, get_sa_db
):
    # Create psycopg2 connection pool
    connection_pool = pool.ThreadedConnectionPool(
        1,
        5,
        host=get_sa_host,
        port=get_sa_port,
        user=get_sa_user,
        password=get_sa_password,
        database=get_sa_db,
    )

    # Get a connection from the pool and create a cursor
    conn = connection_pool.getconn()
    cursor = conn.cursor()

    # TODO: Add your test logic here
    pgmq_sqlalchemy.func.create_queue(cursor, get_queue_name())

    # Close the cursor and return the connection to the pool
    cursor.close()
    conn.commit()
    connection_pool.putconn(conn)


def test_psycopg_create_queue(
    get_sa_host, get_sa_port, get_sa_user, get_sa_password, get_sa_db
):
    # Create psycopg connection pool
    connection_pool = psycopg_pool.ConnectionPool(
        f"host={get_sa_host} port={get_sa_port} user={get_sa_user} password={get_sa_password} dbname={get_sa_db}",
        min_size=1,
        max_size=5,
    )

    conn = connection_pool.getconn()

    pgmq_sqlalchemy.func.create_queue(conn, get_queue_name())
    conn.commit()
    connection_pool.putconn(conn)


@pytest.mark.asyncio
async def test_asyncpg_create_queue(
    get_sa_host, get_sa_port, get_sa_user, get_sa_password, get_sa_db
):
    # Create asyncpg connection pool
    connection_pool = await asyncpg.create_pool(
        f"postgresql://{get_sa_user}:{get_sa_password}@{get_sa_host}:{get_sa_port}/{get_sa_db}",
        min_size=1,
        max_size=5,
    )
    conn: asyncpg.Connection = await connection_pool.acquire()

    await pgmq_sqlalchemy.async_func.create_queue(conn, get_queue_name())
    await connection_pool.release(conn)


def test_sqlalchemy_psycopg_create_queue(
    get_sa_host, get_sa_port, get_sa_user, get_sa_password, get_sa_db
):
    # Create SQLAlchemy engine with connection pool
    engine = create_engine(
        f"postgresql+psycopg://{get_sa_user}:{get_sa_password}@{get_sa_host}:{get_sa_port}/{get_sa_db}",
        pool_size=5,
        max_overflow=10,
    )

    # Get a connection from the pool and create a cursor
    with engine.connect() as conn:
        with conn.connection.cursor() as cursor:
            # TODO: Add your test logic here
            pgmq_sqlalchemy.func.create_queue(cursor, get_queue_name())


# Remove or update this function if not needed
def test_sqlalchemy_psycopg2_create_queue(
    get_sa_host, get_sa_port, get_sa_user, get_sa_password, get_sa_db
):
    # Create SQLAlchemy engine with connection pool
    engine = create_engine(
        f"postgresql+psycopg2://{get_sa_user}:{get_sa_password}@{get_sa_host}:{get_sa_port}/{get_sa_db}",
        pool_size=5,
        max_overflow=10,
    )

    # Get a connection from the pool
    with engine.connect() as conn:
        # Create a cursor using the raw DBAPI connection
        with conn.connection.cursor() as cursor:
            # TODO: Add your test logic here
            pgmq_sqlalchemy.func.create_queue(cursor, get_queue_name())


@pytest.mark.asyncio
async def test_sqlalchemy_asyncpg_create_queue(
    get_sa_host, get_sa_port, get_sa_user, get_sa_password, get_sa_db
):
    # Create SQLAlchemy engine with connection pool
    engine = create_async_engine(
        f"postgresql+asyncpg://{get_sa_user}:{get_sa_password}@{get_sa_host}:{get_sa_port}/{get_sa_db}",
        pool_size=5,
        max_overflow=10,
    )
    sql, params = pgmq_sqlalchemy.statement.sqlalchemy_statement.create_queue(
        get_queue_name()
    )
    print("test_sqlalchemy_asyncpg_create_queue")
    print(sql)
    print(params)
    async with engine.connect() as conn:
        # async with conn.connection.cursor() as cursor:
        #     await cursor.execute(
        #     text("select pgmq.create(:queue_name);"), {"queue_name": get_queue_name()}
        #     )
        # await conn.commit()

        await conn.execute(sql, params)


def test_sqlalchemy_session_maker_psycopg_create_queue(
    get_sa_host, get_sa_port, get_sa_user, get_sa_password, get_sa_db
):
    # Create SQLAlchemy engine with connection pool
    engine = create_engine(
        f"postgresql+psycopg://{get_sa_user}:{get_sa_password}@{get_sa_host}:{get_sa_port}/{get_sa_db}",
        pool_size=5,
        max_overflow=10,
    )
    sql, params = pgmq_sqlalchemy.statement.sqlalchemy_statement.create_queue(
        get_queue_name()
    )
    session_maker = sessionmaker(engine)
    session = session_maker()
    session.execute(sql, params)
    session.commit()
    session.close()


def test_sqlalchemy_session_maker_psycopg2_create_queue(
    get_sa_host, get_sa_port, get_sa_user, get_sa_password, get_sa_db
):
    # Create SQLAlchemy engine with connection pool
    engine = create_engine(
        f"postgresql+psycopg2://{get_sa_user}:{get_sa_password}@{get_sa_host}:{get_sa_port}/{get_sa_db}",
        pool_size=5,
        max_overflow=10,
    )
    sql, params = pgmq_sqlalchemy.statement.sqlalchemy_statement.create_queue(
        get_queue_name()
    )
    session_maker = sessionmaker(engine)
    session = session_maker()
    session.execute(sql, params)
    session.commit()


@pytest.mark.asyncio
async def test_sqlalchemy_session_maker_asyncpg_create_queue(
    get_sa_host, get_sa_port, get_sa_user, get_sa_password, get_sa_db
):
    # Create SQLAlchemy engine with connection pool
    engine = create_async_engine(
        f"postgresql+asyncpg://{get_sa_user}:{get_sa_password}@{get_sa_host}:{get_sa_port}/{get_sa_db}",
        pool_size=5,
        max_overflow=10,
    )
    sql, params = pgmq_sqlalchemy.statement.sqlalchemy_statement.create_queue(
        get_queue_name()
    )
    session_maker = sessionmaker(engine, class_=AsyncSession)
    print("test_sqlalchemy_session_maker_asyncpg_create_queue")
    print(sql)
    print(params)
    async with session_maker() as session:
        await session.execute(sql, params)
        await session.commit()
