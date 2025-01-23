import asyncio
from typing import List, Optional, Literal

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine

from .schema import Message, QueueMetrics
from ._types import ENGINE_TYPE
from ._utils import (
    get_session_type,
    is_async_session_maker,
    is_async_dsn,
    encode_dict_to_psql,
    encode_list_to_psql,
)

DIALECTS_TYPE = Literal[
    "sqlalchemy",
    "asyncpg",
    "psycopg2",
    "psycopg3",
]


class PGMQueue:
    engine: ENGINE_TYPE = None
    session_maker: sessionmaker = None
    delay: int = 0
    vt: int = 30

    is_async: bool = False
    is_pg_partman_ext_checked: bool = False
    loop: asyncio.AbstractEventLoop = None

    def __init__(
        self,
        # dialect: DIALECTS_TYPE,
        # for sqlalchemy
        dsn: Optional[str] = None,
        engine: Optional[ENGINE_TYPE] = None,
        session_maker: Optional[sessionmaker] = None,
        # for other db api drivers
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
    ) -> None:
        """

        | There are **3** ways to initialize ``PGMQueue`` class:
        | 1. Initialize with a ``dsn``:

        .. code-block:: python

            from pgmq_sqlalchemy import PGMQueue

            pgmq_client = PGMQueue(dsn='postgresql+psycopg://postgres:postgres@localhost:5432/postgres')
            # or async dsn
            async_pgmq_client = PGMQueue(dsn='postgresql+asyncpg://postgres:postgres@localhost:5432/postgres')

        | 2. Initialize with an ``engine`` or ``async_engine``:

        .. code-block:: python

            from pgmq_sqlalchemy import PGMQueue
            from sqlalchemy import create_engine
            from sqlalchemy.ext.asyncio import create_async_engine

            engine = create_engine('postgresql+psycopg://postgres:postgres@localhost:5432/postgres')
            pgmq_client = PGMQueue(engine=engine)
            # or async engine
            async_engine = create_async_engine('postgresql+asyncpg://postgres:postgres@localhost:5432/postgres')
            async_pgmq_client = PGMQueue(engine=async_engine)

        | 3. Initialize with a ``session_maker``:

        .. code-block:: python

            from pgmq_sqlalchemy import PGMQueue
            from sqlalchemy.orm import sessionmaker
            from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

            engine = create_engine('postgresql+psycopg://postgres:postgres@localhost:5432/postgres')
            session_maker = sessionmaker(bind=engine)
            pgmq_client = PGMQueue(session_maker=session_maker)
            # or async session_maker
            async_engine = create_async_engine('postgresql+asyncpg://postgres:postgres@localhost:5432/post
            async_session_maker = sessionmaker(bind=async_engine, class_=AsyncSession)
            async_pgmq_client = PGMQueue(session_maker=async_session_maker)

        .. note::
            | ``PGMQueue`` will **auto create** the ``pgmq`` extension ( and ``pg_partman`` extension if the method is related with **partitioned_queue** ) if it does not exist in the Postgres.
            | But you must make sure that the ``pgmq`` extension ( or ``pg_partman`` extension ) already **installed** in the Postgres.
        """
        if not dsn and not engine and not session_maker:
            raise ValueError("Must provide either dsn, engine, or session_maker")
        # initialize the engine and session_maker
        if session_maker:
            self.session_maker = session_maker
            self.is_async = is_async_session_maker(session_maker)
        elif engine:
            self.engine = engine
            self.is_async = self.engine.dialect.is_async
            self.session_maker = sessionmaker(
                bind=self.engine, class_=get_session_type(self.engine)
            )
        else:
            self.engine = (
                create_async_engine(dsn) if is_async_dsn(dsn) else create_engine(dsn)
            )
            self.is_async = self.engine.dialect.is_async
            self.session_maker = sessionmaker(
                bind=self.engine, class_=get_session_type(self.engine)
            )

        if self.is_async:
            self.loop = asyncio.new_event_loop()

        # create pgmq extension if not exists
        self._check_pgmq_ext()

    async def _check_pgmq_ext_async(self) -> None:
        """Check if the pgmq extension exists."""
        async with self.session_maker() as session:
            await session.execute(text("create extension if not exists pgmq cascade;"))
            await session.commit()

    def _check_pgmq_ext_sync(self) -> None:
        """Check if the pgmq extension exists."""
        with self.session_maker() as session:
            session.execute(text("create extension if not exists pgmq cascade;"))
            session.commit()

    def _check_pgmq_ext(self) -> None:
        """Check if the pgmq extension exists."""
        if self.is_async:
            return self.loop.run_until_complete(self._check_pgmq_ext_async())
        return self._check_pgmq_ext_sync()

    async def _check_pg_partman_ext_async(self) -> None:
        """Check if the pg_partman extension exists."""
        async with self.session_maker() as session:
            await session.execute(
                text("create extension if not exists pg_partman cascade;")
            )
            await session.commit()

    def _check_pg_partman_ext_sync(self) -> None:
        """Check if the pg_partman extension exists."""
        with self.session_maker() as session:
            session.execute(text("create extension if not exists pg_partman cascade;"))
            session.commit()

    def _check_pg_partman_ext(self) -> None:
        """Check if the pg_partman extension exists."""
        if self.is_pg_partman_ext_checked:
            return
        self.is_pg_partman_ext_checked

        if self.is_async:
            return self.loop.run_until_complete(self._check_pg_partman_ext_async())
        return self._check_pg_partman_ext_sync()

    def _create_queue_sync(self, queue_name: str, unlogged: bool = False) -> None:
        """ """
        with self.session_maker() as session:
            if unlogged:
                session.execute(
                    text("select pgmq.create_unlogged(:queue);"), {"queue": queue_name}
                )
            else:
                session.execute(
                    text("select pgmq.create(:queue);"), {"queue": queue_name}
                )
            session.commit()

    async def _create_queue_async(
        self, queue_name: str, unlogged: bool = False
    ) -> None:
        """Create a new queue."""
        async with self.session_maker() as session:
            if unlogged:
                await session.execute(
                    text("select pgmq.create_unlogged(:queue);"), {"queue": queue_name}
                )
            else:
                await session.execute(
                    text("select pgmq.create(:queue);"), {"queue": queue_name}
                )
            await session.commit()

    def create_queue(self, queue_name: str, unlogged: bool = False) -> None:
        # doc.CREATE_QUEUE_DOCSTRING_BEFORE
        """

        .. code-block:: python

            pgmq_client.create_queue('my_queue')
            # or unlogged table queue
            pgmq_client.create_queue('my_queue', unlogged=True)

        """
        # doc.CREATE_QUEUE_DOCSTRING_AFTER

        if self.is_async:
            return self.loop.run_until_complete(
                self._create_queue_async(queue_name, unlogged)
            )
        return self._create_queue_sync(queue_name, unlogged)

    def _create_partitioned_queue_sync(
        self,
        queue_name: str,
        partition_interval: str,
        retention_interval: str,
    ) -> None:
        """Create a new partitioned queue."""
        with self.session_maker() as session:
            session.execute(
                text(
                    "select pgmq.create_partitioned(:queue_name, :partition_interval, :retention_interval);"
                ),
                {
                    "queue_name": queue_name,
                    "partition_interval": partition_interval,
                    "retention_interval": retention_interval,
                },
            )
            session.commit()

    async def _create_partitioned_queue_async(
        self,
        queue_name: str,
        partition_interval: str,
        retention_interval: str,
    ) -> None:
        """Create a new partitioned queue."""
        async with self.session_maker() as session:
            await session.execute(
                text(
                    "select pgmq.create_partitioned(:queue_name, :partition_interval, :retention_interval);"
                ),
                {
                    "queue_name": queue_name,
                    "partition_interval": partition_interval,
                    "retention_interval": retention_interval,
                },
            )
            await session.commit()

    def create_partitioned_queue(
        self,
        queue_name: str,
        partition_interval: int = 10000,
        retention_interval: int = 100000,
    ) -> None:
        # doc.CREATE_QUEUE_DOCSTRING_BEFORE
        """

        .. code-block:: python

            pgmq_client.create_partitioned_queue('my_partitioned_queue', partition_interval=10000, retention_interval=100000)

        """
        # doc.CREATE_QUEUE_DOCSTRING_AFTER

        # check if the pg_partman extension exists before creating a partitioned queue at runtime
        self._check_pg_partman_ext()

        if self.is_async:
            return self.loop.run_until_complete(
                self._create_partitioned_queue_async(
                    queue_name, str(partition_interval), str(retention_interval)
                )
            )
        return self._create_partitioned_queue_sync(
            queue_name, str(partition_interval), str(retention_interval)
        )

    def _validate_queue_name_sync(self, queue_name: str) -> None:
        """Validate the length of a queue name."""
        with self.session_maker() as session:
            session.execute(
                text("select pgmq.validate_queue_name(:queue);"), {"queue": queue_name}
            )
            session.commit()

    async def _validate_queue_name_async(self, queue_name: str) -> None:
        """Validate the length of a queue name."""
        async with self.session_maker() as session:
            await session.execute(
                text("select pgmq.validate_queue_name(:queue);"), {"queue": queue_name}
            )
            await session.commit()

    def validate_queue_name(self, queue_name: str) -> None:
        """
        * Will raise an error if the ``queue_name`` is more than 48 characters.
        """
        if self.is_async:
            return self.loop.run_until_complete(
                self._validate_queue_name_async(queue_name)
            )
        return self._validate_queue_name_sync(queue_name)

    def _drop_queue_sync(self, queue: str, partitioned: bool = False) -> bool:
        """Drop a queue."""
        with self.session_maker() as session:
            row = session.execute(
                text("select pgmq.drop_queue(:queue, :partitioned);"),
                {"queue": queue, "partitioned": partitioned},
            ).fetchone()
            session.commit()
            return row[0]

    async def _drop_queue_async(self, queue: str, partitioned: bool = False) -> bool:
        """Drop a queue."""
        async with self.session_maker() as session:
            row = (
                await session.execute(
                    text("select pgmq.drop_queue(:queue, :partitioned);"),
                    {"queue": queue, "partitioned": partitioned},
                )
            ).fetchone()
            await session.commit()
            return row[0]

    def drop_queue(self, queue: str, partitioned: bool = False) -> bool:
        # doc.DROP_QUEUE_DOCSTRING_BEFORE
        """

        .. code-block:: python

            pgmq_client.drop_queue('my_queue')
            # for partitioned queue
            pgmq_client.drop_queue('my_partitioned_queue', partitioned=True)

        """
        # doc.DROP_QUEUE_DOCSTRING_AFTER

        # check if the pg_partman extension exists before dropping a partitioned queue at runtime
        if partitioned:
            self._check_pg_partman_ext()

        if self.is_async:
            return self.loop.run_until_complete(
                self._drop_queue_async(queue, partitioned)
            )
        return self._drop_queue_sync(queue, partitioned)

    def _list_queues_sync(self) -> List[str]:
        """List all queues."""
        with self.session_maker() as session:
            rows = session.execute(
                text("select queue_name from pgmq.list_queues();")
            ).fetchall()
            session.commit()
            return [row[0] for row in rows]

    async def _list_queues_async(self) -> List[str]:
        """List all queues."""
        async with self.session_maker() as session:
            rows = (
                await session.execute(
                    text("select queue_name from pgmq.list_queues();")
                )
            ).fetchall()
            await session.commit()
            return [row[0] for row in rows]

    def list_queues(self) -> List[str]:
        # doc.LIST_QUEUES_DOCSTRING_BEFORE
        """

        .. code-block:: python

            queue_list = pgmq_client.list_queues()
            print(queue_list)
        """
        # doc.LIST_QUEUES_DOCSTRING_AFTER
        if self.is_async:
            return self.loop.run_until_complete(self._list_queues_async())
        return self._list_queues_sync()

    def _send_sync(self, queue_name: str, message: str, delay: int = 0) -> int:
        with self.session_maker() as session:
            row = (
                session.execute(
                    text(f"select * from pgmq.send('{queue_name}',{message},{delay});")
                )
            ).fetchone()
            session.commit()
        return row[0]

    async def _send_async(self, queue_name: str, message: str, delay: int = 0) -> int:
        async with self.session_maker() as session:
            row = (
                await session.execute(
                    text(f"select * from pgmq.send('{queue_name}',{message},{delay});")
                )
            ).fetchone()
            await session.commit()
        return row[0]

    def send(self, queue_name: str, message: dict, delay: int = 0) -> int:
        # doc.SEND_DOCSTRING_BEFORE
        """
        .. code-block:: python

            msg_id = pgmq_client.send('my_queue', {'key': 'value', 'key2': 'value2'})
            print(msg_id)

        Example with delay:

        .. code-block:: python

            msg_id = pgmq_client.send('my_queue', {'key': 'value', 'key2': 'value2'}, delay=10)
            msg = pgmq_client.read('my_queue')
            assert msg is None
            time.sleep(10)
            msg = pgmq_client.read('my_queue')
            assert msg is not None
        """
        if self.is_async:
            return self.loop.run_until_complete(
                self._send_async(queue_name, encode_dict_to_psql(message), delay)
            )
        return self._send_sync(queue_name, encode_dict_to_psql(message), delay)

    def _send_batch_sync(
        self, queue_name: str, messages: str, delay: int = 0
    ) -> List[int]:
        with self.session_maker() as session:
            rows = (
                session.execute(
                    text(
                        f"select * from pgmq.send_batch('{queue_name}',{messages},{delay});"
                    )
                )
            ).fetchall()
            session.commit()
        return [row[0] for row in rows]

    async def _send_batch_async(
        self, queue_name: str, messages: str, delay: int = 0
    ) -> List[int]:
        async with self.session_maker() as session:
            rows = (
                await session.execute(
                    text(
                        f"select * from pgmq.send_batch('{queue_name}',{messages},{delay});"
                    )
                )
            ).fetchall()
            await session.commit()
        return [row[0] for row in rows]

    def send_batch(
        self, queue_name: str, messages: List[dict], delay: int = 0
    ) -> List[int]:
        # doc.SEND_BATCH_DOCSTRING_BEFORE
        """

        .. code-block:: python

            msgs = [{'key': 'value', 'key2': 'value2'}, {'key': 'value', 'key2': 'value2'}]
            msg_ids = pgmq_client.send_batch('my_queue', msgs)
            print(msg_ids)
            # send with delay
            msg_ids = pgmq_client.send_batch('my_queue', msgs, delay=10)

        """
        # doc.SEND_BATCH_DOCSTRING_AFTER
        if self.is_async:
            return self.loop.run_until_complete(
                self._send_batch_async(queue_name, encode_list_to_psql(messages), delay)
            )
        return self._send_batch_sync(queue_name, encode_list_to_psql(messages), delay)

    def _read_sync(self, queue_name: str, vt: int) -> Optional[Message]:
        with self.session_maker() as session:
            row = session.execute(
                text("select * from pgmq.read(:queue_name,:vt,1);"),
                {"queue_name": queue_name, "vt": vt},
            ).fetchone()
            session.commit()
        if row is None:
            return None
        return Message(
            msg_id=row[0], read_ct=row[1], enqueued_at=row[2], vt=row[3], message=row[4]
        )

    async def _read_async(self, queue_name: str, vt: int) -> Optional[Message]:
        async with self.session_maker() as session:
            row = (
                await session.execute(
                    text("select * from pgmq.read(:queue_name,:vt,1);"),
                    {"queue_name": queue_name, "vt": vt},
                )
            ).fetchone()
            await session.commit()
        if row is None:
            return None
        return Message(
            msg_id=row[0], read_ct=row[1], enqueued_at=row[2], vt=row[3], message=row[4]
        )

    def read(self, queue_name: str, vt: Optional[int] = None) -> Optional[Message]:
        # doc.READ_DOCSTRING_BEFORE
        """

        .. code-block:: python

        .. code-block:: python

            from pgmq_sqlalchemy.schema import Message

            msg:Message = pgmq_client.read('my_queue')
            print(msg.msg_id)
            print(msg.message)
            print(msg.read_ct) # read count, how many times the message has been read

        Example with ``vt``:

        .. code-block:: python

            # assert `read_vt_demo` is empty
            pgmq_client.send('read_vt_demo', {'key': 'value', 'key2': 'value2'})
            msg = pgmq_client.read('read_vt_demo', vt=10)
            assert msg is not None

            # try to read immediately
            msg = pgmq_client.read('read_vt_demo')
            assert msg is None # will return None because the message is still invisible

            # try to read after 5 seconds
            time.sleep(5)
            msg = pgmq_client.read('read_vt_demo')
            assert msg is None # still invisible after 5 seconds

             # try to read after 11 seconds
            time.sleep(6)
            msg = pgmq_client.read('read_vt_demo')
            assert msg is not None # the message is visible after 10 seconds


        """
        if self.is_async:
            return self.loop.run_until_complete(self._read_async(queue_name, vt))
        return self._read_sync(queue_name, vt)

    def _read_batch_sync(
        self,
        queue_name: str,
        vt: int,
        batch_size: int = 1,
    ) -> Optional[List[Message]]:
        if vt is None:
            vt = self.vt
        with self.session_maker() as session:
            rows = session.execute(
                text("select * from pgmq.read(:queue_name,:vt,:batch_size);"),
                {
                    "queue_name": queue_name,
                    "vt": vt,
                    "batch_size": batch_size,
                },
            ).fetchall()
            session.commit()
        if not rows:
            return None
        return [
            Message(
                msg_id=row[0],
                read_ct=row[1],
                enqueued_at=row[2],
                vt=row[3],
                message=row[4],
            )
            for row in rows
        ]

    async def _read_batch_async(
        self,
        queue_name: str,
        vt: int,
        batch_size: int = 1,
    ) -> Optional[List[Message]]:
        async with self.session_maker() as session:
            rows = (
                await session.execute(
                    text("select * from pgmq.read(:queue_name,:vt,:batch_size);"),
                    {
                        "queue_name": queue_name,
                        "vt": vt,
                        "batch_size": batch_size,
                    },
                )
            ).fetchall()
            await session.commit()
        if not rows:
            return None
        return [
            Message(
                msg_id=row[0],
                read_ct=row[1],
                enqueued_at=row[2],
                vt=row[3],
                message=row[4],
            )
            for row in rows
        ]

    def read_batch(
        self,
        queue_name: str,
        batch_size: int = 1,
        vt: Optional[int] = None,
    ) -> Optional[List[Message]]:
        # doc.READ_BATCH_DOCSTRING_BEFORE
        """

        .. code-block:: python

            from pgmq_sqlalchemy.schema import Message

            msgs:List[Message] = pgmq_client.read_batch('my_queue', batch_size=10)
            # with vt
            msgs:List[Message] = pgmq_client.read_batch('my_queue', batch_size=10, vt=10)

        """
        if vt is None:
            vt = self.vt
        if self.is_async:
            return self.loop.run_until_complete(
                self._read_batch_async(queue_name, batch_size, vt)
            )
        return self._read_batch_sync(queue_name, batch_size, vt)

    def _read_with_poll_sync(
        self,
        queue_name: str,
        vt: int,
        qty: int = 1,
        max_poll_seconds: int = 5,
        poll_interval_ms: int = 100,
    ) -> Optional[List[Message]]:
        """Read messages from a queue with polling."""
        with self.session_maker() as session:
            rows = session.execute(
                text(
                    "select * from pgmq.read_with_poll(:queue_name,:vt,:qty,:max_poll_seconds,:poll_interval_ms);"
                ),
                {
                    "queue_name": queue_name,
                    "vt": vt,
                    "qty": qty,
                    "max_poll_seconds": max_poll_seconds,
                    "poll_interval_ms": poll_interval_ms,
                },
            ).fetchall()
            session.commit()
        if not rows:
            return None
        return [
            Message(
                msg_id=row[0],
                read_ct=row[1],
                enqueued_at=row[2],
                vt=row[3],
                message=row[4],
            )
            for row in rows
        ]

    async def _read_with_poll_async(
        self,
        queue_name: str,
        vt: int,
        qty: int = 1,
        max_poll_seconds: int = 5,
        poll_interval_ms: int = 100,
    ) -> Optional[List[Message]]:
        """Read messages from a queue with polling."""
        async with self.session_maker() as session:
            rows = (
                await session.execute(
                    text(
                        "select * from pgmq.read_with_poll(:queue_name,:vt,:qty,:max_poll_seconds,:poll_interval_ms);"
                    ),
                    {
                        "queue_name": queue_name,
                        "vt": vt,
                        "qty": qty,
                        "max_poll_seconds": max_poll_seconds,
                        "poll_interval_ms": poll_interval_ms,
                    },
                )
            ).fetchall()
            await session.commit()
        if not rows:
            return None
        return [
            Message(
                msg_id=row[0],
                read_ct=row[1],
                enqueued_at=row[2],
                vt=row[3],
                message=row[4],
            )
            for row in rows
        ]

    def read_with_poll(
        self,
        queue_name: str,
        vt: Optional[int] = None,
        qty: int = 1,
        max_poll_seconds: int = 5,
        poll_interval_ms: int = 100,
    ) -> Optional[List[Message]]:
        # doc.READ_WITH_POLL_DOCSTRING_BEFORE
        """

        Usage:

        .. code-block:: python

            msg_id = pgmq_client.send('my_queue', {'key': 'value'}, delay=6)

            # the following code will block for 5 seconds
            msgs = pgmq_client.read_with_poll('my_queue', qty=1, max_poll_seconds=5, poll_interval_ms=100)
            assert msgs is None

            # try read_with_poll again
            # the following code will only block for 1 second
            msgs = pgmq_client.read_with_poll('my_queue', qty=1, max_poll_seconds=5, poll_interval_ms=100)
            assert msgs is not None

        Another example:

        .. code-block:: python

            msg = {'key': 'value'}
            msg_ids = pgmq_client.send_batch('my_queue', [msg, msg, msg, msg], delay=3)

            # the following code will block for 3 seconds
            msgs = pgmq_client.read_with_poll('my_queue', qty=3, max_poll_seconds=5, poll_interval_ms=100)
            assert len(msgs) == 3 # will read at most 3 messages (qty=3)

        """
        if vt is None:
            vt = self.vt

        if self.is_async:
            return self.loop.run_until_complete(
                self._read_with_poll_async(
                    queue_name, vt, qty, max_poll_seconds, poll_interval_ms
                )
            )
        return self._read_with_poll_sync(
            queue_name, vt, qty, max_poll_seconds, poll_interval_ms
        )

    def _pop_sync(self, queue_name: str) -> Optional[Message]:
        with self.session_maker() as session:
            row = session.execute(
                text("select * from pgmq.pop(:queue_name);"),
                {"queue_name": queue_name},
            ).fetchone()
            session.commit()
        if row is None:
            return None
        return Message(
            msg_id=row[0], read_ct=row[1], enqueued_at=row[2], vt=row[3], message=row[4]
        )

    async def _pop_async(self, queue_name: str) -> Optional[Message]:
        async with self.session_maker() as session:
            row = (
                await session.execute(
                    text("select * from pgmq.pop(:queue_name);"),
                    {"queue_name": queue_name},
                )
            ).fetchone()
            await session.commit()
        if row is None:
            return None
        return Message(
            msg_id=row[0], read_ct=row[1], enqueued_at=row[2], vt=row[3], message=row[4]
        )

    def pop(self, queue_name: str) -> Optional[Message]:
        # doc.POP_DOCSTRING_BEFORE
        """

        .. code-block:: python

            msg = pgmq_client.pop('my_queue')
            print(msg.msg_id)
            print(msg.message)

        """
        if self.is_async:
            return self.loop.run_until_complete(self._pop_async(queue_name))
        return self._pop_sync(queue_name)

    def _delete_sync(
        self,
        queue_name: str,
        msg_id: int,
    ) -> bool:
        with self.session_maker() as session:
            # should add explicit type casts to choose the correct candidate function
            row = session.execute(
                text(f"select * from pgmq.delete('{queue_name}',{msg_id}::BIGINT);")
            ).fetchone()
            session.commit()
        return row[0]

    async def _delete_async(
        self,
        queue_name: str,
        msg_id: int,
    ) -> bool:
        async with self.session_maker() as session:
            # should add explicit type casts to choose the correct candidate function
            row = (
                await session.execute(
                    text(f"select * from pgmq.delete('{queue_name}',{msg_id}::BIGINT);")
                )
            ).fetchone()
            await session.commit()
        return row[0]

    def delete(self, queue_name: str, msg_id: int) -> bool:
        # doc.DELETE_DOCSTRING_BEFORE
        """

        .. code-block:: python

            msg_id = pgmq_client.send('my_queue', {'key': 'value'})
            assert pgmq_client.delete('my_queue', msg_id)
            assert not pgmq_client.delete('my_queue', msg_id)

        """
        if self.is_async:
            return self.loop.run_until_complete(self._delete_async(queue_name, msg_id))
        return self._delete_sync(queue_name, msg_id)

    def _delete_batch_sync(
        self,
        queue_name: str,
        msg_ids: List[int],
    ) -> List[int]:
        # should add explicit type casts to choose the correct candidate function
        with self.session_maker() as session:
            rows = session.execute(
                text(f"select * from pgmq.delete('{queue_name}',ARRAY{msg_ids});")
            ).fetchall()
            session.commit()
        return [row[0] for row in rows]

    async def _delete_batch_async(
        self,
        queue_name: str,
        msg_ids: List[int],
    ) -> List[int]:
        # should add explicit type casts to choose the correct candidate function
        async with self.session_maker() as session:
            rows = (
                await session.execute(
                    text(f"select * from pgmq.delete('{queue_name}',ARRAY{msg_ids});")
                )
            ).fetchall()
            await session.commit()
        return [row[0] for row in rows]

    def delete_batch(self, queue_name: str, msg_ids: List[int]) -> List[int]:
        # doc.DELETE_BATCH_DOCSTRING_BEFORE
        """

        .. code-block:: python

            msg_ids = pgmq_client.send_batch('my_queue', [{'key': 'value'}, {'key': 'value'}])
            assert pgmq_client.delete_batch('my_queue', msg_ids) == msg_ids

        """
        if self.is_async:
            return self.loop.run_until_complete(
                self._delete_batch_async(queue_name, msg_ids)
            )
        return self._delete_batch_sync(queue_name, msg_ids)

    def _archive_sync(self, queue_name: str, msg_id: int) -> bool:
        """Archive a message from a queue synchronously."""
        with self.session_maker() as session:
            row = session.execute(
                text(f"select pgmq.archive('{queue_name}',{msg_id}::BIGINT);")
            ).fetchone()
            session.commit()
        return row[0]

    async def _archive_async(self, queue_name: str, msg_id: int) -> bool:
        """Archive a message from a queue asynchronously."""
        async with self.session_maker() as session:
            row = (
                await session.execute(
                    text(f"select pgmq.archive('{queue_name}',{msg_id}::BIGINT);")
                )
            ).fetchone()
            await session.commit()
        return row[0]

    def archive(self, queue_name: str, msg_id: int) -> bool:
        # doc.ARCHIVE_DOCSTRING_BEFORE
        """

        .. code-block:: python

            msg_id = pgmq_client.send('my_queue', {'key': 'value'})
            assert pgmq_client.archive('my_queue', msg_id)
            # since the message is archived, queue will be empty
            assert pgmq_client.read('my_queue') is None

        """
        if self.is_async:
            return self.loop.run_until_complete(self._archive_async(queue_name, msg_id))
        return self._archive_sync(queue_name, msg_id)

    def _archive_batch_sync(self, queue_name: str, msg_ids: List[int]) -> List[int]:
        """Archive multiple messages from a queue synchronously."""
        with self.session_maker() as session:
            rows = session.execute(
                text(f"select * from pgmq.archive('{queue_name}',ARRAY{msg_ids});")
            ).fetchall()
            session.commit()
        return [row[0] for row in rows]

    async def _archive_batch_async(
        self, queue_name: str, msg_ids: List[int]
    ) -> List[int]:
        """Archive multiple messages from a queue asynchronously."""
        async with self.session_maker() as session:
            rows = (
                await session.execute(
                    text(f"select * from pgmq.archive('{queue_name}',ARRAY{msg_ids});")
                )
            ).fetchall()
            await session.commit()
        return [row[0] for row in rows]

    def archive_batch(self, queue_name: str, msg_ids: List[int]) -> List[int]:
        # doc.ARCHIVE_BATCH_DOCSTRING_BEFORE
        """

        .. code-block:: python

            msg_ids = pgmq_client.send_batch('my_queue', [{'key': 'value'}, {'key': 'value'}])
            assert pgmq_client.archive_batch('my_queue', msg_ids) == msg_ids
            assert pgmq_client.read('my_queue') is None

        """
        if self.is_async:
            return self.loop.run_until_complete(
                self._archive_batch_async(queue_name, msg_ids)
            )
        return self._archive_batch_sync(queue_name, msg_ids)

    def _purge_sync(self, queue_name: str) -> int:
        """Purge a queue synchronously,return deleted_count."""
        with self.session_maker() as session:
            row = session.execute(
                text("select pgmq.purge_queue(:queue_name);"),
                {"queue_name": queue_name},
            ).fetchone()
            session.commit()
        return row[0]

    async def _purge_async(self, queue_name: str) -> int:
        """Purge a queue asynchronously,return deleted_count."""
        async with self.session_maker() as session:
            row = (
                await session.execute(
                    text("select pgmq.purge_queue(:queue_name);"),
                    {"queue_name": queue_name},
                )
            ).fetchone()
            await session.commit()
        return row[0]

    def purge(self, queue_name: str) -> int:
        # doc.PURGE_DOCSTRING_BEFORE
        """

        .. code-block:: python

            msg_ids = pgmq_client.send_batch('my_queue', [{'key': 'value'}, {'key': 'value'}])
            assert pgmq_client.purge('my_queue') == 2
            assert pgmq_client.read('my_queue') is None

        """
        if self.is_async:
            return self.loop.run_until_complete(self._purge_async(queue_name))
        return self._purge_sync(queue_name)

    def _metrics_sync(self, queue_name: str) -> Optional[QueueMetrics]:
        """Get queue metrics synchronously."""
        with self.session_maker() as session:
            row = session.execute(
                text("select * from pgmq.metrics(:queue_name);"),
                {"queue_name": queue_name},
            ).fetchone()
            session.commit()
        if row is None:
            return None
        return QueueMetrics(
            queue_name=row[0],
            queue_length=row[1],
            newest_msg_age_sec=row[2],
            oldest_msg_age_sec=row[3],
            total_messages=row[4],
        )

    async def _metrics_async(self, queue_name: str) -> Optional[QueueMetrics]:
        """Get queue metrics asynchronously."""
        async with self.session_maker() as session:
            row = (
                await session.execute(
                    text("select * from pgmq.metrics(:queue_name);"),
                    {"queue_name": queue_name},
                )
            ).fetchone()
        if row is None:
            return None
        return QueueMetrics(
            queue_name=row[0],
            queue_length=row[1],
            newest_msg_age_sec=row[2],
            oldest_msg_age_sec=row[3],
            total_messages=row[4],
        )

    def metrics(self, queue_name: str) -> Optional[QueueMetrics]:
        # doc.METRICS_DOCSTRING_BEFORE
        """

        Usage:

        .. code-block:: python

            from pgmq_sqlalchemy.schema import QueueMetrics

            metrics:QueueMetrics = pgmq_client.metrics('my_queue')
            print(metrics.queue_name)
            print(metrics.queue_length)
            print(metrics.queue_length)

        """
        if self.is_async:
            return self.loop.run_until_complete(self._metrics_async(queue_name))
        return self._metrics_sync(queue_name)

    def _metrics_all_sync(self) -> Optional[List[QueueMetrics]]:
        """Get metrics for all queues synchronously."""
        with self.session_maker() as session:
            rows = session.execute(text("select * from pgmq.metrics_all();")).fetchall()
        if not rows:
            return None
        return [
            QueueMetrics(
                queue_name=row[0],
                queue_length=row[1],
                newest_msg_age_sec=row[2],
                oldest_msg_age_sec=row[3],
                total_messages=row[4],
            )
            for row in rows
        ]

    async def _metrics_all_async(self) -> Optional[List[QueueMetrics]]:
        """Get metrics for all queues asynchronously."""
        async with self.session_maker() as session:
            rows = (
                await session.execute(text("select * from pgmq.metrics_all();"))
            ).fetchall()
        if not rows:
            return None
        return [
            QueueMetrics(
                queue_name=row[0],
                queue_length=row[1],
                newest_msg_age_sec=row[2],
                oldest_msg_age_sec=row[3],
                total_messages=row[4],
            )
            for row in rows
        ]

    def metrics_all(self) -> Optional[List[QueueMetrics]]:
        # doc.METRICS_ALL_DOCSTRING_BEFORE
        """

        Usage:

        .. code-block:: python

            from pgmq_sqlalchemy.schema import QueueMetrics

            metrics:List[QueueMetrics] = pgmq_client.metrics_all()
            for m in metrics:
                print(m.queue_name)
                print(m.queue_length)
                print(m.queue_length)

        .. warning::
            | You should use a **distributed lock** to avoid **race conditions** when calling |metrics_all_method|_ in **concurrent** |drop_queue_method|_ **scenarios**.
            |
            | Since the default PostgreSQL isolation level is |read_committed_isolation_level|_, the queue metrics to be fetched **may not exist** if there are **concurrent** |drop_queue_method|_ **operations**.
            | Check the `pgmq.metrics_all <https://github.com/tembo-io/pgmq/blob/main/pgmq-extension/sql/pgmq.sql?plain=1#L334-L346>`_ function for more details.


        """
        if self.is_async:
            return self.loop.run_until_complete(self._metrics_all_async())
        return self._metrics_all_sync()
