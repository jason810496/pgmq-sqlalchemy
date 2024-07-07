import asyncio
from typing import List, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine

from .schema import Message
from ._types import ENGINE_TYPE
from ._utils import (
    get_session_type,
    is_async_session_maker,
    is_async_dsn,
    encode_dict_to_psql,
    encode_list_to_psql,
)


class PGMQueue:
    """Base class for interacting with a queue"""

    engine: ENGINE_TYPE = None
    session_maker: sessionmaker = None
    delay: int = 0
    vt: int = 30

    is_async: bool = False
    is_pg_partman_ext_checked: bool = False
    loop: asyncio.AbstractEventLoop = None

    def __init__(
        self,
        dsn: str | None = None,
        engine: ENGINE_TYPE | None = None,
        session_maker: sessionmaker | None = None,
    ) -> None:
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

    def _create_queue_sync(self, queue: str, unlogged: bool = False) -> None:
        """Create a new queue."""
        with self.session_maker() as session:
            if unlogged:
                session.execute(
                    text("select pgmq.create_unlogged(:queue);"), {"queue": queue}
                )
            else:
                session.execute(text("select pgmq.create(:queue);"), {"queue": queue})
            session.commit()

    async def _create_queue_async(self, queue: str, unlogged: bool = False) -> None:
        """Create a new queue."""
        async with self.session_maker() as session:
            if unlogged:
                await session.execute(
                    text("select pgmq.create_unlogged(:queue);"), {"queue": queue}
                )
            else:
                await session.execute(
                    text("select pgmq.create(:queue);"), {"queue": queue}
                )
            await session.commit()

    def create_queue(self, queue: str, unlogged: bool = False) -> None:
        """Create a new queue."""
        if self.is_async:
            return self.loop.run_until_complete(
                self._create_queue_async(queue, unlogged)
            )
        return self._create_queue_sync(queue, unlogged)

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
        """Create a new queue

        Note: Partitions are created PGMQ_partman which must be configured in postgresql.conf
            Set `PGMQ_partman_bgw.interval` to set the interval for partition creation and deletion.
            A value of 10 will create new/delete partitions every 10 seconds. This value should be tuned
            according to the volume of messages being sent to the queue.

        Args:
            queue: The name of the queue.
            partition_interval: The number of messages per partition. Defaults to 10,000.
            retention_interval: The number of messages to retain. Messages exceeding this number will be dropped.
                Defaults to 100,000.
        """
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
        """Validate the length of a queue name."""
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
        """Drop a queue."""
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
        """List all queues."""
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
        """Send a message to a queue."""
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
        """Send a batch of messages to a queue."""
        if self.is_async:
            return self.loop.run_until_complete(
                self._send_batch_async(queue_name, encode_list_to_psql(messages), delay)
            )
        return self._send_batch_sync(queue_name, encode_list_to_psql(messages), delay)

    def _read_sync(
        self, queue_name: str, vt: Optional[int] = None
    ) -> Optional[Message]:
        with self.session_maker() as session:
            row = session.execute(
                text("select * from pgmq.read(:queue_name,:vt,1);"),
                {"queue_name": queue_name, "vt": vt or self.vt},
            ).fetchone()
            session.commit()
        if row is None:
            return None
        return Message(
            msg_id=row[0], read_ct=row[1], enqueued_at=row[2], vt=row[3], message=row[4]
        )

    async def _read_async(
        self, queue_name: str, vt: Optional[int] = None
    ) -> Optional[Message]:
        async with self.session_maker() as session:
            row = (
                await session.execute(
                    text("select * from pgmq.read(:queue_name,:vt,1);"),
                    {"queue_name": queue_name, "vt": vt or self.vt},
                )
            ).fetchone()
            await session.commit()
        if row is None:
            return None
        return Message(
            msg_id=row[0], read_ct=row[1], enqueued_at=row[2], vt=row[3], message=row[4]
        )

    def read(self, queue_name: str, vt: Optional[int] = None) -> Optional[Message]:
        """Read a message from the queue."""
        if self.is_async:
            return self.loop.run_until_complete(self._read_async(queue_name, vt))
        return self._read_sync(queue_name, vt)

    def _read_batch_sync(
        self,
        queue_name: str,
        batch_size: int = 1,
        vt: Optional[int] = None,
    ) -> Optional[List[Message]]:
        with self.session_maker() as session:
            rows = session.execute(
                text("select * from pgmq.read(:queue_name,:vt,:batch_size);"),
                {
                    "queue_name": queue_name,
                    "vt": vt or self.vt,
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
        batch_size: int = 1,
        vt: Optional[int] = None,
    ) -> Optional[List[Message]]:
        async with self.session_maker() as session:
            rows = (
                await session.execute(
                    text("select * from pgmq.read(:queue_name,:vt,:batch_size);"),
                    {
                        "queue_name": queue_name,
                        "vt": vt or self.vt,
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
        """Read a batch of messages from the queue."""
        if self.is_async:
            return self.loop.run_until_complete(
                self._read_batch_async(queue_name, batch_size, vt)
            )
        return self._read_batch_sync(queue_name, batch_size, vt)

    def _read_with_poll_sync(
        self,
        queue_name: str,
        vt: Optional[int] = None,
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
                    "vt": vt or self.vt,
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
        vt: Optional[int] = None,
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
                        "vt": vt or self.vt,
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
        """Read messages from a queue with polling."""
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
        """Pop a message from the queue."""
        if self.is_async:
            return self.loop.run_until_complete(self._pop_async(queue_name))
        return self._pop_sync(queue_name)
