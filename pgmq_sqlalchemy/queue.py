import asyncio

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine

from ._types import ENGINE_TYPE

from ._utils import (
    get_session_type,
    is_async_session_maker,
    is_async_dsn,
)


class PGMQueue:
    """Base class for interacting with a queue"""

    engine: ENGINE_TYPE = None
    session_maker: sessionmaker = None
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
