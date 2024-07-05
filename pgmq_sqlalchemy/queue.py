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

        # create pgmq schema if not exists
        self._check_schema()

    async def _check_schema_async(self) -> None:
        """Check if the pgmq schema exists."""
        async with self.session_maker() as session:
            await session.execute(text("create extension if not exists pgmq cascade;"))
            await session.commit()

    def _check_schema_sync(self) -> None:
        """Check if the pgmq schema exists."""
        with self.session_maker() as session:
            session.execute(text("create extension if not exists pgmq cascade;"))
            session.commit()

    def _check_schema(self) -> None:
        """Check if the pgmq schema exists."""
        if self.is_async:
            return self.loop.run_until_complete(self._check_schema_async())
        return self._check_schema_sync()

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
