from typing import List, Optional, Tuple, Dict, Any, Union, TYPE_CHECKING
import re

from sqlalchemy import text, bindparam
from sqlalchemy.dialects.postgresql import JSONB, ARRAY, BIGINT
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession

from .schema import Message, QueueMetrics

if TYPE_CHECKING:
    from sqlalchemy import TextClause


class PGMQOperation:
    """
    Static operations for PGMQ that accept user-provided sessions.

    All methods are static and require a session to be passed in.
    Users are responsible for session management and transaction handling.
    """

    # Private helper methods for statement and params generation

    @staticmethod
    def _get_check_pgmq_ext_statement() -> Tuple[str, Dict[str, Any]]:
        """Get statement and params for checking/creating pgmq extension."""
        return "create extension if not exists pgmq cascade;", {}

    @staticmethod
    def _get_check_pg_partman_ext_statement() -> Tuple[str, Dict[str, Any]]:
        """Get statement and params for checking/creating pg_partman extension."""
        return "create extension if not exists pg_partman cascade;", {}

    @staticmethod
    def _validate_partition_interval(interval: Union[int, str]) -> str:
        """Validate partition interval format.

        Args:
            interval: Either an integer for numeric partitioning or a string for time-based partitioning
                     (e.g., '1 day', '1 hour', '7 days')

        Returns:
            The validated interval as a string

        Raises:
            ValueError: If the interval format is invalid
        """
        if isinstance(interval, int):
            if interval <= 0:
                raise ValueError("Numeric partition interval must be positive")
            return str(interval)

        # Check if it's a numeric string (including negative numbers)
        stripped = interval.strip()
        is_numeric = False
        try:
            numeric_value = int(stripped)
            is_numeric = True
            if numeric_value <= 0:
                raise ValueError("Numeric partition interval must be positive")
            return str(numeric_value)
        except ValueError:
            # If it was a numeric string but invalid (e.g., negative), re-raise
            if is_numeric:
                raise
            # Not a numeric string, continue to time-based validation
            pass

        # Validate time-based interval format
        # Valid PostgreSQL interval formats: '1 day', '7 days', '1 hour', '1 month', etc.
        time_pattern = r"^\d+\s+(microsecond|millisecond|second|minute|hour|day|week|month|year)s?$"
        if not re.match(time_pattern, stripped, re.IGNORECASE):
            raise ValueError(
                f"Invalid time-based partition interval: '{interval}'. "
                "Expected format: '<number> <unit>' where unit is one of: "
                "microsecond, millisecond, second, minute, hour, day, week, month, year"
            )
        return stripped

    @staticmethod
    def _get_create_queue_statement(
        queue_name: str, unlogged: bool
    ) -> Tuple[str, Dict[str, Any]]:
        """Get statement and params for create_queue."""
        if unlogged:
            return "select pgmq.create_unlogged(:queue);", {"queue": queue_name}
        else:
            return "select pgmq.create(:queue);", {"queue": queue_name}

    @staticmethod
    def _get_create_partitioned_queue_statement(
        queue_name: str, partition_interval: str, retention_interval: str
    ) -> Tuple[str, Dict[str, Any]]:
        """Get statement and params for create_partitioned_queue."""
        return (
            "select pgmq.create_partitioned(:queue_name, :partition_interval, :retention_interval);",
            {
                "queue_name": queue_name,
                "partition_interval": partition_interval,
                "retention_interval": retention_interval,
            },
        )

    @staticmethod
    def _get_validate_queue_name_statement(
        queue_name: str,
    ) -> Tuple[str, Dict[str, Any]]:
        """Get statement and params for validate_queue_name."""
        return "select pgmq.validate_queue_name(:queue);", {"queue": queue_name}

    @staticmethod
    def _get_drop_queue_statement(
        queue: str, partitioned: bool
    ) -> Tuple[str, Dict[str, Any]]:
        """Get statement and params for drop_queue."""
        return "select pgmq.drop_queue(:queue, :partitioned);", {
            "queue": queue,
            "partitioned": partitioned,
        }

    @staticmethod
    def _get_list_queues_statement() -> Tuple[str, Dict[str, Any]]:
        """Get statement and params for list_queues."""
        return "select queue_name from pgmq.list_queues();", {}

    @staticmethod
    def _get_send_statement(
        queue_name: str, message: dict, delay: int
    ) -> Tuple["TextClause", Dict[str, Any]]:
        """Get statement and params for send."""
        stmt = text(
            "select * from pgmq.send((:queue_name)::text, :message, :delay);"
        ).bindparams(bindparam("queue_name"), bindparam("message", type_=JSONB))
        return (
            stmt,
            {
                "queue_name": queue_name,
                "message": message,
                "delay": delay,
            },
        )

    @staticmethod
    def _get_send_batch_statement(
        queue_name: str, messages: List[dict], delay: int
    ) -> Tuple["TextClause", Dict[str, Any]]:
        """Get statement and params for send_batch.

        Note: This uses SQLAlchemy's bindparam with JSONB array type for proper
        cross-driver compatibility and type adaptation.
        """
        stmt = text(
            "select * from pgmq.send_batch((:queue_name)::text, :messages, :delay);"
        ).bindparams(
            bindparam("queue_name"),
            bindparam("messages", type_=ARRAY(JSONB)),
        )

        return (
            stmt,
            {
                "queue_name": queue_name,
                "messages": messages,
                "delay": delay,
            },
        )

    @staticmethod
    def _get_read_statement(queue_name: str, vt: int) -> Tuple[str, Dict[str, Any]]:
        """Get statement and params for read."""
        return "select * from pgmq.read(:queue_name,:vt,1);", {
            "queue_name": queue_name,
            "vt": vt,
        }

    @staticmethod
    def _get_read_batch_statement(
        queue_name: str, vt: int, batch_size: int
    ) -> Tuple[str, Dict[str, Any]]:
        """Get statement and params for read_batch."""
        return (
            "select * from pgmq.read(:queue_name,:vt,:batch_size);",
            {"queue_name": queue_name, "vt": vt, "batch_size": batch_size},
        )

    @staticmethod
    def _get_read_with_poll_statement(
        queue_name: str, vt: int, qty: int, max_poll_seconds: int, poll_interval_ms: int
    ) -> Tuple[str, Dict[str, Any]]:
        """Get statement and params for read_with_poll."""
        return (
            "select * from pgmq.read_with_poll(:queue_name,:vt,:qty,:max_poll_seconds,:poll_interval_ms);",
            {
                "queue_name": queue_name,
                "vt": vt,
                "qty": qty,
                "max_poll_seconds": max_poll_seconds,
                "poll_interval_ms": poll_interval_ms,
            },
        )

    @staticmethod
    def _get_set_vt_statement(
        queue_name: str, msg_id: int, vt: int
    ) -> Tuple[str, Dict[str, Any]]:
        """Get statement and params for set_vt."""
        return (
            "select * from pgmq.set_vt(:queue_name, :msg_id, :vt);",
            {"queue_name": queue_name, "msg_id": msg_id, "vt": vt},
        )

    @staticmethod
    def _get_pop_statement(queue_name: str) -> Tuple[str, Dict[str, Any]]:
        """Get statement and params for pop."""
        return "select * from pgmq.pop(:queue_name);", {"queue_name": queue_name}

    @staticmethod
    def _get_delete_statement(
        queue_name: str, msg_id: int
    ) -> Tuple["TextClause", Dict[str, Any]]:
        """Get statement and params for delete."""
        stmt = text(
            "select pgmq.delete((:queue_name)::text, :msg_id) as deleted;"
        ).bindparams(bindparam("queue_name"), bindparam("msg_id", type_=BIGINT))

        return stmt, {
            "queue_name": queue_name,
            "msg_id": msg_id,
        }

    @staticmethod
    def _get_delete_batch_statement(
        queue_name: str, msg_ids: List[int]
    ) -> Tuple["TextClause", Dict[str, Any]]:
        """Get statement and params for delete_batch."""
        stmt = text(
            "select * from pgmq.delete((:queue_name)::text, :msg_ids);"
        ).bindparams(
            bindparam("queue_name"),
            bindparam("msg_ids", type_=ARRAY(BIGINT)),
        )

        return (
            stmt,
            {"queue_name": queue_name, "msg_ids": msg_ids},
        )

    @staticmethod
    def _get_archive_statement(
        queue_name: str, msg_id: int
    ) -> Tuple["TextClause", Dict[str, Any]]:
        """Get statement and params for archive."""
        stmt = text(
            "select pgmq.archive((:queue_name)::text, :msg_id) as archived;"
        ).bindparams(bindparam("queue_name"), bindparam("msg_id", type_=BIGINT))
        return stmt, {
            "queue_name": queue_name,
            "msg_id": msg_id,
        }

    @staticmethod
    def _get_archive_batch_statement(
        queue_name: str, msg_ids: List[int]
    ) -> Tuple["TextClause", Dict[str, Any]]:
        """Get statement and params for archive_batch."""
        stmt = text(
            "select * from pgmq.archive((:queue_name)::text, :msg_ids);"
        ).bindparams(
            bindparam("queue_name"),
            bindparam("msg_ids", type_=ARRAY(BIGINT)),
        )
        return stmt, {
            "queue_name": queue_name,
            "msg_ids": msg_ids,
        }

    @staticmethod
    def _get_purge_statement(queue_name: str) -> Tuple[str, Dict[str, Any]]:
        """Get statement and params for purge."""
        return "select pgmq.purge_queue(:queue_name);", {"queue_name": queue_name}

    @staticmethod
    def _get_metrics_statement(queue_name: str) -> Tuple[str, Dict[str, Any]]:
        """Get statement and params for metrics."""
        return "select * from pgmq.metrics(:queue_name);", {"queue_name": queue_name}

    @staticmethod
    def _get_metrics_all_statement() -> Tuple[str, Dict[str, Any]]:
        """Get statement and params for metrics_all."""
        return "select * from pgmq.metrics_all();", {}

    # Public methods

    @staticmethod
    def check_pgmq_ext(
        *,
        session: Session,
        commit: bool = True,
    ) -> None:
        """Check if pgmq extension exists and create it if not.

        Args:
            session: SQLAlchemy session.
            commit: Whether to commit the transaction.
        """
        stmt, params = PGMQOperation._get_check_pgmq_ext_statement()
        session.execute(text(stmt), params)
        if commit:
            session.commit()

    @staticmethod
    async def check_pgmq_ext_async(
        *,
        session: AsyncSession,
        commit: bool = True,
    ) -> None:
        """Check if pgmq extension exists and create it if not (async).

        Args:
            session: Async SQLAlchemy session.
            commit: Whether to commit the transaction.
        """
        stmt, params = PGMQOperation._get_check_pgmq_ext_statement()
        await session.execute(text(stmt), params)
        if commit:
            await session.commit()

    @staticmethod
    def check_pg_partman_ext(
        *,
        session: Session,
        commit: bool = True,
    ) -> None:
        """Check if pg_partman extension exists and create it if not.

        Args:
            session: SQLAlchemy session.
            commit: Whether to commit the transaction.
        """
        stmt, params = PGMQOperation._get_check_pg_partman_ext_statement()
        session.execute(text(stmt), params)
        if commit:
            session.commit()

    @staticmethod
    async def check_pg_partman_ext_async(
        *,
        session: AsyncSession,
        commit: bool = True,
    ) -> None:
        """Check if pg_partman extension exists and create it if not (async).

        Args:
            session: Async SQLAlchemy session.
            commit: Whether to commit the transaction.
        """
        stmt, params = PGMQOperation._get_check_pg_partman_ext_statement()
        await session.execute(text(stmt), params)
        if commit:
            await session.commit()

    @staticmethod
    def create_queue(
        queue_name: str,
        unlogged: bool = False,
        *,
        session: Session,
        commit: bool = True,
    ) -> None:
        """Create a new queue.

        Args:
            queue_name: The name of the queue.
            unlogged: If True, creates an unlogged table.
            session: SQLAlchemy session.
            commit: Whether to commit the transaction.
        """
        stmt, params = PGMQOperation._get_create_queue_statement(queue_name, unlogged)
        session.execute(text(stmt), params)
        if commit:
            session.commit()

    @staticmethod
    async def create_queue_async(
        queue_name: str,
        unlogged: bool = False,
        *,
        session: AsyncSession,
        commit: bool = True,
    ) -> None:
        """Create a new queue asynchronously.

        Args:
            queue_name: The name of the queue.
            unlogged: If True, creates an unlogged table.
            session: Async SQLAlchemy session.
            commit: Whether to commit the transaction.
        """
        stmt, params = PGMQOperation._get_create_queue_statement(queue_name, unlogged)
        await session.execute(text(stmt), params)
        if commit:
            await session.commit()

    @staticmethod
    def create_partitioned_queue(
        queue_name: str,
        partition_interval: str,
        retention_interval: str,
        *,
        session: Session,
        commit: bool = True,
    ) -> None:
        """Create a new partitioned queue.

        Args:
            queue_name: The name of the queue.
            partition_interval: Partition interval as string.
            retention_interval: Retention interval as string.
            session: SQLAlchemy session.
            commit: Whether to commit the transaction.
        """
        # Validate partition intervals
        partition_interval = PGMQOperation._validate_partition_interval(
            partition_interval
        )
        retention_interval = PGMQOperation._validate_partition_interval(
            retention_interval
        )

        stmt, params = PGMQOperation._get_create_partitioned_queue_statement(
            queue_name, partition_interval, retention_interval
        )
        session.execute(text(stmt), params)
        if commit:
            session.commit()

    @staticmethod
    async def create_partitioned_queue_async(
        queue_name: str,
        partition_interval: str,
        retention_interval: str,
        *,
        session: AsyncSession,
        commit: bool = True,
    ) -> None:
        """Create a new partitioned queue asynchronously.

        Args:
            queue_name: The name of the queue.
            partition_interval: Partition interval as string.
            retention_interval: Retention interval as string.
            session: Async SQLAlchemy session.
            commit: Whether to commit the transaction.
        """
        # Validate partition intervals
        partition_interval = PGMQOperation._validate_partition_interval(
            partition_interval
        )
        retention_interval = PGMQOperation._validate_partition_interval(
            retention_interval
        )

        stmt, params = PGMQOperation._get_create_partitioned_queue_statement(
            queue_name, partition_interval, retention_interval
        )
        await session.execute(text(stmt), params)
        if commit:
            await session.commit()

    @staticmethod
    def validate_queue_name(
        queue_name: str,
        *,
        session: Session,
        commit: bool = True,
    ) -> None:
        """Validate the length of a queue name.

        Args:
            queue_name: The name of the queue.
            session: SQLAlchemy session.
            commit: Whether to commit the transaction.
        """
        stmt, params = PGMQOperation._get_validate_queue_name_statement(queue_name)
        session.execute(text(stmt), params)
        if commit:
            session.commit()

    @staticmethod
    async def validate_queue_name_async(
        queue_name: str,
        *,
        session: AsyncSession,
        commit: bool = True,
    ) -> None:
        """Validate the length of a queue name asynchronously.

        Args:
            queue_name: The name of the queue.
            session: Async SQLAlchemy session.
            commit: Whether to commit the transaction.
        """
        stmt, params = PGMQOperation._get_validate_queue_name_statement(queue_name)
        await session.execute(text(stmt), params)
        if commit:
            await session.commit()

    @staticmethod
    def drop_queue(
        queue: str,
        partitioned: bool = False,
        *,
        session: Session,
        commit: bool = True,
    ) -> bool:
        """Drop a queue.

        Args:
            queue: The name of the queue.
            partitioned: Whether the queue is partitioned.
            session: SQLAlchemy session.
            commit: Whether to commit the transaction.

        Returns:
            True if the queue was dropped successfully.
        """
        stmt, params = PGMQOperation._get_drop_queue_statement(queue, partitioned)
        row = session.execute(text(stmt), params).fetchone()
        if commit:
            session.commit()
        return row[0]

    @staticmethod
    async def drop_queue_async(
        queue: str,
        partitioned: bool = False,
        *,
        session: AsyncSession,
        commit: bool = True,
    ) -> bool:
        """Drop a queue asynchronously.

        Args:
            queue: The name of the queue.
            partitioned: Whether the queue is partitioned.
            session: Async SQLAlchemy session.
            commit: Whether to commit the transaction.

        Returns:
            True if the queue was dropped successfully.
        """
        stmt, params = PGMQOperation._get_drop_queue_statement(queue, partitioned)
        row = (await session.execute(text(stmt), params)).fetchone()
        if commit:
            await session.commit()
        return row[0]

    @staticmethod
    def list_queues(
        *,
        session: Session,
        commit: bool = True,
    ) -> List[str]:
        """List all queues.

        Args:
            session: SQLAlchemy session.
            commit: Whether to commit the transaction.

        Returns:
            List of queue names.
        """
        stmt, params = PGMQOperation._get_list_queues_statement()
        rows = session.execute(text(stmt), params).fetchall()
        if commit:
            session.commit()
        return [row[0] for row in rows]

    @staticmethod
    async def list_queues_async(
        *,
        session: AsyncSession,
        commit: bool = True,
    ) -> List[str]:
        """List all queues asynchronously.

        Args:
            session: Async SQLAlchemy session.
            commit: Whether to commit the transaction.

        Returns:
            List of queue names.
        """
        stmt, params = PGMQOperation._get_list_queues_statement()
        rows = (await session.execute(text(stmt), params)).fetchall()
        if commit:
            await session.commit()
        return [row[0] for row in rows]

    @staticmethod
    def send(
        queue_name: str,
        message: dict,
        delay: int = 0,
        *,
        session: Session,
        commit: bool = True,
    ) -> int:
        """Send a message to a queue.

        Args:
            queue_name: The name of the queue.
            message: The message as a dictionary.
            delay: Delay in seconds before the message becomes visible.
            session: SQLAlchemy session.
            commit: Whether to commit the transaction.

        Returns:
            The message ID.
        """
        stmt, params = PGMQOperation._get_send_statement(queue_name, message, delay)
        row = session.execute(stmt, params).fetchone()
        if commit:
            session.commit()
        return row[0]

    @staticmethod
    async def send_async(
        queue_name: str,
        message: dict,
        delay: int = 0,
        *,
        session: AsyncSession,
        commit: bool = True,
    ) -> int:
        """Send a message to a queue asynchronously.

        Args:
            queue_name: The name of the queue.
            message: The message as a dictionary.
            delay: Delay in seconds before the message becomes visible.
            session: Async SQLAlchemy session.
            commit: Whether to commit the transaction.

        Returns:
            The message ID.
        """
        stmt, params = PGMQOperation._get_send_statement(queue_name, message, delay)
        row = (await session.execute(stmt, params)).fetchone()
        if commit:
            await session.commit()
        return row[0]

    @staticmethod
    def send_batch(
        queue_name: str,
        messages: List[dict],
        delay: int = 0,
        *,
        session: Session,
        commit: bool = True,
    ) -> List[int]:
        """Send a batch of messages to a queue.

        Args:
            queue_name: The name of the queue.
            messages: The messages as a list of dictionaries.
            delay: Delay in seconds before the messages become visible.
            session: SQLAlchemy session.
            commit: Whether to commit the transaction.

        Returns:
            List of message IDs.
        """
        stmt, params = PGMQOperation._get_send_batch_statement(
            queue_name, messages, delay
        )
        rows = session.execute(stmt, params).fetchall()
        if commit:
            session.commit()
        return [row[0] for row in rows]

    @staticmethod
    async def send_batch_async(
        queue_name: str,
        messages: List[dict],
        delay: int = 0,
        *,
        session: AsyncSession,
        commit: bool = True,
    ) -> List[int]:
        """Send a batch of messages to a queue asynchronously.

        Args:
            queue_name: The name of the queue.
            messages: The messages as a list of dictionaries.
            delay: Delay in seconds before the messages become visible.
            session: Async SQLAlchemy session.
            commit: Whether to commit the transaction.

        Returns:
            List of message IDs.
        """
        stmt, params = PGMQOperation._get_send_batch_statement(
            queue_name, messages, delay
        )
        rows = (await session.execute(stmt, params)).fetchall()
        if commit:
            await session.commit()
        return [row[0] for row in rows]

    @staticmethod
    def read(
        queue_name: str,
        vt: int,
        *,
        session: Session,
        commit: bool = True,
    ) -> Optional[Message]:
        """Read a message from the queue.

        Args:
            queue_name: The name of the queue.
            vt: Visibility timeout in seconds.
            session: SQLAlchemy session.
            commit: Whether to commit the transaction.

        Returns:
            Message or None if the queue is empty.
        """
        stmt, params = PGMQOperation._get_read_statement(queue_name, vt)
        row = session.execute(text(stmt), params).fetchone()
        if commit:
            session.commit()
        if row is None:
            return None
        return Message(
            msg_id=row[0], read_ct=row[1], enqueued_at=row[2], vt=row[3], message=row[4]
        )

    @staticmethod
    async def read_async(
        queue_name: str,
        vt: int,
        *,
        session: AsyncSession,
        commit: bool = True,
    ) -> Optional[Message]:
        """Read a message from the queue asynchronously.

        Args:
            queue_name: The name of the queue.
            vt: Visibility timeout in seconds.
            session: Async SQLAlchemy session.
            commit: Whether to commit the transaction.

        Returns:
            Message or None if the queue is empty.
        """
        stmt, params = PGMQOperation._get_read_statement(queue_name, vt)
        row = (await session.execute(text(stmt), params)).fetchone()
        if commit:
            await session.commit()
        if row is None:
            return None
        return Message(
            msg_id=row[0], read_ct=row[1], enqueued_at=row[2], vt=row[3], message=row[4]
        )

    @staticmethod
    def read_batch(
        queue_name: str,
        vt: int,
        batch_size: int = 1,
        *,
        session: Session,
        commit: bool = True,
    ) -> Optional[List[Message]]:
        """Read a batch of messages from the queue.

        Args:
            queue_name: The name of the queue.
            vt: Visibility timeout in seconds.
            batch_size: Number of messages to read.
            session: SQLAlchemy session.
            commit: Whether to commit the transaction.

        Returns:
            List of messages or None if the queue is empty.
        """
        stmt, params = PGMQOperation._get_read_batch_statement(
            queue_name, vt, batch_size
        )
        rows = session.execute(text(stmt), params).fetchall()
        if commit:
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

    @staticmethod
    async def read_batch_async(
        queue_name: str,
        vt: int,
        batch_size: int = 1,
        *,
        session: AsyncSession,
        commit: bool = True,
    ) -> Optional[List[Message]]:
        """Read a batch of messages from the queue asynchronously.

        Args:
            queue_name: The name of the queue.
            vt: Visibility timeout in seconds.
            batch_size: Number of messages to read.
            session: Async SQLAlchemy session.
            commit: Whether to commit the transaction.

        Returns:
            List of messages or None if the queue is empty.
        """
        stmt, params = PGMQOperation._get_read_batch_statement(
            queue_name, vt, batch_size
        )
        rows = (await session.execute(text(stmt), params)).fetchall()
        if commit:
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

    @staticmethod
    def read_with_poll(
        queue_name: str,
        vt: int,
        qty: int = 1,
        max_poll_seconds: int = 5,
        poll_interval_ms: int = 100,
        *,
        session: Session,
        commit: bool = True,
    ) -> Optional[List[Message]]:
        """Read messages from a queue with polling.

        Args:
            queue_name: The name of the queue.
            vt: Visibility timeout in seconds.
            qty: Number of messages to read.
            max_poll_seconds: Maximum number of seconds to poll.
            poll_interval_ms: Interval in milliseconds to poll.
            session: SQLAlchemy session.
            commit: Whether to commit the transaction.

        Returns:
            List of messages or None if the queue is empty.
        """
        stmt, params = PGMQOperation._get_read_with_poll_statement(
            queue_name, vt, qty, max_poll_seconds, poll_interval_ms
        )
        rows = session.execute(text(stmt), params).fetchall()
        if commit:
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

    @staticmethod
    async def read_with_poll_async(
        queue_name: str,
        vt: int,
        qty: int = 1,
        max_poll_seconds: int = 5,
        poll_interval_ms: int = 100,
        *,
        session: AsyncSession,
        commit: bool = True,
    ) -> Optional[List[Message]]:
        """Read messages from a queue with polling asynchronously.

        Args:
            queue_name: The name of the queue.
            vt: Visibility timeout in seconds.
            qty: Number of messages to read.
            max_poll_seconds: Maximum number of seconds to poll.
            poll_interval_ms: Interval in milliseconds to poll.
            session: Async SQLAlchemy session.
            commit: Whether to commit the transaction.

        Returns:
            List of messages or None if the queue is empty.
        """
        stmt, params = PGMQOperation._get_read_with_poll_statement(
            queue_name, vt, qty, max_poll_seconds, poll_interval_ms
        )
        rows = (await session.execute(text(stmt), params)).fetchall()
        if commit:
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

    @staticmethod
    def set_vt(
        queue_name: str,
        msg_id: int,
        vt: int,
        *,
        session: Session,
        commit: bool = True,
    ) -> Optional[Message]:
        """Set the visibility timeout for a message.

        Args:
            queue_name: The name of the queue.
            msg_id: The message ID.
            vt: Visibility timeout in seconds.
            session: SQLAlchemy session.
            commit: Whether to commit the transaction.

        Returns:
            Message or None if the message does not exist.
        """
        stmt, params = PGMQOperation._get_set_vt_statement(queue_name, msg_id, vt)
        row = session.execute(text(stmt), params).fetchone()
        if commit:
            session.commit()
        if row is None:
            return None
        return Message(
            msg_id=row[0], read_ct=row[1], enqueued_at=row[2], vt=row[3], message=row[4]
        )

    @staticmethod
    async def set_vt_async(
        queue_name: str,
        msg_id: int,
        vt: int,
        *,
        session: AsyncSession,
        commit: bool = True,
    ) -> Optional[Message]:
        """Set the visibility timeout for a message asynchronously.

        Args:
            queue_name: The name of the queue.
            msg_id: The message ID.
            vt: Visibility timeout in seconds.
            session: Async SQLAlchemy session.
            commit: Whether to commit the transaction.

        Returns:
            Message or None if the message does not exist.
        """
        stmt, params = PGMQOperation._get_set_vt_statement(queue_name, msg_id, vt)
        row = (await session.execute(text(stmt), params)).fetchone()
        if commit:
            await session.commit()
        if row is None:
            return None
        return Message(
            msg_id=row[0], read_ct=row[1], enqueued_at=row[2], vt=row[3], message=row[4]
        )

    @staticmethod
    def pop(
        queue_name: str,
        *,
        session: Session,
        commit: bool = True,
    ) -> Optional[Message]:
        """Read and delete a message from the queue.

        Args:
            queue_name: The name of the queue.
            session: SQLAlchemy session.
            commit: Whether to commit the transaction.

        Returns:
            Message or None if the queue is empty.
        """
        stmt, params = PGMQOperation._get_pop_statement(queue_name)
        row = session.execute(text(stmt), params).fetchone()
        if commit:
            session.commit()
        if row is None:
            return None
        return Message(
            msg_id=row[0], read_ct=row[1], enqueued_at=row[2], vt=row[3], message=row[4]
        )

    @staticmethod
    async def pop_async(
        queue_name: str,
        *,
        session: AsyncSession,
        commit: bool = True,
    ) -> Optional[Message]:
        """Read and delete a message from the queue asynchronously.

        Args:
            queue_name: The name of the queue.
            session: Async SQLAlchemy session.
            commit: Whether to commit the transaction.

        Returns:
            Message or None if the queue is empty.
        """
        stmt, params = PGMQOperation._get_pop_statement(queue_name)
        row = (await session.execute(text(stmt), params)).fetchone()
        if commit:
            await session.commit()
        if row is None:
            return None
        return Message(
            msg_id=row[0], read_ct=row[1], enqueued_at=row[2], vt=row[3], message=row[4]
        )

    @staticmethod
    def delete(
        queue_name: str,
        msg_id: int,
        *,
        session: Session,
        commit: bool = True,
    ) -> bool:
        """Delete a message from the queue.

        Args:
            queue_name: The name of the queue.
            msg_id: The message ID.
            session: SQLAlchemy session.
            commit: Whether to commit the transaction.

        Returns:
            True if the message was deleted successfully.
        """
        stmt, params = PGMQOperation._get_delete_statement(queue_name, msg_id)
        row = session.execute(stmt, params).fetchone()
        if commit:
            session.commit()
        return row[0]

    @staticmethod
    async def delete_async(
        queue_name: str,
        msg_id: int,
        *,
        session: AsyncSession,
        commit: bool = True,
    ) -> bool:
        """Delete a message from the queue asynchronously.

        Args:
            queue_name: The name of the queue.
            msg_id: The message ID.
            session: Async SQLAlchemy session.
            commit: Whether to commit the transaction.

        Returns:
            True if the message was deleted successfully.
        """
        stmt, params = PGMQOperation._get_delete_statement(queue_name, msg_id)
        row = (await session.execute(stmt, params)).fetchone()
        if commit:
            await session.commit()
        return row[0]

    @staticmethod
    def delete_batch(
        queue_name: str,
        msg_ids: List[int],
        *,
        session: Session,
        commit: bool = True,
    ) -> List[int]:
        """Delete a batch of messages from the queue.

        Args:
            queue_name: The name of the queue.
            msg_ids: List of message IDs.
            session: SQLAlchemy session.
            commit: Whether to commit the transaction.

        Returns:
            List of message IDs that were successfully deleted.
        """
        stmt, params = PGMQOperation._get_delete_batch_statement(queue_name, msg_ids)
        rows = session.execute(stmt, params).fetchall()
        if commit:
            session.commit()
        return [row[0] for row in rows]

    @staticmethod
    async def delete_batch_async(
        queue_name: str,
        msg_ids: List[int],
        *,
        session: AsyncSession,
        commit: bool = True,
    ) -> List[int]:
        """Delete a batch of messages from the queue asynchronously.

        Args:
            queue_name: The name of the queue.
            msg_ids: List of message IDs.
            session: Async SQLAlchemy session.
            commit: Whether to commit the transaction.

        Returns:
            List of message IDs that were successfully deleted.
        """
        stmt, params = PGMQOperation._get_delete_batch_statement(queue_name, msg_ids)
        rows = (await session.execute(stmt, params)).fetchall()
        if commit:
            await session.commit()
        return [row[0] for row in rows]

    @staticmethod
    def archive(
        queue_name: str,
        msg_id: int,
        *,
        session: Session,
        commit: bool = True,
    ) -> bool:
        """Archive a message from a queue.

        Args:
            queue_name: The name of the queue.
            msg_id: The message ID.
            session: SQLAlchemy session.
            commit: Whether to commit the transaction.

        Returns:
            True if the message was archived successfully.
        """
        stmt, params = PGMQOperation._get_archive_statement(queue_name, msg_id)
        row = session.execute(stmt, params).fetchone()
        if commit:
            session.commit()
        return row[0]

    @staticmethod
    async def archive_async(
        queue_name: str,
        msg_id: int,
        *,
        session: AsyncSession,
        commit: bool = True,
    ) -> bool:
        """Archive a message from a queue asynchronously.

        Args:
            queue_name: The name of the queue.
            msg_id: The message ID.
            session: Async SQLAlchemy session.
            commit: Whether to commit the transaction.

        Returns:
            True if the message was archived successfully.
        """
        stmt, params = PGMQOperation._get_archive_statement(queue_name, msg_id)
        row = (await session.execute(stmt, params)).fetchone()
        if commit:
            await session.commit()
        return row[0]

    @staticmethod
    def archive_batch(
        queue_name: str,
        msg_ids: List[int],
        *,
        session: Session,
        commit: bool = True,
    ) -> List[int]:
        """Archive a batch of messages from the queue.

        Args:
            queue_name: The name of the queue.
            msg_ids: List of message IDs.
            session: SQLAlchemy session.
            commit: Whether to commit the transaction.

        Returns:
            List of message IDs that were successfully archived.
        """
        stmt, params = PGMQOperation._get_archive_batch_statement(queue_name, msg_ids)
        rows = session.execute(stmt, params).fetchall()
        if commit:
            session.commit()
        return [row[0] for row in rows]

    @staticmethod
    async def archive_batch_async(
        queue_name: str,
        msg_ids: List[int],
        *,
        session: AsyncSession,
        commit: bool = True,
    ) -> List[int]:
        """Archive a batch of messages from the queue asynchronously.

        Args:
            queue_name: The name of the queue.
            msg_ids: List of message IDs.
            session: Async SQLAlchemy session.
            commit: Whether to commit the transaction.

        Returns:
            List of message IDs that were successfully archived.
        """
        stmt, params = PGMQOperation._get_archive_batch_statement(queue_name, msg_ids)
        rows = (await session.execute(stmt, params)).fetchall()
        if commit:
            await session.commit()
        return [row[0] for row in rows]

    @staticmethod
    def purge(
        queue_name: str,
        *,
        session: Session,
        commit: bool = True,
    ) -> int:
        """Purge all messages from a queue.

        Args:
            queue_name: The name of the queue.
            session: SQLAlchemy session.
            commit: Whether to commit the transaction.

        Returns:
            Number of messages purged.
        """
        stmt, params = PGMQOperation._get_purge_statement(queue_name)
        row = session.execute(text(stmt), params).fetchone()
        if commit:
            session.commit()
        return row[0]

    @staticmethod
    async def purge_async(
        queue_name: str,
        *,
        session: AsyncSession,
        commit: bool = True,
    ) -> int:
        """Purge all messages from a queue asynchronously.

        Args:
            queue_name: The name of the queue.
            session: Async SQLAlchemy session.
            commit: Whether to commit the transaction.

        Returns:
            Number of messages purged.
        """
        stmt, params = PGMQOperation._get_purge_statement(queue_name)
        row = (await session.execute(text(stmt), params)).fetchone()
        if commit:
            await session.commit()
        return row[0]

    @staticmethod
    def metrics(
        queue_name: str,
        *,
        session: Session,
        commit: bool = True,
    ) -> Optional[QueueMetrics]:
        """Get metrics for a queue.

        Args:
            queue_name: The name of the queue.
            session: SQLAlchemy session.
            commit: Whether to commit the transaction.

        Returns:
            QueueMetrics or None if the queue does not exist.
        """
        stmt, params = PGMQOperation._get_metrics_statement(queue_name)
        row = session.execute(text(stmt), params).fetchone()
        if commit:
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

    @staticmethod
    async def metrics_async(
        queue_name: str,
        *,
        session: AsyncSession,
        commit: bool = True,
    ) -> Optional[QueueMetrics]:
        """Get metrics for a queue asynchronously.

        Args:
            queue_name: The name of the queue.
            session: Async SQLAlchemy session.
            commit: Whether to commit the transaction.

        Returns:
            QueueMetrics or None if the queue does not exist.
        """
        stmt, params = PGMQOperation._get_metrics_statement(queue_name)
        row = (await session.execute(text(stmt), params)).fetchone()
        if commit:
            await session.commit()
        if row is None:
            return None
        return QueueMetrics(
            queue_name=row[0],
            queue_length=row[1],
            newest_msg_age_sec=row[2],
            oldest_msg_age_sec=row[3],
            total_messages=row[4],
        )

    @staticmethod
    def metrics_all(
        *,
        session: Session,
        commit: bool = True,
    ) -> Optional[List[QueueMetrics]]:
        """Get metrics for all queues.

        Args:
            session: SQLAlchemy session.
            commit: Whether to commit the transaction.

        Returns:
            List of QueueMetrics or None if no queues exist.
        """
        stmt, params = PGMQOperation._get_metrics_all_statement()
        rows = session.execute(text(stmt), params).fetchall()
        if commit:
            session.commit()
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

    @staticmethod
    async def metrics_all_async(
        *,
        session: AsyncSession,
        commit: bool = True,
    ) -> Optional[List[QueueMetrics]]:
        """Get metrics for all queues asynchronously.

        Args:
            session: Async SQLAlchemy session.
            commit: Whether to commit the transaction.

        Returns:
            List of QueueMetrics or None if no queues exist.
        """
        stmt, params = PGMQOperation._get_metrics_all_statement()
        rows = (await session.execute(text(stmt), params)).fetchall()
        if commit:
            await session.commit()
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
