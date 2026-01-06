from typing import TYPE_CHECKING

from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from sqlalchemy.ext.asyncio import AsyncSession


def check_queue_exists(db_session: "Session", queue_name: str) -> bool:
    row = db_session.execute(
        text(
            "SELECT queue_name FROM pgmq.list_queues() WHERE queue_name = :queue_name ;"
        ),
        {"queue_name": queue_name},
    ).first()
    return row is not None and row[0] == queue_name


async def check_queue_exists_async(
    async_db_session: "AsyncSession", queue_name: str
) -> bool:
    row = (
        await async_db_session.execute(
            text(
                "SELECT queue_name FROM pgmq.list_queues() WHERE queue_name = :queue_name ;"
            ),
            {"queue_name": queue_name},
        )
    ).first()
    return row is not None and row[0] == queue_name
