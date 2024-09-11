from typing import Union, Any
from collections.abc import Mapping, Sequence
from ._db_api_interface import DBAPICursor

from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

ENGINE_TYPE = Union[Engine, AsyncEngine]
SESSION_TYPE = Union[Session, AsyncSession]


class AsyncDBAPICursor(DBAPICursor):
    async def execute(
        self,
        operation: str,
        parameters: Union[Sequence[Any], Mapping[str, Any]] = ...,
        /,
    ) -> object:
        ...


__all__ = [
    "ENGINE_TYPE",
    "SESSION_TYPE",
    "DBAPICursor",
    "AsyncDBAPICursor",
]
