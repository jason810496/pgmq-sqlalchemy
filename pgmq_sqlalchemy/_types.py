from typing import Union

from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

ENGINE_TYPE = Union[Engine, AsyncEngine]
SESSION_TYPE = Union[Session, AsyncSession]
