# PEP 249 Database API 2.0 Types
# https://www.python.org/dev/peps/pep-0249/
from typing import Union, Any, Protocol, Sequence, Mapping
from typing_extensions import TypeAlias

DBAPITypeCode: TypeAlias = Union[Any, None]
# Strictly speaking, this should be a Sequence, but the type system does
# not support fixed-length sequences.
DBAPIColumnDescription: TypeAlias = tuple[
    str,
    DBAPITypeCode,
    Union[int, None],
    Union[int, None],
    Union[int, None],
    Union[int, None],
    Union[bool, None],
]


class DBAPIConnection(Protocol):
    def close(self) -> object:
        ...

    def commit(self) -> object:
        ...

    # optional:
    # def rollback(self) -> Any: ...
    def cursor(self) -> "DBAPICursor":
        ...


class DBAPICursor(Protocol):
    @property
    def description(self) -> Union[Sequence[DBAPIColumnDescription], None]:
        ...

    @property
    def rowcount(self) -> int:
        ...

    # optional:
    # def callproc(self, procname: str, parameters: Sequence[Any] = ..., /) -> Sequence[Any]: ...
    def close(self) -> object:
        ...

    def execute(
        self,
        operation: str,
        parameters: Union[Sequence[Any], Mapping[str, Any]] = ...,
        /,
    ) -> object:
        ...

    def executemany(
        self, operation: str, seq_of_parameters: Sequence[Sequence[Any]], /
    ) -> object:
        ...

    def fetchone(self) -> Union[Sequence[Any], None]:
        ...

    def fetchmany(self, size: int = ..., /) -> Sequence[Sequence[Any]]:
        ...

    def fetchall(self) -> Sequence[Sequence[Any]]:
        ...

    # optional:
    # def nextset(self) -> None | Literal[True]: ...
    arraysize: int

    def setinputsizes(
        self, sizes: Sequence[Union[DBAPITypeCode, int, None]], /
    ) -> object:
        ...

    def setoutputsize(self, size: int, column: int = ..., /) -> object:
        ...
