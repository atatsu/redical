from __future__ import annotations

import logging
from types import TracebackType
from typing import Any, AnyStr, Awaitable, Final, Optional, Tuple, Type, Union, TYPE_CHECKING

from .command import (
	KeyCommandsMixin,
	ServerCommandsMixin,
	SetCommandsMixin,
	StringCommandsMixin,
)
from .connection import create_connection

if TYPE_CHECKING:
	from .abstract import AbstractParser
	from .connection import Connection

LOG: Final[logging.Logger] = logging.getLogger(__name__)


async def create_redical(
	address_or_uri: Union[Tuple[str, int], str],
	*,
	db: int = 0,
	encoding: str = 'utf-8',
	parser: Optional['AbstractParser'] = None
) -> Redical:
	conn: 'Connection' = await create_connection(
		address_or_uri, db=db, encoding=encoding, parser=parser
	)
	return Redical(conn)


class Redical(
	KeyCommandsMixin,
	ServerCommandsMixin,
	SetCommandsMixin,
	StringCommandsMixin,
):
	_conn: 'Connection'

	def __init__(self, conn: 'Connection') -> None:
		self._conn = conn

	def execute(self, command: AnyStr, *args: Any, **kwargs: Any) -> Awaitable[Any]:
		return self._conn.execute(command, *args, **kwargs)

	def close(self) -> None:
		self._conn.close()

	async def wait_closed(self) -> None:
		await self._conn.wait_closed()

	async def __aenter__(self) -> None:
		await self._conn.__aenter__()

	async def __aexit__(
		self, exc_type: Optional[Type[BaseException]], exc: Optional[BaseException], tb: Optional[TracebackType]
	) -> Optional[bool]:
		return await self._conn.__aexit__(exc_type, exc, tb)
