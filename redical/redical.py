from __future__ import annotations

from typing import Any, AnyStr, Awaitable, Optional, Tuple, Union, TYPE_CHECKING

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


async def create_redical(
	address_or_uri: Union[Tuple[str, int], str],
	*,
	db: int = 0,
	parser: Optional['AbstractParser'] = None
) -> Redical:
	conn: 'Connection' = await create_connection(
		address_or_uri, db=db, parser=parser
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

	def execute(self, command: AnyStr, *args: Any, encoding: str = 'utf-8', **kwargs: Any) -> Awaitable[Any]:
		return self._conn.execute(command, *args, encoding=encoding, **kwargs)

	def close(self) -> None:
		self._conn.close()

	async def wait_closed(self) -> None:
		await self._conn.wait_closed()
