from __future__ import annotations

import logging
from types import TracebackType
from typing import Any, AnyStr, Awaitable, Final, Optional, Tuple, Type, Union

from .abstract import AbstractParser, RedicalResource
from .command import (
	KeyCommandsMixin,
	ServerCommandsMixin,
	SetCommandsMixin,
	StringCommandsMixin,
)
from .connection import create_connection, Connection
from .pool import create_pool, ConnectionPool

LOG: Final[logging.Logger] = logging.getLogger(__name__)


async def create_redical(
	address_or_uri: Union[Tuple[str, int], str],
	*,
	db: int = 0,
	encoding: str = 'utf-8',
	max_chunk_size: int = 65535,
	parser: Optional[AbstractParser] = None
) -> Redical:
	conn: Connection = await create_connection(
		address_or_uri, db=db, encoding=encoding, max_chunk_size=max_chunk_size, parser=parser
	)
	return Redical(conn)


async def create_redical_pool(
	address_or_uri: Union[Tuple[str, int], str],
	*,
	db: int = 0,
	encoding: str = 'utf-8',
	max_chunk_size: int = 65535,
	parser: Optional[AbstractParser] = None
) -> Redical:
	pool: ConnectionPool = await create_pool(
		address_or_uri, db=db, encoding=encoding, max_chunk_size=max_chunk_size, parser=parser
	)
	return Redical(pool)


class Redical(
	KeyCommandsMixin,
	ServerCommandsMixin,
	SetCommandsMixin,
	StringCommandsMixin,
):
	_resource: RedicalResource

	@property
	def is_closed(self) -> bool:
		return self._resource.is_closed

	@property
	def is_closing(self) -> bool:
		return self._resource.is_closing

	def __init__(self, resource: RedicalResource) -> None:
		self._resource = resource

	def execute(self, command: AnyStr, *args: Any, **kwargs: Any) -> Awaitable[Any]:
		return self._resource.execute(command, *args, **kwargs)

	def close(self) -> None:
		self._resource.close()

	async def wait_closed(self) -> None:
		await self._resource.wait_closed()

	async def __aenter__(self) -> None:
		await self._resource.__aenter__()

	async def __aexit__(
		self, exc_type: Optional[Type[BaseException]], exc: Optional[BaseException], tb: Optional[TracebackType]
	) -> Optional[bool]:
		return await self._resource.__aexit__(exc_type, exc, tb)
