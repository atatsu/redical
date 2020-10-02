from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from types import TracebackType
from typing import (
	cast,
	overload,
	Any,
	AnyStr,
	AsyncIterator,
	Awaitable,
	Final,
	Generic,
	List,
	Optional,
	Tuple,
	Type,
	TypeVar,
	Union,
)

from .abstract import AbstractParser, RedicalResource
from .command import (
	KeyCommandsMixin,
	HashCommandsMixin,
	ServerCommandsMixin,
	SetCommandsMixin,
	StringCommandsMixin,
)
from .connection import create_connection, Connection
from .exception import PipelineError, TransactionError
from .pool import create_pool, ConnectionPool

__all__: List[str] = ['Redical', 'Pipeline', 'Transaction']

LOG: Final[logging.Logger] = logging.getLogger(__name__)

R = TypeVar('R', bound='Redical')


class RedicalBase:
	_resource: RedicalResource

	@property
	def resource(self) -> RedicalResource:
		return self._resource

	def __init__(self, resource: RedicalResource) -> None:
		self._resource = resource

	@overload
	def execute(self, command: AnyStr, *args: Any, encoding: str = 'utf-8') -> Awaitable[Any]:
		...
	@overload  # noqa: E301
	def execute(self, command: AnyStr, *args: Any, **kwargs: Any) -> Awaitable[Any]:
		...
	def execute(self, command, *args, **kwargs):  # noqa: E301
		return self._resource.execute(command, *args, **kwargs)


class Pipeline:
	def close(self) -> None:
		raise PipelineError('Do not close from within pipeline')

	async def wait_closed(self) -> None:
		raise PipelineError('Do not close from within pipeline')


class Transaction:
	def close(self) -> None:
		raise TransactionError('Do not close from within transaction')

	async def wait_closed(self) -> None:
		raise TransactionError('Do not close from within transaction')


class Redical(
	Generic[R],
	RedicalBase,
	KeyCommandsMixin,
	HashCommandsMixin,
	ServerCommandsMixin,
	SetCommandsMixin,
	StringCommandsMixin
):
	@property
	def is_closed(self) -> bool:
		return self._resource.is_closed

	@property
	def is_closing(self) -> bool:
		return self._resource.is_closing

	def close(self) -> None:
		self._resource.close()

	@asynccontextmanager
	async def transaction(self, *watch_keys: str) -> AsyncIterator[R]:
		conn: RedicalResource
		async with self._resource.transaction(*watch_keys) as conn:
			T: Type[R] = type('Transaction', (Transaction, self.__class__), {})
			yield T(conn)

	async def wait_closed(self) -> None:
		await self._resource.wait_closed()

	async def __aenter__(self) -> R:
		conn: Connection = cast(Connection, await self._resource.__aenter__())
		P: Type[R] = type('Pipeline', (Pipeline, self.__class__), {})
		return P(conn)

	async def __aexit__(
		self, exc_type: Optional[Type[BaseException]], exc: Optional[BaseException], tb: Optional[TracebackType]
	) -> Optional[bool]:
		return await self._resource.__aexit__(exc_type, exc, tb)


async def create_redical(
	address_or_uri: Union[Tuple[str, int], str],
	*,
	db: int = 0,
	encoding: str = 'utf-8',
	max_chunk_size: int = 65535,
	parser: Optional[AbstractParser] = None,
	redical_cls: Optional[Type[R]] = None
) -> R:
	conn: Connection = await create_connection(
		address_or_uri, db=db, encoding=encoding, max_chunk_size=max_chunk_size, parser=parser
	)
	cls: Type[R] = redical_cls if redical_cls is not None else Redical
	return cls(conn)


async def create_redical_pool(
	address_or_uri: Union[Tuple[str, int], str],
	*,
	db: int = 0,
	encoding: str = 'utf-8',
	max_chunk_size: int = 65535,
	max_size: int = 10,
	min_size: int = 1,
	parser: Optional[AbstractParser] = None,
	redical_cls: Optional[Type[R]] = None
) -> R:
	pool: ConnectionPool = await create_pool(
		address_or_uri,
		db=db,
		encoding=encoding,
		max_chunk_size=max_chunk_size,
		max_size=max_size,
		min_size=min_size,
		parser=parser
	)
	cls: Type[R] = redical_cls if redical_cls is not None else Redical
	return cls(pool)
