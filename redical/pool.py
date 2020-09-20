from __future__ import annotations

import asyncio
import logging
from collections import deque
from types import TracebackType
from typing import Any, AnyStr, Awaitable, Callable, Deque, Final, List, Optional, Set, Tuple, Type, Union

from .abstract import AbstractParser, RedicalResource
from .connection import create_connection, Connection
from .exception import PoolClosedError, PoolClosingError

ConversionFunc = Callable[[Any], Any]

LOG: Final[logging.Logger] = logging.getLogger(__name__)


class undefined:
	pass


async def create_pool(
	address_or_uri: Union[Tuple[str, int], str],
	*,
	db: int = 0,
	encoding: str = 'utf-8',
	# theoretical maximum of a TCP socket
	max_chunk_size: int = 65535,
	max_size: int = 10,
	min_size: int = 1,
	parser: Optional[AbstractParser] = None
) -> ConnectionPool:
	if int(min_size) > int(max_size):
		raise ValueError("'min_size' must be lower than 'max_size'")
	if int(db) < 0:
		raise ValueError("'db' must be a non-negative number")
	if int(max_chunk_size) < 1:
		raise ValueError("'max_chunk_size' must be a number greater than zero")

	pool: ConnectionPool = ConnectionPool(
		address_or_uri=address_or_uri,
		db=db,
		encoding=encoding,
		max_chunk_size=max_chunk_size,
		max_size=int(max_size),
		min_size=int(min_size),
		parser=parser,
	)
	await pool._populate()
	return pool


# A pool's connections consist of the following:
# * a possible maximum: `max_size`
# * an absolute minimum: `min_size`
# * connections it is acquiring: `_acquiring`
# * inactive connections sitting around in the pool collection: `_pool`
# * connections currently in use: `_in_use`
# * available = `len(_pool)`
# * size = `_acquiring + available + len(_in_use`

# * upon creation fill pool with active connections up to its `min_size`

# whenever an operation is being performed on the connections as a group use a `Condition`
# whenever a connection is released back into th pool `notify` on `Condition`
# whenever a connection is being retrieved from the pool, if one isn't immediately available
#   (and the pool is at max size) `wait` on `Condition`

# Exclusive connection situations
# * pub/sub
# * transaction
# * pipeline

# Connection use without exclusivity
# * one-shot commands

class ConnectionPool(RedicalResource):
	_address_or_uri: Union[Tuple[str, int], str]
	_acquiring: int
	_close_event: asyncio.Event
	_closing: bool
	_connections_condition: asyncio.Condition
	_db: int
	_encoding: str
	_in_use: Set[Connection]
	_max_chunk_size: int
	_min_size: int
	_parser: Optional[AbstractParser]
	_pool: Deque[Connection]

	@property
	def address(self) -> Tuple[str, int]:
		# return self._address
		return ('not implemented', 0)

	@property
	def available(self) -> int:
		"""
		Number of connections that are sitting idle (potentially) and available for use.
		"""
		return len(self._pool)

	@property
	def db(self) -> int:
		return self._db

	@property
	def is_closed(self) -> bool:
		return self._close_event.is_set()

	@property
	def is_closing(self) -> bool:
		return self._closing

	@property
	def encoding(self) -> str:
		return self._encoding

	@property
	def size(self) -> int:
		"""
		Total number of connections in this pool, regardless of their current state (active or idle).
		"""
		return self.available + len(self._in_use) + self._acquiring

	def __init__(
		self,
		*,
		address_or_uri: Union[Tuple[str, int], str],
		db: int,
		encoding: str,
		max_chunk_size: int,
		max_size: int,
		min_size: int,
		parser: Optional[AbstractParser]
	) -> None:
		self._address_or_uri = address_or_uri
		self._acquiring = 0
		self._db = db
		self._close_event = asyncio.Event()
		self._closing = False
		self._connections_condition = asyncio.Condition()
		self._encoding = encoding
		# any connections in here are considered in use and will need to be returned to
		# `self._pool` before they can be used elsewhere
		self._in_use = set()
		self._max_chunk_size = max_chunk_size
		self._min_size = min_size
		self._parser = parser
		# any connections in here are considered available for use
		self._pool = deque(maxlen=max_size)

	def close(self) -> None:
		if self._closing:
			raise PoolClosingError()
		elif self.is_closed:
			raise PoolClosedError()

		LOG.info(f'Closing all connections ({self.size})')
		self._closing = True
		task: asyncio.Task = asyncio.create_task(self._close_all_connections())
		task.add_done_callback(lambda x: self._close_event.set())

	def execute(
		self,
		command: AnyStr,
		*args: Any,
		conversion_func: Optional[ConversionFunc] = None,
		encoding: Union[Type[undefined], Optional[str]] = undefined,
		**kwargs: Any
	) -> Awaitable[Any]:
		raise NotImplementedError('execute')

	async def wait_closed(self) -> None:
		if not self._closing:
			raise RuntimeError('Pool is not closing')

		await self._close_event.wait()
		LOG.info('All connections have been closed')
		self._closing = False

	async def _close_all_connections(self) -> None:
		close_waits: List[Awaitable[None]] = []
		conn: Connection
		for conn in self._pool:
			conn.close()
			close_waits.append(conn.wait_closed())
		for conn in self._in_use:
			conn.close()
			close_waits.append(conn.wait_closed())
		await asyncio.gather(*close_waits)

	async def _populate(self) -> None:
		while self.size < self._min_size:
			self._acquiring += 1
			try:
				conn: Connection = await create_connection(
					self._address_or_uri,
					db=self._db,
					encoding=self._encoding,
					max_chunk_size=self._max_chunk_size,
					parser=self._parser
				)
				# TODO: do a ping to verify connection is good
				self._pool.append(conn)
			finally:
				self._acquiring -= 1
		LOG.info(f'Populated connection pool with {self.available} connection(s)')

	async def __aenter__(self) -> ConnectionPool:
		raise NotImplementedError('__aenter__')

	async def __aexit__(
		self, exc_type: Optional[Type[BaseException]], exc: Optional[BaseException], tb: Optional[TracebackType]
	) -> Optional[bool]:
		raise NotImplementedError('__aexit__')
