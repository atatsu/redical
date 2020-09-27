from __future__ import annotations

import asyncio
import contextvars
import logging
from collections import deque
from types import TracebackType
from typing import (
	Any,
	AnyStr,
	Awaitable,
	Deque,
	Final,
	List,
	Optional,
	Set,
	Tuple,
	Type,
	Union,
)

from .abstract import AbstractParser, ConversionFunc, ErrorFunc, RedicalResource
from .connection import create_connection, undefined, Connection
from .exception import PoolClosedError, PoolClosingError

LOG: Final[logging.Logger] = logging.getLogger(__name__)


connection_ctx: contextvars.ContextVar = contextvars.ContextVar('connection')


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
	_max_size: int
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
	def max_size(self) -> int:
		return self._max_size

	@property
	def min_size(self) -> int:
		return self._min_size

	@property
	def size(self) -> int:
		"""
		Total number of connections in this pool, regardless of their current state (active or idle).
		"""
		return self.available + len(self._in_use) + self._acquiring

	@property
	def supports_multiple_pipelines(self) -> bool:
		return True

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
		self._max_size = max_size
		self._min_size = min_size
		self._parser = parser
		# any connections in here are considered available for use
		self._pool = deque(maxlen=max_size)

	def close(self) -> None:
		if self._closing:
			raise PoolClosingError('Pool is already closing')
		elif self.is_closed:
			raise PoolClosedError('Pool is already closed')

		LOG.info(f'Closing all connections ({self.size})')
		self._closing = True
		task: asyncio.Task = asyncio.create_task(self._close_all_connections())
		task.add_done_callback(lambda x: self._close_event.set())

	async def execute(
		self,
		command: AnyStr,
		*args: Any,
		conversion_func: Optional[ConversionFunc] = None,
		encoding: Union[Type[undefined], Optional[str]] = undefined,
		error_func: Optional[ErrorFunc] = None
	) -> Awaitable[Any]:
		if self.is_closed:
			raise PoolClosedError()
		if self.is_closing:
			raise PoolClosingError()

		conn: Connection = await self._acquire_unused_connection()
		return await conn.execute(
			command, *args, conversion_func=conversion_func, encoding=encoding, error_func=error_func
		)

	async def wait_closed(self) -> None:
		if not self._closing:
			raise RuntimeError('Pool is not closing')

		await self._close_event.wait()
		LOG.info('All connections have been closed')
		self._closing = False

	async def _acquire_unused_connection(self, remove_from_pool: bool = False) -> Connection:
		async with self._connections_condition:
			while True:
				i: int
				conn: Optional[Connection] = None
				available: int = self.available
				pruned: int = 0
				for i in range(available):
					# always take the first connection in the pool since we'll be rotating the pool for every
					# connection we decide not to use
					conn = self._pool[0]
					if conn.is_closed or conn.is_closing:
						self._pool.popleft()
						LOG.info('Removed stale connection from pool %s', self)
						pruned += 1
						conn = None
						if i + pruned > available:
							LOG.debug('no available connections')
							break
						continue
					else:
						self._pool.rotate(1)
						LOG.debug('rotated pool')
					if conn.in_use:
						continue

				if conn is None and self.size < self._max_size:
					conn = await self._add_additional_connection()

				if conn is None:
					LOG.debug('waiting for next available connection')
					await self._connections_condition.wait()
					continue

				if remove_from_pool:
					self._pool.remove(conn)
					LOG.debug('sequestered connection from pool %s', self)
				LOG.debug('retrieved connection from pool')
				return conn

	async def _add_additional_connection(self) -> Connection:
		self._acquiring += 1
		try:
			conn: Connection = await create_connection(
				self._address_or_uri,
				db=self._db,
				encoding=self._encoding,
				max_chunk_size=self._max_chunk_size,
				parser=self._parser
			)
			# TODO?: do a ping to verify connection is good
			self._pool.append(conn)
			LOG.info('Added additional connection to pool %s', self)
			return conn
		finally:
			self._acquiring -= 1

	async def _close_all_connections(self) -> None:
		async with self._connections_condition:
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
			await self._add_additional_connection()
		LOG.info(f'Populated connection pool with {self.available} connection(s)')

	async def _release_connection(self, conn: Connection) -> None:
		async with self._connections_condition:
			if conn in self._in_use:
				self._in_use.remove(conn)
			# Only add open connections back to the pool
			if not conn.is_closed and not conn.is_closing:
				self._pool.append(conn)
			self._connections_condition.notify()

	async def __aenter__(self) -> Connection:
		if self.is_closed:
			raise PoolClosedError()
		if self.is_closing:
			raise PoolClosingError()

		# get an unused connection, if need be and there is room to grow
		# create another one
		conn: Connection = await self._acquire_unused_connection(remove_from_pool=True)
		connection_ctx.set(conn)
		self._in_use.add(conn)
		return await conn.__aenter__()

	async def __aexit__(
		self, exc_type: Optional[Type[BaseException]], exc: Optional[BaseException], tb: Optional[TracebackType]
	) -> Optional[bool]:
		conn: Connection = connection_ctx.get()
		status: Optional[bool] = await conn.__aexit__(exc_type, exc, tb)
		await self._release_connection(conn)
		return status

	def __repr__(self) -> str:
		return (
			f'<ConnectionPool(available={self.available}, db={self.db}, in_use={len(self._in_use)})>'
		)
