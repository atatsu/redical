from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass
from types import TracebackType
from typing import (
	cast,
	Any,
	AnyStr,
	Awaitable,
	Callable,
	Deque,
	Final,
	Generator,
	List,
	NamedTuple,
	Optional,
	Tuple,
	Type,
	Union,
	TYPE_CHECKING,
)
from urllib.parse import unquote

from yarl import URL

from .abstract import AbstractParser, RedicalResource
from .exception import ConnectionClosedError, ConnectionClosingError, PipelineError, ResponseError
from .parser import Parser

ConversionFunc = Callable[[Any], Any]

if TYPE_CHECKING:
	from asyncio import Future, StreamReader, StreamWriter, Task

__all__: List[str] = ['create_connection', 'Address', 'Connection']

LOG: Final[logging.Logger] = logging.getLogger(__name__)


class Address(NamedTuple):
	host: str
	port: int


class undefined:
	pass


@dataclass
class Resolver:
	encoding: str
	future: 'Future'
	conversion_func: Optional[ConversionFunc] = None


# TODO: SSL
# TODO: password
# TODO: connection timeout
# TODO: db passed in uri (only allow in uri *or* keyword arg, but not both)
# TODO: support other options as uri query params (encoding, max_chunk_size)

async def create_connection(
	address_or_uri: Union[Tuple[str, int], str],
	*,
	db: int = 0,
	encoding: str = 'utf-8',
	# theoretical maximum size of a TCP packet
	max_chunk_size: int = 65535,
	parser: Optional[AbstractParser] = None
) -> Connection:
	"""
	"""
	address: Optional[Address] = None
	scheme: Optional[str] = None
	host: str
	port: int
	reader: 'StreamReader'
	writer: 'StreamWriter'
	if isinstance(address_or_uri, str):
		url: URL = URL(address_or_uri)
		scheme = url.scheme
		if scheme in ('redis', 'rediss'):
			host = str(url.host)
			port = int(str(url.port))
			address = Address(host, port)
		elif scheme == 'unix':
			if url.host is None:
				raise NotImplementedError('not a valid unix socket')
			path: str = unquote(str(url.host))
			LOG.debug(f'unix scheme {path} ({url})')
			address = Address(path, 0)
	elif isinstance(address_or_uri, (list, tuple)):
		host, port = address_or_uri
		address = Address(host, port)
	else:
		raise NotImplementedError()

	if address is None:
		raise NotImplementedError('not a valid address')

	LOG.debug(f'attempting to connect to {address}')
	if scheme != 'unix':
		reader, writer = await asyncio.open_connection(address.host, address.port)
	else:
		reader, writer = await asyncio.open_unix_connection(address.host)

	if parser is None:
		parser = Parser()
	conn: Connection = Connection(
		reader,
		writer,
		address=address,
		db=db,
		encoding=encoding,
		max_chunk_size=max_chunk_size,
		parser=parser
	)
	# TODO: do a ping to verify connection is good
	LOG.info(f'Successfully connected to {address}')
	return conn


def _build_command(command: AnyStr, *args: Any) -> bytes:
	"""
	Serializes a command and its arguments via RESP (https://redis.io/topics/protocol).
	"""
	# TODO: only allow str, bytes, bytearray, int, float
	cmd: bytearray = bytearray()
	command = command.strip().upper()
	_command: bytes
	cmd.extend(b'*%d\r\n' % (len(args) + 1))
	if isinstance(command, str):
		_command = command.encode()
	else:
		_command = command
	cmd.extend(b'$%d\r\n' % len(_command))
	cmd.extend(b'%s\r\n' % _command)
	arg: Any
	for arg in args:
		_arg: bytes
		if isinstance(arg, bytes):
			_arg = arg
		elif isinstance(arg, str):
			# FIXME?: encoding
			_arg = arg.encode()
		elif isinstance(arg, int):
			_arg = b'%d' % arg
		elif isinstance(arg, float):
			_arg = b'%a' % arg
		else:
			raise NotImplementedError(f'Unable to encode type {type(arg)}')
		cmd.extend(b'$%d\r\n' % len(_arg))
		cmd.extend(b'%s\r\n' % _arg)
	return cmd


def _decode(parsed: Any, encoding: str, conversion_func: Optional[ConversionFunc] = None) -> Any:
	if isinstance(parsed, bytes):
		decoded: str = parsed.decode(encoding)
		if callable(conversion_func):
			return conversion_func(decoded)
		return decoded
	elif isinstance(parsed, list):
		x: Any
		decoded_members: List[Any] = [_decode(x, encoding) for x in parsed]
		if callable(conversion_func):
			return conversion_func(decoded_members)
		return decoded_members

	if callable(conversion_func):
		return conversion_func(parsed)
	return parsed


if TYPE_CHECKING:
	WrapperBase = asyncio.Future
else:
	WrapperBase = object


class PipelineFutureWrapper(WrapperBase):
	"""
	Wraps a future being used by `Connection.execute` when in pipeline mode.
	If awaited before the pipeline is in its final stage an exception will be raised.
	This prevents the premature await from blocking forever.

	Awaiting one of the pipeline futures inside the pipeline block itself would prevent
	the pipeline command from being sent which in turn would prevent the pipeline futures
	from ever being resolved.
	"""
	__slots__: Tuple[str, str] = ('_future', '_pipeline_in_progress')

	_future: 'Future'
	_pipeline_in_progress: bool

	def __init__(self, future: 'Future') -> None:
		self._future = future
		self._pipeline_in_progress = True

	def clear_in_progress(self) -> None:
		self._pipeline_in_progress = False

	def __getattr__(self, attr: str) -> Any:
		return getattr(self._future, attr)

	def __await__(self) -> Generator[None, None, Any]:
		if self._pipeline_in_progress:
			LOG.warning("'await' detected inside pipeline block")
			raise PipelineError('Do not await connection method calls inside a pipeline block!')
		return self._future.__await__()

	def __repr__(self) -> str:
		return self._future.__repr__()


class Connection(RedicalResource):
	"""
	"""
	_address: Tuple[str, int]
	_closing: bool
	_db: int
	_encoding: str
	_in_pipeline: bool
	_max_chunk_size: int
	_parser: AbstractParser
	_pipeline_buffer: bytearray
	_reader: 'StreamReader'
	_read_data_cancel_event: asyncio.Event
	_read_data_task: 'Task'
	_resolvers: Deque[Resolver]
	_writer: 'StreamWriter'

	@property
	def address(self) -> Tuple[str, int]:
		return self._address

	@property
	def db(self) -> int:
		return self._db

	@property
	def encoding(self) -> str:
		return self._encoding

	@property
	def in_use(self) -> bool:
		return len(self._resolvers) > 0 or self._in_pipeline

	@property
	def is_closed(self) -> bool:
		return self._read_data_cancel_event.is_set() or (self._reader.at_eof() and self._writer.is_closing())

	@property
	def is_closing(self) -> bool:
		return self._closing

	@property
	def supports_multiple_pipelines(self) -> bool:
		return False

	def __init__(
		self,
		reader: 'StreamReader',
		writer: 'StreamWriter',
		/,
		*,
		address: Tuple[str, int],
		db: int,
		encoding: str,
		max_chunk_size: int,
		parser: AbstractParser
	) -> None:
		self._address = address
		self._closing = False
		self._db = db
		self._encoding = encoding
		self._in_pipeline = False
		self._max_chunk_size = max_chunk_size
		self._parser = parser
		self._pipeline_buffer = bytearray()
		self._reader = reader
		self._read_data_cancel_event = asyncio.Event()
		self._read_data_task = asyncio.create_task(self._read_data())
		self._read_data_task.add_done_callback(self._set_read_state)
		self._resolvers = deque()
		self._writer = writer

	def close(self) -> None:
		if self._closing:
			raise ConnectionClosingError('Connection is already closing')
		elif self.is_closed:
			raise ConnectionClosedError('Connection is already closed')

		self._closing = True
		LOG.info(f'Connection closing [{self!r}]')
		self._writer.close()
		self._read_data_task.cancel()

	def execute(
		self,
		command: AnyStr,
		*args: Any,
		conversion_func: Optional[ConversionFunc] = None,
		encoding: Union[Type[undefined], Optional[str]] = undefined,
		**kwargs: Any
	) -> Awaitable[Any]:
		if self.is_closed:
			raise ConnectionClosedError()
		if self.is_closing:
			raise ConnectionClosingError()

		_encoding: Optional[str]
		if encoding is undefined:
			_encoding = self._encoding
		else:
			_encoding = 'utf-8' if encoding is None else str(encoding)

		cmd: bytes = _build_command(command, *args)
		if not self._in_pipeline:
			LOG.debug(f'executing command: {cmd!r}')
			self._writer.write(cmd)
			asyncio.create_task(self._writer.drain())
		else:
			LOG.debug(f'buffering command: {cmd!r}')
			self._pipeline_buffer.extend(cmd)
		future: 'Future' = asyncio.get_running_loop().create_future()
		if self._in_pipeline:
			future = PipelineFutureWrapper(future)
		self._resolvers.append(
			Resolver(encoding=_encoding, future=future, conversion_func=conversion_func)
		)
		return future

	async def wait_closed(self) -> None:
		if not self._closing:
			raise RuntimeError('Connection is not closing')

		await self._writer.wait_closed()
		await self._read_data_cancel_event.wait()
		LOG.info(f'Disconnected gracefully from {self.address}')
		self._closing = False

	async def _read_data(self) -> None:
		while not self._reader.at_eof():
			try:
				data: bytes = await self._reader.read(self._max_chunk_size)
				LOG.debug(f'received data: {data!r}')
				self._parser.feed(data)
				parsed: Any
				while (parsed := self._parser.gets()) is not False:
					LOG.debug(f'parsed response object: {parsed}')
					# TODO: what if there is no future to pop?
					resolver: Resolver = self._resolvers.popleft()
					try:
						if isinstance(parsed, ResponseError):
							resolver.future.set_exception(parsed)
							continue
						decoded: Any = _decode(parsed, resolver.encoding, resolver.conversion_func)
						LOG.debug(f'decoded response: {decoded}')
						resolver.future.set_result(decoded)
					except Exception as ex:
						resolver.future.set_exception(ex)
			except asyncio.IncompleteReadError:
				# lost connection to remote host
				break
			except Exception:
				LOG.exception('Unhandled exception while reading data')
		LOG.warning(f'Lost connection to {self.address}')
		self.close()
		await self.wait_closed()

	def _set_read_state(self, read_data_task: 'Task') -> None:
		LOG.debug('read task successfully cancelled')
		self._read_data_cancel_event.set()

	async def __aenter__(self) -> Connection:
		if self.is_closed:
			raise ConnectionClosedError()
		if self.is_closing:
			raise ConnectionClosingError()
		if self._in_pipeline:
			raise PipelineError('Already in pipeline mode')

		self._in_pipeline = True
		self._pipeline_buffer = bytearray()
		return self

	async def __aexit__(
		self, exc_type: Optional[Type[BaseException]], exc: Optional[BaseException], tb: Optional[TracebackType]
	) -> Optional[bool]:
		if not self._pipeline_buffer:
			# no commands executed
			self._in_pipeline = False
			LOG.debug('pipeline exiting with no buffered commands')
			return None
		resolvers: List[Resolver] = list(self._resolvers)
		resolver: Resolver
		LOG.debug('writing pipeline buffer')
		self._writer.write(self._pipeline_buffer)
		await self._writer.drain()
		self._in_pipeline = False
		for resolver in resolvers:
			cast(PipelineFutureWrapper, resolver.future).clear_in_progress()
		self._pipeline_buffer = bytearray()
		await asyncio.gather(*[resolver.future for resolver in resolvers], return_exceptions=True)
		return None

	def __repr__(self) -> str:
		return (
			f'<Connection(address={self._address}, db={self._db}, is_closed={self.is_closed}, '
			f'is_closing={self.is_closing})>'
		)
