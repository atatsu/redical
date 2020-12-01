from __future__ import annotations

import asyncio
import logging
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass
from types import TracebackType
from typing import (
	cast,
	Any,
	AnyStr,
	AsyncIterator,
	Awaitable,
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

from .abstract import AbstractParser, ConversionFunc, ErrorFunc, RedicalResource
from .exception import (
	AbortTransaction,
	ConnectionClosedError,
	ConnectionClosingError,
	PipelineError,
	ResponseError,
	TransactionError,
	WatchError,
)
from .parser import Parser
from .util import undefined

if TYPE_CHECKING:
	from asyncio import Future, StreamReader, StreamWriter, Task

__all__: List[str] = ['create_connection', 'Address', 'Connection']

LOG: Final[logging.Logger] = logging.getLogger(__name__)


class Address(NamedTuple):
	host: str
	port: int


@dataclass
class Resolver:
	encoding: str
	future: 'Future'
	conversion_func: Optional[ConversionFunc] = None
	error_func: Optional[ErrorFunc] = None


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
	parser: Optional[AbstractParser] = None,
	timeout: Union[float, int] = 10,
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
		parser=parser,
		timeout=timeout
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
			LOG.warning(f'Unable to encode argument, command: {command!r}, args: {args}')
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
	_in_transaction: bool
	_max_chunk_size: int
	_parser: AbstractParser
	_pipeline_buffer: bytearray
	_reader: 'StreamReader'
	_read_data_cancel_event: asyncio.Event
	_read_data_task: 'Task'
	_resolvers: Deque[Resolver]
	_timeout: float
	_watched_keys: Tuple[str, ...]
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
		parser: AbstractParser,
		timeout: Union[float, int]
	) -> None:
		self._address = address
		self._closing = False
		self._db = db
		self._encoding = encoding
		self._in_pipeline = False
		self._in_transaction = False
		self._max_chunk_size = max_chunk_size
		self._parser = parser
		self._pipeline_buffer = bytearray()
		self._reader = reader
		self._read_data_cancel_event = asyncio.Event()
		self._read_data_task = asyncio.create_task(self._read_data())
		self._read_data_task.add_done_callback(self._set_read_state)
		self._resolvers = deque()
		self._timeout = float(timeout)
		self._watched_keys = ()
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
		error_func: Optional[ErrorFunc] = None,
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
			LOG.debug(f'executing command: {cmd!r} [{self}]')
			self._writer.write(cmd)
			asyncio.create_task(self._writer.drain())
		else:
			LOG.debug(f'buffering command: {cmd!r}')
			self._pipeline_buffer.extend(cmd)
		future: 'Future' = asyncio.get_running_loop().create_future()
		if self._in_pipeline:
			future = PipelineFutureWrapper(future)
		self._resolvers.append(
			Resolver(encoding=_encoding, future=future, conversion_func=conversion_func, error_func=error_func)
		)
		return future

	@asynccontextmanager
	async def transaction(self, *watch_keys: str) -> AsyncIterator[Connection]:
		if self.is_closed:
			raise ConnectionClosedError()
		if self.is_closing:
			raise ConnectionClosingError()
		if self._in_transaction:
			raise TransactionError('Already in transaction mode')

		self._in_transaction = True
		self._watched_keys = watch_keys
		if len(watch_keys) > 0:
			await self.execute('WATCH', *watch_keys)
			LOG.debug(f'WATCHing keys {", ".join(watch_keys)!r}')
		try:
			yield self
		except Exception:
			LOG.exception('Unhandled error while in transaction block')
			if len(watch_keys) > 0:
				LOG.debug(f'[{self}] UNWATCHing keys {self._watched_keys}')
				await self.execute('UNWATCH')
			raise
		finally:
			self._in_transaction = False
			self._watched_keys = ()

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
				resolver: Resolver
				while (parsed := self._parser.gets()) is not False:
					LOG.debug(f'parsed response object: {parsed}')

					parsed_results: List[Any]
					if self._in_transaction and parsed in ('QUEUED', b'QUEUED'):
						# we need to wait until the EXEC response to actually set
						# the futures' results
						continue

					if self._in_transaction and isinstance(parsed, list):
						# EXEC results received
						parsed_results = parsed
					elif self._in_transaction and parsed is None:
						# set a `WatchError` on all futures that are queued up
						while len(self._resolvers) > 0:
							resolver = self._resolvers.popleft()
							resolver.future.set_exception(WatchError(*self._watched_keys))
						continue
					else:
						parsed_results = [parsed]

					for parsed in parsed_results:
						# convert 'OK' responses to `True`
						if parsed in ('OK', b'OK'):
							parsed = True

						# TODO: what if there is no future to pop?
						resolver = self._resolvers.popleft()
						try:
							if isinstance(parsed, ResponseError):
								error: Exception = parsed
								if resolver.error_func is not None:
									error = resolver.error_func(error)
								resolver.future.set_exception(error)
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
		aborting: bool = False
		erroring: bool = False
		if exc is not None and isinstance(exc, AbortTransaction):
			LOG.debug('transaction aborted')
			aborting = True
		elif exc is not None and exc_type is not None and tb is not None:
			LOG.error('Unhandled error while in pipeline block', exc_info=(exc_type, exc, tb))
			erroring = True

		if not self._pipeline_buffer:
			# no commands executed
			self._in_pipeline = False
			if len(self._watched_keys) > 0:
				LOG.debug(f'[{self}] UNWATCHing keys {self._watched_keys}')
				await self.execute('UNWATCH')
			if aborting:
				# suppress the `AbortTransaction` exception
				return True
			LOG.debug('pipeline exiting with no buffered commands')
			return None

		resolver: Resolver
		resolvers: List[Resolver]
		if aborting or erroring:
			resolvers = []
			while len(self._resolvers) > 0:
				resolver = self._resolvers.popleft()
				resolver.future.set_exception(cast(BaseException, exc))
				resolvers.append(resolver)
		else:
			resolvers = list(self._resolvers)

		if self._in_transaction and not aborting:
			self._pipeline_buffer = bytearray(_build_command('MULTI')) + self._pipeline_buffer
			future: 'Future' = PipelineFutureWrapper(asyncio.get_running_loop().create_future())
			self._resolvers.insert(
				0,
				Resolver(encoding=self._encoding, future=future, conversion_func=None, error_func=None)
			)
			# The `EXEC` commaned is replied to with (if the transaction was executed) an array
			# of all replies for all commands executed in the EXEC block. Since it doesn't have
			# it's own dedicated reply we don't need to add a future for it
			self._pipeline_buffer.extend(_build_command('EXEC'))

		if not aborting and not erroring:
			LOG.debug(f'writing pipeline buffer: {self._pipeline_buffer!r} [{self}]')
			self._writer.write(self._pipeline_buffer)
			await self._writer.drain()

		self._in_pipeline = False
		self._pipeline_buffer = bytearray()

		for resolver in resolvers:
			cast(PipelineFutureWrapper, resolver.future).clear_in_progress()
		await asyncio.wait_for(
			asyncio.gather(*[resolver.future for resolver in resolvers], return_exceptions=True),
			timeout=self._timeout
		)
		if any(isinstance(resolver.future.exception(), WatchError) for resolver in resolvers):
			raise WatchError(*self._watched_keys)

		if aborting:
			LOG.debug(f'[{self}] UNWATCHing keys {self._watched_keys}')
			await self.execute('UNWATCH')
			# suppress the `AbortTransaction` exception
			return True
		return None

	def __repr__(self) -> str:
		return (
			f'<Connection[{id(self)}]('
			f'address={self._address}, db={self._db}, in_pipeline={self._in_pipeline}, '
			f'in_transaction={self._in_transaction}, is_closing={self.is_closing}, is_closed={self.is_closed}'
			')>'
		)
