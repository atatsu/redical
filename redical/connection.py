from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass
from types import TracebackType
from typing import (
	Any,
	AnyStr,
	Callable,
	Deque,
	Final,
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

from .abstract import AbstractConnection, AbstractParser
from .parser import Parser

ConversionFunc = Callable[[Any], Any]

if TYPE_CHECKING:
	from asyncio import Future, StreamReader, StreamWriter, Task

__all__: List[str] = ['create_connection', 'Address', 'Connection']

LOG: Final[logging.Logger] = logging.getLogger(__name__)


class Address(NamedTuple):
	host: str
	port: int
	scheme: Optional[str]


@dataclass
class Resolver:
	encoding: str
	future: 'Future'
	conversion_func: Optional[ConversionFunc] = None


# TODO: SSL
# TODO: passwords

async def create_connection(
	address_or_uri: Union[Tuple[str, int], str],
	*,
	db: int = 0,
	parser: Optional[AbstractParser] = None
) -> Connection:
	"""
	"""
	address: Optional[Address] = None
	host: str
	port: int
	reader: 'StreamReader'
	writer: 'StreamWriter'
	if isinstance(address_or_uri, str):
		url: URL = URL(address_or_uri)
		if url.scheme in ('redis', 'rediss'):
			host = str(url.host)
			port = int(str(url.port))
			address = Address(host, port, url.scheme)
		elif url.scheme == 'unix':
			if url.host is None:
				raise NotImplementedError('not a valid unix socket')
			path: str = unquote(str(url.host))
			LOG.debug(f'unix scheme {path} ({url})')
			address = Address(path, 0, url.scheme)
	elif isinstance(address_or_uri, (list, tuple)):
		host, port = address_or_uri
		address = Address(host, port, None)
	else:
		raise NotImplementedError()

	if address is None:
		raise NotImplementedError('not a valid address')

	LOG.debug(f'attempting to connect to {address}')
	if address.scheme != 'unix':
		reader, writer = await asyncio.open_connection(address.host, address.port)
	else:
		reader, writer = await asyncio.open_unix_connection(address.host)

	if parser is None:
		parser = Parser()
	conn: Connection = Connection(reader, writer, address=(address[0], address[1]), parser=parser)
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
			# FIXME: encoding
			_arg = arg.encode()
		elif isinstance(arg, int):
			_arg = b'%d' % arg
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

	if callable(conversion_func):
		return conversion_func(parsed)
	return parsed


class Connection(AbstractConnection):
	"""
	"""
	_address: Tuple[str, int]
	_closing: bool
	_db: int
	_in_pipeline: bool
	_parser: AbstractParser
	_pipeline_buffer: bytearray
	_reader: 'StreamReader'
	_reader_state: asyncio.Event
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
	def is_closed(self) -> bool:
		return self._reader.at_eof() or self._writer.is_closing()

	def __init__(
		self,
		reader: 'StreamReader',
		writer: 'StreamWriter',
		/,
		*,
		address: Tuple[str, int],
		parser: AbstractParser
	) -> None:
		self._address = address
		self._closing = False
		self._db = 0
		self._in_pipeline = False
		self._parser = parser
		self._pipeline_buffer = bytearray()
		self._reader = reader
		self._read_data_cancel_event = asyncio.Event()
		self._read_data_task = asyncio.create_task(self._read_data())
		self._read_data_task.add_done_callback(self._set_read_state)
		self._resolvers = deque()
		self._writer = writer

	def close(self) -> None:
		self._writer.close()
		self._read_data_task.cancel()
		self._closing = True

	async def execute(
		self,
		command: AnyStr,
		*args: Any,
		encoding: str = 'utf-8',
		conversion_func: Optional[ConversionFunc] = None,
		**kwargs: Any
	) -> Any:
		cmd: bytes = _build_command(command, *args)
		if not self._in_pipeline:
			LOG.debug(f'executing command: {cmd!r}')
			self._writer.write(cmd)
			await self._writer.drain()
		else:
			LOG.debug(f'buffering command: {cmd!r}')
			self._pipeline_buffer.extend(cmd)
		future: 'Future' = asyncio.get_running_loop().create_future()
		self._resolvers.append(
			Resolver(encoding=encoding, future=future, conversion_func=conversion_func)
		)
		# if not in multi or pipeline await result immediately
		if not self._in_pipeline:
			return await future
		return future

	async def wait_closed(self) -> None:
		if not self._closing:
			raise RuntimeError('Connection is not closing')
		await self._writer.wait_closed()
		await self._read_data_cancel_event.wait()
		LOG.info(f'Disconnected gracefully from {self.address}')

	async def _read_data(self) -> None:
		while not self._reader.at_eof():
			try:
				data: bytes = await self._reader.read(1024)
				LOG.debug(f'received data: {data!r}')
				self._parser.feed(data)
				parsed: Any = self._parser.gets()
				if parsed is False:
					continue
				LOG.debug(f'parsed response object: {parsed}')
				# TODO: what if there is no future to pop?
				resolver: Resolver = self._resolvers.popleft()
				try:
					resolver.future.set_result(_decode(parsed, resolver.encoding, resolver.conversion_func))
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

	async def __aenter__(self) -> None:
		self._in_pipeline = True
		self._pipeline_buffer = bytearray()

	async def __aexit__(
		self, exc_type: Optional[Type[BaseException]], exc: Optional[BaseException], tb: Optional[TracebackType]
	) -> Optional[bool]:
		self._writer.write(self._pipeline_buffer)
		self._pipeline_buffer = bytearray()
		await self._writer.drain()
		self._in_pipeline = False
		return None

	def __repr__(self) -> str:
		return f'Connection(db={self._db})'
