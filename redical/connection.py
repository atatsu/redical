from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass
from typing import Any, AnyStr, Deque, Final, List, NamedTuple, Optional, Tuple, Union, TYPE_CHECKING
from urllib.parse import unquote

from yarl import URL

from .abstract import AbstractConnection, AbstractParser
from .parser import Parser

if TYPE_CHECKING:
	from asyncio import Future, StreamReader, StreamWriter, Task

__all__: List[str] = ['create_connection', 'Address', 'Connection']

LOG: Final[logging.Logger] = logging.getLogger(__name__)

# Simple Strings are encoded in the following way: a plus character, followed by a string that cannot
# contain a CR or LF character (no newlines are allowed), terminated by CRLF (that is "\r\n").
SIMPLE_STRING: Final[str] = '+'
ERROR: Final[str] = '-'
INTEGER: Final[str] = ':'
BULK_STRING: Final[str] = '$'
ARRAY: Final[str] = '*'
TERMINATE: Final[str] = '\r\n'


class Address(NamedTuple):
	host: str
	port: int
	scheme: Optional[str]


@dataclass
class EncodingFuture:
	encoding: str
	future: 'Future'


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


def _decode(parsed: Any, encoding: str) -> Any:
	if isinstance(parsed, bytes):
		return parsed.decode(encoding)
	elif isinstance(parsed, list):
		x: Any
		return [_decode(x, encoding) for x in parsed]
	return parsed


class Connection(AbstractConnection):
	"""
	"""
	_address: Tuple[str, int]
	_closing: bool
	_encoding_futures: Deque[EncodingFuture]
	_db: int
	_parser: AbstractParser
	_reader: 'StreamReader'
	_reader_state: asyncio.Event
	_read_data_task: 'Task'
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
		self._encoding_futures = deque()
		self._parser = parser
		self._reader = reader
		self._read_data_cancel_event = asyncio.Event()
		self._read_data_task = asyncio.create_task(self._read_data())
		self._read_data_task.add_done_callback(self._set_read_state)
		self._writer = writer

	def close(self) -> None:
		self._writer.close()
		self._read_data_task.cancel()
		self._closing = True

	async def execute(self, command: AnyStr, *args: Any, encoding: str = 'utf-8', **kwargs: Any) -> Any:
		cmd: bytes = _build_command(command, *args)
		LOG.debug(f'executing command: {cmd!r}')
		self._writer.write(cmd)
		await self._writer.drain()
		future: 'Future' = asyncio.get_running_loop().create_future()
		self._encoding_futures.append(EncodingFuture(encoding=encoding, future=future))
		# if not in multi or pipeline await result immediately
		return await future

	async def wait_closed(self) -> None:
		if not self._closing:
			raise RuntimeError('Connection is not closing')
		await self._writer.wait_closed()
		await self._read_data_cancel_event.wait()
		LOG.info(f'Disconnected gracefully from {self.address}')

	async def _read_data(self) -> None:
		while not self._reader.at_eof():
			try:
				# XXX: I'm not sure if this is a good idea or not. Depending on the command that was
				#      sent the RESP response could contain a ton of these which would mean there
				#      would need to be *several* iterations of this loop in order to retrieve the
				#      entire response.
				data: bytes = await self._reader.read(1024)
				LOG.debug(f'received data: {data!r}')
				self._parser.feed(data)
				parsed: Any = self._parser.gets()
				if parsed is False:
					continue
				LOG.debug(f'parsed response object: {parsed}')
				# TODO: what if there is no future to pop?
				encoding_future: EncodingFuture = self._encoding_futures.popleft()
				encoding_future.future.set_result(_decode(parsed, encoding_future.encoding))
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

	def __repr__(self) -> str:
		return f'Connection(db={self._db})'
