import asyncio
from dataclasses import dataclass
from typing import Any, Tuple
from unittest import mock
from urllib.parse import quote

import pytest
from yarl import URL

from redical import (
	create_connection,
	Connection,
	ConnectionClosedError,
	ConnectionClosingError,
	PipelineError,
	ResponseError,
)

pytestmark = [pytest.mark.asyncio]


# TODO: SSL

@dataclass
class Server:
	address: Tuple[str, int]
	server: Any
	event: asyncio.Event


@dataclass
class UnixServer:
	path: str
	server: Any


@pytest.fixture
async def unix_server(unix_socket):
	server = None
	_writer = None

	async def handler(reader, writer):
		nonlocal _writer
		_writer = writer

	server = await asyncio.start_unix_server(handler, path=unix_socket)
	quoted = quote(unix_socket, safe='')
	path = f'unix://{quoted}'
	yield UnixServer(path=path, server=server)
	_writer.close()
	await _writer.wait_closed()


@pytest.fixture
async def disconnecting_server(unused_port):
	event = asyncio.Event()
	server = None

	async def handler(reader, writer):
		nonlocal server
		nonlocal event
		writer.write_eof()
		writer.close()
		await writer.wait_closed()
		server.close()
		await server.wait_closed()
		event.set()

	server = await asyncio.start_server(handler, '127.0.0.1', unused_port)
	return Server(address=('127.0.0.1', unused_port), server=server, event=event)


@pytest.fixture
async def conn(redis_uri):
	conn = await create_connection(redis_uri)
	await conn.execute('flushdb')
	yield conn
	if not conn.is_closed and conn.is_closing:
		await conn.wait_closed()
		return
	if not conn.is_closed:
		conn.close()
		await conn.wait_closed()


async def test_create_connection_uri(redis_uri):
	conn = await create_connection(redis_uri)
	assert isinstance(conn, Connection)
	assert not conn.is_closed
	conn.close()
	try:
		await asyncio.wait_for(conn.wait_closed(), timeout=1)
	except asyncio.TimeoutError:
		pytest.fail('connection failed to close gracefully')
	assert conn.is_closed


async def test_create_connection_address(redis_uri):
	url = URL(redis_uri)
	conn = await create_connection((url.host, url.port))
	assert isinstance(conn, Connection)
	assert not conn.is_closed
	conn.close()
	try:
		await asyncio.wait_for(conn.wait_closed(), timeout=1)
	except asyncio.TimeoutError:
		pytest.fail('connection failed to close gracefully')
	assert conn.is_closed


async def test_create_connection_wait_no_close(redis_uri):
	conn = await create_connection(redis_uri)
	try:
		await asyncio.wait_for(conn.wait_closed(), timeout=1)
	except asyncio.TimeoutError:
		pytest.fail('a proper exception was not raised')
	except RuntimeError:
		pass
	finally:
		conn.close()
		await conn.wait_closed()


async def test_create_connection_remote_closed(disconnecting_server):
	async with disconnecting_server.server:
		conn = await create_connection(disconnecting_server.address)
		await disconnecting_server.event.wait()
	assert conn.is_closed


async def test_create_connection_unix_socket(unix_server):
	async with unix_server.server:
		conn = await create_connection(unix_server.path)
		assert not conn.is_closed
		conn.close()
		try:
			await asyncio.wait_for(conn.wait_closed(), timeout=1)
		except asyncio.TimeoutError:
			pytest.fail('connection failed to close gracefully')


async def test_execute_resolve_immediately(conn):
	result = await conn.execute('set', 'mykey', 'foo')
	assert True is result
	result = await conn.execute('exists', 'mykey')
	assert 1 == result
	result = await conn.execute('get', 'mykey')
	assert 'foo' == result


async def test_conn_double_close(conn):
	conn.close()
	with pytest.raises(ConnectionClosingError, match='Connection is already closing'):
		conn.close()


async def test_conn_already_closed(conn):
	conn.close()
	await conn.wait_closed()
	with pytest.raises(ConnectionClosedError, match='Connection is already closed'):
		conn.close()


async def test_wait_not_closed(conn):
	with pytest.raises(RuntimeError, match='Connection is not closing'):
		await conn.wait_closed()


async def test_double_wait_closed(conn):
	conn.close()
	await conn.wait_closed()
	with pytest.raises(RuntimeError, match='Connection is not closing'):
		await conn.wait_closed()


async def test_execute_closed(conn):
	conn.close()
	await conn.wait_closed()
	with pytest.raises(ConnectionClosedError, match='Connection is closed'):
		await conn.execute('ping')


async def test_execute_closing(conn):
	conn.close()
	with pytest.raises(ConnectionClosingError, match='Connection is closing'):
		await conn.execute('ping')


# |-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|
# Error responses

async def test_response_error(conn):
	with pytest.raises(ResponseError, match="ERR wrong number of arguments for 'hset' command"):
		await conn.execute('hset', 'mykey')


async def test_response_error_pipeline(conn):
	async with conn:
		fut1 = conn.execute('set', 'foo', 'bar')
		fut2 = conn.execute('hset', 'mykey')
		fut3 = conn.execute('get', 'foo')

	assert True is await fut1
	assert 'bar' == await fut3
	with pytest.raises(ResponseError, match="ERR wrong number of arguments for 'hset' command"):
		await fut2


async def test_custom_error_response(conn):
	def custom_error(exc):
		return ValueError(str(exc).replace('ERR ', ''))

	with pytest.raises(ValueError, match="wrong number of arguments for 'hset' command"):
		await conn.execute('hset', 'mykey', error_func=custom_error)


# |-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|
# Encoding-specific tests

async def test_execute_encoding(conn):
	"""
	Custom encoding passed via the `execute` method.
	"""
	result = await conn.execute('set', 'myotherkey', '😀')
	assert True is result
	result = await conn.execute('get', 'myotherkey')
	assert '😀' == result

	korean = '훈민정음'
	encoded = korean.encode('iso2022_kr')
	result = await conn.execute('set', 'myotherkey', encoded)
	assert True is result
	result = await conn.execute('get', 'myotherkey', encoding='iso2022_kr')
	assert '훈민정음' == result


async def test_execute_encoding_conn(redis_uri):
	"""
	Custom encoding passed via connection creation.
	"""
	conn = await create_connection(redis_uri, encoding='iso2022_kr')
	await conn.execute('flushdb')
	korean = '훈민정음'
	encoded = korean.encode('iso2022_kr')
	await conn.execute('set', 'mykey', encoded)
	assert '훈민정음' == await conn.execute('get', 'mykey')
	conn.close()
	await conn.wait_closed()


async def test_execute_encoding_conn_override(redis_uri):
	"""
	Custom encoding passed via `execute` method overriding instance setting.
	"""
	conn = await create_connection(redis_uri, encoding='iso2022_kr')
	await conn.execute('flushdb')
	await conn.execute('set', 'mykey', '훈민정음')
	assert '훈민정음' == await conn.execute('get', 'mykey', encoding=None)
	conn.close()
	await conn.wait_closed()


# |-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|
# Decoding-specific tests

async def test_list_results_no_conversion(conn):
	await conn.execute('sadd', 'mykey', 'one', 'two', 'three', 'four', 'five', 'six')
	assert set(['one', 'two', 'three', 'four', 'five', 'six']) == set(await conn.execute('smembers', 'mykey'))


# |-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|
# Pipelines

async def test_pipeline(conn):
	async with conn:
		fut1 = conn.execute('set', 'a', 'foo')
		fut2 = conn.execute('set', 'b', 'bar')
		fut3 = conn.execute('set', 'c', 'baz')
		fut4 = conn.execute('get', 'a')

	assert 'foo' == await conn.execute('get', 'a')
	assert 'bar' == await conn.execute('get', 'b')
	assert 'baz' == await conn.execute('get', 'c')

	assert True is await fut1
	assert True is await fut2
	assert True is await fut3
	assert 'foo' == await fut4


async def test_pipeline_conn_in_use(conn):
	async with conn:
		assert conn.in_use


async def test_pipeline_improper_await(conn):
	async with conn:
		fut1 = conn.execute('set', 'a', 'foo')
		with pytest.raises(PipelineError, match='Do not await connection method calls inside a pipeline block!'):
			await conn.execute('set', 'b', 'bar')
		fut2 = conn.execute('set', 'c', 'baz')

	assert 'foo' == await conn.execute('get', 'a')
	assert 'bar' == await conn.execute('get', 'b')
	assert 'baz' == await conn.execute('get', 'c')

	assert True is await fut1
	assert True is await fut2


async def test_pipeline_already_in_pipeline(conn):
	async with conn:
		with pytest.raises(PipelineError, match='Already in pipeline mode'):
			async with conn:
				pass


async def test_pipeline_closed(conn):
	conn.close()
	await conn.wait_closed()
	with pytest.raises(ConnectionClosedError, match='Connection is closed'):
		async with conn:
			pass


async def test_pipeline_closing(conn):
	conn.close()
	with pytest.raises(ConnectionClosingError, match='Connection is closing'):
		async with conn:
			pass


async def test_pipeline_no_commands(conn):
	with mock.patch.object(conn, '_writer') as _writer:
		_writer.drain = mock.AsyncMock()
		async with conn:
			pass
	_writer.write.assert_not_called()
	_writer.drain.assert_not_called()
	assert False is conn._in_pipeline
