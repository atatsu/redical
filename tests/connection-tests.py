import asyncio
from dataclasses import dataclass
from typing import Any, Tuple
from unittest import mock
from urllib.parse import quote

import pytest
from yarl import URL

from redical import (
	create_connection,
	AbortTransaction,
	Connection,
	ConnectionClosedError,
	ConnectionClosingError,
	PipelineError,
	ResponseError,
	TransactionError,
	WatchError,
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
	conn = await create_connection(redis_uri, timeout=1)
	await conn.execute('flushdb')
	yield conn
	if not conn.is_closed and conn.is_closing:
		await conn.wait_closed()
		return
	if not conn.is_closed:
		conn.close()
		await conn.wait_closed()


@pytest.fixture
async def conn2(redis_uri):
	conn = await create_connection(redis_uri, timeout=1)
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
	result = await conn.execute('set', 'myotherkey', '????')
	assert True is result
	result = await conn.execute('get', 'myotherkey')
	assert '????' == result

	korean = '????????????'
	encoded = korean.encode('iso2022_kr')
	result = await conn.execute('set', 'myotherkey', encoded)
	assert True is result
	result = await conn.execute('get', 'myotherkey', encoding='iso2022_kr')
	assert '????????????' == result


async def test_execute_encoding_conn(redis_uri):
	"""
	Custom encoding passed via connection creation.
	"""
	conn = await create_connection(redis_uri, encoding='iso2022_kr')
	await conn.execute('flushdb')
	korean = '????????????'
	encoded = korean.encode('iso2022_kr')
	await conn.execute('set', 'mykey', encoded)
	assert '????????????' == await conn.execute('get', 'mykey')
	conn.close()
	await conn.wait_closed()


async def test_execute_encoding_conn_override(redis_uri):
	"""
	Custom encoding passed via `execute` method overriding instance setting.
	"""
	conn = await create_connection(redis_uri, encoding='iso2022_kr')
	await conn.execute('flushdb')
	await conn.execute('set', 'mykey', '????????????')
	assert '????????????' == await conn.execute('get', 'mykey', encoding='utf-8')
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


async def test_pipeline_error_prevents_buffer_write(conn):
	with pytest.raises(ValueError):
		async with conn as pipe:
			fut1 = pipe.execute('SET', 'foo', 'bar')
			fut2 = pipe.execute('SET', 'bar', 'baz')
			raise ValueError('an error')
	assert 0 == await conn.execute('EXISTS', 'foo')
	assert 0 == await conn.execute('EXISTS', 'bar')
	with pytest.raises(ValueError):
		await fut1
	with pytest.raises(ValueError):
		await fut2
	assert False is conn._in_pipeline
	assert 0 == len(conn._pipeline_buffer)
	assert 0 == len(conn._resolvers)


async def test_pipeline_sanity(conn):
	"""
	Ensure pipeline futures can be cleared while not interferring with normal
	futures.
	"""
	fut1 = conn.execute('set', 'foo', 'bar')
	try:
		async with conn as pipe:
			fut2 = pipe.execute('set', 'bar', 'baz')
	except AttributeError:
		pytest.fail('Clear attempt on normal future')
	await asyncio.gather(fut1, fut2)


# |-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|
# Transactions

async def test_transaction_basics(conn):
	"""
	Scenario:
		WATCH mykey
		val = GET mykey
		val = val + 1
		MULTI
		SET mykey $val
		EXEC
	"""
	await conn.execute('SET', 'mykey', 1)
	with mock.patch.object(conn, 'execute', wraps=conn.execute) as _execute:
		async with conn.transaction('mykey', 'myotherkey') as t:
			assert True is conn._in_transaction
			val = int(await asyncio.wait_for(t.execute('GET', 'mykey'), 1))
			val += 1
			async with t as pipe:
				assert True is pipe._in_pipeline
				fut = pipe.execute('SET', 'mykey', val)
		_execute.assert_any_call('WATCH', 'mykey', 'myotherkey')
	assert False is conn._in_transaction
	assert True is await asyncio.wait_for(fut, 1)
	assert 2 == int(await conn.execute('GET', 'mykey'))


async def test_transaction_no_watch(conn):
	with mock.patch.object(conn, 'execute', wraps=conn.execute) as _execute:
		async with conn.transaction():
			pass
		_execute.assert_not_called()


async def test_transaction_pre_multi_exec(conn):
	"""
	Found a logic error with the transaction handling that assumed all list replies
	were the EXEC response. If any commands are executed before the MULTI/EXEC commands
	(before the connection's `__aenter__`/`__aexit__` are triggered) that result in a list
	response (such as `hgetall`) stuff blows up. This verifies the fix.
	"""
	await conn.execute('HSET', 'mykey', 'field1', 'value1', 'field2', 'value2')
	async with conn.transaction('mykey') as t:
		res = await conn.execute('hgetall', 'mykey')
		async with t as pipe:
			pipe.execute('hset', 'mykey', 'field1', 'foo')
			pipe.execute('hset', 'mykey', 'field2', 'bar')
	assert ['field1', 'value1', 'field2', 'value2'] == res

	res = await conn.execute('hgetall', 'mykey')
	assert ['field1', 'foo', 'field2', 'bar'] == res


async def test_transaction_watch_error(conn, conn2):
	await conn.execute('SET', 'mykey', 1)
	async with conn.transaction('mykey', 'myotherkey') as t:
		val = int(await t.execute('GET', 'mykey'))
		val += 1
		with pytest.raises(WatchError, match='Transaction aborted, WATCHed keys: mykey, myotherkey'):
			async with t as pipe:
				await conn2.execute('SET', 'mykey', 'foo')
				fut = pipe.execute('SET', 'mykey', val)
	assert 'foo' == await conn.execute('GET', 'mykey')
	with pytest.raises(WatchError, match='Transaction aborted, WATCHed keys: mykey, myotherkey'):
		await asyncio.wait_for(fut, 1)


async def test_transaction_user_abort(conn):
	with mock.patch.object(conn, 'execute', wraps=conn.execute) as _execute:
		async with conn.transaction('akey') as t:
			try:
				async with t as pipe:
					fut1 = pipe.execute('SET', 'key1', 'value1')
					fut2 = pipe.execute('SET', 'key2', 'value2')
					fut3 = pipe.execute('SET', 'key3', 'value3')
					raise AbortTransaction()
			except Exception:
				pytest.fail('did not abort gracefully')
		_execute.assert_called_with('UNWATCH')

	assert False is conn._in_pipeline
	assert 0 == len(conn._pipeline_buffer)
	assert 0 == len(conn._resolvers)
	assert 0 == len(conn._watched_keys)
	assert 0 == await conn.execute('EXISTS', 'key1')
	assert 0 == await conn.execute('EXISTS', 'key2')
	assert 0 == await conn.execute('EXISTS', 'key3')

	with pytest.raises(AbortTransaction):
		await fut1
	with pytest.raises(AbortTransaction):
		await fut2
	with pytest.raises(AbortTransaction):
		await fut3


async def test_transaction_user_abort_no_commands(conn):
	with mock.patch.object(conn, 'execute', wraps=conn.execute) as _execute:
		async with conn.transaction('akey') as t:
			try:
				async with t:
					raise AbortTransaction()
			except Exception as ex:
				pytest.fail(f'did not abort gracefully: {ex}')
		_execute.assert_called_with('UNWATCH')
	assert False is conn._in_pipeline
	assert 0 == len(conn._pipeline_buffer)
	assert 0 == len(conn._resolvers)
	assert 0 == len(conn._watched_keys)


async def test_transaction_error_prevents_buffer_write(conn):
	with mock.patch.object(conn, 'execute', wraps=conn.execute) as _execute:
		with pytest.raises(ValueError):
			async with conn.transaction('akey'):
				raise ValueError('an error')
		_execute.assert_called_with('UNWATCH')
	assert False is conn._in_transaction
	assert 0 == len(conn._pipeline_buffer)
	assert 0 == len(conn._resolvers)
	assert 0 == len(conn._watched_keys)


async def test_transaction_watch_no_buffered_commands(conn):
	with mock.patch.object(conn, 'execute', wraps=conn.execute) as _execute:
		async with conn.transaction('mykey') as t:
			async with t:
				pass
		_execute.assert_called_with('UNWATCH')


async def test_transaction_already_in_transaction(conn):
	async with conn.transaction():
		with pytest.raises(TransactionError, match='Already in transaction mode'):
			async with conn.transaction():
				pass


async def test_transaction_closed(conn):
	conn.close()
	await conn.wait_closed()
	with pytest.raises(ConnectionClosedError, match='Connection is closed'):
		async with conn.transaction():
			pass


async def test_transaction_closing(conn):
	conn.close()
	with pytest.raises(ConnectionClosingError, match='Connection is closing'):
		async with conn.transaction():
			pass
