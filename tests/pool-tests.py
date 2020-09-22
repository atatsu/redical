import asyncio
from unittest import mock

import pytest

from redical import create_pool, PoolClosedError, PoolClosingError

pytestmark = [pytest.mark.asyncio]


@pytest.fixture
async def pool(redis_uri):
	pool = await create_pool(redis_uri, max_size=4, min_size=2)
	await pool.execute('flushdb')
	yield pool
	if not pool.is_closed and pool.is_closing:
		await pool.wait_closed()
		return
	if not pool.is_closed:
		pool.close()
		await pool.wait_closed()


async def test_min_lower_than_max(redis_uri):
	with pytest.raises(ValueError, match="'min_size' must be lower than 'max_size'"):
		await create_pool(redis_uri, min_size=10, max_size=1)


async def test_db_less_than_zero(redis_uri):
	with pytest.raises(ValueError, match="'db' must be a non-negative number"):
		await create_pool(redis_uri, db=-1)


async def test_max_chunk_size_less_than_zero(redis_uri):
	with pytest.raises(ValueError, match="'max_chunk_size' must be a number greater than zero"):
		await create_pool(redis_uri, max_chunk_size=-1)


async def test_min_pool_filled(pool):
	assert 2 == pool.available
	assert 2 == pool.size


async def test_pool_double_close(pool):
	pool.close()
	with pytest.raises(PoolClosingError, match='Pool is already closing'):
		pool.close()


async def test_pool_already_closed(pool):
	pool.close()
	await pool.wait_closed()
	with pytest.raises(PoolClosedError, match='Pool is already closed'):
		pool.close()


async def test_wait_not_closed(pool):
	with pytest.raises(RuntimeError, match='Pool is not closing'):
		await pool.wait_closed()


async def test_double_wait_closed(pool):
	pool.close()
	await pool.wait_closed()
	with pytest.raises(RuntimeError, match='Pool is not closing'):
		await pool.wait_closed()


# |-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|
# Internal connection state

async def test_acquiring_connection_rotates_pool(pool):
	# make it look like the first connection in the pool is being used
	# so that we *should* pick the second one to execute our command
	# the connection needs to stay in the pool so that we know we're rotating past it.
	# if not we could just enter a pipeline and in the same block execute a random command

	# get rid of one of the connections
	conn = pool._pool.popleft()
	conn.close()
	await conn.wait_closed()

	# replace with a mock connection
	conn = mock.Mock(in_use=True, is_closed=False, is_closing=False, execute=mock.AsyncMock(return_value='PONG'))
	pool._pool.appendleft(conn)
	assert 'PONG' == await pool.execute('ping')
	# remove the mock from the internal pool so cleanup doesn't complain
	pool._pool.remove(conn)
	conn.execute.assert_not_called()


async def test_release_drop_closed_connection(pool):
	conn = await pool._acquire_unused_connection(remove_from_pool=True)
	assert 1 == pool.size
	conn.close()
	await conn.wait_closed()
	await pool._release_connection(conn)
	assert 1 == pool.size
	assert conn not in pool._pool


async def test_release_drop_closing_connection(pool):
	conn = await pool._acquire_unused_connection(remove_from_pool=True)
	assert 1 == pool.size
	conn.close()
	await pool._release_connection(conn)
	await conn.wait_closed()
	assert 1 == pool.size
	assert conn not in pool._pool


async def test_acquire_create_new(pool):
	conn = await pool._acquire_unused_connection(remove_from_pool=True)
	pool._in_use.add(conn)
	conn = await pool._acquire_unused_connection(remove_from_pool=True)
	pool._in_use.add(conn)
	conn = await pool._acquire_unused_connection()
	assert 3 == pool.size
	assert 1 == pool.available


async def test_acquire_waits_if_no_available_connection(pool):
	conns = []
	for x in range(4):
		conn = await pool._acquire_unused_connection(remove_from_pool=True)
		pool._in_use.add(conn)
		conns.append(conn)
	assert 4 == pool.size
	# there are now no available connections in the pool and it is at its maximum size
	event = asyncio.Event()

	async def acquire(event):
		loop = asyncio.get_running_loop()
		loop.call_soon(event.set)
		conn = await pool._acquire_unused_connection()
		assert conn in conns

	async def release(event):
		await event.wait()
		await pool._release_connection(conns[-1])

	await asyncio.wait_for(asyncio.gather(release(event), acquire(event)), timeout=1)


async def test_acquire_prune_stale_connections(pool):
	assert 2 == pool.available
	for conn in pool._pool:
		conn.close()
		await conn.wait_closed()
	assert all([conn.is_closed for conn in pool._pool])
	await pool.execute('set', 'foo', 'bar')
	assert 1 == pool.size


# |-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|
# Execute

async def test_execute_basic(pool):
	await pool.execute('set', 'foo', 'bar')
	assert 'bar' == await pool.execute('get', 'foo')


async def test_execute_no_free_connections(pool):
	conns = []
	for x in range(4):
		conns.append(await pool._acquire_unused_connection(remove_from_pool=True))
		pool._in_use.add(conns[-1])
	assert pool.size == pool.max_size

	event = asyncio.Event()

	async def execute(event):
		loop = asyncio.get_event_loop()
		loop.call_soon(event.set)
		True is await pool.execute('set', 'foo', 'bar')

	async def release(event):
		await event.wait()
		await pool._release_connection(conns[-1])

	await asyncio.wait_for(asyncio.gather(release(event), execute(event)), timeout=1)


async def test_execute_pool_closed(pool):
	pool.close()
	await pool.wait_closed()
	with pytest.raises(PoolClosedError, match='Pool is closed'):
		await pool.execute('get', 'foo')


async def test_execute_pool_closing(pool):
	pool.close()
	with pytest.raises(PoolClosingError, match='Pool is closing'):
		await asyncio.wait_for(pool.execute('get', 'foo'), timeout=1)


# |-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|
# Pipelines

async def test_pipeline(pool):
	async with pool as conn:
		fut1 = conn.execute('set', 'foo', 'bar')
		fut2 = conn.execute('set', 'bar', 'baz')
		fut3 = conn.execute('get', 'foo')
		fut4 = conn.execute('get', 'bar')

	assert True is await fut1
	assert True is await fut2
	assert 'bar' == await fut3
	assert 'baz' == await fut4


async def test_pipelines_sequester_connection(pool):
	async with pool:
		assert 2 == pool.size
		assert 1 == pool.available


async def test_context_sanity_check(pool):
	async def t1(event):
		async with pool as conn:
			conn.execute('set', 'foo', 'bar')
			fut = conn.execute('get', 'foo')
			await event.wait()
		assert 'bar' == await fut

	async def t2(event):
		async with pool as conn:
			conn.execute('set', 'foo', 'baz')
			fut = conn.execute('get', 'foo')
			assert 0 == pool.available
			assert 2 == pool.size
		event.set()
		assert 'baz' == await fut

	event = asyncio.Event()
	await asyncio.gather(t1(event), t2(event))


async def test_pipeline_releases_connection(pool):
	async with pool:
		assert 2 == pool.size
		assert 1 == pool.available
	assert 2 == pool.size
	assert 2 == pool.available


async def test_pipeline_pool_closed(pool):
	pool.close()
	await pool.wait_closed()
	with pytest.raises(PoolClosedError, match='Pool is closed'):
		async with pool:
			pass


async def test_pipeline_pool_closing(pool):
	pool.close()
	with pytest.raises(PoolClosingError, match='Pool is closing'):
		async with pool:
			pass
