import asyncio

import pytest

from redical import create_redical, PipelineError, TransactionError, WatchError

pytestmark = [pytest.mark.asyncio]

# There will probably be a lot of duplicated tests here (specifically
# test cases pulled straight from the connection-specific and pool-specific
# tests) to ensure the "higher-level" functionality plays nicely with the
# expected behaviors of the "lower-level" functionality.


async def test_error_func(redical):
	def error_func(exc):
		return TypeError('no!')

	with pytest.raises(TypeError, match='no!'):
		await redical.execute('hset', 'mykey', error_func=error_func)


# |-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|
# Pipelines

async def test_pipeline(redical):
	async with redical as pipe:
		fut1 = pipe.set('foo', 'bar')
		fut2 = pipe.set('bar', 'baz')
		fut3 = pipe.get('foo')
		fut4 = pipe.get('bar')

	assert True is await fut1
	assert True is await fut2
	assert 'bar' == await fut3
	assert 'baz' == await fut4


async def test_multiple_pipelines_prevented(redis_uri):
	redical = await create_redical(redis_uri)
	async with redical:
		with pytest.raises(PipelineError):
			async with redical:
				pass
	redical.close()
	await redical.wait_closed()


async def test_context_sanity_check(redical):
	"""
	More geared towards the pool-based redical instance.
	"""
	if not redical.resource.supports_multiple_pipelines:
		return

	async def t1(event):
		async with redical as pipe:
			pipe.set('foo', 'bar')
			fut = pipe.get('foo')
			await event.wait()
		assert 'bar' == await fut

	async def t2(event):
		async with redical as pipe:
			pipe.set('foo', 'baz')
			fut = pipe.get('foo')
		event.set()
		assert 'baz' == await fut

	event = asyncio.Event()
	await asyncio.gather(t1(event), t2(event))


async def test_pipeline_disallow_close(redical):
	async with redical as pipe:
		with pytest.raises(PipelineError, match='Do not close from within pipeline'):
			pipe.close()


async def test_pipeline_disallow_wait_closed(redical):
	async with redical as pipe:
		with pytest.raises(PipelineError, match='Do not close from within pipeline'):
			await pipe.wait_closed()


# |-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|
# Transactions

async def test_transaction(redical):
	async with redical.transaction() as tr:
		async with tr as pipe:
			fut1 = pipe.set('foo', 'bar')
			fut2 = pipe.set('bar', 'baz')
			fut3 = pipe.get('foo')
			fut4 = pipe.get('bar')

	assert True is await fut1
	assert True is await fut2
	assert 'bar' == await fut3
	assert 'baz' == await fut4


async def test_transaction_watch_error(redical, conn):
	await redical.set('mykey', 1)
	async with redical.transaction('mykey') as tr:
		val = int(await tr.get('mykey'))
		val += 1
		with pytest.raises(WatchError, match='Transaction aborted'):
			async with tr as pipe:
				await conn.execute('SET', 'mykey', 'foo')
				fut = pipe.set('mykey', val)
	assert 'foo' == await redical.get('mykey')
	with pytest.raises(WatchError, match='Transaction aborted'):
		await fut


async def test_transaction_disallow_close(redical):
	async with redical.transaction() as tr:
		with pytest.raises(TransactionError, match='Do not close from within transaction'):
			tr.close()


async def test_transaction_disallow_wait_closed(redical):
	async with redical.transaction() as tr:
		with pytest.raises(TransactionError, match='Do not close from within transaction'):
			await tr.wait_closed()
