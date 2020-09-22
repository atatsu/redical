import asyncio

import pytest

pytestmark = [pytest.mark.asyncio]

# There will probably be a lot of duplicated tests here (specifically
# test cases pulled straight from the connection-specific and pool-specific
# tests) to ensure the "higher-level" functionality plays nicely with the
# expected behaviors of the "lower-level" functionality.


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
