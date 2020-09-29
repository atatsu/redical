import pytest

pytestmark = [pytest.mark.asyncio]


async def test_hset(redical):
	assert 3 == await redical.hset('mykey', 'foo', 'bar', *dict(bar='baz', baz='foo').items())
	expected = ['foo', 'bar', 'bar', 'baz', 'baz', 'foo']
	assert expected == await redical.execute('hgetall', 'mykey')


async def test_hset_typeerror(redical):
	"""raise a ValueError for WRONGTYPE"""
	assert True is await redical.set('mykey', 'foo')
	with pytest.raises(TypeError, match='Operation against a key holding the wrong kind of value'):
		await redical.hset('mykey', 'foo', 'bar', *dict(bar='baz', baz='foo').items())


async def test_hset_valueerror(redical):
	"""raise a TypeError when odd number of field/value pairs"""
	with pytest.raises(ValueError, match='Number of supplied fields does not match the number of supplied values'):
		await redical.hset('mykey', 'foo', 'bar', *[('bar', 'baz'), ('baz',)])
