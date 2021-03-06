import pytest

pytestmark = [pytest.mark.asyncio]


async def test_hdel(redical):
	await redical.hset('mykey', field1='foo', field2='bar', field3='baz')
	assert 2 == await redical.hdel('mykey', 'field1', 'field3', 'notafield')


async def test_hdel_typeerror(redical):
	"""raises a TypeError for WRONGTYPE"""
	assert True is await redical.set('mykey', 'foo')
	with pytest.raises(TypeError, match='Operation against a key holding the wrong kind of value'):
		await redical.hdel('mykey', 'field')


async def test_hget(redical):
	assert 1 == await redical.hset('mykey', myfield='foo')
	actual = await redical.hget('mykey', 'myfield')
	assert 'foo' == actual
	actual = await redical.hget('notakey', 'myfield')
	assert None is actual


async def test_hget_typeerror(redical):
	"""
	raise a TypeError for WRONGTYPE
	"""
	assert True is await redical.set('mykey', 'foo')
	with pytest.raises(TypeError, match='Operation against a key holding the wrong kind of value'):
		await redical.hget('mykey', 'myfield')


async def test_hgetall(redical):
	expected = dict(foo='bar', bar='baz', baz='foo')
	assert 3 == await redical.hset('mykey', **expected)
	assert expected == await redical.hgetall('mykey')


async def test_hgetall_typeerror(redical):
	"""raise a TypeError for WRONGTYPE"""
	assert True is await redical.set('mykey', 'foo')
	with pytest.raises(TypeError, match='Operation against a key holding the wrong kind of value'):
		await redical.hgetall('mykey')


async def test_hgetall_conversions(redical):
	"""make sure multiple conversions work"""
	expected = dict(foo='bar', bar='baz', baz='foo')
	assert 3 == await redical.hset('mykey', **expected)
	actual = await redical.hgetall('mykey', transform=lambda d: list(d.items()))
	assert list(expected.items()) == actual


async def test_hmget(redical):
	expected = dict(foo='bar', bar='baz', baz='foo')
	assert 3 == await redical.hset('mykey', **expected)
	expected.update(dict(foobar=None))
	assert expected == await redical.hmget('mykey', 'foo', 'bar', 'baz', 'foobar')


async def test_hmget_typeerror(redical):
	"""raise a TypeError for WRONGTYPE"""
	assert True is await redical.set('mykey', 'foo')
	with pytest.raises(TypeError, match='Operation against a key holding the wrong kind of value'):
		await redical.hmget('mykey', 'field1', 'field2')


async def test_hset_args(redical):
	assert 3 == await redical.hset('mykey', ('foo', 'bar'), ('bar', 'baz'), ('baz', 'foo'))
	expected = ['foo', 'bar', 'bar', 'baz', 'baz', 'foo']
	assert expected == await redical.execute('hgetall', 'mykey')


async def test_hset_kwargs(redical):
	assert 3 == await redical.hset('mykey', foo='bar', bar='baz', baz='foo')
	expected = ['foo', 'bar', 'bar', 'baz', 'baz', 'foo']
	assert expected == await redical.execute('hgetall', 'mykey')


async def test_hset_args_and_kwargs(redical):
	assert 3 == await redical.hset('mykey', ('foo', 'bar'), ('bar', 'baz'), baz='foo')
	expected = ['foo', 'bar', 'bar', 'baz', 'baz', 'foo']
	assert expected == await redical.execute('hgetall', 'mykey')


async def test_hset_typeerror(redical):
	"""raise a TypeError for WRONGTYPE"""
	assert True is await redical.set('mykey', 'foo')
	with pytest.raises(TypeError, match='Operation against a key holding the wrong kind of value'):
		await redical.hset('mykey', 'foo', 'bar', *dict(bar='baz', baz='foo').items())


async def test_hset_valueerror(redical):
	"""raise a ValueError when odd number of field/value pairs"""
	with pytest.raises(ValueError, match='Number of supplied fields does not match the number of supplied values'):
		await redical.hset('mykey', 'foo', 'bar', *[('bar', 'baz'), ('baz',)])


async def test_hexists(redical):
	assert False is await redical.hexists('mykey', 'myfield')
	assert 1 == await redical.hset('mykey', myfield='myvalue')
	assert True is await redical.hexists('mykey', 'myfield')


async def test_hexists_typeerror(redical):
	"""raise TypeError for WRONGTYPE"""
	assert True is await redical.set('mykey', 'foo')
	with pytest.raises(TypeError, match='Operation against a key holding the wrong kind of value'):
		await redical.hexists('mykey', 'myfield')
