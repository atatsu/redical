import pytest

from redical import NoExpiryError

pytestmark = [pytest.mark.asyncio]


async def test_get(redical):
	assert True is await redical.set('mykey', 'foo')
	assert 'foo' == await redical.get('mykey')


async def test_get_typeerror(redical):
	await redical.hset('mykey', field1='value1', field2='value2')
	with pytest.raises(TypeError):
		await redical.get('mykey')


async def test_get_no_key(redical):
	assert None is await redical.get('mykey')


async def test_incr(redical):
	assert 1 == await redical.incr('mykey')
	assert 2 == await redical.incr('mykey')


async def test_incr_typeerror(redical):
	await redical.hset('mykey', field1='value1', field2='value2')
	with pytest.raises(TypeError):
		await redical.incr('mykey')


async def test_incr_valueerror(redical):
	assert True is await redical.set('mykey', 'foo')
	with pytest.raises(ValueError):
		await redical.incr('mykey')


async def test_incrby(redical):
	assert 1 == await redical.incr('mykey')
	assert 11 == await redical.incrby('mykey', 10)


async def test_incrby_typeerror(redical):
	assert 1 == await redical.hset('mykey', field1='value1')
	with pytest.raises(TypeError):
		await redical.incrby('mykey', 10)


async def test_incrby_valueerror(redical):
	assert True is await redical.set('mykey', 'foo')
	with pytest.raises(ValueError):
		await redical.incrby('mykey', 10)


async def test_set_basic(redical):
	assert True is await redical.set('mykey', 'foo and a bar')
	assert 'foo and a bar' == await redical.execute('get', 'mykey')


async def test_set_both_only_if(redical):
	with pytest.raises(ValueError):
		await redical.set('myvalueerrorkey', 'foo', only_if_exists=True, only_if_not_exists=True)
	assert 0 == await redical.exists('myvalueerrorkey')


async def test_set_both_expires(redical):
	with pytest.raises(ValueError):
		await redical.set('myvalueerrorkey', 'foo', expire_in_seconds=500, expire_in_milliseconds=500)
	assert 0 == await redical.exists('myvalueerrorkey')


async def test_set_only_if_exists(redical):
	await redical.set('myexistingkey', 'foo')
	assert False is await redical.set('mykey', 'foo', only_if_exists=True)
	assert 0 == await redical.exists('mykey')
	assert True is await redical.set('myexistingkey', 'bar', only_if_exists=True)
	assert 'bar' == await redical.execute('get', 'myexistingkey')


async def test_set_only_if_not_exists(redical):
	assert True is await redical.set('myexistingkey', 'foo')
	assert True is await redical.set('mykey', 'foo', only_if_not_exists=True)
	assert False is await redical.set('myexistingkey', 'bar', only_if_not_exists=True)
	assert 'foo' == await redical.execute('get', 'myexistingkey')
	assert 'foo' == await redical.execute('get', 'mykey')


async def test_set_expire_in_seconds(redical):
	assert True is await redical.set('mykey', 'foo', expire_in_seconds=10)
	assert 10 == await redical.ttl('mykey')


async def test_set_expire_in_milliseconds(redical):
	assert True is await redical.set('mykey', 'foo', expire_in_milliseconds=10000)
	# give some leeway since we're in milliseconds
	assert 9990 < await redical.pttl('mykey') <= 10000


async def test_set_expire_in_seconds_converted_milliseconds(redical):
	assert True is await redical.set('mykey', 'foo', expire_in_seconds=5.5)
	assert 5490 < await redical.pttl('mykey') <= 5500


async def test_set_keep_ttl(redical):
	assert True is await redical.set('mykey', 'foo', expire_in_seconds=10)
	assert True is await redical.set('mykey', 'bar')
	try:
		await redical.ttl('mykey')
	except NoExpiryError:
		pass

	assert True is await redical.set('mykey', 'foo', expire_in_seconds=10)
	assert True is await redical.set('mykey', 'bar', keep_ttl=True)
	assert 10 == await redical.ttl('mykey')
