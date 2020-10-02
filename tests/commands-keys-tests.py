import pytest

from redical import InvalidKeyError, NoExpiryError

pytestmark = [pytest.mark.asyncio]


async def test_delete(redical):
	assert True is await redical.set('foo', 'bar')
	assert True is await redical.set('bar', 'baz')
	assert True is await redical.set('baz', 'foo')
	assert 1 == await redical.delete('foo')
	assert 2 == await redical.delete('bar', 'baz')
	assert 0 == await redical.delete('foobar')
	assert 0 == await redical.exists('foo', 'bar', 'baz')


async def test_exists_no_key(redical):
	assert 0 == await redical.exists('mykey')


async def test_exists_one_key(redical):
	await redical.execute('set', 'mykey', 'foo')
	assert 1 == await redical.exists('mykey')


async def test_exists_multiple(redical):
	await redical.execute('set', 'mykey1', 'foo')
	await redical.execute('set', 'mykey2', 'foo')
	assert 2 == await redical.exists('mykey1', 'mykey2', 'mykey3')


async def test_expire(redical):
	assert True is await redical.set('mykey', 'foo')
	assert True is await redical.expire('mykey', 10)
	assert 10 == await redical.ttl('mykey')
	assert False is await redical.expire('notakey', 10)


async def test_pttl_no_key(redical):
	with pytest.raises(InvalidKeyError, match="Key with name 'mykey' does not exist"):
		await redical.pttl('mykey')


async def test_pttl_no_expire(redical):
	assert True is await redical.set('mykey', 'foo')
	with pytest.raises(NoExpiryError, match="Key with name 'mykey' has no expiry set"):
		await redical.pttl('mykey')


async def test_pttl_expire(redical):
	assert True is await redical.set('mykey', 'foo')
	await redical.execute('PEXPIRE', 'mykey', 10000)
	# since we're using milliseconds we'll allow some leeway
	assert 9990 < await redical.pttl('mykey') <= 10000


async def test_ttl_no_key(redical):
	with pytest.raises(InvalidKeyError, match="Key with name 'mykey' does not exist"):
		await redical.ttl('mykey')


async def test_ttl_no_expire(redical):
	assert True is await redical.set('mykey', 'foo')
	with pytest.raises(NoExpiryError, match="Key with name 'mykey' has no expiry set"):
		await redical.ttl('mykey')


async def test_ttl_expire(redical):
	await redical.set('mykey', 'foo')
	await redical.execute('EXPIRE', 'mykey', 10)
	assert 10 == await redical.ttl('mykey')
