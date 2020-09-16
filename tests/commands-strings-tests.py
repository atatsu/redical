import pytest  # type: ignore

from redical import InvalidKeyError


@pytest.mark.asyncio
async def test_get(redical):
	assert True is await redical.set('mykey', 'foo')
	assert 'foo' == await redical.get('mykey')


@pytest.mark.asyncio
async def test_get_no_key(redical):
	with pytest.raises(InvalidKeyError, match="Key with name 'mykey' does not exist"):
		await redical.get('mykey')


@pytest.mark.asyncio
async def test_incr(redical):
	assert 1 == await redical.incr('mykey')
	assert 2 == await redical.incr('mykey')


@pytest.mark.asyncio
async def test_set_basic(redical):
	assert True is await redical.set('mykey', 'foo and a bar')
	assert 'foo and a bar' == await redical.execute('get', 'mykey')


@pytest.mark.asyncio
async def test_set_both_only_if(redical):
	with pytest.raises(ValueError):
		await redical.set('myvalueerrorkey', 'foo', only_if_exists=True, only_if_not_exists=True)
	assert 0 == await redical.exists('myvalueerrorkey')


@pytest.mark.asyncio
async def test_set_both_expires(redical):
	with pytest.raises(ValueError):
		await redical.set('myvalueerrorkey', 'foo', expire_in_seconds=500, expire_in_milliseconds=500)
	assert 0 == await redical.exists('myvalueerrorkey')


@pytest.mark.asyncio
async def test_set_only_if_exists(redical):
	await redical.set('myexistingkey', 'foo')
	assert False is await redical.set('mykey', 'foo', only_if_exists=True)
	assert 0 == await redical.exists('mykey')
	assert True is await redical.set('myexistingkey', 'bar', only_if_exists=True)
	assert 'bar' == await redical.execute('get', 'myexistingkey')


@pytest.mark.asyncio
async def test_set_only_if_not_exists(redical):
	assert True is await redical.set('myexistingkey', 'foo')
	assert True is await redical.set('mykey', 'foo', only_if_not_exists=True)
	assert False is await redical.set('myexistingkey', 'bar', only_if_not_exists=True)
	assert 'foo' == await redical.execute('get', 'myexistingkey')
	assert 'foo' == await redical.execute('get', 'mykey')


@pytest.mark.asyncio
async def test_set_expire_in_seconds(redical):
	assert True is await redical.set('mykey', 'foo', expire_in_seconds=10)
	assert 10 == await redical.ttl('mykey')


@pytest.mark.asyncio
async def test_set_expire_in_milliseconds(redical):
	assert True is await redical.set('mykey', 'foo', expire_in_milliseconds=10000)
	# give some leeway since we're in milliseconds
	assert 9990 < await redical.pttl('mykey') <= 10000


@pytest.mark.asyncio
async def test_set_expire_in_seconds_converted_milliseconds(redical):
	assert True is await redical.set('mykey', 'foo', expire_in_seconds=5.5)
	assert 5490 < await redical.pttl('mykey') <= 5500
