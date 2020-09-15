import pytest  # type: ignore

from redical import InvalidKeyError, NoExpiryError


@pytest.mark.asyncio
async def test_exists_no_key(redical):
	assert 0 == await redical.exists('mykey')


@pytest.mark.asyncio
async def test_exists_one_key(redical):
	await redical.execute('set', 'mykey', 'foo')
	assert 1 == await redical.exists('mykey')


@pytest.mark.asyncio
async def test_exists_multiple(redical):
	await redical.execute('set', 'mykey1', 'foo')
	await redical.execute('set', 'mykey2', 'foo')
	assert 2 == await redical.exists('mykey1', 'mykey2', 'mykey3')


@pytest.mark.asyncio
async def test_pttl_no_key(redical):
	with pytest.raises(InvalidKeyError):
		await redical.pttl('mykey')


@pytest.mark.asyncio
async def test_pttl_no_expire(redical):
	assert True is await redical.set('mykey', 'foo')
	with pytest.raises(NoExpiryError):
		await redical.pttl('mykey')


@pytest.mark.asyncio
async def test_pttl_expire(redical):
	assert True is await redical.set('mykey', 'foo')
	await redical.execute('PEXPIRE', 'mykey', 10000)
	# since we're using milliseconds we'll allow some leeway
	assert 9990 < await redical.pttl('mykey') <= 10000


@pytest.mark.asyncio
async def test_ttl_no_key(redical):
	with pytest.raises(InvalidKeyError):
		await redical.ttl('mykey')


@pytest.mark.asyncio
async def test_ttl_no_expire(redical):
	assert True is await redical.set('mykey', 'foo')
	with pytest.raises(NoExpiryError):
		await redical.ttl('mykey')


@pytest.mark.asyncio
async def test_ttl_expire(redical):
	await redical.set('mykey', 'foo')
	await redical.execute('EXPIRE', 'mykey', 10)
	assert 10 == await redical.ttl('mykey')
