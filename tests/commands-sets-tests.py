import pytest  # type: ignore

pytestmark = [pytest.mark.asyncio]


async def test_set_ismember(redical):
	assert 1 == await redical.sadd('mykey', 'one')
	assert False is await redical.sismember('mykey', 'two')
	assert True is await redical.sismember('mykey', 'one')


async def test_set_smembers(redical):
	await redical.execute('SADD', 'mykey', 'one', 'two', 'three')
	assert {'one', 'two', 'three'} == await redical.smembers('mykey')


async def test_set_srem(redical):
	await redical.execute('SADD', 'mykey', 'one', 'two', 'three')
	assert 2 == await redical.srem('mykey', 'two', 'one', 'four')
	assert 1 == await redical.srem('mykey', 'three')
	assert 0 == await redical.srem('mykey', 'one', 'two', 'three')


async def test_set_sadd(redical):
	assert 3 == await redical.sadd('mykey', 'one', 'two', 'three')
	assert {'one', 'two', 'three'} == await redical.smembers('mykey')
