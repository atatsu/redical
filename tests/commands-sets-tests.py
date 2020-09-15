import pytest  # type: ignore


@pytest.mark.asyncio
async def test_set_smembers(redical):
	await redical.execute('SADD', 'mykey', 'one', 'two', 'three')
	assert {'one', 'two', 'three'} == await redical.smembers('mykey')


@pytest.mark.asyncio
async def test_set_srem(redical):
	await redical.execute('SADD', 'mykey', 'one', 'two', 'three')
	assert 2 == await redical.srem('mykey', 'two', 'one', 'four')
	assert 1 == await redical.srem('mykey', 'three')
	assert 0 == await redical.srem('mykey', 'one', 'two', 'three')
