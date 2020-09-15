import pytest  # type: ignore


@pytest.mark.asyncio
async def test_flushdb(redical):
	await redical.execute('set', 'mykey', 'foo')
	assert 1 == await redical.execute('exists', 'mykey')
	await redical.flushdb()
	assert 0 == await redical.execute('exists', 'mykey')
