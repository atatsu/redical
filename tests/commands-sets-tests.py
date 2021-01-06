import pytest  # type: ignore

from redical.command.set import SscanResponse

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


async def test_set_sscan_full_iteration_with_count(redical):
	# first populate a set with a sufficient number of elements
	expected = list(range(1000))
	assert 1000 == await redical.sadd('mykey', *expected)
	actual = []
	iterations = 0
	max_iterations = 12  # little bit of a buffer
	response = None
	while iterations < max_iterations:
		cursor = 0 if response is None else response.cursor
		if cursor == "0":
			break
		response = await redical.sscan('mykey', cursor, count=100)
		assert isinstance(response, SscanResponse)
		assert isinstance(response.elements, set)
		actual.extend([int(x) for x in response.elements])
		iterations += 1
	else:
		pytest.fail('failed to break which means the `count` parameter was not sent')
	actual.sort()
	assert expected == actual


async def test_set_sscan_full_iteration_with_match(redical):
	a = [f'yy:{x}' for x in range(20)]
	b = [str(x) for x in range(20, 40)]
	assert 40 == await redical.sadd('mykey', *b, *a)
	response = await redical.sscan('mykey', 0, match='yy:*', count=1000)
	assert "0" == response.cursor
	assert set(a) == response.elements


async def test_set_sscan_iter_full_iteration(redical):
	expected = list(range(1000))
	assert 1000 == await redical.sadd('mykey', *expected)
	actual = []
	async for x in redical.sscan_iter('mykey', count=10):
		actual.append(int(x))
	actual.sort()
	assert expected == actual
