from unittest import mock

import pytest

from redical import Redical
from redical import ScorePolicy, UpdatePolicy
from redical.command.sortedset import _zadd_error_wrapper

pytestmark = [pytest.mark.asyncio]


async def test_zadd(redical: Redical):
	assert 2 == await redical.zadd('mykey', ('one', 1), ('two', 2))
	assert 1.0 == await redical.zscore('mykey', 'one')
	assert 2.0 == await redical.zscore('mykey', 'two')


async def test_zadd_invalid_key(redical):
	"""raises a TypeError for WRONGTYPE"""
	assert True is await redical.set('mykey', 'foo')
	with pytest.raises(TypeError, match='Operation against a key holding the wrong kind of value'):
		await redical.zadd('mykey', ('one', 1))


async def test_zadd_changed(redical):
	assert 2 == await redical.zadd('mykey', ('one', 1), ('two', 2))
	assert 0 == await redical.zadd('mykey', ('one', 3), ('two', 4))
	assert 2 == await redical.zadd('mykey', ('one', 5), ('two', 6), changed=True)


async def test_zadd_uneven_score_member_pairs(redical):
	with pytest.raises(ValueError):
		await redical.zadd('mykey', ('one', 1), (2,))


async def test_zadd_increment(redical):
	await redical.zadd('mykey', ('one', 1))
	await redical.zadd('mykey', ('one', 4), increment=True)
	assert 5.0 == await redical.zscore('mykey', 'one')


async def test_zadd_increment_additional_pairs(redical):
	with pytest.raises(ValueError):
		await redical.zadd('mykey', ('one', 1), ('two', 2), increment=True)


@pytest.mark.parametrize('update_policy, expected_flag', [
	(UpdatePolicy.EXISTS, 'XX'),
	(UpdatePolicy.NOT_EXISTS, 'NX'),
])
def test_zadd_update_policy(update_policy, expected_flag, redical):
	with mock.patch.object(redical, 'execute') as _execute:
		redical.zadd('mykey', ('one', 1), update_policy=update_policy)
		_execute.assert_called_once_with(
			'ZADD', 'mykey', expected_flag, 1, 'one', encoding='utf-8', error_func=_zadd_error_wrapper, transform=None
		)


@pytest.mark.parametrize('score_policy, expected_flag', [
	(ScorePolicy.GREATER_THAN, 'GT'),
	(ScorePolicy.LESS_THAN, 'LT')
])
def test_zadd_score_policy(score_policy, expected_flag, redical):
	with mock.patch.object(redical, 'execute') as _execute:
		redical.zadd('mykey', ('one', 1), score_policy=score_policy)
		_execute.assert_called_once_with(
			'ZADD', 'mykey', expected_flag, 1, 'one', encoding='utf-8', error_func=_zadd_error_wrapper, transform=None
		)


async def test_zadd_kwargs(redical):
	await redical.zadd('mykey', one=1, two=2)
	assert 1.0 == await redical.zscore('mykey', 'one')
	assert 2.0 == await redical.zscore('mykey', 'two')


async def test_zscore(redical):
	await redical.zadd('mykey', ('five', 5.5))
	assert 5.5 == await redical.zscore('mykey', 'five')


async def test_zscore_nonexistent_key(redical):
	assert None is await redical.zscore('notakey', 'notamember')


async def test_zscore_nonexistent_member(redical):
	await redical.zadd('mykey', five=5)
	assert None is await redical.zscore('mykey', 'notamember')


async def test_zscore_invalid_key(redical):
	assert True is await redical.set('mykey', 'foo')
	with pytest.raises(TypeError, match='Operation against a key holding the wrong kind of value'):
		await redical.zscore('mykey', 'five')


@pytest.fixture
async def add(redical):
	values = dict(one=1, two=2, three=3, four=4, five=5, six=6)
	await redical.zadd('mykey', **values)
	return values


async def test_zrange_index_vanilla(redical, add):
	results = await redical.zrange_index('mykey', 0, 3)
	assert ('one', 'two', 'three', 'four') == results


async def test_zrange_index_reversed(redical, add):
	results = await redical.zrange_index('mykey', 0, 3, reversed=True)
	assert ('six', 'five', 'four', 'three') == results


async def test_zrange_index_with_scores(redical, add):
	results = await redical.zrange_index('mykey', 0, 2, with_scores=True)
	assert (
		('one', 1),
		('two', 2),
		('three', 3),
	) == results


async def test_zrange_index_invalid_key(redical):
	assert True is await redical.set('mykey', 'foo')
	with pytest.raises(TypeError, match='Operation against a key holding the wrong kind of value'):
		await redical.zrange_index('mykey', 0, 2)
