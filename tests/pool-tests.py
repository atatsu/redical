import pytest

from redical import create_pool, PoolClosedError, PoolClosingError


@pytest.fixture
async def pool(redis_uri):
	pool = await create_pool(redis_uri, min_size=2)
	yield pool
	if not pool.is_closed and pool.is_closing:
		await pool.wait_closed()
		return
	if not pool.is_closed:
		pool.close()
		await pool.wait_closed()


@pytest.mark.asyncio
async def test_min_lower_than_max(redis_uri):
	with pytest.raises(ValueError, match="'min_size' must be lower than 'max_size'"):
		await create_pool(redis_uri, min_size=10, max_size=1)


@pytest.mark.asyncio
async def test_db_less_than_zero(redis_uri):
	with pytest.raises(ValueError, match="'db' must be a non-negative number"):
		await create_pool(redis_uri, db=-1)


@pytest.mark.asyncio
async def test_max_chunk_size_less_than_zero(redis_uri):
	with pytest.raises(ValueError, match="'max_chunk_size' must be a number greater than zero"):
		await create_pool(redis_uri, max_chunk_size=-1)


@pytest.mark.asyncio
async def test_min_pool_filled(pool):
	assert 2 == pool.available
	assert 2 == pool.size


@pytest.mark.asyncio
async def test_pool_double_close(pool):
	pool.close()
	with pytest.raises(PoolClosingError, match='Pool is already closing'):
		pool.close()


@pytest.mark.asyncio
async def test_pool_already_closed(pool):
	pool.close()
	await pool.wait_closed()
	with pytest.raises(PoolClosedError, match='Pool is already closed'):
		pool.close()


@pytest.mark.asyncio
async def test_wait_not_closed(pool):
	with pytest.raises(RuntimeError, match='Pool is not closing'):
		await pool.wait_closed()


@pytest.mark.asyncio
async def test_double_wait_closed(pool):
	pool.close()
	await pool.wait_closed()
	with pytest.raises(RuntimeError, match='Pool is not closing'):
		await pool.wait_closed()
