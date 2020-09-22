import logging
import os
import socket

import pytest  # type: ignore

from redical import create_connection, create_redical, create_redical_pool

LOG = logging.getLogger('tests')


@pytest.fixture
def redis_uri():
	redis_uri = os.environ['REDICAL_REDIS_URI']
	return redis_uri


@pytest.fixture(scope='session')
def unused_port():
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
		s.bind(('127.0.0.1', 0))
		return s.getsockname()[1]


@pytest.fixture(scope='session')
def unix_socket():
	path = '/tmp/tests.sock'
	try:
		os.unlink(path)
	except OSError:
		if os.path.exists(path):
			raise
	with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
		s.bind('/tmp/tests.sock')
		return '/tmp/tests.sock'


@pytest.fixture
async def conn(redis_uri):
	conn = await create_connection(redis_uri)
	yield conn
	conn.close()
	await conn.wait_closed()


@pytest.fixture(params=['connection', 'pool'])
async def redical(request, redis_uri, conn):
	await conn.execute('flushdb')
	_redical = None
	if request.param == 'connection':
		LOG.info('Creating standalone Redical')
		_redical = await create_redical(redis_uri)
	else:
		LOG.info('Creating pool-based Redical')
		_redical = await create_redical_pool(redis_uri, max_size=4, min_size=2)
	yield _redical
	# this might look goofy but it allows tests to do silly things like attempt operations
	# while the connections are in a partially closed state and still have them cleaned
	# up properly
	if not _redical.is_closed and _redical.is_closing:
		await _redical.wait_closed()
		return
	if not _redical.is_closed:
		_redical.close()
		await _redical.wait_closed()
