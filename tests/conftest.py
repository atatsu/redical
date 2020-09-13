import os
import socket

import pytest  # type: ignore

from redical import create_connection


@pytest.fixture
def redis_uri():
	redis_uri = os.environ['REDICAL_REDIS_URI']
	return redis_uri


@pytest.fixture
async def conn():
	conn = await create_connection(redis_uri)
	return conn


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
