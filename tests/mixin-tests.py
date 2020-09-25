from unittest import mock

import pytest

from redical import create_redical, create_redical_pool, Redical as _Redical


class FakeCommandMixin:
	def fake(self, key):
		return self.execute('fake', key, 'dostuff')


class Redical(FakeCommandMixin, _Redical):
	pass


@pytest.mark.asyncio
@mock.patch('redical.redical.create_connection')
@mock.patch('redical.redical.RedicalBase.execute')
async def test_mixin_attached_single(_execute, _create_connection, redis_uri):
	redical = await create_redical(redis_uri, redical_cls=Redical)
	redical.fake('mykey')
	_execute.assert_called_once_with('fake', 'mykey', 'dostuff')


@pytest.mark.asyncio
@mock.patch('redical.redical.create_pool')
@mock.patch('redical.redical.RedicalBase.execute')
async def test_mixin_attached_pool(_execute, _create_pool, redis_uri):
	redical = await create_redical_pool(redis_uri, redical_cls=Redical)
	redical.fake('mykey')
	_execute.assert_called_once_with('fake', 'mykey', 'dostuff')


@pytest.mark.asyncio
@mock.patch('redical.redical.create_connection')
@mock.patch('redical.redical.RedicalBase.execute')
async def test_mixin_attached_single_pipeline(_execute, _create_connection, redis_uri):
	redical = await create_redical(redis_uri, redical_cls=Redical)
	async with redical as pipe:
		pipe.fake('mykey')
	_execute.assert_called_once_with('fake', 'mykey', 'dostuff')


@pytest.mark.asyncio
@mock.patch('redical.redical.create_pool')
@mock.patch('redical.redical.RedicalBase.execute')
async def test_mixin_attached_pool_pipeline(_execute, _create_pool, redis_uri):
	redical = await create_redical_pool(redis_uri, redical_cls=Redical)
	async with redical as pipe:
		pipe.fake('mykey')
	_execute.assert_called_once_with('fake', 'mykey', 'dostuff')
