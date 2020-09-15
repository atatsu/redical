from typing import Awaitable

from .base import BaseMixin
from ..exception import InvalidKeyError, NoExpiryError


async def _ttl_error_wrapper(key: str, coro: Awaitable[int]) -> int:
	result: int = await coro
	if result == -2:
		raise InvalidKeyError(key, message=f'Key with name {key!r} does not exist')
	elif result == -1:
		raise NoExpiryError(key, message=f'Key with name {key!r} has no expiry set')
	return result
_pttl_error_wrapper = _ttl_error_wrapper


class KeyCommandsMixin(BaseMixin):
	def exists(self, *keys: str) -> Awaitable[int]:
		"""
		Return a count for the number of supplied keys that exist.

		Args:
			*keys: Variable length key list to check the existence of.

		Returns:
			The number of keys existing among those supplied as arguments.

		Note: If the same existing key is supplied multiple times it will be counted
			multiple times. So if `somekey` exists, `Redical.exists('somekey', 'somekey')`
			will return `2`.
		"""
		return self.execute('EXISTS', *keys)

	def pttl(self, key: str) -> Awaitable[int]:
		"""
		Returns the remaining time to live of a key that has been set to expire, in milliseconds.

		Args:
			key: Name of the key to check the expiry of.

		Returns:
			Number of milliseconds left until `key` expires.

		Raises:
			InvalidKeyError: If the supplied `key` does not exist.
			NoExpiryError: If the supplied `key` exists but has no associated expiry.
		"""
		return _pttl_error_wrapper(key, self.execute('PTTL', key))

	def ttl(self, key: str) -> Awaitable[int]:
		"""
		Returns the remaining time to live of a key that has been set to expire, in seconds.

		Args:
			key: Name of the key to check the expiry of.

		Returns:
			Number of seconds left until `key` expires.

		Raises:
			InvalidKeyError: If the supplied `key` does not exist.
			NoExpiryError: If the supplied `key` exists but has no associated expiry.
		"""
		return _ttl_error_wrapper(key, self.execute('TTL', key))
