from functools import partial
from typing import Any, Awaitable

from ..exception import InvalidKeyError, NoExpiryError
from ..mixin import Executable


def _ttl_error_wrapper(response: int, *, key: str) -> int:
	if response == -2:
		raise InvalidKeyError(key)
	elif response == -1:
		raise NoExpiryError(key)
	return response


_pttl_error_wrapper = _ttl_error_wrapper


class KeyCommandsMixin:
	"""
	Implemented commands:
		* exists
		* pttl
		* ttl

	TODO:
		* del
		* dump
		* expire
		* expireat
		* keys
		* migrate
		* move
		* object
		* persist
		* pexpire
		* pexpireat
		* randomkey
		* rename
		* renamenx
		* restore
		* sort
		* touch
		* type
		* unlink
		* wait
		* scan
	"""
	def exists(self: Executable, *keys: str, **kwargs: Any) -> Awaitable[int]:
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
		return self.execute('EXISTS', *keys, **kwargs)

	def pttl(self: Executable, key: str, **kwargs: Any) -> Awaitable[int]:
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
		return self.execute('PTTL', key, conversion_func=partial(_pttl_error_wrapper, key=key), **kwargs)

	def ttl(self: Executable, key: str, **kwargs: Any) -> Awaitable[int]:
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
		return self.execute('TTL', key, conversion_func=partial(_ttl_error_wrapper, key=key), **kwargs)
