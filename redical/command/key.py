from functools import partial
from typing import overload, Any, Awaitable, Callable, List, TypeVar

from ..exception import InvalidKeyError, NoExpiryError
from ..mixin import Executable
from ..type import TransformFuncType
from ..util import collect_transforms

T = TypeVar('T')


def _expire_convert_to_bool(response: int) -> bool:
	return bool(response)


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
		* del
		* exists
		* expire
		* pttl
		* ttl

	TODO:
		* dump
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
	def delete(self: Executable, key: str, *keys: str, **kwargs: Any) -> Awaitable[int]:
		"""
		Remove the specified keys.

		Returns:
			Number of keys removed.
		"""
		return self.execute('DEL', key, *keys, **kwargs)

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

	@overload
	def expire(self: Executable, key: str, /, timeout: int, transform: None = None, **kwargs: Any) -> Awaitable[bool]:
		...
	@overload  # noqa: E301
	def expire(
		self: Executable, key: str, /, timeout: int, transform: Callable[[bool], T], **kwargs: Any
	) -> Awaitable[T]:
		...
	def expire(self, key, /, timeout, **kwargs):  # noqa: E301
		"""
		Set a timeout on `key` in seconds.

		Args:
			key: Name of the key to set the timeout on.
			timeout: Number of seconds in which to expire `key`.

		Returns:
			True if the timeout was set, False if `key` does not exist.
		"""
		transforms: List[TransformFuncType]
		transforms, kwargs = collect_transforms(_expire_convert_to_bool, kwargs)
		return self.execute('EXPIRE', key, int(timeout), transform=transforms, **kwargs)

	@overload
	def pttl(self: Executable, key: str, /, transform: None = None, **kwargs: Any) -> Awaitable[int]:
		...
	@overload  # noqa: E301
	def pttl(self: Executable, key: str, /, transform: Callable[[int], T], **kwargs: Any) -> Awaitable[T]:
		...
	def pttl(self, key, /, **kwargs):  # noqa: E301
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
		transforms: List[TransformFuncType]
		transforms, kwargs = collect_transforms(partial(_pttl_error_wrapper, key=key), kwargs)
		return self.execute('PTTL', key, transform=transforms, **kwargs)

	@overload
	def ttl(self: Executable, key: str, /, transform: None = None, **kwargs: Any) -> Awaitable[int]:
		...
	@overload  # noqa: E301
	def ttl(self: Executable, key: str, /, transform: Callable[[int], T], **kwargs: Any) -> Awaitable[T]:
		...
	def ttl(self, key, /, **kwargs):  # noqa: E301
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
		transforms: List[TransformFuncType]
		transforms, kwargs = collect_transforms(partial(_ttl_error_wrapper, key=key), kwargs)
		return self.execute('TTL', key, transform=transforms, **kwargs)
