from functools import partial
from typing import overload, Any, AnyStr, Awaitable, Callable, List, Optional, TypeVar, Union

from ..exception import InvalidKeyError
from ..mixin import Executable
from ..type import TransformFuncType
from ..util import collect_transforms

T = TypeVar('T')


def _set_convert_to_bool(response: Optional[str]) -> bool:
	return bool(response)


def _get_error_wrapper(response: Optional[Any], *, key: str) -> Any:
	if response is None:
		raise InvalidKeyError(key)
	return response


class StringCommandsMixin:
	"""
	Implemented commands:
		* get
		* incr
		* set [psetex, setex]

	TODO:
		* append
		* bitcount
		* bitfield
		* bitop
		* bitpos
		* decr
		* decrby
		* getbit
		* getrange
		* getset
		* incrby
		* incrbyfloat
		* mget
		* mset
		* msetnx
		* setbit
		* setnx
		* setrange
		* stralgo
		* strlen
	"""
	# FIXME: This should *not* throw an exception if the key does not exist. It should instead
	#        return `None`
	@overload
	def get(self: Executable, key: str, /, transform: None = None, **kwargs: Any) -> Awaitable[str]:
		...
	@overload  # noqa: E301
	def get(self: Executable, key: str, /, transform: Callable[[str], T], **kwargs: Any) -> Awaitable[T]:
		...
	def get(self, key, /, **kwargs):  # noqa: E301
		"""
		Retrieve the value of a key.

		Args:
			key: Name of the key whose data to fetch.

		Returns:
			The string stored at `key`.

		Raises:
			InvalidKeyError: If the supplied `key` does not exist.
		"""
		transforms: List[TransformFuncType]
		transforms, kwargs = collect_transforms(partial(_get_error_wrapper, key=key), kwargs)
		return self.execute('GET', key, transform=transforms, **kwargs)

	def incr(self: Executable, key: str, /, **kwargs: Any) -> Awaitable[int]:
		"""
		Increments the number stored at `key` by one.

		Args:
			key: Name of the key to increment.

		Returns:
			The value of `key` after the increment.
		"""
		return self.execute('INCR', key, **kwargs)

	# FIXME: `only_if_exists` and `only_if_not_exists` should be replaced with `const.UpdatePolicy`
	# TODO: Add support for `GET` option
	# TODO: Add support for `EXAT` option
	# TODO: Add support for `PXAT` option
	@overload
	def set(
		self: Executable,
		key: str,
		/,
		value: AnyStr,
		*,
		expire_in_seconds: Optional[Union[int, float]] = None,
		expire_in_milliseconds: Optional[int] = None,
		only_if_exists: bool = False,
		only_if_not_exists: bool = False,
		keep_ttl: bool = False,
		transform: None = None,
		**kwargs: Any
	) -> Awaitable[bool]:
		...
	@overload  # noqa: E301
	def set(
		self: Executable,
		key: str,
		/,
		value: AnyStr,
		*,
		expire_in_seconds: Optional[Union[int, float]] = None,
		expire_in_milliseconds: Optional[int] = None,
		only_if_exists: bool = False,
		only_if_not_exists: bool = False,
		keep_ttl: bool = False,
		transform: Callable[[bool], T],
		**kwargs: Any
	) -> Awaitable[T]:
		...
	def set(  # noqa: E301
		self, key, /, value, *,
		expire_in_seconds=None, expire_in_milliseconds=None, only_if_exists=False, only_if_not_exists=False,
		keep_ttl=False, **kwargs
	):
		"""
		Set `key` to hold the string `value`. If `key` already holds a value it is overwritten,
		regardless of its type. Any previous time to live associated with the key is discarded.

		Args:
			key: Name of the key to set.
			value: Value to set `key` to.
			expire_in_seconds: Instruct `key` to expire in the specified seconds. If a float value
				is used it will be multiplied by 1000, coerced to an int, and be applied as milliseconds.
				Defaults to None.
			expire_in_milliseconds: Instruct `key` to expire in the specified milliseconds. Defaults to None.
			only_if_exists: Only set the `key` if it already exists. Defaults to `False`.
			only_if_not_exists: Only set the `key` if it does not already exist. Defaults to `False`.
			keep_ttl: When setting `key`, retain the time to live associated with it if it was
				previously set to expire. Defaults to False.

		Returns:
			True if the `key` with the specified `value` was set, False otherwise

		Raises:
			ValueError: If both `only_if_exists` and `only_if_not_exists` are True.
			ValueError: If both `expire_in_seconds` and `expire_in_milliseconds` are non-None values.
		"""
		if all([only_if_exists, only_if_not_exists]):
			raise ValueError(
				'Expected only one of `only_if_exists` and `only_if_not_exists` to be `True`, but both were'
			)
		if all([expire_in_seconds, expire_in_milliseconds]):
			raise ValueError(
				'Expected only one of `expire_in_seconds` and `expire_in_milliseconds` to be set, but both were'
			)
		additional_args: List[Any] = []
		if bool(only_if_exists):
			additional_args.append('XX')
		if bool(only_if_not_exists):
			additional_args.append('NX')
		if expire_in_seconds is not None and isinstance(expire_in_seconds, int):
			additional_args.extend(['EX', int(expire_in_seconds)])
		elif expire_in_seconds is not None and isinstance(expire_in_seconds, float):
			additional_args.extend(['PX', int(expire_in_seconds * 1000)])
		if expire_in_milliseconds is not None:
			additional_args.extend(['PX', int(expire_in_milliseconds)])
		if bool(keep_ttl):
			additional_args.append('KEEPTTL')

		transforms: List[TransformFuncType]
		transforms, kwargs = collect_transforms(_set_convert_to_bool, kwargs)
		return self.execute('SET', key, value, *additional_args, transform=transforms, **kwargs)
