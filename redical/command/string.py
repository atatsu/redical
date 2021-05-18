from typing import overload, Any, AnyStr, Awaitable, Callable, List, Optional, TypeVar, Union

from ..mixin import Executable
from ..type import TransformFuncType
from ..util import collect_transforms

T = TypeVar('T')


def _set_convert_to_bool(response: Optional[str]) -> bool:
	return bool(response)


def _get_error_wrapper(exc: Exception) -> Exception:
	if str(exc).startswith('WRONGTYPE'):
		return TypeError(str(exc).replace('WRONGTYPE ', ''))
	return exc


def _incr_error_wrapper(exc: Exception) -> Exception:
	exc = _get_error_wrapper(exc)
	if str(exc).startswith('ERR value'):
		return ValueError(str(exc).replace('ERR ', ''))
	return exc


_incrby_error_wrapper = _incr_error_wrapper


class StringCommandsMixin:
	"""
	Implemented commands:
		* get
		* incr
		* incrby
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
	@overload
	def get(
		self: Executable, key: str, /, *, transform: None = None, encoding: Optional[str] = 'utf-8'
	) -> Awaitable[Optional[str]]:
		...
	@overload  # noqa: E301
	def get(
		self: Executable, key: str, /, *, transform: Callable[[Optional[str]], T], encoding: Optional[str] = 'utf-8'
	) -> Awaitable[T]:
		...
	def get(self, key, /, **kwargs):  # noqa: E301
		"""
		Retrieve the value of a key.

		Args:
			key: Name of the key whose data to fetch.

		Returns:
			The string stored at `key`.

		Raises:
			TypeError: If the supplied `key` exists and is not a string.
		"""
		return self.execute('GET', key, error_func=_get_error_wrapper, **kwargs)

	@overload
	def incr(
		self: Executable, key: str, /, *, transform: None = None, encoding: Optional[str] = 'utf-8'
	) -> Awaitable[int]:
		...
	@overload  # noqa: E301
	def incr(
		self: Executable, key: str, /, *, transform: Callable[[int], T], encoding: Optional[str] = 'utf-8'
	) -> Awaitable[T]:
		...
	def incr(self: Executable, key, /, **kwargs):  # noqa: E301
		"""
		Increments the number stored at `key` by one.

		Args:
			key: Name of the key to increment.

		Returns:
			The value of `key` after the increment.

		Raises:
			TypeError: If the supplied `key` exists and is not a string.
			ValueError: If the supplied `key` exists but contains a value that cannot be represented
				as an integer.
		"""
		return self.execute('INCR', key, error_func=_incr_error_wrapper, **kwargs)

	@overload
	def incrby(
		self: Executable, key: str, /, increment: int, *, transform: None = None, encoding: Optional[str]
	) -> Awaitable[int]:
		...
	@overload  # noqa: E301
	def incrby(
		self: Executable, key: str, /, increment: int, *, transform: Callable[[int], T], encoding: Optional[str]
	) -> Awaitable[T]:
		...
	def incrby(self: Executable, key, /, increment, **kwargs):  # noqa: E301
		"""
		Increments the number stored at `key` by `increment`. If the key does not
		exist it is set to `0` before performing the operation. This operation is
		limited to 64 bit signed integers. It is possible to provide a negative
		value to decrement the score.

		Args:
			key: Name of the key to increment.
			increment: Value by which to increment (or decrement) the value by.

		Returns:
			The new value of `key` after the increment (or decrement).

		Raises:
			TypeError: If the supplied `key` exists and is not a string.
			ValueError: If the supplied `key` exists but contains a value that cannot be represented
				as an integer.
		"""
		return self.execute('INCRBY', key, increment, error_func=_incrby_error_wrapper, **kwargs)

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
		encoding: Optional[str] = 'utf-8'
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
		encoding: Optional[str] = 'utf-8'
	) -> Awaitable[T]:
		...
	def set(  # noqa: E301
		self: Executable, key, /, value, *,
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
