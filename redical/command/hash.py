from functools import partial
from typing import Any, Awaitable, Dict, List, Sequence

from ..mixin import Executable
from ..util import undefined


def _hset_error_wrapper(exc: Exception) -> Exception:
	if str(exc).startswith('WRONGTYPE'):
		return TypeError(str(exc).replace('WRONGTYPE ', ''))
	return exc


_hdel_error_wrapper = _hset_error_wrapper
_hget_error_wrapper = _hset_error_wrapper
_hmget_error_wrapper = _hset_error_wrapper
_hgetall_error_wrapper = _hset_error_wrapper


def _hmget_convert_to_dict(response: Sequence[str], *, fields: Sequence[str]) -> Dict[str, Any]:
	return dict(zip(fields, response))


def _hgetall_convert_to_dict(response: Sequence[str]) -> Dict[str, Any]:
	x: int
	fields: List[str] = [response[x] for x in range(0, len(response), 2)]
	values: List[str] = [response[x] for x in range(1, len(response), 2)]
	return dict(zip(fields, values))


class HashCommandsMixin:
	"""
	Implemented commands:
		* hdel
		* hget
		* hgetall
		* hmget
		* hset

	TODO:
		* hexists
		* hincrby
		* hincrbyfloat
		* hkeys
		* hlen
		* hsetnx
		* hstrlen
		* hvals
		* hscan
	"""
	def hdel(self: Executable, key: str, *fields: str, **kwargs: Any) -> Awaitable[int]:
		"""
		Removes the specified fields from the hash stored at `key`. Specified fields
		that do not exist within the hash are ignored. If `key` does not exist it is
		treated as an empty hash and this method returns `0`.

		Args:
			key: Name of the hash key to delete fields from.
			*fields: A variable length list of field names to delete.

		Returns: The number of fields that were removed from the hash, not including
			specified but non-existing fields.

		Raises:
			TypeError: If the supplied `key` doesn't contain a hash.
		"""
		return self.execute('HDEL', key, *fields, error_func=_hdel_error_wrapper, **kwargs)

	def hget(self: Executable, key: str, field: str, **kwargs: Any) -> Awaitable[Any]:
		"""
		Returns the value associated with `field` in the hash stored at `key`.

		Args:
			key: Name of the hash key to retrieve field value from.
			field: Name of the field to retrieve value.

		Returns: The value associated with `field`, or `None` when field is not
			present in the hash or `key` does not exist.

		Raises:
			TypeError: If the supplied `key` doesn't contain a hash.
		"""
		return self.execute('HGET', key, field, error_func=_hget_error_wrapper, **kwargs)

	def hgetall(self: Executable, key: str, **kwargs: Any) -> Awaitable[Dict[str, Any]]:
		"""
		Returns all fields and values of the hash stored at `key`.

		Args:
			key: Name of the hash key to retrieve all fields and values from.

		Returns:
			A dictionary of all fields and values contained in the hash.

		Raises:
			TypeError: If the supplied `key` doesn't contain a hash.
		"""
		return self.execute(
			'HGETALL', key, conversion_func=_hgetall_convert_to_dict, error_func=_hgetall_error_wrapper, **kwargs
		)

	def hmget(self: Executable, key: str, /, *fields: str, **kwargs: Any) -> Awaitable[Dict[str, Any]]:
		"""
		Returns the values associated with the specified `fields` in the hash stored at `key`.
		For every field that does not exist in the hash a `None` value is returned.

		Note: Non-existing keys are treated as empty hashes.

		Args:
			key: Name of the hash key to retrieve values from.
			*fields: A variable length list of field names whose values to retrieve.

		Returns:
			A dictionary containing the fields requested and values associated with them.

		Raises:
			TypeError: If the supplied `key` doesn't contain a hash.
		"""
		return self.execute(
			'HMGET',
			key,
			*fields,
			conversion_func=partial(_hmget_convert_to_dict, fields=fields),
			error_func=_hmget_error_wrapper,
			**kwargs
		)

	def hset(
		self: Executable, key: str, /, *field_value_pairs: Any, **kwargs: Any
	) -> Awaitable[int]:
		"""
		Sets `field` in the hash stored at `key` to `value`. If `key` does not exist, a new key
		holding a hash is created. If `field` already exists in the hash it is overwritten.

		Args:
			key: Name of the hash key to set values in.
			*field_value_pairs: A variable length list of field/value pairs to set in the hash.

				Example:
					*[('field1', 'value1'), ('field2', 'value2'), ('field3', 'value3')]
					or
					*dict(field1='value1', field2='value2', field3='value3').items()
			**kwargs: Field/value pairs in keyword argument form.

		Returns:
			The number of fields that were added.

		Raises:
			TypeError: If the supplied `key` doesn't contain a hash.
			ValueError: Wrong number of arguments for `field_value_pairs` (unequal field count
				versus value count).
		"""
		conversion_func: Any = kwargs.pop('conversion_func', None)
		encoding: Any = kwargs.pop('encoding', undefined)

		command: List[Any] = ['HSET', key]
		x: Any
		y: Any
		flattened: List[Any] = [y for x in field_value_pairs for y in x]
		flattened.extend([y for x in kwargs.items() for y in x])
		if len(flattened) % 2 != 0:
			raise ValueError('Number of supplied fields does not match the number of supplied values')
		command.extend(flattened)
		return self.execute(
			*command, conversion_func=conversion_func, encoding=encoding, error_func=_hset_error_wrapper
		)
