from typing import Any, Awaitable, List

from ..mixin import Executable


def _hset_error_wrapper(exc: Exception) -> Exception:
	if str(exc).startswith('WRONGTYPE'):
		return TypeError(str(exc).replace('WRONGTYPE ', ''))
	return exc


class HashCommandsMixin:
	"""
	Implemented commands:
		* hset

	TODO:
		* hdel
		* hexists
		* hget
		* hgetall
		* hincrby
		* hincrbyfloat
		* hkeys
		* hlen
		* hmget
		* hsetnx
		* hstrlen
		* hvals
		* hscan
	"""
	def hset(
		self: Executable, key: str, /, field: str, value: Any, *field_value_pairs: Any, **kwargs: Any
	) -> Awaitable[int]:
		"""
		Sets `field` in the hash stored at `key` to `value`. If `key` does not exist, a new key
		holding a hash is created. If `field` already exists in the hash it is overwritten.

		Args:
			key: Name of the hash key to set values in.
			field: Name of hash field to set.
			value: Value to set `field` to.
			*field_value_pairs: A variable length list of field/value pairs to set in the hash.

				Example:
					*[('field1', 'value1'), ('field2', 'value2'), ('field3', 'value3')]
					or
					*dict(field1='value1', field2='value2', field3='value3').items()

		Returns:
			The number of fields that were added.

		Raises:
			TypeError: If the supplied `key` doesn't contain a hash.
			ValueError: Wrong number of arguments for `field_value_pairs` (unequal field count
				versus value count).
		"""
		command: List[Any] = ['HSET', key]
		x: Any
		y: Any
		flattened: List[Any] = [field, value]
		flattened.extend([y for x in field_value_pairs for y in x])
		if len(flattened) % 2 != 0:
			raise ValueError('Number of supplied fields does not match the number of supplied values')
		command.extend(flattened)
		return self.execute(*command, error_func=_hset_error_wrapper)
