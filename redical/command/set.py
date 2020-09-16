from typing import Any, AnyStr, Awaitable, Set, Sequence

from .base import BaseMixin


def _smembers_convert_to_set(response: Sequence[Any]) -> Set[Any]:
	return set(response)


class SetCommandsMixin(BaseMixin):
	"""
	Implemented commands:
		* smembers
		* srem

	TODO:
		* sadd
		* scard
		* sdiff
		* sdiffstore
		* sinter
		* sinterstore
		* sismember
		* smove
		* spop
		* srandmember
		* sunion
		* sunionstore
		* sscan
	"""
	def smembers(self, key: str, **kwargs: Any) -> Awaitable[Set[Any]]:
		"""
		Returns all the members of the set value stored at `key`.

		Args:
			key: Name of the key set is stored at.

		Returns:
			All elements of the set.
		"""
		return self.execute('SMEMBERS', key, conversion_func=_smembers_convert_to_set, **kwargs)

	def srem(self, key: str, *members: AnyStr, **kwargs: Any) -> Awaitable[int]:
		"""
		Remove the specified members from teh set stored at `key`.

		Args:
			key: Name of the key set is stored at.
			*members: Variable list of members to remove.

		Returns:
			The number of members that were removed from the set, not including
				non-existing members.
		"""
		return self.execute('SREM', key, *members, **kwargs)
