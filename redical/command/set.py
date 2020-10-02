from typing import Any, AnyStr, Awaitable, Set, Sequence

from ..mixin import Executable


def _smembers_convert_to_set(response: Sequence[Any]) -> Set[Any]:
	return set(response)


class SetCommandsMixin:
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
	def sadd(self: Executable, key: str, *members: Any, **kwargs: Any) -> Awaitable[int]:
		"""
		Add the specified members to the set stored at `key`. Specified members that are
		already a member of this set are ignored. If `key` does not exist a new set is
		created before adding the specified members.

		Args:
			key: Name of the key set is stored at.
			*members: Variable list of members to add.

		Returns:
			The number of members that were added to the set, not including all the members
				already present in the set.
		"""
		return self.execute('SADD', key, *members, conversion_func=int, **kwargs)

	def smembers(self: Executable, key: str, **kwargs: Any) -> Awaitable[Set[str]]:
		"""
		Returns all the members of the set value stored at `key`.

		Args:
			key: Name of the key set is stored at.

		Returns:
			All elements of the set.
		"""
		return self.execute('SMEMBERS', key, conversion_func=_smembers_convert_to_set, **kwargs)

	def srem(self: Executable, key: str, *members: AnyStr, **kwargs: Any) -> Awaitable[int]:
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
