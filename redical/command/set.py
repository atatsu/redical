from typing import Any, AnyStr, Awaitable, List, Set

from .base import BaseMixin


async def _smembers_convert_to_set(coro: Awaitable[List[Any]]) -> Set[Any]:
	return set(await coro)


class SetCommandsMixin(BaseMixin):
	def smembers(self, key: str) -> Awaitable[Set[Any]]:
		"""
		Returns all the members of the set value stored at `key`.

		Args:
			key: Name of the key set is stored at.

		Returns:
			All elements of the set.
		"""
		return _smembers_convert_to_set(self.execute('SMEMBERS', key))

	def srem(self, key: str, *members: AnyStr) -> Awaitable[int]:
		"""
		Remove the specified members from teh set stored at `key`.

		Args:
			key: Name of the key set is stored at.
			*members: Variable list of members to remove.

		Returns:
			The number of members that were removed from the set, not including
				non-existing members.
		"""
		return self.execute('SREM', key, *members)
