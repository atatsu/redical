from typing import overload, Any, AnyStr, Awaitable, Callable, List, Set, Sequence, TypeVar

from ..abstract import TransformFunc
from ..mixin import Executable
from ..util import collect_transforms

T = TypeVar('T')


def _smembers_convert_to_set(response: Sequence[Any]) -> Set[Any]:
	return set(response)


class SetCommandsMixin:
	"""
	Implemented commands:
		* sismember
		* smembers
		* srem

	TODO:
		* sadd
		* scard
		* sdiff
		* sdiffstore
		* sinter
		* sinterstore
		* smove
		* spop
		* srandmember
		* sunion
		* sunionstore
		* sscan
	"""
	@overload
	def sadd(self: Executable, key: str, *members: Any, transform: None = None, **kwargs: Any) -> Awaitable[int]:
		...
	@overload  # noqa: E301
	def sadd(self: Executable, key: str, *members: Any, transform: Callable[[int], T]) -> Awaitable[T]:
		...
	def sadd(self, key, *members, **kwargs):  # noqa: E301
		"""
		Add the specified members to the set stored at `key`. Specified members that are
		already a member of this set are ignored. If `key` does not exist a new set is
		created before adding the specified members.

		Args:
			key: Name of the key set is stored at.
			*members: Variable list of items to add.

		Returns:
			The number of members that were added to the set, not including all the members
				already present in the set.
		"""
		transforms: List[TransformFunc]
		transforms, kwargs = collect_transforms(int, kwargs)
		return self.execute('SADD', key, *members, transform=transforms, **kwargs)

	@overload
	def sismember(
		self: Executable, key: str, /, member: Any, transform: None = None, **kwargs: Any
	) -> Awaitable[bool]:
		...
	@overload  # noqa: E301
	def sismember(
		self: Executable, key: str, /, member: Any, transform: Callable[[bool], T], **kwargs: Any
	) -> Awaitable[T]:
		...
	def sismember(self, key, /, member, **kwargs):  # noqa: E301
		"""
		Check whether a specified item beongs to a set.

		Args:
			key: Name of the key set is stored at.
			member: Item to check set membership of.

		Returns:
			True if `member` is in the set, False otherwise.
		"""
		transforms: List[TransformFunc]
		transforms, kwargs = collect_transforms(bool, kwargs)
		return self.execute('SISMEMBER', key, member, transform=transforms, **kwargs)

	@overload
	def smembers(self: Executable, key: str, /, transform: None = None, **kwargs: Any) -> Awaitable[Set[str]]:
		...
	@overload  # noqa: E301
	def smembers(self: Executable, key: str, /, transform: Callable[[Set[str]], T]) -> Awaitable[T]:
		...
	def smembers(self, key, /, **kwargs):  # noqa: E301
		"""
		Returns all the members of the set value stored at `key`.

		Args:
			key: Name of the key set is stored at.

		Returns:
			All elements of the set.
		"""
		transforms: List[TransformFunc]
		transforms, kwargs = collect_transforms(_smembers_convert_to_set, kwargs)
		return self.execute('SMEMBERS', key, transform=transforms, **kwargs)

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
