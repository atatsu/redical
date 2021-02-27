from dataclasses import dataclass
from typing import (
	overload,
	Any,
	AsyncIterator,
	Awaitable,
	Callable,
	List,
	Optional,
	Set,
	Sequence,
	TypeVar,
	Tuple,
	Union,
)

from ..mixin import Executable
from ..type import TransformFuncType
from ..util import collect_transforms

T = TypeVar('T')


@dataclass
class SscanResponse:
	cursor: str
	elements: Set[str]


def _smembers_convert_to_set(response: Sequence[Any]) -> Set[Any]:
	return set(response)


def _sscan_convert_to_results(response: Tuple[str, Sequence[str]]) -> SscanResponse:
	return SscanResponse(cursor=response[0], elements=set(response[1]))


class SetCommandsMixin:
	"""
	Implemented commands:
		* sismember
		* smembers
		* srem
		* sscan

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
	"""
	@overload
	def sadd(self: Executable, key: str, *members: Any, transform: None = None, **kwargs: Any) -> Awaitable[int]:
		...
	@overload  # noqa: E301
	def sadd(self: Executable, key: str, *members: Any, transform: Callable[[int], T], **kwargs: Any) -> Awaitable[T]:
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
		transforms: List[TransformFuncType]
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
		transforms: List[TransformFuncType]
		transforms, kwargs = collect_transforms(bool, kwargs)
		return self.execute('SISMEMBER', key, member, transform=transforms, **kwargs)

	@overload
	def smembers(self: Executable, key: str, /, transform: None = None, **kwargs: Any) -> Awaitable[Set[str]]:
		...
	@overload  # noqa: E301
	def smembers(self: Executable, key: str, /, transform: Callable[[Set[str]], T], **kwargs: Any) -> Awaitable[T]:
		...
	def smembers(self, key, /, **kwargs):  # noqa: E301
		"""
		Returns all the members of the set value stored at `key`.

		Args:
			key: Name of the key set is stored at.

		Returns:
			All elements of the set.
		"""
		transforms: List[TransformFuncType]
		transforms, kwargs = collect_transforms(_smembers_convert_to_set, kwargs)
		return self.execute('SMEMBERS', key, transform=transforms, **kwargs)

	def srem(self: Executable, key: str, *members: Any, **kwargs: Any) -> Awaitable[int]:
		"""
		Remove the specified members from the set stored at `key`.

		Args:
			key: Name of the key set is stored at.
			*members: Variable list of members to remove.

		Returns:
			The number of members that were removed from the set, not including
				non-existing members.
		"""
		return self.execute('SREM', key, *members, **kwargs)

	def sscan(
		self: Executable,
		key: str,
		cursor: Union[int, str],
		*,
		match: Optional[str] = None,
		count: Optional[int] = None,
		**kwargs: Any
	) -> Awaitable[SscanResponse]:
		"""
		Incrementally iterate over the elements of the set stored at `key`.

		Args:
			key: Name of the key set is stored at.
			cursor:
			match: Return only those elements that match the supplied glob-style pattern.
			count: Number of elements that should be returned with each call to the server.
				Please note that this is **just a hint** to the server, but is generally
				what can be epxected most times.

		Returns:
			A `SscanResponse` instance which contains the cursor returned by the server as well
			as a `set` of elements.
		"""
		elements: List[str]
		match_args: List[str] = []
		count_args: List[str] = []
		if match is not None:
			match_args.extend([
				'MATCH',
				str(match),
			])
		if count is not None:
			count_args.extend([
				'COUNT',
				str(count),
			])
		transforms: List[TransformFuncType]
		transforms, kwargs = collect_transforms(_sscan_convert_to_results, kwargs)
		return self.execute('SSCAN', key, cursor, *match_args, *count_args, transform=transforms, **kwargs)

	async def sscan_iter(
		self, key: str, *, match: Optional[str] = None, count: Optional[int] = None, **kwargs: Any
	) -> AsyncIterator[str]:
		"""
		Like `sscan` but instead iterates over the entire set until the iterator is exhausted.
		The cursor returned by the server is managed internally and additional `SSCAN` calls
		are made until there are no elements left to return.

		Note: This method is **not** suitable for pipeline or transaction use.

		Args:
			key: Name of the key set is stored at.
			match: Return only those elements that match the supplied glob-style pattern.
			count: Number of elements that should be returned with each call to the server.
				Please note that this is **just a hint** to the server, but is generally
				what can be epxected most times.

		Returns:
			An asynchronous iterator that exhausts all elements.
		"""
		cursor: Optional[str] = None
		while cursor != "0":
			response: SscanResponse = await self.sscan(key, cursor or 0, match=match, count=count, **kwargs)  # type: ignore
			cursor = response.cursor
			for element in response.elements:
				yield element
