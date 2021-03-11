from enum import Enum
from functools import partial
from typing import (
	overload,
	Any,
	Awaitable,
	Callable,
	List,
	Literal,
	NamedTuple,
	Optional,
	TypeVar,
	Tuple,
	Union,
	TYPE_CHECKING,
)

from ..const import UpdatePolicy
from ..util import collect_transforms

if TYPE_CHECKING:
	from ..mixin import Executable

T = TypeVar('T')


def _zadd_error_wrapper(exc: Exception) -> Exception:
	if str(exc).startswith('WRONGTYPE'):
		return TypeError(str(exc).replace('WRONGTYPE ', ''))
	return exc


_zcard_error_wrapper = _zadd_error_wrapper
_zrange_index_error_wrapper = _zadd_error_wrapper
_zscore_error_wrapper = _zadd_error_wrapper
_zrem_error_wrapper = _zadd_error_wrapper


class ElementScore(NamedTuple):
	element: str
	score: float


def _zrange_index_convert_to_tuple(
	response: List[str], *, with_scores: bool
) -> Union[Tuple[str, ...], Tuple[ElementScore, ...]]:
	if not with_scores:
		return tuple(response)
	return tuple([
		ElementScore(response[x], float(response[x + 1])) for x in range(0, len(response), 2)
	])


def _zscore_convert_to_float(score: Optional[str]) -> Optional[float]:
	if score is None:
		return None
	return float(score)


class ScorePolicy(str, Enum):
	GREATER_THAN = 'GT'
	LESS_THAN = 'LT'

	def __str__(self) -> str:
		return str(self.value)


ScoreType = Union[float, Literal['+inf', '-inf']]


# class ZRangeMode(Enum):
#   INDEX = auto()
#   SCORE = auto()
#   LEXICOGRAPHICAL = auto()


class SortedSetCommandsMixin:
	"""
	Implemented commands:
		* zadd
		* zcard
		* zrange[_index]
		* zrem
		* zscore

	TODO:
		* zcount
		* zdiff
		* zdiffstore
		* zincrby
		* zinter
		* zinterstore
		* zlexcount
		* zmscore
		* zpopmax
		* zpopmin
		* zrandmember
		* zrange[_score, _lex]
		* zrangestore
		* zrank
		* zremrangebylex
		* zremrangebyrank
		* zremrangebyscore
		* zrevrank
		* zscan
		* zunion
		* zunionstore
	"""
	@overload
	def zadd(
		self: 'Executable',
		key: str,
		*member_score_pairs: Tuple[Any, ScoreType],
		changed: bool = False,
		increment: bool = False,
		score_policy: Optional[ScorePolicy] = None,
		update_policy: Optional[UpdatePolicy] = None,
		encoding: Optional[str] = 'utf-8',
		transform: None = None,
		**members_scores: ScoreType
	) -> Awaitable[int]:
		...
	@overload  # noqa: E301
	def zadd(
		self: 'Executable',
		key: str,
		*member_score_pairs: Tuple[Any, ScoreType],
		changed: bool = False,
		increment: bool = False,
		score_policy: Optional[ScorePolicy] = None,
		update_policy: Optional[UpdatePolicy] = None,
		encoding: Optional[str] = 'utf-8',
		transform: Callable[[int], T],
		**members_scores: ScoreType
	) -> Awaitable[T]:
		...
	def zadd(  # noqa: E301
		self, key, *member_score_pairs, changed=False, increment=False, score_policy=None, update_policy=None,
		encoding='utf-8', transform=None, **members_scores
	):
		"""
		Adds all the specified members with the specified scores to the sorted set stored at `key`.
		Multiple score/member pairs can be supplied. If a specified member is already a member
		of the sorted set the score is updated and the element is reinserted at the right position
		to ensure the correct ordering.

		Args:
			key: Name of the key sorted set is stored at.
			member_score_pairs: A variable length list of member/score pairs in the form of:

					(<member>, <score>), (<member>, <score>), ...
			changed: Modify the return value from the number of elements added to the total number
				of elements changed. Changed elements are **new elements added** and elements already
				existing for which **the score was updated**.
			increment: Increment `member` by `score` in `zincrby` fashion.
				Note: Only one score/member pair can be used in this mode.
			update_policy: Controls the behavior for adding/updating elements.
				* `UpdatePolicy.EXISTS` - Only update elements that already exist -- never add elements.
				* `UpdatePolicy.NOT_EXISTS` - Don't update already existing elements -- always add new elements.
			score_policy: Controls the update behavior in regards to the score.
				* `ScorePolicy.GREATER_THAN` - Only update existing elements if the new score is **greater than**
					the current score. This flag doesn't prevent adding new elements.
				* `ScorePolicy.LESS_THAN` - Only update existing elements if the new score is **less than** the
					current score. This flag doesn't prevent adding new elements.
			members_scores: A mapping of members and their associated scores.

		Returns:
			The number of elements *added* when used without the `changed` option.
			The number of elements *changed* when used with the `changed` option.

		Raises:
			TypeError: If the supplied `key` exists and is not a sorted set.
			ValueError: If an uneven number of score/member pairs are supplied via `scores_members`.
			ValueError: If more than one score/member pair is supplied when using the `increment` option.
		"""
		pairs: List[Tuple[Any, ScoreType]] = list(member_score_pairs)
		pairs.extend(members_scores.items())
		flattened: List[Any] = [y for x in pairs for y in x]
		if len(flattened) % 2 != 0:
			raise ValueError('Uneven number of score/member pairs in `scores_members`')
		if increment and len(flattened) > 2:
			raise ValueError('`increment` supports a single score/member pair')
		options: List[str] = []
		if update_policy is not None:
			options.append(str(update_policy))
		if score_policy is not None:
			options.append(str(score_policy))
		if changed:
			options.append('CH')
		if increment:
			options.append('INCR')
		return self.execute(
			'ZADD',
			key,
			*options,
			*reversed(flattened),
			error_func=_zadd_error_wrapper,
			encoding=encoding,
			transform=transform,
		)

	@overload
	def zcard(
		self: 'Executable', key: str, *, transform: None = None, encoding: Optional[str] = 'utf-8'
	) -> Awaitable[int]:
		...
	@overload  # noqa: E301
	def zcard(
		self: 'Executable', key: str, *, transform: Callable[[int], T], encoding: Optional[str] = 'utf-8'
	) -> Awaitable[T]:
		...
	def zcard(self: 'Executable', key: str, **kwargs):  # noqa: E301
		"""
		Returns the cardinality (number of elements) of the sorted set stored at `key`.

		Args:
			key: Name of the key sorted set is stored at.

		Returns:
			The number of elements in the sorted set, or `0` if `key` does not exist.

		Raises:
			TypeError: If the supplied `key` exists and is not a sorted set.
		"""
		command: List[str] = ['ZCARD', key]
		return self.execute(*command, error_func=_zcard_error_wrapper, **kwargs)

	# def zrange(
	#   self: 'Executable',
	#   key: str,
	#   *,
	#   mode: ZRangeMode = ZRangeMode.INDEX,
	#   min: int,
	#   max: int
	# ) -> None:
	#   """
	#   Returns the specified range of elements in the sorted set stored at `key`. Range queries
	#   can be performed by index (default) - or rank, by score, or by lexicographical order. The
	#   default ordering of returned elements is from the lowest to highest score. Elements with
	#   the same score are ordered lexicographically.

	#   ## Index ranges
	#   The default mode. The `min` and `max` arguments represent zero-based indexes and are
	#   *inclusive*. For example, a `min` of `0` and a `max` of `1` will return both the
	#   first and second element of the sorted set.

	#   Args:

	#   Returns:

	#   Raises:
	#   """

	@overload
	def zrange_index(
		self: 'Executable',
		key: str,
		start: int,
		stop: int,
		*,
		with_scores: Literal[False] = ...,
		reversed: bool = False,
		encoding: Optional[str] = 'utf-8',
		transform: None = None,
	) -> Awaitable[Tuple[str, ...]]:
		...
	@overload  # noqa: E301
	def zrange_index(
		self: 'Executable',
		key: str,
		start: int,
		stop: int,
		*,
		with_scores: Literal[False] = ...,
		reversed: bool = False,
		encoding: Optional[str] = 'utf-8',
		transform: Callable[[Tuple[str, ...]], T],
	) -> Awaitable[T]:
		...
	@overload  # noqa: E301
	def zrange_index(
		self: 'Executable',
		key: str,
		start: int,
		stop: int,
		*,
		with_scores: Literal[True],
		reversed: bool = False,
		encoding: Optional[str] = 'utf-8',
		transform: None = None,
	) -> Awaitable[Tuple[ElementScore, ...]]:
		...
	@overload  # noqa: E301
	def zrange_index(
		self: 'Executable',
		key: str,
		start: int,
		stop: int,
		*,
		with_scores: Literal[True],
		reversed: bool = False,
		encoding: Optional[str] = 'utf-8',
		transform: Callable[[Tuple[ElementScore, ...]], T],
	) -> Awaitable[T]:
		...
	def zrange_index(  # noqa: E301
		self, key, start, stop, *, reversed=False, with_scores=False, **kwargs
	):
		"""
		Perform an index range query on a sorted set. The order of elements is from the
		lowest to the highest score. The `start` and `stop` parameters
		represent zero-based indexes, where `0` is the first element, `1` is the next element,
		and so on. These parameters specify an **inclusive range**. So for example, a `start` of
		`0` and a `stop` of `1` will return *both* the first and second element of the
		sorted set.

		The indexes can also be negative numbers indicating offsets from the end of the
		sorted set, with `-1` being the last element, `-2` being the penultimate element,
		and so on.

		Out of range indexes do not produce an error.

		If `start` is greater than either the end index of the sorted set or `stop`, an
		empty list is returned.

		If `stop` is greater than the end index of the sorted set, the last element element
		of the sorted set will be used.

		Args:
			key: Name of the key sorted set is stored at.
			start: Index at which to start the range selection.
			stop: Index at which to stop the range selection. Remember that the range is
				**inclusive**, so the element at position `stop` will be included in the response.
			reverse: If `True` elements will be ordered from the highest to lowest score.
			with_scores: If `True` the response will be supplemented with the scores of the
				elements returned. The response will be a list of `(<element>, <score>)` tuples.

		Returns: A tuple of elements in the specified range; optionally with their
			scores if `with_scores` is used.

		Raises:
			TypeError: If the supplied `key` exists and is not a sorted set.
			ValueError: If either `start` or `stop` are not valid integers.
		"""
		command: List[Any] = ['ZRANGE', key, int(start), int(stop)]
		if reversed:
			command.append('REV')
		if with_scores:
			command.append('WITHSCORES')
		transforms, kwargs = collect_transforms(partial(_zrange_index_convert_to_tuple, with_scores=with_scores), kwargs)
		return self.execute(*command, error_func=_zrange_index_error_wrapper, transform=transforms, **kwargs)

	@overload
	def zrem(
		self: 'Executable',
		key: str,
		*members: Any,
		transform: None = None,
		encoding: Optional[str] = 'utf-8'
	) -> Awaitable[int]:
		...
	@overload  # noqa: E301
	def zrem(
		self: 'Executable',
		key: str,
		*members: Any,
		transform: Callable[[int], T],
		encoding: Optional[str] = 'utf-8'
	) -> Awaitable[T]:
		...
	def zrem(self, key, *members, **kwargs):  # noqa: E301
		"""
		Removes the specified `members` from the sorted set stored at `key`. Non-existing members
		are ignored.

		Args:
			key: Name of the key sorted set is stored at.
			members: Variable list of members to remove.

		Returns:
			The number of members removed from the sorted set not including non-existing members.

		Raises:
			TypeError: If the supplied `key` exists and is not a sorted set.
		"""
		command: List[Any] = ['ZREM', key, *members]
		return self.execute(*command, error_func=_zrem_error_wrapper, transform=kwargs.pop('transform', None), **kwargs)

	@overload
	def zscore(
		self: 'Executable', key: str, member: Any, *, encoding: Optional[str] = 'utf-8', transform: None = None
	) -> Awaitable[Optional[float]]:
		...
	@overload  # noqa: E301
	def zscore(
		self: 'Executable',
		key: str,
		member: Any,
		*,
		transform: Callable[[Optional[float]], T],
		encoding: Optional[str] = 'utf-8'
	) -> Awaitable[Optional[T]]:
		...
	def zscore(self, key, member, **kwargs):  # noqa: E301
		"""
		Returns the score of `member` in the sorted set at `key`. If `member` does not exist in the sorted
		set or `key` does not exist, `None` is returned.

		Args:
			key: Name of the key sorted set is stored at.
			member: Member for which to retrieve the score.

		Returns:
			The score of `member` if it is in the sorted set. Otherwise `None`.

		Raises:
			TypeError: If the supplied `key` exists and is not a sorted set.
		"""
		transforms, kwargs = collect_transforms(_zscore_convert_to_float, kwargs)
		return self.execute('ZSCORE', key, member, error_func=_zscore_error_wrapper, transform=transforms, **kwargs)
