from __future__ import annotations

from abc import abstractmethod, ABC
from types import TracebackType
from typing import Any, AnyStr, Awaitable, Callable, List, Optional, Tuple, Type

__all__: List[str] = ['AbstractParser', 'RedicalResource']


class RedicalResource(ABC):
	@property
	@abstractmethod
	def address(self) -> Tuple[str, int]:
		"""
		The address being used to connect to Redis by the underlying connection or pool.
		"""

	@property
	@abstractmethod
	def db(self) -> int:
		"""
		Index of the currently selected database being used by the underlying
		connection or pool.
		"""

	@property
	@abstractmethod
	def encoding(self) -> Optional[str]:
		"""
		Encoding being used by the underlying connection or pool.
		"""

	@property
	@abstractmethod
	def is_closed(self) -> bool:
		"""
		Whether or not the underlying connection or pool is in a closed state.
		"""

	@property
	@abstractmethod
	def is_closing(self) -> bool:
		"""
		Whether or not the underlying connection or pool is in a closing state.
		"""

	@abstractmethod
	def close(self) -> None:
		"""
		Closes the underlying connection or pool.
		"""

	@abstractmethod
	def execute(
		self,
		command: AnyStr,
		*args: Any,
		conversion_func: Optional[Callable[[Any], Any]] = None,
		encoding: str = 'utf-8',
		**kwargs: Any
	) -> Awaitable[Any]:
		"""
		Execute a Redis command through the underlying connection or pool.
		"""

	@abstractmethod
	async def wait_closed(self) -> None:
		"""
		Wait until all the resources have been cleaned up for the underlying connection or pool.
		"""

	@abstractmethod
	async def __aenter__(self) -> RedicalResource:
		"""
		"""

	@abstractmethod
	async def __aexit__(
		self,
		exc_type: Optional[Type[BaseException]], exc: Optional[BaseException], tb: Optional[TracebackType]
	) -> Optional[bool]:
		"""
		"""


class AbstractParser(ABC):
	"""
	Parsers must conform to the `hiredis.Reader` interface.
	"""
	@abstractmethod
	def feed(self, data: AnyStr) -> None:
		"""
		Add data to the internal buffer.
		"""

	@abstractmethod
	def gets(self) -> Any:
		"""
		Read buffer and return a reply if the buffer contains a full reply.
		Otherwise return `False`.
		"""
