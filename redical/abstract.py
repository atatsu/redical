from __future__ import annotations

from abc import abstractmethod, ABC
from contextlib import asynccontextmanager
from types import TracebackType
from typing import Any, AnyStr, AsyncIterator, Awaitable, Callable, List, Optional, Sequence, Tuple, Type, Union

__all__: List[str] = ['AbstractParser', 'RedicalResource']

ErrorFunc = Callable[[Exception], Exception]
TransformFunc = Callable[[Any], Any]
Transform = Union[Sequence[TransformFunc], TransformFunc]


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

	@property
	@abstractmethod
	def supports_multiple_pipelines(self) -> bool:
		"""
		Whether or not this resource supports multiple simultaneous pipelines.
		If the underlying resource is a `Connection`, it will not.
		If the underlying resource is a `ConnectionPool`, it will.
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
		encoding: str = 'utf-8',
		error_func: Optional[ErrorFunc] = None,
		transform: Optional[Transform] = None
	) -> Awaitable[Any]:
		"""
		Execute a Redis command through the underlying connection or pool.
		"""

	@asynccontextmanager
	@abstractmethod
	async def transaction(self, *watch_keys: str) -> AsyncIterator[RedicalResource]:
		"""
		Starts a transaction block.
		"""
		yield self

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
