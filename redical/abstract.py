from abc import abstractmethod, ABC
from typing import Any, AnyStr, Awaitable, Callable, List, Optional, Tuple

__all__: List[str] = ['AbstractConnection', 'AbstractParser', 'AbstractPool']


class AbstractConnection(ABC):
	"""
	Redis connection interface.
	"""
	@property
	@abstractmethod
	def address(self) -> Tuple[str, int]:
		"""
		Address being used for the connection.
		"""

	@property
	@abstractmethod
	def is_closed(self) -> bool:
		"""
		Indicates whether the connection is closing or already closed.
		"""

	@property
	@abstractmethod
	def db(self) -> int:
		"""
		Index of currently selected DB.
		"""

	@property
	@abstractmethod
	def encoding(self) -> Optional[str]:
		"""
		Connection's encoding, if one was specified.
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
		Execute a redis command.
		"""

	@abstractmethod
	def close(self) -> None:
		"""
		Close the connection.
		"""

	@abstractmethod
	async def wait_closed(self) -> None:
		"""
		Wait until all resources have been cleaned up.

		Should be called after `close()`.
		"""


class AbstractPool(ABC):
	"""
	Redis connection pool interface.

	Inherits from `BaseConnection` so that both use the same interface for executing commands.
	"""
	@abstractmethod
	async def acquire_connection(self, command: bytes, *args: Any) -> Optional[AbstractConnection]:
		"""
		Asynchronously acquire a connection from the pool.
		"""

	@abstractmethod
	def acquire_connection_sync(self, command: bytes, *args: Any) -> Optional[AbstractConnection]:
		"""
		Synchronously acquire a connection from the pool.
		"""

	@abstractmethod
	def release(self, conn: AbstractConnection) -> None:
		"""
		Releases a connection to the pool.
		"""


class AbstractParser(ABC):
	"""
	Parsers must conform to the `hiredis.Reader` interface.
	"""
	@abstractmethod
	def feed(self, data: bytes) -> None:
		"""
		"""

	@abstractmethod
	def gets(self) -> Any:
		"""
		"""
