from typing import List, Optional

__all__: List[str] = [
	'RedicalError',
	'ConnectionError',
	'ConnectionClosedError',
	'ConnectionClosingError',
	'InvalidKeyError',
	'NoExpiryError',
	'PipelineError',
	'PoolError',
	'PoolClosingError',
	'PoolClosedError',
	'ResponseError',
]


class RedicalError(Exception):
	"""
	"""


class ResponseError(RedicalError):
	"""
	Raised when *redis* replies with an error (`-`) response.
	"""


class KeyError(RedicalError):
	key: str

	def __init__(self, key: str, message: str) -> None:
		self.key = key
		super().__init__(message)


class InvalidKeyError(KeyError):
	"""
	Raised in situations when an operation is attempted on a key that doesn't exist.
	"""
	def __init__(self, key: str) -> None:
		super().__init__(key, f'Key with name {key!r} does not exist')


class NoExpiryError(KeyError):
	"""
	"""
	def __init__(self, key: str) -> None:
		super().__init__(key, f'Key with name {key!r} has no expiry set')


class PipelineError(RedicalError):
	"""
	"""


class ConnectionError(RedicalError):
	"""
	"""


class ConnectionClosedError(ConnectionError):
	"""
	"""
	def __init__(self, message: Optional[str] = None) -> None:
		message = message if message is not None else 'Connection is closed'
		super().__init__(message)


class ConnectionClosingError(ConnectionError):
	"""
	"""
	def __init__(self, message: Optional[str] = None) -> None:
		message = message if message is not None else 'Connection is closing'
		super().__init__(message)


class PoolError(RedicalError):
	"""
	"""


class PoolClosedError(PoolError):
	"""
	"""
	def __init__(self, message: Optional[str] = None) -> None:
		message = message if message is not None else 'Pool is closed'
		super().__init__(message)


class PoolClosingError(PoolError):
	"""
	"""
	def __init__(self, message: Optional[str] = None) -> None:
		message = message if message is not None else 'Pool is closing'
		super().__init__(message)
