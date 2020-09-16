from typing import List

__all__: List[str] = [
	'RedicalError',
	'InvalidKeyError',
	'NoExpiryError',
]


class RedicalError(Exception):
	"""
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
