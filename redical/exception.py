from typing import List

__all__: List[str] = [
	'RedicalError',
	'InvalidKeyError',
	'NoExpiryError',
]


class RedicalError(Exception):
	"""
	"""


class InvalidKeyError(RedicalError):
	"""
	"""
	key: str

	def __init__(self, key: str, *, message: str) -> None:
		self.key = key
		super().__init__(message)


class NoExpiryError(InvalidKeyError):
	"""
	"""
