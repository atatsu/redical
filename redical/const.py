from enum import Enum


class UpdatePolicy(str, Enum):
	EXISTS = 'XX'
	NOT_EXISTS = 'NX'

	def __str__(self) -> str:
		return str(self.value)
