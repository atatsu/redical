from typing import Awaitable

from .base import BaseMixin


class ServerCommandsMixin(BaseMixin):
	def flushdb(self) -> Awaitable[bool]:
		"""
		Delete all the keys of the currently selected DB.
		"""
		return self.execute('flushdb')
