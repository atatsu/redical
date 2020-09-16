from typing import Any, Awaitable

from .base import BaseMixin


class ServerCommandsMixin(BaseMixin):
	"""
	Implemented commands:
		* flushdb
	"""
	def flushdb(self, **kwargs: Any) -> Awaitable[bool]:
		"""
		Delete all the keys of the currently selected DB.
		"""
		return self.execute('flushdb', **kwargs)
