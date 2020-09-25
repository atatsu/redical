from typing import Any, Awaitable

from ..mixin import Executable


class ServerCommandsMixin:
	"""
	Implemented commands:
		* flushdb
	"""
	def flushdb(self: Executable, **kwargs: Any) -> Awaitable[bool]:
		"""
		Delete all the keys of the currently selected DB.
		"""
		return self.execute('flushdb', **kwargs)
