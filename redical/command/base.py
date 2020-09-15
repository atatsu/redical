from typing import Any, AnyStr, Awaitable


class BaseMixin:
	def execute(self, command: AnyStr, *args: Any, encoding: str = 'utf-8', **kwargs: Any) -> Awaitable[Any]:
		pass
