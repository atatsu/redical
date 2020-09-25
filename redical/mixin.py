from __future__ import annotations

from typing import overload, Any, AnyStr, Awaitable, Protocol


class Executable(Protocol):
	@overload
	def execute(self, command: AnyStr, *args: Any, encoding: str = 'utf-8') -> Awaitable[Any]:
		...
	@overload  # noqa: E301
	def execute(self, command: AnyStr, *args: Any, **kwargs: Any) -> Awaitable[Any]:
		...
	def execute(self, command, *args, **kwargs):  # noqa: E301
		return self._resource.execute(command, *args, **kwargs)
