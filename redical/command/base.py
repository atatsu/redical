from typing import Any, AnyStr, Awaitable, TYPE_CHECKING


class BaseMixin:
	if TYPE_CHECKING:
		def execute(self, command: AnyStr, *args: Any, encoding: str = 'utf-8', **kwargs: Any) -> Awaitable[Any]:
			pass
