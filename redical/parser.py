from typing import Any

try:
	from hiredis import Reader as HiredisParser  # type: ignore
except ImportError:
	HiredisParser = object

from .abstract import AbstractParser


# TODO: Pure-python parser
class PyParser:
	pass


class Parser(HiredisParser, PyParser, AbstractParser):
	def gets(self) -> Any:
		parsed: Any = super().gets()
		if parsed is False:
			return parsed
		# convert 'OK' responses to bool
		if parsed == b'OK':
			return True
		return parsed
