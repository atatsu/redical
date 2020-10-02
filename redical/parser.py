from .abstract import AbstractParser
from .exception import ResponseError


# TODO: Pure-python parser
class PyParser:
	pass


try:
	from hiredis import Reader as ParserBase  # type: ignore
except ImportError:
	ParserBase = PyParser


class Parser(ParserBase, AbstractParser):
	def __init__(self) -> None:
		if not issubclass(ParserBase, PyParser):
			super().__init__(replyError=ResponseError)

	# def gets(self) -> Any:
	#     parsed: Any = super().gets()
	#     if parsed is False:
	#         return parsed
	#     # convert 'OK' responses to bool
	#     if parsed == b'OK':
	#         return True
	#     return parsed
