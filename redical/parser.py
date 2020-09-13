try:
	from hiredis import Reader as HiredisParser  # type: ignore
except ImportError:
	HiredisParser = object

from .abstract import AbstractParser


# TODO: Pure-python parser
class PyParser:
	pass


class Parser(HiredisParser, PyParser, AbstractParser):
	pass
