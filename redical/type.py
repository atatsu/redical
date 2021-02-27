from typing import Any, Callable, Sequence, Union


class undefined:
	pass


CommandType = Union[str, bytes]
ErrorFuncType = Callable[[Exception], Exception]
TransformFuncType = Callable[[Any], Any]
TransformType = Union[Sequence[TransformFuncType], TransformFuncType]
