import logging
from typing import Any, Dict, Final, List, Optional, Tuple

from .abstract import Transform, TransformFunc

LOG: Final[logging.Logger] = logging.getLogger(__name__)


class undefined:
	pass


def collect_transforms(
	transform: Optional[Transform], kwargs: Optional[Dict[str, Any]] = None
) -> Tuple[List[TransformFunc], Dict[str, Any]]:
	transforms: List[TransformFunc] = []
	f: Any
	if transform is not None:
		if callable(transform):
			transforms.append(transform)
		else:
			for f in list(transform):
				if callable(f):
					transforms.append(f)
					continue
				LOG.warning(f'Not including non-callable transform: {f}')

	if kwargs is not None:
		transform = kwargs.pop('transform', None)
		transforms.extend(collect_transforms(transform)[0])

	return (transforms, kwargs or {})
