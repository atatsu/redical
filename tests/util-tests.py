from redical.util import collect_transforms


def test_collect_transforms():
	transforms = collect_transforms(bool, dict(transform=int))
	transforms = collect_transforms(transforms[0], dict(transform=str))

	assert 3 == len(transforms[0])
	assert True is transforms[0][0]('1')
	assert 1 == transforms[0][1]('1')
	assert '1' == transforms[0][2]('1')
