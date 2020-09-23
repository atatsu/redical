import pytest

from redical.connection import _build_command


@pytest.mark.parametrize('command, args, expected', [
	('set', ('mykey', 'foo'), b'*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$3\r\nfoo\r\n'),
	('set', ('mykey', 5.55), b'*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$4\r\n5.55\r\n'),
])
def test_build_command(command, args, expected):
	cmd = _build_command(command, *args)
	assert expected == bytes(cmd)
