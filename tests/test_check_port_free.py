"""Unit tests for the ``check_port_free`` CLI script."""

from __future__ import annotations

import socket

import scripts.check_port_free as check_port_free


def _find_free_port() -> int:
    """Return a TCP port number that is currently unused."""
    with socket.socket() as sock:
        sock.bind(("localhost", 0))
        return sock.getsockname()[1]


def test_check_port_returns_true_when_free() -> None:
    port = _find_free_port()
    assert check_port_free.check_port("localhost", port, timeout=1.0) is True


def test_check_port_returns_false_when_occupied() -> None:
    port = _find_free_port()
    with socket.socket() as blocker:
        blocker.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        blocker.bind(("localhost", port))
        blocker.listen(1)
        assert check_port_free.check_port("localhost", port, timeout=1.0) is False


def test_main_returns_zero_when_port_is_free() -> None:
    port = _find_free_port()
    assert check_port_free.main([str(port)]) == 0


def test_main_returns_one_when_port_is_occupied() -> None:
    port = _find_free_port()
    with socket.socket() as blocker:
        blocker.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        blocker.bind(("localhost", port))
        blocker.listen(1)
        assert check_port_free.main([str(port)]) == 1


def test_main_respects_host_flag() -> None:
    port = _find_free_port()
    assert check_port_free.main([str(port), "--host", "127.0.0.1"]) == 0
