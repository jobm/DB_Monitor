from __future__ import annotations

import subprocess
from urllib.error import URLError

from examples.sandbox import preflight


class _FakeResponse:
    def __init__(self, status_code: int = 200) -> None:
        self._status_code = status_code

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None

    def getcode(self) -> int:
        return self._status_code


def test_sandbox_pytest_skip_reason_none_when_stack_reachable(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        preflight.urllib.request,
        "urlopen",
        lambda url, timeout: _FakeResponse(),
    )

    assert (
        preflight.sandbox_pytest_skip_reason("http://localhost:8000")
        is None
    )


def test_sandbox_pytest_skip_reason_mentions_docker_when_unavailable(
    monkeypatch,
) -> None:
    def fake_urlopen(url, timeout):
        del url, timeout
        raise URLError("connection refused")

    monkeypatch.setattr(preflight.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(
        preflight.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args=["docker", "info"],
            returncode=1,
            stdout="",
            stderr="cannot connect to docker",
        ),
    )

    reason = preflight.sandbox_pytest_skip_reason("http://localhost:8000")

    assert reason is not None
    assert "Docker" in reason
    assert "connection refused" in reason


def test_require_live_sandbox_raises_clear_error_when_stack_missing(
    monkeypatch,
) -> None:
    def fake_urlopen(url, timeout):
        del url, timeout
        raise URLError("connection refused")

    monkeypatch.setattr(preflight.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(
        preflight.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args=["docker", "info"],
            returncode=0,
            stdout="ok",
            stderr="",
        ),
    )

    try:
        preflight.require_live_sandbox("http://localhost:8000")
    except RuntimeError as exc:
        assert "make monitor-recovery" in str(exc)
    else:  # pragma: no cover - explicit failure branch
        raise AssertionError("Expected require_live_sandbox to raise")
