from __future__ import annotations

import asyncio
import gc
import importlib
import sys
import types
from pathlib import Path

import pytest


_APP_CORE_PATH = Path(__file__).resolve().parents[1] / "app" / "core"
if "core" not in sys.modules:
    core_package = types.ModuleType("core")
    core_package.__path__ = [str(_APP_CORE_PATH)]
    sys.modules["core"] = core_package

lifecycle_module = importlib.import_module("lifecycle_manager")


@pytest.mark.anyio
async def test_task_failure_triggers_shutdown_and_marks_unhealthy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A crashing background task should be supervised and stop the app."""
    manager = lifecycle_module.GracefulShutdownManager(shutdown_timeout=0.05)
    failure_gate = asyncio.Event()
    cleanup_called = asyncio.Event()
    cancelled = asyncio.Event()

    async def fake_cleanup_database() -> None:
        cleanup_called.set()

    monkeypatch.setattr(manager, "_cleanup_database", fake_cleanup_database)

    async def healthy_task() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async def crashing_task() -> None:
        await failure_gate.wait()
        raise RuntimeError("boom")

    healthy = asyncio.create_task(healthy_task(), name="healthy_task")
    crashing = asyncio.create_task(crashing_task(), name="crashing_task")
    manager.register_task(healthy)
    manager.register_task(crashing)

    del healthy
    del crashing
    gc.collect()

    assert {task.get_name() for task in manager.tasks.values()} == {
        "healthy_task",
        "crashing_task",
    }

    failure_gate.set()
    await asyncio.wait_for(cleanup_called.wait(), timeout=1)
    await asyncio.wait_for(cancelled.wait(), timeout=1)

    assert manager.has_task_failure is True
    assert manager.is_shutdown_in_progress is True
    assert manager.shutdown_event.is_set() is True
    assert not manager.tasks


@pytest.mark.anyio
async def test_application_shutdown_uses_configured_timeout_and_is_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Shutdown should honor the configured timeout and stay idempotent."""
    lifecycle = lifecycle_module.ApplicationLifecycleManager(
        shutdown_timeout=0.01,
    )
    shutdown_calls = 0
    wait_for_timeouts: list[float] = []

    async def fake_shutdown() -> None:
        nonlocal shutdown_calls
        shutdown_calls += 1
        lifecycle.shutdown_manager._shutdown_in_progress = True
        lifecycle.shutdown_manager.shutdown_event.set()

    async def fake_wait_for(awaitable, timeout: float):
        wait_for_timeouts.append(timeout)
        return await awaitable

    monkeypatch.setattr(lifecycle.shutdown_manager, "shutdown", fake_shutdown)
    monkeypatch.setattr(lifecycle_module.asyncio, "wait_for", fake_wait_for)

    await lifecycle.shutdown()
    await lifecycle.shutdown()

    assert shutdown_calls == 1
    assert wait_for_timeouts == [0.01, 0.01]
    assert lifecycle.shutdown_manager.is_shutdown_in_progress is True


@pytest.mark.anyio
async def test_noncritical_task_failure_does_not_shutdown(monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-critical background task failure should not trigger shutdown."""
    manager = lifecycle_module.GracefulShutdownManager(shutdown_timeout=0.05)
    failure_gate = asyncio.Event()
    cleanup_called = asyncio.Event()

    async def fake_cleanup_database() -> None:
        cleanup_called.set()

    monkeypatch.setattr(manager, "_cleanup_database", fake_cleanup_database)

    async def noncritical_crash() -> None:
        await failure_gate.wait()
        raise RuntimeError("noncritical boom")

    async def stable_task() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            return

    crash = asyncio.create_task(noncritical_crash(), name="noncritical_crash")
    stable = asyncio.create_task(stable_task(), name="stable_task")
    manager.register_task(crash, critical=False)
    manager.register_task(stable, critical=True)

    failure_gate.set()

    # give the manager a moment to observe the exception
    await asyncio.sleep(0.05)

    # non-critical failure should not trigger shutdown
    assert manager.has_task_failure is False
    assert manager.is_shutdown_in_progress is False
    assert not manager.shutdown_event.is_set()
    # stable task should still be registered
    assert any(t.get_name() == "stable_task" for t in manager.tasks.values())
