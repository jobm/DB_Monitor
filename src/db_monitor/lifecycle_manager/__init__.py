"""Application Lifecycle Management

Handles graceful startup, shutdown, and resource cleanup for the
FastAPI application.
Manages Kafka consumer tasks, database connections, and signal handling.
"""

from __future__ import annotations

import asyncio as _asyncio

from db_monitor.lifecycle_manager.lifecycle import (
    ApplicationLifecycleManager,
    lifecycle_manager,
)
from db_monitor.lifecycle_manager.shutdown import GracefulShutdownManager

# Re-export asyncio so tests can do lifecycle_module.asyncio.wait_for patch.
asyncio = _asyncio

__all__ = [
    "ApplicationLifecycleManager",
    "GracefulShutdownManager",
    "asyncio",
    "lifecycle_manager",
]
