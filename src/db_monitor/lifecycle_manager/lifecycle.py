"""Application lifecycle management — signal handling, startup, and shutdown orchestration."""

from __future__ import annotations

import asyncio
import logging
import signal
import sys

from db_monitor.core.config import settings
from db_monitor.extensions import init_db
from db_monitor.lifecycle_manager.shutdown import GracefulShutdownManager

logger = logging.getLogger(__name__)


class ApplicationLifecycleManager:
    """Manages the complete application lifecycle"""

    def __init__(self, shutdown_timeout: float = 30.0):
        self.shutdown_manager = GracefulShutdownManager(shutdown_timeout)
        self._startup_complete = False

    def setup_signal_handlers(self, loop: asyncio.AbstractEventLoop) -> None:
        """Setup signal handlers for graceful shutdown"""

        async def signal_handler(signum: int) -> None:
            """Handle shutdown signals asynchronously"""
            logger.info(f"Received signal {signum}")

            if not self.shutdown_manager.is_shutdown_in_progress:
                await self.shutdown_manager.shutdown()
            else:
                # Force exit on repeated signals
                logger.warning("Force exit requested")
                await asyncio.sleep(1.0)
                import os

                os._exit(1)

        def add_signal_handler(sig: signal.Signals) -> None:
            """Add a signal handler safely"""
            try:
                loop.add_signal_handler(
                    sig, lambda: asyncio.create_task(signal_handler(sig.value))
                )
                logger.debug(f"Signal handler registered for {sig.name}")
            except (OSError, NotImplementedError) as e:
                logger.warning(
                    f"Could not register signal handler for {sig.name}: {e}"
                )

        # Register signal handlers (Unix only)
        if sys.platform != "win32":
            add_signal_handler(signal.SIGINT)
            add_signal_handler(signal.SIGTERM)
        else:
            logger.info(
                "Running on Windows - limited signal handling available"
            )

    async def startup(self) -> None:
        """Initialize application resources"""
        logger.info("Starting application...")

        try:
            # Initialize database
            await init_db()
            logger.info("Database initialized")

            self._startup_complete = True
            logger.info("Application startup completed")

        except Exception as e:
            logger.error(f"Failed to initialize application: {e}")
            raise

    async def shutdown(self) -> None:
        """Shutdown application resources"""
        logger.info("Application shutdown initiated")

        if not self.shutdown_manager.is_shutdown_in_progress:
            await self.shutdown_manager.shutdown()

        # Wait for shutdown to complete
        try:
            await asyncio.wait_for(
                self.shutdown_manager.shutdown_event.wait(),
                timeout=self.shutdown_manager.shutdown_timeout,
            )
        except asyncio.TimeoutError:
            logger.error("Shutdown timeout exceeded")

    def register_task(
        self,
        task: asyncio.Task,
        *,
        critical: bool = True,
    ) -> None:
        """Register a task for cleanup during shutdown"""
        self.shutdown_manager.register_task(task, critical=critical)

    @property
    def is_healthy(self) -> bool:
        """Check if application is healthy"""
        return (
            self._startup_complete
            and not self.shutdown_manager.is_shutdown_in_progress
            and not self.shutdown_manager.has_task_failure
        )

    @property
    def is_shutting_down(self) -> bool:
        """Check if application is shutting down"""
        return self.shutdown_manager.is_shutdown_in_progress


# Global lifecycle manager instance
lifecycle_manager = ApplicationLifecycleManager(
    shutdown_timeout=settings.app_shutdown_timeout_seconds,
)
