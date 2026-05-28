"""
Application Lifecycle Management

Handles graceful startup, shutdown, and resource cleanup for the
FastAPI application.
Manages Kafka consumer tasks, database connections, and signal handling.
"""

import asyncio
import logging
import signal
import sys
from typing import Any

from core.config import APP_SHUTDOWN_TIMEOUT_SECONDS
from core.db import init_db

logger = logging.getLogger(__name__)


class GracefulShutdownManager:
    """Manages graceful shutdown of the application"""

    def __init__(self, shutdown_timeout: float = 30.0):
        self.shutdown_event = asyncio.Event()
        self.tasks: dict[int, asyncio.Task[Any]] = {}
        self._shutdown_timeout = shutdown_timeout
        self._task_critical: dict[int, bool] = {}
        self._shutdown_in_progress = False
        self._task_failure_detected = False
        self._shutdown_on_task_failure: asyncio.Task[Any] | None = None

    def register_task(
        self,
        task: asyncio.Task,
        *,
        critical: bool = True,
    ) -> None:
        """Register a task for cleanup during shutdown.

        critical: when True, a task failure triggers a controlled shutdown.
        When False, failures are logged but do not cause shutdown.
        """
        self.tasks[id(task)] = task
        self._task_critical[id(task)] = bool(critical)
        task.add_done_callback(self._handle_task_completion)
        logger.debug(
            "Registered task: %s (critical=%s)",
            task.get_name(),
            critical,
        )

    def _handle_task_completion(self, task: asyncio.Task[Any]) -> None:
        """Record task failures and trigger shutdown when needed."""
        try:
            exception = task.exception()
        except asyncio.CancelledError:
            return

        if exception is None:
            return
        task_id = id(task)
        critical = self._task_critical.get(task_id, True)

        logger.error(
            "Background task failed",
            extra={"task_name": task.get_name(), "critical": critical},
            exc_info=(type(exception), exception, exception.__traceback__),
        )

        if critical:
            # Critical tasks mark failure and trigger controlled shutdown.
            self._task_failure_detected = True
            if (
                not self._shutdown_in_progress
                and self._shutdown_on_task_failure is None
            ):
                loop = task.get_loop()
                self._shutdown_on_task_failure = loop.create_task(
                    self.shutdown(),
                    name="shutdown_after_task_failure",
                )
        else:
            # Non-critical failures are logged but do not bring down the app.
            logger.warning(
                "Non-critical background task failed; continuing",
                extra={"task_name": task.get_name()},
                exc_info=(type(exception), exception, exception.__traceback__),
            )

    @property
    def shutdown_timeout(self) -> float:
        """Return the configured graceful-shutdown timeout."""
        return self._shutdown_timeout

    @property
    def has_task_failure(self) -> bool:
        """Return whether any registered background task failed."""
        return self._task_failure_detected

    @property
    def is_shutdown_in_progress(self) -> bool:
        """Check if shutdown is in progress"""
        return self._shutdown_in_progress

    async def shutdown(self) -> None:
        """Perform graceful shutdown with timeout"""
        if self._shutdown_in_progress:
            logger.warning("Shutdown already in progress")
            return

        self._shutdown_in_progress = True
        logger.info("Initiating graceful shutdown...")

        try:
            # Cancel all registered tasks
            active_tasks = [
                task for task in self.tasks.values() if not task.done()
            ]
            if active_tasks:
                logger.info(
                    "Cancelling %s active tasks...",
                    len(active_tasks),
                )
                for task in active_tasks:
                    task.cancel()

                # Wait for tasks with timeout
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*active_tasks, return_exceptions=True),
                        timeout=self._shutdown_timeout,
                    )
                    logger.info("All tasks cancelled successfully")
                except asyncio.TimeoutError:
                    logger.warning(
                        "Some tasks didn't complete within %ss timeout",
                        self._shutdown_timeout,
                    )

            # Close database connections
            await self._cleanup_database()

        except Exception as e:
            logger.error("Error during shutdown: %s", e)
        finally:
            self.tasks.clear()
            self._task_critical.clear()
            self._shutdown_on_task_failure = None
            self.shutdown_event.set()
            logger.info("Graceful shutdown completed")

    async def _cleanup_database(self) -> None:
        """Clean up database connections"""
        try:
            # For SQLAlchemy async sessions, cleanup is typically automatic
            # Add specific cleanup logic here if needed
            logger.info("Database cleanup completed")
        except Exception as e:
            logger.error("Error during database cleanup: %s", e)


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
    shutdown_timeout=APP_SHUTDOWN_TIMEOUT_SECONDS,
)
