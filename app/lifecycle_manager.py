"""
Application Lifecycle Management

Handles graceful startup, shutdown, and resource cleanup for the FastAPI application.
Manages Kafka consumer tasks, database connections, and signal handling.
"""

import asyncio
import logging
import signal
import sys
import weakref
from typing import Optional, Set
from extensions import AsyncSessionLocal, init_db

logger = logging.getLogger(__name__)


class GracefulShutdownManager:
    """Manages graceful shutdown of the application"""
    
    def __init__(self, shutdown_timeout: float = 30.0):
        self.shutdown_event = asyncio.Event()
        self.tasks: weakref.WeakSet = weakref.WeakSet()
        self._shutdown_timeout = shutdown_timeout
        self._shutdown_in_progress = False
    
    def register_task(self, task: asyncio.Task) -> None:
        """Register a task for cleanup during shutdown"""
        self.tasks.add(task)
        logger.debug(f"Registered task: {task.get_name()}")
    
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
            active_tasks = [task for task in self.tasks if not task.done()]
            if active_tasks:
                logger.info(f"Cancelling {len(active_tasks)} active tasks...")
                for task in active_tasks:
                    task.cancel()
                
                # Wait for tasks with timeout
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*active_tasks, return_exceptions=True),
                        timeout=self._shutdown_timeout
                    )
                    logger.info("All tasks cancelled successfully")
                except asyncio.TimeoutError:
                    logger.warning(f"Some tasks didn't complete within {self._shutdown_timeout}s timeout")
            
            # Close database connections
            await self._cleanup_database()
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
        finally:
            self.shutdown_event.set()
            logger.info("Graceful shutdown completed")
    
    async def _cleanup_database(self) -> None:
        """Clean up database connections"""
        try:
            # For SQLAlchemy async sessions, cleanup is typically automatic
            # Add specific cleanup logic here if needed
            logger.info("Database cleanup completed")
        except Exception as e:
            logger.error(f"Error during database cleanup: {e}")


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
                    sig, 
                    lambda: asyncio.create_task(signal_handler(sig.value))
                )
                logger.debug(f"Signal handler registered for {sig.name}")
            except (OSError, NotImplementedError) as e:
                logger.warning(f"Could not register signal handler for {sig.name}: {e}")
        
        # Register signal handlers (Unix only)
        if sys.platform != 'win32':
            add_signal_handler(signal.SIGINT)
            add_signal_handler(signal.SIGTERM)
        else:
            logger.info("Running on Windows - limited signal handling available")
    
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
                timeout=30.0
            )
        except asyncio.TimeoutError:
            logger.error("Shutdown timeout exceeded")
    
    def register_task(self, task: asyncio.Task) -> None:
        """Register a task for cleanup during shutdown"""
        self.shutdown_manager.register_task(task)
    
    @property
    def is_healthy(self) -> bool:
        """Check if application is healthy"""
        return (
            self._startup_complete and 
            not self.shutdown_manager.is_shutdown_in_progress
        )
    
    @property
    def is_shutting_down(self) -> bool:
        """Check if application is shutting down"""
        return self.shutdown_manager.is_shutdown_in_progress


# Global lifecycle manager instance
lifecycle_manager = ApplicationLifecycleManager()
