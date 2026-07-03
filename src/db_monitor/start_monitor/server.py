"""Uvicorn server startup with graceful signal handling."""

from __future__ import annotations

import asyncio
import logging
import signal
from typing import TYPE_CHECKING

from db_monitor.start_monitor.manifest import _configure_topics

if TYPE_CHECKING:
    import uvicorn

logger = logging.getLogger(__name__)


def _register_shutdown_signals(
    loop: asyncio.AbstractEventLoop,
    server: "uvicorn.Server",
) -> None:
    """Register SIGINT/SIGTERM handlers that trigger a graceful stop."""

    def _request_shutdown(sig_name: str) -> None:
        if server.should_exit:
            return
        logger.info("Received %s; requesting graceful shutdown", sig_name)
        server.should_exit = True

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(
                sig,
                _request_shutdown,
                sig.name,
            )
        except (NotImplementedError, RuntimeError, ValueError):
            # Some environments (e.g. Windows/sub-threads) do not support
            # add_signal_handler. Uvicorn's defaults still apply there.
            logger.debug(
                "Signal handler registration unsupported for %s",
                sig.name,
            )


async def _run_server() -> None:
    """Run the Uvicorn server with graceful signal handling hooks."""
    import uvicorn

    config = uvicorn.Config(
        "db_monitor.asgi:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
    )
    server = uvicorn.Server(config)
    _register_shutdown_signals(asyncio.get_running_loop(), server)
    await server.serve()


def main() -> None:
    """Start the API after aligning topics with the connector manifest."""
    _configure_topics()
    asyncio.run(_run_server())
