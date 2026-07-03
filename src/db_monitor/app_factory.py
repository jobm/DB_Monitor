"""FastAPI application factory.

Provides the ``create_application`` helper that builds a configured FastAPI
instance with the specified title, description, version, and lifespan.

Key exports:
    - create_application: Build and return a configured FastAPI ASGI app

Related modules:
    - :mod:`db_monitor.main` — calls create_application and wires routes
    - :mod:`db_monitor.lifespan` — provides the managed lifespan context
"""

from __future__ import annotations

from collections.abc import Callable

from fastapi import FastAPI


def create_application(
    *,
    title: str,
    description: str,
    version: str,
    lifespan: Callable,
) -> FastAPI:
    """Create and return the configured FastAPI application instance."""
    return FastAPI(
        title=title,
        description=description,
        version=version,
        lifespan=lifespan,
    )
