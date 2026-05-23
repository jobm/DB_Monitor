"""FastAPI application factory helpers."""

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
