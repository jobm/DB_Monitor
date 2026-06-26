"""Top-level API router composed from focused route modules."""

from __future__ import annotations

from fastapi import APIRouter

from db_monitor.routes import auth as routes_auth
from db_monitor.routes import data as routes_data
from db_monitor.routes import ops as routes_ops

router = APIRouter()
router.include_router(routes_data.router)
router.include_router(routes_ops.router)
router.include_router(routes_auth.router)

__all__ = ["router"]
