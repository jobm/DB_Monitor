"""Top-level API router composed from focused route modules."""

from __future__ import annotations

from fastapi import APIRouter

from . import auth as routes_auth
from . import data as routes_data
from . import ops as routes_ops

router = APIRouter()
router.include_router(routes_data.router)
router.include_router(routes_ops.router)
router.include_router(routes_auth.router)
