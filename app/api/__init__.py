"""API-layer namespace exports."""

from api.factory import create_application
from api.router import router

__all__ = ["create_application", "router"]
