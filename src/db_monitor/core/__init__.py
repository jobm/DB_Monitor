"""Core namespace exports for the canonical db_monitor package."""

__all__ = ["config_loader"]

# Import config_loader so it is accessible via db_monitor.core.config_loader
from db_monitor.core import config_loader  # noqa: F401
