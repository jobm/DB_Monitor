from __future__ import annotations

from db_monitor.extensions import engine as package_engine
from db_monitor.extensions import AsyncSessionLocal as package_session
from db_monitor.core.config import settings as package_settings


def test_package_config_surface_exposes_env() -> None:
    """Canonical package config surface should expose runtime env."""
    assert isinstance(package_settings.env, str)
    assert package_settings.env != ""


def test_package_extensions_surface_exposes_engine() -> None:
    """Canonical package extensions surface should expose shared engine."""
    assert hasattr(package_engine, "connect")
    assert hasattr(package_session, "__call__")
