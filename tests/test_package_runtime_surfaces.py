from __future__ import annotations

import db_monitor.config as package_config
import db_monitor.extensions as package_extensions


def test_package_config_surface_exposes_app_env() -> None:
    """Canonical package config surface should expose runtime APP_ENV."""
    assert isinstance(package_config.APP_ENV, str)
    assert package_config.APP_ENV != ""


def test_package_extensions_surface_exposes_engine() -> None:
    """Canonical package extensions surface should expose shared engine."""
    assert hasattr(package_extensions, "engine")
    assert hasattr(package_extensions, "AsyncSessionLocal")
