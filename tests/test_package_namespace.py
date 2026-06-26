from __future__ import annotations

from db_monitor.core.config import settings as core_settings
from db_monitor.extensions import engine as core_engine
from db_monitor.extensions import build_engine as core_build_engine


def test_core_config_namespace_exposes_runtime_settings() -> None:
    """Canonical core config namespace should expose runtime settings."""
    assert isinstance(core_settings.env, str)
    assert core_settings.env != ""


def test_core_db_namespace_exposes_engine_surface() -> None:
    """Canonical core DB namespace should expose legacy DB helpers."""
    assert hasattr(core_engine, "connect")
    assert callable(core_build_engine)
