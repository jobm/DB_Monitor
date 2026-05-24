from __future__ import annotations

import db_monitor.core.config as core_config
import db_monitor.core.db as core_db


def test_core_config_namespace_proxy_exposes_runtime_settings() -> None:
    """Canonical core config namespace should expose legacy runtime settings."""
    assert isinstance(core_config.APP_ENV, str)
    assert core_config.APP_ENV != ""


def test_core_db_namespace_proxy_exposes_engine_surface() -> None:
    """Canonical core DB namespace should expose legacy DB helpers."""
    assert hasattr(core_db, "engine")
    assert callable(core_db.build_engine)
