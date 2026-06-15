"""Startup import-chain smoke tests.

These tests verify that the full application object can be imported without
triggering circular-import errors or missing-module failures.  A regression
here (e.g. a new dependency cycle through ``core/__init__``) will surface
immediately in the unit-test suite rather than at deploy time.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest
from fastapi import FastAPI


def test_main_app_imports_without_error() -> None:
    """Import the ``main`` module and confirm the FastAPI *app* is reachable.

    This exercises the entire import chain that uvicorn follows when it
    loads ``main:app`` — the same path that the CI smoke-test and the
    production container entrypoint use.
    """
    main = importlib.import_module("main")
    assert hasattr(main, "app"), "main module must expose an 'app' attribute"
    assert isinstance(
        main.app, FastAPI
    ), "app must be a FastAPI instance"


def test_core_proxy_modules_are_importable() -> None:
    """All ``core`` proxy sub-modules must be importable without error.

    ``app/core/__init__.py`` eagerly imports every sub-module, so this
    test also catches any cycle introduced through the proxy layer.
    """
    core_config = importlib.import_module("core.config")
    core_db = importlib.import_module("core.db")
    core_lifecycle = importlib.import_module("core.lifecycle")
    core_models = importlib.import_module("core.models")
    core_responses = importlib.import_module("core.responses")

    assert hasattr(core_config, "KAFKA_BROKER")
    assert hasattr(core_db, "AsyncSessionLocal")
    assert hasattr(core_db, "engine")
    assert hasattr(core_lifecycle, "lifecycle_manager")
    assert hasattr(core_models, "ApiKey")
    assert hasattr(core_responses, "HealthResponse")


def test_extensions_exposes_expected_symbols() -> None:
    """The ``extensions`` module must expose the DB engine and session factory."""
    extensions = importlib.import_module("extensions")

    assert hasattr(extensions, "engine")
    assert hasattr(extensions, "AsyncSessionLocal")
    assert hasattr(extensions, "build_engine")
    assert hasattr(extensions, "init_db")


def test_db_monitor_package_imports_without_error() -> None:
    """The ``db_monitor`` namespace proxies must all be importable.

    This catches regressions where a ``src/db_monitor`` proxy module
    triggers a circular import through the legacy ``app/`` tree.
    """
    proxy_modules = [
        "db_monitor.core.config",
        "db_monitor.core.db",
        "db_monitor.config",
        "db_monitor.extensions",
        "db_monitor.metrics",
        "db_monitor.retention",
        "db_monitor.lifespan",
        "db_monitor.tracing",
        "db_monitor.ws_manager",
    ]

    for module_name in proxy_modules:
        importlib.import_module(module_name)


def test_circular_import_in_extensions_is_caught(
    tmp_path: Path,
) -> None:
    """Drop a broken ``extensions.py`` into *tmp_path* and verify the
    import chain fails.

    This is a *negative* test: it deliberately reintroduces the circular
    import (``extensions → core.config → core/__init__ → core.db →
    extensions``) and asserts that importing ``main`` raises an error.
    This proves the positive smoke tests above actually catch the
    regression they were designed for.

    Instead of modifying the real source file (which would race with
    parallel test workers), a minimal broken ``extensions.py`` is placed
    in a temporary directory that takes priority on ``sys.path``.
    """
    # Broken extensions.py that triggers the circular import chain.
    (tmp_path / "extensions.py").write_text(
        "from core.config import POSTGRES_URL\n"
    )

    # Clear every module that participates in or caches the import chain
    # so Python is forced to re-import from scratch (hitting our broken
    # file at the front of sys.path).
    saved: dict[str, object] = {}
    for name in list(sys.modules):
        if (
            name == "extensions"
            or name.startswith("core")
            or name.startswith("db_monitor")
            or name == "main"
        ):
            saved[name] = sys.modules.pop(name)

    sys.path.insert(0, str(tmp_path))
    try:
        with pytest.raises((ImportError, AttributeError, ValueError)):
            importlib.import_module("main")
    finally:
        sys.path.remove(str(tmp_path))
        sys.modules.update(saved)
