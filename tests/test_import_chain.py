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


def test_asgi_app_imports_without_error() -> None:
    """Import the ``db_monitor.asgi`` module and confirm the FastAPI *app*
    is reachable.

    This exercises the entire import chain that uvicorn follows when it
    loads ``db_monitor.asgi:app`` -- the same path that the CI smoke-test and
    the production container entrypoint use.
    """
    asgi = importlib.import_module("db_monitor.asgi")
    assert hasattr(
        asgi, "app"
    ), "db_monitor.asgi module must expose an 'app' attribute"
    assert isinstance(
        asgi.app, FastAPI
    ), "app must be a FastAPI instance"


def test_extensions_exposes_expected_symbols() -> None:
    """The ``db_monitor.extensions`` module must expose the DB engine and
    session factory.
    """
    extensions = importlib.import_module("db_monitor.extensions")
    assert hasattr(extensions, "engine")
    assert hasattr(extensions, "AsyncSessionLocal")
    assert hasattr(extensions, "build_engine")
    assert hasattr(extensions, "init_db")


def test_db_monitor_package_imports_without_error() -> None:
    """The ``db_monitor`` namespace modules must all be importable.

    This catches regressions where a ``src/db_monitor`` module triggers a
    circular import through the legacy ``app/`` tree.
    """
    modules = [
        "db_monitor.core.config",
        "db_monitor.extensions",
        "db_monitor.lifecycle_manager",
        "db_monitor.metrics",
        "db_monitor.retention",
        "db_monitor.lifespan",
        "db_monitor.tracing",
        "db_monitor.ws_manager",
        "db_monitor.main",
        "db_monitor.asgi",
    ]

    for module_name in modules:
        importlib.import_module(module_name)


def test_broken_extensions_is_caught(
    tmp_path: Path,
) -> None:
    """Drop a broken ``extensions.py`` into *tmp_path* and verify the
    import chain fails.

    This is a *negative* test: it supplies a minimal ``extensions.py``
    that imports from ``db_monitor.core.config`` but does **not** define the
    symbols (``engine``, ``AsyncSessionLocal``, ...) that ``db_monitor.core.db``
    expects.  When ``core.db`` tries to access those names it raises
    ``AttributeError``, which propagates through the import chain and causes
    the ASGI app to fail to load.

    This proves the positive smoke tests above actually catch a broken
    ``extensions`` module -- whether the failure manifests as a
    missing-symbol error, a circular-import error, or another import-time
    exception.

    Instead of modifying the real source file (which would race with parallel
    test workers), a minimal broken ``extensions.py`` is placed in a temporary
    directory that takes priority on ``sys.path``.
    """
    # Broken extensions.py missing the symbols db_monitor.core.db expects.
    (tmp_path / "extensions.py").write_text(
        "from db_monitor.core.config import settings\n"
    )

    # Clear every module that participates in or caches the import chain
    # so Python is forced to re-import from scratch (hitting our broken
    # file at the front of sys.path).
    saved: dict[str, object] = {}
    for name in list(sys.modules):
        if (
            name == "extensions"
            or name.startswith("db_monitor")
            or name == "main"
        ):
            saved[name] = sys.modules.pop(name)

    sys.path.insert(0, str(tmp_path))
    try:
        with pytest.raises((ImportError, AttributeError, ValueError)):
            importlib.import_module("db_monitor.asgi")
    finally:
        sys.path.remove(str(tmp_path))
        sys.modules.update(saved)
