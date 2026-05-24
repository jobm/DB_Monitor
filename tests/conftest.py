from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = REPO_ROOT / "app"
SRC_ROOT = REPO_ROOT / "src"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

sandbox_pytest_skip_reason = importlib.import_module(
    "examples.sandbox.preflight"
).sandbox_pytest_skip_reason


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Default repository tests to the core-platform marker.

    Sandbox validation currently runs through the live integration and smoke
    scripts rather than pytest modules. This hook makes the core-vs-sandbox
    split explicit for CI and local development without requiring every test
    file to repeat the same marker.
    """
    for item in items:
        if item.get_closest_marker("sandbox") is None:
            item.add_marker(pytest.mark.core)


def pytest_runtest_setup(item: pytest.Item) -> None:
    """Skip sandbox tests cleanly when the live stack is unavailable."""
    if item.get_closest_marker("sandbox") is None:
        return

    skip_reason = sandbox_pytest_skip_reason()
    if skip_reason is not None:
        pytest.skip(skip_reason)
