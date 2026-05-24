from __future__ import annotations

import importlib

import db_monitor.public_api as public_api


def test_declared_public_modules_are_importable() -> None:
    """Each declared public module path should be importable."""
    for module_name in public_api.SUPPORTED_PUBLIC_MODULES:
        module = importlib.import_module(module_name)
        assert module is not None


def test_declared_public_symbols_are_available() -> None:
    """Each declared public symbol should exist on its target module."""
    for entry in public_api.SUPPORTED_PUBLIC_SYMBOLS:
        module = importlib.import_module(entry.module)
        assert hasattr(module, entry.symbol)


def test_deprecation_grace_window_policy_defaults() -> None:
    """Deprecation grace windows should match the documented policy."""
    assert public_api.DEPRECATION_MIN_GRACE_MINOR_RELEASES >= 2
    assert public_api.DEPRECATION_MIN_GRACE_DAYS >= 90
