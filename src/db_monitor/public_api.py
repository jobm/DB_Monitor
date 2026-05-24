"""Public API stability contract for the ``db_monitor`` package.

This module defines the set of import paths and symbols that DB Monitor
supports as public Python API surfaces.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PublicSymbol:
    """Describe one public symbol exported by a package module."""

    module: str
    symbol: str


SUPPORTED_PUBLIC_MODULES: tuple[str, ...] = (
    "db_monitor",
    "db_monitor.__main__",
    "db_monitor.asgi",
    "db_monitor.cli",
    "db_monitor.main",
    "db_monitor.start_monitor",
)
"""Package import paths covered by the public stability policy."""

SUPPORTED_PUBLIC_SYMBOLS: tuple[PublicSymbol, ...] = (
    PublicSymbol("db_monitor", "__version__"),
    PublicSymbol("db_monitor.asgi", "app"),
    PublicSymbol("db_monitor.cli", "main"),
    PublicSymbol("db_monitor.cli", "serve"),
    PublicSymbol("db_monitor.cli", "migrate"),
    PublicSymbol("db_monitor.main", "app"),
    PublicSymbol("db_monitor.start_monitor", "main"),
)
"""Public symbols covered by the public stability policy."""

DEPRECATION_MIN_GRACE_MINOR_RELEASES = 2
"""Minimum number of minor releases before removing a deprecated API."""

DEPRECATION_MIN_GRACE_DAYS = 90
"""Minimum number of days before removing a deprecated API."""


__all__ = [
    "DEPRECATION_MIN_GRACE_DAYS",
    "DEPRECATION_MIN_GRACE_MINOR_RELEASES",
    "PublicSymbol",
    "SUPPORTED_PUBLIC_MODULES",
    "SUPPORTED_PUBLIC_SYMBOLS",
]
