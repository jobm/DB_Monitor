"""Compatibility bridge into the current app-based runtime.

The package-standardization migration is intentionally phased. This module
keeps the new `db_monitor` namespace functional while existing production
entrypoints still live under `app/`.
"""

from __future__ import annotations

from collections.abc import Sequence
import sys
from pathlib import Path


def _legacy_app_root() -> Path:
    """Return the repository-local legacy app directory path."""
    return Path(__file__).resolve().parents[2] / "app"


def _ensure_legacy_path() -> None:
    """Add the legacy app directory to sys.path when available."""
    app_root = _legacy_app_root()
    if not app_root.exists():
        raise RuntimeError(
            "Legacy runtime directory 'app/' was not found. "
            "Complete Phase 2+ namespace migration or run from a full "
            "repository checkout."
        )

    app_root_text = str(app_root)
    if app_root_text not in sys.path:
        sys.path.insert(0, app_root_text)


def ensure_legacy_path() -> None:
    """Public helper to prepare legacy import paths during migration."""
    _ensure_legacy_path()


def run_legacy_server() -> None:
    """Start the current monitor server via legacy startup module."""
    _ensure_legacy_path()

    import start_monitor

    start_monitor.main()


def run_legacy_migrate(argv: Sequence[str] | None = None) -> int:
    """Execute legacy migration CLI with forwarded arguments."""
    _ensure_legacy_path()

    import migrate

    forwarded_args = list(argv or [])
    original_argv = sys.argv[:]
    try:
        sys.argv = ["migrate.py", *forwarded_args]
        return migrate.main()
    finally:
        sys.argv = original_argv
