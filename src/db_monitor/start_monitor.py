"""Canonical startup entrypoint for DB Monitor.

This module provides a package-native startup surface while the phased
migration continues to delegate runtime behavior to legacy startup logic.
"""

from __future__ import annotations

from db_monitor.legacy_runtime import run_legacy_server


def main() -> None:
    """Start the DB Monitor API server."""
    run_legacy_server()


if __name__ == "__main__":
    main()
