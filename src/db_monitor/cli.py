"""Command-line interface for DB Monitor.

This CLI is the canonical package entrypoint and currently delegates runtime
operations to compatibility adapters while namespace migration is in progress.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from db_monitor.legacy_runtime import run_legacy_migrate, run_legacy_server


def _build_parser() -> argparse.ArgumentParser:
    """Build and return the root CLI parser."""
    parser = argparse.ArgumentParser(prog="db-monitor")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser(
        "serve",
        help="Run the DB Monitor API server.",
    )

    migrate_parser = subparsers.add_parser(
        "migrate",
        help="Apply or validate database migrations.",
    )
    migrate_parser.add_argument(
        "action",
        choices=("apply", "validate"),
        nargs="?",
        default="apply",
        help="Migration operation to execute.",
    )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the top-level CLI command dispatcher."""
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.command == "serve":
        run_legacy_server()
        return 0

    if args.command == "migrate":
        migrate_args = [args.action] if args.action else []
        return run_legacy_migrate(migrate_args)

    parser.print_help()
    return 1


def serve() -> int:
    """Script entrypoint for `db-monitor-serve`."""
    return main(["serve"])


def migrate() -> int:
    """Script entrypoint for `db-monitor-migrate`."""
    return main(["migrate"])
