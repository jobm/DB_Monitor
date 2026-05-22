"""Run or validate tracked database migrations for DB Monitor."""

from __future__ import annotations

import argparse
import asyncio

from extensions import AsyncSessionLocal, engine
from migrations import apply_migrations, validate_migrations


def _build_parser() -> argparse.ArgumentParser:
    """Create the CLI parser for migration operations."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=(
            "apply",
            "validate",
        ),
        nargs="?",
        default="apply",
        help=(
            "Whether to apply pending migrations or only validate "
            "schema state."
        ),
    )
    return parser


async def _run(command: str) -> int:
    """Execute the requested migration command."""
    if command == "validate":
        await validate_migrations(engine)
        print("Database schema validation succeeded.")
        return 0

    applied_versions = await apply_migrations(AsyncSessionLocal, engine)
    if applied_versions:
        print("Applied migrations:", ", ".join(applied_versions))
    else:
        print("No pending migrations.")
    return 0


def main() -> int:
    """CLI entrypoint."""
    args = _build_parser().parse_args()
    return asyncio.run(_run(args.command))


if __name__ == "__main__":
    raise SystemExit(main())
